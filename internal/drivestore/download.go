// Package drivestore holds the Drive-specific on-disk pieces that did NOT
// generalize into the shared agent-kit-go/storage toolbox: the resumable,
// md5-verified Drive media download (StreamDownload) and the per-account mirror
// stats sidecar (stats.go). Everything transport-neutral — AtomicWriteJSON,
// MoveAtomic, ManifestStore, VersionLayout, Graveyard, SanitizeName/Path, plus
// the new CopyToFileAtomic/ChunkStage — now lives in
// github.com/Daniel-dev22/agent-kit-go/storage and is consumed from there.
//
// StreamDownload is intentionally NOT re-expressed on top of the kit's
// CopyToFileAtomic. CopyToFileAtomic opens its .part with O_TRUNC and removes
// it on ANY error, whereas StreamDownload's byte-range resume relies on the
// .part surviving a mid-stream failure so the NEXT run can re-`Range` from
// where it stopped (see the resumeAt logic below). Routing the copy core
// through CopyToFileAtomic would silently break resume for large interrupted
// downloads — the failure mode being a multi-GB file that restarts from byte 0
// on every attempt — so the copy core stays inlined here.
package drivestore

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

// bufferSize is the per-copy block size. 64KB is large enough that read/write
// syscalls aren't the bottleneck on any reasonable hardware, small enough that
// goroutine memory stays flat even with hundreds of concurrent transfers.
const bufferSize = 64 * 1024

// bufPool reuses 64KB byte slices across every Copy operation. sync.Pool is
// goroutine-safe and the Go runtime drains it under memory pressure, so the
// agent never holds idle buffers when nothing's downloading.
var bufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, bufferSize)
		return &b
	},
}

// DownloadResult captures the post-download state of a streamed file: the
// final on-disk size and the hex-encoded md5 hash computed in-band. Callers
// use this to (a) validate against Drive's md5Checksum and (b) record into
// the manifest without rehashing.
type DownloadResult struct {
	Size   int64
	MD5Hex string
}

// DownloadReq packages the inputs to a streaming download with resume.
type DownloadReq struct {
	// URL is the absolute Drive media URL (typically the result of
	// https://www.googleapis.com/drive/v3/files/<id>?alt=media). Caller
	// must have appended any required query string (e.g. supportsAllDrives).
	URL string

	// AuthHeader is the value of the Authorization header to send. Built
	// by the caller from the oauth2 TokenSource so the storage package
	// doesn't depend on oauth2.
	AuthHeader string

	// IfModifiedSince — RFC1123 timestamp. If set, request gets that
	// header and a 304 response is treated as a successful no-op
	// (returns IsNotModified=true).
	IfModifiedSince string

	// DestPath is the final on-disk path the .part file will be renamed
	// to on success. Parent dirs are created with mkdir -p.
	DestPath string

	// ExpectedMD5 — if non-empty, agent compares the in-band md5 to this
	// after download. Mismatch causes .part removal and an error so the
	// caller can retry. Native files (no md5Checksum) pass "".
	ExpectedMD5 string

	// HTTPClient is the shared *http.Client (with HTTP/2 transport) the
	// drive.ClientFactory exposes. Reuse across calls is mandatory for
	// connection pooling.
	HTTPClient *http.Client
}

// StreamDownload performs the layered streaming download:
//
//  1. Create parent dir.
//  2. Open .part with O_CREATE|O_RDWR (so we can re-stat for resume).
//  3. If .part has bytes, send Range: bytes=N- and seek to end before write.
//  4. io.CopyBuffer through io.MultiWriter(.part, md5Hasher) — md5 is
//     computed as the bytes pass through, no second pass.
//  5. On md5 mismatch, unlink .part and return an error tagged with
//     ErrMD5Mismatch so the caller retries once.
//  6. fsync the file, fsync the parent dir, then os.Rename .part → final.
//
// Returns IsNotModified=true if the server replied 304 (only possible if
// IfModifiedSince was set). In that case nothing is touched on disk.
func StreamDownload(ctx context.Context, req DownloadReq) (DownloadResult, bool, error) {
	if req.URL == "" || req.DestPath == "" || req.HTTPClient == nil {
		return DownloadResult{}, false, errors.New("storage: invalid DownloadReq")
	}

	partPath := req.DestPath + ".part"
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		return DownloadResult{}, false, fmt.Errorf("mkdir parent: %w", err)
	}

	// Open .part RDWR so we can both append and re-hash from scratch (we
	// don't re-hash on resume — see comment below).
	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return DownloadResult{}, false, fmt.Errorf("open .part: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return DownloadResult{}, false, fmt.Errorf("stat .part: %w", err)
	}
	resumeAt := info.Size()

	// Build request.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
	if err != nil {
		return DownloadResult{}, false, err
	}
	if req.AuthHeader != "" {
		httpReq.Header.Set("Authorization", req.AuthHeader)
	}
	if req.IfModifiedSince != "" {
		httpReq.Header.Set("If-Modified-Since", req.IfModifiedSince)
	}
	if resumeAt > 0 {
		httpReq.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeAt))
	}

	resp, err := req.HTTPClient.Do(httpReq)
	if err != nil {
		return DownloadResult{}, false, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		// Caller decides whether to keep or discard the .part. We leave
		// it so a later retry can finish. For typical "tier-3 304" the
		// .part is empty anyway.
		return DownloadResult{}, true, nil
	case http.StatusOK:
		// Server ignored Range (or we sent none) — truncate any partial
		// bytes and hash from scratch.
		if resumeAt > 0 {
			if err := f.Truncate(0); err != nil {
				return DownloadResult{}, false, fmt.Errorf("truncate .part: %w", err)
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return DownloadResult{}, false, fmt.Errorf("seek .part: %w", err)
			}
			resumeAt = 0
		}
	case http.StatusPartialContent:
		// Server honored Range — seek to end and append.
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			return DownloadResult{}, false, fmt.Errorf("seek .part end: %w", err)
		}
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return DownloadResult{}, false, fmt.Errorf("download HTTP %d: %s", resp.StatusCode, string(body))
	}

	// md5 hashing path. If resumeAt > 0 we'd need to re-hash the already-
	// downloaded prefix to get a valid digest; Drive's md5Checksum applies
	// to the full file, so a partial-only digest is useless. Solution:
	// re-hash from the beginning of the .part by reading what's on disk
	// THEN piping new bytes through.
	var h hash.Hash
	if req.ExpectedMD5 != "" || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		h = md5.New()
		if resumeAt > 0 {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return DownloadResult{}, false, fmt.Errorf("seek for rehash: %w", err)
			}
			bufPtr := bufPool.Get().(*[]byte)
			_, copyErr := io.CopyBuffer(h, io.LimitReader(f, resumeAt), *bufPtr)
			bufPool.Put(bufPtr)
			if copyErr != nil {
				return DownloadResult{}, false, fmt.Errorf("rehash existing: %w", copyErr)
			}
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				return DownloadResult{}, false, fmt.Errorf("seek post-rehash: %w", err)
			}
		}
	}

	// Stream remaining body into file + hasher.
	var dst io.Writer = f
	if h != nil {
		dst = io.MultiWriter(f, h)
	}
	bufPtr := bufPool.Get().(*[]byte)
	written, copyErr := io.CopyBuffer(dst, resp.Body, *bufPtr)
	bufPool.Put(bufPtr)
	if copyErr != nil {
		return DownloadResult{}, false, fmt.Errorf("stream body: %w", copyErr)
	}

	finalSize := resumeAt + written
	res := DownloadResult{Size: finalSize}
	if h != nil {
		res.MD5Hex = hex.EncodeToString(h.Sum(nil))
	}

	// md5 validation (binary files only — caller passes empty for natives).
	if req.ExpectedMD5 != "" && res.MD5Hex != "" && req.ExpectedMD5 != res.MD5Hex {
		_ = os.Remove(partPath)
		return res, false, fmt.Errorf("%w: expected %s got %s", ErrMD5Mismatch, req.ExpectedMD5, res.MD5Hex)
	}

	// fsync file, then atomically rename into place.
	if err := f.Sync(); err != nil {
		return res, false, fmt.Errorf("fsync .part: %w", err)
	}
	if err := f.Close(); err != nil {
		return res, false, fmt.Errorf("close .part: %w", err)
	}
	// Re-open defer Close()'d file — Sync above is what mattered, and we
	// want the file descriptor gone before rename on Windows. (Harmless on
	// linux; we kept it to defend a future cross-platform port.)
	if err := os.Rename(partPath, req.DestPath); err != nil {
		return res, false, fmt.Errorf("rename .part → final: %w", err)
	}
	// fsync the parent dir so the rename is durable after a crash.
	if dirFD, err := os.Open(filepath.Dir(req.DestPath)); err == nil {
		_ = dirFD.Sync()
		_ = dirFD.Close()
	}
	return res, false, nil
}

// ErrMD5Mismatch is returned (wrapped) when an in-band md5 disagrees with
// Drive's md5Checksum. Caller pattern: errors.Is(err, ErrMD5Mismatch).
var ErrMD5Mismatch = errors.New("md5 mismatch")
