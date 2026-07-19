package drivestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/storage"
)

// statsFileName is the per-account stats sidecar, written next to manifest.json
// at <BACKUP_ROOT>/<account_id>/stats.json. It caches the aggregate footprint of
// the mirror so the UI never has to re-read the whole manifest + crawl the disk
// on every page load. The numbers only change when a backup runs, so they're
// recomputed once at backup completion (see job.Runner) and served from here.
const statsFileName = "stats.json"

// RepoStats is the cached aggregate view of one account's backup mirror. It is
// recomputed once per successful backup and persisted as a JSON sidecar; reads
// are O(1) (load one small file) instead of re-deriving from the manifest and
// crawling the on-disk tree on every request.
//
// "Repo" in the name matches the API surface (the per-repo stats endpoint), but
// the data is per-account: the manifest and the on-disk tree
// (<accountRoot>/{files,_versions,graveyard,_drive_revisions}) are keyed by
// account_id, and multiple repos bound to the same account share these numbers.
type RepoStats struct {
	AccountID  string    `json:"account_id"`
	ComputedAt time.Time `json:"computed_at"`

	// Current live mirror — derived from the manifest (non-deleted entries).
	CurrentFiles   int   `json:"current_files"`        // entries with DeletedAt == nil
	CurrentBytes   int64 `json:"current_bytes"`        // sum of mf.Size (logical Drive size)
	FilesWithRev   int   `json:"files_with_revisions"` // entries with VersionCount > 0
	TotalRevisions int   `json:"total_revisions"`      // sum of mf.VersionCount

	// On-disk footprint — a single crawl of each bucket under accountRoot.
	LiveBytes      int64 `json:"live_bytes"`           // files/
	VersionFiles   int   `json:"version_files"`        // count under _versions/
	VersionBytes   int64 `json:"version_bytes"`        // _versions/
	GraveyardFiles int   `json:"graveyard_files"`      // count under graveyard/
	GraveyardBytes int64 `json:"graveyard_bytes"`      // graveyard/
	DriveRevFiles  int   `json:"drive_revision_files"` // count under _drive_revisions/
	DriveRevBytes  int64 `json:"drive_revision_bytes"` // _drive_revisions/
	TotalDiskBytes int64 `json:"total_disk_bytes"`     // live + version + graveyard + driverev
}

// ComputeRepoStats derives the aggregate stats from the in-memory manifest plus
// a one-shot crawl of the four on-disk buckets. accountRoot is
// <BACKUP_ROOT>/<account_id>. Missing bucket dirs are treated as empty (a fresh
// account that has never had versions/graveyard/revisions). The crawl skips
// directory entries and only sums regular files.
func ComputeRepoStats(accountID, accountRoot string, manifest map[string]storage.ManifestFile) (RepoStats, error) {
	s := RepoStats{AccountID: accountID, ComputedAt: time.Now().UTC()}

	for _, mf := range manifest {
		if mf.DeletedAt != nil {
			continue
		}
		s.CurrentFiles++
		s.CurrentBytes += mf.Size
		if mf.VersionCount > 0 {
			s.FilesWithRev++
		}
		s.TotalRevisions += mf.VersionCount
	}

	// files/ contributes bytes only — the live file count is the manifest's
	// CurrentFiles above (the on-disk tree may transiently include .part files
	// mid-download, which we don't want to count as files).
	liveBytes, _, err := crawlBucket(filepath.Join(accountRoot, "files"))
	if err != nil {
		return RepoStats{}, fmt.Errorf("crawl files: %w", err)
	}
	s.LiveBytes = liveBytes

	if s.VersionBytes, s.VersionFiles, err = crawlBucket(filepath.Join(accountRoot, "_versions")); err != nil {
		return RepoStats{}, fmt.Errorf("crawl _versions: %w", err)
	}
	if s.GraveyardBytes, s.GraveyardFiles, err = crawlBucket(filepath.Join(accountRoot, "graveyard")); err != nil {
		return RepoStats{}, fmt.Errorf("crawl graveyard: %w", err)
	}
	if s.DriveRevBytes, s.DriveRevFiles, err = crawlBucket(filepath.Join(accountRoot, "_drive_revisions")); err != nil {
		return RepoStats{}, fmt.Errorf("crawl _drive_revisions: %w", err)
	}

	s.TotalDiskBytes = s.LiveBytes + s.VersionBytes + s.GraveyardBytes + s.DriveRevBytes
	return s, nil
}

// crawlBucket walks dir, summing regular-file sizes and counts. A missing dir
// is not an error (returns zeroes) — buckets are created lazily on first use.
func crawlBucket(dir string) (totalBytes int64, fileCount int, err error) {
	walkErr := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return fs.SkipDir
			}
			return walkErr
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			if errors.Is(infoErr, fs.ErrNotExist) {
				return nil // raced with a concurrent rotate/prune — skip
			}
			return infoErr
		}
		totalBytes += info.Size()
		fileCount++
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, fs.ErrNotExist) {
		return 0, 0, walkErr
	}
	return totalBytes, fileCount, nil
}

// WriteRepoStats persists s to <accountRoot>/stats.json atomically (tmp+rename
// via AtomicWriteJSON), so a crash mid-write never leaves a torn file.
func WriteRepoStats(accountRoot string, s RepoStats) error {
	payload, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal repo stats: %w", err)
	}
	return storage.AtomicWriteJSON(filepath.Join(accountRoot, statsFileName), payload)
}

// ReadRepoStats loads the cached stats for an account. ok is false (with no
// error) when the sidecar doesn't exist yet — i.e. no backup has completed, so
// callers should surface "no stats yet" rather than an error. A corrupt file
// returns an error.
func ReadRepoStats(accountRoot string) (RepoStats, bool, error) {
	data, err := os.ReadFile(filepath.Join(accountRoot, statsFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return RepoStats{}, false, nil
		}
		return RepoStats{}, false, fmt.Errorf("read repo stats: %w", err)
	}
	var s RepoStats
	if err := json.Unmarshal(data, &s); err != nil {
		return RepoStats{}, false, fmt.Errorf("unmarshal repo stats: %w", err)
	}
	return s, true, nil
}
