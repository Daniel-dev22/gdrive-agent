package drivestore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/storage"
)

// writeFile creates a file with `size` bytes of zeroes under accountRoot,
// making parent dirs as needed.
func writeFile(t *testing.T, accountRoot string, size int, parts ...string) {
	t.Helper()
	p := filepath.Join(append([]string{accountRoot}, parts...)...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, make([]byte, size), 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}

func TestComputeRepoStats(t *testing.T) {
	root := t.TempDir()

	// Live mirror: 3 files (one is a stray .part that must NOT be counted as a
	// file, but its bytes still land in the files/ crawl total).
	writeFile(t, root, 100, "files", "a.txt")
	writeFile(t, root, 200, "files", "sub", "b.txt")

	// Two rotated versions for one file, one for another.
	writeFile(t, root, 50, "_versions", "fid1", "2026-05-14T03-00-00Z__a.txt")
	writeFile(t, root, 60, "_versions", "fid1", "2026-05-13T03-00-00Z__a.txt")
	writeFile(t, root, 70, "_versions", "fid2", "2026-05-14T03-00-00Z__b.txt")

	// One graveyard entry.
	writeFile(t, root, 25, "graveyard", "fid3", "2026-05-10T03-00-00Z__gone.txt")

	// Two drive revisions.
	writeFile(t, root, 10, "_drive_revisions", "fid1", "rev1")
	writeFile(t, root, 15, "_drive_revisions", "fid1", "rev2")

	deletedAt := time.Now().UTC()
	manifest := map[string]storage.ManifestFile{
		"fid1": {Path: "a.txt", Size: 100, VersionCount: 2},
		"fid2": {Path: "sub/b.txt", Size: 200, VersionCount: 1},
		// Soft-deleted entry: excluded from current counts/bytes.
		"fid3": {Path: "gone.txt", Size: 999, VersionCount: 1, DeletedAt: &deletedAt},
	}

	s, err := ComputeRepoStats("acct-1", root, manifest)
	if err != nil {
		t.Fatalf("ComputeRepoStats: %v", err)
	}

	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"CurrentFiles", int64(s.CurrentFiles), 2},
		{"CurrentBytes", s.CurrentBytes, 300},   // 100 + 200 (fid3 excluded)
		{"FilesWithRev", int64(s.FilesWithRev), 2}, // fid1, fid2 (fid3 deleted)
		{"TotalRevisions", int64(s.TotalRevisions), 3}, // 2 + 1
		{"LiveBytes", s.LiveBytes, 300},
		{"VersionFiles", int64(s.VersionFiles), 3},
		{"VersionBytes", s.VersionBytes, 180}, // 50 + 60 + 70
		{"GraveyardFiles", int64(s.GraveyardFiles), 1},
		{"GraveyardBytes", s.GraveyardBytes, 25},
		{"DriveRevFiles", int64(s.DriveRevFiles), 2},
		{"DriveRevBytes", s.DriveRevBytes, 25},      // 10 + 15
		{"TotalDiskBytes", s.TotalDiskBytes, 530},   // 300 + 180 + 25 + 25
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if s.AccountID != "acct-1" {
		t.Errorf("AccountID = %q, want acct-1", s.AccountID)
	}
	if s.ComputedAt.IsZero() {
		t.Error("ComputedAt should be set")
	}
}

func TestComputeRepoStatsEmptyAccount(t *testing.T) {
	// A fresh account with no on-disk buckets at all must not error.
	root := t.TempDir()
	s, err := ComputeRepoStats("acct-empty", root, map[string]storage.ManifestFile{})
	if err != nil {
		t.Fatalf("ComputeRepoStats on empty account: %v", err)
	}
	if s.TotalDiskBytes != 0 || s.CurrentFiles != 0 {
		t.Errorf("expected all-zero stats, got %+v", s)
	}
}

func TestReadWriteRepoStatsRoundTrip(t *testing.T) {
	root := t.TempDir()

	// Before any write, ReadRepoStats reports "not present" with no error.
	if _, ok, err := ReadRepoStats(root); err != nil || ok {
		t.Fatalf("pre-write ReadRepoStats: ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	want := RepoStats{
		AccountID:      "acct-1",
		ComputedAt:     time.Now().UTC().Truncate(time.Second),
		CurrentFiles:   2,
		CurrentBytes:   300,
		TotalDiskBytes: 530,
	}
	if err := WriteRepoStats(root, want); err != nil {
		t.Fatalf("WriteRepoStats: %v", err)
	}

	got, ok, err := ReadRepoStats(root)
	if err != nil || !ok {
		t.Fatalf("post-write ReadRepoStats: ok=%v err=%v", ok, err)
	}
	if got.AccountID != want.AccountID || got.CurrentBytes != want.CurrentBytes ||
		got.TotalDiskBytes != want.TotalDiskBytes || !got.ComputedAt.Equal(want.ComputedAt) {
		t.Errorf("round-trip mismatch: got %+v, want %+v", got, want)
	}
}
