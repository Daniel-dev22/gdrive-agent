package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// VersionLayout exposes the on-disk geometry for one account's version dir
// tree. Callers don't see paths directly; they hand a (fileID, srcPath,
// name) tuple and we put bits where they belong.
type VersionLayout struct {
	AccountRoot string // BACKUP_ROOT/<account_id>
}

func NewVersionLayout(accountRoot string) VersionLayout {
	return VersionLayout{AccountRoot: accountRoot}
}

// FileVersionsDir returns the directory under _versions/<fileID> holding
// rotated copies of one file. Caller does NOT need to mkdir — Rotate does.
func (v VersionLayout) FileVersionsDir(fileID string) string {
	return filepath.Join(v.AccountRoot, "_versions", fileID)
}

// Rotate moves a live file into _versions/<fileID>/<iso>__<name>. Atomic
// rename, no copy. Returns the version entry name (e.g.
// "2026-05-14T03-00-00Z__foo.md") for the caller to append to manifest.Versions.
//
// Pass "" for at to use time.Now().UTC().
func (v VersionLayout) Rotate(fileID, livePath, displayName string, at time.Time) (string, error) {
	if _, err := os.Stat(livePath); os.IsNotExist(err) {
		// Nothing to rotate. Not an error — first download of a file.
		return "", nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	stamp := at.UTC().Format("2006-01-02T15-04-05Z")
	name := stamp + "__" + SanitizeName(displayName)
	dst := filepath.Join(v.FileVersionsDir(fileID), name)
	if err := MoveAtomic(livePath, dst); err != nil {
		return "", fmt.Errorf("rotate live → version: %w", err)
	}
	return name, nil
}

// Prune trims _versions/<fileID>/ down to `keep` newest entries (by
// timestamp prefix). Returns the slice of entries pruned (just for the
// job log).
//
// keep <= 0 → no-op (treat as "keep all"). Operators set keep_versions=0
// in the UI when they want unlimited retention.
func (v VersionLayout) Prune(fileID string, keep int) ([]string, error) {
	if keep <= 0 {
		return nil, nil
	}
	dir := v.FileVersionsDir(fileID)
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read _versions dir: %w", err)
	}
	if len(ents) <= keep {
		return nil, nil
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	// Sort ascending — the timestamp prefix sorts lexicographically because
	// we use a zero-padded ISO-8601-like format.
	slices.Sort(names)
	cut := len(names) - keep
	pruned := make([]string, 0, cut)
	for i := 0; i < cut; i++ {
		p := filepath.Join(dir, names[i])
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return pruned, fmt.Errorf("prune %s: %w", names[i], err)
		}
		pruned = append(pruned, names[i])
	}
	return pruned, nil
}

// ListVersions returns the version filenames for one file, newest last.
// Empty slice (no error) if the dir doesn't exist.
func (v VersionLayout) ListVersions(fileID string) ([]string, error) {
	dir := v.FileVersionsDir(fileID)
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	slices.Sort(out)
	return out, nil
}

// VersionPath returns the absolute path to one specific version. Caller is
// responsible for stat'ing it (could be deleted by a concurrent prune,
// though job-runner takes a global mutex to avoid that).
func (v VersionLayout) VersionPath(fileID, versionName string) string {
	return filepath.Join(v.FileVersionsDir(fileID), versionName)
}

// SplitVersionName extracts the ISO timestamp + display name from a
// rotated filename. Returns ok=false if the layout doesn't match
// "<iso>__<name>".
func SplitVersionName(name string) (iso, display string, ok bool) {
	idx := strings.Index(name, "__")
	if idx <= 0 {
		return "", "", false
	}
	return name[:idx], name[idx+2:], true
}
