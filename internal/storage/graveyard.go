package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Graveyard handles soft-delete + retention for files that vanished from
// Drive. Layout:
//
//	<accountRoot>/graveyard/<fileID>/<iso>__<sanitizedName>
//
// Backup walker calls Bury() once per missing file. A periodic prune job
// (or end-of-backup sweep) calls Sweep() with the keep-days window from
// the repo config.
type Graveyard struct {
	AccountRoot string
}

func NewGraveyard(accountRoot string) Graveyard {
	return Graveyard{AccountRoot: accountRoot}
}

// Bury moves livePath into graveyard/<fileID>/<iso>__<displayName>. If
// livePath doesn't exist, Bury is a no-op (caller may have already buried
// it on an earlier run before crash-recovery).
func (g Graveyard) Bury(fileID, livePath, displayName string, at time.Time) (string, error) {
	if _, err := os.Stat(livePath); os.IsNotExist(err) {
		return "", nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	stamp := at.UTC().Format("2006-01-02T15-04-05Z")
	name := stamp + "__" + SanitizeName(displayName)
	dst := filepath.Join(g.AccountRoot, "graveyard", fileID, name)
	if err := MoveAtomic(livePath, dst); err != nil {
		return "", fmt.Errorf("bury %s: %w", livePath, err)
	}
	return name, nil
}

// Sweep removes graveyard entries whose ISO-timestamp prefix is older than
// keepDays. Returns the count pruned and the first error encountered (rest
// continue best-effort).
//
// keepDays <= 0 → no-op (unlimited retention).
func (g Graveyard) Sweep(keepDays int, now time.Time) (int, error) {
	if keepDays <= 0 {
		return 0, nil
	}
	root := filepath.Join(g.AccountRoot, "graveyard")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return 0, nil
	}
	cutoff := now.UTC().Add(-time.Duration(keepDays) * 24 * time.Hour)
	pruned := 0
	var firstErr error

	fileDirs, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	for _, fd := range fileDirs {
		if !fd.IsDir() {
			continue
		}
		fileDir := filepath.Join(root, fd.Name())
		ents, err := os.ReadDir(fileDir)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, e := range ents {
			iso, _, ok := SplitVersionName(e.Name())
			if !ok {
				continue
			}
			// Replace dashes-in-time with colons to parse RFC3339-like.
			t, err := parseGraveStamp(iso)
			if err != nil {
				continue
			}
			if t.Before(cutoff) {
				p := filepath.Join(fileDir, e.Name())
				if rmErr := os.Remove(p); rmErr != nil && !os.IsNotExist(rmErr) {
					if firstErr == nil {
						firstErr = rmErr
					}
					continue
				}
				pruned++
			}
		}
		// If the file's graveyard subdir is now empty, remove it too.
		remaining, _ := os.ReadDir(fileDir)
		if len(remaining) == 0 {
			_ = os.Remove(fileDir)
		}
	}
	return pruned, firstErr
}

// parseGraveStamp accepts "2026-05-14T03-00-00Z" (our format) and converts
// back to time.Time. The format uses '-' between H/M/S instead of ':' so the
// string is filesystem-safe everywhere.
func parseGraveStamp(s string) (time.Time, error) {
	// Restore RFC3339 colons.
	if !strings.HasSuffix(s, "Z") {
		return time.Time{}, fmt.Errorf("not a Z-terminated stamp: %s", s)
	}
	// Replace last two '-' in the time portion with ':'.
	parts := strings.SplitN(s, "T", 2)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("missing T: %s", s)
	}
	timePart := strings.TrimSuffix(parts[1], "Z")
	timePart = strings.Replace(timePart, "-", ":", 2)
	return time.Parse(time.RFC3339, parts[0]+"T"+timePart+"Z")
}
