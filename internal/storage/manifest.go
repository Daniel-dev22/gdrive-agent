package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Manifest is the per-account file index that powers the 3-tier idempotency
// check in the backup walker. One file per account_id, stored at
// <BACKUP_ROOT>/<account_id>/manifest.json. A previous-good copy lives at
// manifest.json.bak — written by promoting the current file before each
// rename.
type Manifest struct {
	AccountID string                  `json:"account_id"`
	UpdatedAt time.Time               `json:"updated_at"`
	Files     map[string]ManifestFile `json:"files"`
}

// ManifestFile mirrors the schema layout from plan §F.4.3. Binary files
// populate MD5; native files (Sheets/Docs) leave it empty and rely on
// HeadRevisionID + Version + Size for change detection.
type ManifestFile struct {
	Path           string    `json:"path"`
	ModifiedTime   time.Time `json:"modified_time"`
	MD5            string    `json:"md5,omitempty"`
	Size           int64     `json:"size"`
	HeadRevisionID string    `json:"head_revision_id,omitempty"`
	Version        int64     `json:"version,omitempty"`
	MimeType       string    `json:"mime_type"`
	VersionCount   int       `json:"version_count,omitempty"`
	Versions       []string  `json:"versions,omitempty"`
	// DeletedAt is set when the file disappeared from Drive. The file's
	// content is moved to graveyard/ and its entry is retained in the
	// manifest until the graveyard sweeper drops it (graveyard_keep_days).
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

// ManifestStore is the threadsafe owner of one account's manifest. Backup
// runner instantiates it at job start, calls Get/Set during the walk, then
// Save() at the end. No partial writes — Save() builds the full new JSON,
// promotes the current file to .bak, then renames the temp into place.
type ManifestStore struct {
	path string
	mu   sync.Mutex
	m    *Manifest
}

// NewManifestStore opens (or fresh-initializes) the manifest for the given
// account. Missing file → fresh empty Manifest; corrupt JSON → fall back to
// .bak; both missing → fresh empty. Never errors on missing input.
func NewManifestStore(backupRoot, accountID string) (*ManifestStore, error) {
	if accountID == "" {
		return nil, errors.New("manifest: accountID required")
	}
	path := filepath.Join(backupRoot, accountID, "manifest.json")
	s := &ManifestStore{path: path}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ManifestStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.m = &Manifest{Files: map[string]ManifestFile{}}
			return nil
		}
		return fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if jsonErr := json.Unmarshal(data, &m); jsonErr != nil {
		// Try .bak
		if bakData, bakErr := os.ReadFile(s.path + ".bak"); bakErr == nil {
			if json.Unmarshal(bakData, &m) == nil {
				s.m = ensureFilesMap(&m)
				return nil
			}
		}
		// Both corrupted — start fresh rather than refuse to run.
		s.m = &Manifest{Files: map[string]ManifestFile{}}
		return nil
	}
	s.m = ensureFilesMap(&m)
	return nil
}

func ensureFilesMap(m *Manifest) *Manifest {
	if m.Files == nil {
		m.Files = map[string]ManifestFile{}
	}
	return m
}

// Get returns a copy of the file's entry (ok=false if absent).
func (s *ManifestStore) Get(fileID string) (ManifestFile, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mf, ok := s.m.Files[fileID]
	return mf, ok
}

// Set inserts/updates the entry. Concurrent-safe.
func (s *ManifestStore) Set(fileID string, mf ManifestFile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m.Files[fileID] = mf
}

// Delete removes the entry entirely (used after graveyard prune).
func (s *ManifestStore) Delete(fileID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m.Files, fileID)
}

// MarkDeleted stamps DeletedAt on the entry without removing it. Used when
// a file vanishes from Drive — we keep the manifest row until the
// graveyard retention window expires.
func (s *ManifestStore) MarkDeleted(fileID string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mf, ok := s.m.Files[fileID]
	if !ok {
		return
	}
	mf.DeletedAt = &at
	s.m.Files[fileID] = mf
}

// Snapshot returns a deep-ish copy of all entries (map value semantics; the
// caller MAY mutate the returned map without affecting the store). Use this
// in the walker's "files in manifest but missing from Drive" pass.
func (s *ManifestStore) Snapshot() map[string]ManifestFile {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]ManifestFile, len(s.m.Files))
	for k, v := range s.m.Files {
		out[k] = v
	}
	return out
}

// AccountID returns the manifest's account_id (after first Save it's
// guaranteed correct; before first Save it may be empty).
func (s *ManifestStore) AccountID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.AccountID
}

// SetAccountID stamps the account_id on the manifest. Called once at
// store init.
func (s *ManifestStore) SetAccountID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m.AccountID = id
}

// Save serializes the manifest and writes it atomically:
//  1. Marshal current state.
//  2. If manifest.json exists, rename it to manifest.json.bak (atomic).
//  3. Write the new state to a sibling tmp file, fsync, rename → manifest.json.
//
// On a crash mid-Save:
//   - between (2) and (3): manifest.json missing, .bak holds last good copy.
//   - during (3) tmp write: tmp file orphaned (no effect on current).
//   - after rename: new file in place. Recovery sees it.
func (s *ManifestStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m.UpdatedAt = time.Now().UTC()

	payload, err := json.MarshalIndent(s.m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	bakPath := s.path + ".bak"
	if _, err := os.Stat(s.path); err == nil {
		// Promote current → .bak. os.Rename is atomic on same filesystem.
		if err := os.Rename(s.path, bakPath); err != nil {
			return fmt.Errorf("promote manifest.json → .bak: %w", err)
		}
	}
	return AtomicWriteJSON(s.path, payload)
}
