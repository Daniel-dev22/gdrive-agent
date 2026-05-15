package job

import (
	"io"
	"strings"
	"sync"
)

// helperBufPool mirrors storage.bufPool for restore-side copies. Kept here
// rather than exporting the storage pool to avoid widening that package's
// public surface.
var helperBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 64*1024)
		return &b
	},
}

func copyPooled(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := helperBufPool.Get().(*[]byte)
	defer helperBufPool.Put(bufPtr)
	return io.CopyBuffer(dst, src, *bufPtr)
}

// isFolder reports whether the mime type is a Google Drive folder.
func isFolder(mime string) bool {
	return mime == "application/vnd.google-apps.folder"
}

// isNativeMime detects Sheets/Docs/Slides/Drawings — files Drive doesn't
// allow direct binary download for. Caller must use the files.export API
// with a target export mime instead.
func isNativeMime(mime string) bool {
	return strings.HasPrefix(mime, "application/vnd.google-apps.") && !isFolder(mime) && mime != "application/vnd.google-apps.shortcut"
}

// defaultExportMime maps a native Google mime type to the best-fidelity
// export format. Choices:
//   - Docs → docx (preserves formatting better than odt/pdf for round-trip)
//   - Sheets → xlsx
//   - Slides → pptx
//   - Drawings → png (lossy but the only sane default)
//   - Forms → no export; return empty so caller skips
//   - Sites → no export
//   - Maps → no export
// Returns "" for unsupported types.
func defaultExportMime(mime string) string {
	switch mime {
	case "application/vnd.google-apps.document":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case "application/vnd.google-apps.spreadsheet":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "application/vnd.google-apps.presentation":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case "application/vnd.google-apps.drawing":
		return "image/png"
	case "application/vnd.google-apps.script":
		return "application/vnd.google-apps.script+json"
	default:
		return ""
	}
}
