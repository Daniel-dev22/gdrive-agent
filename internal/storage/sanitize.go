package storage

import (
	"strings"
	"unicode/utf8"
)

// MaxNameLen caps each path component at 200 bytes. Most filesystems allow
// 255, but trimming earlier leaves headroom for the "<iso>__" prefix the
// version directory layout prepends.
const MaxNameLen = 200

// SanitizeName replaces every byte that would be unsafe on a POSIX
// filesystem (or that breaks shell pasting) with "_". This is path-component
// safety only — callers split on "/" before calling and reassemble after.
//
// Rejected bytes:
//   - control chars 0x00..0x1f
//   - path separators / \
//   - shell-breaking whitespace \r \n \t
//
// Note: spaces are intentionally kept. Drive lets users name files with
// spaces and we want to preserve that for human-friendly _versions/ entries.
func SanitizeName(in string) string {
	if in == "" {
		return "_"
	}
	if !utf8.ValidString(in) {
		// Defensive: drop invalid UTF-8 rather than panic in path code.
		in = strings.ToValidUTF8(in, "_")
	}
	b := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		c := in[i]
		switch {
		case c < 0x20:
			b = append(b, '_')
		case c == '/' || c == '\\':
			b = append(b, '_')
		case c == '\r' || c == '\n' || c == '\t':
			b = append(b, '_')
		default:
			b = append(b, c)
		}
	}
	out := string(b)
	// Strip leading dots so we never accidentally write a "hidden" file
	// or step on macOS resource-fork-style names.
	out = strings.TrimLeft(out, ".")
	if out == "" {
		out = "_"
	}
	if len(out) > MaxNameLen {
		out = out[:MaxNameLen]
	}
	return out
}

// SanitizePath splits on "/", sanitizes each component, and re-joins. Empty
// components are dropped (collapses "a//b" → "a/b").
func SanitizePath(in string) string {
	parts := strings.Split(in, "/")
	out := parts[:0]
	for _, p := range parts {
		if p == "" {
			continue
		}
		out = append(out, SanitizeName(p))
	}
	return strings.Join(out, "/")
}
