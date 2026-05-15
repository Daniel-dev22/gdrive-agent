package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// RequireBearer returns a gin middleware that enforces the `Authorization:
// Bearer <token>` header on every request. The expected token is a plaintext
// node token loaded from GDRIVE_NODE_TOKEN. Comparison is done over the
// SHA-256 digest using crypto/subtle.ConstantTimeCompare so a slow attacker
// can't probe the secret byte-by-byte through timing.
//
// Why SHA-256 of both sides?
// constant-time compare requires equal-length inputs. Hashing normalizes
// length without leaking which side was shorter; even if the attacker
// supplies a 1-byte header, both digests are 32 bytes and the compare runs
// in fixed time regardless of input shape.
func RequireBearer(expected string) gin.HandlerFunc {
	expectedDigest := sha256.Sum256([]byte(expected))
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}
		got := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		if got == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "empty bearer token"})
			return
		}
		gotDigest := sha256.Sum256([]byte(got))
		if subtle.ConstantTimeCompare(gotDigest[:], expectedDigest[:]) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid bearer token"})
			return
		}
		c.Next()
	}
}
