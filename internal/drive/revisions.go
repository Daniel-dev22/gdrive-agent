package drive

import (
	"context"

	"google.golang.org/api/drive/v3"
)

// RevisionList wraps Revisions.List for one file. The Drive Go SDK doesn't
// expose a stable batch API for revisions, so the agent makes one call per
// file. HTTP/2 multiplexing + AdaptiveLimiter gating make this acceptable
// for our scale (a few hundred files with revisions enabled). Caller must
// have already passed through limiter.Wait().
func RevisionList(ctx context.Context, svc *drive.Service, fileID string) ([]*drive.Revision, error) {
	var out []*drive.Revision
	pageToken := ""
	for {
		call := svc.Revisions.List(fileID).
			Fields("revisions(id,mimeType,modifiedTime,size,md5Checksum,originalFilename,keepForever),nextPageToken").
			PageSize(1000).
			Context(ctx)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		resp, err := call.Do()
		if err != nil {
			return nil, err
		}
		out = append(out, resp.Revisions...)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return out, nil
}
