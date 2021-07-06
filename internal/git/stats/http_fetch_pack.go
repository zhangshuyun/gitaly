package stats

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

// HTTPFetchPack is a FetchPack obtained via a clone of a target repository via HTTP. It contains
// additional information about the cloning process like status codes and timings.
type HTTPFetchPack struct {
	start          time.Time
	responseHeader time.Duration
	httpStatus     int
	stats          FetchPack
	wantedRefs     []string
}

// ResponseHeader returns how long it took to receive the response header.
func (p *HTTPFetchPack) ResponseHeader() time.Duration { return p.responseHeader }

// HTTPStatus returns the HTTP status code.
func (p *HTTPFetchPack) HTTPStatus() int { return p.httpStatus }

// NAK returns how long it took to receive the NAK which signals that negotiation has concluded.
func (p *HTTPFetchPack) NAK() time.Duration { return p.stats.nak.Sub(p.start) }

// ResponseBody returns how long it took to receive the first bytes of the response body.
func (p *HTTPFetchPack) ResponseBody() time.Duration { return p.stats.responseBody.Sub(p.start) }

// Packets returns the number of Git packets received.
func (p *HTTPFetchPack) Packets() int { return p.stats.packets }

// LargestPacketSize returns the largest packet size received.
func (p *HTTPFetchPack) LargestPacketSize() int { return p.stats.largestPacketSize }

// RefsWanted returns the number of references sent to the remote repository as "want"s.
func (p *HTTPFetchPack) RefsWanted() int { return len(p.wantedRefs) }

// BandPackets returns how many packets were received on a specific sideband.
func (p *HTTPFetchPack) BandPackets(b string) int { return p.stats.multiband[b].packets }

// BandPayloadSize returns how many bytes were received on a specific sideband.
func (p *HTTPFetchPack) BandPayloadSize(b string) int64 { return p.stats.multiband[b].size }

// BandFirstPacket returns how long it took to receive the first packet on a specific sideband.
func (p *HTTPFetchPack) BandFirstPacket(b string) time.Duration {
	return p.stats.multiband[b].firstPacket.Sub(p.start)
}

// See https://github.com/git/git/blob/v2.25.0/Documentation/technical/http-protocol.txt#L351
// for background information.
func buildFetchPackRequest(
	ctx context.Context,
	url, user, password string,
	announcedRefs []Reference,
) (*http.Request, []string, error) {
	var wants []string
	for _, ref := range announcedRefs {
		if strings.HasPrefix(ref.Name, "refs/heads/") || strings.HasPrefix(ref.Name, "refs/tags/") {
			wants = append(wants, ref.Oid)
		}
	}

	reqBodyRaw := &bytes.Buffer{}
	reqBodyGzip := gzip.NewWriter(reqBodyRaw)
	for i, oid := range wants {
		if i == 0 {
			oid += " multi_ack_detailed no-done side-band-64k thin-pack ofs-delta deepen-since deepen-not agent=git/2.21.0"
		}
		if _, err := pktline.WriteString(reqBodyGzip, "want "+oid+"\n"); err != nil {
			return nil, nil, err
		}
	}
	if err := pktline.WriteFlush(reqBodyGzip); err != nil {
		return nil, nil, err
	}
	if _, err := pktline.WriteString(reqBodyGzip, "done\n"); err != nil {
		return nil, nil, err
	}
	if err := reqBodyGzip.Close(); err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequest("POST", url+"/git-upload-pack", reqBodyRaw)
	if err != nil {
		return nil, nil, err
	}

	req = req.WithContext(ctx)
	if user != "" {
		req.SetBasicAuth(user, password)
	}

	for k, v := range map[string]string{
		"User-Agent":       "gitaly-debug",
		"Content-Type":     "application/x-git-upload-pack-request",
		"Accept":           "application/x-git-upload-pack-result",
		"Content-Encoding": "gzip",
	} {
		req.Header.Set(k, v)
	}

	return req, wants, nil
}

func performFetchPack(
	ctx context.Context,
	url, user, password string,
	announcedRefs []Reference,
	reportProgress func(string, ...interface{}),
) (HTTPFetchPack, error) {
	var fetchPack HTTPFetchPack

	req, wants, err := buildFetchPackRequest(ctx, url, user, password, announcedRefs)
	if err != nil {
		return HTTPFetchPack{}, err
	}
	fetchPack.wantedRefs = wants

	fetchPack.start = time.Now()
	reportProgress("---\n")
	reportProgress("--- POST %v\n", req.URL)
	reportProgress("---\n")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return HTTPFetchPack{}, err
	}
	defer resp.Body.Close()

	if code := resp.StatusCode; code < 200 || code >= 400 {
		return HTTPFetchPack{}, fmt.Errorf("git http post: unexpected http status: %d", code)
	}

	fetchPack.responseHeader = time.Since(fetchPack.start)
	fetchPack.httpStatus = resp.StatusCode
	reportProgress("response code: %d\n", resp.StatusCode)
	reportProgress("response header: %v\n", resp.Header)

	fetchPack.stats.ReportProgress = func(b []byte) { reportProgress("%s", string(b)) }

	if err := fetchPack.stats.Parse(resp.Body); err != nil {
		return HTTPFetchPack{}, err
	}

	reportProgress("\n")

	return fetchPack, nil
}
