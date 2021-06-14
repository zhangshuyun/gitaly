package stats

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

// Clone hosts information about a typical HTTP-based clone.
type Clone struct {
	// ReferenceDiscovery is the reference discovery performed as part of the clone.
	ReferenceDiscovery HTTPReferenceDiscovery
	// FetchPack is the response to a git-fetch-pack(1) request which computes transmits the
	// packfile.
	FetchPack HTTPFetchPack
}

// PerformClone does a Git HTTP clone, discarding cloned data to /dev/null.
func PerformClone(ctx context.Context, url, user, password string, interactive bool) (Clone, error) {
	printInteractive := func(format string, a ...interface{}) {
		if interactive {
			// Ignore any errors returned by this given that we only use it as a
			// debugging aid to write to stdout.
			fmt.Printf(format, a...)
		}
	}

	referenceDiscovery, err := performReferenceDiscovery(ctx, url, user, password, printInteractive)
	if err != nil {
		return Clone{}, ctxErr(ctx, err)
	}

	fetchPack, err := performFetchPack(ctx, url, user, password,
		referenceDiscovery.Refs(), printInteractive)
	if err != nil {
		return Clone{}, ctxErr(ctx, err)
	}

	return Clone{
		ReferenceDiscovery: referenceDiscovery,
		FetchPack:          fetchPack,
	}, nil
}

func ctxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// HTTPReferenceDiscovery is a ReferenceDiscovery obtained via a clone of a target repository via
// HTTP. It contains additional information about the cloning process like status codes and
// timings.
type HTTPReferenceDiscovery struct {
	start          time.Time
	responseHeader time.Duration
	httpStatus     int
	stats          ReferenceDiscovery
}

// ResponseHeader returns how long it took to receive the response header.
func (d HTTPReferenceDiscovery) ResponseHeader() time.Duration { return d.responseHeader }

// HTTPStatus returns the HTTP status code.
func (d HTTPReferenceDiscovery) HTTPStatus() int { return d.httpStatus }

// FirstGitPacket returns how long it took to receive the first Git packet.
func (d HTTPReferenceDiscovery) FirstGitPacket() time.Duration {
	return d.stats.FirstPacket.Sub(d.start)
}

// ResponseBody returns how long it took to receive the first bytes of the response body.
func (d HTTPReferenceDiscovery) ResponseBody() time.Duration {
	return d.stats.LastPacket.Sub(d.start)
}

// Refs returns all announced references.
func (d HTTPReferenceDiscovery) Refs() []Reference { return d.stats.Refs }

// Packets returns the number of Git packets received.
func (d HTTPReferenceDiscovery) Packets() int { return d.stats.Packets }

// PayloadSize returns the total size of all pktlines' data.
func (d HTTPReferenceDiscovery) PayloadSize() int64 { return d.stats.PayloadSize }

// Caps returns all announced capabilities.
func (d HTTPReferenceDiscovery) Caps() []string { return d.stats.Caps }

func performReferenceDiscovery(
	ctx context.Context,
	url, user, password string,
	reportProgress func(string, ...interface{}),
) (HTTPReferenceDiscovery, error) {
	var referenceDiscovery HTTPReferenceDiscovery

	req, err := http.NewRequest("GET", url+"/info/refs?service=git-upload-pack", nil)
	if err != nil {
		return HTTPReferenceDiscovery{}, err
	}

	req = req.WithContext(ctx)
	if user != "" {
		req.SetBasicAuth(user, password)
	}

	for k, v := range map[string]string{
		"User-Agent":      "gitaly-debug",
		"Accept":          "*/*",
		"Accept-Encoding": "deflate, gzip",
		"Pragma":          "no-cache",
	} {
		req.Header.Set(k, v)
	}

	referenceDiscovery.start = time.Now()
	reportProgress("---\n")
	reportProgress("--- GET %v\n", req.URL)
	reportProgress("---\n")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return HTTPReferenceDiscovery{}, err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if code := resp.StatusCode; code < 200 || code >= 400 {
		return HTTPReferenceDiscovery{}, fmt.Errorf("git http get: unexpected http status: %d", code)
	}

	referenceDiscovery.responseHeader = time.Since(referenceDiscovery.start)
	referenceDiscovery.httpStatus = resp.StatusCode
	reportProgress("response code: %d\n", resp.StatusCode)
	reportProgress("response header: %v\n", resp.Header)

	body := resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		body, err = gzip.NewReader(body)
		if err != nil {
			return HTTPReferenceDiscovery{}, err
		}
	}

	if err := referenceDiscovery.stats.Parse(body); err != nil {
		return HTTPReferenceDiscovery{}, err
	}

	return referenceDiscovery, nil
}

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
