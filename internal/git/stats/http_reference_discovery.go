package stats

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

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

func performHTTPReferenceDiscovery(
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
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
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
