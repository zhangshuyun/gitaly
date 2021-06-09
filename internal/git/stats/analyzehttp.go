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

type Clone struct {
	URL         string
	Interactive bool
	User        string
	Password    string

	// ReferenceDiscovery is the reference discovery performed as part of the clone.
	ReferenceDiscovery HTTPReferenceDiscovery
	Post               Post
}

// Perform does a Git HTTP clone, discarding cloned data to /dev/null.
func (cl *Clone) Perform(ctx context.Context) error {
	referenceDiscovery, err := performReferenceDiscovery(ctx, cl.URL, cl.User, cl.Password, cl.printInteractive)
	if err != nil {
		return ctxErr(ctx, err)
	}

	if err := cl.doPost(ctx, referenceDiscovery.Refs()); err != nil {
		return ctxErr(ctx, err)
	}

	cl.ReferenceDiscovery = referenceDiscovery

	return nil
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

type Post struct {
	start          time.Time
	responseHeader time.Duration
	httpStatus     int
	stats          FetchPack
	wantedRefs     []string
}

func (p *Post) ResponseHeader() time.Duration { return p.responseHeader }
func (p *Post) HTTPStatus() int               { return p.httpStatus }
func (p *Post) NAK() time.Duration            { return p.stats.nak.Sub(p.start) }
func (p *Post) ResponseBody() time.Duration   { return p.stats.responseBody.Sub(p.start) }
func (p *Post) Packets() int                  { return p.stats.packets }
func (p *Post) LargestPacketSize() int        { return p.stats.largestPacketSize }

// RefsWanted returns the number of references sent to the remote repository as "want"s.
func (p *Post) RefsWanted() int { return len(p.wantedRefs) }

func (p *Post) BandPackets(b string) int       { return p.stats.multiband[b].packets }
func (p *Post) BandPayloadSize(b string) int64 { return p.stats.multiband[b].size }
func (p *Post) BandFirstPacket(b string) time.Duration {
	return p.stats.multiband[b].firstPacket.Sub(p.start)
}

// See
// https://github.com/git/git/blob/v2.25.0/Documentation/technical/http-protocol.txt#L351
// for background information.
func (cl *Clone) buildPost(ctx context.Context, announcedRefs []Reference) (*http.Request, error) {
	for _, ref := range announcedRefs {
		if strings.HasPrefix(ref.Name, "refs/heads/") || strings.HasPrefix(ref.Name, "refs/tags/") {
			cl.Post.wantedRefs = append(cl.Post.wantedRefs, ref.Oid)
		}
	}

	reqBodyRaw := &bytes.Buffer{}
	reqBodyGzip := gzip.NewWriter(reqBodyRaw)
	for i, oid := range cl.Post.wantedRefs {
		if i == 0 {
			oid += " multi_ack_detailed no-done side-band-64k thin-pack ofs-delta deepen-since deepen-not agent=git/2.21.0"
		}
		if _, err := pktline.WriteString(reqBodyGzip, "want "+oid+"\n"); err != nil {
			return nil, err
		}
	}
	if err := pktline.WriteFlush(reqBodyGzip); err != nil {
		return nil, err
	}
	if _, err := pktline.WriteString(reqBodyGzip, "done\n"); err != nil {
		return nil, err
	}
	if err := reqBodyGzip.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", cl.URL+"/git-upload-pack", reqBodyRaw)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	if cl.User != "" {
		req.SetBasicAuth(cl.User, cl.Password)
	}

	for k, v := range map[string]string{
		"User-Agent":       "gitaly-debug",
		"Content-Type":     "application/x-git-upload-pack-request",
		"Accept":           "application/x-git-upload-pack-result",
		"Content-Encoding": "gzip",
	} {
		req.Header.Set(k, v)
	}

	return req, nil
}

func (cl *Clone) doPost(ctx context.Context, announcedRefs []Reference) error {
	req, err := cl.buildPost(ctx, announcedRefs)
	if err != nil {
		return err
	}

	cl.Post.start = time.Now()
	cl.printInteractive("---\n")
	cl.printInteractive("--- POST %v\n", req.URL)
	cl.printInteractive("---\n")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if code := resp.StatusCode; code < 200 || code >= 400 {
		return fmt.Errorf("git http post: unexpected http status: %d", code)
	}

	cl.Post.responseHeader = time.Since(cl.Post.start)
	cl.Post.httpStatus = resp.StatusCode
	cl.printInteractive("response code: %d\n", resp.StatusCode)
	cl.printInteractive("response header: %v\n", resp.Header)

	cl.Post.stats.ReportProgress = func(b []byte) { cl.printInteractive("%s", string(b)) }

	if err := cl.Post.stats.Parse(resp.Body); err != nil {
		return err
	}

	cl.printInteractive("\n")

	return nil
}

func (cl *Clone) printInteractive(format string, a ...interface{}) {
	if !cl.Interactive {
		return
	}

	// Ignore any errors returned by this given that we only use it as a debugging aid
	// to write to stdout.
	fmt.Printf(format, a...)
}
