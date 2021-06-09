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

	wants []string // all branch and tag pointers
	Get   Get
	Post  Post
}

func (cl *Clone) RefsWanted() int { return len(cl.wants) }

// Perform does a Git HTTP clone, discarding cloned data to /dev/null.
func (cl *Clone) Perform(ctx context.Context) error {
	if err := cl.doGet(ctx); err != nil {
		return ctxErr(ctx, err)
	}

	if err := cl.doPost(ctx); err != nil {
		return ctxErr(ctx, err)
	}

	return nil
}

func ctxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

type Get struct {
	start          time.Time
	responseHeader time.Duration
	httpStatus     int
	stats          ReferenceDiscovery
}

func (g *Get) ResponseHeader() time.Duration { return g.responseHeader }
func (g *Get) HTTPStatus() int               { return g.httpStatus }
func (g *Get) FirstGitPacket() time.Duration { return g.stats.FirstPacket.Sub(g.start) }
func (g *Get) ResponseBody() time.Duration   { return g.stats.LastPacket.Sub(g.start) }

// Refs returns all announced references.
func (g *Get) Refs() []Reference { return g.stats.Refs }

// Packets returns the number of Git packets received.
func (g *Get) Packets() int { return g.stats.Packets }

// PayloadSize returns the total size of all pktlines' data.
func (g *Get) PayloadSize() int64 { return g.stats.PayloadSize }

// Caps returns all announced capabilities.
func (g *Get) Caps() []string { return g.stats.Caps }

func (cl *Clone) doGet(ctx context.Context) error {
	req, err := http.NewRequest("GET", cl.URL+"/info/refs?service=git-upload-pack", nil)
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	if cl.User != "" {
		req.SetBasicAuth(cl.User, cl.Password)
	}

	for k, v := range map[string]string{
		"User-Agent":      "gitaly-debug",
		"Accept":          "*/*",
		"Accept-Encoding": "deflate, gzip",
		"Pragma":          "no-cache",
	} {
		req.Header.Set(k, v)
	}

	cl.Get.start = time.Now()
	cl.printInteractive("---\n")
	cl.printInteractive("--- GET %v\n", req.URL)
	cl.printInteractive("---\n")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if code := resp.StatusCode; code < 200 || code >= 400 {
		return fmt.Errorf("git http get: unexpected http status: %d", code)
	}

	cl.Get.responseHeader = time.Since(cl.Get.start)
	cl.Get.httpStatus = resp.StatusCode
	cl.printInteractive("response code: %d\n", resp.StatusCode)
	cl.printInteractive("response header: %v\n", resp.Header)

	body := resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		body, err = gzip.NewReader(body)
		if err != nil {
			return err
		}
	}

	if err := cl.Get.stats.Parse(body); err != nil {
		return err
	}

	for _, ref := range cl.Get.Refs() {
		if strings.HasPrefix(ref.Name, "refs/heads/") || strings.HasPrefix(ref.Name, "refs/tags/") {
			cl.wants = append(cl.wants, ref.Oid)
		}
	}

	return nil
}

type Post struct {
	start          time.Time
	responseHeader time.Duration
	httpStatus     int
	stats          FetchPack
}

func (p *Post) ResponseHeader() time.Duration { return p.responseHeader }
func (p *Post) HTTPStatus() int               { return p.httpStatus }
func (p *Post) NAK() time.Duration            { return p.stats.nak.Sub(p.start) }
func (p *Post) ResponseBody() time.Duration   { return p.stats.responseBody.Sub(p.start) }
func (p *Post) Packets() int                  { return p.stats.packets }
func (p *Post) LargestPacketSize() int        { return p.stats.largestPacketSize }

func (p *Post) BandPackets(b string) int       { return p.stats.multiband[b].packets }
func (p *Post) BandPayloadSize(b string) int64 { return p.stats.multiband[b].size }
func (p *Post) BandFirstPacket(b string) time.Duration {
	return p.stats.multiband[b].firstPacket.Sub(p.start)
}

// See
// https://github.com/git/git/blob/v2.25.0/Documentation/technical/http-protocol.txt#L351
// for background information.
func (cl *Clone) buildPost(ctx context.Context) (*http.Request, error) {
	reqBodyRaw := &bytes.Buffer{}
	reqBodyGzip := gzip.NewWriter(reqBodyRaw)
	for i, oid := range cl.wants {
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

func (cl *Clone) doPost(ctx context.Context) error {
	req, err := cl.buildPost(ctx)
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
