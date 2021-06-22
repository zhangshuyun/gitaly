package stats

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

// ResponseBody returns how long it took to receive the last bytes of the response body.
func (p *HTTPSendPack) ResponseBody() time.Duration { return p.stats.responseBody.Sub(p.start) }

// BandPayloadSize returns how many bytes were received on a specific sideband.
func (p *HTTPSendPack) BandPayloadSize(b string) int64 { return p.stats.multiband[b].size }

// BandFirstPacket returns how long it took to receive the first packet on a specific sideband.
func (p *HTTPSendPack) BandFirstPacket(b string) time.Duration {
	return p.stats.multiband[b].firstPacket.Sub(p.start)
}

// HTTPSendPack encapsulates statistics about an emulated git-send-pack(1) request.
type HTTPSendPack struct {
	start  time.Time
	header time.Time
	stats  SendPack
}

func buildSendPackRequest(
	ctx context.Context,
	url, user, password string,
	commands []PushCommand,
	packfile io.Reader,
) (*http.Request, error) {
	var requestBuffer bytes.Buffer
	zipper := gzip.NewWriter(&requestBuffer)

	for i, command := range commands {
		c := fmt.Sprintf("%s %s %s", command.OldOID, command.NewOID, command.Reference)
		if i == 0 {
			c += "\x00side-band-64k report-status delete-refs"
		}

		if _, err := pktline.WriteString(zipper, c); err != nil {
			return nil, fmt.Errorf("writing command: %w", err)
		}
	}

	if err := pktline.WriteFlush(zipper); err != nil {
		return nil, fmt.Errorf("terminating command list: %w", err)
	}

	if packfile != nil {
		if _, err := io.Copy(zipper, packfile); err != nil {
			return nil, fmt.Errorf("sending packfile: %w", err)
		}
	}

	if err := zipper.Close(); err != nil {
		return nil, fmt.Errorf("finalizing request body: %w", err)
	}

	request, err := http.NewRequest("POST", url+"/git-receive-pack", &requestBuffer)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	request = request.WithContext(ctx)

	if user != "" {
		request.SetBasicAuth(user, password)
	}

	for k, v := range map[string]string{
		"User-Agent":       "gitaly-debug",
		"Content-Type":     "application/x-git-receive-pack-request",
		"Accept":           "application/x-git-receive-pack-result",
		"Content-Encoding": "gzip",
	} {
		request.Header.Set(k, v)
	}

	return request, nil
}

func performHTTPSendPack(
	ctx context.Context,
	url, user, password string,
	commands []PushCommand,
	packfile io.Reader,
	reportProgress func(string, ...interface{}),
) (HTTPSendPack, error) {
	request, err := buildSendPackRequest(ctx, url, user, password, commands, packfile)
	if err != nil {
		return HTTPSendPack{}, err
	}

	reportProgress("---\n")
	reportProgress("--- git-send-pack %v\n", request.URL)
	reportProgress("---\n")

	sendPack := HTTPSendPack{
		start: time.Now(),
		stats: SendPack{
			ReportProgress: func(b []byte) { reportProgress("%s", string(b)) },
		},
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return HTTPSendPack{}, fmt.Errorf("creating send-pack request: %w", err)
	}
	defer response.Body.Close()

	if code := response.StatusCode; code < 200 || code >= 400 {
		return HTTPSendPack{}, fmt.Errorf("unexpected HTTP status: %d", code)
	}

	sendPack.header = time.Now()
	reportProgress("response code: %d\n", response.StatusCode)
	reportProgress("response header: %v\n", response.Header)

	body := response.Body
	if response.Header.Get("Content-Encoding") == "gzip" {
		body, err = gzip.NewReader(body)
		if err != nil {
			return HTTPSendPack{}, fmt.Errorf("setting up gzip reader: %w", err)
		}
	}

	if err := sendPack.stats.Parse(body); err != nil {
		return HTTPSendPack{}, fmt.Errorf("parsing packfile response: %w", err)
	}

	return sendPack, nil
}
