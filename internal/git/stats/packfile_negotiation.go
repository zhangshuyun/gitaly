package stats

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
)

type PackfileNegotiation struct {
	// Total size of all pktlines' data
	PayloadSize int64
	// Total number of packets
	Packets int
	// Capabilities announced by the client
	Caps []string
	// Wants is the number of objects the client announced it wants
	Wants int
	// Haves is the number of objects the client announced it has
	Haves int
	// Shallows is the number of shallow boundaries announced by the client
	Shallows int
	// Deepen-filter. One of "deepen <depth>", "deepen-since <timestamp>", "deepen-not <ref>".
	Deepen string
	// Filter-spec specified by the client.
	Filter string
}

func ParsePackfileNegotiation(body io.Reader) (PackfileNegotiation, error) {
	n := PackfileNegotiation{}
	return n, n.Parse(body)
}

// Parse parses a packfile negotiation. It expects the following format:
//
// want <OID> <capabilities\n
// [want <OID>...]
// [shallow <OID>]
// [deepen <depth>|deepen-since <timestamp>|deepen-not <ref>]
// [filter <filter-spec>]
// flush
// have <OID>
// flush|done
func (n *PackfileNegotiation) Parse(body io.Reader) error {
	defer io.Copy(ioutil.Discard, body)

	scanner := pktline.NewScanner(body)

	for ; scanner.Scan(); n.Packets++ {
		pkt := scanner.Bytes()
		data := text.ChompBytes(pktline.Data(pkt))
		split := strings.Split(data, " ")
		n.PayloadSize += int64(len(data))

		done := false

		switch split[0] {
		case "want":
			if len(split) < 2 {
				return fmt.Errorf("invalid 'want' for packet %d: %v", n.Packets, data)
			}
			if len(split) > 2 && n.Caps != nil {
				return fmt.Errorf("capabilities announced multiple times in packet %d: %v", n.Packets, data)
			}

			n.Wants++
			if len(split) > 2 {
				n.Caps = split[2:]
			}
		case "shallow":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'shallow' for packet %d: %v", n.Packets, data)
			}
			n.Shallows++
		case "deepen", "deepen-since", "deepen-not":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'deepen' for packet %d: %v", n.Packets, data)
			}
			n.Deepen = data
		case "filter":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'filter' for packet %d: %v", n.Packets, data)
			}
			n.Filter = split[1]
		case "have":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'have' for packet %d: %v", n.Packets, data)
			}
			n.Haves++
		case "done":
			done = true
		}

		if done {
			break
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}
	if n.Wants == 0 {
		return errors.New("no 'want' sent by client")
	}

	return nil
}

// UpdateMetrics updates Prometheus counters with features that have been used
// during a packfile negotiation.
func (n *PackfileNegotiation) UpdateMetrics(metrics *prometheus.CounterVec) {
	if n.Deepen != "" {
		metrics.WithLabelValues("deepen").Inc()
	}
	if n.Filter != "" {
		metrics.WithLabelValues("filter").Inc()
	}
	if n.Haves > 0 {
		metrics.WithLabelValues("have").Inc()
	}
	metrics.WithLabelValues("total").Inc()
}
