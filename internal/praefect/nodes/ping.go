package nodes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type (
	virtualStorage string
	gitalyStorage  string
)

func newPingSet(conf config.Config, printer Printer, quiet bool) map[string]*Ping {
	nodeByAddress := map[string]*Ping{} // key is address

	// flatten nodes between virtual storages
	for _, vs := range conf.VirtualStorages {
		vsName := virtualStorage(vs.Name)
		for _, node := range vs.Nodes {
			gsName := gitalyStorage(node.Storage)

			n, ok := nodeByAddress[node.Address]
			if !ok {
				n = &Ping{
					storages:  map[gitalyStorage][]virtualStorage{},
					vStorages: map[virtualStorage]struct{}{},
					printer:   printer,
					quiet:     quiet,
				}
			}
			n.address = node.Address

			s := n.storages[gsName]
			n.storages[gsName] = append(s, vsName)

			n.vStorages[vsName] = struct{}{}
			n.token = node.Token
			nodeByAddress[node.Address] = n
		}
	}
	return nodeByAddress
}

// Ping is used to determine node health for a gitaly node
type Ping struct {
	address string
	// set of storages this node hosts
	storages  map[gitalyStorage][]virtualStorage
	vStorages map[virtualStorage]struct{} // set of virtual storages node belongs to
	token     string                      // auth token
	err       error                       // any error during dial/ping
	printer   Printer
	quiet     bool
}

// Address returns the address of the node
func (p *Ping) Address() string {
	return p.address
}

func (p *Ping) dial(ctx context.Context) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}

	if len(p.token) > 0 {
		opts = append(opts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(p.token)))
	}

	return client.DialContext(ctx, p.address, opts)
}

func (p *Ping) healthCheck(ctx context.Context, cc *grpc.ClientConn) (grpc_health_v1.HealthCheckResponse_ServingStatus, error) {
	hClient := grpc_health_v1.NewHealthClient(cc)

	resp, err := hClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return 0, err
	}

	return resp.GetStatus(), nil
}

func (p *Ping) isConsistent(ctx context.Context, cc *grpc.ClientConn) bool {
	praefect := gitalypb.NewServerServiceClient(cc)

	if len(p.storages) == 0 {
		p.log("ERROR: current configuration has no storages")
		return false
	}

	resp, err := praefect.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	if err != nil {
		p.log("ERROR: failed to receive state from the remote: %v", err)
		return false
	}

	if len(resp.StorageStatuses) == 0 {
		p.log("ERROR: remote has no configured storages")
		return false
	}

	storagesSet := make(map[gitalyStorage]bool, len(resp.StorageStatuses))

	knownStoragesSet := make(map[gitalyStorage]bool, len(p.storages))
	for k := range p.storages {
		knownStoragesSet[k] = true
	}

	consistent := true
	for _, status := range resp.StorageStatuses {
		gStorage := gitalyStorage(status.StorageName)

		// only proceed if the gitaly storage belongs to a configured
		// virtual storage
		if len(p.storages[gStorage]) == 0 {
			continue
		}

		if storagesSet[gStorage] {
			p.log("ERROR: remote has duplicated storage: %q", status.StorageName)
			consistent = false
			continue
		}
		storagesSet[gStorage] = true

		if status.Readable && status.Writeable {
			p.log(
				"SUCCESS: confirmed Gitaly storage %q in virtual storages %v is served",
				status.StorageName,
				p.storages[gStorage],
			)
			delete(knownStoragesSet, gStorage) // storage found
		} else {
			p.log("ERROR: storage %q is not readable or writable", status.StorageName)
			consistent = false
		}
	}

	for storage := range knownStoragesSet {
		p.log("ERROR: configured storage was not reported by remote: %q", storage)
		consistent = false
	}

	return consistent
}

func (p *Ping) log(msg string, args ...interface{}) {
	if p.quiet {
		return
	}

	p.printer.Printf("[%s]: %s", p.address, fmt.Sprintf(msg, args...))
}

// Printer is an interface for Ping to print messages
type Printer interface {
	// Printf prints a message, taking into account whether
	// or not the verbose flag has been set
	Printf(format string, args ...interface{})
}

// TextPrinter is a basic printer that writes to a writer
type TextPrinter struct {
	w io.Writer
}

// NewTextPrinter creates a new TextPrinter instance
func NewTextPrinter(w io.Writer) *TextPrinter {
	return &TextPrinter{w: w}
}

// Printf prints the message and adds a newline
func (t *TextPrinter) Printf(format string, args ...interface{}) {
	fmt.Fprintf(t.w, format, args...)
	fmt.Fprint(t.w, "\n")
}

// CheckNode checks network connectivity by issuing a healthcheck request, and
//  also calls the ServerInfo RPC to check disk read/write access.
func (p *Ping) CheckNode(ctx context.Context) {
	p.log("dialing...")
	cc, err := p.dial(ctx)
	if err != nil {
		p.log("ERROR: dialing failed: %v", err)
		p.err = err
		return
	}
	defer cc.Close()
	p.log("dialed successfully!")

	p.log("checking health...")
	health, err := p.healthCheck(ctx, cc)
	if err != nil {
		p.log("ERROR: unable to request health check: %v", err)
		p.err = err
		return
	}

	if health != grpc_health_v1.HealthCheckResponse_SERVING {
		p.err = fmt.Errorf(
			"health check did not report serving, instead reported: %s",
			health.String())
		p.log("ERROR: %v", p.err)
		return
	}

	p.log("SUCCESS: node is healthy!")

	p.log("checking consistency...")
	if !p.isConsistent(ctx, cc) {
		p.err = errors.New("consistency check failed")
		p.log("ERROR: %v", p.err)
		return
	}
	p.log("SUCCESS: node configuration is consistent!")
}

func (p *Ping) Error() error {
	return p.err
}

// PingAll loops through all the pings and calls CheckNode on them. Returns a PingError in case
// pinging a subset of nodes failed.
func PingAll(ctx context.Context, cfg config.Config, printer Printer, quiet bool) error {
	pings := newPingSet(cfg, printer, quiet)

	var wg sync.WaitGroup
	for _, n := range pings {
		wg.Add(1)
		go func(n *Ping) {
			defer wg.Done()
			n.CheckNode(ctx)
		}(n)
	}
	wg.Wait()

	var unhealthyAddresses []string
	for _, n := range pings {
		if n.Error() != nil {
			unhealthyAddresses = append(unhealthyAddresses, n.address)
		}
	}

	if len(unhealthyAddresses) > 0 {
		return &PingError{unhealthyAddresses}
	}

	return nil
}

// PingError is an error returned in case pinging a node failed.
type PingError struct {
	// UnhealthyAddresses contains all addresses which
	UnhealthyAddresses []string
}

// Error returns a composite error message based on which nodes were deemed unhealthy
func (n *PingError) Error() string {
	return fmt.Sprintf("the following nodes are not healthy: %s", strings.Join(n.UnhealthyAddresses, ", "))
}
