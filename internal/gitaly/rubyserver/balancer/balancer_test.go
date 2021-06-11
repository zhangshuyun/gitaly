package balancer

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
)

func TestServiceConfig(t *testing.T) {
	configureBuilderTest(3)

	tcc := &testClientConn{}
	_, err := lbBuilder.Build(resolver.Target{}, tcc, resolver.BuildOptions{})
	require.NoError(t, err)

	configUpdates := tcc.ConfigUpdates()
	require.Len(t, configUpdates, 1, "expect exactly one config update")

	svcConfig := struct{ LoadBalancingPolicy string }{}
	require.NoError(t, json.NewDecoder(strings.NewReader(configUpdates[0])).Decode(&svcConfig))
	require.Equal(t, "round_robin", svcConfig.LoadBalancingPolicy)
}

func TestAddressUpdatesSmallestPool(t *testing.T) {
	// The smallest number of addresses is 2: 1 standby, and 1 active.
	addrs := configureBuilderTest(2)

	tcc := &testClientConn{}
	_, err := lbBuilder.Build(resolver.Target{}, tcc, resolver.BuildOptions{})
	require.NoError(t, err)

	// Simulate some random updates
	RemoveAddress(addrs[0])
	RemoveAddress(addrs[0])
	AddAddress(addrs[0])
	RemoveAddress(addrs[1])
	RemoveAddress(addrs[0])
	AddAddress(addrs[1])
	AddAddress(addrs[1])
	RemoveAddress(addrs[1])
	RemoveAddress(addrs[1])
	RemoveAddress(addrs[1])
	RemoveAddress(addrs[0])
	AddAddress(addrs[0])

	addrUpdates := tcc.AddrUpdates()
	require.True(t, len(addrUpdates) > 0, "expected at least one address update")

	expectedActive := len(addrs) - 1 // subtract 1 for the standby
	for _, update := range addrUpdates {
		require.Equal(t, expectedActive, len(update))
	}
}

func TestAddressUpdatesRoundRobinPool(t *testing.T) {
	// With 3 addresses in the pool, 2 will be active.
	addrs := configureBuilderTest(3)

	tcc := &testClientConn{}
	_, err := lbBuilder.Build(resolver.Target{}, tcc, resolver.BuildOptions{})
	require.NoError(t, err)

	// Simulate some random updates
	RemoveAddress(addrs[0])
	RemoveAddress(addrs[0])
	RemoveAddress(addrs[2])
	AddAddress(addrs[0])
	RemoveAddress(addrs[1])
	RemoveAddress(addrs[0])
	AddAddress(addrs[2])
	AddAddress(addrs[1])
	AddAddress(addrs[1])
	RemoveAddress(addrs[1])
	RemoveAddress(addrs[2])
	RemoveAddress(addrs[1])
	AddAddress(addrs[1])
	RemoveAddress(addrs[2])
	RemoveAddress(addrs[1])
	RemoveAddress(addrs[0])
	AddAddress(addrs[0])

	addrUpdates := tcc.AddrUpdates()
	require.True(t, len(addrUpdates) > 0, "expected at least one address update")

	expectedActive := len(addrs) - 1 // subtract 1 for the standby
	for _, update := range addrUpdates {
		require.Equal(t, expectedActive, len(update))
	}
}

func TestRemovals(t *testing.T) {
	okActions := []action{
		{add: "foo"},
		{add: "bar"},
		{add: "qux"},
		{remove: "bar"},
		{add: "baz"},
		{remove: "foo"},
	}
	numAddr := 3
	removeDelay := 1 * time.Millisecond
	now := time.Now()
	ConfigureBuilder(numAddr, removeDelay, func() time.Time { return now })

	testCases := []struct {
		desc      string
		actions   []action
		lastFails bool
		delay     time.Duration
	}{
		{
			desc:    "add then remove",
			actions: okActions,
			delay:   2 * removeDelay,
		},
		{
			desc:      "add then remove but too fast",
			actions:   okActions,
			lastFails: true,
			delay:     0,
		},
		{
			desc:      "remove one address too many",
			actions:   append(okActions, action{remove: "qux"}),
			lastFails: true,
			delay:     2 * removeDelay,
		},
		{
			desc: "remove unknown address",
			actions: []action{
				{add: "foo"},
				{add: "qux"},
				{add: "baz"},
				{remove: "bar"},
			},
			lastFails: true,
			delay:     2 * removeDelay,
		},
		{
			// This relies on the implementation detail that the first address added
			// to the balancer is the standby. The standby cannot be removed.
			desc: "remove standby address",
			actions: []action{
				{add: "foo"},
				{add: "qux"},
				{add: "baz"},
				{remove: "foo"},
			},
			lastFails: true,
			delay:     2 * removeDelay,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			lbBuilder.testingTriggerRestart <- struct{}{}

			for i, a := range tc.actions {
				if a.add != "" {
					AddAddress(a.add)
				} else {
					now = now.Add(tc.delay)

					expected := true
					if i+1 == len(tc.actions) && tc.lastFails {
						expected = false
					}

					require.Equal(t, expected, RemoveAddress(a.remove), "expected result from removing %q", a.remove)
				}
			}
		})
	}
}

type action struct {
	add    string
	remove string
}

type testClientConn struct {
	resolver.ClientConn

	addrUpdates   [][]resolver.Address
	configUpdates []string
	mu            sync.Mutex
}

func (tcc *testClientConn) NewAddress(addresses []resolver.Address) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()

	tcc.addrUpdates = append(tcc.addrUpdates, addresses)
}

func (tcc *testClientConn) NewServiceConfig(serviceConfig string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()

	tcc.configUpdates = append(tcc.configUpdates, serviceConfig)
}

func (tcc *testClientConn) AddrUpdates() [][]resolver.Address {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()

	return tcc.addrUpdates
}

func (tcc *testClientConn) ConfigUpdates() []string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()

	return tcc.configUpdates
}

func (tcc *testClientConn) UpdateState(state resolver.State) error {
	return nil
}

// configureBuilderTest reconfigures the global builder and pre-populates
// it with addresses. It returns the list of addresses it added.
func configureBuilderTest(numAddrs int) []string {
	delay := 1 * time.Millisecond
	ConfigureBuilder(numAddrs, delay, time.Now)
	lbBuilder.testingTriggerRestart <- struct{}{}

	var addrs []string
	for i := 0; i < numAddrs; i++ {
		a := fmt.Sprintf("test.%d", i)
		AddAddress(a)
		addrs = append(addrs, a)
	}

	return addrs
}
