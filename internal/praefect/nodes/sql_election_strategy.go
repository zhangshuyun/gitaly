package nodes

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
)

var failoverThresholdSeconds = 20

type sqlNode struct {
	node    Node
	primary bool
}

type SqlElectionStrategy struct {
	m           sync.RWMutex
	shardName   string
	nodes       []*sqlNode
	primaryNode *sqlNode
	db          *sql.DB
	log         *logrus.Entry
}

func newSqlElectionStrategy(name string, c config.Config, log *logrus.Entry) (*SqlElectionStrategy, error) {
	db, err := glsql.OpenDB(c.DB)
	if err != nil {
		return nil, err
	}

	return &SqlElectionStrategy{
		shardName: name,
		db:        db,
		log:       log,
	}, nil
}

// AddNode registers a primary or secondary in the internal
// datastore.
func (s *SqlElectionStrategy) AddNode(node Node, primary bool) {
	localNode := sqlNode{
		node:    node,
		primary: primary,
	}

	s.m.Lock()
	defer s.m.Unlock()

	if primary {
		s.primaryNode = &localNode
	}

	s.nodes = append(s.nodes, &localNode)
}

// Start launches a Goroutine to check the state of the nodes and
// continuously monitor their health via gRPC health checks.
func (s *SqlElectionStrategy) Start(bootstrapInterval, monitorInterval time.Duration) error {
	s.bootstrap(bootstrapInterval)
	go s.monitor(monitorInterval)

	return nil
}

func (s *SqlElectionStrategy) bootstrap(d time.Duration) error {
	s.CheckShard()

	return nil
}

func (s *SqlElectionStrategy) monitor(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		<-ticker.C
		s.CheckShard()
	}
}

// CheckShard issues a gRPC health check for each node managed by the
// shard.
func (s *SqlElectionStrategy) CheckShard() {
	defer s.updateMetrics()
	var wg sync.WaitGroup

	for _, n := range s.nodes {
		s.log.Debug("checking node " + n.node.GetStorage() + ": " + n.node.GetAddress())

		wg.Add(1)
		go func(n *sqlNode) {
			defer wg.Done()

			if n.node.check() {
				s.updateLeader(n.node.GetStorage())
			} else {
				s.log.Info("No response from " + n.node.GetStorage())
			}
		}(n)
	}

	wg.Wait()
	candidate, err := s.lookupPrimary()

	if err == nil && candidate != s.primaryNode {
		s.log.WithFields(logrus.Fields{
			"old_primary": s.primaryNode.node.GetStorage(),
			"new_primary": candidate.node.GetStorage(),
			"shard":       s.shardName}).Info("primary node changed")

		s.m.Lock()
		defer s.m.Unlock()

		if s.primaryNode != nil {
			s.primaryNode.primary = false
		}

		s.primaryNode = candidate
		candidate.primary = true
	}
}

// GetPrimary gets the primary of a shard. If no primary exists, it will
// be nil. If a primary has been elected but is down, err will be
// ErrPrimaryNotHealthy.
func (s *SqlElectionStrategy) GetPrimary() (Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.primaryNode == nil {
		return nil, ErrPrimaryNotHealthy
	}

	return s.primaryNode.node, nil
}

// GetSecondaries gets the secondaries of a shard
func (s *SqlElectionStrategy) GetSecondaries() ([]Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var secondaries []Node
	for _, n := range s.nodes {
		if !n.primary {
			secondaries = append(secondaries, n.node)
		}
	}

	return secondaries, nil
}

func (s *SqlElectionStrategy) updateMetrics() {
	s.m.RLock()
	defer s.m.RUnlock()

	for _, node := range s.nodes {
		val := float64(0)

		if node.primary {
			val = float64(1)
		}

		metrics.PrimaryGauge.WithLabelValues(s.shardName, node.node.GetStorage()).Set(val)
	}
}

func (s *SqlElectionStrategy) updateLeader(storageName string) error {
	q := `INSERT INTO shard_elections (is_primary, shard_name, node_name, last_seen_active)
	VALUES ('t', '%s', '%s', now()) ON CONFLICT (is_primary, shard_name)
	DO UPDATE SET
	node_name =
	  CASE WHEN (shard_elections.last_seen_active < now() - interval '%d seconds') THEN
  	    excluded.node_name
	  ELSE
	    shard_elections.node_name
	  END,
	last_seen_active =
	  CASE WHEN (shard_elections.last_seen_active < now() - interval '%d seconds') THEN
	    now()
	  ELSE
   		shard_elections.last_seen_active
	  END`

	_, err := s.db.Exec(fmt.Sprintf(q, s.shardName, storageName, failoverThresholdSeconds, failoverThresholdSeconds))

	if err != nil {
		s.log.Errorf("Error updating leader: %s", err)
	}
	return err
}

func (s *SqlElectionStrategy) lookupPrimary() (*sqlNode, error) {
	q := fmt.Sprintf(`SELECT node_name FROM shard_elections
	WHERE last_seen_active > now() - interval '%d seconds'
	AND is_primary IS TRUE
	AND shard_name = '%s'`, failoverThresholdSeconds, s.shardName)

	rows, err := s.db.Query(q)
	if err != nil {
		s.log.Errorf("Error looking up primary: %s", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		for _, n := range s.nodes {
			if n.node.GetStorage() == name {
				return n, nil
			}
		}
	}

	return nil, err
}
