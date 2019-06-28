// Package praefect provides data models and datastore persistence abstractions
// for tracking the state of repository replicas.
//
// See original design discussion:
// https://gitlab.com/gitlab-org/gitaly/issues/1495
package praefect

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

var (
	// ErrPrimaryNotSet indicates the primary has not been set in the datastore
	ErrPrimaryNotSet = errors.New("primary is not set")
)

// JobState is an enum that indicates the state of a job
type JobState uint8

const (
	// JobStatePending is the initial job state when it is not yet ready to run
	// and may indicate recovery from a failure prior to the ready-state
	JobStatePending JobState = 1 << iota
	// JobStateReady indicates the job is now ready to proceed
	JobStateReady
	// JobStateInProgress indicates the job is being processed by a worker
	JobStateInProgress
	// JobStateComplete indicates the job is now complete
	JobStateComplete
	// JobStateCancelled indicates the job was cancelled. This can occur if the
	// job is no longer relevant (e.g. a node is moved out of a shard)
	JobStateCancelled
)

// ReplJob is an instance of a queued replication job. A replication job is
// meant for updating the repository so that it is synced with the primary
// copy. Scheduled indicates when a replication job should be performed.
type ReplJob struct {
	ID     uint64     // autoincrement ID
	Target string     // which storage location to replicate to?
	Source Repository // source for replication
	State  JobState
}

// replJobs provides sort manipulation behavior
type replJobs []ReplJob

func (rjs replJobs) Len() int      { return len(rjs) }
func (rjs replJobs) Swap(i, j int) { rjs[i], rjs[j] = rjs[j], rjs[i] }

// byJobID provides a comparator for sorting jobs
type byJobID struct{ replJobs }

func (b byJobID) Less(i, j int) bool { return b.replJobs[i].ID < b.replJobs[j].ID }

// Datastore is a data persistence abstraction for all of Praefect's
// persistence needs
type Datastore interface {
	ReplJobsDatastore
	ReplicasDatastore
	PrimaryDatastore
}

// PrimaryDatastore manages accessing and setting the primary storage location
type PrimaryDatastore interface {
	// GetPrimary gets the primary storage location
	GetPrimary() (config.GitalyServer, error)
	// SetPrimary sets the primary storage location
	SetPrimary(server config.GitalyServer) error
}

// ReplicasDatastore manages accessing and setting which secondary replicas
// backup a repository
type ReplicasDatastore interface {
	// GetSecondaries will retrieve all secondary replica storage locations for
	// a primary replica
	GetSecondaries(primary Repository) ([]string, error)

	// SetSecondaries will set the secondary storage locations for a repository
	// in a primary replica.
	SetSecondaries(primary Repository, secondaries []string) error
}

// ReplJobsDatastore represents the behavior needed for fetching and updating
// replication jobs from the datastore
type ReplJobsDatastore interface {
	// GetJobs fetches a list of chronologically ordered replication
	// jobs for the given storage replica. The returned list will be at most
	// count-length.
	GetJobs(flag JobState, storage string, count int) ([]ReplJob, error)

	// CreateSecondaryJobs will create replication jobs for each secondary
	// replica of a repository known to the datastore. A set of replication job
	// ID's for the created jobs will be returned upon success.
	CreateSecondaryReplJobs(source Repository) ([]uint64, error)

	// UpdateReplJob updates the state of an existing replication job
	UpdateReplJob(jobID uint64, newState JobState) error
}

// shard is a set of primary and secondary storage replicas for a project
type shard struct {
	primary     string
	secondaries []string
}

type jobRecord struct {
	relativePath string // project's relative path
	target       string
	state        JobState
}

// MemoryDatastore is a simple datastore that isn't persisted to disk. It is
// only intended for early beta requirements and as a reference implementation
// for the eventual SQL implementation
type MemoryDatastore struct {
	replicas *struct {
		sync.RWMutex
		m map[string]shard // keyed by project's relative path
	}

	jobs *struct {
		sync.RWMutex
		next    uint64
		records map[uint64]jobRecord // all jobs indexed by ID
	}

	primary *struct {
		sync.RWMutex
		server config.GitalyServer
	}
}

// NewMemoryDatastore returns an initialized in-memory datastore
func NewMemoryDatastore(cfg config.Config) *MemoryDatastore {
	m := &MemoryDatastore{
		replicas: &struct {
			sync.RWMutex
			m map[string]shard
		}{
			m: map[string]shard{},
		},
		jobs: &struct {
			sync.RWMutex
			next    uint64
			records map[uint64]jobRecord // all jobs indexed by ID
		}{
			next:    0,
			records: map[uint64]jobRecord{},
		},
		primary: &struct {
			sync.RWMutex
			server config.GitalyServer
		}{
			server: config.GitalyServer{
				Name:       cfg.PrimaryServer.Name,
				ListenAddr: cfg.PrimaryServer.ListenAddr,
			},
		},
	}

	secondaries := make([]string, len(cfg.SecondaryServers))
	for i, server := range cfg.SecondaryServers {
		secondaries[i] = server.Name
	}

	for _, relativePath := range cfg.Whitelist {
		// store the configuration file specified shard
		m.replicas.m[relativePath] = shard{
			primary:     cfg.PrimaryServer.Name,
			secondaries: secondaries,
		}

		// initialize replication job queue to replicate all whitelisted repos
		// to every secondary server
		for _, secondary := range cfg.SecondaryServers {
			m.jobs.next++
			m.jobs.records[m.jobs.next] = jobRecord{
				state:        JobStateReady,
				target:       secondary.Name,
				relativePath: relativePath,
			}
		}

	}

	return m
}

// GetSecondaries will return the set of secondary storage locations for a
// given repository if they exist
func (md *MemoryDatastore) GetSecondaries(primary Repository) ([]string, error) {
	shard, _ := md.getShard(primary.RelativePath)

	return shard.secondaries, nil
}

// SetSecondaries will replace the set of replicas for a repository
func (md *MemoryDatastore) SetSecondaries(primary Repository, secondaries []string) error {
	md.replicas.Lock()
	md.replicas.m[primary.RelativePath] = shard{
		primary:     primary.Storage,
		secondaries: secondaries,
	}
	md.replicas.Unlock()

	return nil
}

func (md *MemoryDatastore) getShard(project string) (shard, bool) {
	md.replicas.RLock()
	replicas, ok := md.replicas.m[project]
	md.replicas.RUnlock()

	return replicas, ok
}

// ErrSecondariesMissing indicates the repository does not have any backup
// replicas
var ErrSecondariesMissing = errors.New("repository missing secondary replicas")

// GetJobs is a more general method to retrieve jobs of a certain state from the datastore
func (md *MemoryDatastore) GetJobs(state JobState, storage string, count int) ([]ReplJob, error) {
	md.jobs.RLock()
	defer md.jobs.RUnlock()

	var results []ReplJob

	for i, record := range md.jobs.records {
		// state is a bitmap that is a combination of one or more JobStates
		if record.state&state != 0 && record.target == storage {
			job, err := md.replJobFromRecord(i, record)
			if err != nil {
				return nil, err
			}

			results = append(results, job)
			if len(results) >= count {
				break
			}
		}
	}

	sort.Sort(byJobID{results})

	return results, nil
}

// replJobFromRecord constructs a replication job from a record and by cross
// referencing the current shard for the project being replicated
func (md *MemoryDatastore) replJobFromRecord(jobID uint64, record jobRecord) (ReplJob, error) {
	shard, ok := md.getShard(record.relativePath)
	if !ok {
		return ReplJob{}, fmt.Errorf(
			"unable to find shard for project at relative path %q",
			record.relativePath,
		)
	}

	return ReplJob{
		ID: jobID,
		Source: Repository{
			RelativePath: record.relativePath,
			Storage:      shard.primary,
		},
		State:  record.state,
		Target: record.target,
	}, nil
}

// ErrInvalidReplTarget indicates a target repository cannot be chosen because
// it fails preconditions for being replicatable
var ErrInvalidReplTarget = errors.New("target repository fails preconditions for replication")

// CreateSecondaryReplJobs creates a replication job for each secondary that
// backs the specified repository. Upon success, the job IDs will be returned.
func (md *MemoryDatastore) CreateSecondaryReplJobs(source Repository) ([]uint64, error) {
	md.jobs.Lock()
	defer md.jobs.Unlock()

	emptyRepo := Repository{}
	if source == emptyRepo {
		return nil, errors.New("invalid source repository")
	}

	shard, ok := md.getShard(source.RelativePath)
	if !ok {
		return nil, fmt.Errorf(
			"unable to find shard for project at relative path %q",
			source.RelativePath,
		)
	}

	var jobIDs []uint64

	for _, secondary := range shard.secondaries {
		nextID := uint64(len(md.jobs.records) + 1)

		md.jobs.next++
		md.jobs.records[md.jobs.next] = jobRecord{
			target:       secondary,
			state:        JobStatePending,
			relativePath: source.RelativePath,
		}

		jobIDs = append(jobIDs, nextID)
	}

	return jobIDs, nil
}

// UpdateReplJob updates an existing replication job's state
func (md *MemoryDatastore) UpdateReplJob(jobID uint64, newState JobState) error {
	md.jobs.Lock()
	defer md.jobs.Unlock()

	job, ok := md.jobs.records[jobID]
	if !ok {
		return fmt.Errorf("job ID %d does not exist", jobID)
	}

	if newState == JobStateComplete || newState == JobStateCancelled {
		// remove the job to avoid filling up memory with unneeded job records
		delete(md.jobs.records, jobID)
		return nil
	}

	job.state = newState
	md.jobs.records[jobID] = job
	return nil
}

// SetPrimary sets the primary datastore location
func (md *MemoryDatastore) SetPrimary(primary config.GitalyServer) error {
	md.primary.Lock()
	defer md.primary.Unlock()

	md.primary.server = primary

	return nil
}

// GetPrimary gets the primary datastore location
func (md *MemoryDatastore) GetPrimary() (config.GitalyServer, error) {
	md.primary.RLock()
	defer md.primary.RUnlock()

	server := md.primary.server
	if server == (config.GitalyServer{}) {
		return config.GitalyServer{}, ErrPrimaryNotSet
	}

	return server, nil
}
