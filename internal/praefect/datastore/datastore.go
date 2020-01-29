// Package datastore provides data models and datastore persistence abstractions
// for tracking the state of repository replicas.
//
// See original design discussion:
// https://gitlab.com/gitlab-org/gitaly/issues/1495
package datastore

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
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
	// job is no longer relevant (e.g. a node is moved out of a repository)
	JobStateCancelled
)

// ChangeType indicates what kind of change the replication is propagating
type ChangeType int

const (
	// UpdateRepo is when a replication updates a repository in place
	UpdateRepo ChangeType = iota + 1
	// DeleteRepo is when a replication deletes a repo
	DeleteRepo
)

// ReplJob is an instance of a queued replication job. A replication job is
// meant for updating the repository so that it is synced with the primary
// copy. Scheduled indicates when a replication job should be performed.
type ReplJob struct {
	Change                 ChangeType
	ID                     uint64      // autoincrement ID
	TargetNode, SourceNode models.Node // which node to replicate to?
	RelativePath           string      // source for replication
	State                  JobState
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
}

// ReplicasDatastore manages accessing and setting which secondary replicas
// backup a repository
type ReplicasDatastore interface {
	GetPrimary(virtualStorage string) (models.Node, error)

	GetSecondaries(virtualStorage string) ([]models.Node, error)

	GetReplicas(relativePath string) ([]models.Node, error)

	GetStorageNode(nodeStorage string) (models.Node, error)

	GetStorageNodes() ([]models.Node, error)
}

// ReplJobsDatastore represents the behavior needed for fetching and updating
// replication jobs from the datastore
type ReplJobsDatastore interface {
	// GetJobs fetches a list of chronologically ordered replication
	// jobs for the given storage replica. The returned list will be at most
	// count-length.
	GetJobs(flag JobState, nodeStorage string, count int) ([]ReplJob, error)

	// CreateReplicaReplJobs will create replication jobs for each secondary
	// replica of a repository known to the datastore. A set of replication job
	// ID's for the created jobs will be returned upon success.
	CreateReplicaReplJobs(relativePath string, primaryStorage string, secondaryStorages []string, change ChangeType) ([]uint64, error)

	// UpdateReplJob updates the state of an existing replication job
	UpdateReplJob(jobID uint64, newState JobState) error
}

type jobRecord struct {
	change                               ChangeType
	relativePath                         string // project's relative path
	targetNodeStorage, sourceNodeStorage string
	state                                JobState
}

// MemoryDatastore is a simple datastore that isn't persisted to disk. It is
// only intended for early beta requirements and as a reference implementation
// for the eventual SQL implementation
type MemoryDatastore struct {
	jobs *struct {
		sync.RWMutex
		records map[uint64]jobRecord // all jobs indexed by ID
	}

	// storageNodes is read-only after initialization
	// if modification needed there must be synchronization for concurrent access to it
	storageNodes map[string]models.Node

	repositories *struct {
		sync.RWMutex
		m map[string]models.Repository
	}

	// virtualStorages is read-only after initialization
	// if modification needed there must be synchronization for concurrent access to it
	virtualStorages map[string][]*models.Node
}

// NewInMemory returns an initialized in-memory datastore
func NewInMemory(cfg config.Config) *MemoryDatastore {
	m := &MemoryDatastore{
		storageNodes: map[string]models.Node{},
		jobs: &struct {
			sync.RWMutex
			records map[uint64]jobRecord // all jobs indexed by ID
		}{
			records: map[uint64]jobRecord{},
		},
		repositories: &struct {
			sync.RWMutex
			m map[string]models.Repository
		}{
			m: map[string]models.Repository{},
		},
		virtualStorages: map[string][]*models.Node{},
	}

	for _, virtualStorage := range cfg.VirtualStorages {
		m.virtualStorages[virtualStorage.Name] = virtualStorage.Nodes

		for _, node := range virtualStorage.Nodes {
			if _, ok := m.storageNodes[node.Storage]; ok {
				continue
			}
			m.storageNodes[node.Storage] = *node
		}
	}

	return m
}

// ErrNoPrimaryForStorage indicates a virtual storage has no primary associated with it
var ErrNoPrimaryForStorage = errors.New("no primary for storage")

// GetPrimary returns the primary configured in the config file
func (md *MemoryDatastore) GetPrimary(virtualStorage string) (models.Node, error) {
	for _, node := range md.virtualStorages[virtualStorage] {
		if node.DefaultPrimary {
			return *node, nil
		}
	}

	return models.Node{}, ErrNoPrimaryForStorage
}

// GetSecondaries gets the secondary nodes associated with a virtual storage
func (md *MemoryDatastore) GetSecondaries(virtualStorage string) ([]models.Node, error) {
	var secondaries []models.Node

	for _, node := range md.virtualStorages[virtualStorage] {
		if !node.DefaultPrimary {
			secondaries = append(secondaries, *node)
		}
	}

	return secondaries, nil
}

// GetReplicas gets the secondaries for a repository based on the relative path
func (md *MemoryDatastore) GetReplicas(relativePath string) ([]models.Node, error) {
	md.repositories.RLock()
	defer md.repositories.RUnlock()

	repository, ok := md.repositories.m[relativePath]
	if !ok {
		return nil, errors.New("repository not found")
	}

	// to prevent possible modification of element of the slice
	copied := repository.Clone()
	return copied.Replicas, nil
}

// GetStorageNode gets all storage nodes
func (md *MemoryDatastore) GetStorageNode(nodeStorage string) (models.Node, error) {
	node, ok := md.storageNodes[nodeStorage]
	if !ok {
		return models.Node{}, errors.New("node not found")
	}

	return node, nil
}

// GetStorageNodes gets all storage nodes
func (md *MemoryDatastore) GetStorageNodes() ([]models.Node, error) {
	var storageNodes []models.Node
	for _, storageNode := range md.storageNodes {
		storageNodes = append(storageNodes, storageNode)
	}

	return storageNodes, nil
}

// ErrReplicasMissing indicates the repository does not have any backup
// replicas
var ErrReplicasMissing = errors.New("repository missing secondary replicas")

// GetJobs is a more general method to retrieve jobs of a certain state from the datastore
func (md *MemoryDatastore) GetJobs(state JobState, targetNodeStorage string, count int) ([]ReplJob, error) {
	md.jobs.RLock()
	defer md.jobs.RUnlock()

	var results []ReplJob

	for i, record := range md.jobs.records {
		// state is a bitmap that is a combination of one or more JobStates
		if record.state&state != 0 && record.targetNodeStorage == targetNodeStorage {
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
// referencing the current repository for the project being replicated
func (md *MemoryDatastore) replJobFromRecord(jobID uint64, record jobRecord) (ReplJob, error) {
	sourceNode, err := md.GetStorageNode(record.sourceNodeStorage)
	if err != nil {
		return ReplJob{}, err
	}
	targetNode, err := md.GetStorageNode(record.targetNodeStorage)
	if err != nil {
		return ReplJob{}, err
	}

	return ReplJob{
		Change:       record.change,
		ID:           jobID,
		RelativePath: record.relativePath,
		SourceNode:   sourceNode,
		State:        record.state,
		TargetNode:   targetNode,
	}, nil
}

// ErrInvalidReplTarget indicates a targetStorage repository cannot be chosen because
// it fails preconditions for being replicatable
var ErrInvalidReplTarget = errors.New("targetStorage repository fails preconditions for replication")

// CreateReplicaReplJobs creates a replication job for each secondary that
// backs the specified repository. Upon success, the job IDs will be returned.
func (md *MemoryDatastore) CreateReplicaReplJobs(relativePath string, primaryStorage string, secondaryStorages []string, change ChangeType) ([]uint64, error) {
	md.jobs.Lock()
	defer md.jobs.Unlock()

	if relativePath == "" {
		return nil, errors.New("invalid source repository")
	}

	var jobIDs []uint64

	for _, secondaryStorage := range secondaryStorages {
		nextID := uint64(len(md.jobs.records) + 1)

		md.jobs.records[nextID] = jobRecord{
			change:            change,
			targetNodeStorage: secondaryStorage,
			state:             JobStatePending,
			relativePath:      relativePath,
			sourceNodeStorage: primaryStorage,
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
