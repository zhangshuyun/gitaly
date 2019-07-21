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
	// job is no longer relevant (e.g. a node is moved out of a shard)
	JobStateCancelled
)

// ReplJob is an instance of a queued replication job. A replication job is
// meant for updating the repository so that it is synced with the primary
// copy. Scheduled indicates when a replication job should be performed.
type ReplJob struct {
	ID     uint64            // autoincrement ID
	Target string            // which storage location to replicate to?
	Source models.Repository // source for replication
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
}

// ReplicasDatastore manages accessing and setting which secondary replicas
// backup a repository
type ReplicasDatastore interface {
	GetSecondaries(relativePath string) ([]models.StorageNode, error)

	GetNodesForStorage(storageName string) ([]models.StorageNode, error)

	GetNodeStorages() ([]models.StorageNode, error)

	GetPrimary(relativePath string) (*models.StorageNode, error)

	SetPrimary(relativePath string, storageNodeID int) error

	AddSecondary(relativePath string, storageNodeID int) error

	RemoveSecondary(relativePath string, storageNodeID int) error

	GetShard(relativePath string) (*models.Shard, error)
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
	CreateSecondaryReplJobs(relativePath string) ([]uint64, error)

	// UpdateReplJob updates the state of an existing replication job
	UpdateReplJob(jobID uint64, newState JobState) error
}

type jobRecord struct {
	relativePath  string // project's relative path
	targetStorage string
	state         JobState
}

// MemoryDatastore is a simple datastore that isn't persisted to disk. It is
// only intended for early beta requirements and as a reference implementation
// for the eventual SQL implementation
type MemoryDatastore struct {
	jobs *struct {
		sync.RWMutex
		next    uint64
		records map[uint64]jobRecord // all jobs indexed by ID
	}

	nodeStorages *struct {
		sync.RWMutex
		m map[int]models.StorageNode
	}

	shards *struct {
		sync.RWMutex
		m map[string]models.Shard
	}
}

// NewMemoryDatastore returns an initialized in-memory datastore
func NewMemoryDatastore() *MemoryDatastore {
	return &MemoryDatastore{
		nodeStorages: &struct {
			sync.RWMutex
			m map[int]models.StorageNode
		}{
			m: map[int]models.StorageNode{},
		},
		jobs: &struct {
			sync.RWMutex
			next    uint64
			records map[uint64]jobRecord // all jobs indexed by ID
		}{
			next:    0,
			records: map[uint64]jobRecord{},
		},
		shards: &struct {
			sync.RWMutex
			m map[string]models.Shard
		}{
			m: map[string]models.Shard{},
		},
	}
}

// GetSecondaries gets the secondaries for a shard based on the relative path
func (md *MemoryDatastore) GetSecondaries(relativePath string) ([]models.StorageNode, error) {
	md.shards.RLock()
	md.nodeStorages.RLock()
	defer md.nodeStorages.RUnlock()
	defer md.shards.RUnlock()

	shard, ok := md.shards.m[relativePath]
	if !ok {
		return nil, errors.New("shard not found")
	}

	return shard.Secondaries, nil
}

// GetNodesForStorage gets all storage nodes that serve a specified storage
func (md *MemoryDatastore) GetNodesForStorage(storageName string) ([]models.StorageNode, error) {
	md.nodeStorages.RLock()
	defer md.nodeStorages.RUnlock()

	var nodes []models.StorageNode
	for _, nodeStorage := range md.nodeStorages.m {
		if nodeStorage.StorageName == storageName {
			nodes = append(nodes, nodeStorage)
		}
	}
	return nodes, nil
}

// GetNodeStorages gets all storage nodes
func (md *MemoryDatastore) GetNodeStorages() ([]models.StorageNode, error) {
	md.nodeStorages.RLock()
	defer md.nodeStorages.RUnlock()

	var nodeStorages []models.StorageNode
	for _, nodeStorage := range md.nodeStorages.m {
		nodeStorages = append(nodeStorages, nodeStorage)
	}

	return nodeStorages, nil
}

// GetPrimary gets the primary storage node for a shard of a repository relative path
func (md *MemoryDatastore) GetPrimary(relativePath string) (*models.StorageNode, error) {
	md.shards.RLock()
	defer md.shards.RUnlock()

	shard, ok := md.shards.m[relativePath]
	if !ok {
		return nil, errors.New("shard not found")
	}

	nodeStorage, ok := md.nodeStorages.m[shard.Primary.ID]
	if !ok {
		return nil, errors.New("node storage not found")
	}
	return &nodeStorage, nil

}

// SetPrimary sets the primary storagee node for a shard of a repository relative path
func (md *MemoryDatastore) SetPrimary(relativePath string, storageNodeID int) error {
	md.shards.Lock()
	defer md.shards.Unlock()

	shard, ok := md.shards.m[relativePath]
	if !ok {
		return errors.New("shard not found")
	}

	nodeStorage, ok := md.nodeStorages.m[storageNodeID]
	if !ok {
		return errors.New("node storage not found")
	}

	shard.Primary = nodeStorage

	md.shards.m[relativePath] = shard
	return nil
}

// AddSecondary adds a secondary to a shard of a repository relative path
func (md *MemoryDatastore) AddSecondary(relativePath string, storageNodeID int) error {
	md.shards.Lock()
	defer md.shards.Unlock()

	shard, ok := md.shards.m[relativePath]
	if !ok {
		return errors.New("shard not found")
	}

	nodeStorage, ok := md.nodeStorages.m[storageNodeID]
	if !ok {
		return errors.New("node storage not found")
	}

	shard.Secondaries = append(shard.Secondaries, nodeStorage)

	md.shards.m[relativePath] = shard
	return nil
}

// RemoveSecondary removes a secondary from a shard of a repository relative path
func (md *MemoryDatastore) RemoveSecondary(relativePath string, storageNodeID int) error {
	md.shards.Lock()
	defer md.shards.Unlock()

	shard, ok := md.shards.m[relativePath]
	if !ok {
		return errors.New("shard not found")
	}

	var secondaries []models.StorageNode
	for _, secondary := range shard.Secondaries {
		if secondary.ID != storageNodeID {
			secondaries = append(secondaries, secondary)
		}
	}

	shard.Secondaries = secondaries
	md.shards.m[relativePath] = shard
	return nil
}

// GetShard gets the shard for a repository relative path
func (md *MemoryDatastore) GetShard(relativePath string) (*models.Shard, error) {
	md.shards.Lock()
	defer md.shards.Unlock()

	shard, ok := md.shards.m[relativePath]
	if !ok {
		return nil, errors.New("shard not found")
	}

	return &shard, nil
}

// ErrSecondariesMissing indicates the repository does not have any backup
// replicas
var ErrSecondariesMissing = errors.New("repository missing secondary replicas")

// GetJobs is a more general method to retrieve jobs of a certain state from the datastore
func (md *MemoryDatastore) GetJobs(state JobState, targetStorage string, count int) ([]ReplJob, error) {
	md.jobs.RLock()
	defer md.jobs.RUnlock()

	var results []ReplJob

	for i, record := range md.jobs.records {
		// state is a bitmap that is a combination of one or more JobStates
		if record.state&state != 0 && record.targetStorage == targetStorage {
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
	shard, err := md.GetShard(record.relativePath)
	if err != nil {
		return ReplJob{}, fmt.Errorf(
			"unable to find shard for project at relative path %q",
			record.relativePath,
		)
	}

	return ReplJob{
		ID: jobID,
		Source: models.Repository{
			RelativePath: record.relativePath,
			Storage:      shard.Primary.StorageName,
		},
		State:  record.state,
		Target: record.targetStorage,
	}, nil
}

// ErrInvalidReplTarget indicates a targetStorage repository cannot be chosen because
// it fails preconditions for being replicatable
var ErrInvalidReplTarget = errors.New("targetStorage repository fails preconditions for replication")

// CreateSecondaryReplJobs creates a replication job for each secondary that
// backs the specified repository. Upon success, the job IDs will be returned.
func (md *MemoryDatastore) CreateSecondaryReplJobs(relativePath string) ([]uint64, error) {
	md.jobs.Lock()
	defer md.jobs.Unlock()

	if relativePath == "" {
		return nil, errors.New("invalid source repository")
	}

	shard, err := md.GetShard(relativePath)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to find shard for project at relative path %q",
			relativePath,
		)
	}

	var jobIDs []uint64

	for _, secondary := range shard.Secondaries {
		nextID := uint64(len(md.jobs.records) + 1)

		md.jobs.next++
		md.jobs.records[md.jobs.next] = jobRecord{
			targetStorage: secondary.StorageName,
			state:         JobStatePending,
			relativePath:  relativePath,
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
