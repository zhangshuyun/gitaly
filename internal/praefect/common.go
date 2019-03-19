package praefect

// Repository contains all the information necessary to address a specific
// repository
type Repository struct {
	Project string // e.g. gitlab.com/gitaly-org/gitaly
	Storage string // e.g. Default
}

// ReplJob indicates which repo replicas require syncing
type ReplJob struct {
	ID      string // unique ID to track job progress in datastore
	Primary Repository
	Replica Node
}
