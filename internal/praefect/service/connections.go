package service

import "google.golang.org/grpc"

// Connections contains connections to Gitaly nodes keyed by virtual storage and storage
//
// This duplicates the praefect.Connections type as it is not possible to import anything from `praefect`
// to `info` or `server` packages due to cyclic dependencies.
type Connections map[string]map[string]*grpc.ClientConn
