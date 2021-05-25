module gitlab.com/gitlab-org/gitaly/v14

exclude (
	// grpc-go version v1.34.0 and v1.35.0-dev have a bug that affects unix domain docket
	// dialing. It should be avoided until upgraded to a newer fixed
	// version. More details:
	// https://github.com/grpc/grpc-go/issues/3990
	github.com/grpc/grpc-go v1.34.0
	github.com/grpc/grpc-go v1.35.0-dev
)

require (
	github.com/cloudflare/tableflip v1.2.2
	github.com/containerd/cgroups v0.0.0-20201118023556-2819c83ced99
	github.com/getsentry/sentry-go v0.7.0
	github.com/git-lfs/git-lfs v1.5.1-0.20210304194248-2e1d981afbe3
	github.com/golang/protobuf v1.3.3
	github.com/google/uuid v1.1.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.3-0.20210213123510-be4c235f9d1c
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hashicorp/yamux v0.0.0-20210316155119-a95892c5f864
	github.com/kelseyhightower/envconfig v1.3.0
	github.com/lib/pq v1.2.0
	github.com/libgit2/git2go/v31 v31.4.12
	github.com/olekukonko/tablewriter v0.0.2
	github.com/opencontainers/runtime-spec v1.0.2
	github.com/opentracing/opentracing-go v1.2.0
	github.com/otiai10/curr v1.0.0 // indirect
	github.com/pelletier/go-toml v1.8.1
	github.com/prometheus/client_golang v1.0.0
	github.com/rubenv/sql-migrate v0.0.0-20191213152630-06338513c237
	github.com/sirupsen/logrus v1.7.0
	github.com/stretchr/testify v1.6.1
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	gitlab.com/gitlab-org/gitlab-shell v1.9.8-0.20201117050822-3f9890ef73dc
	gitlab.com/gitlab-org/labkit v1.0.0
	go.uber.org/goleak v1.1.10
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20210119212857-b64e53b001e4
	golang.org/x/text v0.3.3
	google.golang.org/grpc v1.29.1
)

go 1.15
