module gitlab.com/gitlab-org/gitaly

exclude (
	// grpc-go version v1.34.0 and v1.35.0-dev have a bug that affects unix domain docket
	// dialing. It should be avoided until upgraded to a newer fixed
	// version. More details:
	// https://github.com/grpc/grpc-go/issues/3990
	github.com/grpc/grpc-go v1.34.0
	github.com/grpc/grpc-go v1.35.0-dev
)

require (
	github.com/alexbrainman/sspi v0.0.0-20210105120005-909beea2cc74 // indirect
	github.com/avast/retry-go v3.0.0+incompatible // indirect
	github.com/cloudflare/tableflip v1.2.2
	github.com/containerd/cgroups v0.0.0-20201118023556-2819c83ced99
	github.com/getsentry/sentry-go v0.10.0
	github.com/git-lfs/git-lfs v1.5.1-0.20210304194248-2e1d981afbe3
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hashicorp/yamux v0.0.0-20210316155119-a95892c5f864
	github.com/kelseyhightower/envconfig v1.3.0
	github.com/lib/pq v1.2.0
	github.com/libgit2/git2go/v31 v31.4.12
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/olekukonko/tablewriter v0.0.2
	github.com/opencontainers/runtime-spec v1.0.2
	github.com/opentracing/opentracing-go v1.2.0
	github.com/otiai10/curr v1.0.0 // indirect
	github.com/pelletier/go-toml v1.8.1
	github.com/prometheus/client_golang v1.10.0
	github.com/rubenv/sql-migrate v0.0.0-20191213152630-06338513c237
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.27.0+incompatible
	gitlab.com/gitlab-org/gitlab-shell v1.9.8-0.20201117050822-3f9890ef73dc
	gitlab.com/gitlab-org/labkit v1.4.0
	go.uber.org/goleak v1.1.10
	golang.org/x/crypto v0.0.0-20210421170649-83a5a9bb288b // indirect
	golang.org/x/exp v0.0.0-20200331195152-e8c3332aa8e5 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210412220455-f1c623a9e750
	golang.org/x/text v0.3.5
	google.golang.org/grpc v1.37.0
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

go 1.15
