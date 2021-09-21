package catfile

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/labkit/correlation"
)

func revisionTag(revision git.Revision) opentracing.Tag {
	return opentracing.Tag{Key: "revision", Value: revision}
}

func correlationIDTag(ctx context.Context) opentracing.Tag {
	return opentracing.Tag{Key: "correlation_id", Value: correlation.ExtractFromContext(ctx)}
}

func startSpan(innerCtx context.Context, outerCtx context.Context, methodName string, revision git.Revision) func() {
	innerSpan, ctx := opentracing.StartSpanFromContext(innerCtx, methodName, revisionTag(revision))
	outerSpan, _ := opentracing.StartSpanFromContext(outerCtx, methodName, revisionTag(revision), correlationIDTag(ctx))

	return func() {
		innerSpan.Finish()
		outerSpan.Finish()
	}
}
