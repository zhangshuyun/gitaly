package operations

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDateFromProto(t *testing.T) {
	locUtc, _ := time.LoadLocation("UTC")
	locShanghai, _ := time.LoadLocation("Asia/Shanghai")
	staticNow := time.Now()

	testCases := []struct {
		timezone string
		want     *time.Location
		err      error
	}{
		{"UTC", locUtc, nil},
		{"Asia/Shanghai", locShanghai, nil},
		{"Illegal/Format", locUtc, errors.New("unknown time zone Illegal/Format")},
		{"", locUtc, nil},
	}
	for _, testCase := range testCases {
		t.Run(testCase.timezone, func(t *testing.T) {
			req := &gitalypb.UserSquashRequest{
				User:      &gitalypb.User{Timezone: testCase.timezone},
				Timestamp: timestamppb.New(staticNow),
			}
			got, err := dateFromProto(req)

			if testCase.err != nil {
				assert.Equal(t, testCase.err.Error(), err.Error())
			} else {
				assert.Equal(t, staticNow.UnixNano(), got.UnixNano())
			}
			assert.Equal(t, testCase.want, got.Location())
		})
	}
}
