package env_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestGetBool(t *testing.T) {
	for _, tc := range []struct {
		value         string
		fallback      bool
		expected      bool
		expectedErrIs error
	}{
		{
			value:    "true",
			expected: true,
		},
		{
			value:    "false",
			expected: false,
		},
		{
			value:    "1",
			expected: true,
		},
		{
			value:    "0",
			expected: false,
		},
		{
			value:    "",
			expected: false,
		},
		{
			value:    "",
			fallback: true,
			expected: true,
		},
		{
			value:         "bad",
			expected:      false,
			expectedErrIs: strconv.ErrSyntax,
		},
		{
			value:         "bad",
			fallback:      true,
			expected:      true,
			expectedErrIs: strconv.ErrSyntax,
		},
	} {
		t.Run(fmt.Sprintf("value=%s,fallback=%t", tc.value, tc.fallback), func(t *testing.T) {
			cleanup := testhelper.ModifyEnvironment(t, "TEST_BOOL", tc.value)
			t.Cleanup(cleanup)

			result, err := env.GetBool("TEST_BOOL", tc.fallback)

			if tc.expectedErrIs != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, tc.expectedErrIs), err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestGetInt(t *testing.T) {
	for _, tc := range []struct {
		value         string
		fallback      int
		expected      int
		expectedErrIs error
	}{
		{
			value:    "3",
			expected: 3,
		},
		{
			value:    "",
			expected: 0,
		},
		{
			value:    "",
			fallback: 3,
			expected: 3,
		},
		{
			value:         "bad",
			expected:      0,
			expectedErrIs: strconv.ErrSyntax,
		},
		{
			value:         "bad",
			fallback:      3,
			expected:      3,
			expectedErrIs: strconv.ErrSyntax,
		},
	} {
		t.Run(fmt.Sprintf("value=%s,fallback=%d", tc.value, tc.fallback), func(t *testing.T) {
			cleanup := testhelper.ModifyEnvironment(t, "TEST_INT", tc.value)
			t.Cleanup(cleanup)

			result, err := env.GetInt("TEST_INT", tc.fallback)

			if tc.expectedErrIs != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, tc.expectedErrIs), err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}
