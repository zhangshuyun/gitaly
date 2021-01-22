package git

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateObjectID(t *testing.T) {
	for _, tc := range []struct {
		desc  string
		oid   string
		valid bool
	}{
		{
			desc:  "valid object ID",
			oid:   "356e7793f9654d51dfb27312a1464062bceb9fa3",
			valid: true,
		},
		{
			desc:  "object ID with non-hex characters fails",
			oid:   "x56e7793f9654d51dfb27312a1464062bceb9fa3",
			valid: false,
		},
		{
			desc:  "object ID with upper-case letters fails",
			oid:   "356E7793F9654D51DFB27312A1464062BCEB9FA3",
			valid: false,
		},
		{
			desc:  "too short object ID fails",
			oid:   "356e7793f9654d51dfb27312a1464062bceb9fa",
			valid: false,
		},
		{
			desc:  "too long object ID fails",
			oid:   "356e7793f9654d51dfb27312a1464062bceb9fa33",
			valid: false,
		},
		{
			desc:  "empty string fails",
			oid:   "",
			valid: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := ValidateObjectID(tc.oid)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.EqualError(t, err, fmt.Sprintf("invalid object ID: %q", tc.oid))
			}
		})
	}
}

func TestNewObjectIDFromHex(t *testing.T) {
	for _, tc := range []struct {
		desc  string
		oid   string
		valid bool
	}{
		{
			desc:  "valid object ID",
			oid:   "356e7793f9654d51dfb27312a1464062bceb9fa3",
			valid: true,
		},
		{
			desc:  "object ID with non-hex characters fails",
			oid:   "x56e7793f9654d51dfb27312a1464062bceb9fa3",
			valid: false,
		},
		{
			desc:  "object ID with upper-case letters fails",
			oid:   "356E7793F9654D51DFB27312A1464062BCEB9FA3",
			valid: false,
		},
		{
			desc:  "too short object ID fails",
			oid:   "356e7793f9654d51dfb27312a1464062bceb9fa",
			valid: false,
		},
		{
			desc:  "too long object ID fails",
			oid:   "356e7793f9654d51dfb27312a1464062bceb9fa33",
			valid: false,
		},
		{
			desc:  "empty string fails",
			oid:   "",
			valid: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := NewObjectIDFromHex(tc.oid)
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, tc.oid, oid.String())
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestIsZeroOID(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		oid    ObjectID
		isZero bool
	}{
		{
			desc:   "zero object ID",
			oid:    ZeroOID,
			isZero: true,
		},
		{
			desc:   "zero object ID",
			oid:    EmptyTreeOID,
			isZero: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.isZero, tc.oid.IsZeroOID())
		})
	}
}
