package command

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStderrBufferSingleWrite(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedOutput string
	}{
		{
			name:           "case empty input",
			input:          "",
			expectedOutput: "",
		},
		{
			name:           "case single short line with delimiter",
			input:          "12345\n",
			expectedOutput: "12345\n",
		},
		{
			name:           "case single short line without delimiter",
			input:          "12345",
			expectedOutput: "12345",
		},
		{
			name:           "case single long line with delimiter",
			input:          "12345678901234567890\n",
			expectedOutput: "1234567890\n",
		},
		{
			name:           "case single long line without delimiter",
			input:          "12345678901234567890",
			expectedOutput: "1234567890",
		},
		{
			name:           "case multi lines not exceeding line limit",
			input:          "123\n1234\n12345",
			expectedOutput: "123\n1234\n12345",
		},
		{
			name:           "case multi lines exceeding line limit",
			input:          "123\n12345678901234567890\n12345",
			expectedOutput: "123\n1234567890\n12345",
		},
		{
			name:           "case multi lines exceeding buf limit",
			input:          "1234567890\n1234567890\n1234567890\n1234567890",
			expectedOutput: "1234567890\n1234567890\n12345678",
		},
		{
			name:           "case multi lines exceeding line limit and buf limit",
			input:          "12345678901234567890\n12345678901234567890\n12345678901234567890\nn12345678901234567890",
			expectedOutput: "1234567890\n1234567890\n12345678",
		},
		{
			name:           "case multi lines with blank lines",
			input:          "1234567890\n\n\n\n\n1234567890\n1234567890",
			expectedOutput: "1234567890\n\n\n\n\n1234567890\n1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := newStderrBuffer(30, 10, nil)
			require.NoError(t, err)

			n, err := buf.Write([]byte(tt.input))
			require.NoError(t, err)
			require.Equal(t, len(tt.input), n)
			require.Equal(t, tt.expectedOutput, buf.String())
		})
	}
}

func TestStderrBufferMultiWrite(t *testing.T) {
	tests := []struct {
		name           string
		input          []string
		expectedOutput string
	}{
		{
			name:           "case write not exceeding limit",
			input:          []string{"12345\n123", "45\n12345\n"},
			expectedOutput: "12345\n12345\n12345\n",
		},
		{
			name:           "case write exceeding line limit between two writes",
			input:          []string{"12345\n12345", "678901234567890\n12345\n"},
			expectedOutput: "12345\n1234567890\n12345\n",
		},
		{
			name:           "case write exceeding limit for second write",
			input:          []string{"1234567890\n1234567890\n12345", "67890\n1234567890\n"},
			expectedOutput: "1234567890\n1234567890\n12345678",
		},
		{
			name:           "case write exceeding limit for first write",
			input:          []string{"1234567890\n1234567890\n1234567890\n1234567890\n", "1234567890\n1234567890\n"},
			expectedOutput: "1234567890\n1234567890\n12345678",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := newStderrBuffer(30, 10, nil)
			require.NoError(t, err)

			for _, chunk := range tt.input {
				n, err := buf.Write([]byte(chunk))
				require.Equal(t, len(chunk), n)
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedOutput, buf.String())
		})
	}
}

func TestStderrBufferWithInvalidLimit(t *testing.T) {
	bufInvalidLimit, err := newStderrBuffer(100, -1, nil)
	require.Nil(t, bufInvalidLimit)
	require.Error(t, err)
}
