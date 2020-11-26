package trailerparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmptyMessage(t *testing.T) {
	input := []byte{}
	pairs := Parse(input)

	assert.Equal(t, 0, len(pairs))
}

func TestMessageWithoutTrailers(t *testing.T) {
	input := []byte("")
	pairs := Parse(input)

	assert.Equal(t, 0, len(pairs))
}

func TestSingleTrailer(t *testing.T) {
	input := []byte("Key: Value")
	pairs := Parse(input)

	assert.Equal(t, 1, len(pairs))
	assert.Equal(t, "Key", string(pairs[0].Key))
	assert.Equal(t, "Value", string(pairs[0].Value))
}

func TestSingleTrailerWithTooLongKey(t *testing.T) {
	input := []byte("Foo: Bar\000")

	for i := 0; i < (maxKeySize + 1); i++ {
		input = append(input, byte('a'))
	}

	input = append(input, []byte(": Value")...)

	pairs := Parse(input)

	assert.Equal(t, 1, len(pairs))
	assert.Equal(t, "Foo", string(pairs[0].Key))
	assert.Equal(t, "Bar", string(pairs[0].Value))
}

func TestSingleTrailerWithTooLongValue(t *testing.T) {
	input := []byte("Foo: Bar\000Key: ")

	for i := 0; i < (maxValueSize + 1); i++ {
		input = append(input, byte('a'))
	}

	pairs := Parse(input)

	assert.Equal(t, 1, len(pairs))
	assert.Equal(t, "Foo", string(pairs[0].Key))
	assert.Equal(t, "Bar", string(pairs[0].Value))
}

func TestTooManyTrailers(t *testing.T) {
	input := []byte{}

	for i := 0; i < maxTrailers+1; i++ {
		input = append(input, []byte("Key: value\000")...)
	}

	pairs := Parse(input)

	assert.Equal(t, maxTrailers, len(pairs))
}

func TestSingleTrailerWithoutValue(t *testing.T) {
	input := []byte("Key:")
	pairs := Parse(input)

	assert.Equal(t, 1, len(pairs))
	assert.Equal(t, "Key", string(pairs[0].Key))
	assert.Equal(t, "", string(pairs[0].Value))
}

func TestSingleTrailerWithoutValueWithTrailingNullByte(t *testing.T) {
	input := []byte("Key:")
	pairs := Parse(input)

	assert.Equal(t, 1, len(pairs))
	assert.Equal(t, "Key", string(pairs[0].Key))
	assert.Equal(t, "", string(pairs[0].Value))
}

func TestMultipleTrailers(t *testing.T) {
	input := []byte("Key1: Value1\000Key2: Value2")
	pairs := Parse(input)

	assert.Equal(t, 2, len(pairs))
	assert.Equal(t, "Key1", string(pairs[0].Key))
	assert.Equal(t, "Value1", string(pairs[0].Value))
	assert.Equal(t, "Key2", string(pairs[1].Key))
	assert.Equal(t, "Value2", string(pairs[1].Value))
}

func TestMultipleTrailersWithTrailingNullByte(t *testing.T) {
	input := []byte("Key1: Value1\000Key2: Value2")
	pairs := Parse(input)

	assert.Equal(t, 2, len(pairs))
	assert.Equal(t, "Key1", string(pairs[0].Key))
	assert.Equal(t, "Value1", string(pairs[0].Value))
	assert.Equal(t, "Key2", string(pairs[1].Key))
	assert.Equal(t, "Value2", string(pairs[1].Value))
}

func TestInvalidTrailer(t *testing.T) {
	// When a string like this is included in a commit message, Git for some
	// reason treats it as a trailer.
	input := []byte("(cherry picked from commit ABC)")
	pairs := Parse(input)

	assert.Equal(t, 0, len(pairs))
}
