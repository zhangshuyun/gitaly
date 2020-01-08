package protoregistry

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	protobufTag      = "protobuf"
	protobufOneOfTag = "protobuf_oneof"
)

// ErrTargetRepoMissing indicates that the target repo is missing or not set
var ErrTargetRepoMissing = errors.New("empty Repository")

func reflectFindRepoTarget(pbMsg proto.Message, targetOID []int) (*gitalypb.Repository, error) {
	msgV, e := reflectFindOID(pbMsg, targetOID)
	if e != nil {
		if e == ErrProtoFieldEmpty {
			return nil, ErrTargetRepoMissing
		}
		return nil, e
	}

	targetRepo, ok := msgV.Interface().(*gitalypb.Repository)
	if !ok {
		return nil, fmt.Errorf("repo target OID %v points to non-Repo type %+v", targetOID, msgV.Interface())
	}

	return targetRepo, nil
}

func reflectFindStorage(pbMsg proto.Message, targetOID []int) (string, error) {
	msgV, e := reflectFindOID(pbMsg, targetOID)
	if e != nil {
		return "", e
	}

	targetRepo, ok := msgV.Interface().(string)
	if !ok {
		return "", fmt.Errorf("repo target OID %v points to non-string type %+v", targetOID, msgV.Interface())
	}

	return targetRepo, nil
}

// ErrProtoFieldEmpty indicates the protobuf field is empty
var ErrProtoFieldEmpty = errors.New("proto field is empty")

// reflectFindOID finds the target repository by using the OID to
// navigate the struct tags
// Warning: this reflection filled function is full of forbidden dark elf magic
func reflectFindOID(pbMsg proto.Message, targetOID []int) (reflect.Value, error) {
	msgV := reflect.ValueOf(pbMsg)
	for _, fieldNo := range targetOID {
		var err error

		msgV, err = findProtoField(msgV, fieldNo)
		if err != nil {
			return reflect.Value{}, err
		}
	}
	return msgV, nil
}

// matches a tag string like "bytes,1,opt,name=repository,proto3"
var protobufTagRegex = regexp.MustCompile(`^(.*?),(\d+),(.*?),name=(.*?),proto3(\,oneof)?$`)

const (
	protobufTagRegexGroups     = 6
	protobufTagRegexFieldGroup = 2
)

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return math.Float64bits(v.Float()) == 0
	case reflect.Complex64, reflect.Complex128:
		c := v.Complex()
		return math.Float64bits(real(c)) == 0 && math.Float64bits(imag(c)) == 0
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if !isZero(v.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		return v.IsNil()
	case reflect.String:
		return v.Len() == 0
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !isZero(v.Field(i)) {
				return false
			}
		}
		return true
	default:
		// This should never happens, but will act as a safeguard for
		// later, as a default value doesn't makes sense here.
		panic(&reflect.ValueError{"reflect.Value.IsZero", v.Kind()})
	}
}

func findProtoField(msgV reflect.Value, protoField int) (reflect.Value, error) {
	if isZero(msgV) {
		return reflect.Value{}, ErrProtoFieldEmpty
	}

	msgV = reflect.Indirect(msgV)
	for i := 0; i < msgV.NumField(); i++ {
		field := msgV.Type().Field(i)

		ok, err := tryNumberedField(field, protoField)
		if err != nil {
			return reflect.Value{}, err
		}
		if ok {
			return msgV.FieldByName(field.Name), nil
		}

		oneofField, ok := tryOneOfField(msgV, field, protoField)
		if !ok {
			continue
		}
		return oneofField, nil
	}

	err := fmt.Errorf(
		"unable to find protobuf field %d in message %s",
		protoField, msgV.Type().Name(),
	)
	return reflect.Value{}, err
}

func tryNumberedField(field reflect.StructField, protoField int) (bool, error) {
	tag := field.Tag.Get(protobufTag)
	matches := protobufTagRegex.FindStringSubmatch(tag)
	if len(matches) == protobufTagRegexGroups {
		fieldStr := matches[protobufTagRegexFieldGroup]
		if fieldStr == strconv.Itoa(protoField) {
			return true, nil
		}
	}

	return false, nil
}

func tryOneOfField(msgV reflect.Value, field reflect.StructField, protoField int) (reflect.Value, bool) {
	if isZero(msgV) {
		return reflect.Value{}, false
	}

	oneOfTag := field.Tag.Get(protobufOneOfTag)
	if oneOfTag == "" {
		return reflect.Value{}, false // empty tag means this is not a oneOf field
	}

	// try all of the oneOf fields until a match is found
	msgV = msgV.FieldByName(field.Name).Elem().Elem()
	for i := 0; i < msgV.NumField(); i++ {
		field = msgV.Type().Field(i)

		ok, err := tryNumberedField(field, protoField)
		if err != nil {
			return reflect.Value{}, false
		}
		if ok {
			return msgV.FieldByName(field.Name), true
		}
	}

	return reflect.Value{}, false
}
