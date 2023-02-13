package table

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func simplifyType(desiredType simplifiedKeyType, val interface{}) (interface{}, error) {
	if val == nil || reflect.TypeOf(val) == nil {
		return nil, nil
	}
	impliedType := reflect.TypeOf(val).Kind()
	switch desiredType {
	case signedType:
		if impliedType == reflect.Float64 {
			return int64(val.(float64)), nil
		}
		return reflect.ValueOf(val).Int(), nil
	case unsignedType:
		if impliedType == reflect.Float64 {
			return uint64(val.(float64)), nil
		}
		return reflect.ValueOf(val).Uint(), nil
	case binaryType:
		if strings.HasPrefix(val.(string), "0x") {
			return hexStringToUint64(val.(string))
		}
		return nil, errors.New("unsure how to convert a binary string to min/max")
	default:
		return nil, ErrUnsupportedPKType
	}
}

func hexStringToUint64(s string) (uint64, error) {
	// It will be in the form 0xACDC01
	if !strings.HasPrefix(s, "0x") {
		return 0, fmt.Errorf("invalid hex string: %s", s)
	}
	s = strings.TrimPrefix(s, "0x")
	// base 16 for hexadecimal
	return strconv.ParseUint(s, 16, 64)
}

func mySQLTypeToSimplifiedKeyType(mysqlTp string) simplifiedKeyType {
	switch removeWidth(mysqlTp) {
	case "int", "bigint", "smallint", "tinyint":
		return signedType
	case "int unsigned", "bigint unsigned", "smallint unsigned", "tinyint unsigned":
		return unsignedType
	case "varbinary", "blob", "binary": // no varchar, char, text
		return binaryType
	}
	return unknownType
}

func removeWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func hexString(i uint64) string {
	return fmt.Sprintf("0x%X", i)
}

// lazyFindP90 finds the second to last value in a slice.
// This is the same as a p90 if there are 10 values, but if
// there were 100 values it would technically be a p99 etc.
func lazyFindP90(a []time.Duration) time.Duration {
	sort.Slice(a, func(i, j int) bool {
		return a[i] > a[j]
	})
	return a[len(a)/10]
}
