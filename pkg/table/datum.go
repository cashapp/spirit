package table

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

type datumTp int

const (
	unknownType datumTp = iota
	signedType
	unsignedType
	binaryType
)

// datum could be a binary string, uint64 or int64.
type datum struct {
	val interface{}
	tp  datumTp // signed, unsigned, binary
}

func mySQLTypeToDatumTp(mysqlTp string) datumTp {
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

func newDatum(val interface{}, tp datumTp) datum {
	var err error
	if tp == signedType {
		// We expect the value to be an int64, but it could be an int.
		// Anything else we convert it
		switch v := val.(type) {
		case int64:
			// do nothing
		case int:
			val = int64(v)
		default:
			val, err = strconv.ParseInt(fmt.Sprint(val), 10, 64)
			if err != nil {
				panic("could not convert datum to int64")
			}
		}
	} else if tp == unsignedType || tp == binaryType {
		// We expect uint64, but it could be uint.
		// We convert anything else.
		switch v := val.(type) {
		case uint64:
			// do nothing
		case uint:
			val = uint64(v)
		default:
			val, err = strconv.ParseUint(fmt.Sprint(val), 10, 64)
			if err != nil {
				panic("could not convert datum to uint64")
			}
		}
	}
	return datum{
		val: val,
		tp:  tp,
	}
}

func datumValFromString(val string, tp datumTp) (interface{}, error) {
	if tp == signedType {
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	}
	return strconv.ParseUint(val, 10, 64)
}

func newDatumFromMySQL(val string, mysqlTp string) (datum, error) {
	// Figure out the matching simplified type (signed, unsigned, binary)
	// We also have to simplify the value to the type.
	tp := mySQLTypeToDatumTp(mysqlTp)
	sVal, err := datumValFromString(val, tp)
	if err != nil {
		return datum{}, err
	}
	return datum{
		val: sVal,
		tp:  tp,
	}, nil
}

func NewNilDatum(tp datumTp) datum {
	return datum{
		val: nil,
		tp:  tp,
	}
}

func (d datum) MaxValue() datum {
	if d.tp == signedType {
		return datum{
			val: int64(math.MaxInt64),
			tp:  signedType,
		}
	}
	return datum{
		val: uint64(math.MaxUint64),
		tp:  d.tp,
	}
}

func (d datum) MinValue() datum {
	if d.tp == signedType {
		return datum{
			val: int64(math.MinInt64),
			tp:  signedType,
		}
	}
	return datum{
		val: uint64(0),
		tp:  d.tp,
	}
}

func (d datum) Add(addVal uint64) datum {
	ret := d
	if d.tp == signedType {
		returnVal := d.val.(int64) + int64(addVal)
		if returnVal < d.val.(int64) {
			returnVal = int64(math.MaxInt64) // overflow
		}
		ret.val = returnVal
		return ret
	}
	returnVal := d.val.(uint64) + addVal
	if returnVal < d.val.(uint64) {
		returnVal = uint64(math.MaxUint64) // overflow
	}
	ret.val = returnVal
	return ret
}

// Range returns the diff between 2 datums as an uint64.
func (d datum) Range(d2 datum) uint64 {
	if d.tp == signedType {
		return uint64(d.val.(int64) - d2.val.(int64))
	}
	return d.val.(uint64) - d2.val.(uint64)
}

func (d datum) String() string {
	if d.tp == binaryType {
		return fmt.Sprintf("0x%X", d.val)
	}
	return fmt.Sprintf("%v", d.val)
}

func (d datum) IsNil() bool {
	return d.val == nil
}

func (d datum) GreaterThanOrEqual(d2 datum) bool {
	if d.tp == signedType {
		return d.val.(int64) >= d2.val.(int64)
	}
	return d.val.(uint64) >= d2.val.(uint64)
}
