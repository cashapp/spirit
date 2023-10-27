package table

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/tidb/pkg/util/sqlescape"
)

type datumTp int

const (
	unknownType datumTp = iota
	signedType
	unsignedType
	binaryType
)

// Datum could be a binary string, uint64 or int64.
type Datum struct {
	Val interface{}
	Tp  datumTp // signed, unsigned, binary
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

func newDatum(val interface{}, tp datumTp) Datum {
	var err error
	if tp == signedType {
		// We expect the value to be an int64, but it could be an int.
		// Anything else we attempt to convert it
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
	} else if tp == unsignedType {
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
	return Datum{
		Val: val,
		Tp:  tp,
	}
}

func datumValFromString(val string, tp datumTp) (interface{}, error) {
	if tp == signedType {
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	} else if tp == unsignedType {
		return strconv.ParseUint(val, 10, 64)
	}
	return val, nil
}

func newDatumFromMySQL(val string, mysqlTp string) (Datum, error) {
	// Figure out the matching simplified type (signed, unsigned, binary)
	// We also have to simplify the value to the type.
	tp := mySQLTypeToDatumTp(mysqlTp)
	sVal, err := datumValFromString(val, tp)
	if err != nil {
		return Datum{}, err
	}
	return Datum{
		Val: sVal,
		Tp:  tp,
	}, nil
}

func NewNilDatum(tp datumTp) Datum {
	return Datum{
		Val: nil,
		Tp:  tp,
	}
}

func (d Datum) MaxValue() Datum {
	if d.Tp == signedType {
		return Datum{
			Val: int64(math.MaxInt64),
			Tp:  signedType,
		}
	}
	return Datum{
		Val: uint64(math.MaxUint64),
		Tp:  d.Tp,
	}
}

func (d Datum) MinValue() Datum {
	if d.Tp == signedType {
		return Datum{
			Val: int64(math.MinInt64),
			Tp:  signedType,
		}
	}
	return Datum{
		Val: uint64(0),
		Tp:  d.Tp,
	}
}

func (d Datum) Add(addVal uint64) Datum {
	if !d.IsNumeric() {
		panic("not supported on binary type")
	}
	ret := d
	if d.Tp == signedType {
		returnVal := d.Val.(int64) + int64(addVal)
		if returnVal < d.Val.(int64) {
			returnVal = int64(math.MaxInt64) // overflow
		}
		ret.Val = returnVal
		return ret
	}
	returnVal := d.Val.(uint64) + addVal
	if returnVal < d.Val.(uint64) {
		returnVal = uint64(math.MaxUint64) // overflow
	}
	ret.Val = returnVal
	return ret
}

// Range returns the diff between 2 datums as an uint64.
func (d Datum) Range(d2 Datum) uint64 {
	if !d.IsNumeric() {
		panic("not supported on binary type")
	}
	if d.Tp == signedType {
		return uint64(d.Val.(int64) - d2.Val.(int64))
	}
	return d.Val.(uint64) - d2.Val.(uint64)
}

// String returns the datum as a SQL escaped string
func (d Datum) String() string {
	if !d.IsNumeric() {
		return "\"" + sqlescape.EscapeString(d.Val.(string)) + "\""
	}
	return fmt.Sprintf("%v", d.Val)
}

// IsNumeric checks if it's signed or unsigned
func (d Datum) IsNumeric() bool {
	return d.Tp == signedType || d.Tp == unsignedType
}

func (d Datum) IsNil() bool {
	return d.Val == nil
}

func (d Datum) GreaterThanOrEqual(d2 Datum) bool {
	if !d.IsNumeric() {
		panic("not supported on binary type")
	}
	if d.Tp == signedType {
		return d.Val.(int64) >= d2.Val.(int64)
	}
	return d.Val.(uint64) >= d2.Val.(uint64)
}
