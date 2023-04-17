package table

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type DatumTp int

const (
	unknownType DatumTp = iota
	signedType
	unsignedType
	binaryType
)

// Datum could be a binary string, uint64 or int64.
type Datum struct {
	Val interface{}
	Tp  DatumTp // signed, unsigned, binary
}

func DONOTUSEsimplifyType(desiredType DatumTp, val interface{}) (interface{}, error) {
	// the Val always comes in as a string because when using an interface in reading, it's never typed correctly.
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

func mySQLTypeToDatumTp(mysqlTp string) DatumTp {
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

func NewDatum(val interface{}, tp DatumTp) Datum {
	return Datum{
		Val: val,
		Tp:  tp,
	}
}

func NewDatumConvertTp(val interface{}, tp DatumTp) Datum {
	var err error
	if tp == signedType {
		val, err = strconv.ParseInt(fmt.Sprint(val), 10, 64)
	} else {
		val, err = strconv.ParseUint(fmt.Sprint(val), 10, 64)
	}
	if err != nil {
		panic(err)
	}
	return Datum{
		Val: val,
		Tp:  tp,
	}
}

func datumValFromString(val string, tp DatumTp) (interface{}, error) {
	if tp == signedType {
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	}
	return strconv.ParseUint(val, 10, 64)
}

func NewDatumFromMySQL(val string, mysqlTp string) (Datum, error) {
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

func NewNilDatum(tp DatumTp) Datum {
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
	ret := d
	if d.Tp == signedType {
		ret.Val = d.Val.(int64) + int64(addVal)
		return ret
	}
	ret.Val = d.Val.(uint64) + addVal
	return ret
	// TODO: make sure to handle overflow gracefully.
}

// Return the diff between 2 datums as a uint64.
func (d Datum) Range(d2 Datum) uint64 {
	if d.Tp == signedType {
		return uint64(d.Val.(int64) - d2.Val.(int64))
	}
	return d.Val.(uint64) - d2.Val.(uint64)
}

func (d Datum) String() string {
	return fmt.Sprintf("%v", d.Val)
}

func (d Datum) IsNil() bool {
	return d.Val == nil
}

func (d Datum) GreaterThanOrEqual(d2 Datum) bool {
	if d.Tp == signedType {
		return d.Val.(int64) >= d2.Val.(int64)
	}
	return d.Val.(uint64) >= d2.Val.(uint64)
}

func (d Datum) GreaterThan(d2 Datum) bool {
	if d.Tp == signedType {
		return d.Val.(int64) > d2.Val.(int64)
	}
	return d.Val.(uint64) > d2.Val.(uint64)
}
