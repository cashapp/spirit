package table

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Chunk is returned by chunk.Next()
// Applications can use it to iterate over the rows.
type Chunk struct {
	Key                  []string
	ChunkSize            uint64
	LowerBound           *Boundary
	UpperBound           *Boundary
	AdditionalConditions string
}

// Boundary is used by chunk for lower or upper boundary
type Boundary struct {
	Value     []Datum
	Inclusive bool
}

// Operator is used to compare values in a WHERE clause.
type Operator string

const (
	OpGreaterThan  Operator = ">"
	OpGreaterEqual Operator = ">="
	OpLessThan     Operator = "<"
	OpLessEqual    Operator = "<="
)

// String strigifies a chunk into a fragment what can be used in a WHERE clause.
// i.e. pk > 100 and pk < 200
func (c *Chunk) String() string {
	var conds []string
	if c.LowerBound != nil {
		operator := OpGreaterEqual
		if !c.LowerBound.Inclusive {
			operator = OpGreaterThan
		}
		str := expandRowConstructorComparison(c.Key, operator, c.LowerBound.Value)
		conds = append(conds, str)
	}
	if c.UpperBound != nil {
		operator := OpLessEqual
		if !c.UpperBound.Inclusive {
			operator = OpLessThan
		}
		str := expandRowConstructorComparison(c.Key, operator, c.UpperBound.Value)
		conds = append(conds, str)
	}
	if c.LowerBound == nil && c.UpperBound == nil {
		conds = append(conds, "1=1")
	}
	if c.AdditionalConditions != "" {
		conds = append(conds, "("+c.AdditionalConditions+")")
	}
	return strings.Join(conds, " AND ")
}

func (c *Chunk) JSON() string {
	return fmt.Sprintf(`{"Key":["%s"],"ChunkSize":%d,"LowerBound":%s,"UpperBound":%s}`,
		strings.Join(c.Key, `","`),
		c.ChunkSize,
		c.LowerBound.JSON(),
		c.UpperBound.JSON(),
	)
}

// JSON encodes a boundary as JSON. The values are represented as strings,
// to avoid JSON float behavior. See Issue #125
func (b *Boundary) JSON() string {
	vals := make([]string, len(b.Value))
	for i, v := range b.Value {
		vals[i] = fmt.Sprintf(`"%s"`, v)
	}
	return fmt.Sprintf(`{"Value": [%s],"Inclusive":%t}`, strings.Join(vals, ","), b.Inclusive)
}

// comparesTo returns true if the boundaries are the same.
// It is used to compare two boundaries, such as if
// a.Upper == b.Lower. For this reason it does not compare
// the operator (inclusive or not)!
func (b *Boundary) comparesTo(b2 *Boundary) bool {
	if len(b.Value) != len(b2.Value) {
		return false
	}
	for i := range b.Value {
		if b.Value[i].Tp != b2.Value[i].Tp {
			return false
		}
		if b.Value[i].Val != b2.Value[i].Val {
			return false
		}
	}
	return true
}

type JSONChunk struct {
	Key        []string
	ChunkSize  uint64
	LowerBound JSONBoundary
	UpperBound JSONBoundary
}

type JSONBoundary struct {
	Value     []string
	Inclusive bool
}

func jsonStrings2Datums(ti *TableInfo, keys []string, vals []string) ([]Datum, error) {
	datums := make([]Datum, len(keys))
	for i, str := range vals {
		tp := ti.datumTp(keys[i])
		datums[i] = newDatum(fmt.Sprint(str), tp)
	}
	return datums, nil
}

func newChunkFromJSON(ti *TableInfo, jsonStr string) (*Chunk, error) {
	var chunk JSONChunk
	err := json.Unmarshal([]byte(jsonStr), &chunk)
	if err != nil {
		return nil, err
	}
	lowerVals, err := jsonStrings2Datums(ti, chunk.Key, chunk.LowerBound.Value)
	if err != nil {
		return nil, err
	}
	upperVals, err := jsonStrings2Datums(ti, chunk.Key, chunk.UpperBound.Value)
	if err != nil {
		return nil, err
	}
	return &Chunk{
		Key:       chunk.Key,
		ChunkSize: chunk.ChunkSize,
		LowerBound: &Boundary{
			Value:     lowerVals,
			Inclusive: chunk.LowerBound.Inclusive,
		},
		UpperBound: &Boundary{
			Value:     upperVals,
			Inclusive: chunk.UpperBound.Inclusive,
		},
	}, nil
}
