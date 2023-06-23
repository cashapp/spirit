package table

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Chunk is returned by chunk.Next()
// Applications can use it to iterate over the rows.
type Chunk struct {
	Key        string
	ChunkSize  uint64
	LowerBound *Boundary
	UpperBound *Boundary
}

// Boundary is used by chunk for lower or upper boundary
type Boundary struct {
	Value     datum
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
		conds = append(conds, fmt.Sprintf("`%s` %s %s", c.Key, operator, c.LowerBound.Value))
	}
	if c.UpperBound != nil {
		operator := OpLessEqual
		if !c.UpperBound.Inclusive {
			operator = OpLessThan
		}
		conds = append(conds, fmt.Sprintf("`%s` %s %s", c.Key, operator, c.UpperBound.Value))
	}
	if c.LowerBound == nil && c.UpperBound == nil {
		conds = append(conds, "1=1")
	}
	return strings.Join(conds, " AND ")
}

func (c *Chunk) JSON() string {
	return fmt.Sprintf(`{"Key":"%s","ChunkSize":%d,"LowerBound":%s,"UpperBound":%s}`,
		c.Key,
		c.ChunkSize,
		c.LowerBound.JSON(),
		c.UpperBound.JSON(),
	)
}

func (b *Boundary) JSON() string {
	// encode values as strings otherwise we get JSON floats
	// which can corrupt larger values. Issue #125
	return fmt.Sprintf(`{"Value": "%s","Inclusive":%t}`, b.Value, b.Inclusive)
}

type JSONChunk struct {
	Key        string
	ChunkSize  uint64
	LowerBound JSONBoundary
	UpperBound JSONBoundary
}

type JSONBoundary struct {
	Value     interface{}
	Inclusive bool
}

func NewChunkFromJSON(jsonStr string, tp string) (*Chunk, error) {
	var chunk JSONChunk
	err := json.Unmarshal([]byte(jsonStr), &chunk)
	if err != nil {
		return nil, err
	}
	lowerVal, err := newDatumFromMySQL(fmt.Sprint(chunk.LowerBound.Value), tp)
	if err != nil {
		return nil, err
	}
	upperVal, err := newDatumFromMySQL(fmt.Sprint(chunk.UpperBound.Value), tp)
	if err != nil {
		return nil, err
	}
	return &Chunk{
		Key:       chunk.Key,
		ChunkSize: chunk.ChunkSize,
		LowerBound: &Boundary{
			Value:     lowerVal,
			Inclusive: chunk.LowerBound.Inclusive,
		},
		UpperBound: &Boundary{
			Value:     upperVal,
			Inclusive: chunk.UpperBound.Inclusive,
		},
	}, nil
}
