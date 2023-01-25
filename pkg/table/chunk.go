package table

import (
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
	Value     interface{}
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
		conds = append(conds, fmt.Sprintf("%s %s %v", c.Key, operator, c.LowerBound.Value))
	}
	if c.UpperBound != nil {
		operator := OpLessEqual
		if !c.UpperBound.Inclusive {
			operator = OpLessThan
		}
		conds = append(conds, fmt.Sprintf("%s %s %v", c.Key, operator, c.UpperBound.Value))
	}
	if c.LowerBound == nil && c.UpperBound == nil {
		conds = append(conds, "1=1")
	}
	return strings.Join(conds, " AND ")
}
