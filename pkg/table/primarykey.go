package table

// primaryKey is comprised of 1 or more datums
type primaryKey struct {
	datums []datum
}

func newPrimaryKey(datums ...datum) primaryKey {
	return primaryKey{datums: datums}
}

func (pk primaryKey) MaxValue() datum {
	return pk.datums[0].MaxValue()
}

func (pk primaryKey) MinValue() datum {
	return pk.datums[0].MinValue()
}

func (pk primaryKey) String() string {
	return pk.datums[0].String()
}

func (pk primaryKey) IsNil() bool {
	return pk.datums[0].IsNil()
}

func (pk primaryKey) GreaterThanOrEqual(d2 datum) bool {
	return pk.datums[0].GreaterThanOrEqual(d2)
}
