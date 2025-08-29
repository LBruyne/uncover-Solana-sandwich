package types

// SlotLeader represents a mapping of a slot to its leader (validator)
type SlotLeader struct {
	Slot   uint64 `ch:"slot"`
	Leader string `ch:"leader"`
}

type SlotLeaders []*SlotLeader
