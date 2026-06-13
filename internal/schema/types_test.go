package schema

import "testing"

func TestLookupStorage_UnknownAllocUnitFallsBackToIDs(t *testing.T) {
	table := &Table{ObjectID: 42, Schema: "dbo", Name: "Orders", BaseIndexName: "PK_Orders"}
	sch := &Schema{
		Tables: map[int]*Table{42: table},
		ByName: map[string]*Table{"dbo.Orders": table},
		CompressionByAllocUnit: map[int64]*PartitionCompression{
			1001: {ObjectID: 42, AllocUnitID: 1001, PartitionID: 2001},
		},
		CompressionByPartition: map[int64]*PartitionCompression{
			2001: {ObjectID: 42, AllocUnitID: 1001, PartitionID: 2001},
		},
	}

	if got := sch.LookupStorage("Unknown Alloc Unit", 1001, 2001); got != table {
		t.Fatalf("alloc-unit fallback=%v", got)
	}
	if got := sch.LookupStorage("", 0, 2001); got != table {
		t.Fatalf("partition fallback=%v", got)
	}
	if got := sch.LookupStorage("dbo.Orders.PK_Orders", 0, 0); got != table {
		t.Fatalf("clustered index fallback=%v", got)
	}
	if got := sch.LookupStorage("dbo.Orders.IX_Orders_Date", 0, 0); got != nil {
		t.Fatalf("nonclustered index resolved as base table: %v", got)
	}
	if !sch.IsKnownNonBaseIndex("dbo.Orders.IX_Orders_Date") {
		t.Fatal("nonclustered index was not recognized")
	}
	if sch.IsKnownNonBaseIndex("dbo.Orders.PK_Orders") {
		t.Fatal("base clustered index was classified as nonclustered")
	}
}
