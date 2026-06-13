package mssql

import (
	"errors"

	"github.com/uns/mssqllogrecovery/internal/rowdecoder"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

var (
	ErrCompressionMetadataMissing = errors.New("schema_compression_metadata_missing")
	ErrCompressedRowParseFailed   = errors.New("compressed_row_parse_failed")
	ErrCompressedTailParseFailed  = errors.New("compressed_tail_parse_failed")
	ErrCompressedTypeNotSupported = errors.New("compressed_type_not_supported")
	ErrCompressedPageReference    = errors.New("page_dictionary_required")
)

// CompressedColumnSlice describes one column slot inside a compressed row image.
type CompressedColumnSlice struct {
	Column           *schema.Column
	Descriptor       byte
	Offset           int
	LogicalLength    int
	PhysicalLength   int
	CompressedValue  []byte
	IsNull           bool
	IsZeroCompressed bool
	IsTailValue      bool
	IsPageReference  bool
}

// CompressionColumnDebug is one column entry in decompressed_debug_json.
type CompressionColumnDebug struct {
	Name            string `json:"name"`
	Type            string `json:"type"`
	Descriptor      string `json:"descriptor"`
	Offset          int    `json:"offset"`
	PhysicalLength  int    `json:"physical_length"`
	LogicalLength   int    `json:"logical_length"`
	CompressedHex   string `json:"compressed_hex,omitempty"`
	UncompressedHex string `json:"uncompressed_hex,omitempty"`
	Error           string `json:"error,omitempty"`
}

// CompressionDebugInfo is stored on partial/successful compressed decode events.
type CompressionDebugInfo struct {
	CompressionType      string                   `json:"compression_type"`
	Operation            string                   `json:"operation,omitempty"`
	Context              string                   `json:"context,omitempty"`
	PageID               string                   `json:"page_id,omitempty"`
	SlotID               *int                     `json:"slot_id,omitempty"`
	AllocUnitID          int64                    `json:"alloc_unit_id,omitempty"`
	PartitionID          int64                    `json:"partition_id,omitempty"`
	OffsetInRow          int                      `json:"offset_in_row,omitempty"`
	ModifySize           int                      `json:"modify_size,omitempty"`
	RowLog0Hex           string                   `json:"rowlog0_hex,omitempty"`
	RowLog1Hex           string                   `json:"rowlog1_hex,omitempty"`
	RowLog2Hex           string                   `json:"rowlog2_hex,omitempty"`
	RowLog3Hex           string                   `json:"rowlog3_hex,omitempty"`
	RowLog4Hex           string                   `json:"rowlog4_hex,omitempty"`
	RawLogRecordHex      string                   `json:"raw_log_record_hex,omitempty"`
	DescriptorNibbles    []string                 `json:"descriptor_nibbles,omitempty"`
	PayloadStart         int                      `json:"payload_start,omitempty"`
	TailColumns          []int                    `json:"tail_columns,omitempty"`
	PhysicalColumnCount  int                      `json:"physical_column_count,omitempty"`
	LogicalColumnCount   int                      `json:"logical_column_count,omitempty"`
	UsingPhysicalColumns bool                     `json:"using_physical_columns,omitempty"`
	Columns              []CompressionColumnDebug `json:"columns,omitempty"`
	Warning              string                   `json:"warning,omitempty"`
}

// DecodedRow holds decoded column values plus optional compression diagnostics.
type DecodedRow struct {
	Values           []*rowdecoder.Value
	CompressionDebug CompressionDebugInfo
}

// CompressedRowDecoder decodes ROW/PAGE compressed row images from transaction logs.
type CompressedRowDecoder interface {
	Decode(row []byte, table *schema.Table, compression CompressionType) (DecodedRow, error)
}

// RowCompressionAffectsStorage lists types whose logical length stays fixed under row compression.
var RowCompressionAffectsStorage = map[string]bool{
	"smallint":       true,
	"int":            true,
	"bigint":         true,
	"decimal":        true,
	"numeric":        true,
	"bit":            true,
	"smallmoney":     true,
	"money":          true,
	"float":          true,
	"real":           true,
	"datetime":       true,
	"datetime2":      true,
	"datetimeoffset": true,
	"char":           true,
	"nchar":          true,
	"binary":         true,
	"timestamp":      true,
	"rowversion":     true,
}
