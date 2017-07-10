package documentdb

// Resource
type Resource struct {
	Id   string `json:"id,omitempty"`
	Self string `json:"_self,omitempty"`
	Etag string `json:"_etag,omitempty"`
	Rid  string `json:"_rid,omitempty"`
	Ts   int    `json:"_ts,omitempty"`
}

type IndexingMode string

const (
	Consistent = IndexingMode("Consistent")
	Lazy       = IndexingMode("Lazy")
)

// Indexing policy
type IndexingPolicy struct {
	IndexingMode IndexingMode   `json:"indexingMode,omitempty"`
	Automatic    bool           `json:"automatic"`
	Included     []IncludedPath `json:"includedPaths,omitempty"`
	Excluded     []ExcludedPath `json:"excludedPaths,omitempty"`
}

type DataType string

const (
	StringType     = DataType("String")
	NumberType     = DataType("Number")
	PointType      = DataType("Point")
	PolygonType    = DataType("Polygon")
	LineStringType = DataType("LineString")
)

type IndexKind string

const (
	Hash    = IndexKind("Hash")
	Range   = IndexKind("Range")
	Spatial = IndexKind("Spatial")
)

const MaxPrecision = -1

type Index struct {
	DataType  DataType  `json:"dataType,omitempty"`
	Kind      IndexKind `json:"kind,omitempty"`
	Precision int       `json:"precision,omitempty"`
}

type IncludedPath struct {
	Path    string  `json:"path"`
	Indexes []Index `json:"indexes,omitempty"`
}

type ExcludedPath struct {
	Path string `json:"path"`
}

// Database
type Database struct {
	Resource
	Colls string `json:"_colls,omitempty"`
	Users string `json:"_users,omitempty"`
}

// Collection
type Collection struct {
	Resource
	IndexingPolicy *IndexingPolicy `json:"indexingPolicy,omitempty"`
	Docs           string          `json:"_docs,omitempty"`
	Udf            string          `json:"_udfs,omitempty"`
	Sporcs         string          `json:"_sporcs,omitempty"`
	Triggers       string          `json:"_triggers,omitempty"`
	Conflicts      string          `json:"_conflicts,omitempty"`
}

// Document
type Document struct {
	Resource
	Attachments string `json:"attachments,omitempty"`
}

// Stored Procedure
type Sproc struct {
	Resource
	Body string `json:"body,omitempty"`
}

// User Defined Function
type UDF struct {
	Resource
	Body string `json:"body,omitempty"`
}
