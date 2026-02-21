package bridge

// BanquetDTO mirrors the key fields from banquet.Banquet for transport across FFI.
// We avoid complex types like *url.URL and use basic types (string, []string).
type BanquetDTO struct {
	Where         string
	Table         string
	Select        []string
	SortDirection string
	Limit         string
	Offset        string
	GroupBy       string
	Having        string
	OrderBy       string
	DataSetPath   string
	ColumnPath    string
	OriginalURL   string
}
