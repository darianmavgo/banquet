package bridge

import (
	"github.com/darianmavgo/banquet"
)

// Parse takes a raw URL string and returns a parsed BanquetDTO.
func Parse(rawURL string) (*BanquetDTO, error) {
	b, err := banquet.ParseBanquet(rawURL)
	if err != nil {
		return nil, err
	}

	return &BanquetDTO{
		Where:         b.Where,
		Table:         b.Table,
		Select:        b.Select,
		SortDirection: b.SortDirection,
		Limit:         b.Limit,
		Offset:        b.Offset,
		GroupBy:       b.GroupBy,
		Having:        b.Having,
		OrderBy:       b.OrderBy,
		DataSetPath:   b.DataSetPath,
		ColumnPath:    b.ColumnPath,
		OriginalURL:   b.String(),
	}, nil
}

func Ping() string {
	return "pong"
}
