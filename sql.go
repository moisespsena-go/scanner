package scanner

import (
	"database/sql"
	"io"
)

func SqlRowsLoop(rows *sql.Rows, it Iterator, factory RowFactory, cb func(record any) error) (err error) {
	if it == nil {
		it = SqlRowsIterator(rows)
	}
	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return
	}
	do := factory.Factory(columns)
	return Loop(rows, columns, it, do, cb)
}

func SqlRowsIterator(rows *sql.Rows) Iterator {
	return IteratorFunc(func() error {
		if !rows.Next() {
			return io.EOF
		}
		return nil
	})
}
