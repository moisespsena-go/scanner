package scanner

import "io"

func Loop(scanner Scanner, columns []string, it Iterator, initializer RowInitializer, cb func(record any) error) (err error) {
	var (
		values   []any
		recorder func([]any) (_ any, err error)
		record   any
	)

	for {
		if err = it.Next(); err != nil {
			if err == io.EOF {
				return nil
			}
			return
		}

		values = make([]any, len(columns))

		if recorder, err = initializer.InitRow(columns, values); err != nil {
			return
		}

		if err = scanner.Scan(values...); err != nil {
			return
		}

		if record, err = recorder(values); err != nil {
			return
		}

		if err = cb(record); err != nil {
			if err == Break {
				err = nil
			}
			return
		}
	}
}
