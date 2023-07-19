package scanner

type (
	Scanner interface {
		Scan(values ...any) error
	}

	ScannerFunc func(values ...any) error

	RowScanner struct {
		rowFactories []RowFactory
	}

	RowScanContext struct {
		Columns []string
		Values  []any
		Value   any
	}

	RowInitializer interface {
		InitRow(columns []string, dst []any) (record func(dst []any) (_ any, err error), err error)
		Pipe(to func(next RowInitializer) RowInitializer) RowInitializer
	}

	RowInitializerFunc func(columns []string, dst []any) (record func(dst []any) (_ any, err error), err error)

	RowFactory interface {
		Factory(columns []string) RowInitializer
	}

	RowFactoryFunc func(columns []string) RowInitializer

	RowsHandler interface {
		NewInitializer(columns []string) RowInitializer
		Pipe(to func(next RowsHandler) RowsHandler) RowsHandler
	}

	RowsHandlerFunc func(columns []string) RowInitializer
)

func (f ScannerFunc) Scan(values ...any) error {
	return f(values...)
}

func (f RowsHandlerFunc) Pipe(to func(next RowsHandler) RowsHandler) RowsHandler {
	return RowsHandlerFunc(func(columns []string) RowInitializer {
		return to(f).NewInitializer(columns)
	})
}

func (f RowsHandlerFunc) NewInitializer(columns []string) RowInitializer {
	return f(columns)
}

var RawRowsHandler = RowsHandlerFunc(func([]string) RowInitializer {
	return RowInitializerFunc(func(_ []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
		RawRowScanner(dst)
		return func(dst []any) (_ any, err error) {
			return dst, nil
		}, nil
	})
})

type RawRowScann struct {
}

func (RawRowScann) Row(_ []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
	for i := range dst {
		dst[i] = rawValueScanner{}
	}
	return func(dst []any) (_ any, err error) {
		return dst, nil
	}, nil
}

func (f RowInitializerFunc) InitRow(columns []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
	return f(columns, dst)
}

func (f RowInitializerFunc) Pipe(to func(next RowInitializer) RowInitializer) RowInitializer {
	return RowInitializerFunc(func(columns []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
		return to(f).InitRow(columns, dst)
	})
}

func (f RowFactoryFunc) Factory(columns []string) RowInitializer {
	return f(columns)
}
