package scanner

import "database/sql"

type (
	rawValueScanner struct {
		dst   []any
		index int
	}

	discardValue struct{}
)

func DiscardValuer() sql.Scanner {
	return discardValue{}
}

func (discardValue) Scan(any) error {
	return nil
}

func RawValueScanner(dst []any, index int) sql.Scanner {
	return &rawValueScanner{dst: dst, index: index}
}

func RawRowScanner(dst []any) {
	for i := range dst {
		if dst[i] == nil {
			dst[i] = &rawValueScanner{dst, i}
		}
	}
}

func (s *rawValueScanner) Scan(src any) error {
	s.dst[s.index] = src
	return nil
}
