package scanner

import (
	"io"
)

type (
	Iterator interface {
		Next() error
	}

	IteratorFunc func() error
)

func (f IteratorFunc) Next() error {
	return f()
}

func LimitedIterator(maxRows int, next Iterator) Iterator {
	var read int
	return IteratorFunc(func() error {
		if read >= maxRows {
			return io.EOF
		}
		read++
		return next.Next()
	})
}
