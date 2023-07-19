package scanner

type (
	Slice struct {
		Next          RowsHandler
		Start, Length int
	}
)

func (s *Slice) Factory(columns []string) RowInitializer {
	return s.NewInitializer(columns)
}

func NewSlice(next RowsHandler, start int, length int) *Slice {
	return &Slice{Next: next, Start: start, Length: length}
}

func (s *Slice) NewInitializer(columns []string) RowInitializer {
	iz := s.Next.NewInitializer(columns[s.Start : s.Start+s.Length])
	return RowInitializerFunc(func(columns []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
		var (
			i int
			l = len(dst)
		)

		for ; i < s.Start; i++ {
			dst[i] = DiscardValuer()
		}

		for i = s.Start + s.Length; i < l; i++ {
			dst[i] = DiscardValuer()
		}

		var record_ func(dst []any) (_ any, err error)
		if record_, err = iz.InitRow(columns[s.Start:s.Start+s.Length], dst[s.Start:s.Start+s.Length]); err != nil {
			return
		}
		record = func(dst []any) (_ any, err error) {
			for ; i < s.Start; i++ {
				dst[i] = nil
			}

			for i = s.Start + s.Length; i < l; i++ {
				dst[i] = nil
			}

			return record_(dst[s.Start : s.Start+s.Length])
		}
		return
	})
}

func (s *Slice) Pipe(to func(next RowsHandler) RowsHandler) RowsHandler {
	return RowsHandlerFunc(func(columns []string) RowInitializer {
		return to(s).NewInitializer(columns)
	})
}
