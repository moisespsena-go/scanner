package scanner

import (
	"reflect"
	"strings"
)

type (
	RecordMap map[string]any
	Map       struct {
		next             RowsHandler
		types            RecordMap
		ignoreUndefineds bool
	}
	recordMapHandlerTypeElem struct {
		index   int
		typ     reflect.Type
		handler func(v any) (any, error)
	}
)

func (m RecordMap) Set(key string, val any) {
	var (
		self = m
		keys = strings.Split(key, "__")
		subI any
		sub  map[string]any
		ok   bool
	)
	for _, k := range keys[:len(keys)-1] {
		if subI, ok = self[k]; !ok {
			sub = make(map[string]any)
			self[k] = sub
		} else if sub, ok = subI.(map[string]any); !ok {
			sub = make(map[string]any)
			self[k] = sub
		}
		self = sub
	}
	self[keys[len(keys)-1]] = val
}

func (m RecordMap) SetIfExists(key string, val any) {
	var (
		self = m
		keys = strings.Split(key, "__")
		subI any
		sub  map[string]any
		ok   bool
	)
	for _, k := range keys[:len(keys)-1] {
		if subI, ok = self[k]; !ok {
			return
		} else if sub, ok = subI.(map[string]any); !ok {
			return
		}
		self = sub
	}

	key = keys[len(keys)-1]
	if _, ok = self[key]; !ok {
		return
	}
	self[key] = val
}

func (m RecordMap) Get(key string) (val any, ok bool) {
	return m.GetDefault(key, nil)
}

func (m RecordMap) GetDefault(key string, defaul any) (val any, ok bool) {
	var (
		self = m
		keys = strings.Split(key, "__")
		subI any
		sub  map[string]any
	)
	for _, k := range keys[:len(keys)-1] {
		if subI, ok = self[k]; !ok {
			val = defaul
			return
		} else if sub, ok = subI.(map[string]any); !ok {
			val = defaul
			return
		}
		self = sub
	}
	val, ok = self[keys[len(keys)-1]]
	return
}

func (m RecordMap) Walk(cb func(key string, val any)) {
	m.walk(".", "", cb)
}

func (m RecordMap) WalkSep(sep string, cb func(key string, val any)) {
	m.walk(sep, "", cb)
}

func (m RecordMap) walk(sep, prefix string, cb func(key string, val any)) {
	for k, val := range m {
		switch t := val.(type) {
		case RecordMap:
			t.walk(sep, prefix+k+sep, cb)
		default:
			cb(prefix+k, val)
		}
	}
}

func (m RecordMap) Map(cb func(key string, val any) any) {
	for k, val := range m {
		switch t := val.(type) {
		case RecordMap:
			t.Map(cb)
		default:
			m[k] = cb(k, val)
		}
	}
}

func (r *Map) Types() RecordMap {
	return r.types
}

func (r *Map) WithTypes(types RecordMap) *Map {
	r.types = types
	return r
}

func (r *Map) IsIgnoreUndefineds() bool {
	return r.ignoreUndefineds
}

func (r *Map) SetIgnoreUndefineds(ignoreUndefineds bool) *Map {
	r.ignoreUndefineds = ignoreUndefineds
	return r
}

func (r *Map) IgnoreUndefineds() *Map {
	r.ignoreUndefineds = true
	return r
}

func (s *Map) Factory(columns []string) RowInitializer {
	return s.NewInitializer(columns)
}

func (r *Map) NewInitializer(columns []string) RowInitializer {
	if r.types == nil {
		return r.initilizerWitoutTypes(columns)
	}
	return r.initilizerWithTypes(columns)
}

func (r *Map) Pipe(to func(next RowsHandler) RowsHandler) RowsHandler {
	return RowsHandlerFunc(func(columns []string) RowInitializer {
		return to(r).NewInitializer(columns)
	})
}

func (r *Map) initilizerWithTypesRowHandlerIgnoreFields(typesByName map[string]recordMapHandlerTypeElem,
	dst []any) (_ any, err error) {
	var recordMap = make(RecordMap)
	for key, elem := range typesByName {
		if elem.handler != nil {
			var val = dst[elem.index]
			if val, err = elem.handler(val); err != nil {
				return
			}
			recordMap.Set(key, val)
		} else {
			recordMap.Set(key, reflect.ValueOf(dst[elem.index]).Elem().Interface())
		}
	}
	return recordMap, nil
}

func (r *Map) initilizerWithTypesRowHandlerNoIgnoreFields(typesByName map[string]recordMapHandlerTypeElem,
	columns []string, dst []any) (_ any, err error) {
	var recordMap = make(RecordMap)
	for i, key := range columns {
		if elem, ok := typesByName[key]; ok {
			if elem.handler != nil {
				var val = dst[elem.index]
				if val, err = elem.handler(val); err != nil {
					return
				}
				recordMap.Set(key, val)
			} else {
				recordMap.Set(key, reflect.ValueOf(dst[elem.index]).Elem().Interface())
			}
		} else {
			recordMap.Set(key, dst[i])
		}
	}
	return recordMap, nil
}

func (r *Map) initilizerWithTypesRowHandler(typesByName map[string]recordMapHandlerTypeElem,
	iz RowInitializer, columns []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
	if _, err = iz.InitRow(columns, dst); err != nil {
		return
	}

	for i, key := range columns {
		if val, ok := typesByName[key]; ok {
			if val.handler == nil {
				dst[i] = reflect.New(val.typ).Interface()
			}
		}
	}

	if r.ignoreUndefineds {
		return func(dst []any) (any, error) {
			return r.initilizerWithTypesRowHandlerIgnoreFields(typesByName, dst)
		}, nil
	}
	return func(dst []any) (any, error) {
		return r.initilizerWithTypesRowHandlerNoIgnoreFields(typesByName, columns, dst)
	}, nil
}

func (r *Map) initilizerWithTypes(columns []string) RowInitializer {
	var typesByName = make(map[string]recordMapHandlerTypeElem, len(columns))
	for i, key := range columns {
		if val, ok := r.types.Get(key); ok {
			switch t := val.(type) {
			case reflect.Type:
				typesByName[key] = recordMapHandlerTypeElem{i, t, nil}
			case func(v any) (any, error):
				typesByName[key] = recordMapHandlerTypeElem{i, nil, t}
			default:
				typesByName[key] = recordMapHandlerTypeElem{i, reflect.TypeOf(t), nil}
			}
		}
	}

	iz := r.next.NewInitializer(columns)
	return RowInitializerFunc(func(columns []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
		return r.initilizerWithTypesRowHandler(typesByName, iz, columns, dst)
	})
}

func (r *Map) initilizerWitoutTypes(columns []string) RowInitializer {
	iz := r.next.NewInitializer(columns)
	return RowInitializerFunc(func(columns []string, dst []any) (record func(dst []any) (_ any, err error), err error) {
		if _, err = iz.InitRow(columns, dst); err != nil {
			return
		}
		return func(dst []any) (_ any, err error) {
			var recordMap = make(RecordMap)
			for i, key := range columns {
				recordMap.Set(key, dst[i])
			}
			return recordMap, nil
		}, nil
	})
}

func NewRecordMapHandler(next RowsHandler) *Map {
	return &Map{next: next}
}
