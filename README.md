# Scanner
Scanner is utils for scan data of slice values

Example:

```go
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/moisespsena-go/scanner"
)

func main() {
	config, err := pgx.ParseConfig("postgres://postgres:1@127.0.0.1:5432/cp-prod")
	if err != nil {
		panic(err)
	}

	conn := stdlib.OpenDB(*config)
	defer conn.Close()

	var rows *sql.Rows
	if rows, err = conn.Query("SELECT generate_series as user__count FROM generate_series(1, 5)"); err != nil {
		panic(err)
	}

	defer rows.Close()

	fmt.Println(
		scanner.SqlRowsLoop(rows,
			scanner.LimitedIterator(
				2,
				scanner.SqlRowsIterator(rows),
			),
			scanner.NewSlice(
				scanner.NewRecordMapHandler(scanner.RawRowsHandler).
					WithTypes(map[string]any{
						"user": map[string]any{
							"count": 0,
						},
					}),
				0,
				1),
			json.NewEncoder(os.Stdout).Encode,
		),
	)
}
```

Outputs:

    {"user":{"count":1}}
    {"user":{"count":2}}
    <nil>
