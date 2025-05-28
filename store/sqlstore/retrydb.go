package sqlstore

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"go.mau.fi/util/dbutil"
)

type RetryDB struct {
	*dbutil.Database
}

func (db *RetryDB) Exec(ctx context.Context, query string, args ...any) (res sql.Result, err error) {
	for i := 0; i < 10; i++ {
		res, err = db.Execable(ctx).ExecContext(ctx, query, args...)
		if err == nil || strings.Contains(err.Error(), "constraint") {
			return
		}
		time.Sleep(time.Millisecond * 500)
	}
	return
}

func (db *RetryDB) PrepareContext(ctx context.Context, query string) (dbutil.LoggingStmt, error) {
	return db.Execable(ctx).PrepareContext(ctx, query)
}
