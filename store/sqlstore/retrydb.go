package sqlstore

import (
	"context"
	"database/sql"

	"go.mau.fi/util/dbutil"
)

type RetryDB struct {
	*dbutil.Database
}

func (db *RetryDB) Exec(ctx context.Context, query string, args ...any) (res sql.Result, err error) {
	res, err = db.Execable(ctx).ExecContext(ctx, query, args...)
	return
}
func (db *RetryDB) PrepareContext(ctx context.Context, query string) (dbutil.LoggingStmt, error) {
	return db.Execable(ctx).PrepareContext(ctx, query)
}
