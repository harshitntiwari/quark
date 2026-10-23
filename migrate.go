package quark

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func runMigrations(ctx context.Context, tx pgx.Tx) error {

	// CREATE migrations table if not already created
	_, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS quark_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW());")
	if err != nil {
		return fmt.Errorf("error creating migrations table: %w", err)
	}

	// READ the migration files
	files, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("error reading migration files: %w", err)
	}

	for _, f := range files {
		file, err := fs.ReadFile(migrationsFS, path.Join("migrations", f.Name()))
		if err != nil {
			return fmt.Errorf("failed to read migration file %s : %w", f.Name(), err)
		}
		// skip if the migration has already been applied
		var exists bool
		err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM quark_migrations WHERE version =$1)", f.Name()).Scan(&exists)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		// apply migration
		// fmt.Printf("Executing migration: %s", f.Name())
		_, err = tx.Exec(ctx, string(file))
		if err != nil {
			return fmt.Errorf("migration execution failed for %s: %w", f.Name(), err)
		}
		// record in migrations table
		_, err = tx.Exec(ctx, "INSERT INTO quark_migrations (version) VALUES ($1)", f.Name())
		if err != nil {
			return fmt.Errorf("failed to record migration tracking row: %w", err)
		}
	}
	return nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// ACQUIRE transaction level lock
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(72839401);"); err != nil {
			return fmt.Errorf("error acquiring migration lock: %w", err)
		}
		return runMigrations(ctx, tx)
	})
	if err != nil {
		return err
	}
	return nil
}
