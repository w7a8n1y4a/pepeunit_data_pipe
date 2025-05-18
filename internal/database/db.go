package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB представляет собой обертку над пулом соединений PostgreSQL
type DB struct {
	pool *pgxpool.Pool
}

// New создает новое подключение к базе данных
func New(connString string) (*DB, error) {
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Проверяем соединение
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &DB{pool: pool}, nil
}

// Close закрывает пул соединений
func (db *DB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// Query выполняет запрос и возвращает результат в виде строк
func (db *DB) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	return db.pool.Query(ctx, query, args...)
}

// QueryRow выполняет запрос и возвращает одну строку
func (db *DB) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	return db.pool.QueryRow(ctx, query, args...)
}

// Exec выполняет запрос без возврата результата
func (db *DB) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := db.pool.Exec(ctx, query, args...)
	return err
}
