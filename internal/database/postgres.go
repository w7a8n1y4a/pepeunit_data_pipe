package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDB struct {
	pool *pgxpool.Pool
}

func NewPostgres(connString string) (*PostgresDB, error) {
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &PostgresDB{pool: pool}, nil
}

func (db *PostgresDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

func (db *PostgresDB) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	return db.pool.Query(ctx, query, args...)
}

func (db *PostgresDB) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	return db.pool.QueryRow(ctx, query, args...)
}

func (db *PostgresDB) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := db.pool.Exec(ctx, query, args...)
	return err
}

type DataPipeStatus string

const (
	DataPipeStatusActive   DataPipeStatus = "Active"
	DataPipeStatusInactive DataPipeStatus = "Inactive"
	DataPipeStatusError    DataPipeStatus = "Error"
)

type UnitNode struct {
	UUID               uuid.UUID `db:"uuid"`
	Type               string    `db:"type"`
	VisibilityLevel    string    `db:"visibility_level"`
	IsRewritableInput  bool      `db:"is_rewritable_input"`
	TopicName          string    `db:"topic_name"`
	CreateDatetime     time.Time `db:"create_datetime"`
	State              *string   `db:"state"`
	LastUpdateDatetime time.Time `db:"last_update_datetime"`
	IsDataPipeActive   bool      `db:"is_data_pipe_active"`
	DataPipeYML        *string   `db:"data_pipe_yml"`
	DataPipeStatus     *string   `db:"data_pipe_status"`
	DataPipeError      *string   `db:"data_pipe_error"`
	CreatorUUID        uuid.UUID `db:"creator_uuid"`
	UnitUUID           uuid.UUID `db:"unit_uuid"`
}

// GetActiveUnitNodes returns all active unit nodes
func (db *PostgresDB) GetActiveUnitNodes(ctx context.Context) ([]*UnitNode, error) {
	query := `
		SELECT uuid, type, visibility_level, is_rewritable_input, topic_name,
			create_datetime, state, last_update_datetime, is_data_pipe_active,
			data_pipe_yml, data_pipe_status, data_pipe_error, creator_uuid, unit_uuid
		FROM units_nodes
		WHERE is_data_pipe_active = true`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active unit nodes: %w", err)
	}
	defer rows.Close()

	var nodes []*UnitNode
	for rows.Next() {
		node := &UnitNode{}
		if err := rows.Scan(
			&node.UUID, &node.Type, &node.VisibilityLevel, &node.IsRewritableInput,
			&node.TopicName, &node.CreateDatetime, &node.State, &node.LastUpdateDatetime,
			&node.IsDataPipeActive, &node.DataPipeYML, &node.DataPipeStatus,
			&node.DataPipeError, &node.CreatorUUID, &node.UnitUUID,
		); err != nil {
			return nil, fmt.Errorf("failed to scan unit node: %w", err)
		}
		nodes = append(nodes, node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating unit nodes: %w", err)
	}

	return nodes, nil
}

// BulkUpdateUnitNodeStates updates multiple unit node states in a single query
func (db *PostgresDB) BulkUpdateUnitNodeStates(ctx context.Context, updates map[uuid.UUID]struct {
	State      string
	UpdateTime time.Time
}) error {
	if len(updates) == 0 {
		return nil
	}

	query := `
		UPDATE units_nodes AS t SET
			state = c.state,
			last_update_datetime = c.last_update_datetime::timestamp
		FROM (VALUES 
	`

	values := make([]interface{}, 0, len(updates)*3)
	valueStrings := make([]string, 0, len(updates))

	i := 1
	for uuid, update := range updates {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d)", i, i+1, i+2))
		// Format time as 'YYYY-MM-DD HH:MM:SS' with explicit timestamp cast
		formattedTime := update.UpdateTime.Format("2006-01-02 15:04:05")
		values = append(values, uuid, update.State, formattedTime)
		i += 3
	}

	query += strings.Join(valueStrings, ",")
	query += `
		) AS c(uuid, state, last_update_datetime)
		WHERE t.uuid = c.uuid::uuid
	`

	_, err := db.pool.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to bulk update unit node states: %w", err)
	}

	return nil
}
