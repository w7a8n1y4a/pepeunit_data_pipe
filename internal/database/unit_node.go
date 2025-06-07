package database

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

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

func (db *DB) GetUnitNodeByUUID(ctx context.Context, uuid uuid.UUID) (*UnitNode, error) {
	query := `
		SELECT uuid, type, visibility_level, is_rewritable_input, topic_name,
			create_datetime, state, last_update_datetime, is_data_pipe_active,
			data_pipe_yml, data_pipe_status, data_pipe_error, creator_uuid, unit_uuid
		FROM units_nodes
		WHERE uuid = $1`

	node := &UnitNode{}
	err := db.QueryRow(ctx, query, uuid).Scan(
		&node.UUID, &node.Type, &node.VisibilityLevel, &node.IsRewritableInput,
		&node.TopicName, &node.CreateDatetime, &node.State, &node.LastUpdateDatetime,
		&node.IsDataPipeActive, &node.DataPipeYML, &node.DataPipeStatus,
		&node.DataPipeError, &node.CreatorUUID, &node.UnitUUID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get unit node: %w", err)
	}

	return node, nil
}

func (db *DB) GetUnitNodesByUnitUUID(ctx context.Context, unitUUID uuid.UUID) ([]*UnitNode, error) {
	query := `
		SELECT uuid, type, visibility_level, is_rewritable_input, topic_name,
			create_datetime, state, last_update_datetime, is_data_pipe_active,
			data_pipe_yml, data_pipe_status, data_pipe_error, creator_uuid, unit_uuid
		FROM units_nodes
		WHERE unit_uuid = $1`

	rows, err := db.Query(ctx, query, unitUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to query unit nodes: %w", err)
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

func (db *DB) GetActiveUnitNodes(ctx context.Context) ([]*UnitNode, error) {
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

func (db *DB) GetUnitNodeByTopicName(ctx context.Context, topicName string) (*UnitNode, error) {
	query := `
		SELECT uuid, type, visibility_level, is_rewritable_input, topic_name,
			create_datetime, state, last_update_datetime, is_data_pipe_active,
			data_pipe_yml, data_pipe_status, data_pipe_error, creator_uuid, unit_uuid
		FROM units_nodes
		WHERE topic_name = $1`

	node := &UnitNode{}
	err := db.QueryRow(ctx, query, topicName).Scan(
		&node.UUID, &node.Type, &node.VisibilityLevel, &node.IsRewritableInput,
		&node.TopicName, &node.CreateDatetime, &node.State, &node.LastUpdateDatetime,
		&node.IsDataPipeActive, &node.DataPipeYML, &node.DataPipeStatus,
		&node.DataPipeError, &node.CreatorUUID, &node.UnitUUID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get unit node by topic name: %w", err)
	}

	return node, nil
}

func (db *DB) UpdateUnitNodeStatus(ctx context.Context, uuid uuid.UUID, status DataPipeStatus, errorMsg *string) error {
	query := `
		UPDATE units_nodes
		SET data_pipe_status = $1, data_pipe_error = $2
		WHERE uuid = $3`

	_, err := db.pool.Exec(ctx, query, string(status), errorMsg, uuid)
	if err != nil {
		return fmt.Errorf("failed to update unit node status: %w", err)
	}

	return nil
}

func (db *DB) UpdateUnitNodeState(ctx context.Context, uuid uuid.UUID, state string, updateTime time.Time) error {
	query := `
		UPDATE units_nodes
		SET state = $1, last_update_datetime = $2
		WHERE uuid = $3`

	_, err := db.pool.Exec(ctx, query, state, updateTime, uuid)
	if err != nil {
		return fmt.Errorf("failed to update unit node state: %w", err)
	}

	return nil
}
