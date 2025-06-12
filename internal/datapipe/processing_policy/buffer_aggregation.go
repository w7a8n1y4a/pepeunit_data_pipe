package processing_policy

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"data_pipe/internal/database"
	"data_pipe/internal/types"

	"github.com/google/uuid"
)

// AggregationEntry represents a single entry in the aggregation buffer
type AggregationEntry struct {
	State           float64
	UpdateTime      time.Time
	AggregationType string
}

// AggregationBuffer accumulates messages and periodically flushes them to the database
type AggregationBuffer struct {
	mu            sync.Mutex
	updates       map[uuid.UUID][]AggregationEntry
	flushInterval time.Duration
	maxSize       int
	clickhouseDB  *database.ClickHouseDB
	stopCh        chan struct{}
	policy        *types.ProcessingPolicyConfig
}

// NewAggregationBuffer creates a new aggregation buffer
func NewAggregationBuffer(clickhouseDB *database.ClickHouseDB, flushInterval time.Duration, maxSize int) *AggregationBuffer {
	return &AggregationBuffer{
		updates:       make(map[uuid.UUID][]AggregationEntry),
		flushInterval: flushInterval,
		maxSize:       maxSize,
		clickhouseDB:  clickhouseDB,
		stopCh:        make(chan struct{}),
	}
}

// Start begins the periodic flushing of the buffer
func (b *AggregationBuffer) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(b.flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := b.Flush(ctx); err != nil {
					// TODO: Add proper error handling/logging
					fmt.Printf("Failed to flush buffer: %v\n", err)
				}
			case <-b.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop stops the buffer's periodic flushing
func (b *AggregationBuffer) Stop() {
	close(b.stopCh)
}

// Add adds a message to the buffer
func (b *AggregationBuffer) Add(ctx context.Context, uuid uuid.UUID, state string, updateTime time.Time) error {
	// Get the policy config from the context
	policy, ok := ctx.Value("policy").(types.ProcessingPolicyConfig)
	if !ok {
		return fmt.Errorf("policy config not found in context")
	}

	if policy.AggregationFunctions == nil {
		return fmt.Errorf("aggregation functions not specified in policy")
	}

	if policy.TimeWindowSize == nil {
		return fmt.Errorf("time window size not specified in policy")
	}

	// Store policy in buffer
	b.mu.Lock()
	b.policy = &policy
	b.mu.Unlock()

	// Convert state string to float64
	stateFloat, err := strconv.ParseFloat(state, 64)
	if err != nil {
		return fmt.Errorf("failed to parse state as float64: %w", err)
	}

	// Create new entry
	entry := AggregationEntry{
		State:           stateFloat,
		UpdateTime:      updateTime,
		AggregationType: string(*policy.AggregationFunctions),
	}

	// Check if we need to flush before adding new entry
	shouldFlush := false
	b.mu.Lock()
	// Append to the node's entries
	b.updates[uuid] = append(b.updates[uuid], entry)

	// Check if we need to flush based on total number of entries across all nodes
	totalEntries := 0
	for _, entries := range b.updates {
		totalEntries += len(entries)
	}

	if totalEntries >= b.maxSize {
		shouldFlush = true
	}
	b.mu.Unlock()

	if shouldFlush {
		return b.Flush(ctx)
	}

	return nil
}

// Flush writes all buffered messages to the database
func (b *AggregationBuffer) Flush(ctx context.Context) error {
	// Create a copy of updates to work with
	var updatesCopy map[uuid.UUID][]AggregationEntry
	b.mu.Lock()
	if len(b.updates) == 0 {
		b.mu.Unlock()
		return nil
	}
	updatesCopy = make(map[uuid.UUID][]AggregationEntry, len(b.updates))
	for k, v := range b.updates {
		updatesCopy[k] = make([]AggregationEntry, len(v))
		copy(updatesCopy[k], v)
	}
	// Clear the original updates
	b.updates = make(map[uuid.UUID][]AggregationEntry)
	b.mu.Unlock()

	// Count total entries for logging
	totalEntries := 0
	for _, entries := range updatesCopy {
		totalEntries += len(entries)
	}
	log.Printf("Sending %d aggregation entries to ClickHouse", totalEntries)

	// Get policy from buffer
	b.mu.Lock()
	policy := b.policy
	b.mu.Unlock()

	if policy == nil || policy.TimeWindowSize == nil {
		return fmt.Errorf("time window size not specified in policy")
	}

	// Convert updates to AggregationEntry slice
	entries := make([]database.AggregationEntry, 0, totalEntries)
	for nodeUUID, nodeEntries := range updatesCopy {
		for _, entry := range nodeEntries {
			// Calculate window start and end times
			windowStart := entry.UpdateTime.Add(-time.Duration(*policy.TimeWindowSize) * time.Second)
			windowEnd := entry.UpdateTime

			entries = append(entries, database.AggregationEntry{
				UUID:                uuid.New(), // Generate new UUID for each entry
				UnitNodeUUID:        nodeUUID,   // Use the node UUID
				State:               entry.State,
				AggregationType:     entry.AggregationType,
				CreateDateTime:      entry.UpdateTime,
				StartWindowDateTime: windowStart,
				EndWindowDateTime:   windowEnd,
			})
		}
	}

	if err := b.clickhouseDB.BulkCreateAggregationEntries(ctx, entries); err != nil {
		return fmt.Errorf("failed to bulk create aggregation entries: %w", err)
	}

	return nil
}
