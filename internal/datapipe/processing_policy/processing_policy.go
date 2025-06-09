package processing_policy

import (
	"context"
	"time"

	"data_pipe/internal/database"
	"data_pipe/internal/types"

	"github.com/google/uuid"
)

// ApplyProcessingPolicy applies the processing policy based on its type
func ApplyProcessingPolicy(ctx context.Context, postgresDB *database.PostgresDB, nodeUUID string, value string, currentTime time.Time, policy types.ProcessingPolicyConfig) {
	switch policy.PolicyType {
	case types.ProcessingPolicyTypeLastValue:
		uuid, _ := uuid.Parse(nodeUUID)
		_ = postgresDB.UpdateUnitNodeState(ctx, uuid, value, currentTime)
	}
}
