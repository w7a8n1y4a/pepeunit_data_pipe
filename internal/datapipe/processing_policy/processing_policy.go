package processing_policy

import (
	"context"
	"fmt"
	"time"

	"data_pipe/internal/types"

	"github.com/google/uuid"
)

// ProcessingPolicy handles message processing based on policy type
type ProcessingPolicy struct {
	bufferFactory *BufferFactory
}

// NewProcessingPolicy creates a new processing policy handler
func NewProcessingPolicy(bufferFactory *BufferFactory) *ProcessingPolicy {
	return &ProcessingPolicy{
		bufferFactory: bufferFactory,
	}
}

// ApplyProcessingPolicy applies the processing policy based on its type
func (p *ProcessingPolicy) ApplyProcessingPolicy(
	ctx context.Context,
	nodeUUID uuid.UUID,
	value string,
	currentTime time.Time,
	policy types.ProcessingPolicyConfig,
) error {
	buffer := p.bufferFactory.GetBuffer(policy.PolicyType)
	if buffer == nil {
		return fmt.Errorf("no buffer available for policy type: %s", policy.PolicyType)
	}

	switch policy.PolicyType {
	case types.ProcessingPolicyTypeLastValue:
		return buffer.Add(ctx, nodeUUID, value, currentTime)

	case types.ProcessingPolicyTypeNRecords:
		// TODO: Implement NRecords policy
		return fmt.Errorf("NRecords policy not implemented")

	case types.ProcessingPolicyTypeTimeWindow:
		// TODO: Implement TimeWindow policy
		return fmt.Errorf("TimeWindow policy not implemented")

	case types.ProcessingPolicyTypeAggregation:
		// TODO: Implement Aggregation policy
		return fmt.Errorf("Aggregation policy not implemented")

	default:
		return fmt.Errorf("unknown processing policy type: %s", policy.PolicyType)
	}
}
