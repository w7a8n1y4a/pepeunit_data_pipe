package filters

import (
	"data_pipe/internal/types"
	"strconv"
	"time"
)

// ApplyFilters applies filtering rules to the input value
func ApplyFilters(value string, config types.FiltersConfig, lastMessageTime time.Time, lastValue string) bool {
	// Apply filtering rules
	if config.TypeValueFiltering != nil {
		switch *config.TypeValueFiltering {
		case types.FilterTypeValueFilteringWhiteList:
			if !isInList(value, config.FilteringValues) {
				return false
			}
		case types.FilterTypeValueFilteringBlackList:
			if isInList(value, config.FilteringValues) {
				return false
			}
		}
	}

	// Apply threshold rules
	if config.TypeValueThreshold != nil {
		// Try to parse value as float64
		numValue, err := strconv.ParseFloat(value, 64)
		if err == nil {
			switch *config.TypeValueThreshold {
			case types.FilterTypeValueThresholdMin:
				if config.ThresholdMin != nil && numValue < float64(*config.ThresholdMin) {
					return false
				}
			case types.FilterTypeValueThresholdMax:
				if config.ThresholdMax != nil && numValue > float64(*config.ThresholdMax) {
					return false
				}
			case types.FilterTypeValueThresholdRange:
				if (config.ThresholdMin != nil && numValue < float64(*config.ThresholdMin)) ||
					(config.ThresholdMax != nil && numValue > float64(*config.ThresholdMax)) {
					return false
				}
			}
		}
	}

	// Apply rate limiting
	if config.MaxRate > 0 {
		timeSinceLastMessage := time.Since(lastMessageTime)
		if timeSinceLastMessage < time.Duration(config.MaxRate)*time.Second {
			return false
		}
	}

	// Apply size limit
	if config.MaxSize > 0 && len(value) > config.MaxSize {
		return false
	}

	// Apply uniqueness check
	if config.LastUniqueCheck && value == lastValue {
		return false
	}

	return true
}

// isInList checks if a value is in the list of values
func isInList(value string, list []interface{}) bool {
	for _, v := range list {
		switch v := v.(type) {
		case string:
			if v == value {
				return true
			}
		case float64:
			if num, err := strconv.ParseFloat(value, 64); err == nil && num == v {
				return true
			}
		}
	}
	return false
}
