package filters

import (
	"fmt"
	"strconv"

	"data_pipe/internal/types"
)

// ApplyFilters applies filtering rules to the input value
func ApplyFilters(value interface{}, config types.FiltersConfig) (bool, error) {
	// Convert input value based on type
	var stringValue string
	var numericValue *float64

	switch v := value.(type) {
	case string:
		stringValue = v
		if config.TypeInputValue == types.TypeInputValueNumber {
			val, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return false, fmt.Errorf("failed to parse numeric value: %w", err)
			}
			numericValue = &val
		}
	case float64:
		numericValue = &v
		stringValue = strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		f := float64(v)
		numericValue = &f
		stringValue = strconv.Itoa(v)
	case int64:
		f := float64(v)
		numericValue = &f
		stringValue = strconv.FormatInt(v, 10)
	default:
		return false, fmt.Errorf("unsupported value type: %T", value)
	}

	// Apply filtering rules
	if config.TypeValueFiltering != nil {
		switch *config.TypeValueFiltering {
		case types.FilterTypeValueFilteringWhiteList:
			if !isInList(stringValue, config.FilteringValues) {
				return false, nil
			}
		case types.FilterTypeValueFilteringBlackList:
			if isInList(stringValue, config.FilteringValues) {
				return false, nil
			}
		}
	}

	// Apply threshold rules for numeric values
	if numericValue != nil && config.TypeValueThreshold != nil {
		switch *config.TypeValueThreshold {
		case types.FilterTypeValueThresholdMin:
			if config.ThresholdMin != nil && *numericValue < float64(*config.ThresholdMin) {
				return false, nil
			}
		case types.FilterTypeValueThresholdMax:
			if config.ThresholdMax != nil && *numericValue > float64(*config.ThresholdMax) {
				return false, nil
			}
		case types.FilterTypeValueThresholdRange:
			if (config.ThresholdMin != nil && *numericValue < float64(*config.ThresholdMin)) ||
				(config.ThresholdMax != nil && *numericValue > float64(*config.ThresholdMax)) {
				return false, nil
			}
		}
	}

	// Apply rate limiting
	if config.MaxRate > 0 {
		// TODO: Implement rate limiting logic
	}

	// Apply size limit
	if config.MaxSize > 0 && len(stringValue) > config.MaxSize {
		return false, nil
	}

	// Apply uniqueness check
	if config.LastUniqueCheck {
		// TODO: Implement uniqueness check logic
	}

	return true, nil
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
		case int:
			if num, err := strconv.Atoi(value); err == nil && num == v {
				return true
			}
		}
	}
	return false
}
