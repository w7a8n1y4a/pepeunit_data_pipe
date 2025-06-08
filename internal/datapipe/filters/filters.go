package filters

import (
	"data_pipe/internal/types"
	"fmt"
	"strconv"
	"strings"
)

// ApplyFilters applies filtering rules to the input value
func ApplyFilters(value string, config types.FiltersConfig) bool {
	var numericValue *float64

	value = strings.Replace(value, ",", ".", -1)

	if config.TypeInputValue == types.TypeInputValueNumber {
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return false
		}
		numericValue = &val
	}

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

	// Apply threshold rules for numeric values
	if config.TypeInputValue == types.TypeInputValueNumber && config.TypeValueThreshold != nil {
		switch *config.TypeValueThreshold {
		case types.FilterTypeValueThresholdMin:
			if *numericValue < float64(*config.ThresholdMin) {
				return false
			}
		case types.FilterTypeValueThresholdMax:
			if *numericValue > float64(*config.ThresholdMax) {
				return false
			}
		case types.FilterTypeValueThresholdRange:
			if *numericValue < float64(*config.ThresholdMin) ||
				*numericValue > float64(*config.ThresholdMax) {
				return false
			}
		}
	}

	// Apply rate limiting
	if config.MaxRate > 0 {
		// TODO: Implement rate limiting logic
	}

	// Apply uniqueness check
	if config.LastUniqueCheck {
		// TODO: Implement uniqueness check logic
	}

	// Apply size limit
	if config.MaxSize > 0 && len(string(value)) > config.MaxSize {
		fmt.Println(value)
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
