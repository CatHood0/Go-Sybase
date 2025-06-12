package gosybase

import (
	"encoding/json"
	"fmt"
)

func mapToStruct[T any](value map[string]any) (*T, error) {
	var target T
	jsonData, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("Error serializing map: %v", err)
	}

	if err := json.Unmarshal(jsonData, &target); err != nil {
		return nil, fmt.Errorf("Error while deserializing map to struct: %v", err)
	}

	return &target, nil
}
