package gosybase

import (
	"encoding/json"
	"fmt"
)

func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1]) // Elimina las comillas exteriores
}

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
