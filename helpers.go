package gosybase

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (s *sybase) isConnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected
}

func (s *sybase) handleErrors() {
	scanner := bufio.NewScanner(s.stderr)
	for scanner.Scan() {
		if !s.isConnected() {
			break
		}

		errMsg := scanner.Text()
		// normally, these are response logs from the Tds bridge
		// we prefer ignoring them just printing as a common log
		if strings.HasPrefix(errMsg, "JAVAERROR:") {
			fmt.Printf("%s\n", errMsg)
			continue
		} else {
			fmt.Printf("Database error: %s\n", errMsg)
		}
		s.disconnect()
	}
}

func (s *sybase) handleResponses() {
	scanner := bufio.NewScanner(s.stdout)
	for scanner.Scan() {
		if !s.isConnected() {
			break
		}

		if s.logs {
			// normally, these are response logs from the Tds bridge
			// we prefer ignoring them just printing as a common log
			cmdLog := string(scanner.Bytes()[:])
			if strings.HasPrefix(cmdLog, "JAVALOG:") || strings.HasPrefix(cmdLog, "JAVAERROR:") {
				fmt.Printf("%s\n", cmdLog)
				continue
			}
		}

		var resp QueryResponse

		if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
			fmt.Printf("Error parsing response: %v\n", err)
			continue
		}

		s.mu.Lock()
		if ch, exists := s.currentQueries[resp.MsgID]; exists {
			ch <- resp
		}
		s.mu.Unlock()
	}
}

func getExecutableDir() string {
	ex, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(ex)
}

func configureBasePath(configPath string) (*string, error) {
	effectiveBasePath := configPath
	if effectiveBasePath == "" {
		// Buscar automáticamente la ruta relativa
		possiblePaths := []string{
			"libs/JavaSybaseLink",
			"../libs/JavaSybaseLink",
			filepath.Join(getExecutableDir(), "libs", "JavaSybaseLink"),
		}

		for _, path := range possiblePaths {
			if absPath, err := filepath.Abs(path); err == nil {
				if _, err := os.Stat(filepath.Join(absPath, "dist", "JavaSybaseLink.jar")); err == nil {
					effectiveBasePath = absPath
					break
				}
			}
		}

		if effectiveBasePath == "" {
			return nil, errors.New("no se pudo encontrar el directorio JavaSybaseLink")
		}
	}
	return &effectiveBasePath, nil
}

func getTdsJarPath(config *Config) (*string, error) {
	basePath, basePathErr := configureBasePath(config.TdsLink)

	if basePathErr != nil {
		return nil, basePathErr
	}

	// Construir rutas completas
	tdsPath := filepath.Join(*basePath, "dist", "TdsLink.jar")

	if _, err := os.Stat(tdsPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("TdsLink.jar no encontrado en: %s", tdsPath)
	}
	return &tdsPath, nil
}

func convertToRawResponse(data []any) (*RawResponse, error) {
	var response RawResponse = RawResponse{Results: []map[string]any{}}

	for _, jsonItem := range data {
		jsonBytes, err := json.Marshal(jsonItem)
		if err != nil {
			return nil, fmt.Errorf("error al serializar el dato: %v", err)
		}

		var jsonMap []map[string]any
		if err := json.Unmarshal(jsonBytes, &jsonMap); err != nil {
			return nil, fmt.Errorf("error al parsear el dato: %v", err)
		}
		response.Results = append(response.Results, jsonMap...)
	}
	return &response, nil
}
