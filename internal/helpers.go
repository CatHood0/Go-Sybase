package sybase

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (s *Sybase) IsConnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected
}

func (s *Sybase) handleErrors() {
	scanner := bufio.NewScanner(s.stderr)
	for scanner.Scan() {
		if !s.IsConnected() {
			break
		}

		// since output or errors comes in bytes format
		// we prefer converting them into string
		errMsg := string(scanner.Bytes()[:])
		// normally, these are response logs from the Tds bridge
		// we prefer ignoring them just printing as a common log
		if strings.HasPrefix(errMsg, "JAVAERROR:") || strings.HasPrefix(errMsg, "JAVAEXCEPTION:") {
			fmt.Printf("%s\n", errMsg)
			continue
		} else {
			fmt.Printf("Database error: %s\n", errMsg)
		}
		s.Disconnect()
	}
}

func (s *Sybase) handleResponses() {
	scanner := bufio.NewScanner(s.stdout)
	for scanner.Scan() {
		if !s.IsConnected() {
			break
		}

		if s.logs {
			// normally, these are response logs from the Tds bridge
			// we prefer ignoring them just printing as a common log
			cmdLog := string(scanner.Bytes()[:])
			if strings.HasPrefix(cmdLog, "JAVALOG:") {
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
		possiblePaths := []string{
			"libs/TDSLink",
			"../libs/TDSLink",
			filepath.Join(getExecutableDir(), "libs", "TDSLink"),
		}

		for _, path := range possiblePaths {
			if absPath, err := filepath.Abs(path); err == nil {
				if _, err := os.Stat(filepath.Join(absPath, "dist", "TDSLink.jar")); err == nil {
					effectiveBasePath = absPath
					break
				}
			}
		}

		if effectiveBasePath == "" {
			return nil, errors.New("couldn't be founded TDSLink directory")
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
	tdsPath := filepath.Join(*basePath, "dist", "TDSLink.jar")

	if _, err := os.Stat(tdsPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("TDSLink.jar no encontrado en: %s", tdsPath)
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

func checkFileExistence(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}
