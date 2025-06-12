package gosybase

import (
	"os"
	"path/filepath"
)

const (
	configFileName = "tdslink.properties"
	configDir      = "resources"
)

// Search the tdslink.properties that must be under a directory
// called "resources"
//
//	/resources/
//	|
//	|_ tdslink.properties
func FindTds() (string, error) {
	path := filepath.Join(configDir, configFileName)
	// 1. Search at the root directory of the current workspace
	if checkFileExistence(path) {
		return path, nil
	}

	// 2. Search at the same level of the .go that imports this module
	absPath, _ := filepath.Abs(path)
	if checkFileExistence(absPath) {
		return "", os.ErrNotExist
	}

	return absPath, nil
}

func checkFileExistence(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}
