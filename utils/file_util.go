package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func MkdirIsNotExit(path string) error {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}

func GetCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		fmt.Errorf(err.Error())
	}
	return strings.Replace(dir, "\\", "/", -1)
}
