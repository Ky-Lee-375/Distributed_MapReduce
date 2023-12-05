package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// CopyFile copies a file from src to dst.
func CopyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	return err
}

// ProcessDirectory scans the directory, copies files starting with the given prefix, and returns their full paths.
func ProcessDirectory(prefix string) ([]string, error) {
	var copiedFiles []string

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasPrefix(info.Name(), prefix) {
			destPath := filepath.Join("/home/jsku2/maple", info.Name())
			err := CopyFile(path, destPath)
			if err != nil {
				return err
			}
			copiedFiles = append(copiedFiles, destPath) // Store the full destination path
		}
		return nil
	})

	return copiedFiles, err
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./main [prefix]")
		return
	}
	prefix := os.Args[1]

	copiedFiles, err := ProcessDirectory(prefix)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for _, filePath := range copiedFiles {
		fmt.Println(filePath) // Print the full path
	}
}
