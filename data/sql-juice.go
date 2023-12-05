package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// ProcessFiles processes files matching the prefix and regex, then writes matching lines to a CSV file.
func ProcessFiles(prefix, regexStr string) error {
	regex, err := regexp.Compile(regexStr)
	if err != nil {
		return err
	}

	outputFileName := fmt.Sprintf("/home/jsku2/juice/%s-%d.csv", prefix, time.Now().Unix())
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasPrefix(info.Name(), prefix) {
			return processFile(path, regex, outputFile)
		}
		return nil
	})

	return err
}

// processFile reads a file, applies regex to each line, and writes matching lines to the outputFile.
func processFile(filePath string, regex *regexp.Regexp, outputFile *os.File) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if regex.MatchString(line) {
			if _, err := outputFile.WriteString(line + "\n"); err != nil {
				return err
			}
		}
	}

	return scanner.Err()
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./sql-juice [pref] [regex]")
		return
	}
	prefix := os.Args[1]
	regex := os.Args[2]

	err := ProcessFiles(prefix, regex)
	if err != nil {
		fmt.Println("Error:", err)
	}
}