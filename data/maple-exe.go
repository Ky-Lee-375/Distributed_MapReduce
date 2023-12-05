package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	interconneIndex = 10
	detectionIndex  = 9
)

func Map(line string, outputDir string, inputFileBase string, fileExistsMap map[string]bool, interconne string) (string, error) {
	fields := strings.Split(line, ",")
	if len(fields) < interconneIndex+1 {
		return "", fmt.Errorf("not enough fields in line: %s", line)
	}

	interconneValue := strings.TrimSpace(fields[interconneIndex])

	if (interconne != interconneValue) {
		return "", nil;
	}

	detectionValue := strings.TrimSpace(fields[detectionIndex])

	if detectionValue == "" {
		detectionValue = "empty"
	}
	
	safeInterconneValue := strings.ReplaceAll(interconneValue, "/", "_")
	safeInterconneValue = strings.ReplaceAll(safeInterconneValue, " ", "_")
	
	if safeInterconneValue == "" {
		safeInterconneValue = "empty"
	}
	
	outputFilename := fmt.Sprintf("%s/%s--%s.csv", outputDir, inputFileBase, safeInterconneValue)

	// Check if file already exists
	if _, exists := fileExistsMap[outputFilename]; !exists {
		if _, err := os.Stat(outputFilename); os.IsNotExist(err) {
			// Create the file if it does not exist
			file, err := os.Create(outputFilename)
			if err != nil {
				return "", err
			}
			file.Close()
		}
		fileExistsMap[outputFilename] = true
	}

	// Open the file for appending
	file, err := os.OpenFile(outputFilename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Write the 'Detection_' value to the file
	if _, err := file.WriteString(detectionValue + "\n"); err != nil {
		return "", err
	}

	return outputFilename, nil
}

func processFile(inputFile string, outputDir string, fileExistsMap map[string]bool, ic string) []string {
	var generatedFiles []string
	inputFileBase := strings.TrimSuffix(filepath.Base(inputFile), filepath.Ext(inputFile))

	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		filename, err := Map(scanner.Text(), outputDir, inputFileBase, fileExistsMap, ic)
		if err != nil {
			fmt.Println("Error processing file:", err)
			continue
		}
		if filename == "" {
			continue
		}
		generatedFiles = append(generatedFiles, filename)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	return generatedFiles
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./main [filePrefix] [intercon]")
		return
	}

	filePrefix := os.Args[1]
	intercon := os.Args[2]
	outputDir := "/home/jsku2/maple"
	fileExistsMap := make(map[string]bool)

	var allGeneratedFiles []string
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("Error accessing path:", path, err)
			return err
		}

		if strings.HasPrefix(info.Name(), filePrefix) && !info.IsDir() {
			generatedFiles := processFile(path, outputDir, fileExistsMap, intercon)
			allGeneratedFiles = append(allGeneratedFiles, generatedFiles...)
		}

		return nil
	})

	if err != nil {
		fmt.Println("Error walking through files:", err)
	}

	// Remove duplicates from allGeneratedFiles
	uniqueFiles := make(map[string]struct{})
	for _, file := range allGeneratedFiles {
		uniqueFiles[file] = struct{}{}
	}

	// Print all unique generated filenames
	for filename := range uniqueFiles {
		fmt.Println(filename)
	}
}
