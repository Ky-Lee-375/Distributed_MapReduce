package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./juice-exe [intermediate]")
		return
	}

	intermediate := os.Args[1]
	filesToProcess := getFilesToProcess(intermediate)
	fileContents := make(map[string]map[string]int)

	for _, file := range filesToProcess {
		key := extractKey(file)
		lines, _ := readLines(file)
		if fileContents[key] == nil {
			fileContents[key] = make(map[string]int)
		}
		for _, line := range lines {
			fileContents[key][line]++
		}
	}

	outputDir := "/home/jsku2/juice/"
	var generatedFiles []string

	for key, contents := range fileContents {
		outputFile := outputDir + intermediate + "-" + key + ".csv"
		writeProportionsToFile(outputFile, key, contents)
		generatedFiles = append(generatedFiles, filepath.Base(outputFile))
	}

	fmt.Println(strings.Join(generatedFiles, ","))
}

func getFilesToProcess(intermediate string) []string {
	var files []string
	dirEntries, _ := ioutil.ReadDir(".")

	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()
		if strings.HasPrefix(filename, intermediate) && strings.HasSuffix(filename, ".csv") {
			files = append(files, filename)
		}
	}

	return files
}

func extractKey(filename string) string {
	parts := strings.Split(filename, "--")
	if len(parts) < 2 {
		return ""
	}
	return strings.Split(parts[1], ".")[0]
}

func readLines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func writeProportionsToFile(filename, key string, contentMap map[string]int) {
	file, _ := os.Create(filename)
	defer file.Close()

	total := 0
	for _, count := range contentMap {
		total += count
	}

	writer := bufio.NewWriter(file)
	for value, count := range contentMap {
		proportion := float64(count) / float64(total) * 100 // Calculate percentage
		fmt.Println("Key:", value, "Count:", count, "Total:", total)
		line := fmt.Sprintf("%s\t%.2f%%\n", value, proportion) // Format as a percentage string
		writer.WriteString(line)
	}
	writer.Flush()
}
