package klib

import (
	"bufio"

	"os"
	"strings"
)

// RecordValue represents the struct of the value in a Kafka message
type RecordValue struct {
	Count int
}


func GetConfig() (map[string]string, error) {
	configFile := os.Getenv(`KLIB_CONFIG`)
	if configFile == "" {
		configFile = `kafka.rc`
	}

	return ReadCCloudConfig(configFile)
}

// ReadCCloudConfig reads the file specified by configFile and
// creates a map of key-value pairs that correspond to each
// line of the file. ReadCCloudConfig returns the map on success,
// or exits on error
func ReadCCloudConfig(configFile string) (map[string]string, error) {
	m := make(map[string]string)

	file, err := os.Open(configFile)
	if err != nil {

		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			m[parameter] = value
		}
	}

	if err := scanner.Err(); err != nil {
		// fmt.Printf("Failed to read file: %s", err)
		return nil, err
	}

	return m, nil

}
