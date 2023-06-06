package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
	"golang.org/x/crypto/ssh"
)

type Config struct {
	Hosts []string `yaml:"hosts"`
}

type CommandResult struct {
	Host   string
	Output string
	Error  error
}

func main() {
	// Parse command-line flags
	serverAddressesFile := flag.String("server-addresses", "./hosts.yaml", "File containing server addresses in YAML format")
	command := flag.String("command", "", "Command to execute on the servers")
	sshKey := flag.String("ssh-key", "~/.ssh/id_rsa", "Path to the private key for SSH authentication")
	parallelRequests := flag.Int("parallel-requests", 4, "Number of parallel SSH requests to make")
	sshTimeout := flag.Duration("ssh-timeout", 10*time.Second, "Timeout value for SSH connections")
	flag.Parse()

	// Validate flag values
	if *command == "" {
		log.Fatal("Missing command flag")
	}

	// Read server addresses from YAML file
	config, err := readConfig(*serverAddressesFile)
	if err != nil {
		log.Fatalf("Failed to read server addresses: %v", err)
	}

	// Expand tilde (~) in SSH key path
	expandedKeyPath, err := expandTilde(*sshKey)
	if err != nil {
		log.Fatalf("Failed to expand SSH key path: %v", err)
	}

	// Create a limited concurrency parallelism pattern
	// using the specified number of parallel requests
	semaphore := make(chan struct{}, *parallelRequests)

	// Execute command on each server concurrently
	results := make(chan CommandResult)
	var wg sync.WaitGroup

	for _, host := range config.Hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()

			semaphore <- struct{}{} // Acquire a semaphore slot
			output, err := executeCommand(host, *command, expandedKeyPath, *sshTimeout)
			<-semaphore // Release the semaphore slot

			results <- CommandResult{
				Host:   host,
				Output: output,
				Error:  err,
			}
		}(host)
	}

	// Wait for all goroutines to finish and close the results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and display results
	for result := range results {
		if result.Error != nil {
			log.Printf("Failed to execute command on %s: %v", result.Host, result.Error)
		} else {
			fmt.Printf("Output from %s:\n%s\n", result.Host, result.Output)
		}
	}
}

func readConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func executeCommand(host, command, keyPath string, timeout time.Duration) (string, error) {
	// Read private key file
	keyBytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return "", err
	}

	// Parse private key
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		return "", err
	}

	// SSH configuration
	config := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}

	// SSH connection
	conn, err := ssh.Dial("tcp", host+":22", config)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// SSH session
	session, err := conn.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	// Execute the command
	output, err := session.CombinedOutput(command)
	if err != nil {
		return "", err
	}

	return string(output), nil
}

func expandTilde(path string) (string, error) {
	if len(path) == 0 || path[0] != '~' {
		return path, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(homeDir, path[1:]), nil
}
