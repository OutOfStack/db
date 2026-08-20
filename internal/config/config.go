package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"gopkg.in/yaml.v3"
)

// defaultAddress is the default server address shared by server and client configs
const defaultAddress = "127.0.0.1:3223"

// defaultDataDir is the default on-disk directory for WAL segments, snapshots, and tiered-engine segments.
const defaultDataDir = "data"

// getAllowedConfigDirs returns the list of directories to search for config files. It checks current directory, user
// config directory, and user home directory
func getAllowedConfigDirs() []string {
	var dirs []string

	if curDir, err := os.Getwd(); err == nil {
		dirs = append(dirs, curDir)
	}
	if configDir, err := os.UserConfigDir(); err == nil {
		dirs = append(dirs, configDir)
	}
	if homeDir, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, homeDir)
	}

	return dirs
}

// load reads a configuration file into the provided config struct. If the file is not found, it returns nil data and no
// error
func load(filename string) (data []byte, err error) {
	if !fs.ValidPath(filename) {
		return nil, fmt.Errorf("invalid config filename: %q", filename)
	}

	allowedConfigDirs := getAllowedConfigDirs()

	for _, configDir := range allowedConfigDirs {
		dirFS := os.DirFS(configDir)
		data, err = fs.ReadFile(dirFS, filename)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("failed to read config file %s: %w", filename, err)
		}
		return data, nil // file found and read
	}

	return nil, nil
}

// LoadServerConfig loads the server configuration from a YAML file and applies DB_* environment variable overrides.
// Priority: environment variables > file values > defaults
func LoadServerConfig(filename string) (*ServerConfig, error) {
	cfg := DefaultServerConfig()

	if filename != "" {
		var err error
		cfg, err = decodeServerConfig(filename)
		if err != nil {
			return nil, err
		}
	}

	if err := cfg.applyEnvOverrides(); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func decodeServerConfig(filename string) (*ServerConfig, error) {
	// #nosec G304 -- the command-line config path is intentionally operator-controlled.
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", filename, err)
	}

	cfg := DefaultServerConfig()
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err = decoder.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return cfg, nil
	}
	if err == nil {
		return nil, errors.New("failed to parse config file: expected one YAML document")
	}
	return nil, fmt.Errorf("failed to parse config file: %w", err)
}

// LoadClientConfig loads the client configuration from a YAML file.
func LoadClientConfig(filename string) (*ClientConfig, error) {
	if filename == "" {
		return DefaultClientConfig(), nil
	}

	data, err := load(filename)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return DefaultClientConfig(), nil
	}

	var cfg ClientConfig
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err = cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}
