package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source  ConnConfig `yaml:"source"`
	Analysis ConnConfig `yaml:"analysis"`

	SchemaCache string `yaml:"schema_cache"` // local JSON path for schema snapshot

	Input  InputConfig  `yaml:"input"`
	Output OutputConfig `yaml:"output"`
}

type ConnConfig struct {
	DSN string `yaml:"dsn"`
	// e.g. "sqlserver://user:pass@host?database=mydb"
}

type InputConfig struct {
	Mode   string   `yaml:"mode"`  // "trn" or "ldf"
	Files  []string `yaml:"files"` // absolute paths on the SQL Server host (trn mode)
	Tables []string `yaml:"tables"` // empty = all tables
}

type OutputConfig struct {
	Format string `yaml:"format"` // "sql" or "json"
	File   string `yaml:"file"`   // "" = stdout
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}
