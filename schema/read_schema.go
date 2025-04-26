package schema

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Field struct {
	Tag string `yaml:"Tag"`
}

type Schema struct {
	Tag    string  `yaml:"Tag"`
	Fields []Field `yaml:"Fields"`
}

func LoadSchema(yamlPath string) (*Schema, error) {
	fileData, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, err
	}

	var s Schema
	if err := yaml.Unmarshal(fileData, &s); err != nil {
		return nil, err
	}

	return &s, nil
}

func formatFields(fields []Field) string {
	var result string
	for i, field := range fields {
		if i > 0 {
			result += ",\n\t\t"
		}
		result += fmt.Sprintf(`{
			"Tag": "%s"
		}`, field.Tag)
	}
	return result
}

func FormatSchema(s *Schema) string {
	return fmt.Sprintf(`{
	"Tag": "%s",
	"Fields": [
		%s
	]
}`, s.Tag, formatFields(s.Fields))
}

// MustLoadSchema reads YAML or panic if error (helper for simple cases)
func MustLoadSchema(yamlPath string) *Schema {
	s, err := LoadSchema(yamlPath)
	if err != nil {
		log.Fatalf("failed to load schema: %v", err)
	}
	return s
}
