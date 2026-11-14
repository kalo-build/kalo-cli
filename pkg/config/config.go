package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// KaloConfig represents the structure of kalo.yaml
type KaloConfig struct {
	Stores    map[string]Store            `yaml:"stores"`
	Config    map[string]interface{}      `yaml:"config"`
	Pipelines map[string]Pipeline         `yaml:"pipelines"`
	Plugins   map[string]PluginDefinition `yaml:"plugins"`
}

// StoreType constants
const (
	StoreTypeLocalFileSystem  = "localFileSystem"
	StoreTypeGitRepository    = "gitRepository"
	StoreTypeCloudSqlDatabase = "cloudSqlDatabase"
)

// Store represents a data store configuration.
// The Type field determines which options are relevant.
type Store struct {
	Format  string         `yaml:"format"`
	Type    string         `yaml:"type"` // localFileSystem, gitRepository, cloudSqlDatabase
	Options map[string]any `yaml:"options,omitempty"`
}

// GetStringOption returns a string option value.
func (s Store) GetStringOption(key string, defaultVal string) string {
	if s.Options == nil {
		return defaultVal
	}
	if v, ok := s.Options[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return defaultVal
}

// Path returns the path option for localFileSystem stores.
func (s Store) Path() string {
	return s.GetStringOption("path", "")
}

// Connection returns the connection option for cloudSqlDatabase stores.
func (s Store) Connection() string {
	return s.GetStringOption("connection", "")
}

// GitRepoRoot returns the repoRoot option for gitRepository stores.
func (s Store) GitRepoRoot() string {
	return s.GetStringOption("repoRoot", ".")
}

// GitRef returns the ref option for gitRepository stores.
func (s Store) GitRef() string {
	return s.GetStringOption("ref", "HEAD")
}

// GitSubPath returns the subPath option for gitRepository stores.
func (s Store) GitSubPath() string {
	return s.GetStringOption("subPath", "")
}

// Pipeline represents a pipeline configuration
type Pipeline struct {
	Description string  `yaml:"description,omitempty"`
	Alias       string  `yaml:"alias,omitempty"`
	Stages      []Stage `yaml:"stages"`
}

// Stage represents a stage in a pipeline
type Stage struct {
	Name   string                 `yaml:"name"`
	Steps  []StepSpec             `yaml:"steps"`
	Config map[string]interface{} `yaml:"config,omitempty"`
}

// StepSpec represents a pipeline step. It supports two YAML forms:
//   - string: "plugin: my-alias"
//   - object: { plugin: my-alias, input: ..., output: ..., config: ... }
type StepSpec struct {
	Plugin string                  `yaml:"plugin"`
	Input  *PluginIOSpec           `yaml:"input,omitempty"`
	Inputs map[string]PluginIOSpec `yaml:"inputs,omitempty"`
	Output *PluginIOSpec           `yaml:"output,omitempty"`
	Config map[string]any          `yaml:"config,omitempty"`
}

// UnmarshalYAML implements custom unmarshaling so steps can be strings or objects.
func (s *StepSpec) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err == nil {
		if !strings.HasPrefix(str, "plugin: ") {
			return fmt.Errorf("invalid step string %q (expected 'plugin: <name>')", str)
		}
		s.Plugin = strings.TrimPrefix(str, "plugin: ")
		return nil
	}

	type stepSpecRaw StepSpec
	var raw stepSpecRaw
	if err := unmarshal(&raw); err != nil {
		return fmt.Errorf("step must be a string ('plugin: <name>') or an object with a 'plugin' field: %w", err)
	}
	if raw.Plugin == "" {
		return fmt.Errorf("step object requires a 'plugin' field")
	}
	*s = StepSpec(raw)
	return nil
}

// PluginDefinition represents a plugin configuration.
// When Plugin is set, the map key is an alias and Plugin is the actual identity.
// When Plugin is empty, the map key itself is the plugin identity (backward compat).
type PluginDefinition struct {
	Plugin  string                  `yaml:"plugin,omitempty"`
	Version string                  `yaml:"version"`
	Input   *PluginIOSpec           `yaml:"input,omitempty"`
	Inputs  map[string]PluginIOSpec `yaml:"inputs,omitempty"`
	Output  *PluginIOSpec           `yaml:"output,omitempty"`
	Config  map[string]any          `yaml:"config,omitempty"`
}

// PluginIdentity returns the actual plugin identifier (the Plugin field if set, otherwise the alias key).
func (pd PluginDefinition) PluginIdentity(aliasKey string) string {
	if pd.Plugin != "" {
		return pd.Plugin
	}
	return aliasKey
}

// PluginIOSpec represents a plugin's input or output specification
type PluginIOSpec struct {
	Format  string `yaml:"format"`
	Version string `yaml:"version,omitempty"`
	Store   string `yaml:"store"`
}

// ResolvePipeline looks up a pipeline by name or alias.
// Returns (resolvedName, pipeline, found).
func ResolvePipeline(nameOrAlias string, pipelines map[string]Pipeline) (string, Pipeline, bool) {
	if pipeline, exists := pipelines[nameOrAlias]; exists {
		return nameOrAlias, pipeline, true
	}
	for name, pipeline := range pipelines {
		if pipeline.Alias == nameOrAlias {
			return name, pipeline, true
		}
	}
	return "", Pipeline{}, false
}

// ResolvePlugin looks up a plugin alias or name in the plugins map.
// Returns the resolved PluginDefinition, the actual plugin identity, and whether it was found.
func ResolvePlugin(aliasOrName string, plugins map[string]PluginDefinition) (PluginDefinition, string, bool) {
	pluginDef, exists := plugins[aliasOrName]
	if !exists {
		return PluginDefinition{}, "", false
	}
	return pluginDef, pluginDef.PluginIdentity(aliasOrName), true
}

// ApplyStepOverrides returns a copy of pluginDef with step-level I/O overrides applied.
func ApplyStepOverrides(pluginDef PluginDefinition, step StepSpec) PluginDefinition {
	result := pluginDef
	if step.Input != nil {
		if result.Input == nil {
			result.Input = step.Input
		} else {
			merged := *result.Input
			if step.Input.Store != "" {
				merged.Store = step.Input.Store
			}
			if step.Input.Format != "" {
				merged.Format = step.Input.Format
			}
			if step.Input.Version != "" {
				merged.Version = step.Input.Version
			}
			result.Input = &merged
		}
	}
	if step.Inputs != nil {
		if result.Inputs == nil {
			result.Inputs = make(map[string]PluginIOSpec)
		}
		for k, v := range step.Inputs {
			result.Inputs[k] = v
		}
	}
	if step.Output != nil {
		if result.Output == nil {
			result.Output = step.Output
		} else {
			merged := *result.Output
			if step.Output.Store != "" {
				merged.Store = step.Output.Store
			}
			if step.Output.Format != "" {
				merged.Format = step.Output.Format
			}
			if step.Output.Version != "" {
				merged.Version = step.Output.Version
			}
			result.Output = &merged
		}
	}
	return result
}

// PluginIDToAlias generates a short alias from a full plugin identifier.
// E.g. "@kalo-build/plugin-morphe-ts-types" -> "morphe-ts-types"
func PluginIDToAlias(id string) string {
	if idx := strings.LastIndex(id, "/"); idx >= 0 {
		id = id[idx+1:]
	}
	id = strings.TrimPrefix(id, "plugin-")
	return id
}

// FindPluginByIdentity searches the plugins map for an entry whose actual
// plugin identity matches the given ID (handles both aliased and legacy entries).
func FindPluginByIdentity(plugins map[string]PluginDefinition, pluginID string) (string, PluginDefinition, bool) {
	for alias, def := range plugins {
		if def.PluginIdentity(alias) == pluginID {
			return alias, def, true
		}
	}
	return "", PluginDefinition{}, false
}

// MergeConfig builds the effective config for a plugin step with priority:
// global config (deprecated) < instance config < stage config < step config.
func MergeConfig(cfg *KaloConfig, aliasOrName, pluginID string, instanceConfig, stageConfig map[string]interface{}, stepConfig map[string]any) map[string]interface{} {
	merged := make(map[string]interface{})

	for _, key := range []string{pluginID, aliasOrName} {
		if pluginConfig, ok := cfg.Config[key]; ok {
			if configMap, ok := pluginConfig.(map[string]any); ok {
				for k, v := range configMap {
					merged[k] = v
				}
			}
		}
	}
	if instanceConfig != nil {
		for k, v := range instanceConfig {
			merged[k] = v
		}
	}
	if stageConfig != nil {
		for k, v := range stageConfig {
			merged[k] = v
		}
	}
	if stepConfig != nil {
		for k, v := range stepConfig {
			merged[k] = v
		}
	}
	return merged
}

// ParseKaloConfig deserializes YAML bytes into KaloConfig.
func ParseKaloConfig(data []byte) (*KaloConfig, error) {
	var c KaloConfig
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
