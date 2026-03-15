package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestStepSpecUnmarshalYAML_StringForm(t *testing.T) {
	input := `"plugin: my-alias"`
	var step StepSpec
	err := yaml.Unmarshal([]byte(input), &step)
	require.NoError(t, err)
	assert.Equal(t, "my-alias", step.Plugin)
	assert.Nil(t, step.Input)
	assert.Nil(t, step.Output)
	assert.Nil(t, step.Config)
}

func TestStepSpecUnmarshalYAML_StringFormWithOrg(t *testing.T) {
	input := `"plugin: @kalo-build/plugin-morphe-ts-types"`
	var step StepSpec
	err := yaml.Unmarshal([]byte(input), &step)
	require.NoError(t, err)
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", step.Plugin)
}

func TestStepSpecUnmarshalYAML_ObjectForm(t *testing.T) {
	input := `
plugin: ts-types
input:
  store: KA_MO_EXT
  format: "KA:MO1:YAML1"
output:
  store: KA_MO_TS_EXT
config:
  fieldCasing: snake
`
	var step StepSpec
	err := yaml.Unmarshal([]byte(input), &step)
	require.NoError(t, err)
	assert.Equal(t, "ts-types", step.Plugin)
	require.NotNil(t, step.Input)
	assert.Equal(t, "KA_MO_EXT", step.Input.Store)
	require.NotNil(t, step.Output)
	assert.Equal(t, "KA_MO_TS_EXT", step.Output.Store)
	require.NotNil(t, step.Config)
	assert.Equal(t, "snake", step.Config["fieldCasing"])
}

func TestStepSpecUnmarshalYAML_ObjectFormMinimal(t *testing.T) {
	input := `plugin: ts-types`
	var step StepSpec
	err := yaml.Unmarshal([]byte(input), &step)
	require.NoError(t, err)
	assert.Equal(t, "ts-types", step.Plugin)
}

func TestStepSpecUnmarshalYAML_InvalidString(t *testing.T) {
	input := `"not a valid step"`
	var step StepSpec
	err := yaml.Unmarshal([]byte(input), &step)
	require.Error(t, err)
}

func TestStepSpecUnmarshalYAML_ObjectMissingPlugin(t *testing.T) {
	input := `
input:
  store: KA_MO_EXT
`
	var step StepSpec
	err := yaml.Unmarshal([]byte(input), &step)
	require.Error(t, err)
}

func TestStageStepsUnmarshal_MixedForms(t *testing.T) {
	input := `
name: compile-stage
steps:
  - "plugin: ts-types"
  - plugin: zod-types
    input:
      store: CUSTOM_STORE
`
	var stage Stage
	err := yaml.Unmarshal([]byte(input), &stage)
	require.NoError(t, err)
	require.Len(t, stage.Steps, 2)
	assert.Equal(t, "ts-types", stage.Steps[0].Plugin)
	assert.Equal(t, "zod-types", stage.Steps[1].Plugin)
	require.NotNil(t, stage.Steps[1].Input)
	assert.Equal(t, "CUSTOM_STORE", stage.Steps[1].Input.Store)
}

func TestResolvePlugin_Aliased(t *testing.T) {
	plugins := map[string]PluginDefinition{
		"ts-types": {
			Plugin:  "@kalo-build/plugin-morphe-ts-types",
			Version: "v1.0.0",
		},
	}
	def, identity, found := ResolvePlugin("ts-types", plugins)
	require.True(t, found)
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", identity)
	assert.Equal(t, "v1.0.0", def.Version)
}

func TestResolvePlugin_Legacy(t *testing.T) {
	plugins := map[string]PluginDefinition{
		"@kalo-build/plugin-morphe-ts-types": {
			Version: "v1.0.0",
		},
	}
	def, identity, found := ResolvePlugin("@kalo-build/plugin-morphe-ts-types", plugins)
	require.True(t, found)
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", identity)
	assert.Empty(t, def.Plugin)
}

func TestResolvePlugin_NotFound(t *testing.T) {
	plugins := map[string]PluginDefinition{
		"ts-types": {
			Plugin:  "@kalo-build/plugin-morphe-ts-types",
			Version: "v1.0.0",
		},
	}
	_, _, found := ResolvePlugin("nonexistent", plugins)
	assert.False(t, found)
}

func TestPluginIdentity_WithPluginField(t *testing.T) {
	pd := PluginDefinition{
		Plugin:  "@kalo-build/plugin-morphe-ts-types",
		Version: "v1.0.0",
	}
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", pd.PluginIdentity("ts-types"))
}

func TestPluginIdentity_WithoutPluginField(t *testing.T) {
	pd := PluginDefinition{Version: "v1.0.0"}
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", pd.PluginIdentity("@kalo-build/plugin-morphe-ts-types"))
}

func TestApplyStepOverrides_InputStore(t *testing.T) {
	pluginDef := PluginDefinition{
		Version: "v1.0.0",
		Input:   &PluginIOSpec{Format: "KA:MO1:YAML1", Store: "KA_MO_YAML"},
		Output:  &PluginIOSpec{Format: "KA:MO1:TS1", Store: "KA_MO_TS"},
	}
	step := StepSpec{Plugin: "ts-types", Input: &PluginIOSpec{Store: "KA_MO_EXT"}}
	result := ApplyStepOverrides(pluginDef, step)
	require.NotNil(t, result.Input)
	assert.Equal(t, "KA_MO_EXT", result.Input.Store)
	assert.Equal(t, "KA:MO1:YAML1", result.Input.Format)
	assert.Equal(t, "KA_MO_TS", result.Output.Store)
}

func TestApplyStepOverrides_OutputStore(t *testing.T) {
	pluginDef := PluginDefinition{
		Version: "v1.0.0",
		Input:   &PluginIOSpec{Format: "KA:MO1:YAML1", Store: "KA_MO_YAML"},
		Output:  &PluginIOSpec{Format: "KA:MO1:TS1", Store: "KA_MO_TS"},
	}
	step := StepSpec{Plugin: "ts-types", Output: &PluginIOSpec{Store: "KA_MO_TS_EXT"}}
	result := ApplyStepOverrides(pluginDef, step)
	require.NotNil(t, result.Output)
	assert.Equal(t, "KA_MO_TS_EXT", result.Output.Store)
	assert.Equal(t, "KA:MO1:TS1", result.Output.Format)
}

func TestApplyStepOverrides_NoOverrides(t *testing.T) {
	pluginDef := PluginDefinition{
		Version: "v1.0.0",
		Input:   &PluginIOSpec{Format: "KA:MO1:YAML1", Store: "KA_MO_YAML"},
		Output:  &PluginIOSpec{Format: "KA:MO1:TS1", Store: "KA_MO_TS"},
	}
	step := StepSpec{Plugin: "ts-types"}
	result := ApplyStepOverrides(pluginDef, step)
	assert.Equal(t, "KA_MO_YAML", result.Input.Store)
	assert.Equal(t, "KA_MO_TS", result.Output.Store)
}

func TestApplyStepOverrides_NilBaseInput(t *testing.T) {
	pluginDef := PluginDefinition{Version: "v1.0.0"}
	step := StepSpec{Plugin: "ts-types", Input: &PluginIOSpec{Store: "CUSTOM", Format: "FMT"}}
	result := ApplyStepOverrides(pluginDef, step)
	require.NotNil(t, result.Input)
	assert.Equal(t, "CUSTOM", result.Input.Store)
	assert.Equal(t, "FMT", result.Input.Format)
}

func TestPluginIDToAlias(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"@kalo-build/plugin-morphe-ts-types", "morphe-ts-types"},
		{"@kalo-build/plugin-morphe-psql-types", "morphe-psql-types"},
		{"@kalo-build/plugin-morphe-db-manager", "morphe-db-manager"},
		{"@org/plugin-some-thing", "some-thing"},
		{"plugin-local-tool", "local-tool"},
		{"my-plugin", "my-plugin"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.expected, PluginIDToAlias(tc.input))
		})
	}
}

func TestFindPluginByIdentity_Aliased(t *testing.T) {
	plugins := map[string]PluginDefinition{
		"ts-types": {
			Plugin:  "@kalo-build/plugin-morphe-ts-types",
			Version: "v1.0.0",
		},
		"zod-types": {
			Plugin:  "@kalo-build/plugin-morphe-zod-types",
			Version: "v1.0.0",
		},
	}
	alias, def, found := FindPluginByIdentity(plugins, "@kalo-build/plugin-morphe-ts-types")
	require.True(t, found)
	assert.Equal(t, "ts-types", alias)
	assert.Equal(t, "v1.0.0", def.Version)
}

func TestFindPluginByIdentity_Legacy(t *testing.T) {
	plugins := map[string]PluginDefinition{
		"@kalo-build/plugin-morphe-ts-types": {Version: "v1.0.0"},
	}
	alias, _, found := FindPluginByIdentity(plugins, "@kalo-build/plugin-morphe-ts-types")
	require.True(t, found)
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", alias)
}

func TestFindPluginByIdentity_NotFound(t *testing.T) {
	plugins := map[string]PluginDefinition{
		"ts-types": {
			Plugin:  "@kalo-build/plugin-morphe-ts-types",
			Version: "v1.0.0",
		},
	}
	_, _, found := FindPluginByIdentity(plugins, "@kalo-build/plugin-morphe-nonexistent")
	assert.False(t, found)
}

func TestResolvePipeline_DirectName(t *testing.T) {
	pipelines := map[string]Pipeline{
		"compile": {Description: "Build", Stages: []Stage{}},
	}
	name, p, found := ResolvePipeline("compile", pipelines)
	require.True(t, found)
	assert.Equal(t, "compile", name)
	assert.Equal(t, "Build", p.Description)
}

func TestResolvePipeline_ByAlias(t *testing.T) {
	pipelines := map[string]Pipeline{
		"migrate-up": {Alias: "up", Stages: []Stage{}},
	}
	name, p, found := ResolvePipeline("up", pipelines)
	require.True(t, found)
	assert.Equal(t, "migrate-up", name)
	assert.Equal(t, "up", p.Alias)
}

func TestResolvePipeline_NotFound(t *testing.T) {
	pipelines := map[string]Pipeline{"compile": {}}
	_, _, found := ResolvePipeline("nonexistent", pipelines)
	assert.False(t, found)
}

func TestStore_GetStringOption(t *testing.T) {
	s := Store{Options: map[string]any{"path": "/foo", "num": 42}}
	assert.Equal(t, "/foo", s.GetStringOption("path", ""))
	assert.Equal(t, "default", s.GetStringOption("missing", "default"))
	assert.Equal(t, "default", s.GetStringOption("num", "default"))
}

func TestStore_Path(t *testing.T) {
	s := Store{Options: map[string]any{"path": "morphe/"}}
	assert.Equal(t, "morphe/", s.Path())
	assert.Empty(t, Store{}.Path())
}

func TestMergeConfig_Priority(t *testing.T) {
	cfg := &KaloConfig{
		Config: map[string]interface{}{
			"p1": map[string]any{"k": "global"},
		},
	}
	instance := map[string]interface{}{"k": "instance"}
	stage := map[string]interface{}{"k": "stage"}
	step := map[string]any{"k": "step"}
	merged := MergeConfig(cfg, "p1", "p1", instance, stage, step)
	assert.Equal(t, "step", merged["k"])
}

func TestParseKaloConfig_BackwardCompatible(t *testing.T) {
	input := `
stores:
  KA_MO_YAML:
    format: "KA:MO1:YAML1"
    type: localFileSystem
    options:
      path: morphe
plugins:
  '@kalo-build/plugin-morphe-ts-types':
    version: v1.0.0
    input:
      format: "KA:MO1:YAML1"
      store: KA_MO_YAML
    output:
      format: "KA:MO1:TS1"
      store: KA_MO_TS
pipelines:
  compile:
    stages:
      - name: ts-types
        steps:
          - "plugin: @kalo-build/plugin-morphe-ts-types"
`
	cfg, err := ParseKaloConfig([]byte(input))
	require.NoError(t, err)
	pluginDef := cfg.Plugins["@kalo-build/plugin-morphe-ts-types"]
	assert.Empty(t, pluginDef.Plugin)
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", pluginDef.PluginIdentity("@kalo-build/plugin-morphe-ts-types"))
	require.Len(t, cfg.Pipelines["compile"].Stages[0].Steps, 1)
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", cfg.Pipelines["compile"].Stages[0].Steps[0].Plugin)
}

func TestParseKaloConfig_NewFormat(t *testing.T) {
	input := `
plugins:
  ts-types:
    plugin: '@kalo-build/plugin-morphe-ts-types'
    version: v1.0.0
    input:
      format: "KA:MO1:YAML1"
      store: KA_MO_YAML
    output:
      format: "KA:MO1:TS1"
      store: KA_MO_TS
  ts-types-ext:
    plugin: '@kalo-build/plugin-morphe-ts-types'
    version: v1.0.0
    input:
      store: KA_MO_EXT
    output:
      store: KA_MO_TS_EXT
pipelines:
  compile:
    stages:
      - name: a
        steps:
          - "plugin: ts-types"
      - name: b
        steps:
          - "plugin: ts-types-ext"
`
	cfg, err := ParseKaloConfig([]byte(input))
	require.NoError(t, err)
	require.Len(t, cfg.Plugins, 2)
	ts := cfg.Plugins["ts-types"]
	assert.Equal(t, "@kalo-build/plugin-morphe-ts-types", ts.Plugin)
	tsExt := cfg.Plugins["ts-types-ext"]
	assert.Equal(t, "KA_MO_EXT", tsExt.Input.Store)
	require.Len(t, cfg.Pipelines["compile"].Stages, 2)
	assert.Equal(t, "ts-types", cfg.Pipelines["compile"].Stages[0].Steps[0].Plugin)
	assert.Equal(t, "ts-types-ext", cfg.Pipelines["compile"].Stages[1].Steps[0].Plugin)
}

func TestStepSpec_MarshalRoundTrip(t *testing.T) {
	step := StepSpec{Plugin: "ts-types"}
	data, err := yaml.Marshal(step)
	require.NoError(t, err)
	var roundTripped StepSpec
	err = yaml.Unmarshal(data, &roundTripped)
	require.NoError(t, err)
	assert.Equal(t, "ts-types", roundTripped.Plugin)
}
