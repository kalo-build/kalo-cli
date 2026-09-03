package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalo-build/kalo-cli/internal/specv0"
	kconfig "github.com/kalo-build/kalo-cli/pkg/config"
	"github.com/kalo-build/kalo-cli/pkg/hostfuncs"
	"github.com/spf13/cobra"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

func specCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spec",
		Short: "Validate, route, and run SPEC v0.1-alpha catalogs",
	}
	cmd.AddCommand(specValidateCommand(), specRouteCommand(), specRunCommand())
	return cmd
}

func specValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate <catalog-path>",
		Short: "Validate SPEC descriptors and their referenced artifacts",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			catalog, err := specv0.Load(args[0])
			if err != nil {
				return err
			}
			return writeJSON(cmd, map[string]any{
				"spec":    "0.1-alpha",
				"valid":   true,
				"catalog": filepath.ToSlash(catalog.Root),
				"counts":  catalog.Counts(),
			})
		},
	}
	return cmd
}

func specRouteCommand() *cobra.Command {
	var catalogPath, from, to string
	var allowUnsafe, allowOpenWorld bool
	cmd := &cobra.Command{
		Use:   "route",
		Short: "Discover a deterministic route from SPEC Processor edges",
		RunE: func(cmd *cobra.Command, args []string) error {
			catalog, err := specv0.Load(catalogPath)
			if err != nil {
				return err
			}
			route, err := catalog.Route(from, to, specv0.RouteOptions{AllowUnsafe: allowUnsafe, AllowOpenWorld: allowOpenWorld})
			if err != nil {
				return err
			}
			return writeJSON(cmd, route)
		},
	}
	cmd.Flags().StringVar(&catalogPath, "catalog", "", "Path containing SPEC descriptors")
	cmd.Flags().StringVar(&from, "from", "", "Source Contract or Representation Binding address")
	cmd.Flags().StringVar(&to, "to", "", "Target Contract or Representation Binding address")
	cmd.Flags().BoolVar(&allowUnsafe, "allow-unsafe", false, "Authorize Processors declaring safe: false")
	cmd.Flags().BoolVar(&allowOpenWorld, "allow-open-world", false, "Authorize Processors declaring open_world: true")
	_ = cmd.MarkFlagRequired("catalog")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")
	return cmd
}

type specRunOptions struct {
	CatalogPath    string
	From           string
	To             string
	Input          string
	Output         string
	AllowUnsafe    bool
	AllowOpenWorld bool
	Policy         executionPolicy
}

func specRunCommand() *cobra.Command {
	var options specRunOptions
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Discover and execute a SPEC route using declared implementations",
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := runSPEC(options)
			if err != nil {
				return err
			}
			return writeJSON(cmd, result)
		},
	}
	cmd.Flags().StringVar(&options.CatalogPath, "catalog", "", "Path containing SPEC descriptors")
	cmd.Flags().StringVar(&options.From, "from", "", "Source Contract or Representation Binding address")
	cmd.Flags().StringVar(&options.To, "to", "", "Target Contract or Representation Binding address")
	cmd.Flags().StringVar(&options.Input, "input", "", "Input value file")
	cmd.Flags().StringVar(&options.Output, "output", "spec-output", "New directory for values and Receipts")
	cmd.Flags().BoolVar(&options.AllowUnsafe, "allow-unsafe", false, "Authorize Processors declaring safe: false")
	cmd.Flags().BoolVar(&options.AllowOpenWorld, "allow-open-world", false, "Authorize Processors declaring open_world: true")
	addExecutionPolicyFlags(cmd, &options.Policy)
	_ = cmd.MarkFlagRequired("catalog")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")
	_ = cmd.MarkFlagRequired("input")
	return cmd
}

type specReceiptArtifact struct {
	Port              string            `json:"port"`
	Contract          string            `json:"contract"`
	ContractVersionID string            `json:"contract_version_id"`
	Binding           string            `json:"binding"`
	BindingVersionID  string            `json:"binding_version_id"`
	ArtifactDigest    string            `json:"artifact_digest"`
	Source            map[string]string `json:"source,omitempty"`
}

type specReceiptSubject struct {
	Processor            string `json:"processor,omitempty"`
	ProcessorVersionID   string `json:"processor_version_id,omitempty"`
	ImplementationID     string `json:"implementation_id,omitempty"`
	ImplementationDigest string `json:"implementation_digest,omitempty"`
	Composition          string `json:"composition,omitempty"`
	CompositionVersionID string `json:"composition_version_id,omitempty"`
}

type specReceiptRouteNode struct {
	Node               string `json:"node"`
	Processor          string `json:"processor"`
	ProcessorVersionID string `json:"processor_version_id"`
	ReceiptID          string `json:"receipt_id"`
}

type specReceipt struct {
	Schema         string                 `json:"$schema"`
	Spec           string                 `json:"spec"`
	Kind           string                 `json:"kind"`
	ReceiptID      string                 `json:"receipt_id"`
	ExecutionID    string                 `json:"execution_id"`
	Subject        specReceiptSubject     `json:"subject"`
	Status         string                 `json:"status"`
	StartedAt      string                 `json:"started_at"`
	EndedAt        string                 `json:"ended_at"`
	Inputs         []specReceiptArtifact  `json:"inputs"`
	Outputs        []specReceiptArtifact  `json:"outputs"`
	Route          []specReceiptRouteNode `json:"route,omitempty"`
	ParentReceipts []string               `json:"parent_receipts,omitempty"`
}

type specExecutionResult struct {
	Spec               string        `json:"spec"`
	ExecutionID        string        `json:"execution_id"`
	From               string        `json:"from"`
	To                 string        `json:"to"`
	Input              string        `json:"input"`
	Output             string        `json:"output"`
	Route              *specv0.Route `json:"route"`
	Receipts           []string      `json:"receipts"`
	CompositionReceipt string        `json:"composition_receipt,omitempty"`
}

func runSPEC(options specRunOptions) (*specExecutionResult, error) {
	if options.Policy.PluginMemoryMiB > 4096 {
		return nil, fmt.Errorf("plugin memory limit %d MiB exceeds the WebAssembly maximum of 4096 MiB", options.Policy.PluginMemoryMiB)
	}
	catalog, err := specv0.Load(options.CatalogPath)
	if err != nil {
		return nil, err
	}
	route, err := catalog.Route(options.From, options.To, specv0.RouteOptions{AllowUnsafe: options.AllowUnsafe, AllowOpenWorld: options.AllowOpenWorld})
	if err != nil {
		return nil, err
	}
	if len(route.Steps) == 0 {
		return nil, fmt.Errorf("route from %s to %s contains no Processor", options.From, options.To)
	}
	inputPath, err := filepath.Abs(options.Input)
	if err != nil {
		return nil, err
	}
	inputInfo, err := os.Stat(inputPath)
	if err != nil || inputInfo.IsDir() {
		return nil, fmt.Errorf("input must be one readable file: %s", inputPath)
	}
	outputRoot, err := filepath.Abs(options.Output)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(outputRoot); err == nil {
		return nil, fmt.Errorf("output directory already exists: %s", outputRoot)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	artifactsDir := filepath.Join(outputRoot, "artifacts")
	receiptsDir := filepath.Join(outputRoot, "receipts")
	workDir := filepath.Join(outputRoot, "work")
	for _, path := range []string{artifactsDir, receiptsDir, workDir} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, err
		}
	}
	failed := true
	defer func() {
		if failed {
			_ = os.RemoveAll(outputRoot)
		}
	}()

	receiptSchema, err := catalog.SchemaPath("receipt.schema.json")
	if err != nil {
		return nil, err
	}
	schemaRef, err := filepath.Rel(receiptsDir, receiptSchema)
	if err != nil {
		return nil, err
	}
	schemaRef = filepath.ToSlash(schemaRef)

	executionID, err := randomUUID()
	if err != nil {
		return nil, err
	}
	executionStarted := time.Now().UTC()
	currentPath := filepath.Join(workDir, "00-input", filepath.Base(inputPath))
	if err := copyFile(inputPath, currentPath); err != nil {
		return nil, err
	}
	initialEvidencePath := filepath.Join(artifactsDir, "00-"+filepath.Base(inputPath))
	if err := copyFile(inputPath, initialEvidencePath); err != nil {
		return nil, err
	}

	ctx := context.Background()
	runtimeConfig := wazero.NewRuntimeConfig().WithCloseOnContextDone(options.Policy.PluginTimeout > 0)
	if options.Policy.PluginMemoryMiB > 0 {
		runtimeConfig = runtimeConfig.WithMemoryLimitPages(options.Policy.PluginMemoryMiB * 16)
	}
	wasmRuntime := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	defer wasmRuntime.Close(ctx)
	wasi_snapshot_preview1.MustInstantiate(ctx, wasmRuntime)
	kaloHost := hostfuncs.NewKaloHostWithOptions(hostfuncs.HostOptions{Deterministic: options.Policy.Deterministic})
	defer kaloHost.Close()
	if err := kaloHost.Register(ctx, wasmRuntime); err != nil {
		return nil, fmt.Errorf("register Kalo host functions: %w", err)
	}
	cache := &pluginExecutionCache{compiledModules: make(map[string]wazero.CompiledModule)}

	var receiptPaths, receiptIDs []string
	var receiptRoute []specReceiptRouteNode
	var firstArtifact, lastArtifact specReceiptArtifact
	for index, step := range route.Steps {
		if err := catalog.ValidateValue(step.InputBinding, currentPath); err != nil {
			return nil, fmt.Errorf("Processor %s input boundary: %w", step.ProcessorAddress, err)
		}
		inputDigest, err := specv0.DigestFile(currentPath)
		if err != nil {
			return nil, err
		}
		inputBinding := catalog.Bindings[step.InputBinding]
		inputArtifact := specReceiptArtifact{
			Port: step.Processor.Inputs[0].Name, Contract: step.InputContract,
			ContractVersionID: step.InputContractVersionID, Binding: step.InputBinding,
			BindingVersionID: inputBinding.BindingVersionID, ArtifactDigest: inputDigest,
		}
		inputArtifact.Source, err = catalog.ReceiptSource(step.InputBinding, inputDigest)
		if err != nil {
			return nil, err
		}
		if index == 0 {
			firstArtifact = inputArtifact
		}

		stageDir := filepath.Join(workDir, fmt.Sprintf("%02d-%s", index+1, safePathPart(step.ProcessorAddress)))
		if err := os.MkdirAll(stageDir, 0o755); err != nil {
			return nil, err
		}
		stores := map[string]kconfig.Store{
			"SPEC_INPUT":  {Type: kconfig.StoreTypeLocalFileSystem, Format: step.InputBinding, Options: map[string]any{"path": filepath.Dir(currentPath)}},
			"SPEC_OUTPUT": {Type: kconfig.StoreTypeLocalFileSystem, Format: step.OutputBinding, Options: map[string]any{"path": stageDir}},
		}
		plugin := kconfig.PluginDefinition{
			Plugin: step.ImplementationID, Version: "0.1-alpha",
			Input:  &kconfig.PluginIOSpec{Format: step.InputBinding, Store: "SPEC_INPUT"},
			Output: &kconfig.PluginIOSpec{Format: step.OutputBinding, Store: "SPEC_OUTPUT"},
		}
		implementationPath, _, err := specv0.ResolveArtifact(step.Implementation.SourcePath, step.Implementation.Artifact.Ref)
		if err != nil {
			return nil, err
		}
		stageStarted := time.Now().UTC()
		pluginCtx := ctx
		cancel := func() {}
		if options.Policy.PluginTimeout > 0 {
			pluginCtx, cancel = context.WithTimeout(ctx, options.Policy.PluginTimeout)
		}
		stagePolicy := options.Policy
		stagePolicy.ReadOnlyInputs = true
		err = executePlugin(pluginCtx, wasmRuntime, kaloHost, cache, implementationPath, step.Implementation.Artifact.Digest, stores, plugin, stagePolicy)
		cancel()
		if options.Policy.PluginTimeout > 0 && pluginCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("Processor %s exceeded timeout %s", step.ProcessorAddress, options.Policy.PluginTimeout)
		}
		if err != nil {
			return nil, fmt.Errorf("Processor %s implementation %s failed: %w", step.ProcessorAddress, step.ImplementationID, err)
		}
		stageEnded := time.Now().UTC()
		stageOutput, err := singleRegularFile(stageDir)
		if err != nil {
			return nil, fmt.Errorf("Processor %s output: %w", step.ProcessorAddress, err)
		}
		if err := catalog.ValidateValue(step.OutputBinding, stageOutput); err != nil {
			return nil, fmt.Errorf("Processor %s output boundary: %w", step.ProcessorAddress, err)
		}
		outputDigest, err := specv0.DigestFile(stageOutput)
		if err != nil {
			return nil, err
		}
		outputBinding := catalog.Bindings[step.OutputBinding]
		outputArtifact := specReceiptArtifact{
			Port: step.Processor.Outputs[0].Name, Contract: step.OutputContract,
			ContractVersionID: step.OutputContractVersionID, Binding: step.OutputBinding,
			BindingVersionID: outputBinding.BindingVersionID, ArtifactDigest: outputDigest,
		}
		lastArtifact = outputArtifact
		evidenceOutput := filepath.Join(artifactsDir, fmt.Sprintf("%02d-%s", index+1, filepath.Base(stageOutput)))
		if err := copyFile(stageOutput, evidenceOutput); err != nil {
			return nil, err
		}

		receiptID, err := randomUUID()
		if err != nil {
			return nil, err
		}
		receipt := specReceipt{
			Schema: schemaRef, Spec: "0.1-alpha", Kind: "Receipt", ReceiptID: receiptID, ExecutionID: executionID,
			Subject: specReceiptSubject{Processor: step.ProcessorAddress, ProcessorVersionID: step.ProcessorVersionID, ImplementationID: step.ImplementationID, ImplementationDigest: step.ImplementationDigest},
			Status:  "succeeded", StartedAt: stageStarted.Format(time.RFC3339Nano), EndedAt: stageEnded.Format(time.RFC3339Nano),
			Inputs: []specReceiptArtifact{inputArtifact}, Outputs: []specReceiptArtifact{outputArtifact},
		}
		if len(receiptIDs) > 0 {
			receipt.ParentReceipts = []string{receiptIDs[len(receiptIDs)-1]}
		}
		receiptName := fmt.Sprintf("%02d-%s.json", index+1, safePathPart(step.ProcessorAddress))
		receiptPath := filepath.Join(receiptsDir, receiptName)
		if err := writeJSONFile(receiptPath, receipt); err != nil {
			return nil, err
		}
		if err := validateGeneratedReceipt(receiptPath); err != nil {
			return nil, err
		}
		receiptPaths = append(receiptPaths, filepath.ToSlash(filepath.Join("receipts", receiptName)))
		receiptIDs = append(receiptIDs, receiptID)
		receiptRoute = append(receiptRoute, specReceiptRouteNode{Node: fmt.Sprintf("step_%02d", index+1), Processor: step.ProcessorAddress, ProcessorVersionID: step.ProcessorVersionID, ReceiptID: receiptID})
		currentPath = stageOutput
	}

	compositionReceiptPath := ""
	if composition := catalog.MatchComposition(route); composition != nil {
		receiptID, err := randomUUID()
		if err != nil {
			return nil, err
		}
		receipt := specReceipt{
			Schema: schemaRef, Spec: "0.1-alpha", Kind: "Receipt", ReceiptID: receiptID, ExecutionID: executionID,
			Subject: specReceiptSubject{Composition: composition.Address, CompositionVersionID: composition.VersionID},
			Status:  "succeeded", StartedAt: executionStarted.Format(time.RFC3339Nano), EndedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Inputs: []specReceiptArtifact{firstArtifact}, Outputs: []specReceiptArtifact{lastArtifact}, Route: receiptRoute, ParentReceipts: receiptIDs,
		}
		path := filepath.Join(receiptsDir, "composition.json")
		if err := writeJSONFile(path, receipt); err != nil {
			return nil, err
		}
		if err := validateGeneratedReceipt(path); err != nil {
			return nil, err
		}
		compositionReceiptPath = filepath.ToSlash(filepath.Join("receipts", "composition.json"))
	}
	if err := os.RemoveAll(workDir); err != nil {
		return nil, fmt.Errorf("remove temporary SPEC work directory: %w", err)
	}

	result := &specExecutionResult{
		Spec: "0.1-alpha", ExecutionID: executionID, From: options.From, To: options.To,
		Input:  filepath.ToSlash(filepath.Join("artifacts", "00-"+filepath.Base(inputPath))),
		Output: filepath.ToSlash(filepath.Join("artifacts", fmt.Sprintf("%02d-%s", len(route.Steps), filepath.Base(currentPath)))),
		Route:  route, Receipts: receiptPaths, CompositionReceipt: compositionReceiptPath,
	}
	if err := writeJSONFile(filepath.Join(outputRoot, "execution.json"), result); err != nil {
		return nil, err
	}
	failed = false
	return result, nil
}

func writeJSON(cmd *cobra.Command, value any) error {
	encoder := json.NewEncoder(cmd.OutOrStdout())
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func validateGeneratedReceipt(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var header struct {
		Schema string `json:"$schema"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}
	return specv0.ValidateDocument(path, data, header.Schema)
}

func copyFile(source, destination string) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	return os.WriteFile(destination, data, 0o644)
}

func singleRegularFile(root string) (string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(files)
	if len(files) != 1 {
		return "", fmt.Errorf("filesystem-output capability requires exactly one output value, got %d", len(files))
	}
	return files[0], nil
}

func safePathPart(value string) string {
	value = strings.ToLower(value)
	replacer := strings.NewReplacer(":", "-", "@", "-", ".", "-", "/", "-")
	return strings.Trim(replacer.Replace(value), "-")
}

func randomUUID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	data[6] = (data[6] & 0x0f) | 0x40
	data[8] = (data[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(data)
	return fmt.Sprintf("%s-%s-%s-%s-%s", encoded[0:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:32]), nil
}
