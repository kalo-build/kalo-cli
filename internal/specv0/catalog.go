package specv0

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

type Catalog struct {
	Root                string
	Namespaces          map[string]*Namespace
	Contracts           map[string]*Contract
	Bindings            map[string]*Binding
	Processors          map[string]*Processor
	Implementations     map[string][]*Implementation
	Compositions        map[string]*Composition
	Receipts            []*Receipt
	SupportedExtensions map[string]bool
}

func Load(root string) (*Catalog, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	catalog := &Catalog{
		Root: abs, Namespaces: map[string]*Namespace{}, Contracts: map[string]*Contract{},
		Bindings: map[string]*Binding{}, Processors: map[string]*Processor{},
		Implementations: map[string][]*Implementation{}, Compositions: map[string]*Composition{},
		Receipts:            []*Receipt{},
		SupportedExtensions: map[string]bool{},
	}
	err = filepath.WalkDir(abs, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		var header Resource
		if err := yaml.Unmarshal(data, &header); err != nil {
			return fmt.Errorf("%s: decode descriptor: %w", path, err)
		}
		if header.Kind == "" {
			return nil
		}
		if header.Spec != "0.1-alpha" {
			return fmt.Errorf("%s: unsupported SPEC version %q", path, header.Spec)
		}
		if err := validateDescriptorSchema(path, data, header.Schema); err != nil {
			return err
		}
		header.SourcePath = path
		switch header.Kind {
		case "Namespace":
			var value Namespace
			if err := yaml.Unmarshal(data, &value); err != nil {
				return err
			}
			value.SourcePath = path
			if previous, exists := catalog.Namespaces[value.Alias]; exists {
				if previous.NamespaceID != value.NamespaceID {
					return fmt.Errorf("ambiguous namespace %s has identities %s and %s", value.Alias, previous.NamespaceID, value.NamespaceID)
				}
				return fmt.Errorf("duplicate Namespace alias %s", value.Alias)
			}
			catalog.Namespaces[value.Alias] = &value
		case "Contract":
			var value Contract
			if err := yaml.Unmarshal(data, &value); err != nil {
				return err
			}
			value.SourcePath = path
			if err := addAddressed(value.Address, value.VersionID, path, contractIDs(catalog)); err != nil {
				return err
			}
			catalog.Contracts[value.Address] = &value
		case "RepresentationBinding":
			var value Binding
			if err := yaml.Unmarshal(data, &value); err != nil {
				return err
			}
			value.SourcePath = path
			if _, exists := catalog.Bindings[value.Address]; exists {
				return fmt.Errorf("duplicate binding address %s", value.Address)
			}
			catalog.Bindings[value.Address] = &value
		case "Processor":
			var value Processor
			if err := yaml.Unmarshal(data, &value); err != nil {
				return err
			}
			value.SourcePath = path
			if _, exists := catalog.Processors[value.Address]; exists {
				return fmt.Errorf("duplicate Processor address %s", value.Address)
			}
			catalog.Processors[value.Address] = &value
		case "ProcessorImplementation":
			var value Implementation
			if err := yaml.Unmarshal(data, &value); err != nil {
				return err
			}
			value.SourcePath = path
			catalog.Implementations[value.Processor] = append(catalog.Implementations[value.Processor], &value)
		case "Composition":
			var value Composition
			if err := yaml.Unmarshal(data, &value); err != nil {
				return err
			}
			value.SourcePath = path
			if _, exists := catalog.Compositions[value.Address]; exists {
				return fmt.Errorf("duplicate Composition address %s", value.Address)
			}
			catalog.Compositions[value.Address] = &value
		case "Receipt":
			var value Receipt
			if err := yaml.Unmarshal(data, &value); err != nil {
				return err
			}
			value.SourcePath = path
			catalog.Receipts = append(catalog.Receipts, &value)
		default:
			return fmt.Errorf("%s: unsupported SPEC resource kind %q", path, header.Kind)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := catalog.validateLinks(); err != nil {
		return nil, err
	}
	return catalog, nil
}

func contractIDs(catalog *Catalog) map[string]string {
	result := make(map[string]string, len(catalog.Contracts))
	for address, value := range catalog.Contracts {
		result[address] = value.VersionID
	}
	return result
}

func addAddressed(address, versionID, path string, existing map[string]string) error {
	if _, err := ParseAddress(address); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if previous, exists := existing[address]; exists {
		if previous != versionID {
			return fmt.Errorf("immutable address %s has conflicting version identities", address)
		}
		return fmt.Errorf("duplicate resource address %s", address)
	}
	return nil
}

func validateDescriptorSchema(path string, data []byte, schemaRef string) error {
	if schemaRef == "" {
		return fmt.Errorf("%s: missing $schema", path)
	}
	schemaPath := filepath.Clean(filepath.Join(filepath.Dir(path), filepath.FromSlash(schemaRef)))
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(fileURL(schemaPath))
	if err != nil {
		return fmt.Errorf("%s: compile schema: %w", path, err)
	}
	var raw any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%s: decode: %w", path, err)
	}
	if err := schema.Validate(raw); err != nil {
		return fmt.Errorf("%s: schema validation: %w", path, err)
	}
	return nil
}

func fileURL(path string) string {
	abs, _ := filepath.Abs(path)
	value := filepath.ToSlash(abs)
	if runtime.GOOS == "windows" {
		value = "/" + value
	}
	return (&url.URL{Scheme: "file", Path: value}).String()
}

func (catalog *Catalog) validateLinks() error {
	seenIDs := map[string]string{}
	checkID := func(id, label string) error {
		if id == "" {
			return nil
		}
		if prior, ok := seenIDs[id]; ok && prior != label {
			return fmt.Errorf("immutable identity %s is reused by %s and %s", id, prior, label)
		}
		seenIDs[id] = label
		return nil
	}
	for alias, namespace := range catalog.Namespaces {
		if err := checkID(namespace.NamespaceID, "namespace "+alias); err != nil {
			return err
		}
		if err := catalog.checkExtensions(namespace.Resource); err != nil {
			return err
		}
	}
	for address, contract := range catalog.Contracts {
		parsed, err := validateVersionedAddress(contract.Resource)
		if err != nil {
			return err
		}
		namespace := catalog.Namespaces[parsed.Namespace]
		if namespace == nil || namespace.NamespaceID != contract.NamespaceID {
			return fmt.Errorf("%s: unresolved namespace identity", address)
		}
		if err := checkID(contract.VersionID, address); err != nil {
			return err
		}
		if err := validateDefinition(contract.SourcePath, contract.Definition); err != nil {
			return err
		}
		for _, dependency := range contract.Dependencies {
			target := catalog.Contracts[dependency.Contract]
			if target == nil || (dependency.VersionID != "" && target.VersionID != dependency.VersionID) {
				return fmt.Errorf("%s: unresolved dependency %s", address, dependency.Contract)
			}
		}
		for _, relationship := range contract.Relationships {
			target := catalog.Contracts[relationship.Target]
			if target == nil || (relationship.TargetVersionID != "" && target.VersionID != relationship.TargetVersionID) {
				return fmt.Errorf("%s: unresolved relationship target %s", address, relationship.Target)
			}
		}
		if err := catalog.checkExtensions(contract.Resource); err != nil {
			return err
		}
	}
	if err := catalog.validateDependencyCycles(); err != nil {
		return err
	}
	for address, binding := range catalog.Bindings {
		parsed, err := ParseAddress(address)
		if err != nil || !parsed.IsBinding() {
			return fmt.Errorf("invalid binding address %s", address)
		}
		parent := catalog.Contracts[binding.Parent]
		if parent == nil || parent.VersionID != binding.VersionID || parent.FamilyID != binding.FamilyID || parent.NamespaceID != binding.NamespaceID {
			return fmt.Errorf("%s: parent Contract identity mismatch", address)
		}
		if !contains(parent.Bindings, address) {
			return fmt.Errorf("%s: parent Contract does not list binding", address)
		}
		if err := checkID(binding.BindingVersionID, address+" binding version"); err != nil {
			return err
		}
		if binding.Representation.Artifact != nil {
			artifactPath, _, err := ResolveArtifact(binding.SourcePath, binding.Representation.Artifact.Ref)
			if err != nil {
				return err
			}
			if binding.Representation.Artifact.Digest != "" {
				if err := VerifyDigest(artifactPath, binding.Representation.Artifact.Digest); err != nil {
					return fmt.Errorf("%s: %w", address, err)
				}
			}
		}
		if err := catalog.checkExtensions(binding.Resource); err != nil {
			return err
		}
	}
	for address, processor := range catalog.Processors {
		parsed, err := validateVersionedAddress(processor.Resource)
		if err != nil {
			return err
		}
		namespace := catalog.Namespaces[parsed.Namespace]
		if namespace == nil || namespace.NamespaceID != processor.NamespaceID {
			return fmt.Errorf("%s: unresolved namespace identity", address)
		}
		if err := checkID(processor.VersionID, address); err != nil {
			return err
		}
		if processor.Definition != nil {
			if err := validateDefinition(processor.SourcePath, *processor.Definition); err != nil {
				return err
			}
		}
		for _, port := range append(append([]Port{}, processor.Inputs...), processor.Outputs...) {
			contract := catalog.Contracts[port.Contract]
			if contract == nil || (port.ContractVersionID != "" && contract.VersionID != port.ContractVersionID) {
				return fmt.Errorf("%s: unresolved port Contract %s", address, port.Contract)
			}
			for _, binding := range port.Bindings {
				if value := catalog.Bindings[binding]; value == nil || value.Parent != port.Contract {
					return fmt.Errorf("%s: incompatible port binding %s", address, binding)
				}
			}
		}
		for _, state := range processor.State {
			contract := catalog.Contracts[state.Contract]
			if contract == nil || (state.ContractVersionID != "" && contract.VersionID != state.ContractVersionID) {
				return fmt.Errorf("%s: unresolved State Port Contract %s", address, state.Contract)
			}
		}
		if err := catalog.checkExtensions(processor.Resource); err != nil {
			return err
		}
	}
	implementationIDs := map[string]*Implementation{}
	for processorAddress, implementations := range catalog.Implementations {
		processor := catalog.Processors[processorAddress]
		if processor == nil {
			return fmt.Errorf("unresolved ProcessorImplementation target %s", processorAddress)
		}
		for _, implementation := range implementations {
			if implementationIDs[implementation.ImplementationID] != nil {
				return fmt.Errorf("duplicate implementation identity %s", implementation.ImplementationID)
			}
			implementationIDs[implementation.ImplementationID] = implementation
			if implementation.ProcessorVersionID != processor.VersionID {
				return fmt.Errorf("implementation %s: Processor version mismatch", implementation.ImplementationID)
			}
			artifactPath, fragment, err := ResolveArtifact(implementation.SourcePath, implementation.Artifact.Ref)
			if err != nil {
				return err
			}
			if implementation.Artifact.Digest == "" {
				return fmt.Errorf("implementation %s: artifact digest is required", implementation.ImplementationID)
			}
			if err := VerifyDigest(artifactPath, implementation.Artifact.Digest); err != nil {
				return fmt.Errorf("implementation %s: %w", implementation.ImplementationID, err)
			}
			if fragment != "" {
				return fmt.Errorf("implementation %s: executable artifact ref must not contain a fragment", implementation.ImplementationID)
			}
			if err := catalog.checkExtensions(implementation.Resource); err != nil {
				return err
			}
		}
		sort.Slice(implementations, func(i, j int) bool { return implementations[i].ImplementationID < implementations[j].ImplementationID })
	}
	for address, composition := range catalog.Compositions {
		parsed, err := validateVersionedAddress(composition.Resource)
		if err != nil {
			return err
		}
		namespace := catalog.Namespaces[parsed.Namespace]
		if namespace == nil || namespace.NamespaceID != composition.NamespaceID {
			return fmt.Errorf("%s: unresolved namespace identity", address)
		}
		if err := checkID(composition.VersionID, address); err != nil {
			return err
		}
		if err := catalog.validateComposition(composition, implementationIDs); err != nil {
			return err
		}
		if err := catalog.checkExtensions(composition.Resource); err != nil {
			return err
		}
	}
	receiptIDs := map[string]*Receipt{}
	for _, receipt := range catalog.Receipts {
		if err := catalog.checkExtensions(receipt.Resource); err != nil {
			return err
		}
		if receiptIDs[receipt.ReceiptID] != nil {
			return fmt.Errorf("duplicate Receipt identity %s", receipt.ReceiptID)
		}
		receiptIDs[receipt.ReceiptID] = receipt
		if receipt.Subject.Processor != "" {
			processor := catalog.Processors[receipt.Subject.Processor]
			if processor == nil || processor.VersionID != receipt.Subject.ProcessorVersionID {
				return fmt.Errorf("Receipt %s: unresolved Processor subject", receipt.ReceiptID)
			}
			if receipt.Subject.ImplementationID != "" {
				implementation := implementationIDs[receipt.Subject.ImplementationID]
				if implementation == nil || implementation.Processor != processor.Address || implementation.Artifact.Digest != receipt.Subject.ImplementationDigest {
					return fmt.Errorf("Receipt %s: unresolved ProcessorImplementation subject", receipt.ReceiptID)
				}
			}
		}
		if receipt.Subject.Composition != "" {
			composition := catalog.Compositions[receipt.Subject.Composition]
			if composition == nil || composition.VersionID != receipt.Subject.CompositionVersionID {
				return fmt.Errorf("Receipt %s: unresolved Composition subject", receipt.ReceiptID)
			}
		}
		for _, artifact := range append(append([]ReceiptArtifact{}, receipt.Inputs...), receipt.Outputs...) {
			contract := catalog.Contracts[artifact.Contract]
			if contract == nil || contract.VersionID != artifact.ContractVersionID {
				return fmt.Errorf("Receipt %s: unresolved artifact Contract %s", receipt.ReceiptID, artifact.Contract)
			}
			if artifact.Binding != "" {
				binding := catalog.Bindings[artifact.Binding]
				if binding == nil || binding.Parent != artifact.Contract || binding.BindingVersionID != artifact.BindingVersionID {
					return fmt.Errorf("Receipt %s: unresolved artifact binding %s", receipt.ReceiptID, artifact.Binding)
				}
			}
		}
	}
	for _, receipt := range catalog.Receipts {
		for _, parent := range receipt.ParentReceipts {
			if receiptIDs[parent] == nil {
				return fmt.Errorf("Receipt %s: unresolved parent Receipt %s", receipt.ReceiptID, parent)
			}
		}
		for _, node := range receipt.Route {
			processor := catalog.Processors[node.Processor]
			nodeReceipt := receiptIDs[node.ReceiptID]
			if processor == nil || processor.VersionID != node.ProcessorVersionID || nodeReceipt == nil || nodeReceipt.Subject.Processor != node.Processor || nodeReceipt.Subject.ProcessorVersionID != node.ProcessorVersionID {
				return fmt.Errorf("Receipt %s: malformed route node %s", receipt.ReceiptID, node.Node)
			}
		}
		for index := 1; index < len(receipt.Route); index++ {
			previous := receiptIDs[receipt.Route[index-1].ReceiptID]
			current := receiptIDs[receipt.Route[index].ReceiptID]
			if !receiptArtifactsLink(previous.Outputs, current.Inputs) {
				return fmt.Errorf("Receipt %s: route nodes %s and %s have no linked artifact", receipt.ReceiptID, receipt.Route[index-1].Node, receipt.Route[index].Node)
			}
		}
	}
	return nil
}

func receiptArtifactsLink(outputs, inputs []ReceiptArtifact) bool {
	for _, output := range outputs {
		for _, input := range inputs {
			if output.Contract == input.Contract &&
				output.ContractVersionID == input.ContractVersionID &&
				output.Binding == input.Binding &&
				output.BindingVersionID == input.BindingVersionID &&
				output.ArtifactDigest == input.ArtifactDigest {
				return true
			}
		}
	}
	return false
}

func validateVersionedAddress(resource Resource) (Address, error) {
	parsed, err := ParseAddress(resource.Address)
	if err != nil || parsed.IsBinding() {
		return Address{}, fmt.Errorf("%s: invalid %s resource address %q", resource.SourcePath, resource.Kind, resource.Address)
	}
	if parsed.Version != resource.Version {
		return Address{}, fmt.Errorf("%s: address version %s does not match descriptor version %s", resource.Address, parsed.Version, resource.Version)
	}
	return parsed, nil
}

func validateDefinition(descriptorPath string, definition Definition) error {
	if definition.Ref == "" {
		return nil
	}
	path, _, err := ResolveArtifact(descriptorPath, definition.Ref)
	if err != nil {
		return err
	}
	if definition.Digest != "" {
		if err := VerifyDigest(path, definition.Digest); err != nil {
			return fmt.Errorf("%s definition: %w", descriptorPath, err)
		}
	}
	return nil
}

func (catalog *Catalog) validateDependencyCycles() error {
	state := map[string]uint8{}
	var visit func(string) error
	visit = func(address string) error {
		switch state[address] {
		case 1:
			return fmt.Errorf("Contract dependency cycle includes %s", address)
		case 2:
			return nil
		}
		state[address] = 1
		for _, dependency := range catalog.Contracts[address].Dependencies {
			if err := visit(dependency.Contract); err != nil {
				return err
			}
		}
		state[address] = 2
		return nil
	}
	addresses := make([]string, 0, len(catalog.Contracts))
	for address := range catalog.Contracts {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	for _, address := range addresses {
		if err := visit(address); err != nil {
			return err
		}
	}
	return nil
}

func (catalog *Catalog) validateComposition(composition *Composition, implementations map[string]*Implementation) error {
	nodes := map[string]CompositionNode{}
	for _, node := range composition.Nodes {
		if _, exists := nodes[node.ID]; exists {
			return fmt.Errorf("%s: duplicate composition node %s", composition.Address, node.ID)
		}
		processor := catalog.Processors[node.Processor]
		if processor == nil || processor.VersionID != node.ProcessorVersionID {
			return fmt.Errorf("%s: unresolved composition Processor %s", composition.Address, node.Processor)
		}
		if node.ImplementationID != "" {
			implementation := implementations[node.ImplementationID]
			if implementation == nil || implementation.Processor != node.Processor {
				return fmt.Errorf("%s: unresolved composition implementation %s", composition.Address, node.ImplementationID)
			}
		}
		nodes[node.ID] = node
	}
	adjacency := map[string][]string{}
	for _, edge := range composition.Edges {
		sourceNode, sourceOK := nodes[edge.From.Node]
		targetNode, targetOK := nodes[edge.To.Node]
		if !sourceOK || !targetOK {
			return fmt.Errorf("%s: composition edge references an unknown node", composition.Address)
		}
		sourcePort := findPort(catalog.Processors[sourceNode.Processor].Outputs, edge.From.Port)
		targetPort := findPort(catalog.Processors[targetNode.Processor].Inputs, edge.To.Port)
		if sourcePort == nil || targetPort == nil {
			return fmt.Errorf("%s: composition edge references an unknown port", composition.Address)
		}
		contract := catalog.Contracts[edge.Contract]
		if contract == nil || contract.VersionID != edge.ContractVersionID || sourcePort.Contract != edge.Contract || targetPort.Contract != edge.Contract {
			return fmt.Errorf("%s: incompatible semantic edge %s -> %s", composition.Address, edge.From.Node, edge.To.Node)
		}
		if edge.Binding != "" {
			binding := catalog.Bindings[edge.Binding]
			if binding == nil || binding.Parent != edge.Contract || binding.BindingVersionID != edge.BindingVersionID || !contains(sourcePort.Bindings, edge.Binding) || !contains(targetPort.Bindings, edge.Binding) {
				return fmt.Errorf("%s: incompatible representation edge %s", composition.Address, edge.Binding)
			}
		}
		adjacency[edge.From.Node] = append(adjacency[edge.From.Node], edge.To.Node)
	}
	for _, boundary := range composition.Inputs {
		node, ok := nodes[boundary.To.Node]
		if !ok {
			return fmt.Errorf("%s: input boundary %s references an unknown node", composition.Address, boundary.Name)
		}
		port := findPort(catalog.Processors[node.Processor].Inputs, boundary.To.Port)
		if err := catalog.validateBoundary(composition.Address, boundary, port); err != nil {
			return err
		}
	}
	for _, boundary := range composition.Outputs {
		node, ok := nodes[boundary.From.Node]
		if !ok {
			return fmt.Errorf("%s: output boundary %s references an unknown node", composition.Address, boundary.Name)
		}
		port := findPort(catalog.Processors[node.Processor].Outputs, boundary.From.Port)
		if err := catalog.validateBoundary(composition.Address, boundary, port); err != nil {
			return err
		}
	}
	visiting := map[string]uint8{}
	var visit func(string) error
	visit = func(node string) error {
		if visiting[node] == 1 {
			return fmt.Errorf("%s: composition contains a cycle at %s", composition.Address, node)
		}
		if visiting[node] == 2 {
			return nil
		}
		visiting[node] = 1
		for _, next := range adjacency[node] {
			if err := visit(next); err != nil {
				return err
			}
		}
		visiting[node] = 2
		return nil
	}
	for node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

func (catalog *Catalog) validateBoundary(compositionAddress string, boundary CompositionBoundary, port *Port) error {
	contract := catalog.Contracts[boundary.Contract]
	if port == nil || contract == nil || contract.VersionID != boundary.ContractVersionID || port.Contract != boundary.Contract {
		return fmt.Errorf("%s: incompatible composition boundary %s", compositionAddress, boundary.Name)
	}
	if boundary.Binding != "" {
		binding := catalog.Bindings[boundary.Binding]
		if binding == nil || binding.Parent != boundary.Contract || !contains(port.Bindings, boundary.Binding) {
			return fmt.Errorf("%s: incompatible boundary binding %s", compositionAddress, boundary.Binding)
		}
	}
	return nil
}

func findPort(ports []Port, name string) *Port {
	for index := range ports {
		if ports[index].Name == name {
			return &ports[index]
		}
	}
	return nil
}

func (catalog *Catalog) checkExtensions(resource Resource) error {
	for _, key := range resource.RequiredExtensions {
		if !catalog.SupportedExtensions[key] {
			return fmt.Errorf("%s requires unsupported extension %s", resource.SourcePath, key)
		}
	}
	return nil
}

func ResolveArtifact(descriptorPath, ref string) (string, string, error) {
	parsed, err := url.Parse(ref)
	if err != nil {
		return "", "", fmt.Errorf("%s: invalid artifact ref %q: %w", descriptorPath, ref, err)
	}
	if parsed.Scheme != "" && parsed.Scheme != "file" {
		return "", "", fmt.Errorf("%s: network artifact refs are unsupported", descriptorPath)
	}
	path := filepath.FromSlash(parsed.Path)
	if parsed.Scheme == "" {
		path = filepath.Join(filepath.Dir(descriptorPath), path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	if _, err := os.Stat(abs); err != nil {
		return "", "", fmt.Errorf("artifact %s: %w", abs, err)
	}
	return abs, parsed.Fragment, nil
}

func VerifyDigest(path, expected string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	actual := fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	if !strings.EqualFold(actual, expected) {
		return fmt.Errorf("artifact digest mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

func DigestFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), nil
}

func (catalog *Catalog) ValidateValue(bindingAddress, valuePath string) error {
	binding := catalog.Bindings[bindingAddress]
	if binding == nil {
		return fmt.Errorf("unknown Representation Binding %s", bindingAddress)
	}
	if !isJSONSchema(binding.Representation.Standard) {
		return fmt.Errorf("binding %s uses unsupported representation standard %q", bindingAddress, binding.Representation.Standard)
	}
	var schemaDoc any
	if binding.Representation.Inline != nil {
		schemaDoc = binding.Representation.Inline
	} else if binding.Representation.Artifact != nil {
		path, fragment, err := ResolveArtifact(binding.SourcePath, binding.Representation.Artifact.Ref)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &schemaDoc); err != nil {
			return fmt.Errorf("%s: decode JSON Schema: %w", path, err)
		}
		if fragment != "" {
			schemaDoc, err = resolveJSONPointer(schemaDoc, fragment)
			if err != nil {
				return fmt.Errorf("%s#%s: %w", path, fragment, err)
			}
		}
	} else {
		return fmt.Errorf("binding %s has no representation", bindingAddress)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	const schemaURL = "urn:kalo:spec:value-schema"
	if err := compiler.AddResource(schemaURL, schemaDoc); err != nil {
		return err
	}
	schema, err := compiler.Compile(schemaURL)
	if err != nil {
		return fmt.Errorf("binding %s: compile representation schema: %w", bindingAddress, err)
	}
	data, err := os.ReadFile(valuePath)
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("%s: decode represented value: %w", valuePath, err)
	}
	if err := schema.Validate(value); err != nil {
		return fmt.Errorf("%s violates %s: %w", valuePath, bindingAddress, err)
	}
	return nil
}

// ReceiptSource copies an optional, namespaced receipt_source annotation from
// a Representation Binding into a Receipt input. The extension key and source
// system are deliberately opaque to Kalo; this is generic provenance plumbing,
// not source-specific adapter behavior. The digest always identifies the exact
// execution value, never the schema or descriptor.
func (catalog *Catalog) ReceiptSource(bindingAddress, valueDigest string) (map[string]string, error) {
	binding := catalog.Bindings[bindingAddress]
	if binding == nil {
		return nil, fmt.Errorf("unknown Representation Binding %s", bindingAddress)
	}
	keys := make([]string, 0, len(binding.Extensions))
	for key := range binding.Extensions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var result map[string]string
	for _, key := range keys {
		extension, ok := binding.Extensions[key].(map[string]any)
		if !ok {
			continue
		}
		raw, ok := extension["receipt_source"].(map[string]any)
		if !ok {
			continue
		}
		candidate := map[string]string{"digest": valueDigest}
		for _, field := range []string{"system", "resource", "version"} {
			value, ok := raw[field].(string)
			if !ok || value == "" {
				return nil, fmt.Errorf("binding %s extension %s has invalid receipt_source.%s", bindingAddress, key, field)
			}
			candidate[field] = value
		}
		if result != nil {
			return nil, fmt.Errorf("binding %s declares multiple receipt_source annotations", bindingAddress)
		}
		result = candidate
	}
	return result, nil
}

func resolveJSONPointer(value any, fragment string) (any, error) {
	if fragment == "" {
		return value, nil
	}
	if !strings.HasPrefix(fragment, "/") {
		return nil, fmt.Errorf("only JSON Pointer fragments are supported")
	}
	current := value
	for _, raw := range strings.Split(strings.TrimPrefix(fragment, "/"), "/") {
		key := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		object, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("fragment segment %q does not select an object", key)
		}
		current, ok = object[key]
		if !ok {
			return nil, fmt.Errorf("fragment segment %q does not exist", key)
		}
	}
	return current, nil
}

func (catalog *Catalog) Counts() map[string]int {
	implementationCount := 0
	for _, values := range catalog.Implementations {
		implementationCount += len(values)
	}
	return map[string]int{"namespaces": len(catalog.Namespaces), "contracts": len(catalog.Contracts), "bindings": len(catalog.Bindings), "processors": len(catalog.Processors), "implementations": implementationCount, "compositions": len(catalog.Compositions), "receipts": len(catalog.Receipts)}
}

// SchemaPath resolves a sibling schema from one of the catalog's descriptor
// schema references. Generated Receipts can therefore stay portable relative
// to their descriptor set rather than using a Kalo-specific schema location.
func (catalog *Catalog) SchemaPath(name string) (string, error) {
	var resources []Resource
	for _, value := range catalog.Contracts {
		resources = append(resources, value.Resource)
	}
	for _, value := range catalog.Bindings {
		resources = append(resources, value.Resource)
	}
	for _, value := range catalog.Processors {
		resources = append(resources, value.Resource)
	}
	for _, values := range catalog.Implementations {
		for _, value := range values {
			resources = append(resources, value.Resource)
		}
	}
	for _, resource := range resources {
		if resource.Schema == "" || resource.SourcePath == "" {
			continue
		}
		candidate := filepath.Join(filepath.Dir(resource.SourcePath), filepath.FromSlash(resource.Schema))
		candidate = filepath.Join(filepath.Dir(candidate), name)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return filepath.Abs(candidate)
		}
	}
	return "", fmt.Errorf("catalog does not reference %s", name)
}

// ValidateDocument validates a generated descriptor-like document against its
// declared local JSON Schema.
func ValidateDocument(path string, data []byte, schemaRef string) error {
	return validateDescriptorSchema(path, data, schemaRef)
}
