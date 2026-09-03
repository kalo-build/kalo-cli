package specv0

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	resourceAddressPattern = regexp.MustCompile(`^([A-Z][A-Z0-9-]{0,62}):([A-Z][A-Z0-9-]{0,127})@([0-9][A-Za-z0-9.-]*)$`)
	bindingAddressPattern  = regexp.MustCompile(`^([A-Z][A-Z0-9-]{0,62}):([A-Z][A-Z0-9-]{0,127})@([0-9][A-Za-z0-9.-]*):([A-Z][A-Z0-9-]{0,127})@([0-9][A-Za-z0-9.-]*)$`)
)

type Address struct {
	Raw            string `json:"address"`
	Namespace      string `json:"namespace"`
	Name           string `json:"name"`
	Version        string `json:"version"`
	Binding        string `json:"binding,omitempty"`
	BindingVersion string `json:"binding_version,omitempty"`
}

func (a Address) IsBinding() bool { return a.Binding != "" }

func ParseAddress(value string) (Address, error) {
	if match := bindingAddressPattern.FindStringSubmatch(value); match != nil {
		return Address{Raw: value, Namespace: match[1], Name: match[2], Version: match[3], Binding: match[4], BindingVersion: match[5]}, nil
	}
	if match := resourceAddressPattern.FindStringSubmatch(value); match != nil {
		return Address{Raw: value, Namespace: match[1], Name: match[2], Version: match[3]}, nil
	}
	return Address{}, fmt.Errorf("invalid SPEC resource address %q", value)
}

type Resource struct {
	Schema             string         `yaml:"$schema" json:"$schema,omitempty"`
	Spec               string         `yaml:"spec" json:"spec"`
	Kind               string         `yaml:"kind" json:"kind"`
	Address            string         `yaml:"address" json:"address,omitempty"`
	NamespaceID        string         `yaml:"namespace_id" json:"namespace_id,omitempty"`
	FamilyID           string         `yaml:"family_id" json:"family_id,omitempty"`
	VersionID          string         `yaml:"version_id" json:"version_id,omitempty"`
	Version            string         `yaml:"version" json:"version,omitempty"`
	Extensions         map[string]any `yaml:"extensions" json:"extensions,omitempty"`
	RequiredExtensions []string       `yaml:"required_extensions" json:"required_extensions,omitempty"`
	SourcePath         string         `yaml:"-" json:"-"`
}

type Namespace struct {
	Resource  `yaml:",inline"`
	Alias     string `yaml:"alias" json:"alias"`
	Authority any    `yaml:"authority" json:"authority,omitempty"`
}

type Artifact struct {
	Ref       string `yaml:"ref" json:"ref"`
	MediaType string `yaml:"media_type" json:"media_type,omitempty"`
	Digest    string `yaml:"digest" json:"digest,omitempty"`
}

type Definition struct {
	Body      string `yaml:"body" json:"body,omitempty"`
	Ref       string `yaml:"ref" json:"ref,omitempty"`
	MediaType string `yaml:"media_type" json:"media_type,omitempty"`
	Digest    string `yaml:"digest" json:"digest,omitempty"`
}

type ContractRef struct {
	Contract  string `yaml:"contract" json:"contract"`
	VersionID string `yaml:"version_id" json:"version_id,omitempty"`
}

type Relationship struct {
	Relation        string `yaml:"relation" json:"relation"`
	Target          string `yaml:"target" json:"target"`
	TargetVersionID string `yaml:"target_version_id" json:"target_version_id,omitempty"`
}

type Contract struct {
	Resource      `yaml:",inline"`
	Definition    Definition     `yaml:"definition" json:"definition"`
	Bindings      []string       `yaml:"bindings" json:"bindings,omitempty"`
	Dependencies  []ContractRef  `yaml:"dependencies" json:"dependencies,omitempty"`
	Relationships []Relationship `yaml:"relationships" json:"relationships,omitempty"`
}

type Representation struct {
	Standard string    `yaml:"standard" json:"standard"`
	Artifact *Artifact `yaml:"artifact" json:"artifact,omitempty"`
	Inline   any       `yaml:"inline" json:"inline,omitempty"`
}

type Binding struct {
	Resource         `yaml:",inline"`
	Parent           string         `yaml:"parent" json:"parent"`
	BindingID        string         `yaml:"binding_id" json:"binding_id"`
	BindingVersionID string         `yaml:"binding_version_id" json:"binding_version_id"`
	Binding          string         `yaml:"binding" json:"binding"`
	BindingVersion   string         `yaml:"binding_version" json:"binding_version"`
	MediaType        string         `yaml:"media_type" json:"media_type,omitempty"`
	Representation   Representation `yaml:"representation" json:"representation"`
}

type Port struct {
	Name              string   `yaml:"name" json:"name"`
	Contract          string   `yaml:"contract" json:"contract"`
	ContractVersionID string   `yaml:"contract_version_id" json:"contract_version_id,omitempty"`
	Bindings          []string `yaml:"bindings" json:"bindings,omitempty"`
	Required          *bool    `yaml:"required" json:"required,omitempty"`
}

type StatePort struct {
	Name              string `yaml:"name" json:"name"`
	Contract          string `yaml:"contract" json:"contract"`
	ContractVersionID string `yaml:"contract_version_id" json:"contract_version_id,omitempty"`
	Access            string `yaml:"access" json:"access"`
	Required          *bool  `yaml:"required" json:"required,omitempty"`
}

func (p StatePort) IsRequired() bool { return p.Required == nil || *p.Required }

type Effects struct {
	Safe       bool `yaml:"safe" json:"safe"`
	Idempotent bool `yaml:"idempotent" json:"idempotent"`
	OpenWorld  bool `yaml:"open_world" json:"open_world"`
}

type ImplementationRequirements struct {
	Mechanisms   []string `yaml:"mechanisms" json:"mechanisms,omitempty"`
	Capabilities []string `yaml:"capabilities" json:"capabilities,omitempty"`
	Locality     string   `yaml:"locality" json:"locality,omitempty"`
}

type Processor struct {
	Resource                   `yaml:",inline"`
	Definition                 *Definition                `yaml:"definition" json:"definition,omitempty"`
	Inputs                     []Port                     `yaml:"inputs" json:"inputs"`
	Outputs                    []Port                     `yaml:"outputs" json:"outputs"`
	State                      []StatePort                `yaml:"state" json:"state,omitempty"`
	Effects                    *Effects                   `yaml:"effects" json:"effects,omitempty"`
	ImplementationRequirements ImplementationRequirements `yaml:"implementation_requirements" json:"implementation_requirements,omitempty"`
}

type Implementation struct {
	Resource           `yaml:",inline"`
	ImplementationID   string   `yaml:"implementation_id" json:"implementation_id"`
	Processor          string   `yaml:"processor" json:"processor"`
	ProcessorVersionID string   `yaml:"processor_version_id" json:"processor_version_id"`
	Mechanism          string   `yaml:"mechanism" json:"mechanism"`
	Artifact           Artifact `yaml:"artifact" json:"artifact"`
	Capabilities       []string `yaml:"capabilities" json:"capabilities,omitempty"`
	SupportedBindings  []string `yaml:"supported_bindings" json:"supported_bindings,omitempty"`
}

type Endpoint struct {
	Node string `yaml:"node" json:"node"`
	Port string `yaml:"port" json:"port"`
}

type CompositionNode struct {
	ID                 string `yaml:"id" json:"id"`
	Processor          string `yaml:"processor" json:"processor"`
	ProcessorVersionID string `yaml:"processor_version_id" json:"processor_version_id"`
	ImplementationID   string `yaml:"implementation_id" json:"implementation_id,omitempty"`
}

type CompositionEdge struct {
	From              Endpoint `yaml:"from" json:"from"`
	To                Endpoint `yaml:"to" json:"to"`
	Contract          string   `yaml:"contract" json:"contract"`
	ContractVersionID string   `yaml:"contract_version_id" json:"contract_version_id"`
	Binding           string   `yaml:"binding" json:"binding,omitempty"`
	BindingVersionID  string   `yaml:"binding_version_id" json:"binding_version_id,omitempty"`
}

type CompositionBoundary struct {
	Name              string   `yaml:"name" json:"name"`
	To                Endpoint `yaml:"to" json:"to,omitempty"`
	From              Endpoint `yaml:"from" json:"from,omitempty"`
	Contract          string   `yaml:"contract" json:"contract"`
	ContractVersionID string   `yaml:"contract_version_id" json:"contract_version_id"`
	Binding           string   `yaml:"binding" json:"binding,omitempty"`
}

type Composition struct {
	Resource `yaml:",inline"`
	Nodes    []CompositionNode     `yaml:"nodes" json:"nodes"`
	Edges    []CompositionEdge     `yaml:"edges" json:"edges"`
	Inputs   []CompositionBoundary `yaml:"inputs" json:"inputs"`
	Outputs  []CompositionBoundary `yaml:"outputs" json:"outputs"`
}

type ReceiptSubject struct {
	Processor            string `yaml:"processor"`
	ProcessorVersionID   string `yaml:"processor_version_id"`
	ImplementationID     string `yaml:"implementation_id"`
	ImplementationDigest string `yaml:"implementation_digest"`
	Composition          string `yaml:"composition"`
	CompositionVersionID string `yaml:"composition_version_id"`
}

type ReceiptArtifact struct {
	Port              string `yaml:"port"`
	Contract          string `yaml:"contract"`
	ContractVersionID string `yaml:"contract_version_id"`
	Binding           string `yaml:"binding"`
	BindingVersionID  string `yaml:"binding_version_id"`
	ArtifactDigest    string `yaml:"artifact_digest"`
}

type ReceiptRouteNode struct {
	Node               string `yaml:"node"`
	Processor          string `yaml:"processor"`
	ProcessorVersionID string `yaml:"processor_version_id"`
	ReceiptID          string `yaml:"receipt_id"`
}

type Receipt struct {
	Resource       `yaml:",inline"`
	ReceiptID      string             `yaml:"receipt_id"`
	ExecutionID    string             `yaml:"execution_id"`
	Subject        ReceiptSubject     `yaml:"subject"`
	Inputs         []ReceiptArtifact  `yaml:"inputs"`
	Outputs        []ReceiptArtifact  `yaml:"outputs"`
	Route          []ReceiptRouteNode `yaml:"route"`
	ParentReceipts []string           `yaml:"parent_receipts"`
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func hasAll(values, wanted []string) bool {
	for _, item := range wanted {
		if !contains(values, item) {
			return false
		}
	}
	return true
}

func isJSONSchema(standard string) bool {
	return strings.Contains(strings.ToLower(standard), "json schema")
}
