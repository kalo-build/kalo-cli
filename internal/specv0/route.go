package specv0

import (
	"fmt"
	"sort"
)

type RouteOptions struct {
	AllowUnsafe    bool
	AllowOpenWorld bool
}

type RouteStep struct {
	Processor               *Processor      `json:"-"`
	Implementation          *Implementation `json:"-"`
	ProcessorAddress        string          `json:"processor"`
	ProcessorVersionID      string          `json:"processor_version_id"`
	ImplementationID        string          `json:"implementation_id"`
	ImplementationDigest    string          `json:"implementation_digest"`
	InputContract           string          `json:"input_contract"`
	InputContractVersionID  string          `json:"input_contract_version_id"`
	InputBinding            string          `json:"input_binding"`
	OutputContract          string          `json:"output_contract"`
	OutputContractVersionID string          `json:"output_contract_version_id"`
	OutputBinding           string          `json:"output_binding"`
}

type Route struct {
	From  string      `json:"from"`
	To    string      `json:"to"`
	Steps []RouteStep `json:"steps"`
}

type searchState struct {
	contract string
	binding  string
	steps    []RouteStep
}

func (catalog *Catalog) Route(from, to string, options RouteOptions) (*Route, error) {
	fromContract, fromBinding, err := catalog.resolveEndpoint(from)
	if err != nil {
		return nil, err
	}
	toContract, toBinding, err := catalog.resolveEndpoint(to)
	if err != nil {
		return nil, err
	}
	processors := make([]*Processor, 0, len(catalog.Processors))
	for _, processor := range catalog.Processors {
		processors = append(processors, processor)
	}
	sort.Slice(processors, func(i, j int) bool { return processors[i].Address < processors[j].Address })
	queue := []searchState{{contract: fromContract, binding: fromBinding}}
	visited := map[string]bool{fromContract + "|" + fromBinding: true}
	var rejected []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.contract == toContract && (toBinding == "" || current.binding == toBinding) {
			return &Route{From: from, To: to, Steps: current.steps}, nil
		}
		for _, processor := range processors {
			if len(processor.Inputs) != 1 || len(processor.Outputs) != 1 {
				continue
			}
			input := processor.Inputs[0]
			if input.Contract != current.contract {
				continue
			}
			inputBinding := current.binding
			if inputBinding == "" {
				inputBinding = firstExistingBinding(catalog, input.Bindings)
			}
			if inputBinding == "" || !contains(input.Bindings, inputBinding) {
				continue
			}
			output := processor.Outputs[0]
			outputBindings := append([]string(nil), output.Bindings...)
			sort.Strings(outputBindings)
			for _, outputBinding := range outputBindings {
				if catalog.Bindings[outputBinding] == nil {
					continue
				}
				implementation, reason := catalog.selectImplementation(processor, inputBinding, outputBinding, options)
				if implementation == nil {
					rejected = append(rejected, processor.Address+": "+reason)
					continue
				}
				step := RouteStep{
					Processor: processor, Implementation: implementation,
					ProcessorAddress: processor.Address, ProcessorVersionID: processor.VersionID,
					ImplementationID: implementation.ImplementationID, ImplementationDigest: implementation.Artifact.Digest,
					InputContract: input.Contract, InputContractVersionID: catalog.Contracts[input.Contract].VersionID, InputBinding: inputBinding,
					OutputContract: output.Contract, OutputContractVersionID: catalog.Contracts[output.Contract].VersionID, OutputBinding: outputBinding,
				}
				key := output.Contract + "|" + outputBinding
				if visited[key] {
					continue
				}
				visited[key] = true
				nextSteps := append(append([]RouteStep(nil), current.steps...), step)
				queue = append(queue, searchState{contract: output.Contract, binding: outputBinding, steps: nextSteps})
			}
		}
	}
	if len(rejected) > 0 {
		sort.Strings(rejected)
		return nil, fmt.Errorf("no compatible route from %s to %s; rejected requirements: %v", from, to, rejected)
	}
	return nil, fmt.Errorf("no compatible route from %s to %s", from, to)
}

func firstExistingBinding(catalog *Catalog, bindings []string) string {
	values := append([]string(nil), bindings...)
	sort.Strings(values)
	for _, binding := range values {
		if catalog.Bindings[binding] != nil {
			return binding
		}
	}
	return ""
}

func (catalog *Catalog) resolveEndpoint(value string) (string, string, error) {
	address, err := ParseAddress(value)
	if err != nil {
		return "", "", err
	}
	if address.IsBinding() {
		binding := catalog.Bindings[value]
		if binding == nil {
			return "", "", fmt.Errorf("unknown Representation Binding %s", value)
		}
		return binding.Parent, value, nil
	}
	if catalog.Contracts[value] == nil {
		return "", "", fmt.Errorf("unknown Contract %s", value)
	}
	return value, "", nil
}

func (catalog *Catalog) selectImplementation(processor *Processor, inputBinding, outputBinding string, options RouteOptions) (*Implementation, string) {
	if len(processor.State) > 0 {
		for _, port := range processor.State {
			if port.IsRequired() {
				return nil, "required State Port " + port.Name + " is unsupported"
			}
		}
	}
	if processor.Effects == nil {
		return nil, "effect declaration is required for execution"
	}
	if !processor.Effects.Safe && !options.AllowUnsafe {
		return nil, "unsafe effect is not authorized"
	}
	if processor.Effects.OpenWorld && !options.AllowOpenWorld {
		return nil, "open-world effect is not authorized"
	}
	if processor.ImplementationRequirements.Locality == "remote" {
		return nil, "remote implementations are unsupported"
	}
	for _, implementation := range catalog.Implementations[processor.Address] {
		if implementation.Mechanism != "wasm-wasi-preview1" {
			continue
		}
		if len(processor.ImplementationRequirements.Mechanisms) > 0 && !contains(processor.ImplementationRequirements.Mechanisms, implementation.Mechanism) {
			continue
		}
		if !hasAll(implementation.Capabilities, processor.ImplementationRequirements.Capabilities) {
			continue
		}
		if !hasAll(implementation.Capabilities, []string{"filesystem-input", "filesystem-output"}) {
			continue
		}
		if !contains(implementation.SupportedBindings, inputBinding) || !contains(implementation.SupportedBindings, outputBinding) {
			continue
		}
		return implementation, ""
	}
	return nil, "no supported ProcessorImplementation"
}

func (catalog *Catalog) MatchComposition(route *Route) *Composition {
	addresses := make([]string, 0, len(catalog.Compositions))
	for address := range catalog.Compositions {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	for _, address := range addresses {
		composition := catalog.Compositions[address]
		if len(composition.Nodes) != len(route.Steps) || len(composition.Inputs) != 1 || len(composition.Outputs) != 1 {
			continue
		}
		nodes := map[string]CompositionNode{}
		for _, node := range composition.Nodes {
			nodes[node.ID] = node
		}
		current := composition.Inputs[0].To.Node
		matched := true
		for index, step := range route.Steps {
			node, ok := nodes[current]
			if !ok || node.Processor != step.ProcessorAddress || node.ProcessorVersionID != step.ProcessorVersionID {
				matched = false
				break
			}
			if index < len(route.Steps)-1 {
				next := ""
				for _, edge := range composition.Edges {
					if edge.From.Node == current {
						if next != "" {
							matched = false
							break
						}
						next = edge.To.Node
					}
				}
				if next == "" {
					matched = false
					break
				}
				current = next
			}
		}
		if matched && composition.Inputs[0].Contract == route.Steps[0].InputContract && composition.Outputs[0].Contract == route.Steps[len(route.Steps)-1].OutputContract {
			return composition
		}
	}
	return nil
}
