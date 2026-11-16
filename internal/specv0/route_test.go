package specv0

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAddress(t *testing.T) {
	resource, err := ParseAddress("SPECX:PURCHASE@1")
	require.NoError(t, err)
	require.Equal(t, "SPECX", resource.Namespace)
	require.Equal(t, "PURCHASE", resource.Name)
	require.False(t, resource.IsBinding())

	binding, err := ParseAddress("SPECX:PURCHASE@1:JSON@1")
	require.NoError(t, err)
	require.True(t, binding.IsBinding())
	require.Equal(t, "JSON", binding.Binding)

	for _, invalid := range []string{"specx:PURCHASE@1", "SPECX:PURCHASE", "SPECX:PURCHASE@v1", "SPECX:PURCHASE@1:json@1"} {
		_, err := ParseAddress(invalid)
		require.Error(t, err, invalid)
	}
}

func TestRouteIsNominalDeterministicAndImplementationAware(t *testing.T) {
	catalog := routeTestCatalog()
	route, err := catalog.Route("TEST:A@1", "TEST:C@1", RouteOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"TEST:A-TO-B@1", "TEST:B-TO-C@1"}, []string{route.Steps[0].ProcessorAddress, route.Steps[1].ProcessorAddress})
	require.Equal(t, "TEST:A@1:JSON@1", route.Steps[0].InputBinding)
	require.Equal(t, "TEST:C@1:JSON@1", route.Steps[1].OutputBinding)

	// A shape-compatible but nominally different Contract is not substituted.
	_, err = catalog.Route("TEST:LOOKS-LIKE-A@1", "TEST:C@1", RouteOptions{})
	require.ErrorContains(t, err, "no compatible route")

	delete(catalog.Implementations, "TEST:B-TO-C@1")
	_, err = catalog.Route("TEST:A@1", "TEST:C@1", RouteOptions{})
	require.ErrorContains(t, err, "no supported ProcessorImplementation")
}

func TestRouteFailsClosedForStateEffectsAndExtensions(t *testing.T) {
	catalog := routeTestCatalog()
	processor := catalog.Processors["TEST:A-TO-B@1"]
	processor.State = []StatePort{{Name: "state", Contract: "TEST:A@1", Access: "read"}}
	_, err := catalog.Route("TEST:A@1", "TEST:C@1", RouteOptions{})
	require.ErrorContains(t, err, "required State Port")

	processor.State = nil
	processor.Effects = &Effects{Safe: false}
	_, err = catalog.Route("TEST:A@1", "TEST:C@1", RouteOptions{})
	require.ErrorContains(t, err, "unsafe effect")

	processor.Effects = &Effects{Safe: true, OpenWorld: true}
	_, err = catalog.Route("TEST:A@1", "TEST:C@1", RouteOptions{})
	require.ErrorContains(t, err, "open-world effect")

	processor.Resource.RequiredExtensions = []string{"TEST:UNKNOWN"}
	require.ErrorContains(t, catalog.checkExtensions(processor.Resource), "unsupported extension")
}

func routeTestCatalog() *Catalog {
	contracts := map[string]*Contract{}
	bindings := map[string]*Binding{}
	for _, name := range []string{"A", "B", "C", "LOOKS-LIKE-A"} {
		address := "TEST:" + name + "@1"
		versionID := name + "-version"
		contracts[address] = &Contract{Resource: Resource{Address: address, VersionID: versionID}}
		bindingAddress := address + ":JSON@1"
		bindings[bindingAddress] = &Binding{Resource: Resource{Address: bindingAddress}, Parent: address, BindingVersionID: name + "-binding-version"}
	}
	processor := func(address, input, output string) *Processor {
		return &Processor{
			Resource: Resource{Address: address, VersionID: address + "-version"},
			Inputs:   []Port{{Name: "input", Contract: input, Bindings: []string{input + ":JSON@1"}}},
			Outputs:  []Port{{Name: "output", Contract: output, Bindings: []string{output + ":JSON@1"}}},
			Effects:  &Effects{Safe: true, Idempotent: true},
		}
	}
	processors := map[string]*Processor{
		"TEST:A-TO-B@1": processor("TEST:A-TO-B@1", "TEST:A@1", "TEST:B@1"),
		"TEST:B-TO-C@1": processor("TEST:B-TO-C@1", "TEST:B@1", "TEST:C@1"),
	}
	implementations := map[string][]*Implementation{}
	for address, value := range processors {
		implementations[address] = []*Implementation{{
			ImplementationID: address + "-implementation", Processor: address,
			ProcessorVersionID: value.VersionID, Mechanism: "wasm-wasi-preview1",
			Artifact:          Artifact{Digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"},
			Capabilities:      []string{"filesystem-input", "filesystem-output"},
			SupportedBindings: []string{value.Inputs[0].Bindings[0], value.Outputs[0].Bindings[0]},
		}}
	}
	return &Catalog{Contracts: contracts, Bindings: bindings, Processors: processors, Implementations: implementations, SupportedExtensions: map[string]bool{}}
}
