package hostfuncs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemNowDeterministic(t *testing.T) {
	host := NewKaloHostWithOptions(HostOptions{Deterministic: true})

	assert.Equal(t, int64(0), host.systemNow(context.Background()))
	assert.Equal(t, int64(0), host.systemNow(context.Background()))
}

func TestSystemNowDefaultsToWallClock(t *testing.T) {
	host := NewKaloHost()

	require.Positive(t, host.systemNow(context.Background()))
}
