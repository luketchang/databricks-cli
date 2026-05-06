package sync

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/stretchr/testify/require"
)

func apiErr(status int) error {
	return &apierr.APIError{
		StatusCode: status,
		Message:    http.StatusText(status),
	}
}

func TestIsRetryableSyncError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not retryable", nil, false},
		{"502 is retryable", apiErr(http.StatusBadGateway), true},
		{"503 is retryable", apiErr(http.StatusServiceUnavailable), true},
		{"504 is retryable", apiErr(http.StatusGatewayTimeout), true},
		{"500 is not retryable", apiErr(http.StatusInternalServerError), false},
		{"429 is not retried by sync layer (SDK handles it)", apiErr(http.StatusTooManyRequests), false},
		{"404 is not retryable", apiErr(http.StatusNotFound), false},
		{"non-API error is not retryable", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isRetryableSyncError(tc.err))
		})
	}
}

func TestRetryOnTransient_RetriesUntilSuccess(t *testing.T) {
	calls := 0
	err := retryOnTransient(t.Context(), 5, "test", func() error {
		calls++
		if calls < 3 {
			return apiErr(http.StatusBadGateway)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

func TestRetryOnTransient_GivesUpAfterMaxRetries(t *testing.T) {
	calls := 0
	err := retryOnTransient(t.Context(), 2, "test", func() error {
		calls++
		return apiErr(http.StatusBadGateway)
	})
	require.Error(t, err)
	// Total attempts = maxRetries + 1.
	require.Equal(t, 3, calls)
	var aerr *apierr.APIError
	require.ErrorAs(t, err, &aerr)
	require.Equal(t, http.StatusBadGateway, aerr.StatusCode)
}

func TestRetryOnTransient_DoesNotRetryNonTransient(t *testing.T) {
	calls := 0
	err := retryOnTransient(t.Context(), 5, "test", func() error {
		calls++
		return apiErr(http.StatusNotFound)
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestRetryOnTransient_ZeroMaxRetriesDisablesRetry(t *testing.T) {
	calls := 0
	err := retryOnTransient(t.Context(), 0, "test", func() error {
		calls++
		return apiErr(http.StatusBadGateway)
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestRetryOnTransient_HonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	err := retryOnTransient(ctx, 5, "test", func() error {
		calls++
		return apiErr(http.StatusBadGateway)
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls, "should run once, then bail on cancelled ctx before backoff")
}
