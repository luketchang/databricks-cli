package sync

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/databricks/cli/libs/log"
	"github.com/databricks/databricks-sdk-go/apierr"
)

// Default retry settings for transient HTTP 5xx responses during sync.
// Workspace deployments behind load balancers occasionally return 502/503
// during a sync; the SDK only retries 429/504 by default
// (see databricks-sdk-go httpclient/errors.go DefaultErrorRetriable),
// so we layer per-operation retries here.
const (
	DefaultMaxRetries     = 5
	defaultInitialBackoff = 1 * time.Second
	defaultMaxBackoff     = 8 * time.Second
)

// MaxRetriesFromFlag translates a user-facing --max-retries CLI flag value into
// the SyncOptions.MaxRetries contract: a user-supplied 0 means "no retries"
// and is mapped to -1 so it isn't treated as the unset zero value (which
// selects DefaultMaxRetries inside sync.New).
func MaxRetriesFromFlag(flagValue int) int {
	if flagValue == 0 {
		return -1
	}
	return flagValue
}

// retryableStatus reports whether an HTTP status is a transient gateway error
// that the sync layer should retry on top of the SDK's existing retries.
func retryableStatus(code int) bool {
	switch code {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// isRetryableSyncError reports whether an error from a sync filer call should
// be retried by the sync layer. Only transient gateway 5xx responses qualify;
// every other error (404, 409, auth, etc.) is fatal and surfaced immediately.
func isRetryableSyncError(err error) bool {
	if err == nil {
		return false
	}
	var aerr *apierr.APIError
	if errors.As(err, &aerr) && retryableStatus(aerr.StatusCode) {
		return true
	}
	return false
}

// retryOnTransient invokes fn, retrying transient gateway errors with capped
// exponential backoff. maxRetries is the number of additional attempts after
// the first; maxRetries=0 means no retries (single attempt).
func retryOnTransient(ctx context.Context, maxRetries int, label string, fn func() error) error {
	backoff := defaultInitialBackoff
	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = fn()
		if !isRetryableSyncError(err) {
			return err
		}
		if attempt == maxRetries {
			break
		}
		log.Debugf(ctx, "sync %s: retrying after transient error (attempt %d/%d): %s",
			label, attempt+1, maxRetries, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > defaultMaxBackoff {
			backoff = defaultMaxBackoff
		}
	}
	return err
}
