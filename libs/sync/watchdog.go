package sync

import (
	"context"
	"errors"
	"io/fs"

	"github.com/databricks/cli/libs/filer"
	"github.com/databricks/cli/libs/log"
	"golang.org/x/sync/errgroup"
)

// Default maximum number of concurrent requests during sync.
// Callers can override this via SyncOptions.Concurrency (e.g. the --concurrency flag).
const MaxRequestsInFlight = 20

// Delete the specified path.
func (s *Sync) applyDelete(ctx context.Context, remoteName string) error {
	s.notifyProgress(ctx, EventActionDelete, remoteName, 0.0)

	if !s.DryRun {
		err := retryOnTransient(ctx, s.MaxRetries, "delete "+remoteName, func() error {
			return s.filer.Delete(ctx, remoteName)
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}

	s.notifyProgress(ctx, EventActionDelete, remoteName, 1.0)
	return nil
}

// Remove the directory at the specified path.
func (s *Sync) applyRmdir(ctx context.Context, remoteName string) error {
	s.notifyProgress(ctx, EventActionDelete, remoteName, 0.0)

	if !s.DryRun {
		err := retryOnTransient(ctx, s.MaxRetries, "rmdir "+remoteName, func() error {
			return s.filer.Delete(ctx, remoteName)
		})
		if err != nil {
			// Directory deletion is opportunistic, so we ignore errors.
			log.Debugf(ctx, "error removing directory %s: %s", remoteName, err)
		}
	}

	s.notifyProgress(ctx, EventActionDelete, remoteName, 1.0)
	return nil
}

// Create a directory at the specified path.
func (s *Sync) applyMkdir(ctx context.Context, localName string) error {
	s.notifyProgress(ctx, EventActionPut, localName, 0.0)

	if !s.DryRun {
		err := retryOnTransient(ctx, s.MaxRetries, "mkdir "+localName, func() error {
			return s.filer.Mkdir(ctx, localName)
		})
		if err != nil {
			return err
		}
	}

	s.notifyProgress(ctx, EventActionPut, localName, 1.0)
	return nil
}

// Perform a PUT of the specified local path.
func (s *Sync) applyPut(ctx context.Context, localName string) error {
	s.notifyProgress(ctx, EventActionPut, localName, 0.0)

	// Surface a missing/unreadable local file before any work, including in
	// dry-run, matching pre-retry behaviour.
	localFile, err := s.LocalRoot.Open(localName)
	if err != nil {
		return err
	}
	localFile.Close()

	if !s.DryRun {
		opts := []filer.WriteMode{filer.CreateParentDirectories, filer.OverwriteIfExists}
		err := retryOnTransient(ctx, s.MaxRetries, "put "+localName, func() error {
			// Re-open the file each attempt: fs.File is read-only and not
			// guaranteed to support Seek, and filer.Write consumes the reader.
			f, err := s.LocalRoot.Open(localName)
			if err != nil {
				return err
			}
			defer f.Close()
			return s.filer.Write(ctx, localName, f, opts...)
		})
		if err != nil {
			return err
		}
	}

	s.notifyProgress(ctx, EventActionPut, localName, 1.0)
	return nil
}

func groupRunSingle(ctx context.Context, group *errgroup.Group, fn func(context.Context, string) error, path string) {
	// Return early if the context has already been cancelled.
	select {
	case <-ctx.Done():
		return
	default:
		// Proceed.
	}

	group.Go(func() error {
		return fn(ctx, path)
	})
}

func groupRunParallel(ctx context.Context, paths []string, limit int, fn func(context.Context, string) error) error {
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(limit)

	for _, path := range paths {
		groupRunSingle(ctx, group, fn, path)
	}

	// Wait for goroutines to finish and return first non-nil error return if any.
	return group.Wait()
}

func (s *Sync) applyDiff(ctx context.Context, d diff) error {
	var err error
	limit := s.Concurrency

	// Delete files in parallel.
	err = groupRunParallel(ctx, d.delete, limit, s.applyDelete)
	if err != nil {
		return err
	}

	// Delete directories ordered by depth from leaf to root.
	for _, group := range d.groupedRmdir() {
		err = groupRunParallel(ctx, group, limit, s.applyRmdir)
		if err != nil {
			return err
		}
	}

	// Create directories (leafs only because intermediates are created automatically).
	for _, group := range d.groupedMkdir() {
		err = groupRunParallel(ctx, group, limit, s.applyMkdir)
		if err != nil {
			return err
		}
	}

	// Put files in parallel.
	err = groupRunParallel(ctx, d.put, limit, s.applyPut)

	return err
}
