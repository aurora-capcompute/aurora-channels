package assembly

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"aurora-capcompute/aurora"
)

type BrainProvider struct {
	defaultID string
	paths     map[string]string
}

func NewBrainProvider(defaultID string, paths map[string]string) (*BrainProvider, error) {
	defaultID = strings.TrimSpace(defaultID)
	if defaultID == "" {
		return nil, fmt.Errorf("default brain ID is required")
	}
	copied := make(map[string]string, len(paths))
	for id, path := range paths {
		id = strings.TrimSpace(id)
		path = strings.TrimSpace(path)
		if id == "" || path == "" {
			return nil, fmt.Errorf("brain ID and path are required")
		}
		copied[id] = path
	}
	if _, ok := copied[defaultID]; !ok {
		return nil, fmt.Errorf("default brain %q is not configured", defaultID)
	}
	return &BrainProvider{defaultID: defaultID, paths: copied}, nil
}

func SingleBrainProvider(path string) (*BrainProvider, error) {
	return NewBrainProvider(aurora.DefaultBrainID, map[string]string{
		aurora.DefaultBrainID: path,
	})
}

func (p *BrainProvider) DefaultID() string {
	return p.defaultID
}

func (p *BrainProvider) List(ctx context.Context) ([]aurora.BrainSource, error) {
	ids := make([]string, 0, len(p.paths))
	for id := range p.paths {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	sources := make([]aurora.BrainSource, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		wasm, err := os.ReadFile(p.paths[id])
		if err != nil {
			return nil, fmt.Errorf("read brain %q from %q: %w", id, p.paths[id], err)
		}
		sources = append(sources, aurora.BrainSource{ID: id, Wasm: wasm})
	}
	return sources, nil
}
