package assembly

import (
	"context"
	"encoding/json"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/builtin"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/dispatcher"
)

type DispatcherProvider struct {
	registry *registry.Registry
	services registry.Services
}

func NewDispatcherProvider(services registry.Services, registrations ...registry.Registration) *DispatcherProvider {
	return &DispatcherProvider{
		registry: registry.New(registrations...),
		services: services,
	}
}

func (p *DispatcherProvider) Normalize(toolType string, settings json.RawMessage) (json.RawMessage, error) {
	return p.registry.Normalize(toolType, settings)
}

func (p *DispatcherProvider) NewDispatcher(
	ctx context.Context,
	_ aurora.RunContext,
	manifest aurora.Manifest,
) (dispatcher.Dispatcher[aurora.RunContext], error) {
	leaf := manifest.LeafTools()
	entries := make([]registry.Entry, 0, len(leaf))
	for _, tool := range leaf {
		entries = append(entries, registry.Entry{
			Name:     tool.Name,
			Type:     tool.Type,
			Settings: tool.Settings,
			Hidden:   tool.Hidden,
		})
	}
	config, err := p.registry.Build(ctx, entries, p.services)
	if err != nil {
		return nil, err
	}
	return builtin.New[aurora.RunContext](config), nil
}
