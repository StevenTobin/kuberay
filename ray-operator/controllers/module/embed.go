package module

import "embed"

// ModuleManifests embeds the YAML manifests that the module controller deploys
// at runtime: ray.io CRDs, webhook configurations, and the SCC.
// Run `make sync-module-manifests` after CRD changes to keep them in sync.
//
//go:embed all:manifests
var ModuleManifests embed.FS
