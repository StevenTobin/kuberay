// Package v1alpha1 contains API types for the Ray module CR
// (components.platform.opendatahub.io/v1alpha1).
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion = schema.GroupVersion{
		Group:   "components.platform.opendatahub.io",
		Version: "v1alpha1",
	}

	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	AddToScheme = SchemeBuilder.AddToScheme
)
