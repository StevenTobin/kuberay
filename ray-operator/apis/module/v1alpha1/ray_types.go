package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
)

const (
	RayKind         = "Ray"
	RayInstanceName = "default-ray"
)

// RaySpec defines the desired state of the Ray module.
type RaySpec struct {
	common.ManagementSpec `json:",inline"`
}

// RayStatus defines the observed state of the Ray module.
type RayStatus struct {
	common.Status                 `json:",inline"`
	common.ComponentReleaseStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:validation:XValidation:rule="self.metadata.name == 'default-ray'",message="Ray name must be default-ray"

// Ray is the module CR reconciled by the KubeRay module operator.
type Ray struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RaySpec   `json:"spec,omitempty"`
	Status RayStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RayList contains a list of Ray.
type RayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Ray `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Ray{}, &RayList{})
}

// --- PlatformObject interface implementation ---

func (r *Ray) GetStatus() *common.Status {
	return &r.Status.Status
}

func (r *Ray) GetConditions() []common.Condition {
	return r.Status.Conditions
}

func (r *Ray) SetConditions(conditions []common.Condition) {
	r.Status.Conditions = conditions
}

func (r *Ray) GetReleaseStatus() *common.ComponentReleaseStatus {
	return &r.Status.ComponentReleaseStatus
}

func (r *Ray) SetReleaseStatus(status common.ComponentReleaseStatus) {
	r.Status.ComponentReleaseStatus = status
}

// Compile-time PlatformObject conformance check.
var _ common.PlatformObject = &Ray{}
