package module

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"

	modulev1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/module/v1alpha1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, modulev1alpha1.AddToScheme(s))
	return s
}

func newRay(name string, mgmtState common.ManagementState) *modulev1alpha1.Ray {
	return &modulev1alpha1.Ray{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Generation: 1,
		},
		Spec: modulev1alpha1.RaySpec{
			ManagementSpec: common.ManagementSpec{
				ManagementState: mgmtState,
			},
		},
	}
}

func TestReconcile_NotFound(t *testing.T) {
	s := newScheme(t)
	cli := clientfake.NewClientBuilder().WithScheme(s).Build()

	r := &RayModuleReconciler{
		Client:     cli,
		Scheme:     s,
		OperatorNS: "opendatahub",
	}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent"},
	})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
}

func TestReconcile_RemovedWithoutFinalizer(t *testing.T) {
	ray := newRay("default-ray", common.Removed)
	s := newScheme(t)
	cli := clientfake.NewClientBuilder().
		WithScheme(s).
		WithObjects(ray).
		WithStatusSubresource(ray).
		Build()

	r := &RayModuleReconciler{
		Client:     cli,
		Scheme:     s,
		OperatorNS: "opendatahub",
	}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "default-ray"},
	})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
}

func TestOperatorVersion_Default(t *testing.T) {
	r := &RayModuleReconciler{}
	assert.Equal(t, "1.4.4", r.operatorVersion())
}

func TestOperatorVersion_FromRelatedImage(t *testing.T) {
	t.Setenv(envRelatedImage, "quay.io/opendatahub/kuberay-operator:v1.5.0")
	r := &RayModuleReconciler{}
	assert.Equal(t, "v1.5.0", r.operatorVersion())
}

func TestOperatorVersion_RelatedImageNoTag(t *testing.T) {
	t.Setenv(envRelatedImage, "quay.io/opendatahub/kuberay-operator")
	r := &RayModuleReconciler{}
	assert.Equal(t, "1.4.4", r.operatorVersion())
}

func TestPlatformType_Default(t *testing.T) {
	r := &RayModuleReconciler{}
	assert.Equal(t, defaultPlatformType, r.platformType())
}

func TestPlatformType_FromEnv(t *testing.T) {
	t.Setenv("ODH_PLATFORM_TYPE", "rhoai")
	r := &RayModuleReconciler{}
	assert.Equal(t, "rhoai", r.platformType())
}

func TestApplicationsNamespace_Default(t *testing.T) {
	r := &RayModuleReconciler{OperatorNS: "opendatahub"}
	assert.Equal(t, "opendatahub", r.ApplicationsNamespace())
}

func TestApplicationsNamespace_FromEnv(t *testing.T) {
	t.Setenv(envApplicationsNS, "redhat-ods-applications")
	r := &RayModuleReconciler{OperatorNS: "opendatahub"}
	assert.Equal(t, "redhat-ods-applications", r.ApplicationsNamespace())
}

func TestEmbedToKustomizeFS(t *testing.T) {
	kfs, err := embedToKustomizeFS(ModuleManifests, manifestsRoot)
	require.NoError(t, err)

	exists := kfs.Exists("manifests/kustomization.yaml")
	assert.True(t, exists, "kustomization.yaml should exist in embedded FS")

	exists = kfs.Exists("manifests/scc.yaml")
	assert.True(t, exists, "scc.yaml should exist in embedded FS")
}

func TestRenderManifests_Kubernetes(t *testing.T) {
	kfs, err := embedToKustomizeFS(ModuleManifests, manifestsRoot)
	require.NoError(t, err)

	r := &RayModuleReconciler{
		OperatorNS:  "test-ns",
		IsOpenShift: false,
		kustomizeFS: kfs,
	}

	resources, err := r.renderManifests()
	require.NoError(t, err)
	assert.NotEmpty(t, resources)

	for _, res := range resources {
		assert.NotEqual(t, "SecurityContextConstraints", res.GetKind(),
			"SCC should be filtered out on non-OpenShift")
	}

	hasCRD := false
	for _, res := range resources {
		if res.GetKind() == "CustomResourceDefinition" {
			hasCRD = true
			break
		}
	}
	assert.True(t, hasCRD, "CRDs should be present in rendered manifests")
}

func TestRenderManifests_OpenShift(t *testing.T) {
	kfs, err := embedToKustomizeFS(ModuleManifests, manifestsRoot)
	require.NoError(t, err)

	r := &RayModuleReconciler{
		OperatorNS:  "test-ns",
		IsOpenShift: true,
		kustomizeFS: kfs,
	}

	resources, err := r.renderManifests()
	require.NoError(t, err)
	assert.NotEmpty(t, resources)

	hasSCC := false
	for _, res := range resources {
		if res.GetKind() == "SecurityContextConstraints" {
			hasSCC = true
			break
		}
	}
	assert.True(t, hasSCC, "SCC should be present on OpenShift")
}

func TestRenderManifests_SCCUserRewrite(t *testing.T) {
	kfs, err := embedToKustomizeFS(ModuleManifests, manifestsRoot)
	require.NoError(t, err)

	r := &RayModuleReconciler{
		OperatorNS:  "my-custom-ns",
		IsOpenShift: true,
		kustomizeFS: kfs,
	}

	resources, err := r.renderManifests()
	require.NoError(t, err)

	for _, res := range resources {
		if res.GetKind() != "SecurityContextConstraints" {
			continue
		}
		users, found, err := unstructuredSliceToStrings(res.Object, "users")
		require.NoError(t, err)
		if !found {
			continue
		}
		for _, u := range users {
			if u == "system:serviceaccount:my-custom-ns:kuberay-operator" {
				return // SCC user correctly rewritten
			}
		}
	}
	t.Error("expected SCC users to contain the rewritten service account reference")
}

func TestPlatformObjectInterface(t *testing.T) {
	ray := &modulev1alpha1.Ray{}

	ray.SetConditions([]common.Condition{
		{
			Type:   string(common.ConditionTypeReady),
			Status: metav1.ConditionTrue,
		},
	})
	assert.Len(t, ray.GetConditions(), 1)
	assert.Equal(t, string(common.ConditionTypeReady), ray.GetConditions()[0].Type)

	status := ray.GetStatus()
	assert.NotNil(t, status)
	status.Phase = common.PhaseReady
	assert.Equal(t, common.PhaseReady, ray.Status.Phase)

	ray.SetReleaseStatus(common.ComponentReleaseStatus{
		Releases: []common.ComponentRelease{{Name: "test", Version: "1.0"}},
	})
	rs := ray.GetReleaseStatus()
	require.Len(t, rs.Releases, 1)
	assert.Equal(t, "test", rs.Releases[0].Name)
}

func TestNewRayModuleReconciler(t *testing.T) {
	s := newScheme(t)
	cli := clientfake.NewClientBuilder().WithScheme(s).Build()

	rec, err := NewRayModuleReconciler(cli, s, "opendatahub", ModuleManifests, false, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, rec.Deployer)
	assert.NotNil(t, rec.Collector)
	assert.NotNil(t, rec.kustomizeFS)
	assert.False(t, rec.IsOpenShift)
	assert.Equal(t, "opendatahub", rec.OperatorNS)
}

func TestRenderManifests_NamespaceInjection(t *testing.T) {
	kfs, err := embedToKustomizeFS(ModuleManifests, manifestsRoot)
	require.NoError(t, err)

	ns := "injected-ns"
	r := &RayModuleReconciler{
		OperatorNS:  ns,
		IsOpenShift: false,
		kustomizeFS: kfs,
	}

	resources, err := r.renderManifests()
	require.NoError(t, err)

	for _, res := range resources {
		if res.GetNamespace() != "" {
			assert.Equal(t, ns, res.GetNamespace(),
				"namespaced resource %s/%s should have injected namespace", res.GetKind(), res.GetName())
		}
	}
}

func TestEnvVarCleanup(t *testing.T) {
	os.Unsetenv(envRelatedImage)
	os.Unsetenv(envApplicationsNS)
	os.Unsetenv("ODH_PLATFORM_TYPE")

	r := &RayModuleReconciler{OperatorNS: "default"}
	assert.Equal(t, "1.4.4", r.operatorVersion())
	assert.Equal(t, defaultPlatformType, r.platformType())
	assert.Equal(t, "default", r.ApplicationsNamespace())
}

// unstructuredSliceToStrings extracts a []string from a nested unstructured path.
func unstructuredSliceToStrings(obj map[string]interface{}, key string) ([]string, bool, error) {
	raw, ok := obj[key]
	if !ok {
		return nil, false, nil
	}
	slice, ok := raw.([]interface{})
	if !ok {
		return nil, true, nil
	}
	var result []string
	for _, v := range slice {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result, true, nil
}
