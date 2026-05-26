package module

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/controller/conditions"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/controller/gc"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/deploy"
	odhLabels "github.com/opendatahub-io/odh-platform-utilities/pkg/metadata/labels"
	"github.com/opendatahub-io/odh-platform-utilities/pkg/render/kustomize"

	modulev1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/module/v1alpha1"
)

const (
	finalizerName = "components.platform.opendatahub.io/ray-cleanup"
	fieldOwner    = "kuberay-module-controller"
	requeuePeriod = 30 * time.Second
	manifestsRoot = "manifests"

	envRelatedImage     = "RELATED_IMAGE_ODH_KUBERAY_OPERATOR_CONTROLLER_IMAGE"
	envApplicationsNS   = "APPLICATIONS_NAMESPACE"
	defaultPlatformType = "OpenDataHub"
)

// RayModuleReconciler reconciles the Ray module CR
// (components.platform.opendatahub.io/v1alpha1, kind: Ray).
type RayModuleReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	OperatorNS      string
	ManifestsFS     embed.FS
	Deployer        *deploy.Deployer
	Collector       *gc.Collector
	DynamicClient   dynamic.Interface
	DiscoveryClient discovery.DiscoveryInterface
	IsOpenShift     bool
	kustomizeFS     filesys.FileSystem
}

// NewRayModuleReconciler creates a new reconciler for the Ray module CR.
func NewRayModuleReconciler(
	cli client.Client,
	scheme *runtime.Scheme,
	operatorNS string,
	manifestsFS embed.FS,
	isOpenShift bool,
	dynamicClient dynamic.Interface,
	discoveryClient discovery.DiscoveryInterface,
) (*RayModuleReconciler, error) {
	kfs, err := embedToKustomizeFS(manifestsFS, manifestsRoot)
	if err != nil {
		return nil, fmt.Errorf("building kustomize filesystem from embedded manifests: %w", err)
	}

	deployer := deploy.NewDeployer(
		deploy.WithFieldOwner(fieldOwner),
		deploy.WithMode(deploy.ModeSSA),
		deploy.WithApplyOrder(),
		deploy.WithCache(),
	)

	collector := gc.New(
		gc.InNamespace(operatorNS),
	)

	return &RayModuleReconciler{
		Client:          cli,
		Scheme:          scheme,
		OperatorNS:      operatorNS,
		ManifestsFS:     manifestsFS,
		Deployer:        deployer,
		Collector:       collector,
		DynamicClient:   dynamicClient,
		DiscoveryClient: discoveryClient,
		IsOpenShift:     isOpenShift,
		kustomizeFS:     kfs,
	}, nil
}

// SetupWithManager registers the controller with the manager.
func (r *RayModuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&modulev1alpha1.Ray{}).
		Named("ray-module").
		Complete(r)
}

// +kubebuilder:rbac:groups=components.platform.opendatahub.io,resources=rays,verbs=get;list;watch
// +kubebuilder:rbac:groups=components.platform.opendatahub.io,resources=rays/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=components.platform.opendatahub.io,resources=rays/finalizers,verbs=update
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=create;get;list;patch;update;watch
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=create;delete;get;list;patch;update;watch
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=create;delete;get;list;patch;update;watch

func (r *RayModuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var ray modulev1alpha1.Ray
	if err := r.Get(ctx, req.NamespacedName, &ray); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if ray.Spec.ManagementState == common.Removed {
		return r.reconcileRemoved(ctx, l, &ray)
	}

	return r.reconcileManaged(ctx, l, &ray)
}

func (r *RayModuleReconciler) reconcileManaged(ctx context.Context, l logr.Logger, ray *modulev1alpha1.Ray) (ctrl.Result, error) {
	condMgr := conditions.NewManager(
		ray,
		string(common.ConditionTypeReady),
		string(common.ConditionTypeProvisioningSucceeded),
	)

	if !controllerutil.ContainsFinalizer(ray, finalizerName) {
		controllerutil.AddFinalizer(ray, finalizerName)
		if err := r.Update(ctx, ray); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
	}

	// B0: Render manifests via platform-utilities kustomize engine
	resources, err := r.renderManifests()
	if err != nil {
		condMgr.MarkFalse(
			string(common.ConditionTypeProvisioningSucceeded),
			conditions.WithReason("RenderFailed"),
			conditions.WithError(err),
			conditions.WithObservedGeneration(ray.Generation),
		)
		condMgr.Sort()
		return ctrl.Result{}, r.persistStatus(ctx, ray, condMgr)
	}

	// B0: Deploy via SSA using platform-utilities deployer
	version := r.operatorVersion()
	if err := r.Deployer.Deploy(ctx, deploy.DeployInput{
		Client:    r.Client,
		Owner:     ray,
		Resources: resources,
		Release: deploy.ReleaseInfo{
			Type:    r.platformType(),
			Version: version,
		},
	}); err != nil {
		condMgr.MarkFalse(
			string(common.ConditionTypeProvisioningSucceeded),
			conditions.WithReason("DeployFailed"),
			conditions.WithError(err),
			conditions.WithObservedGeneration(ray.Generation),
		)
		condMgr.Sort()
		if statusErr := r.persistStatus(ctx, ray, condMgr); statusErr != nil {
			l.Error(statusErr, "failed to update status after deploy error")
		}
		return ctrl.Result{RequeueAfter: requeuePeriod}, fmt.Errorf("deploying resources: %w", err)
	}

	// B0: GC stale resources from previous versions
	if err := r.Collector.Run(ctx, gc.RunParams{
		Client:          r.Client,
		DynamicClient:   r.DynamicClient,
		DiscoveryClient: r.DiscoveryClient,
		Owner:           ray,
		Version:         version,
		PlatformType:    r.platformType(),
	}); err != nil {
		l.Error(err, "garbage collection failed (non-fatal)")
	}

	// B2: Update PlatformObject-compliant status
	condMgr.MarkTrue(
		string(common.ConditionTypeProvisioningSucceeded),
		conditions.WithReason("Deployed"),
		conditions.WithMessage("All module resources deployed successfully"),
		conditions.WithObservedGeneration(ray.Generation),
	)
	condMgr.Sort()

	ray.Status.ObservedGeneration = ray.Generation
	ray.Status.Phase = common.PhaseReady
	ray.SetReleaseStatus(common.ComponentReleaseStatus{
		Releases: []common.ComponentRelease{
			{
				Name:    "kuberay-operator",
				Version: version,
			},
		},
	})

	if err := r.Client.Status().Update(ctx, ray); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	l.Info("reconciliation complete")
	return ctrl.Result{RequeueAfter: requeuePeriod}, nil
}

func (r *RayModuleReconciler) reconcileRemoved(ctx context.Context, l logr.Logger, ray *modulev1alpha1.Ray) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(ray, finalizerName) {
		return ctrl.Result{}, nil
	}

	l.Info("cleaning up module operands")

	if err := r.deleteOperands(ctx, l); err != nil {
		condMgr := conditions.NewManager(
			ray,
			string(common.ConditionTypeReady),
			string(common.ConditionTypeProvisioningSucceeded),
		)
		condMgr.MarkFalse(
			string(common.ConditionTypeProvisioningSucceeded),
			conditions.WithReason("CleanupFailed"),
			conditions.WithError(err),
			conditions.WithObservedGeneration(ray.Generation),
		)
		condMgr.Sort()
		if statusErr := r.persistStatus(ctx, ray, condMgr); statusErr != nil {
			l.Error(statusErr, "failed to update status during cleanup")
		}
		return ctrl.Result{RequeueAfter: requeuePeriod}, fmt.Errorf("cleaning up operands: %w", err)
	}

	controllerutil.RemoveFinalizer(ray, finalizerName)
	if err := r.Update(ctx, ray); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}

	l.Info("cleanup complete, finalizer removed")
	return ctrl.Result{}, nil
}

// persistStatus writes the current conditions, phase, and observedGeneration.
func (r *RayModuleReconciler) persistStatus(ctx context.Context, ray *modulev1alpha1.Ray, condMgr *conditions.Manager) error {
	ray.Status.ObservedGeneration = ray.Generation

	if condMgr.IsHappy() {
		ray.Status.Phase = common.PhaseReady
	} else {
		ray.Status.Phase = common.PhaseNotReady
	}

	return r.Client.Status().Update(ctx, ray)
}

// --- B3: Platform-injected configuration ---

func (r *RayModuleReconciler) operatorVersion() string {
	if img := os.Getenv(envRelatedImage); img != "" {
		if parts := strings.SplitN(img, ":", 2); len(parts) == 2 {
			return parts[1]
		}
	}
	return "1.4.4"
}

func (r *RayModuleReconciler) platformType() string {
	if pt := os.Getenv("ODH_PLATFORM_TYPE"); pt != "" {
		return pt
	}
	return defaultPlatformType
}

// ApplicationsNamespace returns the platform applications namespace from
// the environment, falling back to the operator namespace.
func (r *RayModuleReconciler) ApplicationsNamespace() string {
	if ns := os.Getenv(envApplicationsNS); ns != "" {
		return ns
	}
	return r.OperatorNS
}

// --- B0: Manifest rendering via platform-utilities kustomize engine ---

// renderManifests uses the platform-utilities kustomize renderer to render
// the embedded overlay with namespace injection, platform-conditional filtering,
// and SCC user patching.
func (r *RayModuleReconciler) renderManifests() ([]unstructured.Unstructured, error) {
	filters := []kustomize.FilterFn{
		r.platformFilter(),
	}

	if r.IsOpenShift {
		filters = append(filters, r.sccUserFilter())
	}

	return kustomize.Render(
		manifestsRoot,
		[]kustomize.EngineOptsFn{kustomize.WithEngineFS(r.kustomizeFS)},
		kustomize.WithNamespace(r.OperatorNS),
		kustomize.WithLabel(odhLabels.PlatformPartOf, "ray"),
		kustomize.WithFilters(filters...),
	)
}

// platformFilter removes resources that are not applicable to the current
// cluster type. On non-OpenShift clusters, SCC resources are stripped.
func (r *RayModuleReconciler) platformFilter() kustomize.FilterFn {
	return func(nodes []*kyaml.RNode) ([]*kyaml.RNode, error) {
		var filtered []*kyaml.RNode
		for _, node := range nodes {
			if !r.IsOpenShift && node.GetKind() == "SecurityContextConstraints" {
				continue
			}
			filtered = append(filtered, node)
		}
		return filtered, nil
	}
}

// sccUserFilter rewrites the SCC users field to reference the operator's
// service account in the correct namespace.
func (r *RayModuleReconciler) sccUserFilter() kustomize.FilterFn {
	return func(nodes []*kyaml.RNode) ([]*kyaml.RNode, error) {
		for _, node := range nodes {
			if node.GetKind() != "SecurityContextConstraints" {
				continue
			}

			usersNode, err := node.Pipe(kyaml.Lookup("users"))
			if err != nil || usersNode == nil {
				continue
			}

			elements, err := usersNode.Elements()
			if err != nil {
				continue
			}

			for _, elem := range elements {
				val, err := elem.String()
				if err != nil {
					continue
				}
				val = strings.TrimSpace(val)
				if strings.Contains(val, "kuberay-operator") {
					newVal := fmt.Sprintf("system:serviceaccount:%s:kuberay-operator", r.OperatorNS)
					elem.YNode().Value = newVal
				}
			}
		}
		return nodes, nil
	}
}

// embedToKustomizeFS walks an embed.FS and copies all files into a kustomize
// in-memory filesystem so the kustomize engine can process them.
func embedToKustomizeFS(efs embed.FS, root string) (filesys.FileSystem, error) {
	memFS := filesys.MakeFsInMemory()

	err := fs.WalkDir(efs, root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return memFS.Mkdir(path)
		}

		data, err := efs.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading embedded file %s: %w", path, err)
		}

		return memFS.WriteFile(path, data)
	})
	if err != nil {
		return nil, err
	}

	return memFS, nil
}

// --- Cleanup ---

// deleteOperands removes the non-CRD resources deployed by this controller
// (webhooks, webhook service, SCC). CRDs are kept to preserve user workloads.
func (r *RayModuleReconciler) deleteOperands(ctx context.Context, l logr.Logger) error {
	operands := []struct {
		obj client.Object
		key types.NamespacedName
	}{
		{
			obj: newUnstructured("admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration"),
			key: types.NamespacedName{Name: "kuberay-mutating-webhook-configuration"},
		},
		{
			obj: newUnstructured("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration"),
			key: types.NamespacedName{Name: "kuberay-validating-webhook-configuration"},
		},
		{
			obj: newUnstructured("v1", "Service"),
			key: types.NamespacedName{Name: "kuberay-webhook-service", Namespace: r.OperatorNS},
		},
	}

	if r.IsOpenShift {
		operands = append(operands, struct {
			obj client.Object
			key types.NamespacedName
		}{
			obj: newUnstructured("security.openshift.io/v1", "SecurityContextConstraints"),
			key: types.NamespacedName{Name: "run-as-ray-user"},
		})
	}

	for _, op := range operands {
		if err := r.Get(ctx, op.key, op.obj); err != nil {
			if errors.IsNotFound(err) || meta.IsNoMatchError(err) {
				continue
			}
			return fmt.Errorf("getting %s %s: %w", op.obj.GetObjectKind().GroupVersionKind().Kind, op.key, err)
		}
		l.Info("deleting operand", "kind", op.obj.GetObjectKind().GroupVersionKind().Kind, "name", op.key.Name)
		if err := r.Delete(ctx, op.obj); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("deleting %s %s: %w", op.obj.GetObjectKind().GroupVersionKind().Kind, op.key, err)
		}
	}

	return nil
}

func newUnstructured(apiVersion, kind string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(apiVersion)
	u.SetKind(kind)
	return u
}
