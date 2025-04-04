/*
Copyright 2024 The kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"context"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/metrics"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

// SparkPodEventHandler watches Spark pods and update the SparkApplication objects accordingly.
type SparkPodEventHandler struct {
	client  client.Client
	metrics *metrics.SparkExecutorMetrics
}

// SparkPodEventHandler implements handler.EventHandler.
var _ handler.EventHandler = &SparkPodEventHandler{}

// NewSparkPodEventHandler creates a new sparkPodEventHandler instance.
func NewSparkPodEventHandler(client client.Client, metrics *metrics.SparkExecutorMetrics) *SparkPodEventHandler {
	handler := &SparkPodEventHandler{
		client:  client,
		metrics: metrics,
	}
	return handler
}

// Create implements handler.EventHandler.
func (h *SparkPodEventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	pod, ok := event.Object.(*corev1.Pod)
	if !ok {
		return
	}
	logger.Info("Spark pod created", "name", pod.Name, "namespace", pod.Namespace, "phase", pod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, pod, queue)

	if h.metrics != nil && util.IsExecutorPod(pod) {
		h.metrics.HandleSparkExecutorCreate(pod)
	}
}

// Update implements handler.EventHandler.
func (h *SparkPodEventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	oldPod, ok := event.ObjectOld.(*corev1.Pod)
	if !ok {
		return
	}

	newPod, ok := event.ObjectNew.(*corev1.Pod)
	if !ok {
		return
	}

	if newPod.Status.Phase == oldPod.Status.Phase {
		return
	}

	logger.Info("Spark pod updated", "name", newPod.Name, "namespace", newPod.Namespace, "oldPhase", oldPod.Status.Phase, "newPhase", newPod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, newPod, queue)

	if h.metrics != nil && util.IsExecutorPod(oldPod) && util.IsExecutorPod(newPod) {
		h.metrics.HandleSparkExecutorUpdate(oldPod, newPod)
	}
}

// Delete implements handler.EventHandler.
func (h *SparkPodEventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	pod, ok := event.Object.(*corev1.Pod)
	if !ok {
		return
	}

	logger.Info("Spark pod deleted", "name", pod.Name, "namespace", pod.Namespace, "phase", pod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, pod, queue)

	if h.metrics != nil && util.IsExecutorPod(pod) {
		h.metrics.HandleSparkExecutorDelete(pod)
	}
}

// Generic implements handler.EventHandler.
func (h *SparkPodEventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	pod, ok := event.Object.(*corev1.Pod)
	if !ok {
		return
	}

	logger.Info("Spark pod generic event ", "name", pod.Name, "namespace", pod.Namespace, "phase", pod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, pod, queue)
}

func (h *SparkPodEventHandler) enqueueSparkAppForUpdate(ctx context.Context, pod *corev1.Pod, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	name := util.GetAppName(pod)
	if name == "" {
		return
	}
	namespace := pod.Namespace
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	app := &v1beta2.SparkApplication{}
	if submissionID, ok := pod.Labels[common.LabelSubmissionID]; ok {
		if err := h.client.Get(ctx, key, app); err != nil {
			return
		}
		if app.Status.SubmissionID != submissionID {
			return
		}
	}

	// Do not enqueue SparkApplication in invalidating state when driver pod get deleted.
	if util.GetApplicationState(app) == v1beta2.ApplicationStateInvalidating {
		return
	}

	queue.AddRateLimited(ctrl.Request{NamespacedName: key})
}

// EventHandler watches SparkApplication events.
type EventHandler struct {
	client  client.Client
	metrics *metrics.SparkApplicationMetrics
}

var _ handler.EventHandler = &EventHandler{}

// NewSparkApplicationEventHandler creates a new SparkApplicationEventHandler instance.
func NewSparkApplicationEventHandler(client client.Client, metrics *metrics.SparkApplicationMetrics) *EventHandler {
	return &EventHandler{
		client:  client,
		metrics: metrics,
	}
}

// Create implements handler.EventHandler.
func (h *EventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	app, ok := event.Object.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	logger.Info("SparkApplication created", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})

	if h.metrics != nil {
		h.metrics.HandleSparkApplicationCreate(app)
	}
}

// Update implements handler.EventHandler.
func (h *EventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	oldApp, ok := event.ObjectOld.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	newApp, ok := event.ObjectNew.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	logger.Info("SparkApplication updated", "name", oldApp.Name, "namespace", oldApp.Namespace, "oldState", oldApp.Status.AppState.State, "newState", newApp.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: newApp.Name, Namespace: newApp.Namespace}})

	if h.metrics != nil {
		h.metrics.HandleSparkApplicationUpdate(oldApp, newApp)
	}
}

// Delete implements handler.EventHandler.
func (h *EventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	app, ok := event.Object.(*v1beta2.SparkApplication)
	if !ok {
		return
	}
	// When not using the webhook, we need to delete the pod driver when the Spark App is getting killed
	// Driver pod name is made of SparkApp + '-driver' suffix
	podDriver := app.Name + "-driver"
	ns := app.Namespace

	// Initializing the Pod structure with the name of the pod & the namespace
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      podDriver,
		},
	}

	// Now the driver is being deleted (its executors will also be deleted at the same time)
	// Even if we're facing an error we only print it as Info message because event_handler is just a read-only handler
	err := h.client.Delete(ctx, pod)
	if err != nil {
		logger.Info("Unable to delete the Spark Driver: %v\n", err, "name", podDriver, "namespace", ns)
		return
	}

	logger.Info("SparkApplication deleted", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})

	if h.metrics != nil {
		h.metrics.HandleSparkApplicationDelete(app)
	}
}

// Generic implements handler.EventHandler.
func (h *EventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	app, ok := event.Object.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	logger.Info("SparkApplication generic event", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
}
