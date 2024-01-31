package hpa

import (
	"context"
	"fmt"
	v2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	v2informers "k8s.io/client-go/informers/autoscaling/v2"
	clientset "k8s.io/client-go/kubernetes"
	v2listers "k8s.io/client-go/listers/autoscaling/v2"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"time"
)

const (
	// maxRetries is the number of times a service will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the
	// sequence of delays between successive queuings of a service.
	//
	// 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.3s, 2.6s, 5.1s, 10.2s, 20.4s, 41s, 82s
	maxRetries = 15
)

type HPAController struct {
	client clientset.Interface

	hpaLister v2listers.HorizontalPodAutoscalerLister
	hpaSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	workerLoopPeriod time.Duration
}

func NewHPAController(hpaInformer v2informers.HorizontalPodAutoscalerInformer, client clientset.Interface) *HPAController {
	v := &HPAController{
		client:           client,
		queue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "hpa"),
		workerLoopPeriod: time.Second,
	}

	v.hpaLister = hpaInformer.Lister()
	v.hpaSynced = hpaInformer.Informer().HasSynced

	hpaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: v.enqueueHPA,
		UpdateFunc: func(old, cur interface{}) {
			v.enqueueHPA(cur)
		},
	})

	return v
}

func (v *HPAController) Start(ctx context.Context) error {
	return v.Run(5, ctx.Done())
}

func (v *HPAController) Run(workers int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer v.queue.ShutDown()

	klog.Info("starting hpa controller")
	defer klog.Info("shutting down hpa controller")

	if !cache.WaitForCacheSync(stopCh, v.hpaSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	for i := 0; i < workers; i++ {
		go wait.Until(v.worker, v.workerLoopPeriod, stopCh)
	}

	<-stopCh
	return nil
}

func (v *HPAController) enqueueHPA(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}
	v.queue.Add(key)
}

func (v *HPAController) worker() {
	for v.processNextWorkItem() {

	}
}

func (v *HPAController) processNextWorkItem() bool {
	eKey, quit := v.queue.Get()
	if quit {
		return false
	}

	defer v.queue.Done(eKey)

	err := v.syncHPA(eKey.(string))
	v.handleErr(err, eKey)

	return true
}

// main function of the reconcile for hpa
func (v *HPAController) syncHPA(key string) error {
	startTime := time.Now()
	defer func() {
		klog.V(4).Info("Finished syncing hps.", "key", key, "duration", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	hpa, err := v.hpaLister.HorizontalPodAutoscalers(namespace).Get(name)
	if err != nil {
		// has been deleted
		if errors.IsNotFound(err) {
			return nil
		}
		klog.Error(err, "get hpa failed", "namespace", namespace, "name", name)
		return err
	}

	hpaCopyed := hpa.DeepCopy()

	annotationsMaps := v.annotations(hpaCopyed)
	if len(annotationsMaps) != 0 {
		if hpaCopyed.Annotations == nil {
			hpaCopyed.Annotations = make(map[string]string)
		}

		for key, value := range annotationsMaps {
			hpaCopyed.Annotations[key] = value
		}
	}

	_, err = v.client.AutoscalingV2().HorizontalPodAutoscalers(hpaCopyed.Namespace).Update(context.Background(), hpaCopyed, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (v *HPAController) handleErr(err error, key interface{}) {
	if err == nil {
		v.queue.Forget(key)
		return
	}

	if v.queue.NumRequeues(key) < maxRetries {
		klog.V(2).Info("Error syncing hpa, retrying.", "key", key, "error", err)
		v.queue.AddRateLimited(key)
		return
	}

	klog.V(4).Info("Dropping hpa out of the queue", "key", key, "error", err)
	v.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (v *HPAController) annotations(hpa *v2.HorizontalPodAutoscaler) map[string]string {
	if len(hpa.Spec.Metrics) == 0 {
		return nil
	}

	m := make(map[string]string, 0)

	for _, metric := range hpa.Spec.Metrics {
		if metric.Resource != nil {
			if metric.Resource.Name == v1.ResourceCPU {
				m["cpuTargetUtilization"] = fmt.Sprintf("%d", *metric.Resource.Target.AverageUtilization)
			}

			if metric.Resource.Name == v1.ResourceMemory {
				m["memoryTargetValue"] = fmt.Sprintf("%d", *metric.Resource.Target.AverageUtilization)
			}
		}
	}

	return m
}
