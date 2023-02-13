/*
 * Copyright ©2020. The virtual-kubelet authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controllers

import (
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	_ "reflect"
	"strings"
	"time"

	goCache "github.com/patrickmn/go-cache"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"

	"github.com/virtual-kubelet/tensile-kube/pkg/util"
)

const (
	defaultCleanCacheInterval = 60 * time.Second
	eventCacheExpiredTime     = 300 * time.Second
)

// EventController is a controller sync configMaps and secrets from master cluster to client cluster
type EventController struct {
	master, client kubernetes.Interface
	eventSink      record.EventSink
	eventQueue     workqueue.RateLimitingInterface

	masterPodLister       corelisters.PodLister
	masterPodListerSynced cache.InformerSynced
	masterPvcLister       corelisters.PersistentVolumeClaimLister
	masterPvcListerSynced cache.InformerSynced

	clientPodLister         corelisters.PodNamespaceLister
	clientPodListerSynced   cache.InformerSynced
	clientPvcLister         corelisters.PersistentVolumeClaimNamespaceLister
	clientPvcListerSynced   cache.InformerSynced
	clientEventLister       corelisters.EventNamespaceLister
	clientEventListerSynced cache.InformerSynced

	syncedEventCache *goCache.Cache
	clusterId        string
}

// NewEventController returns a new *EventController
func NewEventController(master, client kubernetes.Interface,
	masterInformer, clientInformer informers.SharedInformerFactory,
	clusterId string) Controller {
	masterPodInformer := masterInformer.Core().V1().Pods()
	masterPvcInformer := masterInformer.Core().V1().PersistentVolumeClaims()

	clientPodInformer := clientInformer.Core().V1().Pods()
	clientPvcInformer := clientInformer.Core().V1().PersistentVolumeClaims()
	clientEventInformer := clientInformer.Core().V1().Events()

	eventRateLimiter := workqueue.NewItemExponentialFailureRateLimiter(time.Second, 30*time.Second)
	tenantNamespace := fmt.Sprintf("eki-burst-%s", clusterId)
	ctrl := &EventController{
		master: master,
		client: client,

		eventQueue: workqueue.NewNamedRateLimitingQueue(eventRateLimiter, "vk event controller"),

		masterPodLister:       masterPodInformer.Lister(),
		masterPodListerSynced: masterPodInformer.Informer().HasSynced,
		masterPvcLister:       masterPvcInformer.Lister(),
		masterPvcListerSynced: masterPvcInformer.Informer().HasSynced,

		clientPodLister:       clientPodInformer.Lister().Pods(tenantNamespace),
		clientPodListerSynced: clientPodInformer.Informer().HasSynced,
		clientPvcLister:       clientPvcInformer.Lister().PersistentVolumeClaims(tenantNamespace),
		clientPvcListerSynced: clientPvcInformer.Informer().HasSynced,

		eventSink:        &v1core.EventSinkImpl{Interface: master.CoreV1().Events(v1.NamespaceAll)},
		syncedEventCache: goCache.New(eventCacheExpiredTime, defaultCleanCacheInterval),
		clusterId:        clusterId,
	}

	clientEventInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.eventInClientAdded,
		UpdateFunc: ctrl.eventInClientUpdated,
		//DeleteFunc: ctrl.pvAdded,
	})
	ctrl.clientEventLister = clientEventInformer.Lister().Events(tenantNamespace)
	ctrl.clientEventListerSynced = clientEventInformer.Informer().HasSynced

	return ctrl
}

// Run starts and listens on channel events
func (ctrl *EventController) Run(workers int, stopCh <-chan struct{}) {
	klog.Infof("Starting controller")
	defer klog.Infof("Shutting controller")
	if !cache.WaitForCacheSync(stopCh, ctrl.clientEventListerSynced, ctrl.masterPvcListerSynced, ctrl.masterPodListerSynced, ctrl.clientPodListerSynced, ctrl.clientPvcListerSynced) {
		klog.Errorf("Cannot sync caches from master")
		return
	}
	klog.Infof("Sync caches from master successfully")
	//go ctrl.runGC(stopCh)
	ctrl.gc()
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.syncEvent, 0, stopCh)
	}
	<-stopCh
}

// eventInClientAdded reacts to an Event creation
func (ctrl *EventController) eventInClientAdded(obj interface{}) {
	event := obj.(*v1.Event)

	if ctrl.shouldEnqueue(event) {
		key, err := cache.MetaNamespaceKeyFunc(obj)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		klog.Info("Enqueue event add ", "key ", key)
		ctrl.eventQueue.Add(key)
	} else {
		klog.V(6).Infof("Ignoring event %q change", event.Name)
	}
}

// eventInClientUpdated reacts to an Event update
func (ctrl *EventController) eventInClientUpdated(old, new interface{}) {
	newEvent := new.(*v1.Event)

	if ctrl.shouldEnqueue(newEvent) {
		key, err := cache.MetaNamespaceKeyFunc(new)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		ctrl.eventQueue.Add(key)
		klog.V(6).Infof("Event update, enqueue event: %v", key)
	} else {
		klog.V(6).Infof("Ignoring event %q change", newEvent.Name)
	}
}

// syncEvent deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *EventController) syncEvent() {
	keyObj, quit := ctrl.eventQueue.Get()
	if quit {
		return
	}
	defer ctrl.eventQueue.Done(keyObj)
	key := keyObj.(string)
	_, eventName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.eventQueue.Forget(key)
		return
	}
	klog.V(4).Infof("Started event processing %q", key)

	defer func() {
		if err != nil {
			ctrl.eventQueue.AddRateLimited(key)
			return
		}
		ctrl.eventQueue.Forget(key)
	}()

	var event *v1.Event
	if event, err = ctrl.clientEventLister.Get(eventName); err != nil {
		klog.Errorf("get event %q from client failed: %v", key, err)
		return
	}

	involvedObjectMeta := &event.ObjectMeta
	involvedObj := event.InvolvedObject
	switch involvedObj.Kind {
	case "Pod":
		// 1、查询在client集群的Pod
		clientPod, err := ctrl.clientPodLister.Get(involvedObj.Name)
		if err != nil {
			klog.Warningf("get client pod for event: %q error: %v", event.Name, err)
			return
		}

		// 2、根据Pod查找master的映射对象
		ns := clientPod.Annotations[util.UpstreamNamespace]
		name := clientPod.Annotations[util.UpstreamResourceName]

		masterPod, err := ctrl.masterPodLister.Pods(ns).Get(name)
		if err != nil {
			klog.Warningf("Get pod %q from master for event %q, error: %v", involvedObj.Name, event.Name, err)
			return
		}
		involvedObjectMeta = &masterPod.ObjectMeta
	case "PersistentVolumeClaim":
		// 1、查询在client集群的Pvc
		clientPvc, err := ctrl.clientPvcLister.Get(involvedObj.Name)
		if err != nil {
			klog.Warningf("get client pvc for event: %q error: %v", event.Name, err)
			return
		}

		// 2、根据Pod查找master的映射对象
		ns := clientPvc.Annotations[util.UpstreamNamespace]
		name := clientPvc.Annotations[util.UpstreamResourceName]

		masterPvc, err := ctrl.masterPvcLister.PersistentVolumeClaims(ns).Get(name)
		if err != nil {
			klog.Warningf("Get pvc %q from master for event %q, error: %v", involvedObj.Name, event.Name, err)
			return
		}
		involvedObjectMeta = &masterPvc.ObjectMeta
	default:
		return
	}

	// 3、更新event相关信息
	eventCopy := event.DeepCopy()
	ctrl.dealEventMessage(eventCopy, involvedObjectMeta)
	ctrl.dealEventInvolvedObjectMeta(eventCopy, involvedObjectMeta)
	ctrl.dealEventObjectMeta(eventCopy, involvedObjectMeta)

	// 4、根据count判断是create event还是patch event.
	curCount, ok := ctrl.syncedEventCache.Get(eventCopy.Name)
	newCount := event.Count
	if !ok {
		klog.Infof("Create event %q to master.", eventCopy.Name)
		if _, err = ctrl.eventSink.Create(eventCopy); err != nil {
			if !errors.IsAlreadyExists(err) {
				klog.Errorf("Create event %q to master, error: %v", eventCopy.Name, err)
			}
		}
		ctrl.syncedEventCache.Set(eventCopy.Name, newCount, 0)
		return
	}

	if curCount.(int32) >= newCount {
		return
	}

	// 5、patch操作
	eventCopy2 := eventCopy.DeepCopy()
	eventCopy2.Count = newCount
	eventCopy2.LastTimestamp = event.LastTimestamp
	eventCopy.LastTimestamp = metav1.NewTime(time.Unix(0, 0))
	eventCopy.Count = curCount.(int32)

	newData, _ := json.Marshal(eventCopy2)
	oldData, _ := json.Marshal(eventCopy)
	patch, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, event)
	if err != nil {
		return
	}

	klog.Infof("update event %q to master. patch: %s", eventCopy.Name, string(patch))
	if _, err = ctrl.eventSink.Patch(eventCopy, patch); err != nil {
		klog.Errorf("update event %q to master, patch: %q. error %v", eventCopy.Name, string(patch), err)
		return
	}

}

func (ctrl *EventController) shouldEnqueue(obj *v1.Event) bool {
	if obj.Namespace == metav1.NamespaceSystem {
		return false
	}

	if obj.Reason == "Scheduled" {
		return false
	}
	return true
}

func (ctrl *EventController) dealEventMessage(obj *v1.Event, objMeta *metav1.ObjectMeta) {
	if obj.Reason == "Provisioning" {
		obj.Message = fmt.Sprintf("External provisioner is provisioning volume for claim %q", fmt.Sprintf("%s/%s", objMeta.Namespace, objMeta.Name))
	}
	if obj.Reason == "WaitForPodScheduled" {
		obj.Message = strings.Replace(obj.Message, objMeta.Namespace+"-", "", 1)
	}
}

func (ctrl *EventController) dealEventInvolvedObjectMeta(event *v1.Event, objMeta *metav1.ObjectMeta) {
	event.InvolvedObject.Name = objMeta.Name
	event.InvolvedObject.Namespace = objMeta.Namespace
	event.InvolvedObject.UID = objMeta.UID
	event.InvolvedObject.ResourceVersion = objMeta.ResourceVersion
}

func (ctrl *EventController) dealEventObjectMeta(event *v1.Event, objMeta *metav1.ObjectMeta) {
	parts := strings.Split(event.Name, ".")
	event.Name = fmt.Sprintf("%s.%s", objMeta.Name, parts[1]) // 要替换name
	event.Namespace = objMeta.Namespace                       // namespace

	event.UID = ""
	event.ResourceVersion = ""
	event.OwnerReferences = nil
}

func (ctrl *EventController) gc() {
}

func (ctrl *EventController) runGC(stopCh <-chan struct{}) {
	wait.Until(ctrl.gc, 3*time.Minute, stopCh)
}
