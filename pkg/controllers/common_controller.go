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
	"context"
	"fmt"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"

	"github.com/virtual-kubelet/tensile-kube/pkg/util"
)

// CommonController is a controller sync configMaps and secrets from master cluster to client cluster
type CommonController struct {
	client        kubernetes.Interface
	eventRecorder record.EventRecorder

	configMapQueue workqueue.RateLimitingInterface
	secretQueue    workqueue.RateLimitingInterface

	masterConfigMapLister       corelisters.ConfigMapLister
	masterConfigMapListerSynced cache.InformerSynced
	masterSecretLister          corelisters.SecretLister
	masterSecretListerSynced    cache.InformerSynced

	clientConfigMapLister       corelisters.ConfigMapNamespaceLister
	clientConfigMapListerSynced cache.InformerSynced
	clientSecretLister          corelisters.SecretNamespaceLister
	clientSecretListerSynced    cache.InformerSynced
	clusterId                   string
}

// NewCommonController returns a new *CommonController
func NewCommonController(client kubernetes.Interface,
	masterInformer, clientInformer informers.SharedInformerFactory,
	configMapRateLimiter, secretRateLimiter workqueue.RateLimiter,
	clusterId string) Controller {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "virtual-kubelet"})

	configMapInformer := masterInformer.Core().V1().ConfigMaps()
	secretInformer := masterInformer.Core().V1().Secrets()
	clientConfigMapInformer := clientInformer.Core().V1().ConfigMaps()
	clientSecretInformer := clientInformer.Core().V1().Secrets()
	tenantNamespace := fmt.Sprintf("eki-burst-%s", clusterId)
	ctrl := &CommonController{
		client:        client,
		eventRecorder: eventRecorder,

		configMapQueue: workqueue.NewNamedRateLimitingQueue(configMapRateLimiter, "vk configMap controller"),
		secretQueue:    workqueue.NewNamedRateLimitingQueue(secretRateLimiter, "vk secret controller"),

		masterConfigMapLister:       configMapInformer.Lister(),
		masterConfigMapListerSynced: configMapInformer.Informer().HasSynced,
		masterSecretLister:          secretInformer.Lister(),
		masterSecretListerSynced:    secretInformer.Informer().HasSynced,

		clientConfigMapLister:       clientConfigMapInformer.Lister().ConfigMaps(tenantNamespace),
		clientConfigMapListerSynced: clientConfigMapInformer.Informer().HasSynced,
		clientSecretLister:          clientSecretInformer.Lister().Secrets(tenantNamespace),
		clientSecretListerSynced:    clientSecretInformer.Informer().HasSynced,

		clusterId: clusterId,
	}
	configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.configMapAdd,
		UpdateFunc: ctrl.configMapUpdated,
		DeleteFunc: ctrl.configMapDeleted,
	})
	ctrl.masterConfigMapLister = configMapInformer.Lister()
	ctrl.masterConfigMapListerSynced = configMapInformer.Informer().HasSynced

	secretInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.secretAdd,
		UpdateFunc: ctrl.secretUpdated,
		DeleteFunc: ctrl.secretDeleted,
	})
	ctrl.masterSecretLister = secretInformer.Lister()
	ctrl.masterSecretListerSynced = secretInformer.Informer().HasSynced

	return ctrl
}

// Run starts and listens on channel events
func (ctrl *CommonController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.configMapQueue.ShutDown()
	defer ctrl.secretQueue.ShutDown()
	klog.Infof("Starting controller")
	defer klog.Infof("Shutting controller")
	if !cache.WaitForCacheSync(stopCh, ctrl.masterSecretListerSynced, ctrl.masterConfigMapListerSynced) {
		klog.Errorf("Cannot sync caches from master")
		return
	}
	klog.Infof("Sync caches from master successfully")
	//go ctrl.runGC(stopCh)
	ctrl.gc()
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.syncConfigMap, 0, stopCh)
		go wait.Until(ctrl.syncSecret, 0, stopCh)
	}
	<-stopCh
}

// configMapAdd reacts to a ConfigMap add
func (ctrl *CommonController) configMapAdd(obj interface{}) {
	configMap := obj.(*v1.ConfigMap)
	if ctrl.shouldEnqueue(&configMap.ObjectMeta) {
		key, err := cache.MetaNamespaceKeyFunc(obj)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		ctrl.configMapQueue.Add(key)
		klog.V(6).Info("ConfigMap add in master", "key ", key)
	} else {
		klog.V(6).Infof("Ignoring configMap %q add", configMap.Name)
	}
}

// configMapUpdated reacts to a ConfigMap update
func (ctrl *CommonController) configMapUpdated(old, new interface{}) {
	newConfigMap := new.(*v1.ConfigMap)
	oldConfigMap := old.(*v1.ConfigMap)
	if ctrl.shouldEnqueueUpdateConfigMap(oldConfigMap, newConfigMap) && IsObjectGlobal(&newConfigMap.ObjectMeta) {
		key, err := cache.MetaNamespaceKeyFunc(new)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		ctrl.configMapQueue.Add(key)
		klog.V(6).Info("ConfigMap update in master", "key ", key)
	} else {
		klog.V(6).Infof("Ignoring configMap %q change", newConfigMap.Name)
	}
}

// configMapMasterDeleted reacts to a ConfigMap delete
func (ctrl *CommonController) configMapDeleted(obj interface{}) {
	configMap := obj.(*v1.ConfigMap)
	if ctrl.shouldEnqueue(&configMap.ObjectMeta) {
		key, err := cache.MetaNamespaceKeyFunc(configMap)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		ctrl.configMapQueue.Add(key)
		klog.V(6).Info("ConfigMap delete", "key ", key)
	} else {
		klog.V(6).Infof("Ignoring configMap %q change", configMap.Name)
	}
}

// secretAdd reacts to a Secret add
func (ctrl *CommonController) secretAdd(obj interface{}) {
	secret := obj.(*v1.Secret)
	if ctrl.shouldEnqueue(&secret.ObjectMeta) {
		key, err := cache.MetaNamespaceKeyFunc(secret)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		ctrl.secretQueue.Add(key)
		klog.V(6).Info("Secret add in master", "key ", key)
	} else {
		klog.V(6).Infof("Ignoring secret %q add", secret.Name)
	}
}

// secretUpdated reacts to a Secret update
func (ctrl *CommonController) secretUpdated(old, new interface{}) {
	newSecret := new.(*v1.Secret)
	oldSecret := old.(*v1.Secret)
	if ctrl.shouldEnqueueUpdateSecret(oldSecret, newSecret) && IsObjectGlobal(&newSecret.ObjectMeta) {
		key, err := cache.MetaNamespaceKeyFunc(new)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		ctrl.secretQueue.Add(key)
		klog.V(6).Info("Secret update in master", "key ", key)
	} else {
		klog.V(6).Infof("Ignoring secret %q change", newSecret.Name)
	}
}

// secretMasterDelete reacts to a Secret delete
func (ctrl *CommonController) secretDeleted(obj interface{}) {
	secret := obj.(*v1.Secret)
	if ctrl.shouldEnqueue(&secret.ObjectMeta) {
		key, err := cache.MetaNamespaceKeyFunc(secret)
		if err != nil {
			runtime.HandleError(err)
			return
		}
		ctrl.secretQueue.Add(key)

		klog.V(6).Infof("Secret delete, enqueue secret: %v", key)
	} else {
		klog.V(6).Infof("Ignoring secret %q change", secret.Name)
	}
}

// syncConfigMap deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CommonController) syncConfigMap() {
	ctx := context.TODO()
	keyObj, quit := ctrl.configMapQueue.Get()
	if quit {
		return
	}
	defer ctrl.configMapQueue.Done(keyObj)
	key := keyObj.(string)
	namespace, configMapName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.configMapQueue.Forget(key)
		return
	}
	klog.V(4).Infof("Started configMap processing %q", configMapName)

	defer func() {
		if err != nil {
			ctrl.configMapQueue.AddRateLimited(key)
			return
		}
		ctrl.configMapQueue.Forget(key)
	}()
	var configMap *v1.ConfigMap
	delObjInClient := false
	configMap, err = ctrl.masterConfigMapLister.ConfigMaps(namespace).Get(configMapName)
	clientName := fmt.Sprintf("%s-%s", namespace, configMapName)
	if err != nil {
		if !apierrs.IsNotFound(err) {
			return
		}
		_, err = ctrl.clientConfigMapLister.Get(clientName)
		if err != nil {
			if !apierrs.IsNotFound(err) {
				klog.Errorf("Get configMap from client cluster failed, error: %v", err)
				return
			}
			err = nil
			klog.V(3).Infof("ConfigMap %q deleted", configMapName)
			return
		}
		delObjInClient = true

	}

	if delObjInClient || configMap.DeletionTimestamp != nil {
		if err = ctrl.client.CoreV1().ConfigMaps(ctrl.TenantNamespace()).Delete(ctx, clientName,
			metav1.DeleteOptions{}); err != nil {
			if !apierrs.IsNotFound(err) {
				klog.Errorf("Delete configMap from client cluster failed, error: %v", err)
				return
			}
			err = nil
		}
		klog.V(3).Infof("ConfigMap %q deleted", configMapName)
		return
	}

	// data updated
	var objInClient *v1.ConfigMap
	objInClient, err = ctrl.clientConfigMapLister.Get(clientName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			err = nil
			return
		}
		klog.Errorf("Get configMap from client cluster failed, error: %v", err)
		return
	}
	util.UpdateConfigMap(objInClient, configMap)
	if IsObjectGlobal(&objInClient.ObjectMeta) {
		return
	}
	_, err = ctrl.client.CoreV1().ConfigMaps(ctrl.TenantNamespace()).Update(ctx,
		objInClient, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Get configMap from client cluster failed, error: %v", err)
		return
	}
}

// syncSecret deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CommonController) syncSecret() {
	ctx := context.TODO()
	keyObj, quit := ctrl.secretQueue.Get()
	if quit {
		return
	}
	defer ctrl.secretQueue.Done(keyObj)
	key := keyObj.(string)
	namespace, secretName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.secretQueue.Forget(key)
		return
	}
	klog.V(4).Infof("Started secret processing %q", secretName)

	defer func() {
		if err != nil {
			ctrl.secretQueue.AddRateLimited(key)
			return
		}
		ctrl.secretQueue.Forget(key)
	}()

	var secret *v1.Secret
	delObjInClient := false
	secret, err = ctrl.masterSecretLister.Secrets(namespace).Get(secretName)
	clientName := fmt.Sprintf("%s-%s", namespace, secretName)
	if err != nil {
		if !apierrs.IsNotFound(err) {
			return
		}
		_, err = ctrl.clientSecretLister.Get(clientName)
		if err != nil {
			if !apierrs.IsNotFound(err) {
				klog.Errorf("Get secret from master cluster failed, error: %v", err)
				return
			}
			err = nil
			klog.V(3).Infof("Secret %q deleted", secretName)
			return
		}
		delObjInClient = true

	}

	if delObjInClient || secret.DeletionTimestamp != nil {
		if err = ctrl.client.CoreV1().Secrets(ctrl.TenantNamespace()).Delete(ctx, clientName,
			metav1.DeleteOptions{}); err != nil {
			if !apierrs.IsNotFound(err) {
				klog.Errorf("Delete secret from client cluster failed, error: %v", err)
				return
			}
			err = nil
		}
		klog.V(3).Infof("Secret %q deleted", secretName)
		return
	}

	// data updated
	var objInClient *v1.Secret
	objInClient, err = ctrl.clientSecretLister.Get(clientName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			err = nil
			return
		}
		klog.Errorf("Get secret from client cluster failed, error: %v", err)
		return
	}
	util.UpdateSecret(objInClient, secret)
	if IsObjectGlobal(&objInClient.ObjectMeta) {
		return
	}
	_, err = ctrl.client.CoreV1().Secrets(ctrl.TenantNamespace()).Update(ctx, objInClient, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Get secret from client cluster failed, error: %v", err)
		return
	}
}

func (ctrl *CommonController) shouldEnqueue(obj *metav1.ObjectMeta) bool {
	if obj.Namespace == metav1.NamespaceSystem {
		return false
	}
	return true
}

func (ctrl *CommonController) shouldEnqueueUpdateConfigMap(old, new *v1.ConfigMap) bool {
	if !ctrl.shouldEnqueue(&new.ObjectMeta) {
		return false
	}
	if !reflect.DeepEqual(old.Data, new.Data) {
		return true
	}
	if !reflect.DeepEqual(old.BinaryData, new.BinaryData) {
		return true
	}
	if new.DeletionTimestamp != nil {
		return true
	}
	return false
}

func (ctrl *CommonController) shouldEnqueueUpdateSecret(old, new *v1.Secret) bool {
	if !ctrl.shouldEnqueueAddSecret(old) {
		return false
	}
	if !ctrl.shouldEnqueueAddSecret(new) {
		return false
	}
	if !reflect.DeepEqual(old.Data, new.Data) {
		return true
	}
	if !reflect.DeepEqual(old.StringData, new.StringData) {
		return true
	}
	if !reflect.DeepEqual(old.Type, new.Type) {
		return true
	}
	if new.DeletionTimestamp != nil {
		return true
	}
	return false
}

func (ctrl *CommonController) shouldEnqueueAddSecret(secret *v1.Secret) bool {
	if !ctrl.shouldEnqueue(&secret.ObjectMeta) {
		return false
	}
	if secret.Type == v1.SecretTypeServiceAccountToken {
		return false
	}
	return true
}

func (ctrl *CommonController) gcConfigMap(ctx context.Context) {
	configMaps, err := ctrl.clientConfigMapLister.List(labels.Everything())
	if err != nil {
		klog.Error(err)
		return
	}
	for _, configMap := range configMaps {
		if configMap == nil {
			continue
		}
		if !IsObjectGlobal(&configMap.ObjectMeta) {
			continue
		}
		_, err = ctrl.masterConfigMapLister.ConfigMaps(configMap.Annotations[util.UpstreamNamespace]).Get(configMap.Annotations[util.UpstreamResourceName])
		if err != nil && apierrs.IsNotFound(err) {
			err := ctrl.client.CoreV1().ConfigMaps(configMap.Namespace).Delete(ctx,
				configMap.Name, metav1.DeleteOptions{})
			if err != nil && !apierrs.IsNotFound(err) {
				klog.Error(err)
			}
			continue
		}
	}
}

func (ctrl *CommonController) gcSecret(ctx context.Context) {
	secrets, err := ctrl.clientSecretLister.List(labels.Everything())
	if err != nil {
		klog.Error(err)
		return
	}
	for _, secret := range secrets {
		if secret == nil {
			continue
		}
		if !IsObjectGlobal(&secret.ObjectMeta) {
			continue
		}
		_, err = ctrl.masterSecretLister.Secrets(secret.Annotations[util.UpstreamNamespace]).Get(secret.Annotations[util.UpstreamResourceName])
		if err != nil && apierrs.IsNotFound(err) {
			err := ctrl.client.CoreV1().Secrets(secret.Namespace).Delete(ctx, secret.Name, metav1.DeleteOptions{})
			if err != nil && !apierrs.IsNotFound(err) {
				klog.Error(err)
			}
			continue
		}
	}
}

func (ctrl *CommonController) gc() {
	ctx := context.TODO()
	ctrl.gcConfigMap(ctx)
	ctrl.gcSecret(ctx)
}

func (ctrl *CommonController) runGC(stopCh <-chan struct{}) {
	wait.Until(ctrl.gc, 3*time.Minute, stopCh)
}

func (ctrl *CommonController) TenantNamespace() string {
	return fmt.Sprintf("eki-burst-%s", ctrl.clusterId)
}
