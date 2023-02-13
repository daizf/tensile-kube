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
	"github.com/virtual-kubelet/tensile-kube/pkg/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/pkg/controller"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	masterNamespace = "default"
	clientNamespace = "eki-burst-default"
)

type eventTestBase struct {
	c              *EventController
	masterInformer informers.SharedInformerFactory
	clientInformer informers.SharedInformerFactory
	master         *fake.Clientset
	client         *fake.Clientset
}

func TestEventController_RunAddPodEvent(t *testing.T) {
	event := fakePodEvent(1)
	eventCopy := event.DeepCopy()
	cases := []struct {
		name    string
		event   *v1.Event
		existed bool
	}{
		{
			name:    "should add",
			event:   eventCopy,
			existed: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := newEventController()
			stopCh := make(chan struct{})
			go test(b.c, 1, stopCh)
			b.clientInformer.Start(stopCh)
			b.masterInformer.Start(stopCh)

			err := wait.Poll(10*time.Millisecond, 10*time.Second, func() (bool, error) {
				masterEventName := strings.Replace(c.event.Name, masterNamespace+"-", "", 1)
				me, err := b.c.master.CoreV1().Events(masterNamespace).Get(
					context.TODO(), masterEventName, metav1.GetOptions{})
				if err != nil {
					return false, nil
				}
				if reflect.DeepEqual(me.InvolvedObject.Name, fakeMasterPod().Name) == c.existed {
					t.Log("add event involved pod satisfied")
					return true, nil
				}
				return false, nil
			})
			if err != nil {
				t.Error("add event failed")
			}
		})
	}
}

func TestEventController_RunAddPvcEvent(t *testing.T) {
	event := fakePvcEvent(1)
	cases := []struct {
		name    string
		event   *v1.Event
		existed bool
	}{
		{
			name:    "should add",
			event:   event,
			existed: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := newEventController()
			stopCh := make(chan struct{})
			go test(b.c, 1, stopCh)
			b.clientInformer.Start(stopCh)
			b.masterInformer.Start(stopCh)

			err := wait.Poll(10*time.Millisecond, 10*time.Second, func() (bool, error) {
				masterEventName := strings.Replace(c.event.Name, masterNamespace+"-", "", 1)
				me, err := b.c.master.CoreV1().Events(masterNamespace).Get(
					context.TODO(), masterEventName, metav1.GetOptions{})
				if err != nil {
					return false, nil
				}
				if reflect.DeepEqual(me.InvolvedObject.Name, fakeMasterPvc().Name) == c.existed {
					t.Log("add event involved pod satisfied")
					return true, nil
				}
				return false, nil
			})
			if err != nil {
				t.Error("add event failed")
			}
		})
	}
}

func TestEventController_RunUpdatePodEvent(t *testing.T) {
}

func TestEventController_RunUpdatePvcEvent(t *testing.T) {
}

func newEventController() *eventTestBase {
	// 准备资源对象
	client := fake.NewSimpleClientset(fakeClientPod(), fakePodEvent(1), fakePvcEvent(1), fakeClientPvc())
	master := fake.NewSimpleClientset(fakeMasterPod(), fakeMasterPvc())

	clientInformer := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	masterInformer := informers.NewSharedInformerFactory(master, controller.NoResyncPeriodFunc())

	controller := NewEventController(master, client, masterInformer, clientInformer, "default")
	c := controller.(*EventController)
	// hack
	c.eventSink = &v1core.EventSinkImpl{Interface: master.CoreV1().Events(masterNamespace)}
	return &eventTestBase{
		c:              c,
		masterInformer: masterInformer,
		clientInformer: clientInformer,
		master:         master,
		client:         client,
	}
}

func fakePodEvent(count int32) *v1.Event {
	return &v1.Event{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Event",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default-nginx-557b44bcbc-4mkq9.17435af2e11f39d7",
			Namespace: clientNamespace,
		},
		Message: "Back-off pulling image \"cis-hub-chongqing-1.cmecloud.cn/ecloud/nginx:1.21.4.d\"",
		Reason:  "BackOff",
		Type:    v1.EventTypeNormal,
		Count:   count,
		InvolvedObject: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Name:       "default-nginx-557b44bcbc-4mkq9",
			Namespace:  clientNamespace,
		},
	}
}

func fakePvcEvent(count int32) *v1.Event {
	return &v1.Event{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Event",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default-myclaim.17436637b14847b2",
			Namespace: clientNamespace,
		},
		Message: "External provisioner is provisioning volume for claim \"eki-burst-685386d8-c703-4e51-a96d-aea2ca9b14cd/default-myclaim\"",
		Reason:  "Provisioning",
		Type:    v1.EventTypeNormal,
		Count:   count,
		InvolvedObject: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "default-myclaim",
			Namespace:  clientNamespace,
		},
	}
}

func fakeClientPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "default-nginx-557b44bcbc-4mkq9",
			Namespace:   clientNamespace,
			Annotations: map[string]string{util.UpstreamNamespace: masterNamespace, util.UpstreamResourceName: "nginx-557b44bcbc-4mkq9", "global": "true"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Image: "nginx:latest",
				},
			},
		},
		Status: v1.PodStatus{},
	}
}

func fakeMasterPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "nginx-557b44bcbc-4mkq9",
			Namespace:   masterNamespace,
			Annotations: map[string]string{"global": "true"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Image: "nginx:latest",
				},
			},
		},
		Status: v1.PodStatus{},
	}
}

func fakeClientPvc() *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "default-myclaim",
			Namespace:   clientNamespace,
			Annotations: map[string]string{util.UpstreamNamespace: masterNamespace, util.UpstreamResourceName: "myclaim", "global": "true"},
		},
		Spec:   v1.PersistentVolumeClaimSpec{},
		Status: v1.PersistentVolumeClaimStatus{},
	}
}

func fakeMasterPvc() *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "myclaim",
			Namespace:   masterNamespace,
			Annotations: map[string]string{"global": "true"},
		},
		Spec:   v1.PersistentVolumeClaimSpec{},
		Status: v1.PersistentVolumeClaimStatus{},
	}
}
