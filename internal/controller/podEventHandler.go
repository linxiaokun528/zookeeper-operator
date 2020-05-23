package controller

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	cliv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"

	apis "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

type ZkPodEventHandler struct {
	expectations controller.ControllerExpectationsInterface
	cli          cliv1.PodsGetter
}

func (p *ZkPodEventHandler) OnAdd(obj interface{}) {
	pod := obj.(*corev1.Pod)

	if pod.DeletionTimestamp != nil {
		// on a restart of the controller, it's possible a new pod shows up in a state that
		// is already pending deletion. Prevent the pod from being a creation observation.
		return
	}

	if !p.isPodFromZkController(pod) {
		return
	}

	key := getExpectionKey(pod)
	klog.Infoln("Pod addition observed: %s/%s", pod.Namespace, pod.Name)
	// We won't have a record in p.expectations in the first sync. And we don't need to do anything in the first sync.
	_, ok, err := p.expectations.GetExpectations(key)
	if err != nil {
		fmt.Errorf("%s", err)
		return
	}
	if !ok {
		return
	}

	if p.expectations.SatisfiedExpectations(key) {
		p.expectations.RaiseExpectations(key, 0, 1)
		p.cli.Pods(pod.Namespace).Delete(pod.Name, nil)
	} else {
		p.expectations.CreationObserved(key)
	}
}

func (p *ZkPodEventHandler) OnUpdate(oldObj, newObj interface{}) {

}

func (p *ZkPodEventHandler) isPodFromZkController(pod *corev1.Pod) bool {
	for _, ref := range pod.GetOwnerReferences() {
		refGV, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			panic(err)
		}

		if ref.Kind == apis.Kind && refGV.Group == apis.GroupName && refGV.Version == apis.Version {
			return true
		}
	}

	return false
}

func (p *ZkPodEventHandler) OnDelete(obj interface{}) {
	pod := obj.(*corev1.Pod)
	if !p.isPodFromZkController(pod) {
		return
	}

	key := getExpectionKey(pod)

	klog.Infof("Pod deletion observed: %s/%s", pod.Namespace, pod.Name)
	// We won't have a record in p.expectations in the first sync or the zookeeper cluster is deleted.
	// So we don't need to do anything in the first sync.
	_, ok, err := p.expectations.GetExpectations(key)
	if err != nil {
		klog.Errorf("%s", err)
		return
	}
	if !ok {
		return
	}
	if p.expectations.SatisfiedExpectations(key) {
		p.expectations.RaiseExpectations(key, 1, 0)
		pod.DeletionTimestamp = nil
		pod.ResourceVersion = ""
		_, err := p.cli.Pods(pod.Namespace).Create(pod)
		fmt.Errorf("%v", err)
	} else {
		p.expectations.DeletionObserved(key)
	}
}

func fullName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func getExpectionKey(pod *corev1.Pod) string {
	zkCluster := pod.Labels["zookeeper_cluster"]
	return fullName(pod.Namespace, zkCluster)
}
