package controller

import (
	"gopkg.in/fatih/set.v0"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	cliv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"

	apis "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/k8sutil"
)

type ZkPodEventHandler struct {
	podsToDelete set.Interface
	zkLister     cache.GenericLister
	cli          cliv1.PodsGetter
}

func (p *ZkPodEventHandler) OnAdd(obj interface{}) {
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
			obj, err := p.zkLister.ByNamespace(pod.Namespace).Get(ref.Name)
			if err != nil {
				return false
			}
			if obj.(*apis.ZookeeperCluster).UID != ref.UID {
				// The controller we found with this Name is not the same one that the
				// ControllerRef points to.
				return false
			}
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

	if p.podsToDelete.Has(k8sutil.GetFullName(pod)) {
		p.podsToDelete.Remove(k8sutil.GetFullName(pod))
	} else {
		klog.Infof("Pod deletion observed: %s/%s", pod.Namespace, pod.Name)

		pod.DeletionTimestamp = nil
		pod.ResourceVersion = ""
		// Don't need to worry that if the corresponding zookeeper cluster is deleted.
		// Even if we create the pod after the zookeeper cluster is deleted, the pod will be terminated automatically
		// anyway.
		_, err := p.cli.Pods(pod.Namespace).Create(pod)
		if err != nil {
			klog.Error(err)
		} else {
			klog.Infof("Deleted pod %s/%s added back.", pod.Namespace, pod.Name)
		}
	}
}
