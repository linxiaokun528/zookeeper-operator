package zkcluster

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/util/k8sutil"
)

func (c *Cluster) listPods() (pods []*v1.Pod, err error) {
	podList, err := c.client.Pod().List(clusterListOpt(c.zkCR.Name))
	if err != nil {
		ReconcileFailed.WithLabelValues("failed to list pods").Inc()
		return nil, fmt.Errorf("failed to list pods for ZookeeperCluster %v:%v: %v", c.zkCR.Namespace, c.zkCR.Name, err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		// Avoid polling deleted pods. k8s issue where deleted pods would sometimes show the status Pending
		// See https://github.com/coreos/etcd-operator/issues/1693
		if pod.DeletionTimestamp != nil {
			continue
		}
		if len(pod.OwnerReferences) < 1 {
			klog.Warningf("list pods: ignore pod %v: no owner", pod.Name)
			continue
		}
		if pod.OwnerReferences[0].UID != c.zkCR.UID {
			klog.Warningf("list pods: ignore pod %v: owner (%v) is not %v",
				pod.Name, pod.OwnerReferences[0].UID, c.zkCR.UID)
			continue
		}
		pods = append(pods, pod.DeepCopy())
	}

	return pods, nil
}

func (c *Cluster) syncCurrentMembers() (err error) {
	pods, err := c.listPods()
	if err != nil {
		return err
	}
	running_pods, unready_pods, stopped_pods := k8sutil.GetPodsSeparatedByStatus(pods)

	c.zkCR.Status.Members = api.NewZKCluster(c.zkCR.Namespace, c.zkCR.Name, running_pods, unready_pods, stopped_pods)

	return nil
}

func clusterListOpt(clusterName string) metav1.ListOptions {
	return metav1.ListOptions{
		LabelSelector: clusterSelector(clusterName).String(),
	}
}

func clusterSelector(clusterName string) labels.Selector {
	return labels.SelectorFromSet(labelsForCluster(clusterName))
}

func labelsForCluster(clusterName string) map[string]string {
	return map[string]string{
		"zookeeper_cluster": clusterName,
		"app":               "zookeeper",
	}
}
