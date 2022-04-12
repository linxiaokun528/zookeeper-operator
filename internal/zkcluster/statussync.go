package zkcluster

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	k8sutil2 "zookeeper-operator/pkg/k8sutil"
)

func (c *Cluster) listPods() (pods []*v1.Pod, err error) {
	// TODO: If we use shared informer for pod, then use lister to get the pods
	podList := &v1.PodList{}
	err = c.client.List(c.ctx, podList, &client.ListOptions{LabelSelector: clusterSelector(c.zkCR.Name)})
	if err != nil {
		ReconcileFailed.WithLabelValues("failed to list pods").Inc()
		return nil, fmt.Errorf("Failed to list pods for ZookeeperCluster %v:%v: %v", c.zkCR.Namespace, c.zkCR.Name, err)
	}

	for _, pod := range podList.Items {
		// Avoid polling deleted pods. k8s issue where deleted pods would sometimes show the status Pending
		// See https://github.com/coreos/etcd-operator/issues/1693
		if pod.DeletionTimestamp != nil {
			continue
		}
		if !c.checkOwnerReference(&pod) {
			klog.Warningf("List pods: ignore pod %v: zookeeper cluster %v(UID:%v) is not a owner",
				pod.Name, c.zkCR.GetNamespacedName(), c.zkCR.UID)

			continue
		}
		pods = append(pods, pod.DeepCopy())
	}

	return pods, nil
}

func (c *Cluster) checkOwnerReference(pod *v1.Pod) bool {
	if len(pod.OwnerReferences) < 1 {
		klog.Warningf("list pods: ignore pod %v: no owner", pod.Name)
		return false
	}
	for _, ownerReference := range pod.OwnerReferences {
		if ownerReference.UID == c.zkCR.UID {
			return true
		}
	}
	klog.Warningf("list pods: ignore pod %v: owner (%v) is not %v",
		pod.Name, pod.OwnerReferences[0].UID, c.zkCR.UID)
	return false
}

func (c *Cluster) syncCurrentMembers() (err error) {
	klog.V(1).Infof("Syncing zookeeper members for %v...", c.zkCR.GetNamespacedName())
	defer func() {
		if err == nil {
			klog.V(1).Infof("Zookeeper members for %v synced successfully", c.zkCR.GetNamespacedName())
		}
	}()

	pods, err := c.listPods()
	if err != nil {
		return err
	}
	activePods, unreadyPods, stoppedPods := k8sutil2.GetPodsSeparatedByStatus(pods)

	runningMemberNames := map[string]struct{}{}
	if c.zkCR.Status.Members != nil {
		for _, runningMemberName := range c.zkCR.Status.Members.Running.GetMemberNames() {
			runningMemberNames[runningMemberName] = struct{}{}
		}
	}

	runningPods := []*v1.Pod{}
	readyPods := []*v1.Pod{}
	for _, activePod := range activePods {
		if _, ok := runningMemberNames[activePod.Name]; ok {
			runningPods = append(runningPods, activePod)
		} else {
			readyPods = append(readyPods, activePod)
		}
	}

	c.zkCR.Status.Members = api.NewZKCluster(c.zkCR.Namespace, c.zkCR.Name, c.zkCR.Status.CurrentVersion, runningPods,
		readyPods, unreadyPods, stoppedPods)

	return nil
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
