package zkcluster

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog"

	"zookeeper-operator/internal/util/zookeeper"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	k8sutil2 "zookeeper-operator/pkg/k8sutil"
)

func (c *Cluster) listPods() (pods []*v1.Pod, err error) {
	// TODO: If we use shared informer for pod, then use lister to get the pods
	podList, err := c.client.Pod().List(clusterListOpt(c.zkCR.Name))
	if err != nil {
		ReconcileFailed.WithLabelValues("failed to list pods").Inc()
		return nil, fmt.Errorf("Failed to list pods for ZookeeperCluster %v:%v: %v", c.zkCR.Namespace, c.zkCR.Name, err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		// Avoid polling deleted pods. k8s issue where deleted pods would sometimes show the status Pending
		// See https://github.com/coreos/etcd-operator/issues/1693
		if pod.DeletionTimestamp != nil {
			continue
		}
		if !c.checkOwnerReference(pod) {
			klog.Warningf("List pods: ignore pod %v: zookeeper cluster %v(UID:%v) is not a owner",
				pod.Name, c.zkCR.GetFullName(), c.zkCR.UID)

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
	klog.V(4).Infof("Syncing zookeeper members for %v...", c.zkCR.GetFullName())
	defer func() {
		if err == nil {
			klog.V(4).Infof("Zookeeper members for %v synced successfully", c.zkCR.GetFullName())
		}
	}()

	pods, err := c.listPods()
	if err != nil {
		return err
	}
	activePods, unreadyPods, stoppedPods := k8sutil2.GetPodsSeparatedByStatus(pods)

	runningMemberAddress, err := c.getRunningMemberAddresses(activePods)
	if err != nil {
		return err
	}

	runningPods := []*v1.Pod{}
	readyPods := []*v1.Pod{}

	for _, pod := range activePods {
		address := api.Address(pod.Name, pod.Namespace)
		_, ok := runningMemberAddress[address]
		if ok {
			runningPods = append(runningPods, pod)
		} else {
			readyPods = append(readyPods, pod)
		}
	}

	c.zkCR.Status.Members = api.NewZKCluster(c.zkCR.Namespace, c.zkCR.Name, c.zkCR.Status.CurrentVersion, runningPods,
		readyPods, unreadyPods, stoppedPods)

	return nil
}

func (c *Cluster) getRunningMemberAddresses(activePods []*v1.Pod) (map[string]struct{}, error) {
	if len(activePods) == 0 {
		return map[string]struct{}{}, nil
	}

	addresses := make([]string, len(activePods))

	for i, pod := range activePods {
		addresses[i] = api.Address(pod.Name, pod.Namespace)
	}

	// If we don't get the real member from the zookeeper, then we are not able to handle the situation where
	// "c.zkCR.Status.Members.Running" is modified by someone.
	serverStatements, err := getconfig(c.zkCR.GetFullName(), addresses)
	if err != nil {
		return nil, err
	}

	result := make(map[string]struct{}, len(serverStatements))
	for _, serverStatement := range serverStatements {
		s := api.NewServerStatementFromString(serverStatement)
		result[s.Address] = struct{}{}
	}
	return result, nil
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

func getconfig(zkName string, addresses []string) (serverStatements []string, err error) {
	klog.V(4).Infof("Getting config from zookeeper cluster %s with hosts %s...", zkName, addresses)

	defer func() {
		if err == nil {
			klog.V(4).Infof("Get config from zookeeper cluster %s successfully: %s",
				zkName, serverStatements)
		}
	}()
	return zookeeper.GetClusterConfig(addresses)
}
