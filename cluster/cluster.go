// Copyright 2018 The zookeeper-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"encoding/json"
	"fmt"
	"k8s.io/api/core/v1"
	"math"
	"os"
	"reflect"
	"strings"
	"time"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/generated/clientset/versioned"
	"zookeeper-operator/util/k8sutil"
	"zookeeper-operator/util/retryutil"
	"zookeeper-operator/util/zookeeperutil"

	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

var (
	reconcileInterval         = 8 * time.Second
	podTerminationGracePeriod = int64(5)
)

type clusterEventType string

const (
	eventModifyCluster clusterEventType = "Modify"
)

type clusterEvent struct {
	typ     clusterEventType
	cluster *api.ZookeeperCluster
}

type Config struct {
	KubeCli        kubernetes.Interface
	ZookeeperCRCli versioned.Interface
}

type Cluster struct {
	logger *logrus.Entry

	config Config

	cluster *api.ZookeeperCluster

	// Members represents the Members in the zookeeper cluster.
	// the name of the member is the the name of the pod the member
	// process runs in.
	Members zookeeperutil.MemberSet

	eventsCli corev1.EventInterface

	// Pods running as zookeeper members
	runningPods []*v1.Pod

	// Pods are running and ready to be zookeeper members.
	// Need to reconfigure zookeeper cluster to make these members become zookeeper members.
	readyPods []*v1.Pod
}

func New(config Config, cl *api.ZookeeperCluster) *Cluster {
	lg := logrus.WithField("pkg", "cluster").WithField("cluster-name", cl.Name)
	c := &Cluster{
		logger:    lg,
		config:    config,
		cluster:   cl,
		eventsCli: config.KubeCli.CoreV1().Events(cl.Namespace),
	}

	return c
}

func (c *Cluster) ResolvePodServiceAddress(member *zookeeperutil.Member) (string, error) {
	contactPoint := ""
	if len(os.Getenv("KUBECONFIG")) > 0 {
		pod, err := c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).Get(member.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		contactPoint = pod.Status.PodIP
	} else {
		contactPoint = member.Addr()
	}
	return contactPoint, nil
}

func (c *Cluster) Create() error {
	c.cluster.Status.SetPhase(api.ClusterPhaseCreating)
	c.cluster.Status.StartTime = metav1.Now()

	if err := c.setupServices(); err != nil {
		return fmt.Errorf("cluster create: failed to setup service: %v", err)
	}
	if err := c.UpdateCR(); err != nil {
		return fmt.Errorf("cluster create: failed to update cluster phase (%v): %v", api.ClusterPhaseCreating, err)
	}
	c.logClusterCreation()

	return nil
}

func (c *Cluster) Sync() error {
	running, ready, unready, err := c.getLivingPodsAndDeleteStoppedPods()
	if err != nil {
		return nil
	}

	c.runningPods = running
	c.readyPods = ready
	if c.runningPods == nil && unready == nil {
		// Don't start seed member in create. If we do that, and then delete the seed pod, we will
		// never generate the seed member again.
		err = c.StartSeedMember()
		if err != nil {
			return err
		}
	}

	if len(unready) > 0 {
		// Pod startup might take long, e.g. pulling image. It would deterministically become ready or succeeded/failed later.
		c.logger.Infof("skip reconciliation: ready (%v), unready (%v)", k8sutil.GetPodNames(ready), k8sutil.GetPodNames(unready))
		ReconcileFailed.WithLabelValues("not all pods are ready").Inc()
		return nil
	}

	if len(c.runningPods) == 0 {
		return nil
	}

	// On controller restore, we could have "members == nil"
	rerr := c.UpdateMembers()
	if rerr != nil {
		c.logger.Errorf("failed to update members: %v", rerr)
		return rerr
	}

	rerr = c.Reconcile()
	if rerr != nil {
		c.logger.Errorf("failed to reconcile: %v", rerr)
		return rerr
	}
	c.UpdateMemberStatus()
	if err := c.UpdateCR(); err != nil {
		c.logger.Warningf("periodic update CR status failed: %v", err)
	}
	return nil
}

func (c *Cluster) IsFinished() bool {
	return len(c.runningPods) == c.cluster.Spec.Size
}

func (c *Cluster) handleUpdateEvent(event *clusterEvent) error {
	oldSpec := c.cluster.Spec.DeepCopy()
	c.cluster = event.cluster

	if isSpecEqual(event.cluster.Spec, *oldSpec) {
		// We have some fields that once created could not be mutated.
		if !reflect.DeepEqual(event.cluster.Spec, *oldSpec) {
			c.logger.Infof("ignoring update event: %#v", event.cluster.Spec)
		}
		return nil
	}
	// TODO: we can't handle another upgrade while an upgrade is in progress

	c.logSpecUpdate(*oldSpec, event.cluster.Spec)
	return nil
}

func isSpecEqual(s1, s2 api.ClusterSpec) bool {
	if s1.Size != s2.Size || s1.Version != s2.Version {
		return false
	}
	return true
}

func (c *Cluster) StartSeedMember() error {
	m := &zookeeperutil.Member{
		Name:      fmt.Sprintf("%s-1", c.cluster.Name),
		Namespace: c.cluster.Namespace,
	}
	// TODO: @MDF: this fails if someone deletes/recreates a cluster too fast
	pod, err := c.createPod(make([]string, 0), m, "seed")
	if err != nil {
		return fmt.Errorf("failed to create seed member (%s): %v", m.Name, err)
	}
	// Almost impossible, but ...
	if k8sutil.IsPodReady(pod) {
		c.runningPods = append(c.runningPods, pod)
	}

	c.logger.Infof("cluster created with seed member (%s)", m.Name)
	_, err = c.eventsCli.Create(k8sutil.NewMemberAddEvent(m.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create new member add event: %v", err)
	}

	return nil
}

func (c *Cluster) setupServices() error {
	err := k8sutil.CreateClientService(c.config.KubeCli, c.cluster.Name, c.cluster.Namespace, c.cluster.AsOwner())
	if err != nil {
		return err
	}

	return k8sutil.CreatePeerService(c.config.KubeCli, c.cluster.Name, c.cluster.Namespace, c.cluster.AsOwner())
}

func (c *Cluster) isPodPVEnabled() bool {
	if podPolicy := c.cluster.Spec.Pod; podPolicy != nil {
		return podPolicy.PersistentVolumeClaimSpec != nil
	}
	return false
}

func (c *Cluster) createPod(existingCluster []string, m *zookeeperutil.Member, state string) (pod *v1.Pod, err error) {
	pod = k8sutil.NewZookeeperPod(m, existingCluster, c.cluster.Name, state, c.cluster.Spec, c.cluster.AsOwner())
	// TODO: @MDF: add PV support
	/*
		if c.isPodPVEnabled() {
			pvc := k8sutil.NewZookeeperPodPVC(m, *c.cluster.Spec.Pod.PersistentVolumeClaimSpec, c.cluster.Name, c.cluster.Namespace, c.cluster.AsOwner())
			_, err := c.config.KubeCli.CoreV1().PersistentVolumeClaims(c.cluster.Namespace).Create(pvc)
			if err != nil {
				return fmt.Errorf("failed to create PVC for member (%s): %v", m.Name, err)
			}
			k8sutil.AddZookeeperVolumeToPod(pod, pvc)
		} else {
			k8sutil.AddZookeeperVolumeToPod(pod, nil)
		}
	*/
	return c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).Create(pod)
}

func (c *Cluster) removePod(name string, wait bool) error {
	ns := c.cluster.Namespace
	gracePeriod := podTerminationGracePeriod
	if !wait {
		gracePeriod = int64(0)
	}
	opts := metav1.NewDeleteOptions(gracePeriod)
	err := c.config.KubeCli.CoreV1().Pods(ns).Delete(name, opts)
	if err != nil {
		if !k8sutil.IsKubernetesResourceNotFoundError(err) {
			return err
		}
	}
	return nil
}

func (c *Cluster) listPods() (pods []*v1.Pod, err error) {
	podList, err := c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).List(k8sutil.ClusterListOpt(c.cluster.Name))
	if err != nil {
		ReconcileFailed.WithLabelValues("failed to list pods").Inc()
		return nil, fmt.Errorf("failed to list pods for ZookeeperCluster %v:%v: %v", c.cluster.Namespace, c.cluster.Name, err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		// Avoid polling deleted pods. k8s issue where deleted pods would sometimes show the status Pending
		// See https://github.com/coreos/etcd-operator/issues/1693
		if pod.DeletionTimestamp != nil {
			continue
		}
		if len(pod.OwnerReferences) < 1 {
			c.logger.Warningf("list pods: ignore pod %v: no owner", pod.Name)
			continue
		}
		if pod.OwnerReferences[0].UID != c.cluster.UID {
			c.logger.Warningf("list pods: ignore pod %v: owner (%v) is not %v",
				pod.Name, pod.OwnerReferences[0].UID, c.cluster.UID)
			continue
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

func (c *Cluster) getPodsSeparatedByStatus() (running, ready, unready, stopped []*v1.Pod, err error) {
	pods, err := c.listPods()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, pod := range pods {
		switch pod.Status.Phase {
		case v1.PodRunning:
			if k8sutil.IsPodReady(pod) {
				if _, ok := pod.Labels["waiting"]; ok {
					ready = append(ready, pod)
				} else {
					running = append(running, pod)
				}
			} else {
				unready = append(unready, pod)
			}
		case v1.PodPending:
		case v1.PodUnknown:
			unready = append(unready, pod)
		default:
			stopped = append(stopped, pod)
		}
	}

	return running, ready, unready, stopped, nil
}

func (c *Cluster) getLivingPodsAndDeleteStoppedPods() (running, ready, unready []*v1.Pod, err error) {
	running, ready, unready, stopped, err := c.getPodsSeparatedByStatus()
	if err != nil {
		return nil, nil, nil, err
	}

	for _, pod := range stopped {
		err = c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).Delete(pod.Name, nil)
		c.logger.Warn("Fail to delete pod %v: %v", pod, err)
	}

	return running, ready, unready, nil
}

func (c *Cluster) UpdateMemberStatus() {
	var unready []string
	var ready []string
	for _, pod := range c.runningPods {
		if k8sutil.IsPodReady(pod) {
			ready = append(ready, pod.Name)
			continue
		}
		unready = append(unready, pod.Name)
	}

	c.cluster.Status.Members.Ready = ready
	c.cluster.Status.Members.Unready = unready
}

func (c *Cluster) UpdateCR() error {
	newCluster, err := c.config.ZookeeperCRCli.ZookeeperV1alpha1().ZookeeperClusters(c.cluster.Namespace).Update(c.cluster)
	if err != nil {
		return fmt.Errorf("failed to update CR: %v", err)
	}

	c.cluster = newCluster.DeepCopy()

	return nil
}

func (c *Cluster) ReportFailedStatus() {
	c.logger.Info("cluster failed. Reporting failed reason...")

	retryInterval := 5 * time.Second
	f := func() (bool, error) {
		c.cluster.Status.SetPhase(api.ClusterPhaseFailed)
		err := c.UpdateCR()
		if err == nil || k8sutil.IsKubernetesResourceNotFoundError(err) {
			return true, nil
		}

		if !apierrors.IsConflict(err) {
			c.logger.Warningf("retry report status in %v: fail to update: %v", retryInterval, err)
			return false, nil
		}

		cl, err := c.config.ZookeeperCRCli.ZookeeperV1alpha1().ZookeeperClusters(c.cluster.Namespace).
			Get(c.cluster.Name, metav1.GetOptions{})
		if err != nil {
			// Update (PUT) will return conflict even if object is deleted since we have UID set in object.
			// Because it will check UID first and return something like:
			// "Precondition failed: UID in precondition: 0xc42712c0f0, UID in object meta: ".
			if k8sutil.IsKubernetesResourceNotFoundError(err) {
				return true, nil
			}
			c.logger.Warningf("retry report status in %v: fail to get latest version: %v", retryInterval, err)
			return false, nil
		}
		c.cluster = cl
		return false, nil
	}

	retryutil.Retry(retryInterval, math.MaxInt64, f)
}

func (c *Cluster) name() string {
	return c.cluster.GetName()
}

func (c *Cluster) logClusterCreation() {
	specBytes, err := json.MarshalIndent(c.cluster.Spec, "", "    ")
	if err != nil {
		c.logger.Errorf("failed to marshal spec of cluster %s: %v", c.cluster.Name, err)
	}

	c.logger.Info("creating cluster with Spec:")
	for _, m := range strings.Split(string(specBytes), "\n") {
		c.logger.Info(m)
	}
}

func (c *Cluster) logSpecUpdate(oldSpec, newSpec api.ClusterSpec) {
	oldSpecBytes, err := json.MarshalIndent(oldSpec, "", "    ")
	if err != nil {
		c.logger.Errorf("failed to marshal cluster spec: %v", err)
	}
	newSpecBytes, err := json.MarshalIndent(newSpec, "", "    ")
	if err != nil {
		c.logger.Errorf("failed to marshal cluster spec: %v", err)
	}

	c.logger.Infof("spec update: Old Spec:")
	for _, m := range strings.Split(string(oldSpecBytes), "\n") {
		c.logger.Info(m)
	}

	c.logger.Infof("New Spec:")
	for _, m := range strings.Split(string(newSpecBytes), "\n") {
		c.logger.Info(m)
	}

}
