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
	if err := c.createPod(make([]string, 0), m, "seed"); err != nil {
		return fmt.Errorf("failed to create seed member (%s): %v", m.Name, err)
	}
	c.logger.Infof("cluster created with seed member (%s)", m.Name)
	_, err := c.eventsCli.Create(k8sutil.NewMemberAddEvent(m.Name, c.cluster))
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

func (c *Cluster) createPod(existingCluster []string, m *zookeeperutil.Member, state string) error {
	pod := k8sutil.NewZookeeperPod(m, existingCluster, c.cluster.Name, state, c.cluster.Spec, c.cluster.AsOwner())
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
	_, err := c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).Create(pod)
	return err
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

func (c *Cluster) PollPods() (running, pending []*v1.Pod, err error) {
	podList, err := c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).List(k8sutil.ClusterListOpt(c.cluster.Name))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list running pods: %v", err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		// Avoid polling deleted pods. k8s issue where deleted pods would sometimes show the status Pending
		// See https://github.com/coreos/etcd-operator/issues/1693
		if pod.DeletionTimestamp != nil {
			continue
		}
		if len(pod.OwnerReferences) < 1 {
			c.logger.Warningf("pollPods: ignore pod %v: no owner", pod.Name)
			continue
		}
		if pod.OwnerReferences[0].UID != c.cluster.UID {
			c.logger.Warningf("pollPods: ignore pod %v: owner (%v) is not %v",
				pod.Name, pod.OwnerReferences[0].UID, c.cluster.UID)
			continue
		}
		if pod.Labels["app"] != "zookeeper" {
			c.logger.Warningf("pollPods: ignore pod %v: label app (%v) is not \"zookeeper\"",
				pod.Name, pod.Labels["app"])
			continue
		}

		if pod.Labels["zookeeper_cluster"] != c.cluster.Name {
			c.logger.Warningf("pollPods: ignore pod %v: label zookeeper_cluster (%v) is not %v",
				pod.Name, pod.Labels["zookeeper_cluster"], c.cluster.Name)
			continue
		}

		// TODO: release pods whose owner is c.cluster, but the label doesn't match.
		switch pod.Status.Phase {
		case v1.PodRunning:
			running = append(running, pod)
		case v1.PodPending:
			pending = append(pending, pod)
		}
	}

	return running, pending, nil
}

func (c *Cluster) UpdateMemberStatus(running []*v1.Pod) {
	var unready []string
	var ready []string
	for _, pod := range running {
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
