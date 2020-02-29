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
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/client"
	zklisters "zookeeper-operator/generated/listers/zookeeper/v1alpha1"
	"zookeeper-operator/util/k8sutil"
	"zookeeper-operator/util/retryutil"
	"zookeeper-operator/util/zookeeperutil"

	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
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

// TODO: use Lister to list pods/zookeepers
type Lister struct {
	podLister corev1listers.PodLister
	zkLister  zklisters.ZookeeperClusterLister
}

type Cluster struct {
	logger *logrus.Entry
	client client.CRClient

	zkCR     *api.ZookeeperCluster
	zkStatus *zookeeperutil.ZKCluster

	locker sync.Locker
}

func New(client client.CRClient, zkCR *api.ZookeeperCluster) *Cluster {
	lg := logrus.WithField("pkg", "zkCR").WithField("zkCR-name", zkCR.Name)
	c := &Cluster{
		logger: lg,
		client: client,
		zkCR:   zkCR,
		locker: &sync.Mutex{},
	}

	return c
}

func (c *Cluster) Create() error {
	c.zkCR.Status.SetPhase(api.ClusterPhaseCreating)
	c.zkCR.Status.StartTime = metav1.Now()

	if err := c.setupServices(); err != nil {
		return fmt.Errorf("zkCR create: failed to setup service: %v", err)
	}
	if err := c.UpdateCR(); err != nil {
		return fmt.Errorf("zkCR create: failed to update zkCR phase (%v): %v", api.ClusterPhaseCreating, err)
	}
	c.logClusterCreation()

	return nil
}

func (c *Cluster) Sync() error {
	original_cluster := c.zkCR.DeepCopy()
	err := c.initCurrentStatus()
	if err != nil {
		return err
	}

	if c.zkStatus.GetRunningMembers().Size() == 0 && c.zkStatus.GetUnreadyMembers().Size() == 0 &&
		c.zkStatus.GetStoppedMembers().Size() == 0 {
		// Don't start seed member in create. If we do that, and then delete the seed pod, we will
		// never generate the seed member again.
		return c.scaleUp()
	}

	if c.zkStatus.GetUnreadyMembers().Size() > 0 {
		// Pod startup might take long, e.g. pulling image. It would deterministically become ready or succeeded/failed later.
		c.logger.Infof("skip reconciliation: running (%v), unready (%v)",
			c.zkStatus.GetRunningMembers(), c.zkStatus.GetUnreadyMembers())
		ReconcileFailed.WithLabelValues("not all pods are ready").Inc()
		return nil
	}

	if c.zkStatus.GetStoppedMembers().Size() > 0 {
		c.ReplaceStoppedMembers()
		return fmt.Errorf("Some members are stopped: (%v)", c.zkStatus.GetStoppedMembers().GetMemberNames())
	}

	rerr := c.Reconcile()
	if rerr != nil {
		c.logger.Errorf("failed to reconcile: %v", rerr)
		return rerr
	}
	c.UpdateMemberStatus()

	if reflect.DeepEqual(original_cluster, c.zkCR) {
		return nil
	}

	// TODO: move the following to defer. We should update the CR every time something changes
	data, err := k8sutil.CreatePatch(original_cluster, c.zkCR, api.ZookeeperCluster{})
	if err == nil {
		c.logger.Info(string(data))
	}

	if err := c.UpdateCR(); err != nil {
		c.logger.Warningf("	Update CR status failed: %v", err)
	}
	return nil
}

// TODO: also need to check /client/zookeeper
func (c *Cluster) IsFinished() bool {
	return len(c.zkStatus.GetRunningMembers()) == c.zkCR.Spec.Size
}

func (c *Cluster) handleUpdateEvent(event *clusterEvent) error {
	oldSpec := c.zkCR.Spec.DeepCopy()
	c.zkCR = event.cluster

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

func (c *Cluster) setupServices() error {
	client_svc := k8sutil.NewClientService(c.zkCR.Name)
	err := c.createService(client_svc)
	if err != nil {
		return err
	}

	peer_svc := k8sutil.NewPeerService(c.zkCR.Name)

	return c.createService(peer_svc)
}

func (c *Cluster) isPodPVEnabled() bool {
	if podPolicy := c.zkCR.Spec.Pod; podPolicy != nil {
		return podPolicy.PersistentVolumeClaimSpec != nil
	}
	return false
}

func (c *Cluster) createPod(m *zookeeperutil.Member, allClusterMembers zookeeperutil.Members) (pod *v1.Pod, err error) {
	pod = k8sutil.NewZookeeperPod(m, allClusterMembers, c.zkCR.Spec, c.zkCR.AsOwner())
	// TODO: @MDF: add PV support
	/*
		if c.isPodPVEnabled() {
			pvc := k8sutil.NewZookeeperPodPVC(m, *c.zkCR.Spec.Pod.PersistentVolumeClaimSpec, c.zkCR.Name, c.zkCR.Namespace, c.zkCR.AsOwner())
			_, err := c.client.KubeCli.CoreV1().PersistentVolumeClaims(c.zkCR.Namespace).Create(pvc)
			if err != nil {
				return fmt.Errorf("failed to create PVC for member (%s): %v", m.Name, err)
			}
			k8sutil.AddZookeeperVolumeToPod(pod, pvc)
		} else {
			k8sutil.AddZookeeperVolumeToPod(pod, nil)
		}
	*/
	return c.client.Pod().Create(pod)
}

func (c *Cluster) removePod(name string) error {
	err := c.client.Pod().Delete(name, nil)
	if err != nil {
		if !k8sutil.IsKubernetesResourceNotFoundError(err) {
			return err
		}
	}
	return nil
}

func (c *Cluster) listPods() (pods []*v1.Pod, err error) {
	podList, err := c.client.Pod().List(k8sutil.ClusterListOpt(c.zkCR.Name))
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
			c.logger.Warningf("list pods: ignore pod %v: no owner", pod.Name)
			continue
		}
		if pod.OwnerReferences[0].UID != c.zkCR.UID {
			c.logger.Warningf("list pods: ignore pod %v: owner (%v) is not %v",
				pod.Name, pod.OwnerReferences[0].UID, c.zkCR.UID)
			continue
		}
		pods = append(pods, pod.DeepCopy())
	}

	return pods, nil
}

func (c *Cluster) initCurrentStatus() (err error) {
	pods, err := c.listPods()
	if err != nil {
		return err
	}
	running_pods, unready_pods, stopped_pods := k8sutil.GetPodsSeparatedByStatus(pods)

	c.zkStatus = zookeeperutil.NewZKCluster(c.zkCR.Namespace, c.zkCR.Name, running_pods, unready_pods, stopped_pods)

	return nil
}

func (c *Cluster) createService(service *v1.Service) error {
	k8sutil.AddOwnerRefToObject(service, c.zkCR.AsOwner())

	_, err := c.client.Service().Create(service)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// TODO: We may need to decide what kinds of information we need.
// If the ready members means configured members, we may need to add a configuredMembers in zookeeperutil.ZKCluster
func (c *Cluster) UpdateMemberStatus() {
	c.zkCR.Status.Members.Ready = c.zkStatus.GetRunningMembers().GetMemberNames()
	c.zkCR.Status.Members.Unready = c.zkStatus.GetUnreadyMembers().GetMemberNames()

	// If we don't sort here, the different order of ready/unready may cause a update on CR.
	sort.Strings(c.zkCR.Status.Members.Ready)
	sort.Strings(c.zkCR.Status.Members.Unready)
}

func (c *Cluster) UpdateCR() error {
	c.logger.Info("Updating CR")
	newCluster, err := c.client.ZookeeperCluster().Update(c.zkCR)
	if err != nil {
		return fmt.Errorf("failed to update CR: %v", err)
	}
	c.zkCR = newCluster.DeepCopy()

	return nil
}

func (c *Cluster) ReportFailedStatus() {
	c.logger.Info("zkCR failed. Reporting failed reason...")

	retryInterval := 5 * time.Second
	f := func() (bool, error) {
		c.zkCR.Status.SetPhase(api.ClusterPhaseFailed)
		err := c.UpdateCR()
		if err == nil || k8sutil.IsKubernetesResourceNotFoundError(err) {
			return true, nil
		}

		if !apierrors.IsConflict(err) {
			c.logger.Warningf("retry report status in %v: fail to update: %v", retryInterval, err)
			return false, nil
		}

		cl, err := c.client.ZookeeperCluster().Get(c.zkCR.Name, metav1.GetOptions{})
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
		c.zkCR = cl
		return false, nil
	}

	retryutil.Retry(retryInterval, math.MaxInt64, f)
}

func (c *Cluster) logClusterCreation() {
	specBytes, err := json.MarshalIndent(c.zkCR.Spec, "", "    ")
	if err != nil {
		c.logger.Errorf("failed to marshal spec of zkCR %s: %v", c.zkCR.Name, err)
	}

	c.logger.Info("creating zkCR with Spec:")
	for _, m := range strings.Split(string(specBytes), "\n") {
		c.logger.Info(m)
	}
}

func (c *Cluster) logSpecUpdate(oldSpec, newSpec api.ClusterSpec) {
	oldSpecBytes, err := json.MarshalIndent(oldSpec, "", "    ")
	if err != nil {
		c.logger.Errorf("failed to marshal zkCR spec: %v", err)
	}
	newSpecBytes, err := json.MarshalIndent(newSpec, "", "    ")
	if err != nil {
		c.logger.Errorf("failed to marshal zkCR spec: %v", err)
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
