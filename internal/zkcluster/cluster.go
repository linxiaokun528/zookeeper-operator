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

package zkcluster

import (
	"reflect"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetretry "k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"

	client2 "zookeeper-operator/internal/client"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/errors"
)

type Cluster struct {
	client       client2.CRClient
	zkCR         *api.ZookeeperCluster
	locker       sync.Locker
	expectations controller.ControllerExpectationsInterface
}

func New(client client2.CRClient, zkCR *api.ZookeeperCluster,
	expections controller.ControllerExpectationsInterface) *Cluster {
	c := &Cluster{
		client:       client,
		zkCR:         zkCR,
		locker:       &sync.Mutex{},
		expectations: expections,
	}

	return c
}

func (c *Cluster) SyncAndUpdateStatus() (err error) {
	origin := c.zkCR.Status.DeepCopy()
	defer func() {
		if !reflect.DeepEqual(origin, c.zkCR.Status) {
			update_err := c.updateStatus()
			err = errors.NewCompoundedError(err, update_err)
		} else {
			klog.Infof("Status not changed. Don't need to update the status of zookeeper cluster %s", c.zkCR.GetFullName())
		}
	}()
	return c.sync()
}

func (c *Cluster) updateStatus() error {
	return clientsetretry.RetryOnConflict(clientsetretry.DefaultRetry, func() error {
		new, err := c.client.ZookeeperCluster().Get(c.zkCR.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		new.Status = c.zkCR.Status
		// We are supposed to use UpdateStatus here. But we don't have a status resource yet.
		// TODO: add a default value to the status in CRD
		new, err = c.client.ZookeeperCluster().Update(new)
		if err == nil {
			klog.Infof("Status of zookeeper cluster %s updated successfully", c.zkCR.GetFullName())
		} else {
			klog.Warningf("Failed to update the status of zookeeper cluster %s: %v", c.zkCR.GetFullName(), err)
		}
		return err
	})
}

func (c *Cluster) sync() error {
	klog.Infof("Start syncing zookeeper cluster %v", c.zkCR.GetFullName())
	defer klog.Infof("Finish syncing zookeeper cluster %v", c.zkCR.GetFullName())

	if _, ok, err := c.expectations.GetExpectations(c.zkCR.GetFullName()); !ok && err == nil {
		c.expectations.SetExpectations(c.zkCR.GetFullName(), 0, 0)
	}

	c.zkCR.SetDefaults()

	if c.zkCR.Status.StartTime == nil {
		// TODO: consider the situation that the services are deleted
		err := c.create()
		if err != nil {
			return err
		}
	}

	err := c.syncCurrentMembers()
	if err != nil {
		return err
	}

	members := c.zkCR.Status.Members

	defer func() {
		c.zkCR.Status.Size = members.Running.Size()
	}()

	if members.Unready.Size() > 0 {
		klog.Infof("Skip syncing for zookeeper cluster %v. Some pods are unready: %v",
			c.zkCR.GetFullName(), members.Unready.GetMemberNames())
		ReconcileFailed.WithLabelValues("not all pods are ready").Inc()
		return nil
	}

	if members.Stopped.Size() > 0 {
		c.zkCR.Status.AppendRecoveringCondition(&members.Stopped)
		klog.Warningf("There are stopped members of zookeeper cluster %v: %v",
			c.zkCR.GetFullName(), members.Stopped)
		err = c.ReplaceStoppedMembers()
		if err != nil {
			return err
		}
		return nil
	}

	if members.Ready.Size() == 0 && members.Running.Size() < c.zkCR.Spec.Size {
		c.zkCR.Status.AppendScalingUpCondition(members.Running.Size(), c.zkCR.Spec.Size)

		return c.beginScaleUp()
	}

	if c.zkCR.Status.GetCurrentCondition().Type == api.ClusterScalingUp || members.Ready.Size() > 0 {
		err := c.finishScaleUp()
		if err != nil {
			return err
		}
		c.zkCR.Status.SetRunningCondition()
	}

	if members.Running.Size() > c.zkCR.Spec.Size {
		c.zkCR.Status.AppendScalingDownCondition(members.Running.Size(), c.zkCR.Spec.Size)

		err = c.scaleDown()
		if err != nil {
			return err
		}
		c.zkCR.Status.SetRunningCondition()
	}

	if c.zkCR.Spec.Version != c.zkCR.Status.CurrentVersion && c.zkCR.Status.TargetVersion == api.Empty ||
		// Users may change the Spec.Version while doing upgrading
		c.zkCR.Status.TargetVersion != c.zkCR.Spec.Version && c.zkCR.Status.TargetVersion != api.Empty {
		c.zkCR.Status.AppendUpgradingCondition(c.zkCR.Spec.Version)
		c.beginUpgrade()
	}

	if c.zkCR.Status.TargetVersion != api.Empty {
		if c.pickOneOldMember() == nil {
			c.finishUpgrade()
			c.zkCR.Status.SetRunningCondition()
			return nil
		}

		err = c.upgradeOneMember()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) IsFinished() bool {
	return c.zkCR.Status.GetCurrentCondition().Type == api.ClusterRunning &&
		c.zkCR.Status.Members.Running.Size() == c.zkCR.Spec.Size &&
		c.zkCR.Spec.Version == c.zkCR.Status.CurrentVersion &&
		c.zkCR.Status.TargetVersion == api.Empty
}
