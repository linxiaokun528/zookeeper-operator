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
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetretry "k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"reflect"
	"sync"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/client"
)

type clusterEventType string

type clusterEvent struct {
	typ     clusterEventType
	cluster *api.ZookeeperCluster
}

type Cluster struct {
	client client.CRClient

	zkCR *api.ZookeeperCluster

	locker sync.Locker
}

func New(client client.CRClient, zkCR *api.ZookeeperCluster) *Cluster {
	c := &Cluster{
		client: client,
		zkCR:   zkCR,
		locker: &sync.Mutex{},
	}

	return c
}

func (c *Cluster) SyncAndUpdateStatus() error {
	origin := c.zkCR.Status.DeepCopy()
	defer func() {
		if !reflect.DeepEqual(origin, c.zkCR.Status) {
			c.updateStatus()
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
		// TODO: add a value to the status in CRD
		new, err = c.client.ZookeeperCluster().Update(new)
		if err != nil {
			klog.Warningf("	Update CR status failed: %v", err)
		}
		return err
	})
}

func (c *Cluster) sync() error {
	if c.zkCR.Status.StartTime == nil {
		now := metav1.Now()
		c.zkCR.Status.StartTime = &now
		c.zkCR.Status.SetVersion(c.zkCR.Spec.Version)
		c.zkCR.Status.AppendCreatingCondition()

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

	defer func() {
		// TODO: it's not correct
		c.zkCR.Status.Size = c.zkCR.Status.Members.Running.Size()
	}()

	if c.zkCR.Status.Members.Unready.Size() > 0 {
		klog.Infof(fmt.Sprintf("skip reconciliation: running (%v), unready (%v)",
			c.zkCR.Status.Members.Running.GetMemberNames(), c.zkCR.Status.Members.Unready.GetMemberNames()))
		ReconcileFailed.WithLabelValues("not all pods are ready").Inc()
		return nil
	}

	if c.zkCR.Status.Members.Stopped.Size() > 0 {
		c.zkCR.Status.AppendRecoveringCondition(&c.zkCR.Status.Members.Stopped)
		c.ReplaceStoppedMembers()
		return fmt.Errorf("Some members are stopped: (%v)", c.zkCR.Status.Members.Stopped.GetMemberNames())
	}

	if c.zkCR.Status.Members.Running.Size() < c.zkCR.Spec.Size {
		c.zkCR.Status.AppendScalingUpCondition(c.zkCR.Status.Members.Running.Size(), c.zkCR.Spec.Size)

		return c.scaleUp()
	} else if c.zkCR.Status.Members.Running.Size() > c.zkCR.Spec.Size {
		c.zkCR.Status.AppendScalingDownCondition(c.zkCR.Status.Members.Running.Size(), c.zkCR.Spec.Size)

		return c.scaleDown()
	}

	if c.zkCR.Status.GetCurrentCondition().Type == api.ClusterScalingUp {
		err = c.reconfig(c.zkCR.Status.Members.Running.GetClientHosts(),
			c.zkCR.Status.Members.Running.GetClusterConfig())
		if err != nil {
			return err
		}
	}

	if c.needUpgrade() {
		c.zkCR.Status.AppendUpgradingCondition(c.zkCR.Spec.Version)
		c.zkCR.Status.UpgradeVersionTo(c.zkCR.Spec.Version)

		m := c.pickOneOldMember()
		return c.upgradeOneMember(m.Name())
	}

	c.zkCR.Status.SetRunningCondition()

	return nil
}

// TODO: also need to check /client/zookeeper
func (c *Cluster) IsFinished() bool {
	return c.zkCR.Status.Members.Running.Size() == c.zkCR.Spec.Size
}
