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

package zookeeperutil

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"sort"
	"strconv"
	"strings"
)

type clusterID struct {
	clusterName string
	namespace   string
}

func (c *clusterID) ClusterName() string {
	return c.clusterName
}

func (c *clusterID) Namespace() string {
	return c.namespace
}

func (c *clusterID) String() string {
	return fmt.Sprintf("%s(%s)", c.clusterName, c.namespace)
}

type Member struct {
	id      int
	version string
	*clusterID
}

func (m *Member) Name() string {
	return fmt.Sprintf("%s-%d", m.ClusterName(), m.id)
}

func (m *Member) Version() string {
	return m.version
}

func (m *Member) Addr() string {
	// If we use "<pod-name>.<service-name>" or "<pod-name>.<service-name>.<pod-namespace>.svc" as the pod domain,
	// we may get an error like
	// "can't resolve 'example-zookeeper-cluster-1.example-zookeeper-cluster': Name does not resolve" from this pod
	// domain first. Even we can successfully resolve the pod domain one time, we may get the same
	// "Name does not resolve" error the next time. So even if we have init container to make sure the pod domain
	// apprears in kubedns, we can still get errors.
	// But if we use "<pod-name>.<service-name>.<pod-namespace>.svc.<cluster-domain>"(cluster-domain is
	// something like "cluster.local") as the pod domain, we will always resolve the pod domain. Because
	// "<pod-name>.<service-name>.<pod-namespace>.svc.<cluster-domain>" is specified in file "/etc/hosts" of the pod.

	// TODO: for the sake of debug, we set the value to the full form. But we should set it to <pod-name>.<service-name>
	// to reduce the DNS search.
	// "<pod-name>.<service-name>.<pod-namespace>.svc.<cluster-domain>". But this will result in the failure to detect
	// if the domain/endpoint is ready. We need to change this to "<pod-name>.<service-name>" in the final release.
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", m.Name(), m.ClusterName(), m.Namespace())
}

func (m *Member) ID() int {
	return m.id
}

type Members map[int]*Member

func (ms Members) Add(m *Member) {
	ms[m.ID()] = m
}

func (ms Members) Remove(id int) {
	delete(ms, id)
}

func (ms Members) Update(m Members) {
	for id, member := range m {
		ms[id] = member
	}
}

func (ms Members) Size() int {
	return len(ms)
}

func (ms Members) Copy() Members {
	clone := Members{}
	clone.Update(ms)
	return clone
}

// the set of all members of s1 that are not members of s2
func (ms Members) Diff(other Members) Members {
	diff := Members{}
	for n, m := range ms {
		if _, ok := other[n]; !ok {
			diff[n] = m
		}
	}
	return diff
}

func (ms Members) GetMemberNames() (res []string) {
	for _, m := range ms {
		res = append(res, m.Name())
	}
	return res
}

func (ms Members) GetClientHosts() []string {
	hosts := make([]string, 0)
	for _, m := range ms {
		hosts = append(hosts, fmt.Sprintf("%s:2181", m.Addr()))
	}
	sort.Strings(hosts)
	return hosts
}

func (ms Members) GetClusterConfig() []string {
	// TODO: make clusterconfig a struct
	clusterConfig := make([]string, 0)
	for _, m := range ms {
		clusterConfig = append(clusterConfig, fmt.Sprintf("server.%d=%s:2888:3888:participant;0.0.0.0:2181", m.ID(), m.Addr()))
	}
	sort.Strings(clusterConfig)
	return clusterConfig
}

// TODO: Make this struct a property of zookeeper CR
type ZKCluster struct {
	*clusterID
	running Members
	unready Members
	stopped Members
}

func NewZKCluster(namespace, cluster_name string, runningPods, unreadyPods, stoppedPods []*v1.Pod) *ZKCluster {
	id := &clusterID{
		clusterName: cluster_name,
		namespace:   namespace,
	}
	return &ZKCluster{
		clusterID: id,
		running:   podsToMembers(id, runningPods),
		unready:   podsToMembers(id, unreadyPods),
		stopped:   podsToMembers(id, stoppedPods),
	}
}

func podsToMembers(id *clusterID, pods []*v1.Pod) Members {
	members := Members{}
	for _, pod := range pods {
		members.Add(podToMember(id, pod))
	}
	return members
}

func podToMember(clusterID *clusterID, pod *v1.Pod) *Member {
	i := strings.LastIndex(pod.Name, "-")
	if i == -1 {
		panic(fmt.Sprintf("unexpected member name: %s", pod.Name))
	}

	id, err := strconv.Atoi(pod.Name[i+1:])

	if err != nil {
		panic(fmt.Sprintf("unexpected member name: %s", pod.Name))
	}

	return &Member{
		id:        id,
		clusterID: clusterID,
		version:   getZookeeperVersion(pod),
	}
}

// TODO: Figure out another way to do this
func getZookeeperVersion(pod *v1.Pod) string {
	return pod.Annotations["zookeeper.version"]
}

func (z *ZKCluster) RemoveMembers(size int) Members {
	// In the future, we may support removing members while there are some stopped/unready members.
	if size > z.running.Size() {
		panic(fmt.Sprintf(
			"Receive a request of deleting %d member(s), but ZkCluster %s has only %d member(s) running.",
			size, z.clusterID, z.running.Size()))
	}

	result := Members{}
	for i := 0; i < size; i++ {
		result.Add(z.removeOneMember())
	}
	return result
}

func (z *ZKCluster) removeOneMember() *Member {
	var last *Member = nil
	for _, m := range z.running {
		if last == nil {
			last = m
		} else {
			if last.ID() < m.ID() {
				last = m
			}
		}
	}
	z.running.Remove(last.ID())
	return last
}

func (z *ZKCluster) AddMembers(size int) Members {
	result := Members{}
	for i := 0; i < size; i++ {
		result.Add(z.addOneMember())
	}
	return result
}

func (z *ZKCluster) addOneMember() *Member {
	m := Member{
		id:        z.nextMemberID(),
		clusterID: z.clusterID,
	}
	z.unready.Add(&m)
	return &m
}

func (z *ZKCluster) nextMemberID() int {
	// In the future, we may support removing members while there are some stopped/unready members.
	missedID := 0
	ids := map[int]struct{}{}
	for _, m := range z.running {
		ids[m.ID()] = struct{}{}
	}
	for _, m := range z.unready {
		ids[m.ID()] = struct{}{}
	}

	for {
		if _, ok := ids[missedID]; ok {
			missedID++
		} else {
			break
		}
	}
	return missedID
}

func (z *ZKCluster) GetRunningMembers() Members {
	return z.running
}

func (z *ZKCluster) GetUnreadyMembers() Members {
	return z.unready
}

func (z *ZKCluster) GetStoppedMembers() Members {
	return z.stopped
}
