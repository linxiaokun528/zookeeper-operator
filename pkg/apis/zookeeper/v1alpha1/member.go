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

package v1alpha1

import (
	"encoding/json"
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
	return fmt.Sprintf("%s/%s", c.namespace, c.clusterName)
}

func (c *clusterID) stringWithPointAddr() string {
	return fmt.Sprintf("%s(%p)", c.String(), c)
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

func (m *Member) setClusterID(id *clusterID) {
	m.clusterID = id
}

type memberMarshal struct {
	Name    string
	Version string
}

func (m *Member) MarshalJSON() ([]byte, error) {
	return json.Marshal(&memberMarshal{
		Name:    m.Name(),
		Version: m.Version(),
	})
}

func (m *Member) UnmarshalJSON(b []byte) error {
	mMarshal := memberMarshal{}

	err := json.Unmarshal(b, &mMarshal)
	if err != nil {
		return err
	}
	m.id = getIdFromName(mMarshal.Name)
	m.version = mMarshal.Version

	return nil
}

type Members struct {
	members map[int]*Member
	*clusterID
}

func (ms *Members) GetElements() []*Member {
	result := make([]*Member, 0, len(ms.members))
	for _, m := range ms.members {
		result = append(result, m)
	}

	return result
}

func (ms *Members) Add(m *Member) {
	if m.clusterID != ms.clusterID {
		panic(fmt.Sprintf("Add a member of %s to members of %s",
			m.clusterID.stringWithPointAddr(), ms.clusterID.stringWithPointAddr()))
	}
	ms.members[m.ID()] = m
}

func (ms *Members) Remove(id int) {
	delete(ms.members, id)
}

func (ms *Members) Update(m *Members) {
	if m.clusterID != ms.clusterID {
		panic(fmt.Sprintf("Can't update members of %s with members of %s",
			ms.clusterID.stringWithPointAddr(), m.clusterID.stringWithPointAddr()))
	}

	for id, member := range m.members {
		ms.members[id] = member
	}
}

func (ms *Members) Size() int {
	return len(ms.members)
}

func (ms *Members) Copy() *Members {
	clone := Members{
		clusterID: ms.clusterID,
		members:   make(map[int]*Member, len(ms.members)),
	}
	clone.Update(ms)
	return &clone
}

// the set of all members of s1 that are not members of s2
func (ms *Members) Diff(other *Members) *Members {
	if other.clusterID != ms.clusterID {
		panic(fmt.Sprintf("Can't get diffs between members of %s and members of %s",
			ms.clusterID.stringWithPointAddr(), other.clusterID.stringWithPointAddr()))
	}

	diff := Members{
		clusterID: ms.clusterID,
		members:   make(map[int]*Member, 0),
	}
	for n, m := range ms.members {
		if _, ok := other.members[n]; !ok {
			diff.members[n] = m
		}
	}
	return &diff
}

func (ms *Members) GetMemberNames() (res []string) {
	for _, m := range ms.members {
		res = append(res, m.Name())
	}
	return res
}

func (ms *Members) GetClientHosts() []string {
	hosts := make([]string, 0, len(ms.members))
	for _, m := range ms.members {
		hosts = append(hosts, fmt.Sprintf("%s:2181", m.Addr()))
	}
	sort.Strings(hosts)
	return hosts
}

func (ms *Members) GetClusterConfig() []string {
	// TODO: make clusterconfig a struct
	clusterConfig := make([]string, 0, len(ms.members))
	for _, m := range ms.members {
		clusterConfig = append(clusterConfig, fmt.Sprintf("server.%d=%s:2888:3888:participant;0.0.0.0:2181", m.ID(), m.Addr()))
	}
	sort.Strings(clusterConfig)
	return clusterConfig
}

// Do the custom marshal/unmarshal thing to make sure that we have a readable status of zookeeper cluster
func (ms *Members) MarshalJSON() ([]byte, error) {
	keys := make([]int, 0, len(ms.members))
	for k := range ms.members {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	members := make([]*Member, 0, len(ms.members))
	for _, k := range keys {
		members = append(members, ms.members[k])
	}
	return json.Marshal(&members)
}

func (ms *Members) UnmarshalJSON(b []byte) error {
	members := []*Member{}
	err := json.Unmarshal(b, &members)
	if err != nil {
		return err
	}
	ms.members = make(map[int]*Member, len(members))
	for _, member := range members {
		ms.members[member.id] = member
	}
	return nil
}

func (ms *Members) setClusterID(id *clusterID) {
	ms.clusterID = id
	for _, m := range ms.members {
		m.setClusterID(id)
	}
}

type ZKCluster struct {
	*clusterID `json:omit`
	version    string  `json:"omit"`
	Running    Members `json:"running"`
	Unready    Members `json:"unready"`
	Stopped    Members `json:"stopped"`
}

func NewZKCluster(namespace, cluster_name, version string, runningPods, unreadyPods, stoppedPods []*v1.Pod) *ZKCluster {
	id := &clusterID{
		clusterName: cluster_name,
		namespace:   namespace,
	}
	return &ZKCluster{
		clusterID: id,
		version:   version,
		Running:   podsToMembers(id, runningPods),
		Unready:   podsToMembers(id, unreadyPods),
		Stopped:   podsToMembers(id, stoppedPods),
	}
}

func podsToMembers(id *clusterID, pods []*v1.Pod) Members {
	members := Members{
		clusterID: id,
		members:   make(map[int]*Member, len(pods)),
	}
	for _, pod := range pods {
		members.Add(podToMember(id, pod))
	}
	return members
}

func getIdFromName(name string) int {
	i := strings.LastIndex(name, "-")
	if i == -1 {
		panic(fmt.Sprintf("unexpected member name: %s", name))
	}

	id, err := strconv.Atoi(name[i+1:])

	if err != nil {
		panic(fmt.Sprintf("unexpected member name: %s", name))
	}

	return id
}

func podToMember(clusterID *clusterID, pod *v1.Pod) *Member {
	return &Member{
		id:        getIdFromName(pod.Name),
		clusterID: clusterID,
		version:   getZookeeperVersion(pod),
	}
}

// TODO: Figure out another way to do this
func getZookeeperVersion(pod *v1.Pod) string {
	return pod.Annotations["zookeeper.version"]
}

func (z *ZKCluster) RemoveMembers(size int) *Members {
	// In the future, we may support removing members while there are some stopped/unready members.
	if size > z.Running.Size() {
		panic(fmt.Sprintf(
			"Receive a request of deleting %d member(s), but ZkCluster %s has only %d member(s) running.",
			size, z.clusterID, z.Running.Size()))
	}

	result := Members{
		clusterID: z.clusterID,
		members:   make(map[int]*Member, size),
	}
	for i := 0; i < size; i++ {
		result.Add(z.removeOneMember())
	}
	return &result
}

func (z *ZKCluster) removeOneMember() *Member {
	var last *Member = nil
	for _, m := range z.Running.members {
		if last == nil {
			last = m
		} else {
			if last.ID() < m.ID() {
				last = m
			}
		}
	}
	z.Running.Remove(last.ID())
	return last
}

func (z *ZKCluster) AddMembers(size int) *Members {
	result := Members{
		clusterID: z.clusterID,
		members:   make(map[int]*Member, size),
	}
	for i := 0; i < size; i++ {
		result.Add(z.addOneMember())
	}
	return &result
}

func (z *ZKCluster) addOneMember() *Member {
	m := Member{
		id:        z.nextMemberID(),
		clusterID: z.clusterID,
		version:   z.version,
	}
	z.Unready.Add(&m)
	return &m
}

func (z *ZKCluster) nextMemberID() int {
	// In the future, we may support removing members while there are some stopped/unready members.
	missedID := 0
	ids := map[int]struct{}{}
	for _, m := range z.Running.members {
		ids[m.ID()] = struct{}{}
	}
	for _, m := range z.Unready.members {
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

func (z *ZKCluster) setClusterID(id *clusterID) {
	z.clusterID = id
	z.Running.setClusterID(id)
	z.Unready.setClusterID(id)
	z.Stopped.setClusterID(id)
}
