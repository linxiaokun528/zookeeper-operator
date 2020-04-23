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

package zookeeper

import (
	"sort"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/samuel/go-zookeeper/zk"
)

func GetClusterConfig(hosts []string) ([]string, error) {
	conn, _, err := zk.Connect(hosts, time.Second)
	defer conn.Close()
	if err != nil {
		klog.Errorf("Failed to connect to ZK hosts: ", hosts)
		return nil, err
	}

	data, _, err := conn.Get("/zookeeper/config")
	if err != nil {
		return nil, err
	}

	// data is a []byte, we must convert it to a string
	dataStr := string(data)
	// the config data has servers first, last line is the version
	configDataArr := strings.Split(dataStr, "\n")
	clusterConfig := configDataArr[:len(configDataArr)-1]
	sort.Strings(clusterConfig)

	return clusterConfig, nil
}

func ReconfigureCluster(hosts []string, desiredConfig []string) error {
	conn, _, err := zk.Connect(hosts, time.Second)
	if err != nil {
		klog.Errorf("Failed to connect to zookeeper hosts: %v", hosts)
		return err
	}
	defer conn.Close()
	klog.V(4).Infof("Pushing reconfig %s to zookeeper hosts: %s", desiredConfig, hosts)
	_, err = conn.Reconfig(desiredConfig, -1)

	if err != nil {
		klog.Errorf("Failed to push reconfig %s to zookeeper hosts: %s", desiredConfig, hosts)
		return err
	}

	return nil
}
