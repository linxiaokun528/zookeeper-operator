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
	"k8s.io/klog"
	"sort"
	"strings"
	"time"
	/* TODO: The error message of this ZK zkclient is too simple. Maybe change a zk zkclient in the future.
	 */
	"github.com/samuel/go-zookeeper/zk"
)

func GetClusterConfig(hosts []string) ([]string, error) {
	conn, _, err := zk.Connect(hosts, time.Second)
	defer conn.Close()
	if err != nil {
		klog.Error("Failed to connect to ZK hosts: ", hosts)
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
		klog.Error("Failed to connect to ZK hosts: ", hosts)
		return err
	}
	defer conn.Close()
	klog.Info("Pushing reconfig to zookeeper", desiredConfig)
	_, err = conn.Reconfig(desiredConfig, -1)

	if err != nil {
		// TODO: use the same logger in all the operator
		klog.Error("Failed to push reconfig", hosts, desiredConfig)
		return err
	}

	return nil

	//return []string{""},nil
}
