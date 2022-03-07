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

package client

import (
	"context"

	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"zookeeper-operator/pkg/client/clientset/versioned"
	"zookeeper-operator/pkg/client/clientset/versioned/typed/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/k8sutil"
)

// Maybe use sigs.k8s.io/controller-runtime/pkg/manager instead
type Client interface {
	GetConfig() *rest.Config

	GetCRClient(namespace string) CRClient
	GetCRDClient() k8sutil.CRDClient

	KubernetesInterface() kubernetes.Interface
}

func NewClientOrDie(ctx context.Context, masterURL string, kubeconfigPath string) Client {
	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
	if err != nil {
		panic(err)
	}
	return &clientsets{
		config:  cfg,
		kubeCli: kubernetes.NewForConfigOrDie(cfg),
		crdCli:  k8sutil.NewCRDClientOrDie(ctx, cfg),
		zkCli:   versioned.NewForConfigOrDie(cfg),
	}
}

type clientsets struct {
	config  *rest.Config
	kubeCli kubernetes.Interface
	crdCli  k8sutil.CRDClient
	zkCli   versioned.Interface
}

func (c *clientsets) GetConfig() *rest.Config {
	return c.config
}

func (c *clientsets) GetCRClient(namespace string) CRClient {
	return &client{
		pod:              c.kubeCli.CoreV1().Pods(namespace),
		zookeeperCluster: c.zkCli.ZookeeperV1alpha1().ZookeeperClusters(namespace),
		event:            c.kubeCli.CoreV1().Events(namespace),
		service:          c.kubeCli.CoreV1().Services(namespace),
	}
}

func (c *clientsets) GetCRDClient() k8sutil.CRDClient {
	return c.crdCli
}

func (c *clientsets) KubernetesInterface() kubernetes.Interface {
	return c.kubeCli
}

type CRClient interface {
	Pod() corev1.PodInterface
	ZookeeperCluster() v1alpha1.ZookeeperClusterInterface
	Event() corev1.EventInterface
	Service() corev1.ServiceInterface
}

type client struct {
	pod              corev1.PodInterface
	zookeeperCluster v1alpha1.ZookeeperClusterInterface
	event            corev1.EventInterface
	service          corev1.ServiceInterface
}

func (c *client) Pod() corev1.PodInterface {
	return c.pod
}

func (c *client) ZookeeperCluster() v1alpha1.ZookeeperClusterInterface {
	return c.zookeeperCluster
}

func (c *client) Event() corev1.EventInterface {
	return c.event
}

func (c *client) Service() corev1.ServiceInterface {
	return c.service
}
