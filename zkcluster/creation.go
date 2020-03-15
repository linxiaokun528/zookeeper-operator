package zkcluster

import (
	"encoding/json"
	"fmt"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog"
	"strings"
)

func (c *Cluster) setupServices() error {
	client_svc := newClientService(c.zkCR.Name)
	err := c.createService(client_svc)
	if err != nil {
		return err
	}

	peer_svc := newPeerService(c.zkCR.Name)

	return c.createService(peer_svc)
}

func (c *Cluster) createService(service *v1.Service) error {
	service.OwnerReferences = append(service.OwnerReferences, c.zkCR.AsOwner())

	_, err := c.client.Service().Create(service)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (c *Cluster) create() error {
	if err := c.setupServices(); err != nil {
		return fmt.Errorf("zkCR create: failed to setup service: %v", err)
	}
	c.logClusterCreation()

	return nil
}

func (c *Cluster) logClusterCreation() {
	specBytes, err := json.MarshalIndent(c.zkCR.Spec, "", "    ")
	if err != nil {
		klog.Errorf("failed to marshal spec of zookeeper cluster %s: %v", c.zkCR.Name, err)
	}

	klog.Info("creating zookeeper cluster %s with Spec:", c.zkCR.Name)
	for _, m := range strings.Split(string(specBytes), "\n") {
		klog.Info(m)
	}
}

// TODO: write this service into a configurefile
func newClientService(clusterName string) *v1.Service {
	ports := []v1.ServicePort{{
		Name:       "client",
		Port:       ZookeeperClientPort,
		TargetPort: intstr.FromInt(ZookeeperClientPort),
		Protocol:   v1.ProtocolTCP,
	}}
	return newService(clusterName+"-client", clusterName, "", ports)
}

func newPeerService(clusterName string) *v1.Service {
	ports := []v1.ServicePort{{
		Name:       "client",
		Port:       ZookeeperClientPort,
		TargetPort: intstr.FromInt(ZookeeperClientPort),
		Protocol:   v1.ProtocolTCP,
	}, {
		Name:       "peer",
		Port:       2888,
		TargetPort: intstr.FromInt(2888),
		Protocol:   v1.ProtocolTCP,
	}, {
		Name:       "leader",
		Port:       3888,
		TargetPort: intstr.FromInt(3888),
		Protocol:   v1.ProtocolTCP,
	}}

	return newService(clusterName, clusterName, v1.ClusterIPNone, ports)
}

func newService(svcName, clusterName, clusterIP string, ports []v1.ServicePort) *v1.Service {
	labels := labelsForCluster(clusterName)
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   svcName,
			Labels: labels,
		},
		Spec: v1.ServiceSpec{
			Ports:                    ports,
			Selector:                 labels,
			ClusterIP:                clusterIP,
			PublishNotReadyAddresses: true,
		},
	}

	return svc
}
