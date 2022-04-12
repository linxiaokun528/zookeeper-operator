package zkcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

func (c *Cluster) create() (err error) {
	now := metav1.Now()
	c.zkCR.Status.StartTime = &now
	c.zkCR.Status.SetVersion(c.zkCR.Spec.Version)
	c.zkCR.Status.AppendCreatingCondition()

	c.logClusterCreation()
	defer func() {
		if err == nil {
			klog.Infof("Zookeeper cluster %v created successfully", c.zkCR.GetNamespacedName())
		}
	}()

	clusterService := clusterService{
		zkCR:   c.zkCR,
		client: c.client,
		ctx:    c.ctx,
	}
	if err = clusterService.setupServices(); err != nil {
		return fmt.Errorf("zkCR create: failed to setup service: %v", err)
	}

	return nil
}

func (c *Cluster) logClusterCreation() {
	specBytes, err := json.MarshalIndent(c.zkCR.Spec, "", "    ")
	if err != nil {
		klog.Errorf("Failed to marshal spec of zookeeper cluster %s: %v", c.zkCR.Name, err)
		return
	}

	klog.Infof("Creating zookeeper cluster %s with Spec: ", c.zkCR.Name)
	for _, m := range strings.Split(string(specBytes), "\n") {
		klog.Info(m)
	}
}

type clusterService struct {
	zkCR   *api.ZookeeperCluster
	client client.Client
	ctx    context.Context
}

func (c *clusterService) setupServices() error {
	client_svc := c.newClientService(c.zkCR.Namespace, c.zkCR.Name)
	err := c.createService(client_svc)
	if err != nil {
		return err
	}

	peer_svc := c.newPeerService(c.zkCR.Namespace, c.zkCR.Name)

	return c.createService(peer_svc)
}

func (c *clusterService) createService(service *v1.Service) error {
	service.OwnerReferences = append(service.OwnerReferences, c.zkCR.AsOwner())

	err := c.client.Create(c.ctx, service)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (c *clusterService) newClientService(namespace, clusterName string) *v1.Service {
	ports := []v1.ServicePort{{
		Name:       "zkclient",
		Port:       zookeeperClientPort,
		TargetPort: intstr.FromInt(zookeeperClientPort),
		Protocol:   v1.ProtocolTCP,
	}}
	return c.newService(namespace, clusterName+"-zkclient", clusterName, "", ports)
}

func (c *clusterService) newPeerService(namespace, clusterName string) *v1.Service {
	ports := []v1.ServicePort{{
		Name:       "zkclient",
		Port:       zookeeperClientPort,
		TargetPort: intstr.FromInt(zookeeperClientPort),
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

	return c.newService(namespace, clusterName, clusterName, v1.ClusterIPNone, ports)
}

func (c *clusterService) newService(namespace, svcName, clusterName, clusterIP string, ports []v1.ServicePort) *v1.Service {
	labels := labelsForCluster(clusterName)
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      svcName,
			Labels:    labels,
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
