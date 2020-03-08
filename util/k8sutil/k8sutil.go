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

package k8sutil

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1beta1 "k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // for gcp auth
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
)

const (
	// ZookeeperClientPort is the client port on client service and zookeeper nodes.
	ZookeeperClientPort = 2181

	zookeeperDataVolumeMountDir   = "/data"
	zookeeperTlogVolumeMountDir   = "/datalog"
	zookeeperVersionAnnotationKey = "zookeeper.version"

	randomSuffixLength = 10
	// k8s object name has a maximum length
	maxNameLength = 63 - randomSuffixLength - 1

	defaultBusyboxImage = "busybox:1.28.0-glibc"

	defaultKubeAPIRequestTimeout = 5 * time.Minute

	// AnnotationScope annotation name for defining instance scope. Used for specifing cluster wide clusters.
	AnnotationScope = "zookeeper.database.apache.com/scope"
	//AnnotationClusterWide annotation value for cluster wide clusters.
	AnnotationClusterWide = "clusterwide"
)

func GetZookeeperVersion(pod *v1.Pod) string {
	return pod.Annotations[zookeeperVersionAnnotationKey]
}

func SetZookeeperVersion(pod *v1.Pod, version string) {
	pod.Annotations[zookeeperVersionAnnotationKey] = version
}

// PVCNameFromMember the way we get PVC name from the member name
func PVCNameFromMember(memberName string) string {
	return memberName
}

func ImageName(repo, version string) string {
	return fmt.Sprintf("%s:v%v", repo, version)
}

// imageNameBusybox returns the default image for busybox init container, or the image specified in the PodPolicy
func imageNameBusybox(policy *api.PodPolicy) string {
	if policy != nil && len(policy.BusyboxImage) > 0 {
		return policy.BusyboxImage
	}
	return defaultBusyboxImage
}

func PodWithNodeSelector(p *v1.Pod, ns map[string]string) *v1.Pod {
	p.Spec.NodeSelector = ns
	return p
}

func NewClientService(clusterName string) *v1.Service {
	ports := []v1.ServicePort{{
		Name:       "client",
		Port:       ZookeeperClientPort,
		TargetPort: intstr.FromInt(ZookeeperClientPort),
		Protocol:   v1.ProtocolTCP,
	}}
	return newService(clusterName+"-client", clusterName, "", ports)
}

func NewPeerService(clusterName string) *v1.Service {
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
	labels := LabelsForCluster(clusterName)
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

// AddZookeeperVolumeToPod abstract the process of appending volume spec to pod spec
func AddZookeeperVolumeToPod(pod *v1.Pod, pvc *v1.PersistentVolumeClaim) {
	vol := v1.Volume{Name: zookeeperDataVolumeName}
	if pvc != nil {
		vol.VolumeSource = v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
		}
	} else {
		vol.VolumeSource = v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, vol)
}

func AddOwnerRefToObject(o metav1.Object, r metav1.OwnerReference) {
	o.SetOwnerReferences(append(o.GetOwnerReferences(), r))
}

func NewZookeeperPod(m *api.Member, cluster *api.Members, cs api.ClusterSpec, owner metav1.OwnerReference) *v1.Pod {
	labels := map[string]string{
		"app":               "zookeeper",
		"zookeeper_node":    m.Name(),
		"zookeeper_cluster": m.ClusterName(),
	}

	livenessProbe := newZookeeperProbe()
	readinessProbe := newZookeeperProbe()
	readinessProbe.InitialDelaySeconds = 1
	readinessProbe.TimeoutSeconds = 5
	readinessProbe.PeriodSeconds = 5
	readinessProbe.FailureThreshold = 3

	container := containerWithProbes(
		zookeeperContainer(cs.Repository, cs.Version),
		livenessProbe,
		readinessProbe)

	container.Env = append(container.Env, v1.EnvVar{
		Name:  "ZOO_MY_ID",
		Value: strconv.Itoa(m.ID()),
	}, v1.EnvVar{
		Name:  "ZOO_SERVERS",
		Value: strings.Join(cluster.GetClusterConfig(), " "),
	}, v1.EnvVar{
		Name:  "ZOO_MAX_CLIENT_CNXNS",
		Value: "0", // default 60
	})
	// Other available config items:
	// - ZOO_TICK_TIME: 2000
	// - ZOO_INIT_LIMIT: 5
	// - ZOO_SYNC_LIMIT: 2
	// - ZOO_STANDALONE_ENABLED: false (don't change this or you'll have a bad time)
	// - ZOO_RECONFIG_ENABLED: true (don't change this or you'll have a bad time)
	// - ZOO_SKIP_ACL: true
	// - ZOO_4LW_WHITELIST: ruok (probes will fail if ruok is removed)

	volumes := []v1.Volume{
		{Name: "zookeeper-data", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
		{Name: "zookeeper-tlog", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
	}

	runAsNonRoot := true
	podUID := int64(1000)
	fsGroup := podUID
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        m.Name(),
			Labels:      labels,
			Annotations: map[string]string{},
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{{
				// busybox:latest uses uclibc which contains a bug that sometimes prevents name resolution
				// More info: https://github.com/docker-library/busybox/issues/27
				//Image default: "busybox:1.28.0-glibc",
				// TODO: use the same image as the main container
				Image: imageNameBusybox(cs.Pod),
				Name:  "check-dns",
				// We bind to [hostname].[clustername].[namespace].svc which may take some time to appear in kubedns
				Command: []string{"/bin/sh", "-c", fmt.Sprintf(
					`while ( ! nslookup %s )
do
sleep 2
done`, m.Addr())},
			}},
			Containers:    []v1.Container{container},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes:       volumes,
			// DNS A record: `[m.Name].[clusterName].Namespace.svc`
			// For example, zookeeper-795649v9kq in default namespace will have DNS name
			// `zookeeper-795649v9kq.zookeeper.default.svc`.
			Hostname:                     m.Name(),
			Subdomain:                    m.ClusterName(),
			AutomountServiceAccountToken: func(b bool) *bool { return &b }(false),
			SecurityContext: &v1.PodSecurityContext{
				RunAsUser:    &podUID,
				RunAsNonRoot: &runAsNonRoot,
				FSGroup:      &fsGroup,
			},
		},
	}
	// TODO: make this function "SetAnnotations"
	SetZookeeperVersion(pod, cs.Version)
	applyPodPolicy(m.ClusterName(), pod, cs.Pod)
	AddOwnerRefToObject(pod.GetObjectMeta(), owner)
	return pod
}

func IsKubernetesResourceAlreadyExistError(err error) bool {
	return apierrors.IsAlreadyExists(err)
}

func IsKubernetesResourceNotFoundError(err error) bool {
	return apierrors.IsNotFound(err)
}

// We are using internal api types for cluster related.
func ClusterListOpt(clusterName string) metav1.ListOptions {
	return metav1.ListOptions{
		LabelSelector: ClusterSelector(clusterName).String(),
	}
}

func ClusterSelector(clusterName string) labels.Selector {
	return labels.SelectorFromSet(LabelsForCluster(clusterName))
}

func LabelsForCluster(clusterName string) map[string]string {
	return map[string]string{
		"zookeeper_cluster": clusterName,
		"app":               "zookeeper",
	}
}

func CreatePatch(o, n, datastruct interface{}) ([]byte, error) {
	oldData, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	newData, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}
	return strategicpatch.CreateTwoWayMergePatch(oldData, newData, datastruct)
}

func PatchDeployment(kubecli kubernetes.Interface, namespace, name string, updateFunc func(*appsv1beta1.Deployment)) error {
	od, err := kubecli.AppsV1beta1().Deployments(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	nd := od.DeepCopy()
	updateFunc(nd)
	patchData, err := CreatePatch(od, nd, appsv1beta1.Deployment{})
	if err != nil {
		return err
	}
	_, err = kubecli.AppsV1beta1().Deployments(namespace).Patch(name, types.StrategicMergePatchType, patchData)
	return err
}

func CascadeDeleteOptions(gracePeriodSeconds int64) *metav1.DeleteOptions {
	return &metav1.DeleteOptions{
		GracePeriodSeconds: func(t int64) *int64 { return &t }(gracePeriodSeconds),
		PropagationPolicy: func() *metav1.DeletionPropagation {
			foreground := metav1.DeletePropagationForeground
			return &foreground
		}(),
	}
}

// mergeLabels merges l2 into l1. Conflicting label will be skipped.
func mergeLabels(l1, l2 map[string]string) {
	for k, v := range l2 {
		if _, ok := l1[k]; ok {
			continue
		}
		l1[k] = v
	}
}

func UniqueMemberName(clusterName string) string {
	suffix := utilrand.String(randomSuffixLength)
	if len(clusterName) > maxNameLength {
		clusterName = clusterName[:maxNameLength]
	}
	return clusterName + "-" + suffix
}
