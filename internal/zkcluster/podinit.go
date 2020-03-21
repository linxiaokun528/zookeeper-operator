package zkcluster

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"strings"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	k8sutil2 "zookeeper-operator/pkg/k8sutil"
)

const (
	// ZookeeperClientPort is the zkclient port on zkclient service and zookeeper nodes.
	ZookeeperClientPort = 2181

	zookeeperDataVolumeMountDir   = "/data"
	zookeeperTlogVolumeMountDir   = "/datalog"
	zookeeperVersionAnnotationKey = "zookeeper.version"

	zookeeperDataVolumeName = "zookeeper-data"
	zookeeperTlogVolumeName = "zookeeper-tlog"

	defaultBusyboxImage = "busybox:1.28.0-glibc"
)

// TODO: write this configuration into a configuration file
func newZookeeperPod(m *api.Member, clusterMembers *api.Members, zkCR *api.ZookeeperCluster) *v1.Pod {
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

	container := zookeeperContainer(zkCR.Spec.Repository, zkCR.Spec.Version)
	container.LivenessProbe = livenessProbe
	container.ReadinessProbe = readinessProbe

	container.Env = append(container.Env, v1.EnvVar{
		Name:  "ZOO_MY_ID",
		Value: strconv.Itoa(m.ID()),
	}, v1.EnvVar{
		Name:  "ZOO_SERVERS",
		Value: strings.Join(clusterMembers.GetClusterConfig(), " "),
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
				Image: imageNameBusybox(zkCR.Spec.Pod),
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
	setZookeeperVersion(pod, zkCR.Spec.Version)
	applyPodPolicy(m.ClusterName(), pod, zkCR.Spec.Pod)
	pod.OwnerReferences = append(pod.OwnerReferences, zkCR.AsOwner())
	return pod
}

func newZookeeperProbe() *v1.Probe {
	cmd := `OK=$(echo ruok | nc localhost 2181)
if [ "$OK" == "imok" ]; then
        exit 0
else
        exit 1
fi`
	return &v1.Probe{
		Handler: v1.Handler{
			Exec: &v1.ExecAction{
				Command: []string{"/bin/sh", "-c", cmd},
			},
		},
		InitialDelaySeconds: 100,
		TimeoutSeconds:      100,
		PeriodSeconds:       600,
		FailureThreshold:    3,
	}
}

func applyPodPolicy(clusterName string, pod *v1.Pod, policy *api.PodPolicy) {
	if policy == nil {
		return
	}

	if policy.Affinity != nil {
		pod.Spec.Affinity = policy.Affinity
	}

	if len(policy.NodeSelector) != 0 {
		pod.Spec.NodeSelector = policy.NodeSelector
	}
	if len(policy.Tolerations) != 0 {
		pod.Spec.Tolerations = policy.Tolerations
	}

	// TODO: write a map util or import a map util
	k8sutil2.MergeLabels(pod.Labels, policy.Labels)

	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Resources = policy.Resources
		if pod.Spec.Containers[i].Name == "zookeeper" {
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, policy.ZookeeperEnv...)
		}
	}

	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].Resources = policy.Resources
	}

	for key, value := range policy.Annotations {
		pod.ObjectMeta.Annotations[key] = value
	}
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

// imageNameBusybox returns the default image for busybox init container, or the image specified in the PodPolicy
func imageNameBusybox(policy *api.PodPolicy) string {
	if policy != nil && len(policy.BusyboxImage) > 0 {
		return policy.BusyboxImage
	}
	return defaultBusyboxImage
}

func zookeeperVolumeMounts() []v1.VolumeMount {
	return []v1.VolumeMount{
		{Name: zookeeperDataVolumeName, MountPath: zookeeperDataVolumeMountDir},
		{Name: zookeeperTlogVolumeName, MountPath: zookeeperTlogVolumeMountDir},
	}
}

func zookeeperContainer(repo, version string) v1.Container {
	c := v1.Container{
		Name:  "zookeeper",
		Image: k8sutil2.ImageName(repo, version),
		Ports: []v1.ContainerPort{
			{
				Name:          "zkclient",
				ContainerPort: int32(ZookeeperClientPort),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          "peer",
				ContainerPort: int32(2888),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          "server",
				ContainerPort: int32(3888),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          "jolokia",
				ContainerPort: int32(8778),
				Protocol:      v1.ProtocolTCP,
			},
		},
		VolumeMounts: zookeeperVolumeMounts(),
	}

	return c
}

func getZookeeperVersion(pod *v1.Pod) string {
	return pod.Annotations[zookeeperVersionAnnotationKey]
}

func setZookeeperVersion(pod *v1.Pod, version string) {
	pod.Annotations[zookeeperVersionAnnotationKey] = version
}
