package zkcluster

import (
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"zookeeper-operator/internal/util/k8sutil"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

const (
	// zookeeperClientPort is the zkclient port on zkclient service and zookeeper nodes.
	zookeeperClientPort = 2181

	zookeeperDataVolumeMountDir = "/data"
	zookeeperTlogVolumeMountDir = "/datalog"

	zookeeperDataVolumeName = "zookeeper-data"
	zookeeperTlogVolumeName = "zookeeper-tlog"

	defaultBusyboxImage = "busybox:1.28.0-glibc"
)

// TODO: use configmap to write clusterMembers into a configuration file
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
	}, v1.EnvVar{
		Name:  "ZOO_4LW_COMMANDS_WHITELIST",
		Value: "*",
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
				Image: "busybox:1.28.0-glibc",
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

	k8sutil.SetZookeeperVersion(pod, zkCR.Spec.Version)
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
				Command: []string{"/bin/bash", "-c", cmd},
			},
		},
		InitialDelaySeconds: 100,
		TimeoutSeconds:      100,
		PeriodSeconds:       600,
		FailureThreshold:    3,
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

func zookeeperVolumeMounts() []v1.VolumeMount {
	return []v1.VolumeMount{
		{Name: zookeeperDataVolumeName, MountPath: zookeeperDataVolumeMountDir},
		{Name: zookeeperTlogVolumeName, MountPath: zookeeperTlogVolumeMountDir},
	}
}

func zookeeperContainer(repo, version string) v1.Container {
	c := v1.Container{
		Name:  "zookeeper",
		Image: k8sutil.ImageName(repo, version),
		Ports: []v1.ContainerPort{
			{
				Name:          "zkclient",
				ContainerPort: int32(zookeeperClientPort),
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
