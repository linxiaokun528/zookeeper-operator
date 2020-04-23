package k8s

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
)

func GetZookeeperVersion(pod *v1.Pod) string {
	imageName := pod.Spec.Containers[0].Image
	parts := strings.Split(imageName, ":")
	if len(parts) != 2 {
		panic(fmt.Errorf("Invalid image name: %v", imageName))
	}

	return parts[1]
}

func GetRepository(pod *v1.Pod) string {
	imageName := pod.Spec.Containers[0].Image
	parts := strings.Split(imageName, ":")
	if len(parts) != 2 {
		panic(fmt.Errorf("Invalid image name: %v", imageName))
	}

	return parts[0]
}

func SetZookeeperVersion(pod *v1.Pod, version string) {
	pod.Spec.Containers[0].Image = ImageName(GetRepository(pod), version)
}

func ImageName(repo, version string) string {
	return fmt.Sprintf("%s:%v", repo, version)
}
