package k8sutil

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
)

func GetZookeeperVersion(pod *v1.Pod) string {
	imageName := pod.Spec.Containers[0].Image
	parts := strings.Split(imageName, ":v")
	if len(parts) != 2 {
		panic(fmt.Errorf("Invalid image name: %v", imageName))
	}

	return parts[1]
}
