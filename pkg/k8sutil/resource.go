package k8sutil

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func GetNamespacedName(obj client.Object) string {
	return client.ObjectKeyFromObject(obj).String()
}
