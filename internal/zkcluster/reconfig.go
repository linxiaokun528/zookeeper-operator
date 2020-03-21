package zkcluster

import (
	"k8s.io/klog"
	"zookeeper-operator/internal/util/zookeeperutil"
)

//func (c *Cluster) needReconfig() (bool, error) {
//	if c.zkCR.Status.Members.Running.Size() == 0 {
//		return false, nil
//	}
//	actualConfig, err := zookeeperutil.GetClusterConfig(c.zkCR.Status.Members.Running.GetClientHosts())
//	if err != nil {
//		c.logger.Info("Failed to get configure from zookeeper: %v",
//			c.zkCR.Status.Members.Running.GetClientHosts())
//		return false, err
//	}
//	expectedConfig := c.zkCR.Status.Members.Running.GetClusterConfig()
//
//	sort.Strings(actualConfig)
//	sort.Strings(expectedConfig)
//
//	return !reflect.DeepEqual(expectedConfig, actualConfig), nil
//}

func (c *Cluster) reconfig(hosts []string, desiredConfig []string) error {
	klog.Infoln("Reconfiguring zookeeper cluster", c.zkCR.Name)

	return zookeeperutil.ReconfigureCluster(hosts, desiredConfig)
}
