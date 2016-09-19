package com.abin.lee.helix.service;

import com.abin.lee.helix.common.JsonUtil;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: abin
 * Date: 16-9-15
 * Time: 下午11:47
 * To change this template use File | Settings | File Templates.
 */
public class DistributeSystemManage {
    private static final String ZK_ADDRESS = "172.16.2.133:2199";

    public static void main(String[] args) {
//        createCluster();
        getCluster();
        createConfigNode();
        instanceList();
    }

    public static void createCluster(){
        // Create setup tool instance
        ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);

        String CLUSTER_NAME = "helix-demo";
        //Create cluster namespace in zookeeper
        admin.addCluster(CLUSTER_NAME);
    }

    public static void getCluster(){
        // Create setup tool instance
        // Note: ZK_ADDRESS is the host:port of Zookeeper

        ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(ZK_ADDRESS);
        List<String> clusterList = zkHelixAdmin.getClusters();
        System.out.println("clusterList=" + JsonUtil.toJson(clusterList));
    }

    public static void createConfigNode(){
        ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);
        String CLUSTER_NAME = "helix-demo";
        int NUM_NODES = 2;
        String hosts[] = new String[]{"localhost","localhost"};
        String ports[] = new String[]{"7000","7001"};
        for (int i = 0; i < NUM_NODES; i++)
        {
            InstanceConfig instanceConfig = new InstanceConfig(hosts[i]+ "_" + ports[i]);
            instanceConfig.setHostName(hosts[i]);
            instanceConfig.setPort(ports[i]);
            instanceConfig.setInstanceEnabled(true);

            //Add additional system specific configuration if needed. These can be accessed during the node start up.
            instanceConfig.getRecord().setSimpleField("key", "value");
            admin.addInstance(CLUSTER_NAME, instanceConfig);
        }

    }

    public static void instanceList(){
        String CLUSTER_NAME = "helix-demo";
        ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(ZK_ADDRESS);
        List<String> instanceList = zkHelixAdmin.getInstancesInCluster(CLUSTER_NAME);
        System.out.println("instanceList=" + JsonUtil.toJson(instanceList));
    }

}
