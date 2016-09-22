package com.abin.lee.helix.service;

import com.abin.lee.helix.common.JsonUtil;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abin
 * Date: 16-9-15
 * Time: 下午11:47
 * To change this template use File | Settings | File Templates.
 */
public class DistributeSystemManage {
    //    private static final String ZK_ADDRESS = "172.16.2.133:2199";
    private static final String ZK_ADDRESS = "172.16.2.134:2199";
    private static final String CLUSTER_NAME = "helix-demo";

    public static void main(String[] args) {
//        createCluster();
        getCluster();
//        createConfigNode();
        instanceList();
        stateExchangeCreate();
        stateList();
    }

    public static void createCluster() {
        // Create setup tool instance
        ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);

        //Create cluster namespace in zookeeper
        admin.addCluster(CLUSTER_NAME);
    }

    public static void getCluster() {
        // Create setup tool instance
        // Note: ZK_ADDRESS is the host:port of Zookeeper

        ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(ZK_ADDRESS);
        List<String> clusterList = zkHelixAdmin.getClusters();
        System.out.println("clusterList=" + JsonUtil.toJson(clusterList));
    }

    public static void createConfigNode() {
        ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);
        int NUM_NODES = 2;
        String hosts[] = new String[]{"localhost", "localhost"};
        String ports[] = new String[]{"7000", "7001"};
        for (int i = 0; i < NUM_NODES; i++) {
            InstanceConfig instanceConfig = new InstanceConfig(hosts[i] + "_" + ports[i]);
            instanceConfig.setHostName(hosts[i]);
            instanceConfig.setPort(ports[i]);
            instanceConfig.setInstanceEnabled(true);

            //Add additional system specific configuration if needed. These can be accessed during the node start up.
            instanceConfig.getRecord().setSimpleField("key", "value");
            admin.addInstance(CLUSTER_NAME, instanceConfig);
        }

    }

    public static void instanceList() {
        ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(ZK_ADDRESS);
        List<String> instanceList = zkHelixAdmin.getInstancesInCluster(CLUSTER_NAME);
        System.out.println("instanceList=" + JsonUtil.toJson(instanceList));
    }


    public static void stateExchangeCreate() {
        ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);
        String STATE_MODEL_NAME = "MasterSlave";
        admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME,  new StateModelDefinition(generateConfigForMasterSlave()));

    }


    public static ZNRecord generateConfigForMasterSlave() {
        ZNRecord record = new ZNRecord("MasterSlave");
        record.setSimpleField(StateModelDefinition.StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
        List<String> statePriorityList = new ArrayList<String>();
        statePriorityList.add("MASTER");
        statePriorityList.add("SLAVE");
        statePriorityList.add("OFFLINE");
        statePriorityList.add("DROPPED");
        statePriorityList.add("ERROR");
        record.setListField(StateModelDefinition.StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
                statePriorityList);
        for (String state : statePriorityList) {
            String key = state + ".meta";
            Map<String, String> metadata = new HashMap<String, String>();
            if (state.equals("MASTER")) {
                metadata.put("count", "1");
                record.setMapField(key, metadata);
            } else if (state.equals("SLAVE")) {
                metadata.put("count", "R");
                record.setMapField(key, metadata);
            } else if (state.equals("OFFLINE")) {
                metadata.put("count", "-1");
                record.setMapField(key, metadata);
            } else if (state.equals("DROPPED")) {
                metadata.put("count", "-1");
                record.setMapField(key, metadata);
            } else if (state.equals("ERROR")) {
                metadata.put("count", "-1");
                record.setMapField(key, metadata);
            }
        }
        for (String state : statePriorityList) {
            String key = state + ".next";
            if (state.equals("MASTER")) {
                Map<String, String> metadata = new HashMap<String, String>();
                metadata.put("SLAVE", "SLAVE");
                metadata.put("OFFLINE", "SLAVE");
                metadata.put("DROPPED", "SLAVE");
                record.setMapField(key, metadata);
            } else if (state.equals("SLAVE")) {
                Map<String, String> metadata = new HashMap<String, String>();
                metadata.put("MASTER", "MASTER");
                metadata.put("OFFLINE", "OFFLINE");
                metadata.put("DROPPED", "OFFLINE");
                record.setMapField(key, metadata);
            } else if (state.equals("OFFLINE")) {
                Map<String, String> metadata = new HashMap<String, String>();
                metadata.put("SLAVE", "SLAVE");
                metadata.put("MASTER", "SLAVE");
                metadata.put("DROPPED", "DROPPED");
                record.setMapField(key, metadata);
            } else if (state.equals("ERROR")) {
                Map<String, String> metadata = new HashMap<String, String>();
                metadata.put("OFFLINE", "OFFLINE");
                record.setMapField(key, metadata);
            }
        }
        List<String> stateTransitionPriorityList = new ArrayList<String>();
        stateTransitionPriorityList.add("MASTER-SLAVE");
        stateTransitionPriorityList.add("SLAVE-MASTER");
        stateTransitionPriorityList.add("OFFLINE-SLAVE");
        stateTransitionPriorityList.add("SLAVE-OFFLINE");
        stateTransitionPriorityList.add("OFFLINE-DROPPED");
        record.setListField(StateModelDefinition.StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
                stateTransitionPriorityList);
        return record;
        // ZNRecordSerializer serializer = new ZNRecordSerializer();
        // System.out.println(new String(serializer.serialize(record)));
    }


    public static void stateList(){
        ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(ZK_ADDRESS);
        List<String> stateList = zkHelixAdmin.getStateModelDefs(CLUSTER_NAME);
        System.out.println("stateList=" + JsonUtil.toJson(stateList));
    }

}
