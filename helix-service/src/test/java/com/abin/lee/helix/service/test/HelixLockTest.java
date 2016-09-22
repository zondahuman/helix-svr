package com.abin.lee.helix.service.test;


import org.apache.helix.api.Scope;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.lock.zk.ZKHelixLock;
import org.apache.helix.manager.zk.ZkClient;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests that the Zookeeper-based Helix lock can acquire, block, and release as appropriate
 */
public class HelixLockTest {
    public static void main(String[] args) {
        ZkClient _zkclient = null;
        final long TIMEOUT = 30000;
        final long RETRY_INTERVAL = 100;
        _zkclient.waitUntilConnected(TIMEOUT, TimeUnit.MILLISECONDS);
        final AtomicBoolean t1Locked = new AtomicBoolean(false);
        final AtomicBoolean t1Done = new AtomicBoolean(false);
        final AtomicInteger field1 = new AtomicInteger(0);
        final AtomicInteger field2 = new AtomicInteger(1);
        final ClusterId clusterId = ClusterId.from("testCluster");
        final HelixLock lock1 = new ZKHelixLock(clusterId, Scope.cluster(clusterId), _zkclient);
        final HelixLock lock2 = new ZKHelixLock(clusterId, Scope.cluster(clusterId), _zkclient);
    }


}

