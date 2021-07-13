package org.apache.helix.integration.controller;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.ClusterManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterStatus;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterFreezeMode extends ZkTestBase {
  private HelixManager _manager;
  private HelixDataAccessor _accessor;
  private String _clusterName;
  private int _numNodes;
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 5;
    _clusterName = "CLUSTER_" + TestHelper.getTestClassName();
    _participants = new MockParticipantManager[_numNodes];
    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        _numNodes, // number of nodes
        3, // replicas
        "MasterSlave", true);

    _manager = HelixManagerFactory
        .getZKHelixManager(_clusterName, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _accessor = _manager.getHelixDataAccessor();

    // start controller
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    _controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);
      _participants[i].syncStart();
    }

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() {
    _manager.disconnect();
    _controller.syncStop();
    Arrays.stream(_participants).forEach(ClusterManager::syncStop);
    deleteCluster(_clusterName);
  }

  @Test
  public void testEnableFreezeMode() throws Exception {
    // Not in freeze mode
    PauseSignal pauseSignal = _accessor.getProperty(_accessor.keyBuilder().pause());
    Assert.assertNull(pauseSignal);

    // Freeze cluster
    ClusterManagementModeRequest request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.CLUSTER_PAUSE)
        .withReason("test")
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);

    // Verify live instance status and cluster status
    verifyLiveInstanceStatus(_participants, LiveInstance.LiveInstanceStatus.PAUSED);

    ClusterStatus expectedClusterStatus = new ClusterStatus();
    expectedClusterStatus.setManagementMode(ClusterManagementMode.Type.CLUSTER_PAUSE);
    expectedClusterStatus.setManagementModeStatus(ClusterManagementMode.Status.COMPLETED);
    verifyClusterStatus(expectedClusterStatus);

    // Add a new live instance. Simulate an instance is rebooted and back to online
    String newInstanceName = "localhost_" + (12918 + _numNodes + 1);
    _gSetupTool.addInstancesToCluster(_clusterName, new String[]{newInstanceName});
    MockParticipantManager newParticipant =
        new MockParticipantManager(ZK_ADDR, _clusterName, newInstanceName);
    newParticipant.syncStart();

    // The new participant/live instance should be frozen by controller
    verifyLiveInstanceStatus(new MockParticipantManager[]{newParticipant},
        LiveInstance.LiveInstanceStatus.PAUSED);

    newParticipant.syncStop();

    // Unfreeze cluster
    request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.NORMAL)
        .withReason("test")
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);

    verifyLiveInstanceStatus(_participants, LiveInstance.LiveInstanceStatus.PAUSED);

    expectedClusterStatus = new ClusterStatus();
    expectedClusterStatus.setManagementMode(ClusterManagementMode.Type.NORMAL);
    expectedClusterStatus.setManagementModeStatus(ClusterManagementMode.Status.COMPLETED);
    verifyClusterStatus(expectedClusterStatus);
  }

  private void verifyLiveInstanceStatus(MockParticipantManager[] participants,
      LiveInstance.LiveInstanceStatus status) throws Exception {
    final PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
    Assert.assertTrue(TestHelper.verify(() -> {
      for (MockParticipantManager participant : participants) {
        String instanceName = participant.getInstanceName();
        LiveInstance liveInstance = _accessor.getProperty(keyBuilder.liveInstance(instanceName));
        if (status != liveInstance.getStatus()) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  private void verifyClusterStatus(ClusterStatus expectedMode) throws Exception {
    final PropertyKey statusPropertyKey = _accessor.keyBuilder().clusterStatus();
    TestHelper.verify(() -> {
      ClusterStatus clusterStatus = _accessor.getProperty(statusPropertyKey);
      return clusterStatus != null
          && expectedMode.getManagementMode().equals(clusterStatus.getManagementMode())
          && expectedMode.getManagementModeStatus().equals(clusterStatus.getManagementModeStatus());
    }, TestHelper.WAIT_DURATION);
  }
}
