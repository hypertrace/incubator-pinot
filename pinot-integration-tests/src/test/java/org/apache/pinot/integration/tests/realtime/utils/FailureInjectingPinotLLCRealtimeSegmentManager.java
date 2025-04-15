/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests.realtime.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.util.FailureInjectionUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FailureInjectingPinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {
  @VisibleForTesting
  private final Map<String, String> _failureConfig;
  private long _maxSegmentCompletionTimeoutMs = 300000L;
  private static final Logger LOGGER = LoggerFactory.getLogger(FailureInjectingPinotLLCRealtimeSegmentManager.class);

  public FailureInjectingPinotLLCRealtimeSegmentManager(PinotHelixResourceManager helixResourceManager,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super(helixResourceManager, controllerConf, controllerMetrics);
    _failureConfig = new HashMap<>();
  }

  @Override
  protected boolean isExceededMaxSegmentCompletionTime(String realtimeTableName, String segmentName,
      long currentTimeMs) {
    Stat stat = new Stat();
    getSegmentZKMetadata(realtimeTableName, segmentName, stat);
    if (currentTimeMs > stat.getMtime() + _maxSegmentCompletionTimeoutMs) {
      LOGGER.info("Segment: {} exceeds the max completion time: {}ms, metadata update time: {}, current time: {}",
          segmentName, _maxSegmentCompletionTimeoutMs, stat.getMtime(), currentTimeMs);
      return true;
    } else {
      return false;
    }
  }

  public void setMaxSegmentCompletionTimeoutMs(long maxSegmentCompletionTimeoutMs) {
    _maxSegmentCompletionTimeoutMs = maxSegmentCompletionTimeoutMs;
  }

  @VisibleForTesting
  public void enableTestFault(String faultType) {
    _failureConfig.put(faultType, "true");
  }

  @VisibleForTesting
  public void disableTestFault(String faultType) {
    _failureConfig.remove(faultType);
  }

  @Override
  protected void preProcessNewSegmentZKMetadata() {
    FailureInjectionUtils.injectFailure(FailureInjectionUtils.FAULT_BEFORE_NEW_SEGMENT_METADATA_CREATION,
        _failureConfig);
  }

  @Override
  protected void preProcessCommitIdealStateUpdate() {
    FailureInjectionUtils.injectFailure(FailureInjectionUtils.FAULT_BEFORE_IDEAL_STATE_UPDATE, _failureConfig);
  }

  @Override
  protected void preProcessCommitSegmentEndMetadata() {
    FailureInjectionUtils.injectFailure(FailureInjectionUtils.FAULT_BEFORE_COMMIT_END_METADATA, _failureConfig);
  }
}
