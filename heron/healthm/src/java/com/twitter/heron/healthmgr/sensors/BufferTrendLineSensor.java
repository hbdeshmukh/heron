package com.twitter.heron.healthmgr.sensors;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;

import javax.inject.Inject;
import java.util.Map;

public class BufferTrendLineSensor extends BufferSizeSensor {
  @Inject
  BufferTrendLineSensor(PackingPlanProvider packingPlanProvider,
                        TopologyProvider topologyProvider,
                        MetricsProvider metricsProvider) {
    super(packingPlanProvider, topologyProvider, metricsProvider);
  }

  @Override
  protected Map<String, ComponentMetrics> getInstanceMetrics(String metric) {
    Map<String, ComponentMetrics> stmgrResult = metricsProvider.getComponentMetrics(
            metric,
            (int)(System.currentTimeMillis()/1000 - HealthMgrConstants.DEFAULT_METRIC_DURATION),
            HealthMgrConstants.DEFAULT_METRIC_DURATION,
            HealthMgrConstants.COMPONENT_STMGR);

    return stmgrResult;
  }
}
