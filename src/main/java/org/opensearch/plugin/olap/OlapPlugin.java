/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugin.olap.engine.VectorizedEngineExtension;
import org.opensearch.plugin.olap.engine.VeloxExecutionEngine;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.StatisticsCollector;
import org.opensearch.plugin.olap.transport.ExecuteFragmentAction;
import org.opensearch.plugin.olap.transport.ShuffleDataAction;
import org.opensearch.plugin.olap.transport.ShuffleManager;
import org.opensearch.plugin.olap.transport.TransportExecuteFragmentAction;
import org.opensearch.plugin.olap.transport.TransportShuffleDataAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * OpenSearch OLAP plugin that integrates Apache Velox for vectorized analytical query execution.
 *
 * <p>Supports MPP join strategies (broadcast + hash shuffle) when {@code
 * plugins.velox.mpp_enabled=true}.
 */
public class OlapPlugin extends Plugin implements ActionPlugin {

  private VeloxLifecycleService veloxLifecycleService;
  private QueryScheduler queryScheduler;
  private VeloxExecutionEngine veloxExecutionEngine;
  private ShuffleManager shuffleManager;

  @Override
  public Collection<Object> createComponents(
      Client client,
      ClusterService clusterService,
      ThreadPool threadPool,
      ResourceWatcherService resourceWatcherService,
      ScriptService scriptService,
      NamedXContentRegistry xContentRegistry,
      Environment environment,
      NodeEnvironment nodeEnvironment,
      NamedWriteableRegistry namedWriteableRegistry,
      IndexNameExpressionResolver indexNameExpressionResolver,
      Supplier<RepositoriesService> repositoriesServiceSupplier) {
    this.veloxLifecycleService = new VeloxLifecycleService(environment.settings());
    this.queryScheduler = new QueryScheduler(clusterService, threadPool);
    this.shuffleManager = new ShuffleManager();
    this.shuffleManager.setBufferMaxBytes(veloxLifecycleService.getShuffleBufferBytes());

    StatisticsCollector statisticsCollector = new StatisticsCollector(client, clusterService);
    this.veloxExecutionEngine =
        new VeloxExecutionEngine(veloxLifecycleService, queryScheduler, null, statisticsCollector);

    VectorizedEngineExtension.setEngine(veloxExecutionEngine);

    // Register dynamic setting update consumers
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.MPP_ENABLED, veloxLifecycleService::setMppEnabled);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.BROADCAST_MAX_SHARDS,
            veloxLifecycleService::setBroadcastMaxShards);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.SHUFFLE_PARTITIONS, veloxLifecycleService::setShufflePartitions);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.SEGMENT_PARALLELISM,
            veloxLifecycleService::setSegmentParallelism);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.TASK_MAX_RETRIES, veloxLifecycleService::setTaskMaxRetries);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.RUNTIME_FILTER_ENABLED,
            veloxLifecycleService::setRuntimeFilterEnabled);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.RUNTIME_FILTER_MAX_CARDINALITY,
            veloxLifecycleService::setRuntimeFilterMaxCardinality);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.RUNTIME_FILTER_BLOOM_ENABLED,
            veloxLifecycleService::setRuntimeFilterBloomEnabled);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.RUNTIME_FILTER_BLOOM_MAX_CARDINALITY,
            veloxLifecycleService::setRuntimeFilterBloomMaxCardinality);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.RUNTIME_FILTER_BLOOM_TWO_STAGE,
            veloxLifecycleService::setRuntimeFilterBloomTwoStage);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.CBO_STATISTICS_MODE, veloxLifecycleService::setCboStatisticsMode);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.FORCE_VECTORIZE, veloxLifecycleService::setForceVectorize);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.PER_FRAGMENT_ARROW_BYTES,
            veloxLifecycleService::setPerFragmentArrowBytes);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.RESULT_ALLOCATOR_BYTES,
            veloxLifecycleService::setResultAllocatorBytes);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.MAX_RESULT_BYTES, veloxLifecycleService::setMaxResultBytes);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.COORDINATOR_ALLOCATOR_BYTES,
            veloxLifecycleService::setCoordinatorAllocatorBytes);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.COORDINATOR_QUEUE_BYTES,
            veloxLifecycleService::setCoordinatorQueueBytes);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.COORDINATOR_INFLIGHT_BYTES,
            veloxLifecycleService::setCoordinatorInflightBytes);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.SHUFFLE_BUFFER_BYTES,
            value -> {
              veloxLifecycleService.setShuffleBufferBytes(value);
              shuffleManager.setBufferMaxBytes(value.getBytes());
            });
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.BACKPRESSURE_WAIT_MS,
            veloxLifecycleService::setBackpressureWaitMs);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            VeloxLifecycleService.BACKPRESSURE_WATERMARK_PCT,
            veloxLifecycleService::setBackpressureWatermarkPct);

    return Arrays.asList(
        veloxLifecycleService, queryScheduler, veloxExecutionEngine, shuffleManager);
  }

  @Override
  public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
    return List.of(
        new ActionHandler<>(ExecuteFragmentAction.INSTANCE, TransportExecuteFragmentAction.class),
        new ActionHandler<>(ShuffleDataAction.INSTANCE, TransportShuffleDataAction.class));
  }

  @Override
  public List<Setting<?>> getSettings() {
    return VeloxLifecycleService.getSettings();
  }

  @Override
  public void close() throws IOException {
    if (veloxLifecycleService != null) {
      veloxLifecycleService.close();
    }
  }
}
