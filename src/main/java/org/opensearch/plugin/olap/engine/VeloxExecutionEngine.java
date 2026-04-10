/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.engine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.ExternalStreamConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.connector.ExternalStreams.BlockingQueue;
import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.iterator.CloseableIterator;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.plugin.olap.plan.convert.VeloxPlanConverter;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.plugin.olap.plan.fragment.PlanFragmenter;
import org.opensearch.plugin.olap.scheduler.CostEstimator;
import org.opensearch.plugin.olap.scheduler.ExecutionPolicy;
import org.opensearch.plugin.olap.scheduler.JoinStrategy;
import org.opensearch.plugin.olap.scheduler.QueryExecution;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.Stage;
import org.opensearch.plugin.olap.scheduler.TaskDescriptor;
import org.opensearch.plugin.olap.transport.ExecuteFragmentResponse;
import org.opensearch.plugin.olap.transport.NodeResultCollector;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.transport.TransportService;

/**
 * Velox-based execution engine with MPP join support.
 *
 * <p>Join execution strategies:
 *
 * <ul>
 *   <li><b>Coordinator-centric</b> (mpp_enabled=false): Both sides collected on coordinator, join
 *       runs locally
 *   <li><b>Broadcast</b> (mpp_enabled=true, small build side): Build side broadcast to all
 *       probe-side nodes, join runs in parallel
 *   <li><b>Hash shuffle</b> (mpp_enabled=true, large sides): Both sides hash-partitioned by join
 *       key and shuffled to workers, join runs per partition
 * </ul>
 */
public class VeloxExecutionEngine {

  private static final Logger logger = LogManager.getLogger(VeloxExecutionEngine.class);

  private final VeloxPlanConverter planConverter;
  private final PlanFragmenter planFragmenter;
  private final QueryScheduler queryScheduler;
  private final VeloxLifecycleService veloxLifecycle;
  private volatile TransportService transportService;

  public VeloxExecutionEngine(
      VeloxLifecycleService veloxLifecycle,
      QueryScheduler queryScheduler,
      TransportService transportService) {
    this.veloxLifecycle = veloxLifecycle;
    this.planConverter = new VeloxPlanConverter();
    this.planFragmenter = new PlanFragmenter();
    this.queryScheduler = queryScheduler;
    this.transportService = transportService;
  }

  public ExecutionEngine.QueryResponse execute(RelNode relNode) {
    QueryId queryId = QueryId.generate();
    logger.info("Executing query {} via Velox engine", queryId);

    try {
      // Physical optimization: VolcanoPlanner with PhysicalConvention
      org.opensearch.plugin.olap.plan.physical.PhysicalOptimizer optimizer =
          new org.opensearch.plugin.olap.plan.physical.PhysicalOptimizer(
              veloxLifecycle.isMppEnabled());
      RelNode physicalPlan = optimizer.optimize(relNode);

      // Generate Velox PlanNodes + PlanFragments from the physical plan
      org.opensearch.plugin.olap.plan.physical.VeloxPlanGenerator generator =
          new org.opensearch.plugin.olap.plan.physical.VeloxPlanGenerator();
      List<PlanFragment> fragments = generator.generate(physicalPlan);

      return executeFragments(relNode, fragments, queryId);

    } catch (Exception e) {
      logger.error("Velox execution failed for query {}", queryId, e);
      throw new RuntimeException("Velox execution failed: " + e.getMessage(), e);
    }
  }

  // ---- Unified Fragment Execution ----

  /**
   * Execute a list of PlanFragments produced by VeloxPlanGenerator. Handles all topologies:
   * single-table (scan+agg), coordinator-centric join (two leaf stages + coordinator), and MPP
   * shuffle joins (hash-partitioned scan → shuffle → worker join).
   */
  private ExecutionEngine.QueryResponse executeFragments(
      RelNode relNode, List<PlanFragment> fragments, QueryId queryId) {

    // Detect if any leaf fragments require hash shuffle
    boolean hasShuffleScan = fragments.stream().anyMatch(f -> f.getProperties().isShuffleScan());

    if (hasShuffleScan) {
      return executeShuffleFragments(relNode, fragments, queryId);
    }

    QueryExecution execution = queryScheduler.schedule(fragments, ExecutionPolicy.PHASED);
    NodeResultCollector collector = new NodeResultCollector(transportService, queryScheduler);

    List<Stage> leafStages = execution.getLeafStages();
    PlanFragment coordinatorFragment = findCoordinatorFragment(fragments);

    // Phase 1: Dispatch all leaf stages to data nodes
    List<ExecuteFragmentResponse> leafResponses =
        collector.dispatchAndCollect(execution, leafStages);

    // Phase 2: Execute coordinator fragment if present
    if (coordinatorFragment == null || coordinatorFragment.getPlanRoot() == null) {
      return buildQueryResponse(relNode.getRowType(), leafResponses);
    }

    List<ExecuteFragmentResponse> finalResponses;
    if (leafStages.size() <= 1) {
      // Single-input coordinator (aggregation: PARTIAL → FINAL)
      logger.info(
          "Executing coordinator fragment for query {} with {} partial results",
          queryId,
          leafResponses.size());
      finalResponses = executeCoordinatorFragment(coordinatorFragment, leafResponses);
    } else {
      // Multi-input coordinator (join: left scan + right scan → HashJoinNode)
      logger.info(
          "Executing coordinator join for query {} with {} leaf stages",
          queryId,
          leafStages.size());
      Map<Integer, List<ExecuteFragmentResponse>> responsesByFragment =
          groupResponsesByFragment(leafResponses, leafStages);
      // For a two-input join, pass the first two fragment response groups
      List<List<ExecuteFragmentResponse>> inputGroups =
          new ArrayList<>(responsesByFragment.values());
      finalResponses =
          executeCoordinatorJoin(
              coordinatorFragment,
              inputGroups.size() > 0 ? inputGroups.get(0) : Collections.emptyList(),
              inputGroups.size() > 1 ? inputGroups.get(1) : Collections.emptyList());
    }

    return buildQueryResponse(relNode.getRowType(), finalResponses);
  }

  // ---- MPP Shuffle Fragment Execution ----

  /**
   * Execute fragments that use hash shuffle distribution. Shuffle scan fragments hash-partition
   * their data and send partitions to worker nodes via ShuffleDataAction. Worker nodes execute the
   * join plan on their received partitions.
   */
  private ExecutionEngine.QueryResponse executeShuffleFragments(
      RelNode relNode, List<PlanFragment> fragments, QueryId queryId) {
    int partitionCount = veloxLifecycle.getShufflePartitions();
    if (partitionCount <= 0) {
      partitionCount = queryScheduler.getDataNodeIds().size();
    }
    logger.info(
        "Executing shuffle fragments for query {} with {} partitions", queryId, partitionCount);

    // The coordinator fragment contains the join plan. For shuffle execution, we need to
    // create a HASH_PARTITIONED stage from it so it runs on N workers (not just coordinator).
    PlanFragment coordinatorFragment = findCoordinatorFragment(fragments);
    if (coordinatorFragment == null) {
      throw new IllegalStateException("No coordinator fragment found for shuffle join");
    }

    // Replace the coordinator fragment with a HASH_PARTITIONED one for worker execution
    List<PlanFragment> adjustedFragments = new ArrayList<>();
    for (PlanFragment f : fragments) {
      if (f.getProperties().getDistribution() == FragmentProperties.Distribution.COORDINATOR) {
        // Convert coordinator to hash-partitioned worker stage
        adjustedFragments.add(
            new PlanFragment(
                f.getFragmentId(),
                f.getPlanRoot(),
                FragmentProperties.hashPartitioned(partitionCount),
                f.getInputFragmentIds()));
      } else {
        adjustedFragments.add(f);
      }
    }

    QueryExecution execution = queryScheduler.schedule(adjustedFragments, ExecutionPolicy.PHASED);
    NodeResultCollector collector = new NodeResultCollector(transportService, queryScheduler);

    // Identify shuffle scan stages and the join worker stage
    List<Stage> shuffleScanStages = new ArrayList<>();
    Stage joinStage = null;
    for (Stage stage : execution.getStages()) {
      FragmentProperties props = stage.getFragment().getProperties();
      if (props.isShuffleScan()) {
        shuffleScanStages.add(stage);
      } else if (props.getDistribution() == FragmentProperties.Distribution.HASH_PARTITIONED) {
        joinStage = stage;
      }
    }

    if (joinStage == null) {
      throw new IllegalStateException("No hash-partitioned join stage found");
    }

    // Get worker node IDs for shuffle targets
    List<String> workerNodeIds = new ArrayList<>();
    for (TaskDescriptor task : joinStage.getTasks()) {
      workerNodeIds.add(task.getTargetNode().getId());
    }

    // Assign join sides to shuffle scan stages (first=left, second=right)
    for (int i = 0; i < shuffleScanStages.size(); i++) {
      Stage stage = shuffleScanStages.get(i);
      String side = (i == 0) ? "left" : "right";
      // Update the fragment properties with join side info
      FragmentProperties oldProps = stage.getFragment().getProperties();
      if (oldProps.getJoinSide() == null) {
        stage
            .getFragment()
            .setProperties(
                FragmentProperties.shuffleScan(
                    oldProps.getSourceIndex(),
                    side,
                    oldProps.getShuffleKeyChannels(),
                    workerNodeIds.size()));
      }
    }

    // Count senders per side
    int leftSenderCount = 0;
    int rightSenderCount = 0;
    for (int i = 0; i < shuffleScanStages.size(); i++) {
      int taskCount = shuffleScanStages.get(i).getTasks().size();
      if (i == 0) leftSenderCount = taskCount;
      else rightSenderCount += taskCount;
    }

    // Phase 1: Dispatch shuffle scan stages
    int targetStageId = joinStage.getStageId().getStageNumber();
    List<ExecuteFragmentResponse> scanResponses =
        collector.dispatchAndCollectShuffle(
            execution, shuffleScanStages, workerNodeIds, targetStageId);
    logger.info(
        "Shuffle scan phase complete: {} responses for query {}", scanResponses.size(), queryId);

    // Phase 2: Dispatch shuffle join tasks to workers
    List<ExecuteFragmentResponse> joinResponses =
        collector.dispatchAndCollectShuffleJoin(
            execution,
            List.of(joinStage),
            queryId.getId(),
            targetStageId,
            leftSenderCount,
            rightSenderCount);

    return buildQueryResponse(relNode.getRowType(), joinResponses);
  }

  /** Group leaf stage responses by their fragment ID. */
  private Map<Integer, List<ExecuteFragmentResponse>> groupResponsesByFragment(
      List<ExecuteFragmentResponse> responses, List<Stage> leafStages) {
    Map<Integer, List<ExecuteFragmentResponse>> grouped = new HashMap<>();
    int idx = 0;
    for (Stage stage : leafStages) {
      int fragId = stage.getFragment().getFragmentId();
      for (TaskDescriptor task : stage.getTasks()) {
        grouped.computeIfAbsent(fragId, k -> new ArrayList<>()).add(responses.get(idx++));
      }
    }
    return grouped;
  }

  // ---- Join Plan Execution ----

  private ExecutionEngine.QueryResponse executeJoinPlan(
      RelNode relNode, PlanNode veloxPlan, QueryId queryId) {
    // Extract source indices for both sides
    String leftIndex = extractSourceIndex(findLeftInput(relNode));
    String rightIndex = extractSourceIndex(findRightInput(relNode));
    logger.info(
        "Join query {}: left={}, right={}, mpp_enabled={}",
        queryId,
        leftIndex,
        rightIndex,
        veloxLifecycle.isMppEnabled());

    if (!veloxLifecycle.isMppEnabled()) {
      return executeJoinCoordinatorCentric(relNode, veloxPlan, leftIndex, rightIndex, queryId);
    }

    // MPP enabled: use cost-based strategy selection
    CostEstimator estimator =
        new CostEstimator(
            queryScheduler.getClusterService(), veloxLifecycle.getBroadcastMaxShards());
    JoinStrategy strategy = estimator.selectJoinStrategy(leftIndex, rightIndex);
    logger.info("Query {} selected join strategy: {}", queryId, strategy);

    switch (strategy) {
      case BROADCAST:
        return executeJoinBroadcast(relNode, veloxPlan, leftIndex, rightIndex, queryId, estimator);
      case HASH_SHUFFLE:
        return executeJoinHashShuffle(relNode, veloxPlan, leftIndex, rightIndex, queryId);
      default:
        return executeJoinCoordinatorCentric(relNode, veloxPlan, leftIndex, rightIndex, queryId);
    }
  }

  // ---- Option 1: Coordinator-Centric Join ----

  private ExecutionEngine.QueryResponse executeJoinCoordinatorCentric(
      RelNode relNode, PlanNode veloxPlan, String leftIndex, String rightIndex, QueryId queryId) {
    logger.info("Executing coordinator-centric join for query {}", queryId);

    List<PlanFragment> fragments =
        planFragmenter.fragmentJoin(
            veloxPlan, leftIndex, rightIndex, JoinStrategy.COORDINATOR_CENTRIC, 0);
    QueryExecution execution = queryScheduler.schedule(fragments, ExecutionPolicy.PHASED);
    NodeResultCollector collector = new NodeResultCollector(transportService, queryScheduler);

    // Dispatch both leaf stages (left scan + right scan) and collect results
    List<Stage> leafStages = execution.getLeafStages();
    List<ExecuteFragmentResponse> leafResponses =
        collector.dispatchAndCollect(execution, leafStages);

    // Group responses by fragment ID
    Map<Integer, List<ExecuteFragmentResponse>> responsesByFragment = new HashMap<>();
    int responseIdx = 0;
    for (Stage stage : leafStages) {
      int fragId = stage.getFragment().getFragmentId();
      for (TaskDescriptor task : stage.getTasks()) {
        responsesByFragment
            .computeIfAbsent(fragId, k -> new ArrayList<>())
            .add(leafResponses.get(responseIdx++));
      }
    }

    // Find the coordinator fragment (the one with join + everything above)
    PlanFragment coordinatorFragment = null;
    for (PlanFragment f : fragments) {
      if (f.getProperties().getDistribution() == FragmentProperties.Distribution.COORDINATOR) {
        coordinatorFragment = f;
        break;
      }
    }

    if (coordinatorFragment == null) {
      throw new IllegalStateException("No coordinator fragment found for join");
    }

    // Execute coordinator join: wire both sides into the HashJoinNode
    List<ExecuteFragmentResponse> finalResponses =
        executeCoordinatorJoin(
            coordinatorFragment,
            responsesByFragment.getOrDefault(
                fragments.get(0).getFragmentId(), Collections.emptyList()),
            responsesByFragment.getOrDefault(
                fragments.get(1).getFragmentId(), Collections.emptyList()));

    return buildQueryResponse(relNode.getRowType(), finalResponses);
  }

  /**
   * Execute a join on the coordinator node by deserializing both sides into BlockingQueues and
   * running the HashJoinNode through Velox.
   */
  private List<ExecuteFragmentResponse> executeCoordinatorJoin(
      PlanFragment coordinatorFragment,
      List<ExecuteFragmentResponse> leftResponses,
      List<ExecuteFragmentResponse> rightResponses) {
    Session session = veloxLifecycle.getSession();
    String connectorId = "connector-external-stream";

    // The coordinator plan already has exchange scan placeholders from VeloxPlanGenerator.
    // Find their IDs so we can wire splits to the correct scan nodes.
    org.opensearch.plugin.olap.execution.VeloxExecutor tempExecutor =
        new org.opensearch.plugin.olap.execution.VeloxExecutor(session);
    List<String> scanIds = tempExecutor.findAllTableScanNodeIds(coordinatorFragment.getPlanRoot());

    if (scanIds.size() < 2) {
      logger.warn("Coordinator join plan has < 2 scan nodes, falling back to single-input");
      return executeCoordinatorFragment(coordinatorFragment, leftResponses);
    }

    String leftScanId = scanIds.get(0);
    String rightScanId = scanIds.get(1);

    // Create BlockingQueues for both sides
    BlockingQueue leftQueue = session.externalStreamOps().newBlockingQueue();
    BlockingQueue rightQueue = session.externalStreamOps().newBlockingQueue();

    // Build and execute the coordinator plan as-is (exchange scans already in place)
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query = new Query(coordinatorFragment.getPlanRoot(), Config.empty(), connectorConfig);

    logger.info("Executing coordinator join plan with scan IDs: {}, {}", leftScanId, rightScanId);

    SerialTask serialTask = session.queryOps().execute(query);

    // Wire splits to the actual exchange scan node IDs
    serialTask.addSplit(leftScanId, new ExternalStreamConnectorSplit(connectorId, leftQueue.id()));
    serialTask.addSplit(
        rightScanId, new ExternalStreamConnectorSplit(connectorId, rightQueue.id()));
    serialTask.noMoreSplits(leftScanId);
    serialTask.noMoreSplits(rightScanId);

    // Start feeder threads for both sides
    Thread leftFeeder =
        createFeederThread(session, leftResponses, leftQueue, "olap-join-left-feeder");
    Thread rightFeeder =
        createFeederThread(session, rightResponses, rightQueue, "olap-join-right-feeder");
    leftFeeder.start();
    rightFeeder.start();

    // Collect results with timeout
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      byte[] resultData = collectResultsWithTimeout(session, serialTask, allocator, 60);
      leftFeeder.join(5_000);
      rightFeeder.join(5_000);
      return List.of(ExecuteFragmentResponse.success(0, resultData));
    } catch (Exception e) {
      throw new RuntimeException("Coordinator join execution failed", e);
    } finally {
      try {
        allocator.close();
      } catch (IllegalStateException ex) {
        logger.debug("Arrow allocator close warning: {}", ex.getMessage());
      }
    }
  }

  // ---- Option 4: Broadcast Join ----

  private ExecutionEngine.QueryResponse executeJoinBroadcast(
      RelNode relNode,
      PlanNode veloxPlan,
      String leftIndex,
      String rightIndex,
      QueryId queryId,
      CostEstimator estimator) {
    logger.info("Executing broadcast join for query {}", queryId);

    // Determine which side to broadcast (the smaller one)
    String buildSide = estimator.selectBuildSide(leftIndex, rightIndex);

    // If the right side (build) is larger, we need to swap the indices for fragmentation.
    // Our fragmeter convention: left=probe, right=build.
    String probeIndex, buildIndex;
    if ("left".equals(buildSide)) {
      // Swap: left is build, right is probe
      probeIndex = rightIndex;
      buildIndex = leftIndex;
    } else {
      probeIndex = leftIndex;
      buildIndex = rightIndex;
    }

    List<PlanFragment> fragments =
        planFragmenter.fragmentJoin(veloxPlan, probeIndex, buildIndex, JoinStrategy.BROADCAST, 0);
    QueryExecution execution = queryScheduler.schedule(fragments, ExecutionPolicy.PHASED);
    NodeResultCollector collector = new NodeResultCollector(transportService, queryScheduler);

    // Phase 1: Collect build side
    Stage buildStage = null;
    for (Stage stage : execution.getLeafStages()) {
      if ("right".equals(stage.getFragment().getProperties().getJoinSide())) {
        buildStage = stage;
        break;
      }
    }
    if (buildStage == null) {
      // Fallback: first leaf stage is build
      buildStage = execution.getLeafStages().get(0);
    }

    List<ExecuteFragmentResponse> buildResponses =
        collector.dispatchAndCollect(execution, List.of(buildStage));

    // Collect build-side data as native serde batches
    List<byte[]> broadcastData = new ArrayList<>();
    for (ExecuteFragmentResponse resp : buildResponses) {
      if (resp.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) continue;
      if (resp.hasNativeResults()) {
        broadcastData.addAll(resp.getNativeResultBatches());
      } else if (resp.getResultData() != null && resp.getResultData().length > 0) {
        // Convert Arrow IPC to native serde for broadcast
        broadcastData.addAll(arrowIpcToNativeBatches(resp.getResultData()));
      }
    }

    logger.info("Broadcast: collected {} build-side batches", broadcastData.size());

    // Phase 2: Dispatch probe-side join tasks with broadcast data
    Stage probeJoinStage = null;
    for (Stage stage : execution.getStages()) {
      if (stage.getFragment().getProperties().getDistribution()
          == FragmentProperties.Distribution.BROADCAST) {
        probeJoinStage = stage;
        break;
      }
    }

    if (probeJoinStage == null) {
      throw new IllegalStateException("No broadcast probe stage found");
    }

    // Attach broadcast data to each probe task
    for (TaskDescriptor task : probeJoinStage.getTasks()) {
      task.getFragment().setBroadcastData(broadcastData);
    }

    List<ExecuteFragmentResponse> probeResponses =
        collector.dispatchAndCollectBroadcast(execution, List.of(probeJoinStage), broadcastData);

    // Phase 3: If there's a coordinator fragment (final agg), execute it
    PlanFragment coordinatorFragment = findCoordinatorFragment(fragments);
    List<ExecuteFragmentResponse> finalResponses;

    if (coordinatorFragment != null) {
      finalResponses = executeCoordinatorFragment(coordinatorFragment, probeResponses);
    } else {
      finalResponses = probeResponses;
    }

    return buildQueryResponse(relNode.getRowType(), finalResponses);
  }

  // ---- Option 4: Hash Shuffle Join ----

  private ExecutionEngine.QueryResponse executeJoinHashShuffle(
      RelNode relNode, PlanNode veloxPlan, String leftIndex, String rightIndex, QueryId queryId) {
    int partitionCount = veloxLifecycle.getShufflePartitions();
    if (partitionCount <= 0) {
      partitionCount = queryScheduler.getDataNodeIds().size();
    }
    logger.info(
        "Executing hash shuffle join for query {} with {} partitions", queryId, partitionCount);

    List<PlanFragment> fragments =
        planFragmenter.fragmentJoin(
            veloxPlan, leftIndex, rightIndex, JoinStrategy.HASH_SHUFFLE, partitionCount);
    QueryExecution execution = queryScheduler.schedule(fragments, ExecutionPolicy.PHASED);
    NodeResultCollector collector = new NodeResultCollector(transportService, queryScheduler);

    // Identify stages
    List<Stage> leftScanStages = new ArrayList<>();
    List<Stage> rightScanStages = new ArrayList<>();
    Stage joinStage = null;
    PlanFragment coordinatorFragment = null;

    for (Stage stage : execution.getStages()) {
      FragmentProperties props = stage.getFragment().getProperties();
      if (props.isShuffleScan()) {
        if ("left".equals(props.getJoinSide())) {
          leftScanStages.add(stage);
        } else {
          rightScanStages.add(stage);
        }
      } else if (props.getDistribution() == FragmentProperties.Distribution.HASH_PARTITIONED) {
        joinStage = stage;
      } else if (props.getDistribution() == FragmentProperties.Distribution.COORDINATOR) {
        coordinatorFragment = stage.getFragment();
      }
    }

    if (joinStage == null) {
      throw new IllegalStateException("No hash-partitioned join stage found");
    }

    // Get worker node IDs for shuffle targets
    List<String> workerNodeIds = new ArrayList<>();
    for (TaskDescriptor task : joinStage.getTasks()) {
      workerNodeIds.add(task.getTargetNode().getId());
    }

    // Phase 1: Dispatch left and right scan+shuffle tasks in parallel
    int leftSenderCount = 0;
    int rightSenderCount = 0;
    for (Stage s : leftScanStages) leftSenderCount += s.getTasks().size();
    for (Stage s : rightScanStages) rightSenderCount += s.getTasks().size();

    List<Stage> allScanStages = new ArrayList<>();
    allScanStages.addAll(leftScanStages);
    allScanStages.addAll(rightScanStages);

    List<ExecuteFragmentResponse> scanResponses =
        collector.dispatchAndCollectShuffle(
            execution, allScanStages, workerNodeIds, joinStage.getStageId().getStageNumber());

    logger.info("Shuffle scan phase complete: {} responses", scanResponses.size());

    // Phase 2: Dispatch join tasks to workers
    final int finalLeftSenders = leftSenderCount;
    final int finalRightSenders = rightSenderCount;
    List<ExecuteFragmentResponse> joinResponses =
        collector.dispatchAndCollectShuffleJoin(
            execution,
            List.of(joinStage),
            queryId.getId(),
            joinStage.getStageId().getStageNumber(),
            finalLeftSenders,
            finalRightSenders);

    // Phase 3: If there's a coordinator fragment, execute it
    List<ExecuteFragmentResponse> finalResponses;
    if (coordinatorFragment != null && coordinatorFragment.getPlanRoot() != null) {
      finalResponses = executeCoordinatorFragment(coordinatorFragment, joinResponses);
    } else {
      finalResponses = joinResponses;
    }

    return buildQueryResponse(relNode.getRowType(), finalResponses);
  }

  // ---- Coordinator Fragment Execution (for FINAL aggregation) ----

  private List<ExecuteFragmentResponse> executeCoordinatorFragment(
      PlanFragment coordinatorFragment, List<ExecuteFragmentResponse> partialResponses) {
    Session session = veloxLifecycle.getSession();
    String connectorId = "connector-external-stream";

    BlockingQueue queue = session.externalStreamOps().newBlockingQueue();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try {
      Type intermediateType = null;
      for (ExecuteFragmentResponse response : partialResponses) {
        if (response.hasNativeResults()) {
          byte[] firstBatch = response.getNativeResultBatches().get(0);
          BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(firstBatch);
          intermediateType = vec.getType();
          break;
        }
      }

      if (intermediateType == null) {
        logger.warn("No partial results to feed into coordinator fragment");
        return List.of(ExecuteFragmentResponse.success(0, new byte[0]));
      }

      TableScanNode exchangeScan =
          new TableScanNode(
              "exchange_scan",
              intermediateType,
              new ExternalStreamTableHandle(connectorId),
              Collections.emptyList());

      PlanNode coordinatorPlan =
          wireSourceIntoPlan(
              coordinatorFragment.getPlanRoot(), exchangeScan, (RowType) intermediateType);

      ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
      Query query = new Query(coordinatorPlan, Config.empty(), connectorConfig);

      SerialTask serialTask = session.queryOps().execute(query);
      serialTask.addSplit(
          "exchange_scan", new ExternalStreamConnectorSplit(connectorId, queue.id()));
      serialTask.noMoreSplits("exchange_scan");

      Thread feederThread =
          new Thread(
              () -> {
                try {
                  int batchCount = 0;
                  for (ExecuteFragmentResponse response : partialResponses) {
                    if (response.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) continue;
                    if (!response.hasNativeResults()) continue;
                    for (byte[] nativeBatch : response.getNativeResultBatches()) {
                      BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(nativeBatch);
                      queue.put(vec.asRowVector());
                      batchCount++;
                    }
                  }
                  logger.info("Coordinator feeder: pushed {} native batches", batchCount);
                  queue.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Error feeding partial results to coordinator fragment", e);
                  queue.noMoreInput();
                }
              },
              "olap-coordinator-feeder");
      feederThread.setDaemon(true);
      feederThread.start();

      byte[] resultData = collectResultsWithTimeout(session, serialTask, allocator, 30);
      feederThread.join(5_000);
      return List.of(ExecuteFragmentResponse.success(0, resultData));

    } catch (Exception e) {
      throw new RuntimeException("Coordinator fragment execution failed", e);
    } finally {
      try {
        allocator.close();
      } catch (IllegalStateException ex) {
        logger.debug("Arrow allocator close warning: {}", ex.getMessage());
      }
    }
  }

  // ---- Plan Tree Manipulation Helpers ----

  /** Wire two source TableScanNodes into a join plan with null sources. */
  private PlanNode wireJoinSources(
      PlanNode node, TableScanNode leftSource, TableScanNode rightSource) {
    if (node instanceof HashJoinNode) {
      HashJoinNode join = (HashJoinNode) node;
      return new HashJoinNode(
          join.getId(),
          join.getJoinType(),
          join.getLeftKeys(),
          join.getRightKeys(),
          join.getFilter(),
          leftSource,
          rightSource,
          join.getOutputType(),
          false,
          false);
    }

    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      List<PlanNode> newSources = new ArrayList<>();
      boolean changed = false;
      for (PlanNode child : sources) {
        PlanNode wired = wireJoinSources(child, leftSource, rightSource);
        newSources.add(wired);
        if (wired != child) changed = true;
      }
      if (changed) {
        return reconstructNode(node, newSources);
      }
    }
    return node;
  }

  /** Fix aggregate types in the plan if needed (placeholder — delegates to existing logic). */
  private PlanNode fixAggregateTypes(PlanNode plan, Type leftType, Type rightType) {
    // For coordinator-centric join, the data arrives as Arrow IPC which preserves types.
    // No special intermediate type fixing needed unless there's a FINAL agg.
    return plan;
  }

  /** Wire a single source into a plan with empty sources (for FINAL agg). */
  private PlanNode wireSourceIntoPlan(
      PlanNode node, TableScanNode source, RowType intermediateRowType) {
    if (node instanceof org.boostscale.velox4j.plan.AggregationNode) {
      org.boostscale.velox4j.plan.AggregationNode agg =
          (org.boostscale.velox4j.plan.AggregationNode) node;
      if (agg.getSources().isEmpty()) {
        List<org.boostscale.velox4j.aggregate.Aggregate> fixedAggregates =
            fixFinalAggregateTypes(agg, intermediateRowType);
        return new org.boostscale.velox4j.plan.AggregationNode(
            agg.getId(),
            agg.getStep(),
            agg.getGroupingKeys(),
            agg.getPreGroupedKeys(),
            agg.getAggregateNames(),
            fixedAggregates,
            agg.isIgnoreNullKeys(),
            agg.isNoGroupsSpanBatches(),
            List.of(source),
            null,
            Collections.emptyList());
      }
    }

    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      List<PlanNode> newSources = new ArrayList<>();
      boolean changed = false;
      for (PlanNode child : sources) {
        PlanNode wired = wireSourceIntoPlan(child, source, intermediateRowType);
        newSources.add(wired);
        if (wired != child) changed = true;
      }
      if (changed) {
        return reconstructNode(node, newSources);
      }
    }
    return node;
  }

  private List<org.boostscale.velox4j.aggregate.Aggregate> fixFinalAggregateTypes(
      org.boostscale.velox4j.plan.AggregationNode agg, RowType intermediateRowType) {
    int groupKeyCount = agg.getGroupingKeys().size();
    List<org.boostscale.velox4j.aggregate.Aggregate> origAggs = agg.getAggregates();
    List<String> aggNames = agg.getAggregateNames();
    List<org.boostscale.velox4j.aggregate.Aggregate> fixed = new ArrayList<>(origAggs.size());

    for (int i = 0; i < origAggs.size(); i++) {
      org.boostscale.velox4j.aggregate.Aggregate orig = origAggs.get(i);
      Type intermediateColType = intermediateRowType.getChildren().get(groupKeyCount + i);
      String intermediateName = aggNames.get(i);

      org.boostscale.velox4j.expression.TypedExpr intermediateRef =
          org.boostscale.velox4j.expression.FieldAccessTypedExpr.create(
              intermediateColType, intermediateName);
      org.boostscale.velox4j.expression.CallTypedExpr fixedCall =
          new org.boostscale.velox4j.expression.CallTypedExpr(
              orig.getCall().getReturnType(),
              List.of(intermediateRef),
              orig.getCall().getFunctionName());

      fixed.add(
          new org.boostscale.velox4j.aggregate.Aggregate(
              fixedCall,
              orig.getRawInputTypes(),
              orig.getMask(),
              orig.getSortingKeys(),
              orig.getSortingOrders(),
              orig.isDistinct()));
    }
    return fixed;
  }

  // ---- Utility Methods ----

  private Type getResponseOutputType(Session session, List<ExecuteFragmentResponse> responses) {
    for (ExecuteFragmentResponse resp : responses) {
      if (resp.hasNativeResults() && !resp.getNativeResultBatches().isEmpty()) {
        BaseVector vec =
            session.baseVectorOps().deserializeOneFromBuf(resp.getNativeResultBatches().get(0));
        return vec.getType();
      }
      if (resp.getResultData() != null && resp.getResultData().length > 0) {
        return getArrowIpcOutputType(resp.getResultData());
      }
    }
    return null;
  }

  private Type getArrowIpcOutputType(byte[] arrowIpc) {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {
      reader.loadNextBatch();
      return arrowSchemaToVeloxRowType(reader.getVectorSchemaRoot().getSchema());
    } catch (Exception e) {
      logger.warn("Cannot read Arrow IPC type: {}", e.getMessage());
      return null;
    } finally {
      allocator.close();
    }
  }

  private List<byte[]> arrowIpcToNativeBatches(byte[] arrowIpc) {
    Session session = veloxLifecycle.getSession();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    List<byte[]> nativeBatches = new ArrayList<>();

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        RowVector rv = session.arrowOps().fromArrowVectorSchemaRoot(allocator, root);
        nativeBatches.add(org.boostscale.velox4j.data.BaseVectors.serializeOneToBuf(rv));
      }
    } catch (Exception e) {
      logger.warn("Cannot convert Arrow IPC to native: {}", e.getMessage());
    } finally {
      allocator.close();
    }
    return nativeBatches;
  }

  private Thread createFeederThread(
      Session session,
      List<ExecuteFragmentResponse> responses,
      BlockingQueue queue,
      String threadName) {
    Thread thread =
        new Thread(
            () -> {
              try {
                for (ExecuteFragmentResponse resp : responses) {
                  if (resp.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) continue;
                  if (resp.hasNativeResults()) {
                    for (byte[] batch : resp.getNativeResultBatches()) {
                      BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(batch);
                      queue.put(vec.asRowVector());
                    }
                  } else if (resp.getResultData() != null && resp.getResultData().length > 0) {
                    feedArrowIpcToQueue(session, resp.getResultData(), queue);
                  }
                }
                queue.noMoreInput();
              } catch (Throwable e) {
                logger.error("Feeder error in {}", threadName, e);
                queue.noMoreInput();
              }
            },
            threadName);
    thread.setDaemon(true);
    return thread;
  }

  private void feedArrowIpcToQueue(Session session, byte[] arrowIpc, BlockingQueue queue) {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        RowVector rv = session.arrowOps().fromArrowVectorSchemaRoot(allocator, root);
        queue.put(rv);
      }
    } catch (Exception e) {
      logger.error("Error feeding Arrow IPC to queue", e);
    } finally {
      allocator.close();
    }
  }

  private byte[] collectResultsWithTimeout(
      Session session, SerialTask serialTask, BufferAllocator allocator, int timeoutSeconds) {
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "olap-coordinator-collector");
              t.setDaemon(true);
              return t;
            });
    java.util.concurrent.Future<byte[]> resultFuture =
        executor.submit(
            () -> {
              java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
              org.apache.arrow.vector.ipc.ArrowStreamWriter writer = null;
              boolean hasData = false;
              CloseableIterator<RowVector> iter = UpIterators.asJavaIterator(serialTask);
              try {
                while (iter.hasNext()) {
                  RowVector batch = iter.next();
                  if (batch == null) break;
                  VectorSchemaRoot arrowRoot =
                      org.boostscale.velox4j.arrow.Arrow.toArrowVectorSchemaRoot(allocator, batch);
                  if (writer == null) {
                    writer =
                        new org.apache.arrow.vector.ipc.ArrowStreamWriter(
                            arrowRoot, null, java.nio.channels.Channels.newChannel(baos));
                    writer.start();
                  }
                  writer.writeBatch();
                  hasData = true;
                  arrowRoot.close();
                }
                if (writer != null) {
                  writer.end();
                  writer.close();
                }
                iter.close();
              } catch (Exception e) {
                throw new RuntimeException("Velox execution error", e);
              }
              return hasData ? baos.toByteArray() : new byte[0];
            });

    try {
      return resultFuture.get(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException e) {
      org.opensearch.common.util.concurrent.FutureUtils.cancel(resultFuture);
      throw new RuntimeException("Execution timed out after " + timeoutSeconds + "s");
    } catch (Exception e) {
      throw new RuntimeException("Execution failed", e);
    } finally {
      executor.shutdown();
    }
  }

  private PlanFragment findCoordinatorFragment(List<PlanFragment> fragments) {
    for (PlanFragment f : fragments) {
      if (f.getProperties().getDistribution() == FragmentProperties.Distribution.COORDINATOR) {
        return f;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private List<PlanNode> getNodeSources(PlanNode node) {
    if (node instanceof org.boostscale.velox4j.plan.AggregationNode) {
      return ((org.boostscale.velox4j.plan.AggregationNode) node).getSources();
    }
    return getNodeSourcesViaReflection(node);
  }

  @SuppressWarnings("unchecked")
  private List<PlanNode> getNodeSourcesViaReflection(PlanNode node) {
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      return (List<PlanNode>) m.invoke(node);
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  private PlanNode reconstructNode(PlanNode node, List<PlanNode> newSources) {
    if (node instanceof org.boostscale.velox4j.plan.ProjectNode) {
      org.boostscale.velox4j.plan.ProjectNode p = (org.boostscale.velox4j.plan.ProjectNode) node;
      return new org.boostscale.velox4j.plan.ProjectNode(
          p.getId(), newSources, p.getNames(), p.getProjections());
    } else if (node instanceof org.boostscale.velox4j.plan.LimitNode) {
      org.boostscale.velox4j.plan.LimitNode l = (org.boostscale.velox4j.plan.LimitNode) node;
      return new org.boostscale.velox4j.plan.LimitNode(
          l.getId(), newSources, l.getOffset(), l.getCount(), l.isPartial());
    } else if (node instanceof org.boostscale.velox4j.plan.FilterNode) {
      org.boostscale.velox4j.plan.FilterNode f = (org.boostscale.velox4j.plan.FilterNode) node;
      return new org.boostscale.velox4j.plan.FilterNode(f.getId(), newSources, f.getFilter());
    } else if (node instanceof org.boostscale.velox4j.plan.AggregationNode) {
      org.boostscale.velox4j.plan.AggregationNode a =
          (org.boostscale.velox4j.plan.AggregationNode) node;
      return new org.boostscale.velox4j.plan.AggregationNode(
          a.getId(),
          a.getStep(),
          a.getGroupingKeys(),
          a.getPreGroupedKeys(),
          a.getAggregateNames(),
          a.getAggregates(),
          a.isIgnoreNullKeys(),
          a.isNoGroupsSpanBatches(),
          newSources,
          null,
          Collections.emptyList());
    } else if (node instanceof HashJoinNode) {
      HashJoinNode j = (HashJoinNode) node;
      return new HashJoinNode(
          j.getId(),
          j.getJoinType(),
          j.getLeftKeys(),
          j.getRightKeys(),
          j.getFilter(),
          newSources.get(0),
          newSources.size() > 1 ? newSources.get(1) : newSources.get(0),
          j.getOutputType(),
          false,
          false);
    }
    return node;
  }

  // ---- RelNode Traversal ----

  private RelNode findLeftInput(RelNode relNode) {
    if (relNode instanceof LogicalJoin) {
      return ((LogicalJoin) relNode).getLeft();
    }
    for (RelNode input : relNode.getInputs()) {
      RelNode found = findLeftInput(input);
      if (found != null) return found;
    }
    return null;
  }

  private RelNode findRightInput(RelNode relNode) {
    if (relNode instanceof LogicalJoin) {
      return ((LogicalJoin) relNode).getRight();
    }
    for (RelNode input : relNode.getInputs()) {
      RelNode found = findRightInput(input);
      if (found != null) return found;
    }
    return null;
  }

  private String extractSourceIndex(RelNode relNode) {
    if (relNode == null) return null;
    if (relNode instanceof TableScan) {
      List<String> names = ((TableScan) relNode).getTable().getQualifiedName();
      return names.get(names.size() - 1);
    }
    for (RelNode input : relNode.getInputs()) {
      String index = extractSourceIndex(input);
      if (index != null) {
        return index;
      }
    }
    return null;
  }

  // ---- Arrow/Type Helpers ----

  private RowType arrowSchemaToVeloxRowType(org.apache.arrow.vector.types.pojo.Schema schema) {
    List<String> names = new ArrayList<>();
    List<Type> types = new ArrayList<>();
    for (Field field : schema.getFields()) {
      names.add(field.getName());
      types.add(arrowTypeToVeloxType(field));
    }
    return new RowType(names, types);
  }

  private Type arrowTypeToVeloxType(Field field) {
    org.apache.arrow.vector.types.pojo.ArrowType arrowType = field.getType();
    if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
      return new org.boostscale.velox4j.type.BooleanType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int) {
      int bitWidth = ((org.apache.arrow.vector.types.pojo.ArrowType.Int) arrowType).getBitWidth();
      if (bitWidth <= 32) return new org.boostscale.velox4j.type.IntegerType();
      return new org.boostscale.velox4j.type.BigIntType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
      var precision =
          ((org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) arrowType).getPrecision();
      if (precision == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
        return new org.boostscale.velox4j.type.RealType();
      }
      return new org.boostscale.velox4j.type.DoubleType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
      return new org.boostscale.velox4j.type.VarCharType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary) {
      return new org.boostscale.velox4j.type.VarbinaryType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Struct) {
      List<String> childNames = new ArrayList<>();
      List<Type> childTypes = new ArrayList<>();
      for (Field child : field.getChildren()) {
        childNames.add(child.getName());
        childTypes.add(arrowTypeToVeloxType(child));
      }
      return new RowType(childNames, childTypes);
    }
    return new org.boostscale.velox4j.type.VarbinaryType();
  }

  // ---- SQL Plugin Response Building ----

  private ExecutionEngine.QueryResponse buildQueryResponse(
      RelDataType rowType, List<ExecuteFragmentResponse> responses) {
    List<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      ExprType exprType = mapToExprType(field.getType().getSqlTypeName());
      columns.add(new ExecutionEngine.Schema.Column(field.getName(), null, exprType));
    }
    ExecutionEngine.Schema schema = new ExecutionEngine.Schema(columns);

    List<ExprValue> results = new ArrayList<>();
    for (ExecuteFragmentResponse response : responses) {
      if (response.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) {
        throw new RuntimeException("Fragment execution failed: " + response.getErrorMessage());
      }
      byte[] arrowData = response.getResultData();
      if (arrowData != null && arrowData.length > 0) {
        results.addAll(readArrowIpcToExprValues(arrowData));
      }
    }

    return new ExecutionEngine.QueryResponse(schema, results, Cursor.None);
  }

  private List<ExprValue> readArrowIpcToExprValues(byte[] arrowIpc) {
    List<ExprValue> rows = new ArrayList<>();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        List<Field> fields = root.getSchema().getFields();
        int rowCount = root.getRowCount();

        for (int row = 0; row < rowCount; row++) {
          LinkedHashMap<String, Object> tupleValues = new LinkedHashMap<>();
          for (int col = 0; col < fields.size(); col++) {
            String name = fields.get(col).getName();
            Object value = root.getVector(col).getObject(row);
            if (value instanceof org.apache.arrow.vector.util.Text) {
              value = value.toString();
            }
            tupleValues.put(name, value);
          }
          rows.add(ExprValueUtils.tupleValue(tupleValues));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read Arrow IPC result", e);
    } finally {
      allocator.close();
    }
    return rows;
  }

  private ExprType mapToExprType(SqlTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return ExprCoreType.BOOLEAN;
      case TINYINT:
        return ExprCoreType.BYTE;
      case SMALLINT:
        return ExprCoreType.SHORT;
      case INTEGER:
        return ExprCoreType.INTEGER;
      case BIGINT:
        return ExprCoreType.LONG;
      case FLOAT:
      case REAL:
        return ExprCoreType.FLOAT;
      case DOUBLE:
      case DECIMAL:
        return ExprCoreType.DOUBLE;
      case CHAR:
      case VARCHAR:
        return ExprCoreType.STRING;
      case DATE:
        return ExprCoreType.DATE;
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return ExprCoreType.TIMESTAMP;
      default:
        return ExprCoreType.UNKNOWN;
    }
  }

  public void setTransportService(TransportService transportService) {
    this.transportService = transportService;
  }

  public boolean isAvailable() {
    return veloxLifecycle.isEnabled();
  }
}
