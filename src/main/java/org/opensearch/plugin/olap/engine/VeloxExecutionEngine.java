/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.engine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.arrow.Arrow;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.ExternalStreamConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.connector.ExternalStreams.BlockingQueue;
import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.data.BaseVectors;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.iterator.CloseableIterator;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.join.JoinType;
import org.boostscale.velox4j.plan.AggregationNode;
import org.boostscale.velox4j.plan.FilterNode;
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.serde.Serde;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RealType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.type.VarCharType;
import org.boostscale.velox4j.type.VarbinaryType;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.execution.OlapBloomFilter;
import org.opensearch.plugin.olap.execution.RuntimeFilterPayload;
import org.opensearch.plugin.olap.execution.VeloxExecutor;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.plugin.olap.plan.physical.PhysicalOptimizer;
import org.opensearch.plugin.olap.plan.physical.VeloxPlanGenerator;
import org.opensearch.plugin.olap.profile.OlapProfileAssembler;
import org.opensearch.plugin.olap.scheduler.CostEstimator;
import org.opensearch.plugin.olap.scheduler.ExecutionPolicy;
import org.opensearch.plugin.olap.scheduler.JoinStrategy;
import org.opensearch.plugin.olap.scheduler.QueryExecution;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.Stage;
import org.opensearch.plugin.olap.scheduler.StatisticsCollector;
import org.opensearch.plugin.olap.scheduler.TableStatistics;
import org.opensearch.plugin.olap.scheduler.TaskDescriptor;
import org.opensearch.plugin.olap.transport.ExecuteFragmentResponse;
import org.opensearch.plugin.olap.transport.NodeResultCollector;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.QueryProfiling;
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

  private final QueryScheduler queryScheduler;
  private final VeloxLifecycleService veloxLifecycle;
  private final StatisticsCollector statisticsCollector;
  private volatile TransportService transportService;

  public VeloxExecutionEngine(
      VeloxLifecycleService veloxLifecycle,
      QueryScheduler queryScheduler,
      TransportService transportService,
      StatisticsCollector statisticsCollector) {
    this.veloxLifecycle = veloxLifecycle;
    this.queryScheduler = queryScheduler;
    this.transportService = transportService;
    this.statisticsCollector = statisticsCollector;
  }

  /**
   * Explain the Velox physical plan for a query. Runs PhysicalOptimizer + VeloxPlanGenerator and
   * returns the Velox native plan tree for each fragment via {@code PlanNode.toFormatString()}.
   * Used by the SQL plugin's {@code explain} command.
   */
  public ExecutionEngine.ExplainResponse explain(RelNode relNode) {
    PhysicalOptimizer optimizer = new PhysicalOptimizer(veloxLifecycle.isMppEnabled());
    RelNode physicalPlan = optimizer.optimize(relNode);

    VeloxPlanGenerator generator = new VeloxPlanGenerator();
    List<PlanFragment> fragments = generator.generate(physicalPlan);

    StringBuilder veloxPlan = new StringBuilder();
    for (PlanFragment f : fragments) {
      veloxPlan
          .append("Fragment ")
          .append(f.getFragmentId())
          .append(" [")
          .append(f.getProperties().getDistribution())
          .append("]\n");
      veloxPlan.append(f.getPlanRoot().toFormatString(true, true));
      veloxPlan.append("\n");
    }

    return new ExecutionEngine.ExplainResponse(
        new ExecutionEngine.ExplainResponseNodeV2(
            physicalPlan.explain(), veloxPlan.toString(), null));
  }

  public ExecutionEngine.QueryResponse execute(RelNode relNode) {
    QueryId queryId = QueryId.generate();
    logger.info("Executing query {} via Velox engine", queryId);

    // Reset query-scoped profile accumulator. Populated by every NodeResultCollector dispatch
    // and by local coordinator fragment execution. The assembler consumes it at the end of
    // execute() to hand a ProfilePlanNode tree to QueryProfiling.current().setPlanRoot.
    profileAccumulator.set(null);
    profileRfSummary.set(null);
    boolean profile = QueryProfiling.current().isEnabled();
    if (profile) {
      profileAccumulator.set(new ArrayList<>());
    }

    try {
      // Step 0: Collect CBO statistics (row counts) if enabled.
      // Stats are used for join reorder (planning time) and MPP strategy selection (execution
      // time).
      Map<String, TableStatistics> statsMap =
          planContainsJoin(relNode) ? collectStatistics(relNode) : Map.of();

      // Physical optimization: VolcanoPlanner with PhysicalConvention.
      // Stats are passed to enable join reorder via MultiJoinOptimizeBushyRule.
      PhysicalOptimizer optimizer = new PhysicalOptimizer(veloxLifecycle.isMppEnabled(), statsMap);
      RelNode physicalPlan = optimizer.optimize(relNode);

      // Generate Velox PlanNodes + PlanFragments from the physical plan
      VeloxPlanGenerator generator = new VeloxPlanGenerator();
      List<PlanFragment> fragments = generator.generate(physicalPlan);

      // Log plan explain for each fragment (DEBUG level)
      if (logger.isDebugEnabled()) {
        for (PlanFragment f : fragments) {
          logger.debug(
              "Plan explain: query={} fragment={} dist={}\n{}",
              queryId,
              f.getFragmentId(),
              f.getProperties().getDistribution(),
              f.getPlanRoot().toFormatString(true, true));
        }
      }

      ExecutionEngine.QueryResponse result =
          executeFragments(relNode, fragments, queryId, statsMap);

      if (profile) {
        List<ExecuteFragmentResponse> collected = profileAccumulator.get();
        if (collected != null && !collected.isEmpty()) {
          ProfileContext ctx = QueryProfiling.current();
          ctx.setPlanRoot(OlapProfileAssembler.buildPlan(profileRfSummary.get(), collected));
        }
      }

      return result;

    } catch (Exception e) {
      logger.error("Velox execution failed for query {}", queryId, e);
      throw new RuntimeException("Velox execution failed: " + e.getMessage(), e);
    } finally {
      profileAccumulator.remove();
      profileRfSummary.remove();
    }
  }

  /**
   * Query-scoped accumulator for data-node profiles. Populated by {@link #newCollector()} and by
   * paths that have their own response lists (broadcast/shuffle execution). Consumed once in {@link
   * #execute(RelNode)} before returning.
   */
  private final ThreadLocal<List<ExecuteFragmentResponse>> profileAccumulator = new ThreadLocal<>();

  /** Optional label describing the runtime filter applied (e.g. "rf=BLOOM bloomBytes=512"). */
  private final ThreadLocal<String> profileRfSummary = new ThreadLocal<>();

  /** Set the RF summary string surfaced in the profile root node. No-op if profile disabled. */
  private void recordProfileRf(String summary) {
    if (profileAccumulator.get() != null) {
      profileRfSummary.set(summary);
    }
  }

  /**
   * Factory for a {@link NodeResultCollector} with profile state wired in. Prefer this over
   * instantiating directly so every execution path picks up profile=true uniformly.
   */
  private NodeResultCollector newCollector() {
    NodeResultCollector c =
        new NodeResultCollector(
            transportService, queryScheduler, veloxLifecycle.getTaskMaxRetries());
    c.setProfileEnabled(profileAccumulator.get() != null);
    return c;
  }

  /** Record a batch of fragment responses for later profile assembly. No-op if profile disabled. */
  private void recordProfileResponses(List<ExecuteFragmentResponse> responses) {
    List<ExecuteFragmentResponse> acc = profileAccumulator.get();
    if (acc != null && responses != null) {
      acc.addAll(responses);
    }
  }

  // ---- Unified Fragment Execution ----

  /**
   * Execute a list of PlanFragments produced by VeloxPlanGenerator. Handles all topologies:
   * single-table (scan+agg), coordinator-centric join (two leaf stages + coordinator), MPP
   * broadcast join (build collected, join on probe nodes), and MPP shuffle join (hash-partitioned
   * scan → shuffle → worker join).
   */
  private ExecutionEngine.QueryResponse executeFragments(
      RelNode relNode,
      List<PlanFragment> fragments,
      QueryId queryId,
      Map<String, TableStatistics> statsMap) {

    PlanFragment coordinatorFragment = findCoordinatorFragment(fragments);

    // Collect leaf fragments (SOURCE or shuffle scan)
    List<PlanFragment> leafFragments =
        fragments.stream().filter(f -> f.isLeaf()).collect(Collectors.toList());

    // MPP join strategy selection: when mpp_enabled=true and this is a multi-table join,
    // CostEstimator decides between BROADCAST and HASH_SHUFFLE.
    if (veloxLifecycle.isMppEnabled() && leafFragments.size() >= 2 && coordinatorFragment != null) {
      if (leafFragments.size() > 2) {
        // Multi-way join (3+ tables): fall back to coordinator-centric execution.
        // TODO: Implement staged MPP execution — decompose nested joins into
        //  sequential binary broadcast/shuffle operations for full MPP multi-way.
        logger.info(
            "Multi-way join ({} tables) for query {} — using coordinator-centric execution",
            leafFragments.size(),
            queryId);
      } else {
        JoinStrategy strategy = selectJoinStrategy(leafFragments, statsMap);
        logger.info("MPP join strategy for query {}: {}", queryId, strategy);
        switch (strategy) {
          case BROADCAST:
            return executeBroadcastFragments(
                relNode, leafFragments, coordinatorFragment, queryId, statsMap);
          case HASH_SHUFFLE:
            return executeShuffleFragments(relNode, fragments, queryId);
          default:
            break; // fall through to coordinator-centric
        }
      }
    }

    // Default coordinator-centric path (mpp_enabled=false or non-join queries)
    QueryExecution execution = queryScheduler.schedule(fragments, ExecutionPolicy.PHASED);
    NodeResultCollector collector = newCollector();
    List<Stage> leafStages = execution.getLeafStages();

    // Phase 1: Dispatch all leaf stages to data nodes
    List<ExecuteFragmentResponse> leafResponses =
        collector.dispatchAndCollect(execution, leafStages);
    recordProfileResponses(leafResponses);

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
      // Multi-input coordinator (join: N leaf scans → nested HashJoinNodes)
      logger.info(
          "Executing coordinator join for query {} with {} leaf stages",
          queryId,
          leafStages.size());
      Map<Integer, List<ExecuteFragmentResponse>> responsesByFragment =
          groupResponsesByFragment(leafResponses, leafStages);
      List<List<ExecuteFragmentResponse>> inputGroups =
          new ArrayList<>(responsesByFragment.values());
      finalResponses = executeCoordinatorJoin(coordinatorFragment, inputGroups);
    }

    return buildQueryResponse(relNode.getRowType(), finalResponses);
  }

  /** Select join strategy using CostEstimator based on leaf fragment source indices. */
  private JoinStrategy selectJoinStrategy(
      List<PlanFragment> leafFragments, Map<String, TableStatistics> statsMap) {
    String leftIndex = leafFragments.get(0).getProperties().getSourceIndex();
    String rightIndex = leafFragments.get(1).getProperties().getSourceIndex();
    CostEstimator estimator =
        new CostEstimator(
            queryScheduler.getClusterService(), veloxLifecycle.getBroadcastMaxShards(), statsMap);
    return estimator.selectJoinStrategy(leftIndex, rightIndex);
  }

  // ---- MPP Broadcast Fragment Execution ----

  /**
   * Execute a broadcast join: collect the smaller (build) side, then dispatch the join plan to
   * probe-side nodes with the build data broadcast. The coordinator join plan (with two exchange
   * scans) is reused — TransportExecuteFragmentAction wires the first scan to local shards (probe)
   * and the second scan to the broadcast data (build).
   */
  private ExecutionEngine.QueryResponse executeBroadcastFragments(
      RelNode relNode,
      List<PlanFragment> leafFragments,
      PlanFragment coordinatorFragment,
      QueryId queryId,
      Map<String, TableStatistics> statsMap) {

    // Determine build side. For outer joins, the build side is constrained by join semantics:
    // - LEFT JOIN: build must be right (left rows are preserved, must be the probe)
    // - RIGHT JOIN: build must be left (right rows are preserved, must be the probe)
    // - INNER JOIN: build is the smaller side (CostEstimator decides)
    String leftIndex = leafFragments.get(0).getProperties().getSourceIndex();
    String rightIndex = leafFragments.get(1).getProperties().getSourceIndex();
    String buildSide =
        selectBroadcastBuildSide(coordinatorFragment, leftIndex, rightIndex, statsMap);

    PlanFragment buildFragment;
    PlanFragment probeLeafFragment;
    String probeIndex;
    int buildScanIndex; // which exchange scan in the coordinator plan is the build side
    if ("left".equals(buildSide)) {
      buildFragment = leafFragments.get(0);
      probeLeafFragment = leafFragments.get(1);
      probeIndex = rightIndex;
      buildScanIndex = 0; // left exchange scan = build
    } else {
      buildFragment = leafFragments.get(1);
      probeLeafFragment = leafFragments.get(0);
      probeIndex = leftIndex;
      buildScanIndex = 1; // right exchange scan = build
    }

    logger.info(
        "Executing broadcast join for query {}: build={}, probe={}",
        queryId,
        buildFragment.getProperties().getSourceIndex(),
        probeIndex);

    // Create adjusted fragments for broadcast execution:
    // 1. Build scan (leaf, SOURCE) — collects build data to coordinator
    // 2. Join (BROADCAST, probeIndex) — runs on probe-side nodes with broadcast data
    List<PlanFragment> adjustedFragments =
        List.of(
            new PlanFragment(
                0,
                buildFragment.getPlanRoot(),
                FragmentProperties.source(buildFragment.getProperties().getSourceIndex()),
                Collections.emptyList()),
            new PlanFragment(
                1,
                coordinatorFragment.getPlanRoot(),
                FragmentProperties.broadcast(probeIndex),
                List.of(0)));

    QueryExecution execution = queryScheduler.schedule(adjustedFragments, ExecutionPolicy.PHASED);
    NodeResultCollector collector = newCollector();

    // Phase 1: Dispatch build stage, collect results. If two-stage BLOOM is opted in, ask each
    // data node to produce a PARTIAL bloom over the build-side join key column alongside its
    // normal Arrow output, sized identically across all nodes (so the partials can be OR-merged
    // at the coordinator).
    List<Stage> buildStages = execution.getLeafStages();

    boolean rfEnabled = veloxLifecycle.isRuntimeFilterEnabled() && isInnerJoin(coordinatorFragment);
    boolean twoStageBloom =
        rfEnabled
            && veloxLifecycle.isRuntimeFilterBloomEnabled()
            && veloxLifecycle.isRuntimeFilterBloomTwoStage();

    String buildJoinKeyName = null;
    String probeFieldType = null;
    String probeFieldName = null;
    if (twoStageBloom) {
      HashJoinNode joinNode = findHashJoinNode(coordinatorFragment.getPlanRoot());
      if (joinNode != null) {
        List<FieldAccessTypedExpr> buildKeys =
            (buildScanIndex == 0) ? joinNode.getLeftKeys() : joinNode.getRightKeys();
        if (!buildKeys.isEmpty()) {
          buildJoinKeyName = buildKeys.get(0).getFieldName();
          String rawProbeFieldName = extractProbeJoinKeyField(coordinatorFragment, buildScanIndex);
          probeFieldName =
              rawProbeFieldName == null ? null : rawProbeFieldName.replaceAll("\\d+$", "");
          probeFieldType = extractProbeJoinKeyType(relNode, probeFieldName);
        }
      }
      // Suppress PARTIAL bloom build if the field type isn't bloom-compatible.
      if (probeFieldType == null || !OlapBloomFilter.isSupportedType(probeFieldType)) {
        twoStageBloom = false;
      }
    }

    List<ExecuteFragmentResponse> buildResponses;
    if (twoStageBloom && buildJoinKeyName != null) {
      int expectedInsertions = veloxLifecycle.getRuntimeFilterBloomMaxCardinality();
      buildResponses =
          collector.dispatchAndCollectWithPartialBloom(
              execution, buildStages, buildJoinKeyName, probeFieldType, expectedInsertions);
      logger.info(
          "Two-stage BLOOM build requested for query {}: field={}, expectedInsertions={}",
          queryId,
          buildJoinKeyName,
          expectedInsertions);
    } else {
      buildResponses = collector.dispatchAndCollect(execution, buildStages);
    }
    recordProfileResponses(buildResponses);

    // Convert build-side results to native serde for broadcast
    List<byte[]> broadcastData = new ArrayList<>();
    List<byte[]> partialBloomBytesList = new ArrayList<>();
    for (ExecuteFragmentResponse resp : buildResponses) {
      if (resp.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) continue;
      if (resp.hasNativeResults()) {
        broadcastData.addAll(resp.getNativeResultBatches());
      } else if (resp.getResultData() != null && resp.getResultData().length > 0) {
        broadcastData.addAll(arrowIpcToNativeBatches(resp.getResultData()));
      }
      if (resp.hasPartialBloom()) {
        partialBloomBytesList.add(resp.getPartialBloomBytes());
      }
    }
    logger.info(
        "Broadcast: collected {} build-side batches, {} partial blooms for query {}",
        broadcastData.size(),
        partialBloomBytesList.size(),
        queryId);

    // Phase 1.5: Extract runtime filter from build-side data. extractRuntimeFilter chooses
    // TERMS/BLOOM/NONE based on observed cardinality vs. the configured caps. When two-stage
    // blooms were requested, the partial bytes are folded directly.
    RuntimeFilterPayload rfPayload = RuntimeFilterPayload.none();
    if (rfEnabled && !broadcastData.isEmpty()) {
      rfPayload =
          extractRuntimeFilter(
              coordinatorFragment,
              buildScanIndex,
              broadcastData,
              queryId,
              relNode,
              partialBloomBytesList);
    }

    // Extract probe-side pushdown from the probe leaf fragment's plan.
    // The probe leaf may have a FilterNode that needs to be applied at the Lucene level
    // since the coordinator join plan only has exchange scans (no FilterNode).
    String probePlanJson = null;
    if (probeLeafFragment.getPlanRoot() != null) {
      Query probeQuery =
          new Query(probeLeafFragment.getPlanRoot(), Config.empty(), ConnectorConfig.empty());
      probePlanJson = Serde.toJson(probeQuery);
    }

    // Phase 2: Dispatch broadcast join to probe-side nodes
    List<Stage> broadcastStages = new ArrayList<>();
    for (Stage stage : execution.getStages()) {
      if (stage.getFragment().getProperties().getDistribution()
          == FragmentProperties.Distribution.BROADCAST) {
        broadcastStages.add(stage);
      }
    }

    List<ExecuteFragmentResponse> joinResponses =
        collector.dispatchAndCollectBroadcast(
            execution, broadcastStages, broadcastData, buildScanIndex, rfPayload, probePlanJson);
    recordProfileResponses(joinResponses);
    if (rfPayload != null && rfPayload.getKind() != null) {
      recordProfileRf(
          "rf="
              + rfPayload.getKind().name()
              + (rfPayload.getKind().name().equals("BLOOM") && rfPayload.getBloomBytes() != null
                  ? " bloomBytes=" + rfPayload.getBloomBytes().length
                  : ""));
    }

    return buildQueryResponse(relNode.getRowType(), joinResponses);
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
    NodeResultCollector collector = newCollector();

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
    recordProfileResponses(scanResponses);
    logger.info(
        "Shuffle scan phase complete: {} responses for query {}", scanResponses.size(), queryId);

    // Phase 2: Dispatch shuffle join tasks to workers.
    // Use execution's queryId (same as scan tasks) so ShuffleManager lookup matches.
    List<ExecuteFragmentResponse> joinResponses =
        collector.dispatchAndCollectShuffleJoin(
            execution,
            List.of(joinStage),
            execution.getQueryId().getId(),
            targetStageId,
            leftSenderCount,
            rightSenderCount);
    recordProfileResponses(joinResponses);

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

  /**
   * Execute a join on the coordinator node with N input groups (2 for binary join, 3+ for
   * multi-way). Each input group corresponds to a leaf stage's responses. The coordinator plan has
   * N exchange scan placeholders that are wired to N BlockingQueues.
   */
  private List<ExecuteFragmentResponse> executeCoordinatorJoin(
      PlanFragment coordinatorFragment, List<List<ExecuteFragmentResponse>> inputGroups) {
    Session session = veloxLifecycle.getSession();
    String connectorId = "connector-external-stream";

    // The coordinator plan already has exchange scan placeholders from VeloxPlanGenerator.
    // Find their IDs so we can wire splits to the correct scan nodes.
    VeloxExecutor tempExecutor = new VeloxExecutor(session);
    List<String> scanIds = tempExecutor.findAllTableScanNodeIds(coordinatorFragment.getPlanRoot());

    if (scanIds.size() < 2) {
      logger.warn("Coordinator join plan has < 2 scan nodes, falling back to single-input");
      return executeCoordinatorFragment(
          coordinatorFragment,
          inputGroups.isEmpty() ? Collections.emptyList() : inputGroups.get(0));
    }

    int numInputs = Math.min(scanIds.size(), inputGroups.size());
    logger.info("Executing coordinator join plan with {} inputs, scan IDs: {}", numInputs, scanIds);

    // Create BlockingQueues for all inputs
    List<BlockingQueue> queues = new ArrayList<>();
    for (int i = 0; i < numInputs; i++) {
      queues.add(session.externalStreamOps().newBlockingQueue());
    }

    // Build and execute the coordinator plan as-is (exchange scans already in place)
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query = new Query(coordinatorFragment.getPlanRoot(), Config.empty(), connectorConfig);

    SerialTask serialTask = session.queryOps().execute(query);

    // Wire splits for all inputs
    for (int i = 0; i < numInputs; i++) {
      String scanId = scanIds.get(i);
      serialTask.addSplit(
          scanId, new ExternalStreamConnectorSplit(connectorId, queues.get(i).id()));
      serialTask.noMoreSplits(scanId);
    }

    // Start feeder threads for all inputs
    List<Thread> feeders = new ArrayList<>();
    for (int i = 0; i < numInputs; i++) {
      Thread feeder =
          createFeederThread(session, inputGroups.get(i), queues.get(i), "olap-join-feeder-" + i);
      feeder.start();
      feeders.add(feeder);
    }

    // Collect results with timeout
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      byte[] resultData = collectResultsWithTimeout(session, serialTask, allocator, 60);
      for (Thread feeder : feeders) {
        feeder.join(5_000);
      }
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

  /** Wire a single source into a plan with empty sources (for FINAL agg). */
  private PlanNode wireSourceIntoPlan(
      PlanNode node, TableScanNode source, RowType intermediateRowType) {
    if (node instanceof AggregationNode) {
      AggregationNode agg = (AggregationNode) node;
      if (agg.getSources().isEmpty()) {
        List<Aggregate> fixedAggregates = fixFinalAggregateTypes(agg, intermediateRowType);
        return new AggregationNode(
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

    List<PlanNode> sources = node.getSources();
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

  private List<Aggregate> fixFinalAggregateTypes(AggregationNode agg, RowType intermediateRowType) {
    int groupKeyCount = agg.getGroupingKeys().size();
    List<Aggregate> origAggs = agg.getAggregates();
    List<String> aggNames = agg.getAggregateNames();
    List<Aggregate> fixed = new ArrayList<>(origAggs.size());

    for (int i = 0; i < origAggs.size(); i++) {
      Aggregate orig = origAggs.get(i);
      Type intermediateColType = intermediateRowType.getChildren().get(groupKeyCount + i);
      String intermediateName = aggNames.get(i);

      TypedExpr intermediateRef =
          FieldAccessTypedExpr.create(intermediateColType, intermediateName);
      CallTypedExpr fixedCall =
          new CallTypedExpr(
              orig.getCall().getReturnType(),
              List.of(intermediateRef),
              orig.getCall().getFunctionName());

      fixed.add(
          new Aggregate(
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

  private List<byte[]> arrowIpcToNativeBatches(byte[] arrowIpc) {
    Session session = veloxLifecycle.getSession();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    List<byte[]> nativeBatches = new ArrayList<>();

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        RowVector rv = session.arrowOps().fromArrowVectorSchemaRoot(allocator, root);
        nativeBatches.add(BaseVectors.serializeOneToBuf(rv));
      }
    } catch (Exception e) {
      logger.warn("Cannot convert Arrow IPC to native: {}", e.getMessage());
    } finally {
      try {
        allocator.close();
      } catch (IllegalStateException ex) {
        // Arrow C Data Interface may hold buffer references after Velox conversion;
        // the underlying memory is managed by Velox, so this is safe to ignore.
        logger.debug("Arrow allocator close warning: {}", ex.getMessage());
      }
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
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "olap-coordinator-collector");
              t.setDaemon(true);
              return t;
            });
    Future<byte[]> resultFuture =
        executor.submit(
            () -> {
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              ArrowStreamWriter writer = null;
              boolean hasData = false;
              CloseableIterator<RowVector> iter = UpIterators.asJavaIterator(serialTask);
              try {
                while (iter.hasNext()) {
                  RowVector batch = iter.next();
                  if (batch == null) break;
                  VectorSchemaRoot arrowRoot = Arrow.toArrowVectorSchemaRoot(allocator, batch);
                  if (writer == null) {
                    writer = new ArrowStreamWriter(arrowRoot, null, Channels.newChannel(baos));
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
      return resultFuture.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      FutureUtils.cancel(resultFuture);
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

  private PlanNode reconstructNode(PlanNode node, List<PlanNode> newSources) {
    if (node instanceof ProjectNode) {
      ProjectNode p = (ProjectNode) node;
      return new ProjectNode(p.getId(), newSources, p.getNames(), p.getProjections());
    } else if (node instanceof LimitNode) {
      LimitNode l = (LimitNode) node;
      return new LimitNode(l.getId(), newSources, l.getOffset(), l.getCount(), l.isPartial());
    } else if (node instanceof FilterNode) {
      FilterNode f = (FilterNode) node;
      return new FilterNode(f.getId(), newSources, f.getFilter());
    } else if (node instanceof AggregationNode) {
      AggregationNode a = (AggregationNode) node;
      return new AggregationNode(
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
          false,
          false);
    }
    return node;
  }

  // ---- CBO Statistics Helpers ----

  /** Check if the plan tree contains a Join node. */
  private boolean planContainsJoin(RelNode node) {
    if (node instanceof Join) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (planContainsJoin(input)) {
        return true;
      }
    }
    return false;
  }

  /** Collect table statistics for all indices referenced in the plan. */
  private Map<String, TableStatistics> collectStatistics(RelNode relNode) {
    if (!veloxLifecycle.isCboEnabled() || statisticsCollector == null) {
      return Map.of();
    }
    Set<String> indexNames = extractIndexNames(relNode);
    if (indexNames.isEmpty()) {
      return Map.of();
    }
    Map<String, TableStatistics> stats = statisticsCollector.collect(indexNames);
    if (logger.isDebugEnabled()) {
      stats.forEach((name, s) -> logger.debug("CBO stats: {}", s));
    }
    return stats;
  }

  /** Extract all index names from TableScan nodes in the RelNode tree. */
  private Set<String> extractIndexNames(RelNode node) {
    Set<String> names = new LinkedHashSet<>();
    extractIndexNamesRecursive(node, names);
    return names;
  }

  private void extractIndexNamesRecursive(RelNode node, Set<String> names) {
    if (node instanceof TableScan) {
      List<String> qualifiedName = ((TableScan) node).getTable().getQualifiedName();
      names.add(qualifiedName.get(qualifiedName.size() - 1));
    }
    for (RelNode input : node.getInputs()) {
      extractIndexNamesRecursive(input, names);
    }
  }

  // ---- Runtime Filter Helpers ----

  /**
   * Extract a runtime filter from the build side of a broadcast join.
   *
   * <p>Chooses the RF kind by a cardinality ladder:
   *
   * <ul>
   *   <li>≤ {@code runtime_filter_max_cardinality} → TERMS (distinct-value list, Lucene pushdown)
   *   <li>(terms-cap, {@code runtime_filter_bloom_max_cardinality}] → BLOOM (bit-array, probe
   *       feeder predicate)
   *   <li>&gt; bloom-cap → NONE (abandon, same as pre-BLOOM behavior)
   * </ul>
   *
   * <p>Two BLOOM construction paths:
   *
   * <ul>
   *   <li><b>Single-stage</b> ({@code partialBloomBytesList} null/empty): the coordinator rebuilds
   *       the bloom by iterating broadcast build rows in-process.
   *   <li><b>Two-stage</b> ({@code partialBloomBytesList} non-empty): each data node has already
   *       produced a PARTIAL bloom over its local build output with a coordinator-broadcast {@code
   *       expectedInsertions}. The coordinator merges them via {@link OlapBloomFilter#merge}
   *       (bitwise-OR) — cheaper in coordinator memory/CPU and prerequisite for BLOOM on shuffle
   *       joins.
   * </ul>
   */
  private RuntimeFilterPayload extractRuntimeFilter(
      PlanFragment coordinatorFragment,
      int buildScanIndex,
      List<byte[]> broadcastData,
      QueryId queryId,
      RelNode relNode,
      List<byte[]> partialBloomBytesList) {
    try {
      HashJoinNode joinNode = findHashJoinNode(coordinatorFragment.getPlanRoot());
      if (joinNode == null) return RuntimeFilterPayload.none();

      // buildScanIndex=0 means build is left, so build keys = leftKeys
      List<FieldAccessTypedExpr> buildKeys =
          (buildScanIndex == 0) ? joinNode.getLeftKeys() : joinNode.getRightKeys();
      if (buildKeys.isEmpty()) return RuntimeFilterPayload.none();

      // Use the first join key for RF (multi-key RF is future work)
      String buildKeyName = buildKeys.get(0).getFieldName();

      // Resolve the probe-side field name/type up-front. The join plan carries alias-suffixed
      // names like "dept_id0" to disambiguate duplicate columns across the two sides; strip the
      // trailing digits so (a) type lookup on the original RelNode matches, and (b) the field
      // name pushed into Lucene on the probe side matches the real index mapping.
      String rawProbeFieldName = extractProbeJoinKeyField(coordinatorFragment, buildScanIndex);
      String probeFieldName =
          rawProbeFieldName == null ? null : rawProbeFieldName.replaceAll("\\d+$", "");
      String probeFieldType = extractProbeJoinKeyType(relNode, probeFieldName);

      Session session = veloxLifecycle.getSession();
      int termsCap = veloxLifecycle.getRuntimeFilterMaxCardinality();
      boolean bloomEnabled =
          veloxLifecycle.isRuntimeFilterBloomEnabled()
              && probeFieldType != null
              && OlapBloomFilter.isSupportedType(probeFieldType);
      int bloomCap = veloxLifecycle.getRuntimeFilterBloomMaxCardinality();

      // Two-stage path: data nodes already built PARTIAL blooms with a coordinator-chosen
      // expectedInsertions. Merge them via bitwise-OR and return a BLOOM payload directly —
      // no need to iterate broadcast rows here.
      if (bloomEnabled
          && partialBloomBytesList != null
          && !partialBloomBytesList.isEmpty()
          && probeFieldName != null
          && probeFieldType != null) {
        List<OlapBloomFilter> partials = new ArrayList<>();
        for (byte[] pb : partialBloomBytesList) {
          if (pb == null || pb.length == 0) continue;
          partials.add(OlapBloomFilter.fromBytes(pb));
        }
        if (!partials.isEmpty()) {
          OlapBloomFilter merged = OlapBloomFilter.merge(partials);
          byte[] bytes = merged.toBytes();
          logger.info(
              "RF extracted for query {}: field={}, kind=BLOOM (two-stage merged from {}"
                  + " partials), {} bytes, setSizeBits={}, hashCount={}",
              queryId,
              buildKeyName,
              partials.size(),
              bytes.length,
              merged.setSizeBits(),
              merged.hashCount());
          return RuntimeFilterPayload.bloom(probeFieldName, probeFieldType, bytes);
        }
      }

      // Streaming build: while under the TERMS cap, keep a distinct-values set. On overflow
      // (bloom enabled), initialize a bloom filter sized for bloomCap, replay seen values into
      // it, and keep inserting. On overflow a second time (past bloomCap) abandon.
      Set<String> distinctValues = new LinkedHashSet<>();
      OlapBloomFilter bloom = null;
      long bloomInserted = 0;
      boolean bloomOverflow = false;

      for (byte[] batch : broadcastData) {
        BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(batch);
        RowVector rowVec = vec.asRowVector();
        RowType rowType = (RowType) rowVec.getType();

        int keyColIndex = rowType.getNames().indexOf(buildKeyName);
        if (keyColIndex < 0) continue;

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try {
          VectorSchemaRoot arrowRoot = Arrow.toArrowVectorSchemaRoot(allocator, rowVec);
          FieldVector fieldVec = arrowRoot.getVector(keyColIndex);
          for (int row = 0; row < arrowRoot.getRowCount(); row++) {
            Object val = fieldVec.getObject(row);
            if (val == null) continue;

            if (bloom == null) {
              // TERMS accumulation phase.
              distinctValues.add(val.toString());
              if (distinctValues.size() > termsCap) {
                if (!bloomEnabled) {
                  logger.info(
                      "RF skipped for query {}: build cardinality {} exceeds terms cap {}"
                          + " and BLOOM disabled/unsupported",
                      queryId,
                      distinctValues.size(),
                      termsCap);
                  arrowRoot.close();
                  return RuntimeFilterPayload.none();
                }
                // Pivot to bloom: size for the full bloom cap, replay existing distinct values.
                bloom = OlapBloomFilter.create(Math.max(bloomCap, distinctValues.size()));
                for (String seen : distinctValues) {
                  BytesRef enc = OlapBloomFilter.encodeKey(seen, probeFieldType);
                  if (enc != null) bloom.add(enc);
                }
                bloomInserted = distinctValues.size();
                distinctValues = null; // free memory
              }
            } else {
              // BLOOM accumulation phase.
              BytesRef enc = OlapBloomFilter.encodeKey(val, probeFieldType);
              if (enc != null) bloom.add(enc);
              bloomInserted++;
              if (bloomInserted > bloomCap) {
                bloomOverflow = true;
                break;
              }
            }
          }
          arrowRoot.close();
        } finally {
          try {
            allocator.close();
          } catch (IllegalStateException ex) {
            logger.debug("Arrow allocator close warning: {}", ex.getMessage());
          }
        }
        if (bloomOverflow) break;
      }

      if (bloomOverflow) {
        logger.info(
            "RF skipped for query {}: build cardinality exceeds bloom cap {}", queryId, bloomCap);
        return RuntimeFilterPayload.none();
      }

      if (bloom != null) {
        byte[] bytes = bloom.toBytes();
        logger.info(
            "RF extracted for query {}: field={}, kind=BLOOM, ~{} values, {} bytes",
            queryId,
            buildKeyName,
            bloomInserted,
            bytes.length);
        if (probeFieldName == null || probeFieldType == null) {
          return RuntimeFilterPayload.none();
        }
        return RuntimeFilterPayload.bloom(probeFieldName, probeFieldType, bytes);
      }

      if (distinctValues == null || distinctValues.isEmpty()) {
        return RuntimeFilterPayload.none();
      }

      logger.info(
          "RF extracted for query {}: field={}, kind=TERMS, {} distinct values",
          queryId,
          buildKeyName,
          distinctValues.size());
      if (probeFieldName == null || probeFieldType == null) {
        // Probe-side field metadata missing — log the extraction outcome but drop the RF, same
        // as the pre-BLOOM behavior. The caller proceeds without a runtime filter.
        return RuntimeFilterPayload.none();
      }
      return RuntimeFilterPayload.terms(
          probeFieldName, probeFieldType, new ArrayList<>(distinctValues));

    } catch (Exception e) {
      logger.warn("Failed to extract runtime filter for query {}: {}", queryId, e.getMessage());
      return RuntimeFilterPayload.none();
    }
  }

  /** Extract the probe-side join key field name from the coordinator fragment's HashJoinNode. */
  private String extractProbeJoinKeyField(PlanFragment coordinatorFragment, int buildScanIndex) {
    HashJoinNode joinNode = findHashJoinNode(coordinatorFragment.getPlanRoot());
    if (joinNode == null) return null;

    // If build is at index 0 (left), probe is right → probe keys = rightKeys
    // If build is at index 1 (right), probe is left → probe keys = leftKeys
    List<FieldAccessTypedExpr> probeKeys =
        (buildScanIndex == 0) ? joinNode.getRightKeys() : joinNode.getLeftKeys();
    if (probeKeys.isEmpty()) return null;
    return probeKeys.get(0).getFieldName();
  }

  /**
   * Determine the OpenSearch field type for the RF field by searching the entire RelNode tree — not
   * just the outer rowType, which only exposes the final SELECT projection. Also accepts qualified
   * names like {@code e.dept_id} by matching on the suffix after the last dot.
   */
  private String extractProbeJoinKeyType(RelNode relNode, String fieldName) {
    if (fieldName == null) return null;
    return findFieldType(relNode, fieldName);
  }

  private String findFieldType(RelNode node, String fieldName) {
    for (RelDataTypeField field : node.getRowType().getFieldList()) {
      String fname = field.getName();
      if (fname.equals(fieldName) || fname.endsWith("." + fieldName)) {
        switch (field.getType().getSqlTypeName()) {
          case INTEGER:
            return "integer";
          case BIGINT:
            return "long";
          case VARCHAR:
          case CHAR:
            return "keyword";
          default:
            return null;
        }
      }
    }
    for (RelNode input : node.getInputs()) {
      String t = findFieldType(input, fieldName);
      if (t != null) return t;
    }
    return null;
  }

  /**
   * Select which side to broadcast for a join. For outer joins, the preserved side must be the
   * probe (reads from local shards); the non-preserved side is broadcast. For INNER joins, the
   * smaller side is broadcast (CostEstimator decides).
   */
  private String selectBroadcastBuildSide(
      PlanFragment coordinatorFragment,
      String leftIndex,
      String rightIndex,
      Map<String, TableStatistics> statsMap) {
    HashJoinNode joinNode = findHashJoinNode(coordinatorFragment.getPlanRoot());
    if (joinNode != null) {
      JoinType joinType = joinNode.getJoinType();
      if (joinType == JoinType.LEFT || joinType == JoinType.LEFT_SEMI_FILTER) {
        return "right";
      }
      if (joinType == JoinType.RIGHT) {
        return "left";
      }
    }
    // INNER or FULL: use cost estimator to pick smaller side (by row count when available)
    CostEstimator estimator =
        new CostEstimator(
            queryScheduler.getClusterService(), veloxLifecycle.getBroadcastMaxShards(), statsMap);
    return estimator.selectBuildSide(leftIndex, rightIndex);
  }

  /** Check if the coordinator fragment's join is INNER (RF is only safe for inner joins). */
  private boolean isInnerJoin(PlanFragment coordinatorFragment) {
    HashJoinNode joinNode = findHashJoinNode(coordinatorFragment.getPlanRoot());
    return joinNode != null && joinNode.getJoinType() == JoinType.INNER;
  }

  /** Find the HashJoinNode in a plan tree. */
  private HashJoinNode findHashJoinNode(PlanNode node) {
    if (node instanceof HashJoinNode) return (HashJoinNode) node;
    for (PlanNode source : node.getSources()) {
      HashJoinNode found = findHashJoinNode(source);
      if (found != null) return found;
    }
    return null;
  }

  // ---- Arrow/Type Helpers ----

  private RowType arrowSchemaToVeloxRowType(Schema schema) {
    List<String> names = new ArrayList<>();
    List<Type> types = new ArrayList<>();
    for (Field field : schema.getFields()) {
      names.add(field.getName());
      types.add(arrowTypeToVeloxType(field));
    }
    return new RowType(names, types);
  }

  private Type arrowTypeToVeloxType(Field field) {
    ArrowType arrowType = field.getType();
    if (arrowType instanceof ArrowType.Bool) {
      return new BooleanType();
    } else if (arrowType instanceof ArrowType.Int) {
      int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
      if (bitWidth <= 32) return new IntegerType();
      return new BigIntType();
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      var precision = ((ArrowType.FloatingPoint) arrowType).getPrecision();
      if (precision == FloatingPointPrecision.SINGLE) {
        return new RealType();
      }
      return new DoubleType();
    } else if (arrowType instanceof ArrowType.Utf8) {
      return new VarCharType();
    } else if (arrowType instanceof ArrowType.Binary) {
      return new VarbinaryType();
    } else if (arrowType instanceof ArrowType.Struct) {
      List<String> childNames = new ArrayList<>();
      List<Type> childTypes = new ArrayList<>();
      for (Field child : field.getChildren()) {
        childNames.add(child.getName());
        childTypes.add(arrowTypeToVeloxType(child));
      }
      return new RowType(childNames, childTypes);
    }
    return new VarbinaryType();
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

    // Collect the declared flat column names — any dot-containing name in the output schema is
    // a flat column (e.g. a join-alias like "d.dept_name", or a user-projected dot-path) and must
    // NOT be split into a nested struct by reconstructStructs. Only dot-paths that came from an
    // OpenSearch object-field scan expansion (where the flat name is NOT in the output schema)
    // should be reassembled into {parent: {child: value}}.
    Set<String> flatOutputNames = new java.util.HashSet<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      flatOutputNames.add(field.getName());
    }

    List<ExprValue> results = new ArrayList<>();
    for (ExecuteFragmentResponse response : responses) {
      if (response.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) {
        throw new RuntimeException("Fragment execution failed: " + response.getErrorMessage());
      }
      byte[] arrowData = response.getResultData();
      if (arrowData != null && arrowData.length > 0) {
        results.addAll(readArrowIpcToExprValues(arrowData, flatOutputNames));
      }
    }

    return new ExecutionEngine.QueryResponse(schema, results, Cursor.None);
  }

  private List<ExprValue> readArrowIpcToExprValues(byte[] arrowIpc, Set<String> flatOutputNames) {
    List<ExprValue> rows = new ArrayList<>();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        List<Field> fields = root.getSchema().getFields();
        int rowCount = root.getRowCount();

        for (int row = 0; row < rowCount; row++) {
          LinkedHashMap<String, Object> flatValues = new LinkedHashMap<>();
          for (int col = 0; col < fields.size(); col++) {
            String name = fields.get(col).getName();
            Object value = extractVectorValue(root.getVector(col), row);
            flatValues.put(name, value);
          }
          // Reconstruct nested structs from flat dot-path columns that are NOT in the output
          // schema as flat names (i.e. came from OpenSearch object-field expansion). Columns
          // whose name appears verbatim in the schema (e.g. join aliases like "d.dept_name")
          // must stay flat.
          LinkedHashMap<String, Object> tupleValues =
              reconstructStructs(flatValues, flatOutputNames);
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

  /**
   * Extract a value from an Arrow vector, handling StructVector (nested objects) without calling
   * getObject() — Arrow's StructVector.getObject() uses JsonStringHashMap which requires
   * jackson-datatype-jsr310 at class init time.
   */
  private Object extractVectorValue(FieldVector vector, int row) {
    if (vector instanceof org.apache.arrow.vector.complex.StructVector) {
      org.apache.arrow.vector.complex.StructVector structVector =
          (org.apache.arrow.vector.complex.StructVector) vector;
      if (structVector.isNull(row)) {
        return null;
      }
      LinkedHashMap<String, Object> structValues = new LinkedHashMap<>();
      for (FieldVector child : structVector.getChildrenFromFields()) {
        structValues.put(child.getName(), extractVectorValue(child, row));
      }
      return structValues;
    }
    // For non-struct vectors, use getObject() (safe — no Jackson dependency).
    // TimeStampMicroVector.getObject returns a LocalDateTime; ExprValueUtils.fromObjectValue
    // already handles LocalDateTime by wrapping in ExprTimestampValue.
    Object value = vector.getObject(row);
    if (value instanceof Text) {
      return value.toString();
    }
    return value;
  }

  /**
   * Reconstruct nested struct objects from flat dot-path columns. Converts flat result columns like
   * {cloud.region: "eu", metrics.size: 100, message: "hi"} into nested structs: {cloud: {region:
   * "eu"}, metrics: {size: 100}, message: "hi"}.
   *
   * <p>Any key present in {@code flatOutputNames} is treated as a flat column (not wrapped). This
   * distinguishes OpenSearch object-field expansions (which we want to re-nest) from join aliases
   * and user-written dot-path projections (which must stay flat).
   */
  @SuppressWarnings("unchecked")
  private LinkedHashMap<String, Object> reconstructStructs(
      LinkedHashMap<String, Object> flat, Set<String> flatOutputNames) {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : flat.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (flatOutputNames != null && flatOutputNames.contains(key)) {
        // Schema declares this column as flat — don't split on the dot.
        result.put(key, value);
        continue;
      }
      if (key.contains(".")) {
        // Dot-path field: nest into parent struct
        String[] parts = key.split("\\.", 2);
        String parent = parts[0];
        String child = parts[1];
        LinkedHashMap<String, Object> struct =
            (LinkedHashMap<String, Object>)
                result.computeIfAbsent(parent, k -> new LinkedHashMap<String, Object>());
        // Recursively handle multi-level nesting (e.g., log.file.path)
        if (child.contains(".")) {
          LinkedHashMap<String, Object> childFlat = new LinkedHashMap<>();
          childFlat.put(child, value);
          LinkedHashMap<String, Object> childStruct = reconstructStructs(childFlat, null);
          for (Map.Entry<String, Object> ce : childStruct.entrySet()) {
            Object existing = struct.get(ce.getKey());
            if (existing instanceof LinkedHashMap) {
              ((LinkedHashMap<String, Object>) existing)
                  .putAll((LinkedHashMap<String, Object>) ce.getValue());
            } else {
              struct.put(ce.getKey(), ce.getValue());
            }
          }
        } else {
          struct.put(child, value);
        }
      } else {
        result.put(key, value);
      }
    }
    return result;
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

  public boolean isForceVectorize() {
    return veloxLifecycle.isForceVectorize();
  }
}
