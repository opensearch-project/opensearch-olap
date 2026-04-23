/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.memory.AllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.session.Session;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * Manages the lifecycle of the Velox native engine within the OpenSearch process.
 *
 * <p>Velox4j must be initialized exactly once per JVM. This service handles:
 *
 * <ul>
 *   <li>Loading the native libraries
 *   <li>Initializing the Velox engine with configuration
 *   <li>Creating and managing a shared MemoryManager
 *   <li>Providing Session instances for query execution
 *   <li>Cleanup on shutdown
 * </ul>
 */
public class VeloxLifecycleService implements Closeable {

  private static final Logger logger = LogManager.getLogger(VeloxLifecycleService.class);

  public static final Setting<Boolean> OLAP_ENABLED =
      Setting.boolSetting("plugins.velox.enabled", true, Setting.Property.NodeScope);

  public static final Setting<Long> VELOX_MEMORY_LIMIT =
      Setting.longSetting(
          "plugins.velox.memory_limit_bytes",
          4L * 1024 * 1024 * 1024, // 4 GB default
          0L,
          Setting.Property.NodeScope);

  public static final Setting<Integer> VELOX_NUM_THREADS =
      Setting.intSetting("plugins.velox.num_threads", 4, 1, Setting.Property.NodeScope);

  /** Enable MPP (massively parallel processing) join strategies (broadcast + hash shuffle). */
  public static final Setting<Boolean> MPP_ENABLED =
      Setting.boolSetting(
          "plugins.velox.mpp_enabled", false, Setting.Property.NodeScope, Setting.Property.Dynamic);

  /**
   * Max primary shard count for the smaller join side to qualify for broadcast. If the smaller side
   * has more shards than this, hash shuffle is used instead.
   */
  public static final Setting<Integer> BROADCAST_MAX_SHARDS =
      Setting.intSetting(
          "plugins.velox.broadcast_max_shards",
          2,
          1,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Number of shuffle partitions for hash shuffle join. Defaults to 0 which means auto (use the
   * number of data nodes).
   */
  public static final Setting<Integer> SHUFFLE_PARTITIONS =
      Setting.intSetting(
          "plugins.velox.shuffle_partitions",
          0,
          0,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Number of parallel threads for reading Lucene segments within a shard. Each segment is read by
   * a separate thread, producing Arrow batches concurrently into the shared ExternalStreamBridge.
   * Default 4. Set to 1 to disable parallelism.
   */
  public static final Setting<Integer> SEGMENT_PARALLELISM =
      Setting.intSetting(
          "plugins.velox.segment_parallelism",
          4,
          1,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Maximum number of retry attempts per failed task. Retries use replica shards on different nodes
   * when available, or the same node for transient errors. Set to 0 to disable retries.
   */
  public static final Setting<Integer> TASK_MAX_RETRIES =
      Setting.intSetting(
          "plugins.velox.task_max_retries",
          2,
          0,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Enable runtime filter pushdown for broadcast joins. */
  public static final Setting<Boolean> RUNTIME_FILTER_ENABLED =
      Setting.boolSetting(
          "plugins.velox.runtime_filter_enabled",
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Max distinct value count for runtime filter. If build-side join key cardinality exceeds this,
   * RF is skipped (query still works, just without the optimization).
   */
  public static final Setting<Integer> RUNTIME_FILTER_MAX_CARDINALITY =
      Setting.intSetting(
          "plugins.velox.runtime_filter_max_cardinality",
          10000,
          0,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Enable the BLOOM variant of runtime filter. When true, build-side cardinalities above the TERMS
   * cap but below the BLOOM cap produce a bloom filter instead of being abandoned. When false,
   * behavior reverts to pre-BLOOM: the RF is dropped once cardinality exceeds the TERMS cap.
   */
  public static final Setting<Boolean> RUNTIME_FILTER_BLOOM_ENABLED =
      Setting.boolSetting(
          "plugins.velox.runtime_filter_bloom_enabled",
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Upper cap on distinct build-side values for the BLOOM RF. Cardinalities above this are
   * abandoned (no filter emitted). Bounds bloom-filter memory footprint at roughly {@code
   * -N*ln(fpp)/ln(2)^2} bits — at the default 10M insertions and fpp=1%, about 12 MB.
   */
  public static final Setting<Integer> RUNTIME_FILTER_BLOOM_MAX_CARDINALITY =
      Setting.intSetting(
          "plugins.velox.runtime_filter_bloom_max_cardinality",
          10_000_000,
          0,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Enable two-stage distributed BLOOM build. When true, each data node builds a PARTIAL bloom
   * filter over its own build-side rows and ships it back to the coordinator, which merges them via
   * bitwise-OR into a FINAL bloom. When false, the coordinator rebuilds the bloom single-stage by
   * reading the broadcast build data (v1 behavior).
   *
   * <p>Two-stage is cheaper: the coordinator does not need to re-scan build rows, and partial
   * blooms are typically a small fixed-size binary payload (~MB), regardless of build cardinality.
   */
  public static final Setting<Boolean> RUNTIME_FILTER_BLOOM_TWO_STAGE =
      Setting.boolSetting(
          "plugins.velox.runtime_filter_bloom_two_stage",
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * CBO statistics mode. RUNTIME collects row counts from IndicesStatsResponse before optimization.
   * NONE skips statistics collection (uses Calcite defaults).
   */
  public static final Setting<String> CBO_STATISTICS_MODE =
      Setting.simpleString(
          "plugins.velox.cbo_statistics_mode",
          "RUNTIME",
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Force all queries through the vectorized engine, bypassing canVectorize() checks. When enabled,
   * queries that use unsupported types or functions will fail explicitly instead of falling back to
   * the default engine. Intended for integration testing to verify Velox coverage.
   */
  public static final Setting<Boolean> FORCE_VECTORIZE =
      Setting.boolSetting(
          "plugins.velox.force_vectorize",
          false,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---- Backpressure / flow control settings ----

  /**
   * Hard cap on the Arrow allocator used by {@code ExternalStreamBridge} per fragment. Bounds the
   * Java-side Arrow memory held between Lucene reads and Velox consumption. Also drives the feeder
   * soft-watermark gate in {@code LuceneArrowReader}.
   */
  public static final Setting<ByteSizeValue> PER_FRAGMENT_ARROW_BYTES =
      Setting.byteSizeSetting(
          "plugins.velox.per_fragment_arrow_bytes",
          new ByteSizeValue(256, ByteSizeUnit.MB),
          new ByteSizeValue(16, ByteSizeUnit.MB),
          new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Hard cap on the Arrow allocator used by {@code VeloxExecutor} for result serialization. */
  public static final Setting<ByteSizeValue> RESULT_ALLOCATOR_BYTES =
      Setting.byteSizeSetting(
          "plugins.velox.result_allocator_bytes",
          new ByteSizeValue(512, ByteSizeUnit.MB),
          new ByteSizeValue(32, ByteSizeUnit.MB),
          new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Maximum serialized size of a single fragment result (Arrow IPC or native). When exceeded the
   * fragment fails with {@code ResultTooLargeException} (non-retryable) instead of risking OOM.
   */
  public static final Setting<ByteSizeValue> MAX_RESULT_BYTES =
      Setting.byteSizeSetting(
          "plugins.velox.max_result_bytes",
          new ByteSizeValue(1, ByteSizeUnit.GB),
          new ByteSizeValue(64, ByteSizeUnit.MB),
          new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Hard cap on Arrow allocators used by the coordinator path in {@code VeloxExecutionEngine}
   * (FINAL-agg feed, broadcast build, intermediate result deserialization).
   */
  public static final Setting<ByteSizeValue> COORDINATOR_ALLOCATOR_BYTES =
      Setting.byteSizeSetting(
          "plugins.velox.coordinator_allocator_bytes",
          new ByteSizeValue(1, ByteSizeUnit.GB),
          new ByteSizeValue(128, ByteSizeUnit.MB),
          new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Soft watermark for the FINAL-aggregation feed queue on the coordinator. When the
   * coordinator-side Arrow allocator exceeds this, feeders park until it drains.
   */
  public static final Setting<ByteSizeValue> COORDINATOR_QUEUE_BYTES =
      Setting.byteSizeSetting(
          "plugins.velox.coordinator_queue_bytes",
          new ByteSizeValue(256, ByteSizeUnit.MB),
          new ByteSizeValue(32, ByteSizeUnit.MB),
          new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Aggregate cap on all fragment responses accumulated by {@code NodeResultCollector} during a
   * single query. Prevents coordinator heap exhaustion when many large fragments arrive together.
   */
  public static final Setting<ByteSizeValue> COORDINATOR_INFLIGHT_BYTES =
      Setting.byteSizeSetting(
          "plugins.velox.coordinator_inflight_bytes",
          new ByteSizeValue(2, ByteSizeUnit.GB),
          new ByteSizeValue(256, ByteSizeUnit.MB),
          new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Per-partition shuffle buffer cap. Senders that would push over this threshold receive a
   * backpressure response and retry with exponential backoff.
   */
  public static final Setting<ByteSizeValue> SHUFFLE_BUFFER_BYTES =
      Setting.byteSizeSetting(
          "plugins.velox.shuffle_buffer_bytes",
          new ByteSizeValue(512, ByteSizeUnit.MB),
          new ByteSizeValue(64, ByteSizeUnit.MB),
          new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Maximum time a producer will wait for a downstream buffer to drain before giving up. */
  public static final Setting<Integer> BACKPRESSURE_WAIT_MS =
      Setting.intSetting(
          "plugins.velox.backpressure_wait_ms",
          30_000,
          1_000,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Soft watermark as a percentage of the hard cap at which producers start parking. Values below
   * 100 give some headroom between "pause" and "fail".
   */
  public static final Setting<Integer> BACKPRESSURE_WATERMARK_PCT =
      Setting.intSetting(
          "plugins.velox.backpressure_watermark_pct",
          80,
          50,
          100,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---- Co-Routing settings (MPP Phase A) ----

  /**
   * Enable Co-Routing join optimization. When true, binary equi-joins between two co-routed indexes
   * (see {@link #CO_ROUTED_PAIRS}) run shard-local with no shuffle, dominating the
   * BROADCAST/HASH_SHUFFLE strategies when applicable. Requires {@code mpp_enabled=true}.
   */
  public static final Setting<Boolean> CO_ROUTING_ENABLED =
      Setting.boolSetting(
          "plugins.velox.co_routing_enabled",
          false,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /**
   * Registry of co-routed index pairs. Format: {@code "indexA:keyA,indexB:keyB;indexC:keyC,..."} —
   * semicolon-separated entries where each entry is two {@code index:key} tuples joined by a comma.
   * Only joins matching a registered pair (in either left→right or right→left order) are eligible
   * for Co-Routing; all others fall back to BROADCAST/HASH_SHUFFLE/COORDINATOR-CENTRIC.
   *
   * <p>The pair is the user's assertion that both indexes were indexed with {@code _routing} equal
   * to the listed key field <em>and</em> have matching {@code number_of_shards}. Drift produces
   * wrong results; the setting is explicit precisely to avoid silently assuming it.
   */
  public static final Setting<List<String>> CO_ROUTED_PAIRS =
      Setting.listSetting(
          "plugins.velox.co_routed_pairs",
          List.of(),
          s -> s,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  private volatile boolean enabled;
  private volatile boolean forceVectorize;
  private volatile boolean mppEnabled;
  private volatile int broadcastMaxShards;
  private volatile int shufflePartitions;
  private volatile int segmentParallelism;
  private volatile int taskMaxRetries;
  private volatile boolean runtimeFilterEnabled;
  private volatile int runtimeFilterMaxCardinality;
  private volatile boolean runtimeFilterBloomEnabled;
  private volatile int runtimeFilterBloomMaxCardinality;
  private volatile boolean runtimeFilterBloomTwoStage;
  private volatile String cboStatisticsMode;
  private volatile long perFragmentArrowBytes;
  private volatile long resultAllocatorBytes;
  private volatile long maxResultBytes;
  private volatile long coordinatorAllocatorBytes;
  private volatile long coordinatorQueueBytes;
  private volatile long coordinatorInflightBytes;
  private volatile long shuffleBufferBytes;
  private volatile int backpressureWaitMs;
  private volatile int backpressureWatermarkPct;
  private volatile boolean coRoutingEnabled;
  private volatile CoRoutedPairs coRoutedPairs = CoRoutedPairs.EMPTY;
  private volatile MemoryManager memoryManager;
  private volatile Session session;
  private volatile boolean initialized = false;

  public VeloxLifecycleService(Settings settings) {
    this.enabled = OLAP_ENABLED.get(settings);
    this.forceVectorize = FORCE_VECTORIZE.get(settings);
    this.mppEnabled = MPP_ENABLED.get(settings);
    this.broadcastMaxShards = BROADCAST_MAX_SHARDS.get(settings);
    this.shufflePartitions = SHUFFLE_PARTITIONS.get(settings);
    this.segmentParallelism = SEGMENT_PARALLELISM.get(settings);
    this.taskMaxRetries = TASK_MAX_RETRIES.get(settings);
    this.runtimeFilterEnabled = RUNTIME_FILTER_ENABLED.get(settings);
    this.runtimeFilterMaxCardinality = RUNTIME_FILTER_MAX_CARDINALITY.get(settings);
    this.runtimeFilterBloomEnabled = RUNTIME_FILTER_BLOOM_ENABLED.get(settings);
    this.runtimeFilterBloomMaxCardinality = RUNTIME_FILTER_BLOOM_MAX_CARDINALITY.get(settings);
    this.runtimeFilterBloomTwoStage = RUNTIME_FILTER_BLOOM_TWO_STAGE.get(settings);
    this.cboStatisticsMode = CBO_STATISTICS_MODE.get(settings);
    this.perFragmentArrowBytes = PER_FRAGMENT_ARROW_BYTES.get(settings).getBytes();
    this.resultAllocatorBytes = RESULT_ALLOCATOR_BYTES.get(settings).getBytes();
    this.maxResultBytes = MAX_RESULT_BYTES.get(settings).getBytes();
    this.coordinatorAllocatorBytes = COORDINATOR_ALLOCATOR_BYTES.get(settings).getBytes();
    this.coordinatorQueueBytes = COORDINATOR_QUEUE_BYTES.get(settings).getBytes();
    this.coordinatorInflightBytes = COORDINATOR_INFLIGHT_BYTES.get(settings).getBytes();
    this.shuffleBufferBytes = SHUFFLE_BUFFER_BYTES.get(settings).getBytes();
    this.backpressureWaitMs = BACKPRESSURE_WAIT_MS.get(settings);
    this.backpressureWatermarkPct = BACKPRESSURE_WATERMARK_PCT.get(settings);
    this.coRoutingEnabled = CO_ROUTING_ENABLED.get(settings);
    this.coRoutedPairs = CoRoutedPairs.parse(CO_ROUTED_PAIRS.get(settings));

    if (enabled) {
      try {
        initializeVelox(settings);
      } catch (Exception e) {
        logger.warn(
            "Velox engine unavailable on this platform, OLAP plugin will be disabled: {}",
            e.getMessage());
        this.enabled = false;
      }
    }
  }

  private void initializeVelox(Settings settings) {
    long memoryLimit = VELOX_MEMORY_LIMIT.get(settings);
    int numThreads = VELOX_NUM_THREADS.get(settings);

    logger.info(
        "Initializing Velox engine: memory_limit={}MB, threads={}",
        memoryLimit / (1024 * 1024),
        numThreads);

    // OpenSearch uses isolated classloaders for plugins. The thread context
    // classloader may not see the velox4j-repackaged jar that contains the
    // native libraries under velox4j-lib/. Temporarily switch to the classloader
    // that loaded Velox4j so the native library discovery works correctly.
    ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(Velox4j.class.getClassLoader());
    try {
      // Configure before initialization.
      // Use FLINK preset to register both Presto AND Spark scalar functions.
      // The default SPARK preset only registers Spark functions, missing Presto
      // math/string/trig functions (sin, cos, minus, etc.) that PPL UDFs map to.
      Velox4j.configure("velox4j.init.preset", "1"); // 1 = FLINK
      Velox4j.configure("max_memory", String.valueOf(memoryLimit));
      Velox4j.configure("num_threads", String.valueOf(numThreads));

      // Initialize the native engine (loads JNI libraries, registers functions)
      Velox4j.initialize();

      // Create a shared memory manager
      this.memoryManager = Velox4j.newMemoryManager(AllocationListener.NOOP);

      // Create a session for query execution
      this.session = Velox4j.newSession(memoryManager);
    } finally {
      Thread.currentThread().setContextClassLoader(originalCL);
    }

    this.initialized = true;
    logger.info("Velox engine initialized successfully");
  }

  /**
   * Create a new Session for each query execution. Each Session has its own memory pool namespace,
   * avoiding "Leaf child memory pool already exists" collisions when multiple queries run
   * sequentially on the same JVM.
   */
  public Session getSession() {
    if (!initialized) {
      throw new IllegalStateException("Velox engine not initialized");
    }
    return Velox4j.newSession(memoryManager);
  }

  public MemoryManager getMemoryManager() {
    return memoryManager;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isForceVectorize() {
    return forceVectorize;
  }

  public void setForceVectorize(boolean forceVectorize) {
    this.forceVectorize = forceVectorize;
  }

  public boolean isMppEnabled() {
    return mppEnabled;
  }

  public int getBroadcastMaxShards() {
    return broadcastMaxShards;
  }

  public int getShufflePartitions() {
    return shufflePartitions;
  }

  public void setMppEnabled(boolean mppEnabled) {
    this.mppEnabled = mppEnabled;
  }

  public void setBroadcastMaxShards(int broadcastMaxShards) {
    this.broadcastMaxShards = broadcastMaxShards;
  }

  public void setShufflePartitions(int shufflePartitions) {
    this.shufflePartitions = shufflePartitions;
  }

  public int getSegmentParallelism() {
    return segmentParallelism;
  }

  public void setSegmentParallelism(int segmentParallelism) {
    this.segmentParallelism = segmentParallelism;
  }

  public int getTaskMaxRetries() {
    return taskMaxRetries;
  }

  public void setTaskMaxRetries(int taskMaxRetries) {
    this.taskMaxRetries = taskMaxRetries;
  }

  public boolean isRuntimeFilterEnabled() {
    return runtimeFilterEnabled;
  }

  public void setRuntimeFilterEnabled(boolean runtimeFilterEnabled) {
    this.runtimeFilterEnabled = runtimeFilterEnabled;
  }

  public int getRuntimeFilterMaxCardinality() {
    return runtimeFilterMaxCardinality;
  }

  public void setRuntimeFilterMaxCardinality(int runtimeFilterMaxCardinality) {
    this.runtimeFilterMaxCardinality = runtimeFilterMaxCardinality;
  }

  public boolean isRuntimeFilterBloomEnabled() {
    return runtimeFilterBloomEnabled;
  }

  public void setRuntimeFilterBloomEnabled(boolean runtimeFilterBloomEnabled) {
    this.runtimeFilterBloomEnabled = runtimeFilterBloomEnabled;
  }

  public int getRuntimeFilterBloomMaxCardinality() {
    return runtimeFilterBloomMaxCardinality;
  }

  public void setRuntimeFilterBloomMaxCardinality(int runtimeFilterBloomMaxCardinality) {
    this.runtimeFilterBloomMaxCardinality = runtimeFilterBloomMaxCardinality;
  }

  public boolean isRuntimeFilterBloomTwoStage() {
    return runtimeFilterBloomTwoStage;
  }

  public void setRuntimeFilterBloomTwoStage(boolean runtimeFilterBloomTwoStage) {
    this.runtimeFilterBloomTwoStage = runtimeFilterBloomTwoStage;
  }

  public String getCboStatisticsMode() {
    return cboStatisticsMode;
  }

  public void setCboStatisticsMode(String cboStatisticsMode) {
    this.cboStatisticsMode = cboStatisticsMode;
  }

  public boolean isCboEnabled() {
    return "RUNTIME".equalsIgnoreCase(cboStatisticsMode);
  }

  public long getPerFragmentArrowBytes() {
    return perFragmentArrowBytes;
  }

  public void setPerFragmentArrowBytes(ByteSizeValue value) {
    this.perFragmentArrowBytes = value.getBytes();
  }

  public long getResultAllocatorBytes() {
    return resultAllocatorBytes;
  }

  public void setResultAllocatorBytes(ByteSizeValue value) {
    this.resultAllocatorBytes = value.getBytes();
  }

  public long getMaxResultBytes() {
    return maxResultBytes;
  }

  public void setMaxResultBytes(ByteSizeValue value) {
    this.maxResultBytes = value.getBytes();
  }

  public long getCoordinatorAllocatorBytes() {
    return coordinatorAllocatorBytes;
  }

  public void setCoordinatorAllocatorBytes(ByteSizeValue value) {
    this.coordinatorAllocatorBytes = value.getBytes();
  }

  public long getCoordinatorQueueBytes() {
    return coordinatorQueueBytes;
  }

  public void setCoordinatorQueueBytes(ByteSizeValue value) {
    this.coordinatorQueueBytes = value.getBytes();
  }

  public long getCoordinatorInflightBytes() {
    return coordinatorInflightBytes;
  }

  public void setCoordinatorInflightBytes(ByteSizeValue value) {
    this.coordinatorInflightBytes = value.getBytes();
  }

  public long getShuffleBufferBytes() {
    return shuffleBufferBytes;
  }

  public void setShuffleBufferBytes(ByteSizeValue value) {
    this.shuffleBufferBytes = value.getBytes();
  }

  public int getBackpressureWaitMs() {
    return backpressureWaitMs;
  }

  public void setBackpressureWaitMs(int backpressureWaitMs) {
    this.backpressureWaitMs = backpressureWaitMs;
  }

  public int getBackpressureWatermarkPct() {
    return backpressureWatermarkPct;
  }

  public void setBackpressureWatermarkPct(int backpressureWatermarkPct) {
    this.backpressureWatermarkPct = backpressureWatermarkPct;
  }

  /** Soft watermark derived from a hard cap + {@link #getBackpressureWatermarkPct()}. */
  public long softWatermark(long hardCapBytes) {
    return Math.max(1L, hardCapBytes * backpressureWatermarkPct / 100L);
  }

  public boolean isCoRoutingEnabled() {
    return coRoutingEnabled;
  }

  public void setCoRoutingEnabled(boolean coRoutingEnabled) {
    this.coRoutingEnabled = coRoutingEnabled;
  }

  public CoRoutedPairs getCoRoutedPairs() {
    return coRoutedPairs;
  }

  public void setCoRoutedPairs(List<String> raw) {
    this.coRoutedPairs = CoRoutedPairs.parse(raw);
  }

  public static List<Setting<?>> getSettings() {
    return List.of(
        OLAP_ENABLED,
        VELOX_MEMORY_LIMIT,
        VELOX_NUM_THREADS,
        MPP_ENABLED,
        BROADCAST_MAX_SHARDS,
        SHUFFLE_PARTITIONS,
        SEGMENT_PARALLELISM,
        TASK_MAX_RETRIES,
        RUNTIME_FILTER_ENABLED,
        RUNTIME_FILTER_MAX_CARDINALITY,
        RUNTIME_FILTER_BLOOM_ENABLED,
        RUNTIME_FILTER_BLOOM_MAX_CARDINALITY,
        RUNTIME_FILTER_BLOOM_TWO_STAGE,
        CBO_STATISTICS_MODE,
        FORCE_VECTORIZE,
        PER_FRAGMENT_ARROW_BYTES,
        RESULT_ALLOCATOR_BYTES,
        MAX_RESULT_BYTES,
        COORDINATOR_ALLOCATOR_BYTES,
        COORDINATOR_QUEUE_BYTES,
        COORDINATOR_INFLIGHT_BYTES,
        SHUFFLE_BUFFER_BYTES,
        BACKPRESSURE_WAIT_MS,
        BACKPRESSURE_WATERMARK_PCT,
        CO_ROUTING_ENABLED,
        CO_ROUTED_PAIRS);
  }

  @Override
  public void close() throws IOException {
    if (initialized) {
      logger.info("Shutting down Velox engine");
      // Session and MemoryManager are backed by C++ objects
      // that are cleaned up by the Velox engine on shutdown.
      initialized = false;
    }
  }
}
