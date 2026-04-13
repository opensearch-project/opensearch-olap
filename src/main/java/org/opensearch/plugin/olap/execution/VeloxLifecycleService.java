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

  private volatile boolean enabled;
  private volatile boolean mppEnabled;
  private volatile int broadcastMaxShards;
  private volatile int shufflePartitions;
  private volatile int segmentParallelism;
  private volatile int taskMaxRetries;
  private volatile MemoryManager memoryManager;
  private volatile Session session;
  private volatile boolean initialized = false;

  public VeloxLifecycleService(Settings settings) {
    this.enabled = OLAP_ENABLED.get(settings);
    this.mppEnabled = MPP_ENABLED.get(settings);
    this.broadcastMaxShards = BROADCAST_MAX_SHARDS.get(settings);
    this.shufflePartitions = SHUFFLE_PARTITIONS.get(settings);
    this.segmentParallelism = SEGMENT_PARALLELISM.get(settings);
    this.taskMaxRetries = TASK_MAX_RETRIES.get(settings);

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
      // Configure before initialization
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

  public static List<Setting<?>> getSettings() {
    return List.of(
        OLAP_ENABLED,
        VELOX_MEMORY_LIMIT,
        VELOX_NUM_THREADS,
        MPP_ENABLED,
        BROADCAST_MAX_SHARDS,
        SHUFFLE_PARTITIONS,
        SEGMENT_PARALLELISM,
        TASK_MAX_RETRIES);
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
