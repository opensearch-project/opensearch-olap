/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.execution;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.memory.AllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.session.Session;

/**
 * Manages the lifecycle of the Velox native engine within the OpenSearch process.
 *
 * <p>Velox4j must be initialized exactly once per JVM. This service handles:
 * <ul>
 *   <li>Loading the native libraries</li>
 *   <li>Initializing the Velox engine with configuration</li>
 *   <li>Creating and managing a shared MemoryManager</li>
 *   <li>Providing Session instances for query execution</li>
 *   <li>Cleanup on shutdown</li>
 * </ul>
 */
public class VeloxLifecycleService implements Closeable {

    private static final Logger logger = LogManager.getLogger(VeloxLifecycleService.class);

    public static final Setting<Boolean> OLAP_ENABLED = Setting.boolSetting(
        "plugins.velox.enabled", true, Setting.Property.NodeScope
    );

    public static final Setting<Long> VELOX_MEMORY_LIMIT = Setting.longSetting(
        "plugins.velox.memory_limit_bytes", 4L * 1024 * 1024 * 1024, // 4 GB default
        0L, Setting.Property.NodeScope
    );

    public static final Setting<Integer> VELOX_NUM_THREADS = Setting.intSetting(
        "plugins.velox.num_threads", 4, 1, Setting.Property.NodeScope
    );

    private volatile boolean enabled;
    private volatile MemoryManager memoryManager;
    private volatile Session session;
    private volatile boolean initialized = false;

    public VeloxLifecycleService(Settings settings) {
        this.enabled = OLAP_ENABLED.get(settings);

        if (enabled) {
            try {
                initializeVelox(settings);
            } catch (Exception e) {
                logger.warn("Velox engine unavailable on this platform, OLAP plugin will be disabled: {}",
                    e.getMessage());
                this.enabled = false;
            }
        }
    }

    private void initializeVelox(Settings settings) {
        long memoryLimit = VELOX_MEMORY_LIMIT.get(settings);
        int numThreads = VELOX_NUM_THREADS.get(settings);

        logger.info("Initializing Velox engine: memory_limit={}MB, threads={}",
            memoryLimit / (1024 * 1024), numThreads);

        // Configure before initialization
        Velox4j.configure("max_memory", String.valueOf(memoryLimit));
        Velox4j.configure("num_threads", String.valueOf(numThreads));

        // Initialize the native engine (loads JNI libraries, registers functions)
        Velox4j.initialize();

        // Create a shared memory manager
        this.memoryManager = Velox4j.newMemoryManager(AllocationListener.NOOP);

        // Create a session for query execution
        this.session = Velox4j.newSession(memoryManager);

        this.initialized = true;
        logger.info("Velox engine initialized successfully");
    }

    public Session getSession() {
        if (!initialized) {
            throw new IllegalStateException("Velox engine not initialized");
        }
        return session;
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public static List<Setting<?>> getSettings() {
        return List.of(OLAP_ENABLED, VELOX_MEMORY_LIMIT, VELOX_NUM_THREADS);
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
