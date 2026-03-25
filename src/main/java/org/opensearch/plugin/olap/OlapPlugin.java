/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.transport.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.watcher.ResourceWatcherService;

import org.opensearch.plugin.olap.engine.OlapExecutionExtensionImpl;
import org.opensearch.plugin.olap.engine.VeloxExecutionEngine;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.transport.ExecuteFragmentAction;
import org.opensearch.plugin.olap.transport.TransportExecuteFragmentAction;

/**
 * OpenSearch OLAP plugin that integrates Apache Velox for vectorized analytical query execution.
 *
 * <p>This plugin co-works with the SQL plugin ({@code opensearch-sql}). The SQL plugin handles
 * SQL/PPL parsing and Calcite RelNode generation. This plugin provides an alternative execution
 * engine that converts RelNodes to Velox plans and executes them via the native C++ Velox engine.
 *
 * <p>Integration flow:
 * <pre>
 * [SQL Plugin]                              [OLAP Plugin]
 * SQL/PPL → Calcite Analyzer → RelNode  →  VeloxPlanConverter → PlanFragmenter
 *                                           → QueryScheduler → Data Nodes
 *                                           → Lucene DocValues → Arrow → Velox
 *                                           → Results back to SQL Plugin
 * </pre>
 *
 * <p>The SQL plugin discovers this plugin's execution engine via the
 * {@link OlapExecutionExtensionImpl} extension point using OpenSearch's
 * {@code ExtensiblePlugin} mechanism.
 */
public class OlapPlugin extends Plugin implements ActionPlugin {

    private VeloxLifecycleService veloxLifecycleService;
    private QueryScheduler queryScheduler;
    private VeloxExecutionEngine veloxExecutionEngine;

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
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.veloxLifecycleService = new VeloxLifecycleService(environment.settings());
        this.queryScheduler = new QueryScheduler(clusterService, threadPool);

        // TransportService is injected via Guice into TransportExecuteFragmentAction.
        // For the VeloxExecutionEngine on the coordinator, we need it to dispatch fragments.
        // It will be wired when the first query arrives via the extension.
        // For now, create the engine with a deferred transport reference.
        this.veloxExecutionEngine = new VeloxExecutionEngine(
            veloxLifecycleService, queryScheduler, null
        );

        // Wire the execution engine into the extension point for SQL plugin discovery
        OlapExecutionExtensionImpl.setEngine(veloxExecutionEngine);

        return Arrays.asList(veloxLifecycleService, queryScheduler, veloxExecutionEngine);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(
            new ActionHandler<>(ExecuteFragmentAction.INSTANCE, TransportExecuteFragmentAction.class)
        );
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
