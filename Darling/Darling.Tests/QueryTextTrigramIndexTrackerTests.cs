/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7674: the process-wide tracker is read by get_store_metrics and by the all-servers find_queries_by_text refusal from process
/// start, before the worker has migrated the store. Until the option is known the answer must say so, never "the option is off".
/// </summary>
[Collection("darling-config-env")]
public sealed class QueryTextTrigramIndexTrackerTests
{
    private static readonly TrigramCatalogState ValidIndex = new(
        TableExists: true, IndexValid: true, IndexSizeBytes: 1048576, ExtensionSchema: "public", ExtensionAvailable: true, BuildRunning: false);

    [Fact]
    public void AnUnconfiguredTracker_WithAValidIndex_IsNotDropPending_AndOnceConfiguredItClassifiesByTheOption()
    {
        var tracker = new QueryTextTrigramIndexTracker();

        Assert.Equal(TrigramIndexStatus.Unconfigured, QueryTextTrigramIndex.Classify(ValidIndex, tracker.Read()));
        Assert.Equal(
            TrigramIndexStatus.Unconfigured,
            QueryTextTrigramIndex.Classify(ValidIndex with { IndexValid = null, IndexSizeBytes = null }, tracker.Read()));

        tracker.Configure(true);
        Assert.Equal(TrigramIndexStatus.Ready, QueryTextTrigramIndex.Classify(ValidIndex, tracker.Read()));

        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);
        Assert.Equal(TrigramIndexStatus.DropPending, QueryTextTrigramIndex.Classify(ValidIndex, off.Read()));
    }

    [Fact]
    public void TheUnconfiguredState_NeverTellsTheOperatorToTurnTheOptionOn()
    {
        var health = new TrigramIndexHealth(
            TrigramIndexStatus.Unconfigured, false, true, true, 1048576, false, null, true, true, 0, null, null, null, null, null);

        var refusal = DarlingMcpFindQueriesByTextTools.AllServersRefusal(health);
        var note = QueryTextTrigramIndex.Note(health);

        Assert.DoesNotContain("Set queryTextTrigramIndexEnabled", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("is off", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("dropping", refusal, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("is off", note, StringComparison.Ordinal);
        Assert.DoesNotContain("drops", note, StringComparison.Ordinal);
        Assert.Contains("not read darling.json yet", refusal, StringComparison.Ordinal);
    }

    /// <summary>
    /// The real worker, started on a config file that sets the option and is refused at validation (no postgres section), before
    /// any store work. The shared tracker is the one the MCP host reads, and it must carry the option by then.
    /// </summary>
    [Fact]
    public async Task TheWorker_ConfiguresTheSharedTracker_TheMomentTheFileLoads_BeforeAnyStoreWork()
    {
        var dir = Directory.CreateTempSubdirectory("darling-trigram-configure-").FullName;
        var path = Path.Combine(dir, "darling.json");
        File.WriteAllText(path, "{ \"queryTextTrigramIndexEnabled\": true }");
        var previous = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Environment.SetEnvironmentVariable("DARLING_CONFIG", path);
        try
        {
            var worker = new DarlingWorker(
                NullLogger<DarlingWorker>.Instance,
                NullLoggerFactory.Instance,
                new McpRuntimeState(),
                new WebRuntimeState(),
                new MonitoredServerRegistryState(),
                new CollectorRuntimeState(),
                new WebTlsCertificateState(),
                new BaselineCache(),
                new ReadLatencyAccumulator());
            var method = typeof(DarlingWorker).GetMethod("ExecuteAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

            await (Task)method.Invoke(worker, [CancellationToken.None])!;

            var snapshot = QueryTextTrigramIndexTracker.Shared.Read();
            Assert.True(snapshot.Configured);
            Assert.True(snapshot.Enabled);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previous);
            Directory.Delete(dir, recursive: true);
        }
    }
}
