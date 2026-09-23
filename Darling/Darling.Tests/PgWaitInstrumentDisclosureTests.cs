/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The read side of #3604: every surface that serves PostgreSQL waits discloses which of the three
/// instruments fed it, the service tier carries its floor caveat, and the service-side pieces the sampler
/// arm depends on (the connector's probe, the worker's detach, the connection names it excludes) agree with
/// the collector's declarations.
/// </summary>
public sealed class PgWaitInstrumentDisclosureTests
{
    private static readonly DateTime T0 = new(2026, 9, 18, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingPgWaitSamplingReader.PgWaitSamplingRow Row(string type, string evt, long samples, int periodMs) => new(
        EventType: type, Event: evt, QueryId: 7, SampleCount: samples, EstimatedWaitMs: samples * periodMs,
        BackendCount: 1, CounterReset: false, CaptureTime: T0);

    private static JsonElement Build(DarlingPgWaitSamplingReader.WaitInstrumentState? instrument) =>
        JsonDocument.Parse(DarlingMcpPgWaitSamplingTools.BuildWaitSamplingJson(
            "srv", 24,
            new DarlingPgWaitSamplingReader.PgWaitSamplingPage([Row("Lock", "relation", 30, 1000)], 30),
            limit: 20, instrument)).RootElement;

    /* ───────────────────────── the sampled read ───────────────────────── */

    [Fact]
    public void ServiceTier_IsNamedAndCarriesTheFloorCaveat()
    {
        var root = Build(new DarlingPgWaitSamplingReader.WaitInstrumentState(PgWaitInstrument.ServiceSampled, T0));

        Assert.Equal("service_sampled", root.GetProperty("instrument").GetString());
        Assert.Equal(PgWaitInstrument.ServiceSampledCaveat, root.GetProperty("instrument_note").GetString());
        Assert.Contains("FLOOR", root.GetProperty("instrument_note").GetString(), StringComparison.Ordinal);
        Assert.Contains("distinct backends seen in the LAST window", root.GetProperty("note").GetString(), StringComparison.Ordinal);
        Assert.Equal(T0, root.GetProperty("instrument_recorded_at").GetDateTime().ToUniversalTime());

        /* #3645 review: the tally is persisted collector state, reloaded every cycle, so a SERVICE restart does
           not reset the series — the note must say what does (cap eviction, arm reversion) and not claim what
           does not. Head 3 shipped the false claim; this is what stops it coming back. */
        var note = root.GetProperty("note").GetString()!;
        Assert.Contains("SURVIVES a service restart", note, StringComparison.Ordinal);
        Assert.DoesNotContain("resets when the service restarts", note, StringComparison.Ordinal);
        Assert.Contains("500-key cap", note, StringComparison.Ordinal);
    }

    [Fact]
    public void ExtensionTier_IsNamedWithoutTheFloorCaveat()
    {
        var root = Build(new DarlingPgWaitSamplingReader.WaitInstrumentState(PgWaitInstrument.ExtensionSampled, T0));

        Assert.Equal("extension_sampled", root.GetProperty("instrument").GetString());
        Assert.Contains("10 ms", root.GetProperty("instrument_note").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("FLOOR", root.GetProperty("instrument_note").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("LAST window", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    /// <summary>A store written before the arm existed, or a token a future arm introduces, reads as unknown
    /// — never as a grain the caller might act on.</summary>
    [Fact]
    public void NoRecordedInstrument_OrAnUnknownToken_ReadsAsUnknown()
    {
        Assert.Equal("unknown", Build(null).GetProperty("instrument").GetString());
        Assert.Equal(JsonValueKind.Null, Build(null).GetProperty("instrument_recorded_at").ValueKind);

        var foreign = Build(new DarlingPgWaitSamplingReader.WaitInstrumentState("kernel_traced", T0));
        Assert.Equal("unknown", foreign.GetProperty("instrument").GetString());
        Assert.Contains("profile_period_ms", foreign.GetProperty("instrument_note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheEmptyArm_StatesTheFloorOnlyOnTheServiceTier()
    {
        Assert.Contains(PgWaitInstrument.ServiceSampledCaveat,
            DarlingMcpPgWaitSamplingTools.DescribeInstrumentForEmpty(
                new DarlingPgWaitSamplingReader.WaitInstrumentState(PgWaitInstrument.ServiceSampled, T0)), StringComparison.Ordinal);
        Assert.Equal(string.Empty,
            DarlingMcpPgWaitSamplingTools.DescribeInstrumentForEmpty(
                new DarlingPgWaitSamplingReader.WaitInstrumentState(PgWaitInstrument.ExtensionSampled, T0)));
        Assert.Contains("No collection cycle has recorded", DarlingMcpPgWaitSamplingTools.DescribeInstrumentForEmpty(null), StringComparison.Ordinal);
    }

    /// <summary>The instrument is read off the collector's own state row, under the collector's own name and
    /// key — so a rename of either fails here rather than silently reading nothing forever.</summary>
    [Fact]
    public void TheInstrumentSqlReadsTheCollectorsOwnStateRow()
    {
        Assert.Contains("FROM collector_state", DarlingPgWaitSamplingReader.InstrumentSql, StringComparison.Ordinal);
        Assert.Contains($"collector_name = '{PgWaitSamplingCollector.Instance.Name}'", DarlingPgWaitSamplingReader.InstrumentSql, StringComparison.Ordinal);
        Assert.Contains($"state_key = '{PgWaitSamplingCollector.InstrumentStateKey}'", DarlingPgWaitSamplingReader.InstrumentSql, StringComparison.Ordinal);
    }

    /* ───────────────────────── the Aurora read ───────────────────────── */

    [Fact]
    public void TheAuroraRead_DisclosesEngineCumulative()
    {
        var json = DarlingMcpPgWaitTools.BuildWaitStatsJson(
            "srv", 24,
            new DarlingPgWaitReader.PgWaitStatsPage(
                [new DarlingPgWaitReader.PgWaitRow("IO", "DataFileRead", TotalWaits: 10, TotalWaitTimeMs: 5_000, AvgWaitTimeMs: 500)],
                5_000),
            limit: 20);
        var root = JsonDocument.Parse(json).RootElement;

        Assert.Equal("engine_cumulative", root.GetProperty("instrument").GetString());
        Assert.Contains("get_pg_wait_sampling", root.GetProperty("instrument_note").GetString(), StringComparison.Ordinal);
    }

    /* ───────────────────────── descriptions and instructions ───────────────────────── */

    private static string ToolDescription(Type toolType, string toolName) =>
        toolType.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName)
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

    [Fact]
    public void BothToolDescriptions_NameTheThreeTiers()
    {
        /* #3898 D3 re-pointed get_pg_wait_sampling's half deliberately: extension_sampled / service_sampled /
           FLOOR are guardrail facts (misreading the instrument misreads every number in the answer), so they
           are asserted against the SERVED head; the "Aurora native > ... > service sampler" ordering is
           narrative and stays wherever the tail carries it. get_pg_wait_stats is a different lane's tool and
           is not yet converted, so its half is unchanged and still reads the raw literal. */
        var sampling = ToolDescription(typeof(DarlingMcpPgWaitSamplingTools), "get_pg_wait_sampling");
        var (samplingHead, samplingTail) = McpToolGuide.Split(sampling);
        Assert.Contains("extension_sampled", samplingHead, StringComparison.Ordinal);
        Assert.Contains("service_sampled", samplingHead, StringComparison.Ordinal);
        Assert.Contains("FLOOR", samplingHead, StringComparison.Ordinal);
        Assert.NotNull(samplingTail);
        Assert.Contains("Aurora native > pg_wait_sampling extension > service sampler", samplingTail, StringComparison.Ordinal);
        /* The old opening claimed the extension as the ONLY source; it is one of two now. */
        Assert.DoesNotContain("from the pg_wait_sampling extension. This is", sampling, StringComparison.Ordinal);

        var stats = ToolDescription(typeof(DarlingMcpPgWaitTools), "get_pg_wait_stats");
        Assert.Contains("engine_cumulative", stats, StringComparison.Ordinal);
        Assert.Contains("get_pg_wait_sampling", stats, StringComparison.Ordinal);
    }

    [Fact]
    public void TheInstructions_NameTheThreeTiersAndTheOrder()
    {
        var text = DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty);
        Assert.Contains("`engine_cumulative`", text, StringComparison.Ordinal);
        Assert.Contains("`extension_sampled`", text, StringComparison.Ordinal);
        Assert.Contains("`service_sampled`", text, StringComparison.Ordinal);
        Assert.Contains("Aurora native > the `pg_wait_sampling` extension > the service-side sampler", text, StringComparison.Ordinal);
        Assert.Contains("FLOOR, not parity", text, StringComparison.Ordinal);
    }

    /* ───────────────────────── the service-side seams ───────────────────────── */

    /// <summary>The sampler excludes this service's own backends by application_name, and the names it
    /// excludes must be the names the connector presents — the Collectors assembly cannot reference the
    /// service, so the agreement is pinned rather than referenced.</summary>
    [Fact]
    public void TheExcludedApplicationNames_AreTheOnesTheConnectorPresents()
    {
        Assert.Equal(MonitoredServerConnection.RemediationApplicationName, PgWaitSamplingCollector.ServiceRemediationApplicationName);

        var connector = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "MonitoredServerConnection.cs");
        Assert.Contains($"ApplicationName = \"{PgWaitSamplingCollector.ServiceApplicationName}\"", connector, StringComparison.Ordinal);
    }

    [Fact]
    public void TheConnectProbe_AsksPgExtensionForTheModule_AndFlowsToTheTargetInfo()
    {
        Assert.Contains("pg_extension", DarlingServerConnector.PostgresWaitSamplingProbeQueryText, StringComparison.Ordinal);
        Assert.Contains("'pg_wait_sampling'", DarlingServerConnector.PostgresWaitSamplingProbeQueryText, StringComparison.Ordinal);

        var probe = new ConnectionProbeResult(
            Success: true, MajorVersion: 0, EngineEdition: 0, EngineEditionDescription: null,
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null,
            Engine: CollectorTargetEngine.PostgreSql, PostgresMajorVersion: 18, PostgresVersionNum: 180001,
            HasPgWaitSamplingExtension: true);
        Assert.True(probe.ToTargetInfo().HasPgWaitSamplingExtension);
        Assert.False((probe with { HasPgWaitSamplingExtension = false }).ToTargetInfo().HasPgWaitSamplingExtension);
    }

    /// <summary>Detached from the sequential body, behind the generic gate, on a tier the detach policy
    /// allows — see SweepBodyDetachPolicyTests for the invariant; this pins the predicate itself.</summary>
    [Fact]
    public void TheCollectorIsDetached_ByItsOwnDeclaredName()
    {
        Assert.True(DarlingWorker.IsPgWaitSamplingCollector(PgWaitSamplingCollector.Instance.Name));
        Assert.True(DarlingWorker.IsPgWaitSamplingCollector("PG_WAIT_SAMPLING"));
        Assert.False(DarlingWorker.IsPgWaitSamplingCollector("pg_wait_stats"));
    }
}
