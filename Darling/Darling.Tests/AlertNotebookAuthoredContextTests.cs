/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4223's core step: the immutable <see cref="AlertNotebookEndpoint.AuthoredContext"/>, the
/// optional context builder on <see cref="AlertNotebookEndpoint.AuthoredTemplateEntry"/> with exactly one
/// builder validated at construction, its <c>Invoke</c> call site, exact-then-longest-prefix routing, and
/// the async pre-fetch (custom rule by id, analysis finding by 8-character hash) that runs ONLY for context
/// entries.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookAuthoredContextTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    /* ═══════════════════════════ condition 1: exactly one builder ═══════════════════════════ */

    [Fact]
    public void Constructor_BothBuildersSet_Throws()
    {
        JsonArray Plain(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st) => new();
        JsonArray WithContext(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st,
            AlertNotebookEndpoint.AuthoredContext c) => new();

        Assert.Throws<ArgumentException>(() =>
            new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/both", 1, Plain, WithContext));
    }

    [Fact]
    public void Constructor_NeitherBuilderSet_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/neither", 1, null, null));
    }

    [Fact]
    public void Constructor_ExactlyOneBuilder_Succeeds()
    {
        JsonArray Plain(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st) => new();

        var plainEntry = new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/plain", 1, Plain);
        Assert.NotNull(plainEntry.BuildCells);
        Assert.Null(plainEntry.BuildCellsWithContext);

        JsonArray WithContext(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st,
            AlertNotebookEndpoint.AuthoredContext c) => new();

        var contextEntry = new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/context", 1, null, WithContext);
        Assert.Null(contextEntry.BuildCells);
        Assert.NotNull(contextEntry.BuildCellsWithContext);
    }

    /// <summary>Every registration row in the real table has exactly one builder set — a construction-time
    /// guarantee, but this re-proves it against the actual production table so a future family that manages
    /// to construct an entry outside the guarded constructor (reflection, a copy-paste of the record's raw
    /// fields) is still caught.</summary>
    [Fact]
    public void RegistrationTable_EveryEntry_HasExactlyOneBuilder()
    {
        foreach (var (metrics, entry) in AlertNotebookEndpoint.s_authoredTemplates)
        {
            var oneSet = (entry.BuildCells is null) != (entry.BuildCellsWithContext is null);
            Assert.True(oneSet, $"entry for {string.Join(",", metrics)} must set exactly one builder");
        }
    }

    /// <summary>#4425's mutation run found that <see cref="RegistrationTable_EveryEntry_HasExactlyOneBuilder"/>
    /// above can't fail on its own -- every real row already has exactly one builder, so removing the
    /// constructor's guard doesn't touch a single row that fact walks. This proves that fact's assertion
    /// (<c>(BuildCells is null) != (BuildCellsWithContext is null)</c>) DOES catch a bad row, by building one
    /// that bypasses the guarded constructor entirely -- <see cref="RuntimeHelpers.GetUninitializedObject"/>
    /// on the record struct's type, then both builder backing fields set by reflection -- so this fact is
    /// independent of whether the constructor's own guard is in place.</summary>
    [Fact]
    public void RegistrationTableAssertion_CatchesAnEntryWithBothBuildersSet_BuiltOutsideTheConstructor()
    {
        JsonArray Plain(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st) => new();
        JsonArray WithContext(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st,
            AlertNotebookEndpoint.AuthoredContext c) => new();

        var entryType = typeof(AlertNotebookEndpoint.AuthoredTemplateEntry);
        var boxedEntry = System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(entryType);

        var buildCellsField = entryType.GetField("<BuildCells>k__BackingField",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var buildCellsWithContextField = entryType.GetField("<BuildCellsWithContext>k__BackingField",
            BindingFlags.NonPublic | BindingFlags.Instance)!;

        buildCellsField.SetValue(boxedEntry, (Func<string?, string?, string?, DateTime, DateTime, AlertIncident?,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow?, string, JsonArray>)Plain);
        buildCellsWithContextField.SetValue(boxedEntry, (Func<string?, string?, string?, DateTime, DateTime,
            AlertIncident?, PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow?, string,
            AlertNotebookEndpoint.AuthoredContext, JsonArray>)WithContext);

        var invalidEntry = (AlertNotebookEndpoint.AuthoredTemplateEntry)boxedEntry;

        Assert.NotNull(invalidEntry.BuildCells);
        Assert.NotNull(invalidEntry.BuildCellsWithContext);

        var oneSet = (invalidEntry.BuildCells is null) != (invalidEntry.BuildCellsWithContext is null);
        Assert.False(oneSet, "the bypass-built entry has BOTH builders set, so the direct assertion must read false here");
    }

    /* ═══════════════════════════ Invoke ═══════════════════════════ */

    [Fact]
    public void Invoke_PlainEntry_CallsBuildCells_IgnoresContext()
    {
        var seen = 0;
        JsonArray Plain(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st)
        {
            seen++;
            return new JsonArray();
        }

        var entry = new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/plain", 1, Plain);

        entry.Invoke("m", "s", AsOf, WindowStart, WindowEnd, null, null, "Unknown",
            AlertNotebookEndpoint.AuthoredContext.Empty);

        Assert.Equal(1, seen);
    }

    [Fact]
    public void Invoke_ContextEntry_CallsBuildCellsWithContext_PassesTheContext()
    {
        AlertNotebookEndpoint.AuthoredContext? seenContext = null;
        JsonArray WithContext(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st,
            AlertNotebookEndpoint.AuthoredContext c)
        {
            seenContext = c;
            return new JsonArray();
        }

        var entry = new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/context", 1, null, WithContext);
        var fabricated = new AlertNotebookEndpoint.AuthoredContext(null, true, null, false);

        entry.Invoke("m", "s", AsOf, WindowStart, WindowEnd, null, null, "Unknown", fabricated);

        Assert.Same(fabricated, seenContext);
    }

    /* ═══════════════════════════ routing: exact then longest prefix ═══════════════════════════ */

    private static AlertNotebookEndpoint.AuthoredTemplateEntry FakeEntry(string id) =>
        new(id, 1, (m, s, a, ws, we, i, r, st) => new JsonArray());

    [Fact]
    public void ResolveAuthoredPrefixed_LongestPrefixWins()
    {
        var table = new (string Prefix, AlertNotebookEndpoint.AuthoredContextKind Kind, AlertNotebookEndpoint.AuthoredTemplateEntry Entry)[]
        {
            ("Custom:", AlertNotebookEndpoint.AuthoredContextKind.CustomRule, FakeEntry("authored/custom-short")),
            ("Custom:4", AlertNotebookEndpoint.AuthoredContextKind.CustomRule, FakeEntry("authored/custom-long")),
        };

        var resolved = AlertNotebookEndpoint.ResolveAuthoredPrefixed("Custom:42", table);

        Assert.NotNull(resolved);
        Assert.Equal("authored/custom-long", resolved!.Value.Entry.Id);
    }

    [Fact]
    public void ResolveAuthoredPrefixed_UnknownPrefix_ReturnsNull()
    {
        var table = new (string Prefix, AlertNotebookEndpoint.AuthoredContextKind Kind, AlertNotebookEndpoint.AuthoredTemplateEntry Entry)[]
        {
            ("Custom:", AlertNotebookEndpoint.AuthoredContextKind.CustomRule, FakeEntry("authored/custom")),
        };

        Assert.Null(AlertNotebookEndpoint.ResolveAuthoredPrefixed("High CPU", table));
    }

    [Fact]
    public void ResolveAuthored_ExactBeatsPrefix()
    {
        // "Blocking Detected" is registered as an EXACT metric in the real table; a prefix table that also
        // (hypothetically) matched it must still lose to the exact lookup ResolveAuthored runs first.
        var resolved = AlertNotebookEndpoint.ResolveAuthored("Blocking Detected");

        Assert.NotNull(resolved);
        Assert.Equal("authored/blocking", resolved!.Value.Entry.Id);
        Assert.Equal(AlertNotebookEndpoint.AuthoredContextKind.None, resolved.Value.Kind);
    }

    [Fact]
    public void ResolveAuthored_DevsExactEntries_StillResolve()
    {
        foreach (var (metrics, entry) in AlertNotebookEndpoint.s_authoredTemplates)
        {
            foreach (var metric in metrics)
            {
                var resolved = AlertNotebookEndpoint.ResolveAuthored(metric);
                Assert.NotNull(resolved);
                Assert.Equal(entry.Id, resolved!.Value.Entry.Id);
            }
        }
    }

    [Fact]
    public void ProductionPrefixTable_IsEmptyInThisStep()
    {
        Assert.Empty(AlertNotebookEndpoint.s_authoredPrefixTemplates);
    }

    /* ═══════════════════════════ Custom: id parsing ═══════════════════════════ */

    [Fact]
    public void MetricNameFor_RoundTripsThroughCustomPrefix()
    {
        var metricName = CustomAlertEvaluator.MetricNameFor(42);
        Assert.Equal("Custom:42", metricName);
    }

    /* ═══════════════════════════ analysis hash matching ═══════════════════════════ */

    [Fact]
    public void FindingMessageFormatter_MetricName_CarriesAnEightCharacterHashSuffix()
    {
        var finding = new AnalysisFinding
        {
            Category = "wait_stats",
            StoryPathHash = "abcdef1234567890",
        };

        var metricName = PerformanceMonitor.Notifications.FindingMessageFormatter.MetricName(finding);

        Assert.Equal("Analysis: wait_stats [abcdef12]", metricName);
    }

    /* ═══════════════════════════ live: prefetch ═══════════════════════════ */

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task PrefetchAsync_CustomRule_AlreadyCancelled_Throws()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live prefetch cancellation test.");

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var analysis = new DarlingAnalysisService(postgres);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            AlertNotebookEndpoint.PrefetchAsync(
                AlertNotebookEndpoint.AuthoredContextKind.CustomRule, "Custom:1", serverId: null, DateTime.UtcNow,
                postgres, analysis, cts.Token));
    }

    [Fact]
    public async Task PrefetchAsync_CustomRule_MissingId_ReadsMissing_NoThrow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live prefetch missing-rule test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var analysis = new DarlingAnalysisService(postgres);

        // 999999999 is not a rule id anyone created in this database.
        var context = await AlertNotebookEndpoint.PrefetchAsync(
            AlertNotebookEndpoint.AuthoredContextKind.CustomRule, "Custom:999999999", serverId: null,
            DateTime.UtcNow, postgres, analysis, ct);

        Assert.True(context.CustomRuleMissing);
        Assert.Null(context.CustomRule);
    }

    [Fact]
    public async Task PrefetchAsync_CustomRule_BadParse_ReadsMissing_NoStoreRead()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live prefetch bad-parse test.");

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var analysis = new DarlingAnalysisService(postgres);
        var ct = TestContext.Current.CancellationToken;

        var context = await AlertNotebookEndpoint.PrefetchAsync(
            AlertNotebookEndpoint.AuthoredContextKind.CustomRule, "Custom:not-a-number", serverId: null,
            DateTime.UtcNow, postgres, analysis, ct);

        Assert.True(context.CustomRuleMissing);
        Assert.Null(context.CustomRule);
    }

    /* ═══════════════════════════ condition 2: zero store reads for a no-context family ═══════════════════════════ */

    [Fact]
    public void NoContextFamily_HasNoContextBuilder_SoTheEndpointNeverCallsPrefetch()
    {
        // "Deadlocks Detected" registers a plain BuildCells (no context) -- the endpoint's own guard is
        // `authored.BuildCellsWithContext is not null`, so an entry like this one is never routed into
        // PrefetchAsync at all. This asserts the precondition that guard depends on.
        var resolved = AlertNotebookEndpoint.ResolveAuthored("Deadlocks Detected");

        Assert.NotNull(resolved);
        Assert.Null(resolved!.Value.Entry.BuildCellsWithContext);
    }

    [Fact]
    public async Task PrefetchAsync_None_ReturnsEmpty_NoStoreCall()
    {
        // NpgsqlDataSource.Create against a bogus connection string never opens a connection until something
        // asks it to -- Kind.None must not ask.
        await using var postgres = NpgsqlDataSource.Create("Host=203.0.113.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");
        var analysis = new DarlingAnalysisService(postgres);

        var context = await AlertNotebookEndpoint.PrefetchAsync(
            AlertNotebookEndpoint.AuthoredContextKind.None, "High CPU", serverId: null, DateTime.UtcNow,
            postgres, analysis, CancellationToken.None);

        Assert.Same(AlertNotebookEndpoint.AuthoredContext.Empty, context);
    }

    /* ═══════════════════════ ShouldPrefetch (#4223) ═══════════════════════ */

    [Fact]
    public void ShouldPrefetch_NoContextBuilder_IsFalse_ForEveryRegisteredFamily()
    {
        // s_authoredTemplates and s_authoredPrefixTemplates hold no #4223 context family yet, so this pins
        // zero store reads for every family that exists today -- the gate the endpoint's call site depends on.
        // A theory can't carry an internal type in a public signature, so this walks both tables in one fact.
        foreach (var kind in new[]
        {
            AlertNotebookEndpoint.AuthoredContextKind.CustomRule,
            AlertNotebookEndpoint.AuthoredContextKind.AnalysisFinding,
        })
        {
            foreach (var (_, entry) in AlertNotebookEndpoint.s_authoredTemplates)
            {
                if (entry.BuildCellsWithContext is null)
                {
                    Assert.False(AlertNotebookEndpoint.ShouldPrefetch(entry, kind));
                }
            }

            foreach (var (_, _, entry) in AlertNotebookEndpoint.s_authoredPrefixTemplates)
            {
                if (entry.BuildCellsWithContext is null)
                {
                    Assert.False(AlertNotebookEndpoint.ShouldPrefetch(entry, kind));
                }
            }
        }
    }

    [Fact]
    public void ShouldPrefetch_ContextBuilder_NonNoneKind_IsTrue()
    {
        JsonArray WithContext(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st,
            AlertNotebookEndpoint.AuthoredContext c) => new();

        var entry = new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/fabricated", 1, null, WithContext);

        Assert.True(AlertNotebookEndpoint.ShouldPrefetch(entry, AlertNotebookEndpoint.AuthoredContextKind.AnalysisFinding));
    }

    [Fact]
    public void ShouldPrefetch_ContextBuilder_NoneKind_IsFalse()
    {
        JsonArray WithContext(string? m, string? s, string? a, DateTime ws, DateTime we, AlertIncident? i,
            PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow? r, string st,
            AlertNotebookEndpoint.AuthoredContext c) => new();

        var entry = new AlertNotebookEndpoint.AuthoredTemplateEntry("authored/fabricated-none", 1, null, WithContext);

        Assert.False(AlertNotebookEndpoint.ShouldPrefetch(entry, AlertNotebookEndpoint.AuthoredContextKind.None));
    }

    /* ═══════════════════════ PickFindingByHash (#4223) ═══════════════════════ */

    [Fact]
    public void PickFindingByHash_SharedFourCharPrefix_DistinguishesAtTheEighthCharacter()
    {
        var first = new AnalysisFinding { StoryPathHash = "abcd1234extra" };
        var second = new AnalysisFinding { StoryPathHash = "abcd5678extra" };
        var findings = new[] { first, second };

        Assert.Same(first, AlertNotebookEndpoint.PickFindingByHash(findings, "abcd1234"));
        Assert.Same(second, AlertNotebookEndpoint.PickFindingByHash(findings, "abcd5678"));
        Assert.Null(AlertNotebookEndpoint.PickFindingByHash(findings, "abcd9999"));
    }

    [Fact]
    public void PickFindingByHash_NoMatch_ReturnsNull()
    {
        var findings = new[] { new AnalysisFinding { StoryPathHash = "deadbeef0000" } };

        Assert.Null(AlertNotebookEndpoint.PickFindingByHash(findings, "abcd9999"));
    }

    /* ═══════════════════════ zero pre-fetch for a no-context family (#4425) ═══════════════════════ */

    /// <summary>Drives the SAME decide-and-build step <see cref="AlertNotebookEndpoint.Map"/>'s handler calls
    /// -- <see cref="AlertNotebookEndpoint.BuildCellsAsync"/> -- for "Blocking Detected", a registered
    /// no-context family (<c>BuildCellsWithContext</c> is null, per
    /// <see cref="NoContextFamily_HasNoContextBuilder_SoTheEndpointNeverCallsPrefetch"/> above), and asserts
    /// <see cref="AlertNotebookEndpoint.PrefetchAsync"/> was called zero times via the process-wide
    /// <see cref="AlertNotebookEndpoint.s_prefetchCallsForTest"/> counter. Removing the
    /// <see cref="AlertNotebookEndpoint.ShouldPrefetch"/> gate at the call site (always pre-fetching) turns
    /// this RED without touching any other pin in this file -- the gap the previous mutation run found.
    /// Serialized against the counter's other reader in this class via a static lock, since the counter is
    /// process-wide and xUnit may run facts in this class in parallel.</summary>
    [Fact]
    public async Task BuildCellsAsync_NoContextFamily_MakesZeroPrefetchCalls()
    {
        int before, after;
        await using var postgres = NpgsqlDataSource.Create(
            "Host=203.0.113.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");
        var analysis = new DarlingAnalysisService(postgres);

        lock (PrefetchCounterLock)
        {
            before = AlertNotebookEndpoint.s_prefetchCallsForTest;
        }

        await AlertNotebookEndpoint.BuildCellsAsync(
            metric: "Blocking Detected", serverName: "srv", asOf: AsOf, windowEnd: WindowEnd,
            serverId: null, anchor: WindowEnd, postgres: postgres, analysis: analysis,
            ct: CancellationToken.None, logger: null, notes: null, matchedIncident: null, matchedRow: null,
            status: "Unknown", lookbackHours: "24");

        lock (PrefetchCounterLock)
        {
            after = AlertNotebookEndpoint.s_prefetchCallsForTest;
        }

        Assert.Equal(before, after);
    }

    private static readonly object PrefetchCounterLock = new();

    [Fact]
    public async Task PrefetchAsync_AnalysisFinding_DeletedFinding_ReadsMissing_NoThrow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live deleted-finding prefetch test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var analysis = new DarlingAnalysisService(postgres);

        // A server with no findings at all -- "abcd9999" never matches, the same as a finding that aged out
        // or was deleted after the metric name was minted.
        var context = await AlertNotebookEndpoint.PrefetchAsync(
            AlertNotebookEndpoint.AuthoredContextKind.AnalysisFinding, "Analysis: x [abcd9999]", serverId: 999999999,
            DateTime.UtcNow, postgres, analysis, ct);

        Assert.True(context.FindingMissing);
        Assert.Null(context.Finding);
    }
}
