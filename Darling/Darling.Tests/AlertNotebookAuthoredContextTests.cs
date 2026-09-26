/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
}
