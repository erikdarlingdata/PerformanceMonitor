/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */

/// <summary>
/// <c>get_service_starts</c> and the build markers on the store trend reads (#9415) against a real server. The starts are written through
/// <see cref="ServiceStartHistory"/>, the way the service writes them, and read back through the tools.
/// </summary>
[Collection("live-postgres")]
public sealed class ServiceStartsReadLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* Kind Utc, so the store keeps these instants as written: ServiceStartHistory.Naive reads an unspecified kind as local time. */
    private static readonly DateTime Now = DateTime.SpecifyKind(ServiceStartHistory.Naive(DateTime.UtcNow), DateTimeKind.Utc);

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source)> StartAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the service start read's live tests (each mints its own scratch database).");
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString));
    }

    private static ServiceStartRow Start(
        Guid id, string host, string build, double hoursAgo, int? before = 303, int? after = 303,
        long errors = 0, long warns = 0, IReadOnlyList<string>? firstErrors = null) =>
        new(id, host, build, "abc1234", Now.AddHours(-hoursAgo), before, after, Now.AddHours(-hoursAgo).AddSeconds(20), null,
            Now.AddHours(-hoursAgo).AddMinutes(5), errors, warns, firstErrors ?? [], [], errors, warns, null, null);

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    [Fact]
    public async Task TwoStarts_ReadNewestFirst_WithBuild_SchemaChange_Rungs_AndTheCurrentFlag()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var older = Guid.NewGuid();
            var newer = Guid.NewGuid();
            await ServiceStartHistory.UpsertStartAsync(source, Start(older, "svc-a", "3.13.0", 30, before: 306, after: 307, errors: 2, warns: 1, firstErrors: ["boom one", "boom two"]), ct);
            await ServiceStartHistory.InsertRungsAsync(source,
            [
                new ServiceStartRung(older, 307, "service-start-history", 1, Now.AddHours(-30), 1200, "failed", "relation is locked"),
                new ServiceStartRung(older, 307, "service-start-history", 2, Now.AddHours(-30).AddSeconds(5), 850, "applied", null),
            ], ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start(newer, "svc-a", "3.14.0", 2), ct);

            var root = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct));

            Assert.Equal(2, root.GetProperty("starts_returned").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());
            var starts = root.GetProperty("starts").EnumerateArray().ToArray();
            Assert.Equal(["3.14.0", "3.13.0"], starts.Select(s => s.GetProperty("build").GetString()).ToArray());
            Assert.True(starts[0].GetProperty("current").GetBoolean());
            Assert.False(starts[1].GetProperty("current").GetBoolean());
            Assert.Equal(0, starts[0].GetProperty("rungs").GetArrayLength());

            var first = starts[1];
            Assert.Equal(306, first.GetProperty("schema_version_before").GetInt32());
            Assert.Equal(307, first.GetProperty("schema_version_after").GetInt32());
            Assert.Equal(2, first.GetProperty("error_count").GetInt64());
            Assert.Equal(["boom one", "boom two"], first.GetProperty("first_errors").EnumerateArray().Select(e => e.GetString()).ToArray());
            var rungs = first.GetProperty("rungs").EnumerateArray().ToArray();
            Assert.Equal(["failed", "applied"], rungs.Select(r => r.GetProperty("outcome").GetString()).ToArray());
            Assert.Equal([1200L, 850L], rungs.Select(r => r.GetProperty("elapsed_ms").GetInt64()).ToArray());
            Assert.Equal("relation is locked", rungs[0].GetProperty("error_text").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheHostFilter_KeepsOneHostsStarts_AndFlagsEachHostsNewestStartCurrent()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-a", "3.14.0", 10), ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-b", "3.14.0", 8), ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-a", "3.14.0", 6), ct);

            var all = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct)).GetProperty("starts").EnumerateArray().ToArray();
            Assert.Equal(["svc-a", "svc-b", "svc-a"], all.Select(s => s.GetProperty("service_host").GetString()).ToArray());
            Assert.Equal([true, true, false], all.Select(s => s.GetProperty("current").GetBoolean()).ToArray());

            var only = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, service_host: "svc-b", cancellationToken: ct));
            Assert.Equal("svc-b", only.GetProperty("service_host").GetString());
            Assert.Equal(1, only.GetProperty("starts_returned").GetInt32());

            var none = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, service_host: "svc-z", cancellationToken: ct));
            Assert.Equal("empty", none.GetProperty("status").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheLimit_CutsToTheNewestStarts_SaysItWasCut_AndRefusesAValueOutOfRange()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            for (var i = 0; i < 12; i++)
            {
                await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-a", $"3.14.{i}", 24 - i), ct);
            }

            var five = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, limit: 5, cancellationToken: ct));
            Assert.Equal(5, five.GetProperty("starts_returned").GetInt32());
            Assert.True(five.GetProperty("truncated").GetBoolean());
            Assert.Equal(["3.14.11", "3.14.10", "3.14.9", "3.14.8", "3.14.7"],
                five.GetProperty("starts").EnumerateArray().Select(s => s.GetProperty("build").GetString()).ToArray());

            var byDefault = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct));
            Assert.Equal(DarlingMcpServiceStartTools.DefaultLimit, byDefault.GetProperty("starts_returned").GetInt32());
            Assert.True(byDefault.GetProperty("truncated").GetBoolean());

            var wide = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, limit: DarlingMcpServiceStartTools.MaxLimit, cancellationToken: ct));
            Assert.Equal(12, wide.GetProperty("starts_returned").GetInt32());
            Assert.False(wide.GetProperty("truncated").GetBoolean());

            foreach (var bad in new[] { 0, DarlingMcpServiceStartTools.MaxLimit + 1 })
            {
                Assert.Equal("invalid", Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, limit: bad, cancellationToken: ct)).GetProperty("status").GetString());
            }

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AReplyOverTheResponseBudget_LeavesTheOldestStartsOffWhole_AndEachStartSaysItsOwnCuts()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var longLine = new string('x', ServiceStartHistory.LineLength);
            var lines = Enumerable.Range(0, ServiceStartHistory.FirstLinesKept).Select(_ => longLine).ToList();
            for (var i = 0; i < 12; i++)
            {
                var id = Guid.NewGuid();
                var row = Start(id, "svc-a", $"3.14.{i}", 24 - i, errors: 40, warns: 40) with { FirstErrors = lines, FirstWarns = lines };
                await ServiceStartHistory.UpsertStartAsync(source, row, ct);
                await ServiceStartHistory.InsertRungsAsync(source,
                    Enumerable.Range(1, 30).Select(v => new ServiceStartRung(id, v, "rung " + v, 1, Now.AddHours(-24 + i), 100 + v, MigrationRungOutcome.Failed, longLine)).ToList(), ct);
            }

            var json = await DarlingMcpServiceStartTools.GetServiceStarts(source, limit: DarlingMcpServiceStartTools.DefaultLimit, cancellationToken: ct);
            Assert.True(System.Text.Encoding.UTF8.GetByteCount(json) <= PerformanceMonitor.Common.McpResponseBudget.DefaultBytes);
            var reply = Parse(json);
            var returned = reply.GetProperty("starts_returned").GetInt32();
            Assert.InRange(returned, 1, DarlingMcpServiceStartTools.DefaultLimit - 1);
            Assert.True(reply.GetProperty("truncated").GetBoolean());
            Assert.True(reply.GetProperty("budget_truncated").GetBoolean());
            Assert.NotNull(reply.GetProperty("budget_note").GetString());

            /* Newest first: the starts kept are the newest, and the oldest are the ones left off. */
            var builds = reply.GetProperty("starts").EnumerateArray().Select(s => s.GetProperty("build").GetString()).ToArray();
            Assert.Equal(Enumerable.Range(0, returned).Select(k => $"3.14.{11 - k}"), builds);

            var first = reply.GetProperty("starts")[0];
            Assert.True(first.GetProperty("log_lines_truncated").GetBoolean());
            Assert.True(first.GetProperty("rungs_truncated").GetBoolean());
            Assert.Equal(DarlingMcpServiceStartTools.MaxRungsPerStart, first.GetProperty("rungs").GetArrayLength());
            Assert.Equal(20, first.GetProperty("rungs_omitted").GetInt32());

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreWithoutTheTables_AnswersPrecondition_AndAStoreWithNoStartsAnswersEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var empty = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct));
            Assert.Equal("empty", empty.GetProperty("status").GetString());

            await ExecAsync(source, "DROP TABLE collect.service_start_rungs; DROP TABLE collect.service_starts;", ct);
            var missing = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct));
            Assert.Equal("precondition", missing.GetProperty("status").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheStoreTrendReads_MarkWhereTheBuildChanged_NotARestartOnTheSameBuild_AndSayWhenThereIsNoHistory()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            /* The first start sits before the 24 hour window: the change to 3.14.0 inside it is still marked, against that earlier build. */
            await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-a", "3.13.0", 100), ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-a", "3.14.0", 20), ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-a", "3.14.0", 10), ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start(Guid.NewGuid(), "svc-b", "3.14.0", 5), ct);

            await ExecAsync(source,
                "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes, enabled_server_count) VALUES (timezone('UTC', now()) - interval '5 minutes', 'darling', '"
                + StoreSelfMetrics.StoreObjectKind + "', 1000000, 1)", ct);
            await ExecAsync(source,
                "INSERT INTO collect.store_statement_captures VALUES (timezone('UTC', now()) - interval '2 hours', 3600, NULL, 0, 0, 3, 3, 0, 'ok')", ct);
            await ExecAsync(source,
                "INSERT INTO collect.store_statement_history VALUES (timezone('UTC', now()) - interval '2 hours', 3600, 'owner', 77, 4, 40, 1, 2, 3, 4, 9.5, false, false, false)", ct);

            var metrics = Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(source, days_back: 1, cancellationToken: ct));
            var history = Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 24, cancellationToken: ct));
            foreach (var answer in new[] { metrics, history })
            {
                var changes = answer.GetProperty("build_changes").EnumerateArray().ToArray();
                var change = Assert.Single(changes);
                Assert.Equal("svc-a", change.GetProperty("service_host").GetString());
                Assert.Equal("3.13.0", change.GetProperty("from_build").GetString());
                Assert.Equal("3.14.0", change.GetProperty("to_build").GetString());
                Assert.Equal(Now.AddHours(-20), DateTime.Parse(change.GetProperty("at").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal), TimeSpan.FromSeconds(1));
            }

            /* A window that starts after the change has no marker: the two later starts are the same build. */
            var narrow = Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 12, cancellationToken: ct));
            Assert.Equal(0, narrow.GetProperty("build_changes").GetArrayLength());

            await ExecAsync(source, "DROP TABLE collect.service_start_rungs; DROP TABLE collect.service_starts;", ct);
            var without = Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 24, cancellationToken: ct));
            Assert.Equal(JsonValueKind.Null, without.GetProperty("build_changes").ValueKind);
            Assert.Equal(DarlingMcpServiceStartTools.BuildChangesUnavailableNote, without.GetProperty("build_changes_note").GetString());
            Assert.Equal(1, without.GetProperty("statements_returned").GetInt32());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }
}
