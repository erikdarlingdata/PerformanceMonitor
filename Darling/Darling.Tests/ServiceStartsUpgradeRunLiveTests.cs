/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */

/// <summary>
/// <c>get_service_starts</c> names the upgrade run before each start and reads a run the store still holds as <c>in_progress</c> by what happened after it (#9902),
/// against a real server. The starts are written through <see cref="ServiceStartHistory"/> and the runs into <c>collect.upgrade_runs</c> the way the pickup leaves them.
/// </summary>
[Collection("live-postgres")]
public sealed class ServiceStartsUpgradeRunLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* Kind Utc, so the store keeps these instants as written: ServiceStartHistory.Naive reads an unspecified kind as local time. */
    private static readonly DateTime Now = DateTime.SpecifyKind(ServiceStartHistory.Naive(DateTime.UtcNow), DateTimeKind.Utc);

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source)> StartAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the upgrade run read's live tests (each mints its own scratch database).");
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString));
    }

    private static ServiceStartRow Start(string build, double hoursAgo) =>
        new(Guid.NewGuid(), "svc-a", build, "abc1234", Now.AddHours(-hoursAgo), 312, 312, Now.AddHours(-hoursAgo).AddSeconds(20), null,
            Now.AddHours(-hoursAgo).AddMinutes(5), 0, 0, [], [], 0, 0, null, null);

    /* A run row as the pickup leaves it: the file's own columns, with the state the file last said. */
    private static async Task AddRunAsync(NpgsqlDataSource source, Guid id, string state, string? targetBuild, double startedHoursAgo, double? endedHoursAgo, CancellationToken ct)
    {
        await using var command = source.CreateCommand(
            "INSERT INTO collect.upgrade_runs (run_id, state, tool_version, target_build, started_at, ended_at, recorded_at, updated_at) VALUES ($1, $2, '3.14.0', $3, $4, $5, $4, $4)");
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = id });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = state });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)targetBuild ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = ServiceStartHistory.Naive(Now.AddHours(-startedHoursAgo)) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = endedHoursAgo is { } ended ? ServiceStartHistory.Naive(Now.AddHours(-ended)) : DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> StoredStateAsync(NpgsqlDataSource source, Guid id, CancellationToken ct)
    {
        await using var command = source.CreateCommand("SELECT state FROM collect.upgrade_runs WHERE run_id = $1");
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = id });
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<JsonElement[]> StartsAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        var root = JsonDocument.Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct)).RootElement.Clone();
        return root.GetProperty("starts").EnumerateArray().ToArray();
    }

    [Fact]
    public async Task ARunWhoseEndWriteNeverLanded_ThenAStartOnItsTargetBuild_ReadsEndedWithoutAResult_AndTheStoredRowKeepsInProgress()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var run = Guid.NewGuid();
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.13.0", 10), ct);
            await AddRunAsync(source, run, UpgradeRuns.InProgress, "3.14.0", 3, null, ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.14.0", 2.9), ct);

            var starts = await StartsAsync(source, ct);

            Assert.Equal(["3.14.0", "3.13.0"], starts.Select(s => s.GetProperty("build").GetString()).ToArray());
            var named = starts[0].GetProperty("upgrade_run");
            Assert.Equal(run, named.GetProperty("run_id").GetGuid());
            Assert.Equal(UpgradeRuns.EndedWithoutResult, named.GetProperty("state").GetString());
            Assert.Equal(UpgradeRuns.InProgress, named.GetProperty("recorded_state").GetString());
            Assert.Equal("3.14.0", named.GetProperty("target_build").GetString());
            Assert.Equal(JsonValueKind.Null, named.GetProperty("ended_at").ValueKind);
            Assert.True(named.GetProperty("start_on_target_build").GetBoolean());
            Assert.NotNull(named.GetProperty("started_at").GetString());

            /* The start before the run began names no run. */
            Assert.Equal(JsonValueKind.Null, starts[1].GetProperty("upgrade_run").ValueKind);

            /* The row keeps what the file said. */
            Assert.Equal(UpgradeRuns.InProgress, await StoredStateAsync(source, run, ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ATargetBuildWithACommitSuffix_IsTheBuildAStartRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            /* The Windows script records the exe's file version, which can carry the commit; a start's build never does. */
            await AddRunAsync(source, Guid.NewGuid(), UpgradeRuns.InProgress, "3.14.0-nightly.20261124.730+abc1234", 3, null, ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.14.0-nightly.20261124.730", 2.9), ct);

            var named = (await StartsAsync(source, ct)).Single().GetProperty("upgrade_run");

            Assert.Equal(UpgradeRuns.EndedWithoutResult, named.GetProperty("state").GetString());
            Assert.True(named.GetProperty("start_on_target_build").GetBoolean());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ANewerRun_MakesTheOlderInProgressRunReadSuperseded_WhileTheNewerRunStaysInProgress()
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
            await AddRunAsync(source, older, UpgradeRuns.InProgress, "3.14.0", 5, null, ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.13.0", 4), ct);
            await AddRunAsync(source, newer, UpgradeRuns.InProgress, "3.15.0", 1, null, ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.13.0", 0.5), ct);

            var starts = await StartsAsync(source, ct);

            var afterNewer = starts[0].GetProperty("upgrade_run");
            Assert.Equal(newer, afterNewer.GetProperty("run_id").GetGuid());
            Assert.Equal(UpgradeRuns.InProgress, afterNewer.GetProperty("state").GetString());
            Assert.False(afterNewer.GetProperty("start_on_target_build").GetBoolean());

            var afterOlder = starts[1].GetProperty("upgrade_run");
            Assert.Equal(older, afterOlder.GetProperty("run_id").GetGuid());
            Assert.Equal(UpgradeRuns.Superseded, afterOlder.GetProperty("state").GetString());
            Assert.Equal(UpgradeRuns.InProgress, afterOlder.GetProperty("recorded_state").GetString());
            Assert.False(afterOlder.GetProperty("start_on_target_build").GetBoolean());

            Assert.Equal(UpgradeRuns.InProgress, await StoredStateAsync(source, older, ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ARunThatIsTrulyInProgress_WithNoLaterStartOnItsBuildAndNoNewerRun_StillReadsInProgress()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var run = Guid.NewGuid();
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.13.0", 10), ct);
            await AddRunAsync(source, run, UpgradeRuns.InProgress, "3.14.0", 3, null, ct);
            /* A restart on the old build after the run began is not a start on its target build. */
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.13.0", 1), ct);

            var named = (await StartsAsync(source, ct))[0].GetProperty("upgrade_run");

            Assert.Equal(run, named.GetProperty("run_id").GetGuid());
            Assert.Equal(UpgradeRuns.InProgress, named.GetProperty("state").GetString());
            Assert.Equal(UpgradeRuns.InProgress, named.GetProperty("recorded_state").GetString());
            Assert.False(named.GetProperty("start_on_target_build").GetBoolean());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ARunThatHasAResult_ReadsAsStored_WhateverComesAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var failed = Guid.NewGuid();
            var succeeded = Guid.NewGuid();
            await AddRunAsync(source, failed, UpgradeRuns.Failed, "3.14.0", 6, 5.9, ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.14.0", 5.5), ct);
            await AddRunAsync(source, succeeded, UpgradeRuns.Succeeded, "3.15.0", 2, 1.9, ct);
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.15.0", 1.5), ct);

            var starts = await StartsAsync(source, ct);

            Assert.Equal(UpgradeRuns.Succeeded, starts[0].GetProperty("upgrade_run").GetProperty("state").GetString());
            Assert.NotEqual(JsonValueKind.Null, starts[0].GetProperty("upgrade_run").GetProperty("ended_at").ValueKind);
            Assert.Equal(UpgradeRuns.Failed, starts[1].GetProperty("upgrade_run").GetProperty("state").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreWithoutUpgradeRuns_StillAnswersTheStarts_WithNoRunAndANoteThatSaysWhy()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.14.0", 2), ct);
            await using (var drop = source.CreateCommand("DROP TABLE collect.upgrade_runs"))
            {
                await drop.ExecuteNonQueryAsync(ct);
            }

            var root = JsonDocument.Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct)).RootElement.Clone();

            Assert.Equal(1, root.GetProperty("starts_returned").GetInt32());
            Assert.Equal(DarlingMcpServiceStartTools.UpgradeRunsUnavailableNote, root.GetProperty("upgrade_runs_note").GetString());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("starts")[0].GetProperty("upgrade_run").ValueKind);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreWithUpgradeRuns_SaysNoNote_AndAStartBeforeEveryRunNamesNone()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await ServiceStartHistory.UpsertStartAsync(source, Start("3.13.0", 10), ct);

            var root = JsonDocument.Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct)).RootElement.Clone();

            Assert.Equal(JsonValueKind.Null, root.GetProperty("upgrade_runs_note").ValueKind);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("starts")[0].GetProperty("upgrade_run").ValueKind);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }
}
