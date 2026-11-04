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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7432: <c>freezing_is_keeping_up</c> used to be <c>age &lt; window peak</c>. The age of a database climbs between freezes, so a
/// healthy server (or an idle one with a steady age) answered false for every database, which reads as autovacuum losing.
/// The answer is now judged only once a freeze is due (age at or above <c>autovacuum_freeze_max_age</c>).
/// </summary>
public sealed class PgWraparoundFreezeStateTests
{
    private const long Setting = 200_000_000;

    [Fact]
    public void BelowTheFreezeLine_NoFreezeIsDue_AtThePeakOrNot()
    {
        /* A steady age: the case that read "not keeping up" on a healthy server. */
        var steady = DarlingMcpPgWraparoundTools.FreezeState(216_211, 216_211, Setting);
        Assert.Equal("freeze_not_due", steady.State);
        Assert.Null(steady.KeepingUp);

        /* A climbing age, and a sawtooth that fell below the line: still nothing due, and no verdict either way. */
        Assert.Null(DarlingMcpPgWraparoundTools.FreezeState(150_000_000, 150_000_000, Setting).KeepingUp);
        Assert.Equal("freeze_not_due", DarlingMcpPgWraparoundTools.FreezeState(10_000_000, 190_000_000, Setting).State);
        Assert.Equal("freeze_not_due", DarlingMcpPgWraparoundTools.FreezeState(Setting - 1, Setting - 1, Setting).State);
    }

    [Fact]
    public void AtOrPastTheFreezeLine_AnAgeThatFellInsideTheWindow_IsKeepingUp()
    {
        var fell = DarlingMcpPgWraparoundTools.FreezeState(Setting + 5_000_000, Setting + 60_000_000, Setting);
        Assert.Equal("freezing_keeping_up", fell.State);
        Assert.True(fell.KeepingUp);

        /* Exactly at the line counts as due. */
        Assert.True(DarlingMcpPgWraparoundTools.FreezeState(Setting, Setting + 1, Setting).KeepingUp);
    }

    [Fact]
    public void AtOrPastTheFreezeLine_AnAgeStillAtItsPeak_IsTheOnlyProblemState()
    {
        var stuck = DarlingMcpPgWraparoundTools.FreezeState(Setting + 15_000_000, Setting + 15_000_000, Setting);
        Assert.Equal("freeze_due_age_not_falling", stuck.State);
        Assert.False(stuck.KeepingUp);

        /* One sample (or a peak of 0 from a null) cannot show a fall, so with a freeze due it reads as not falling. */
        Assert.False(DarlingMcpPgWraparoundTools.FreezeState(Setting, Setting, Setting).KeepingUp);
        Assert.False(DarlingMcpPgWraparoundTools.FreezeState(Setting + 1, 0, Setting).KeepingUp);
    }

    [Fact]
    public void ASettingThatWasNotCollected_IsUnknown_NotAVerdict()
    {
        var none = DarlingMcpPgWraparoundTools.FreezeState(900_000_000, 900_000_000, 0);
        Assert.Equal("setting_not_collected", none.State);
        Assert.Null(none.KeepingUp);
    }

    [Fact]
    public void EachStateHasItsOwnWords()
    {
        var words = new[]
        {
            DarlingMcpPgWraparoundTools.FreezeState(1, 1, Setting).Text,
            DarlingMcpPgWraparoundTools.FreezeState(Setting + 1, Setting + 2, Setting).Text,
            DarlingMcpPgWraparoundTools.FreezeState(Setting + 1, Setting + 1, Setting).Text,
            DarlingMcpPgWraparoundTools.FreezeState(1, 1, 0).Text,
        };
        Assert.Equal(4, words.Distinct().Count());
    }
}

/// <summary>The tool's reply, end to end: two databases at the same age, a steady age, and a window that ends in the past.</summary>
[Collection("live-postgres")]
public sealed class PgWraparoundFreezeStateLiveTests
{
    private const int ServerId = -743201;
    private const string ServerName = "freeze-state-7432";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheReply_NamesTheFreezeState_PerDatabase_AndForAPastWindow_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live freeze-state test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* The reported case: a steady age far under the line. */
            foreach (var m in new[] { 50, 30, 10 })
                await SeedAsync(connection, ct, now.AddMinutes(-m), "steady_idle", 216_211);

            /* The same latest age (210M), different histories. */
            foreach (var m in new[] { 50, 30, 10 })
                await SeedAsync(connection, ct, now.AddMinutes(-m), "twin_stuck", 210_000_000);
            foreach (var (m, age) in new[] { (50, 230_000_000L), (30, 215_000_000L), (10, 210_000_000L) })
                await SeedAsync(connection, ct, now.AddMinutes(-m), "twin_fell", age);

            /* Stuck at two and three hours ago, then frozen an hour ago: a read ending 90 minutes ago must judge that window only. */
            await SeedAsync(connection, ct, now.AddMinutes(-180), "froze_later", 205_000_000);
            await SeedAsync(connection, ct, now.AddMinutes(-120), "froze_later", 215_000_000);
            await SeedAsync(connection, ct, now.AddMinutes(-60), "froze_later", 20_000_000);

            var current = await ReadAsync(postgres, 2, null);
            Assert.Equal(JsonValueKind.Null, Prop(current, "steady_idle", "freezing_is_keeping_up").ValueKind);
            Assert.Equal("freeze_not_due", State(current, "steady_idle"));
            Assert.Equal("freeze_due_age_not_falling", State(current, "twin_stuck"));
            Assert.False(Prop(current, "twin_stuck", "freezing_is_keeping_up").GetBoolean());
            Assert.Equal("freezing_keeping_up", State(current, "twin_fell"));
            Assert.True(Prop(current, "twin_fell", "freezing_is_keeping_up").GetBoolean());
            Assert.Equal(
                Prop(current, "twin_stuck", "frozen_xid_age").GetInt64(), Prop(current, "twin_fell", "frozen_xid_age").GetInt64());
            Assert.Equal("freeze_not_due", State(current, "froze_later"));

            var past = await ReadAsync(postgres, 3, WebDataStartNote.FormatWindowEnd(DateTime.SpecifyKind(now.AddMinutes(-90), DateTimeKind.Utc)));
            Assert.Equal("freeze_due_age_not_falling", State(past, "froze_later"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task<JsonElement> ReadAsync(NpgsqlDataSource postgres, int hours, string? asOf)
    {
        var json = await DarlingMcpPgWraparoundTools.GetPgWraparoundRisk(
            postgres, ServerName, hours, asOf, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        Assert.Contains("\"databases\"", json);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private static JsonElement Prop(JsonElement reply, string database, string name) =>
        reply.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database_name").GetString() == database).GetProperty(name);

    private static string State(JsonElement reply, string database) => Prop(reply, database, "freeze_state").GetString()!;

    private static Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string database, long xidAge) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wraparound_stats
    (collection_id, collection_time, server_id, server_name, database_name, frozen_xid_age, min_multixid_age,
     autovacuum_freeze_max_age, autovacuum_multixact_freeze_max_age, pct_toward_emergency_vacuum, pct_toward_wraparound,
     pct_toward_multixact_emergency, pct_toward_multixact_wraparound, xids_remaining, multixids_remaining, allows_connections)
VALUES ($1, $2, $3, $4, $5, $6, 1000, 200000000, 400000000, $7, 0, 0, 0, $8, 2147482648, TRUE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, database, xidAge,
            xidAge * 100.0 / 200_000_000, 2_147_483_648L - xidAge);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_wraparound_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
