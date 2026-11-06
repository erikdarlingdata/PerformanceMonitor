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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7878 and #7910: the xmin horizon, autovacuum, replication slot and wraparound reads answer from the collector's newest run only.
/// A holder or a table that cleared stops being written, and a dropped slot or database is missing from the newest capture, so
/// the last stored sample of each must not be reported as the present state. Each test lays down a stored sample, then a LATER
/// successful run in <c>collection_log</c> that did not store it, the shape a cleared state or a drop leaves behind.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgStaleStateLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task Exec(NpgsqlConnection c, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, sql, values);

    private static int Id(string name) => ServerIdHelper.GetDeterministicHashCode(name);

    private static Task RunAsync(string name, string collector, string table, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, [collector], [name], [table], async (c, ds, end) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-30), collector, null, 30, end, [table], TestContext.Current.CancellationToken);
            await body(c, ds, end);
        });

    /// <summary>One successful run of <paramref name="collector"/> that stored its rows at <paramref name="at"/>, logged five seconds later because a run is logged when it ends.</summary>
    private static Task Run(NpgsqlConnection c, string name, string collector, DateTime at) =>
        WindowFloorLiveHarness.SeedRunAsync(c, name, collector, "SUCCESS", at.AddSeconds(5), TestContext.Current.CancellationToken);

    private static string AsOf(DateTime end) => WebDataStartNote.FormatWindowEnd(end);

    private static Task SeedHolder(NpgsqlConnection c, string name, DateTime at, string source, long age, string holder, bool winner) =>
        Exec(c,
            "INSERT INTO pg_xmin_horizon (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner) VALUES ($1, $2, $3, $4, $5, $6, $7, 'detail', $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(name), name, source, age, holder, winner);

    private static Task SeedTable(NpgsqlConnection c, string name, DateTime at, string table, long dead, string database = "appdb") =>
        Exec(c,
            "INSERT INTO pg_autovacuum_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, live_tuples, dead_tuples, vacuum_threshold, mods_since_analyze, analyze_threshold, inserts_since_vacuum, insert_vacuum_threshold, autovacuum_disabled, total_bytes, autovacuum_count) VALUES ($1, $2, $3, $4, $7, 'public', $5, 10000, $6, 1000, 0, 500, 0, 1000, FALSE, 1000000, 1)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(name), name, table, dead, database);

    private static Task SeedSlot(NpgsqlConnection c, string name, DateTime at, string slot, long retained) =>
        Exec(c,
            "INSERT INTO pg_replication_slot_stats (collection_id, collection_time, server_id, server_name, slot_name, slot_type, plugin, database_name, is_active, active_pid, is_temporary, two_phase, wal_status, safe_wal_size_bytes, retained_wal_bytes, xmin_age, catalog_xmin_age, inactive_since, invalidation_reason, conflicting) VALUES ($1, $2, $3, $4, $5, 'logical', 'pgoutput', 'appdb', FALSE, NULL, FALSE, FALSE, 'reserved', -1, $6, NULL, NULL, NULL, NULL, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(name), name, slot, retained);

    private static Task SeedDatabase(NpgsqlConnection c, string name, DateTime at, string database) =>
        Exec(c,
            "INSERT INTO pg_wraparound_stats (collection_id, collection_time, server_id, server_name, database_name, frozen_xid_age, min_multixid_age, autovacuum_freeze_max_age, autovacuum_multixact_freeze_max_age, pct_toward_emergency_vacuum, pct_toward_wraparound, pct_toward_multixact_emergency, pct_toward_multixact_wraparound, xids_remaining, multixids_remaining, allows_connections) VALUES ($1, $2, $3, $4, $5, 1000, 1000, 200000000, 400000000, 0, 0, 0, 0, 2147482648, 2147482648, TRUE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(name), name, database);

    private static string[] Names(JsonElement root, string property, string field) =>
        root.GetProperty(property).EnumerateArray().Select(e => e.GetProperty(field).GetString()!).ToArray();

    // ------------------------------------------------------------------ xmin horizon (#7878)

    [Fact]
    public async Task Xmin_AHolderThatFinishedNineHoursAgo_IsNotReportedAsAHolder_AgainstDevPostgres()
    {
        const string Name = "pg7878-xmin-cleared";
        await RunAsync(Name, "pg_xmin_horizon", "pg_xmin_horizon", async (c, ds, end) =>
        {
            await SeedHolder(c, Name, end.AddHours(-9), "session", 5000, "42", winner: true);
            await Run(c, Name, "pg_xmin_horizon", end.AddHours(-9));
            await Run(c, Name, "pg_xmin_horizon", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgXminTools.GetPgXminHorizon(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("winning_holder", out _), root.ToString());
            Assert.False(root.TryGetProperty("recommended_action", out _), root.ToString());
            var cleared = root.GetProperty("cleared_holders").EnumerateArray().Single();
            Assert.Equal("session", cleared.GetProperty("source").GetString());
            Assert.Equal("holder_cleared", cleared.GetProperty("status").GetString());
            Assert.Equal("42", cleared.GetProperty("last_holder").GetString());
            Assert.False(cleared.TryGetProperty("is_currently_winning", out _), root.ToString());
        });
    }

    [Fact]
    public async Task Xmin_AClearedSource_NextToASourceStillHolding_NeverWinsAndIsNamedAsCleared_AgainstDevPostgres()
    {
        const string Name = "pg7878-xmin-mixed";
        await RunAsync(Name, "pg_xmin_horizon", "pg_xmin_horizon", async (c, ds, end) =>
        {
            /* The session was the winner nine hours ago and has finished. The slot holds in the newest run. */
            await SeedHolder(c, Name, end.AddHours(-9), "session", 90000, "42", winner: true);
            await Run(c, Name, "pg_xmin_horizon", end.AddHours(-9));
            await SeedHolder(c, Name, end.AddMinutes(-5), "replication_slot", 1000, "slot_a", winner: true);
            await Run(c, Name, "pg_xmin_horizon", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgXminTools.GetPgXminHorizon(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            Assert.Equal("replication_slot", root.GetProperty("winning_source").GetString());
            Assert.Equal(["replication_slot"], Names(root, "holders", "source"));
            Assert.Equal(["session"], Names(root, "cleared_holders", "source"));
        });
    }

    [Fact]
    public async Task Xmin_AHolderFromTheNewestRun_StaysCurrent_AgainstDevPostgres()
    {
        const string Name = "pg7878-xmin-current";
        await RunAsync(Name, "pg_xmin_horizon", "pg_xmin_horizon", async (c, ds, end) =>
        {
            await Run(c, Name, "pg_xmin_horizon", end.AddMinutes(-35));
            await SeedHolder(c, Name, end.AddMinutes(-5), "session", 5000, "42", winner: true);
            await Run(c, Name, "pg_xmin_horizon", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgXminTools.GetPgXminHorizon(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            Assert.Equal("session", root.GetProperty("winning_source").GetString());
            Assert.Empty(root.GetProperty("cleared_holders").EnumerateArray());
        });
    }

    // ------------------------------------------------------------------ autovacuum (#7878)

    [Fact]
    public async Task Autovacuum_ATableVacuumedCleanAfterItsLastSample_IsNotGradedOrRanked_AgainstDevPostgres()
    {
        const string Name = "pg7878-autovac-cleared";
        await RunAsync(Name, "pg_autovacuum_stats", "pg_autovacuum_stats", async (c, ds, end) =>
        {
            /* "big" was 50x past its threshold nine hours ago and was vacuumed clean: the collector stores nothing for it since. */
            await SeedTable(c, Name, end.AddHours(-9), "big", 50000);
            await Run(c, Name, "pg_autovacuum_stats", end.AddHours(-9));
            await SeedTable(c, Name, end.AddMinutes(-5), "small", 1500);
            await Run(c, Name, "pg_autovacuum_stats", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("tables_with_pending_maintenance", root.GetProperty("status").GetString());
            Assert.Equal("public.small", root.GetProperty("worst_table").GetString());
            Assert.Equal(1, root.GetProperty("past_threshold_count").GetInt32());
            Assert.Equal(["public.small"], Names(root, "tables", "table_name"));
            Assert.DoesNotContain("critical_far_past_threshold", root.GetProperty("tables").ToString(), StringComparison.Ordinal);
            Assert.Equal(["public.big"], Names(root, "cleared_tables", "table"));
        });
    }

    [Fact]
    public async Task Autovacuum_WhenEveryTableCleared_AnswersEmptyAndNamesThem_AgainstDevPostgres()
    {
        const string Name = "pg7878-autovac-allcleared";
        await RunAsync(Name, "pg_autovacuum_stats", "pg_autovacuum_stats", async (c, ds, end) =>
        {
            await SeedTable(c, Name, end.AddHours(-9), "big", 50000);
            await Run(c, Name, "pg_autovacuum_stats", end.AddHours(-9));
            await Run(c, Name, "pg_autovacuum_stats", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Equal(["public.big"], Names(root, "cleared_tables", "table"));
        });
    }

    [Fact]
    public async Task Autovacuum_ATableFromTheNewestRun_IsStillGraded_AgainstDevPostgres()
    {
        const string Name = "pg7878-autovac-current";
        await RunAsync(Name, "pg_autovacuum_stats", "pg_autovacuum_stats", async (c, ds, end) =>
        {
            await Run(c, Name, "pg_autovacuum_stats", end.AddMinutes(-35));
            await SeedTable(c, Name, end.AddMinutes(-5), "big", 50000);
            await Run(c, Name, "pg_autovacuum_stats", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("public.big", root.GetProperty("worst_table").GetString());
            Assert.Equal("critical_far_past_threshold", root.GetProperty("worst_severity").GetString());
            Assert.Empty(root.GetProperty("cleared_tables").EnumerateArray());
        });
    }

    [Fact]
    public async Task Autovacuum_ADatabaseSkippedOnTheNewestRun_IsNotCalledClearedAndStaysGraded_AgainstDevPostgres()
    {
        const string Name = "pg7878-autovac-partial";
        await RunAsync(Name, "pg_autovacuum_stats", "pg_autovacuum_stats", async (c, ds, end) =>
        {
            /* An older full run read both databases. The newest run logs SUCCESS but its database read for "otherdb" failed: it carries
               the partial-failure note and stored rows for "appdb" only. "otherdb" was not vacuumed clean, it was not read. */
            await SeedTable(c, Name, end.AddHours(-2), "orders", 40000, "otherdb");
            await SeedTable(c, Name, end.AddHours(-2), "small", 1500, "appdb");
            await Run(c, Name, "pg_autovacuum_stats", end.AddHours(-2));
            await SeedTable(c, Name, end.AddMinutes(-5), "small", 1500, "appdb");
            await Run(c, Name, "pg_autovacuum_stats", end.AddMinutes(-5));
            await Exec(c,
                "UPDATE collection_log SET error_message = $1 WHERE server_id = $2 AND collector_name = 'pg_autovacuum_stats' AND collection_time = $3",
                EnumeratedCollectorDriver.BuildPartialFailureNote(1, 2, ["otherdb"], "canceling statement due to statement timeout"),
                Id(Name), DarlingMcpTestData.Naive(end.AddMinutes(-5).AddSeconds(5)));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("tables_with_pending_maintenance", root.GetProperty("status").GetString());
            Assert.Equal("public.orders", root.GetProperty("worst_table").GetString());
            Assert.Equal(2, root.GetProperty("past_threshold_count").GetInt32());
            Assert.Equal(["public.orders", "public.small"], Names(root, "tables", "table_name").Order().ToArray());
            Assert.Empty(root.GetProperty("cleared_tables").EnumerateArray());
        });
    }

    // ------------------------------------------------------------------ replication slots (#7910)

    [Fact]
    public async Task Slots_ASlotAbsentFromTheNewestCapture_IsNotListedAsCurrent_AndIsNamed_AgainstDevPostgres()
    {
        const string Name = "pg7910-slots-dropped";
        await RunAsync(Name, "pg_replication_slots", "pg_replication_slot_stats", async (c, ds, end) =>
        {
            /* Both slots in the first capture, the abandoned one dropped before the second. */
            await SeedSlot(c, Name, end.AddMinutes(-30), "kept", 1000);
            await SeedSlot(c, Name, end.AddMinutes(-30), "abandoned", 50_000_000_000);
            await Run(c, Name, "pg_replication_slots", end.AddMinutes(-30));
            await SeedSlot(c, Name, end.AddMinutes(-5), "kept", 2000);
            await Run(c, Name, "pg_replication_slots", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgSlotTools.GetPgReplicationSlots(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("slots_present", root.GetProperty("status").GetString());
            Assert.Equal(1, root.GetProperty("slot_count").GetInt32());
            Assert.Equal("kept", root.GetProperty("worst_slot").GetString());
            Assert.Equal(["abandoned"], root.GetProperty("omitted_slots").EnumerateArray().Select(e => e.GetString()!).ToArray());
            Assert.Contains("1 slot(s)", root.GetProperty("omitted_slots_note").GetString(), StringComparison.Ordinal);
            Assert.Equal(0d, root.GetProperty("total_retained_wal_gb").GetDouble(), 1);

            /* The slot present in both captures is reported as before, from its newest sample. */
            var kept = root.GetProperty("slots").EnumerateArray().Single();
            Assert.Equal("kept", kept.GetProperty("slot_name").GetString());
            Assert.Equal(2000, kept.GetProperty("retained_wal_bytes").GetInt64());
            Assert.Equal(end.AddMinutes(-5), kept.GetProperty("last_seen").GetDateTime(), TimeSpan.FromSeconds(1));
            Assert.Equal(1000, kept.GetProperty("retained_wal_growth_bytes").GetInt64());
        });
    }

    [Fact]
    public async Task Slots_WhenTheOnlySlotWasDropped_AnswersEmptyAndNamesIt_AgainstDevPostgres()
    {
        const string Name = "pg7910-slots-only-dropped";
        await RunAsync(Name, "pg_replication_slots", "pg_replication_slot_stats", async (c, ds, end) =>
        {
            await SeedSlot(c, Name, end.AddMinutes(-30), "abandoned", 50_000_000_000);
            await Run(c, Name, "pg_replication_slots", end.AddMinutes(-30));
            await Run(c, Name, "pg_replication_slots", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgSlotTools.GetPgReplicationSlots(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Equal(["abandoned"], root.GetProperty("omitted_slots").EnumerateArray().Select(e => e.GetString()!).ToArray());
        });
    }

    // ------------------------------------------------------------------ wraparound (#7910)

    [Fact]
    public async Task Wraparound_ADatabaseAbsentFromTheNewestCapture_IsNotListedAsCurrent_AndIsNamed_AgainstDevPostgres()
    {
        const string Name = "pg7910-wrap-dropped";
        await RunAsync(Name, "pg_wraparound_stats", "pg_wraparound_stats", async (c, ds, end) =>
        {
            await SeedDatabase(c, Name, end.AddMinutes(-30), "appdb");
            await SeedDatabase(c, Name, end.AddMinutes(-30), "olddb");
            await Run(c, Name, "pg_wraparound_stats", end.AddMinutes(-30));
            await SeedDatabase(c, Name, end.AddMinutes(-5), "appdb");
            await Run(c, Name, "pg_wraparound_stats", end.AddMinutes(-5));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgWraparoundTools.GetPgWraparoundRisk(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal(["appdb"], Names(root, "databases", "database_name"));
            Assert.Equal("appdb", root.GetProperty("worst_database").GetString());
            Assert.Equal(["olddb"], root.GetProperty("omitted_databases").EnumerateArray().Select(e => e.GetString()!).ToArray());
            Assert.Contains("1 database(s)", root.GetProperty("omitted_databases_note").GetString(), StringComparison.Ordinal);
            Assert.Equal(end.AddMinutes(-5), root.GetProperty("databases").EnumerateArray().Single().GetProperty("measured_at").GetDateTime(), TimeSpan.FromSeconds(1));
        });
    }

    [Fact]
    public async Task Wraparound_DatabasesPresentInEveryCapture_AreAllReported_AgainstDevPostgres()
    {
        const string Name = "pg7910-wrap-all";
        await RunAsync(Name, "pg_wraparound_stats", "pg_wraparound_stats", async (c, ds, end) =>
        {
            foreach (var at in new[] { end.AddMinutes(-30), end.AddMinutes(-5) })
            {
                await SeedDatabase(c, Name, at, "appdb");
                await SeedDatabase(c, Name, at, "otherdb");
                await Run(c, Name, "pg_wraparound_stats", at);
            }

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgWraparoundTools.GetPgWraparoundRisk(ds, Name, 24, as_of: AsOf(end)));

            Assert.Equal(["appdb", "otherdb"], Names(root, "databases", "database_name").OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Empty(root.GetProperty("omitted_databases").EnumerateArray());
        });
    }
}
