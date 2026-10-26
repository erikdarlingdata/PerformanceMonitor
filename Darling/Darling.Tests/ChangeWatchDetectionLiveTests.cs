/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.ChangeWatch;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ChangeWatchLiveTests;

namespace Darling.Tests;

/// <summary>
/// #6234: change DETECTION on a live store, over seeded snapshot rows of every kind: server configuration, database configuration,
/// database-scoped configuration, trace flags, forced plans (a person's forcing, the ledger, and the last plan being unforced), and
/// the high-water mark that keeps a tick from reading the history. Query Store measures of a plan-change watch are here too.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync (ScratchPostgres), so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class ChangeWatchDetectionLiveTests
{
    internal static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    internal static Task ServerConfigAsync(NpgsqlConnection c, DateTime at, string name, long value, CancellationToken ct) =>
        SeedAsync(c, "server_config", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["capture_time"] = Naive(at), ["configuration_name"] = name,
            ["value_configured"] = value, ["value_in_use"] = value, ["is_dynamic"] = true, ["is_advanced"] = false,
        }, ct);

    internal static Task DatabaseConfigAsync(NpgsqlConnection c, DateTime at, string database, int compatibility, string recovery, CancellationToken ct) =>
        SeedAsync(c, "database_config", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["capture_time"] = Naive(at), ["database_name"] = database,
            ["compatibility_level"] = compatibility, ["recovery_model"] = recovery,
        }, ct);

    internal static Task ScopedConfigAsync(NpgsqlConnection c, DateTime at, string database, string name, string value, CancellationToken ct) =>
        SeedAsync(c, "database_scoped_config", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["capture_time"] = Naive(at), ["database_name"] = database,
            ["configuration_name"] = name, ["value"] = value,
        }, ct);

    internal static Task TraceFlagAsync(NpgsqlConnection c, DateTime at, int flag, CancellationToken ct) =>
        SeedAsync(c, "trace_flags", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["capture_time"] = Naive(at), ["trace_flag"] = flag,
            ["status"] = true, ["is_global"] = true, ["is_session"] = false,
        }, ct);

    internal static Task ForcedPlanAsync(NpgsqlConnection c, DateTime at, string database, long queryId, long planId, CancellationToken ct) =>
        SeedAsync(c, "query_store_forced_plans", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(at), ["database_name"] = database,
            ["query_id"] = queryId, ["plan_id"] = planId,
        }, ct);

    /* The collector writes ONE all-NULL marker row per captured database per pass (database_name only), then the forced plans. */
    internal static Task ForcedPlanMarkerAsync(NpgsqlConnection c, DateTime at, string database, CancellationToken ct) =>
        SeedAsync(c, "query_store_forced_plans", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(at), ["database_name"] = database,
        }, ct);

    /// <summary>One capture of one database exactly as the collector writes it: the marker row, then each forced plan.</summary>
    internal static async Task ForcedPlanCaptureAsync(NpgsqlConnection c, DateTime at, string database, CancellationToken ct, params (long Query, long Plan)[] plans)
    {
        await ForcedPlanMarkerAsync(c, at, database, ct);
        foreach (var (query, plan) in plans)
        {
            await ForcedPlanAsync(c, at, database, query, plan, ct);
        }
    }

    internal static Task LedgerAsync(NpgsqlConnection c, DateTime at, string database, long queryId, long planId, string action, CancellationToken ct) =>
        SeedAsync(c, "plan_force_actions", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["action_time"] = Naive(at), ["database_name"] = database,
            ["query_id"] = queryId, ["plan_id"] = planId, ["action"] = action, ["outcome"] = "succeeded",
        }, ct);

    internal static Task CollectorRunAsync(NpgsqlConnection c, DateTime at, string collector, string status, long rows, CancellationToken ct) =>
        SeedAsync(c, "collection_log", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(at), ["collector_name"] = collector,
            ["status"] = status, ["rows_collected"] = rows,
        }, ct);

    internal static async Task AddServerAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) " +
            "VALUES ($1, $2, $2, true, 16, now(), now())", c);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static string[] Keys(IEnumerable<ChangeWatchChange> changes) => changes.Select(c => c.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray();

    [Fact]
    public async Task ReadChanges_FindsExactlyTheChangesAfterTheMark_ForEverySnapshotSource_AndNothingAtOrBeforeIt()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var mark = h.Now.AddMinutes(-30);
            var t1 = mark.AddMinutes(-20);
            var t2 = mark.AddMinutes(-10);
            var t3 = mark;                      /* a capture AT the mark: a change first seen there was seen before */
            var t4 = mark.AddMinutes(5);
            var t5 = mark.AddMinutes(10);

            /* Server configuration: dop changes before the mark (t2) and after it (t4); cost changes exactly AT the mark (t3). */
            foreach (var (at, dop, cost) in new[] { (t1, 4L, 5L), (t2, 2L, 5L), (t3, 2L, 50L), (t4, 1L, 50L), (t5, 1L, 50L) })
            {
                await ServerConfigAsync(h.Connection, at, "max degree of parallelism", dop, ct);
                await ServerConfigAsync(h.Connection, at, "cost threshold for parallelism", cost, ct);
            }

            /* Database configuration: recovery model changes before the mark, compatibility level after it. */
            foreach (var (at, compat, recovery) in new[] { (t1, 140, "FULL"), (t2, 140, "SIMPLE"), (t3, 140, "SIMPLE"), (t4, 140, "SIMPLE"), (t5, 150, "SIMPLE") })
            {
                await DatabaseConfigAsync(h.Connection, at, "Sales", compat, recovery, ct);
            }

            /* Database-scoped configuration: MAXDOP changes before the mark (t2) and after it (t4); PARAMETER_SNIFFING exactly at it (t3). */
            foreach (var (at, dop, sniffing) in new[] { (t1, "0", "ON"), (t2, "4", "ON"), (t3, "4", "OFF"), (t4, "8", "OFF"), (t5, "8", "OFF") })
            {
                await ScopedConfigAsync(h.Connection, at, "Sales", "MAXDOP", dop, ct);
                await ScopedConfigAsync(h.Connection, at, "Sales", "PARAMETER_SNIFFING", sniffing, ct);
            }

            /* Trace flags: 1117 turned on before the mark, 2371 on after it, 4199 off after it. */
            await TraceFlagAsync(h.Connection, t1, 4199, ct);
            foreach (var at in new[] { t2, t3 })
            {
                await TraceFlagAsync(h.Connection, at, 4199, ct);
                await TraceFlagAsync(h.Connection, at, 1117, ct);
            }

            await TraceFlagAsync(h.Connection, t4, 4199, ct);
            await TraceFlagAsync(h.Connection, t4, 1117, ct);
            await TraceFlagAsync(h.Connection, t4, 2371, ct);
            await TraceFlagAsync(h.Connection, t5, 1117, ct);
            await TraceFlagAsync(h.Connection, t5, 2371, ct);

            /* Forced plans a person forced: plan 10/100 is the baseline, 11/110 appears before the mark, 12/120 after it. The ledger forced
               plan 13/130 before the mark and 14/140 after it, and a person's snapshot row for 14/140 appears right after (the ledger's own). */
            await ForcedPlanCaptureAsync(h.Connection, t1, "Sales", ct, (10, 100));
            foreach (var at in new[] { t2, t3 })
            {
                await ForcedPlanCaptureAsync(h.Connection, at, "Sales", ct, (10, 100), (11, 110));
            }

            foreach (var at in new[] { t4, t5 })
            {
                await ForcedPlanCaptureAsync(h.Connection, at, "Sales", ct, (10, 100), (11, 110), (12, 120));
            }

            await LedgerAsync(h.Connection, t2, "Sales", 13, 130, "force", ct);
            await LedgerAsync(h.Connection, t3, "Sales", 15, 150, "force", ct);
            await LedgerAsync(h.Connection, t4.AddMinutes(1), "Sales", 14, 140, "force", ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, mark, h.Now, ct);

            /* The ledger row has a generated id, so it is checked by what it says: the ONE action after the mark is query 14's. */
            var action = Assert.Single(found, c => c.Key.StartsWith("plan_force:action:", StringComparison.Ordinal));
            Assert.Equal(14, action.QueryId);
            Assert.Equal(
                new[]
                {
                    "database_config:Sales:compatibility_level:" + Stamp(t5),
                    "database_scoped_config:Sales:MAXDOP:" + Stamp(t4),
                    "plan_force:snapshot:Sales:12:120:" + Stamp(t4),
                    "server_config:max degree of parallelism:" + Stamp(t4),
                    "trace_flag:2371:" + Stamp(t4),
                    "trace_flag:4199:" + Stamp(t5),
                }.OrderBy(k => k, StringComparer.Ordinal),
                Keys(found.Where(c => c != action)));

            /* The three changes that sit at or before the mark are real rows: an earlier mark finds them, so the mark is what left them out. */
            var wider = Keys(await h.NewPass().ReadChangesAsync(ServerId, t1, h.Now, ct));
            Assert.Contains("server_config:cost threshold for parallelism:" + Stamp(t3), wider);
            Assert.Contains("server_config:max degree of parallelism:" + Stamp(t2), wider);
            Assert.Contains("database_config:Sales:recovery_model:" + Stamp(t2), wider);
            Assert.Contains("database_scoped_config:Sales:MAXDOP:" + Stamp(t2), wider);
            Assert.Contains("database_scoped_config:Sales:PARAMETER_SNIFFING:" + Stamp(t3), wider);
            Assert.Contains("trace_flag:1117:" + Stamp(t2), wider);
            Assert.Contains("plan_force:snapshot:Sales:11:110:" + Stamp(t2), wider);

            /* A mark at the newest capture finds nothing at all. */
            Assert.Empty(await h.NewPass().ReadChangesAsync(ServerId, t5, h.Now, ct));
        });
    }

    /// <summary>
    /// #6550: a forced or unforced plan carries how it was forced, from the forced-plan capture it was seen in (the previous one for an unforce):
    /// AUTO for automatic plan correction, MANUAL for a person, unknown where the capture holds no value.
    /// </summary>
    [Fact]
    public async Task PlanForceChanges_SayHowThePlanWasForced()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var t1 = h.Now.AddMinutes(-40);
            var t2 = t1.AddMinutes(10);
            var t3 = t1.AddMinutes(20);

            await ForcedPlanCaptureAsync(h.Connection, t1, "Sales", ct);
            await ForcedPlanMarkerAsync(h.Connection, t2, "Sales", ct);
            foreach (var (query, plan, type) in new (long, long, string?)[] { (1, 10, "AUTO"), (2, 20, "MANUAL"), (3, 30, null) })
            {
                var values = new Dictionary<string, object>
                {
                    ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(t2), ["database_name"] = "Sales",
                    ["query_id"] = query, ["plan_id"] = plan,
                };
                if (type is not null)
                {
                    values["plan_forcing_type"] = type;
                }

                await SeedAsync(h.Connection, "query_store_forced_plans", values, ct);
            }

            await ForcedPlanCaptureAsync(h.Connection, t3, "Sales", ct);

            var found = (await h.NewPass().ReadChangesAsync(ServerId, t1, h.Now, ct)).ToDictionary(c => c.Key);

            Assert.Equal("AUTO", found["plan_force:snapshot:Sales:1:10:" + Stamp(t2)].PlanForcingType);
            Assert.Equal("MANUAL", found["plan_force:snapshot:Sales:2:20:" + Stamp(t2)].PlanForcingType);
            Assert.Equal("unknown", found["plan_force:snapshot:Sales:3:30:" + Stamp(t2)].PlanForcingType);
            Assert.Equal("AUTO", found["plan_force:unforced:Sales:1:10:" + Stamp(t3)].PlanForcingType);
            Assert.Equal("MANUAL", found["plan_force:unforced:Sales:2:20:" + Stamp(t3)].PlanForcingType);
            Assert.Contains("[forcing type AUTO: automatic plan correction]", found["plan_force:snapshot:Sales:1:10:" + Stamp(t2)].Description, StringComparison.Ordinal);
            Assert.Contains("[forcing type MANUAL]", found["plan_force:snapshot:Sales:2:20:" + Stamp(t2)].Description, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// #6566: log_reuse_wait_desc is a runtime status, not a configuration. A database whose only change after the mark is that column
    /// reports nothing; recovery_model changing on another database in the same window is still reported.
    /// </summary>
    [Fact]
    public async Task DatabaseConfig_IgnoresLogReuseWaitStatus_ButStillReportsARealOption()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var mark = h.Now.AddMinutes(-30);
            var before = mark.AddMinutes(-10);
            var statusOnly = mark.AddMinutes(5);
            var real = mark.AddMinutes(10);

            static Task Seed(NpgsqlConnection c, DateTime at, string database, string recovery, string logReuse, CancellationToken ct) =>
                SeedAsync(c, "database_config", new Dictionary<string, object>
                {
                    ["server_id"] = ServerId, ["server_name"] = ServerName, ["capture_time"] = Naive(at), ["database_name"] = database,
                    ["compatibility_level"] = 150, ["recovery_model"] = recovery, ["log_reuse_wait_desc"] = logReuse,
                }, ct);

            await Seed(h.Connection, before, "Status", "FULL", "ACTIVE_TRANSACTION", ct);
            await Seed(h.Connection, statusOnly, "Status", "FULL", "NOTHING", ct);
            await Seed(h.Connection, real, "Status", "FULL", "NOTHING", ct);
            await Seed(h.Connection, before, "Real", "FULL", "NOTHING", ct);
            await Seed(h.Connection, statusOnly, "Real", "FULL", "LOG_BACKUP", ct);
            await Seed(h.Connection, real, "Real", "SIMPLE", "LOG_BACKUP", ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, mark, h.Now, ct);

            Assert.Equal(new[] { "database_config:Real:recovery_model:" + Stamp(real) }, Keys(found));
        });
    }

    internal static string Stamp(DateTime time) => time.ToString("yyyyMMddTHHmmssfff", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public async Task APlanBeingUnforced_IsAChange_PerDatabase_FromTheMarkerRows_AndNeverFromADatabaseThatWasNotCaptured()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-40);
            var third = h.Now.AddMinutes(-30);

            /* Sales has three forced plans, then one of them is unforced and the ledger itself unforces another; Orders loses its LAST forced plan (the
               capture still writes its marker); Billing is captured once and is then not captured at all (Query Store off, a failover). */
            await ForcedPlanCaptureAsync(h.Connection, first, "Sales", ct, (1, 10), (2, 20), (3, 30));
            await ForcedPlanCaptureAsync(h.Connection, first, "Orders", ct, (4, 40));
            await ForcedPlanCaptureAsync(h.Connection, first, "Billing", ct, (5, 50));
            await LedgerAsync(h.Connection, second.AddMinutes(-3), "Sales", 3, 30, "unforce", ct);
            await ForcedPlanCaptureAsync(h.Connection, second, "Sales", ct, (2, 20));
            await ForcedPlanCaptureAsync(h.Connection, second, "Orders", ct);
            await ForcedPlanCaptureAsync(h.Connection, third, "Sales", ct, (2, 20));
            await ForcedPlanCaptureAsync(h.Connection, third, "Orders", ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct);
            var plans = found.Where(c => c.Kind == "plan_force" && !c.Key.StartsWith("plan_force:action:", StringComparison.Ordinal)).ToList();
            Assert.Equal(
                new[] { "plan_force:unforced:Orders:4:40:" + Stamp(second), "plan_force:unforced:Sales:1:10:" + Stamp(second) },
                Keys(plans));
            Assert.Equal(new long?[] { 1, 4 }, plans.Select(c => c.QueryId).OrderBy(q => q).ToArray());

            /* Marked at the unforcing capture, it was seen already; and nothing reads Billing, which has no capture after its first. */
            Assert.DoesNotContain(await h.NewPass().ReadChangesAsync(ServerId, second, h.Now, ct), c => c.Kind == "plan_force");
        });
    }

    /* #6551: a readable secondary's Query Store holds the primary's forced plans, so its plan-snapshot changes are the primary's and open no watch here. */
    [Fact]
    public async Task APlanChange_InADatabaseThisServerHoldsAsASecondary_IsNotRecorded_ButTheLedgerAndOtherDatabasesAre()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-40);
            await ForcedPlanCaptureAsync(h.Connection, first, "Replicated", ct);
            await ForcedPlanCaptureAsync(h.Connection, first, "Local", ct);
            await ForcedPlanCaptureAsync(h.Connection, second, "Replicated", ct, (1, 10));
            await ForcedPlanCaptureAsync(h.Connection, second, "Local", ct, (2, 20));
            await LedgerAsync(h.Connection, h.Now.AddMinutes(-20), "Replicated", 3, 30, "force", ct);

            static bool Snapshot(ChangeWatchChange c) => c.Key.StartsWith("plan_force:snapshot:", StringComparison.Ordinal);

            /* Before any availability-group snapshot the server is not known to be a secondary: both databases' changes are read. */
            Assert.Equal(2, (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Count(Snapshot));

            await SeedAsync(h.Connection, "ag_replica_states", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(h.Now.AddMinutes(-5)), ["ag_name"] = "AG1",
                ["replica_server_name"] = ServerName, ["role_desc"] = "SECONDARY", ["is_local"] = true,
            }, ct);
            await SeedAsync(h.Connection, "ag_database_replica_states", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(h.Now.AddMinutes(-5)), ["ag_name"] = "AG1",
                ["database_name"] = "Replicated", ["replica_server_name"] = ServerName, ["is_local"] = true,
            }, ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct);
            Assert.Equal(new[] { "plan_force:snapshot:Local:2:20:" + Stamp(second) }, Keys(found.Where(Snapshot)));
            Assert.Contains(found, c => c.Key.StartsWith("plan_force:action:", StringComparison.Ordinal));

            /* After a failover the newest snapshot says PRIMARY: nothing is left out. */
            await SeedAsync(h.Connection, "ag_replica_states", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(h.Now.AddMinutes(-1)), ["ag_name"] = "AG1",
                ["replica_server_name"] = ServerName, ["role_desc"] = "PRIMARY", ["is_local"] = true,
            }, ct);
            Assert.Equal(2, (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Count(Snapshot));
        });
    }

    /* #6551 review: the secondary rule is the shared one (an availability-group snapshot older than an hour is not trusted). The collectors can stop at or
       before a failover, and a day-old SECONDARY row would then drop this node's own plan changes for a day. */
    [Fact]
    public async Task AStaleSecondarySnapshot_IsNotTrusted_AndDropsNoPlanChange()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-40);
            await ForcedPlanCaptureAsync(h.Connection, first, "Replicated", ct);
            await ForcedPlanCaptureAsync(h.Connection, second, "Replicated", ct, (1, 10));

            static bool Snapshot(ChangeWatchChange c) => c.Key.StartsWith("plan_force:snapshot:", StringComparison.Ordinal);

            /* The newest snapshots say SECONDARY, but they are three hours old: the collectors stopped. */
            await SeedAsync(h.Connection, "ag_replica_states", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(h.Now.AddHours(-3)), ["ag_name"] = "AG1",
                ["replica_server_name"] = ServerName, ["role_desc"] = "SECONDARY", ["is_local"] = true,
            }, ct);
            await SeedAsync(h.Connection, "ag_database_replica_states", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(h.Now.AddHours(-3)), ["ag_name"] = "AG1",
                ["database_name"] = "Replicated", ["replica_server_name"] = ServerName, ["is_local"] = true,
            }, ct);

            Assert.Single(await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct), Snapshot);
        });
    }

    [Fact]
    public async Task MarkerRows_DoNotFailDetection_AndALaterCapturedDatabaseIsABaseline_NotAForce()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var mark = h.Now.AddMinutes(-30);
            /* A server whose only Query Store database has no forced plan: every pass writes a marker row and nothing else. */
            await ServerConfigAsync(h.Connection, mark.AddMinutes(-10), "max degree of parallelism", 4, ct);
            await ServerConfigAsync(h.Connection, mark.AddMinutes(10), "max degree of parallelism", 2, ct);
            await ForcedPlanMarkerAsync(h.Connection, mark.AddMinutes(-10), "Sales", ct);
            await ForcedPlanMarkerAsync(h.Connection, mark.AddMinutes(5), "Sales", ct);
            await ForcedPlanMarkerAsync(h.Connection, mark.AddMinutes(10), "Sales", ct);
            /* A database first captured now has forced plans: its first capture is a baseline, not "forced by a person". */
            await ForcedPlanCaptureAsync(h.Connection, mark.AddMinutes(10), "Orders", ct, (7, 70));

            var found = await h.NewPass().ReadChangesAsync(ServerId, mark, h.Now, ct);

            /* The server setting change is read in the same pass: a marker row used to throw out of the read and drop it. */
            var change = Assert.Single(found);
            Assert.Equal("server_config", change.Kind);
        });
    }

    [Fact]
    public async Task TheLastTraceFlagBeingTurnedOff_IsAChange_FromTheEmptyRun()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var snapshot = h.Now.AddMinutes(-40);
            await TraceFlagAsync(h.Connection, snapshot, 4199, ct);
            await CollectorRunAsync(h.Connection, snapshot.AddMinutes(10), "trace_flags", "SUCCESS", 0, ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, snapshot.AddMinutes(-1), h.Now, ct);

            var change = Assert.Single(found);
            Assert.Equal("trace_flag:4199:" + Stamp(snapshot.AddMinutes(10)), change.Key);
        });
    }

    [Fact]
    public async Task DetectAsync_FirstPassSetsTheMarksToNow_OpensNothingForHistory_ThenReadsOnlyWhatIsNewer()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var clock = h.Now;
            foreach (var (at, dop) in new[] { (h.Now.AddMinutes(-50), 4L), (h.Now.AddMinutes(-30), 2L) })
            {
                await ServerConfigAsync(h.Connection, at, "max degree of parallelism", dop, ct);
            }

            /* The first pass on a store with history: a change 30 minutes ago is history, not a watch. */
            await h.NewPassAt(() => clock).DetectAsync(ct);
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            var marks = await h.Store.GetMarksAsync(ServerId, ct);
            Assert.Equal(ChangeWatchStore.MarkSources.Count, marks.Count);
            Assert.All(marks.Values, m => Assert.Equal(h.Now, m));

            /* Ten minutes later a new capture changes the setting: one watch, one change. */
            clock = h.Now.AddMinutes(10);
            await ServerConfigAsync(h.Connection, h.Now.AddMinutes(5), "max degree of parallelism", 1, ct);
            await h.NewPassAt(() => clock).DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));

            /* The marks moved up to the pass less the slack, and never backwards. */
            marks = await h.Store.GetMarksAsync(ServerId, ct);
            Assert.All(marks.Values, m => Assert.Equal(clock - ChangeWatchPass.MarkSlack, m));

            /* A pass with nothing new adds nothing. */
            await h.NewPassAt(() => clock).DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            await h.Store.SetMarksAsync(ServerId, new Dictionary<string, DateTime> { ["server_config"] = h.Now }, ct);
            Assert.Equal(clock - ChangeWatchPass.MarkSlack, (await h.Store.GetMarksAsync(ServerId, ct))["server_config"]);
        });
    }

    private static async Task SeedQueryStoreAsync(NpgsqlConnection c, DateTime windowStart, string database, long queryId, long executionsPerInterval, long avgUs, CancellationToken ct)
    {
        for (var interval = 0; interval < 6; interval++)
        {
            var start = windowStart.AddMinutes(interval * 10);
            await SeedAsync(c, "query_store_stats", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(start.AddMinutes(10)), ["database_name"] = database,
                ["query_id"] = queryId, ["plan_id"] = 1L, ["runtime_stats_interval_id"] = (long)interval + 1,
                ["first_execution_time"] = Naive(start), ["execution_type_desc"] = "Regular", ["replica_role"] = "PRIMARY",
                ["interval_start_time_utc"] = Naive(start), ["interval_end_time_utc"] = Naive(start.AddMinutes(10)),
                ["execution_count"] = executionsPerInterval, ["avg_duration_us"] = (double)avgUs,
            }, ct);
        }
    }

    [Fact]
    public async Task APlanChangeWatch_MeasuresThatQuery_FromQueryStoreRuntimeData()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedQueryStoreAsync(h.Connection, change.AddMinutes(-60), "Sales", 42, 5, 1_000, ct);   /* the hour before: 30 executions at 1 ms */
            await SeedQueryStoreAsync(h.Connection, change, "Sales", 42, 5, 4_000, ct);                   /* the watch window: 30 executions at 4 ms */
            await h.Store.RecordChangesAsync(ServerId, ServerName,
                new[] { new ChangeWatchChange("plan_force:action:1", "plan_force", change, "plan 7 of query 42 in Sales: force by the plan-force ledger", "Sales", 42) }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
            var json = await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct);
            Assert.Contains("query_avg_duration_ms:Sales:42", json, StringComparison.Ordinal);
            Assert.DoesNotContain("top 20", json, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task APlanChangeWatch_WithTooFewExecutions_OpensNothing()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedQueryStoreAsync(h.Connection, change.AddMinutes(-60), "Sales", 42, 1, 1_000, ct);   /* 6 executions: under the 10 needed */
            await SeedQueryStoreAsync(h.Connection, change, "Sales", 42, 5, 9_000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName,
                new[] { new ChangeWatchChange("plan_force:action:1", "plan_force", change, "plan 7 of query 42 in Sales: force by the plan-force ledger", "Sales", 42) }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task APlanChangeWatch_WhereQueryStoreHasNoIntervals_FallsBackToTheTopQueries_AndSaysSo()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedQueryStoreAsync(h.Connection, change.AddMinutes(-60), "Sales", 41, 5, 1_000, ct);   /* another query: query 42 has none */
            await h.Store.RecordChangesAsync(ServerId, ServerName,
                new[] { new ChangeWatchChange("plan_force:action:1", "plan_force", change, "plan 7 of query 42 in Sales: force by the plan-force ledger", "Sales", 42) }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            var json = await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct);
            Assert.Contains("Query Store holds no interval for query 42 in Sales", json, StringComparison.Ordinal);
            Assert.Contains("top 20 queries by total duration", json, StringComparison.Ordinal);
        });
    }

    /// <summary>Query Store intervals of <paramref name="lengthMinutes"/> minutes, one per start in <paramref name="starts"/>, each holding the same executions at one average duration.</summary>
    private static async Task SeedQueryStoreIntervalsAsync(
        NpgsqlConnection c, IReadOnlyList<DateTime> starts, int lengthMinutes, string database, long queryId, long executions, long avgUs, CancellationToken ct)
    {
        var id = 100L;
        foreach (var start in starts)
        {
            await SeedAsync(c, "query_store_stats", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(start.AddMinutes(lengthMinutes)), ["database_name"] = database,
                ["query_id"] = queryId, ["plan_id"] = 1L, ["runtime_stats_interval_id"] = id++,
                ["first_execution_time"] = Naive(start), ["execution_type_desc"] = "Regular", ["replica_role"] = "PRIMARY",
                ["interval_start_time_utc"] = Naive(start), ["interval_end_time_utc"] = Naive(start.AddMinutes(lengthMinutes)),
                ["execution_count"] = executions, ["avg_duration_us"] = (double)avgUs,
            }, ct);
        }
    }

    private static ChangeWatchChange PlanForce(DateTime at) =>
        new("plan_force:action:1", "plan_force", at, "plan 7 of query 42 in Sales: force by the plan-force ledger", "Sales", 42);

    [Fact]
    public async Task APlanChangeInsideAOneHourQueryStoreInterval_ComparesTheIntervalBeforeItWithTheOneAfter()
    {
        await RunAsync(true, async (h, ct) =>
        {
            /* Query Store's default 60-minute interval. The change lands 5 minutes into the middle interval, so that interval holds the
               regressed plan's executions and must count as neither the baseline nor the after-window. */
            var change = h.Now.AddMinutes(-90);
            var before = change.AddMinutes(-65);        /* ends 5 minutes before the change: the clean baseline, 1 ms */
            var straddling = change.AddMinutes(-5);     /* starts before the change, ends 55 minutes into the watch: 9 ms */
            var after = change.AddMinutes(55);          /* starts inside the watch: 9 ms */
            await SeedQueryStoreIntervalsAsync(h.Connection, new[] { before }, 60, "Sales", 42, 30, 1_000, ct);
            await SeedQueryStoreIntervalsAsync(h.Connection, new[] { straddling, after }, 60, "Sales", 42, 30, 9_000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[] { PlanForce(change) }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
            var measure = JsonNode.Parse((await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct))!)!["measures"]!.AsArray()
                .Single(m => m!["measure"]!.GetValue<string>() == "query_avg_duration_ms:Sales:42")!;
            Assert.Equal(1.0, measure["before"]!.GetValue<double>(), 3);
            Assert.Equal(9.0, measure["after"]!.GetValue<double>(), 3);
        });
    }

    [Fact]
    public async Task APlanChangeWhoseHourBeforeHoldsNoWholeInterval_IsListedAsNotMeasured_NeverFlat()
    {
        await RunAsync(true, async (h, ct) =>
        {
            /* Only the interval the change landed in, and the one after it: no interval ended at or before the change. */
            var change = h.Now.AddMinutes(-90);
            await SeedQueryStoreIntervalsAsync(h.Connection, new[] { change.AddMinutes(-5), change.AddMinutes(55) }, 60, "Sales", 42, 30, 9_000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[] { PlanForce(change) }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            var json = JsonNode.Parse((await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct))!)!;
            var entry = json["not_measured"]!.AsArray().Single(n => n!["measure"]!.GetValue<string>() == "query_avg_duration_ms:Sales:42")!;
            Assert.Contains("ended at or before the change", entry["reason"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Empty(json["measures"]!.AsArray());
            Assert.NotEqual("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task ATargetWithNoWaitCpuOrQueryRows_ListsEachMeasureAsNotMeasured_OnTheSqlServerPathToo()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await h.Store.RecordChangesAsync(ServerId, ServerName,
                new[] { new ChangeWatchChange("server_config:x:1", "server_config", change, "max degree of parallelism") }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            var json = JsonNode.Parse((await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct))!)!;
            var names = json["not_measured"]!.AsArray().Select(n => n!["measure"]!.GetValue<string>()).OrderBy(n => n, StringComparer.Ordinal).ToArray();
            Assert.Equal(new[] { "avg_cpu_percent", "query_avg_duration_ms", "total_wait_ms_per_second" }, names);
            Assert.Equal("no_data", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task APlanQueryUnderTheTenExecutionFloor_IsListedAsNotMeasured()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedQueryStoreAsync(h.Connection, change.AddMinutes(-60), "Sales", 42, 1, 1_000, ct);
            await SeedQueryStoreAsync(h.Connection, change, "Sales", 42, 5, 9_000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[] { PlanForce(change) }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            var json = JsonNode.Parse((await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct))!)!;
            var entry = json["not_measured"]!.AsArray().Single(n => n!["measure"]!.GetValue<string>() == "query_avg_duration_ms:Sales:42")!;
            Assert.Contains("fewer than 10 executions", entry["reason"]!.GetValue<string>(), StringComparison.Ordinal);
        });
    }
}
