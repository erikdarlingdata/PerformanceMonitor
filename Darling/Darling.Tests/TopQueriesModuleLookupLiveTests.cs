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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549 LIVE: the module lookup of the Viewer's Top Queries by Duration read
/// (<see cref="ViewerDataService.TopQueriesModuleCte"/>) ranked every <c>procedure_stats</c> row of the ranked handles with a
/// <c>ROW_NUMBER</c>, which sorts them (an external merge, with temp files, once they outgrow work_mem). On a four-day, 42-server rig that
/// was 126,732 rows sorted to keep 55, 476 ms of a 959 ms statement. It now takes one hash aggregate (the newest collection_time per ranked
/// handle, no sort) and probes <c>idx_procedure_stats_time</c> once per winner. A field store then showed the join back to the winner ran as
/// a hash join over every row of the server, so the read took 14.0 s of a 17.1 s statement (4.2 million rows read twice). It now looks in the
/// rows since the window start first, with no upper bound, and reads the older history only for the ranked handles that have no recent row.
///
/// <para>The read still passes over the server's retained rows once when a ranked handle has no recent row: <c>procedure_stats</c> has no
/// (server_id, sql_handle) index, because #4247 dropped it for its WAL cost, so no statement can prove a handle absent without that pass.
/// When every ranked handle has a recent row it reads only the recent rows. The bounds are therefore block counts and a plan-node row count,
/// never a time: no temp blocks are written (the old statement's sort wrote them at the same work_mem, which this test checks, so a seed that
/// stopped forcing the spill would fail instead of passing), the aggregates keep one group per ranked handle, not one per retained handle, and
/// a ranked set whose handles all have a recent row touches a fraction of the blocks the old statement did.</para>
///
/// <para>The answer is the old statement's, checked on rows: a tied newest collection_time (in the recent rows and in the older history), a
/// NULL module on the newest row, a handle whose only row is a year old, a handle whose newest row is older than the window start, an ad hoc
/// handle with no row, and a newer row of another server's.</para>
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class TopQueriesModuleLookupLiveTests
{
    private const int ServerId = 7;
    private const int OtherServerId = 8;
    private const int Procs = 150;
    private const int SnapshotsPerProc = 600;
    private const int NormalRanked = 10;

    /// <summary>The window start the read passes as $2: only the last half hour of the ten-hour history is recent.</summary>
    private static readonly DateTime NewestRow = new(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = NewestRow.AddMinutes(-30);

    /// <summary>The module CTE as it shipped before #8549, kept here so the test compares the answers and the temp blocks of the two statements.</summary>
    private const string OldModuleCte = """
        module AS (
            SELECT
                sql_handle,
                object_name,
                schema_name,
                database_name
            FROM
            (
                SELECT
                    sql_handle,
                    object_name,
                    schema_name,
                    database_name,
                    ROW_NUMBER() OVER (PARTITION BY sql_handle ORDER BY collection_time DESC) AS rn
                FROM procedure_stats
                WHERE server_id = $1
                AND   sql_handle IS NOT NULL
                AND   sql_handle <> ''
                AND   sql_handle IN (SELECT sql_handle FROM ranked)
            ) AS ranked_modules
            WHERE rn = 1
        )
        """;

    /// <summary>The module CTE as it stood before the host and host-named-row gates (#8549), comments cut: it read the stitch for every ranked handle. The oracle for what the grid shows.</summary>
    private const string BeforeGatesModuleCte = """
        module AS MATERIALIZED (
            WITH handles AS MATERIALIZED (
                SELECT DISTINCT sql_handle
                FROM ranked
                WHERE sql_handle IS NOT NULL
                AND   sql_handle <> ''
            ),
            recent AS MATERIALIZED (
                SELECT
                    sql_handle,
                    MAX(collection_time) AS newest
                FROM procedure_stats
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   sql_handle IN (SELECT sql_handle FROM handles)
                GROUP BY sql_handle
            ),
            missing AS MATERIALIZED (
                SELECT h.sql_handle
                FROM handles AS h
                WHERE NOT EXISTS (SELECT 1 FROM recent AS r WHERE r.sql_handle = h.sql_handle)
            ),
            older AS MATERIALIZED (
                SELECT
                    sql_handle,
                    MAX(collection_time) AS newest
                FROM procedure_stats
                WHERE server_id = $1
                AND   collection_time < $2
                AND   sql_handle IN (SELECT sql_handle FROM missing)
                AND   EXISTS (SELECT 1 FROM missing)
                GROUP BY sql_handle
            )
            SELECT
                w.sql_handle,
                p.object_name,
                p.schema_name,
                p.database_name
            FROM
            (
                SELECT sql_handle, newest FROM recent
                UNION ALL
                SELECT sql_handle, newest FROM older
            ) AS w
            CROSS JOIN LATERAL
            (
                SELECT
                    ps.object_name,
                    ps.schema_name,
                    ps.database_name
                FROM procedure_stats AS ps
                WHERE ps.server_id = $1
                AND   ps.collection_time = w.newest
                AND   ps.sql_handle = w.sql_handle
                ORDER BY ps.collection_id DESC
                LIMIT 1
            ) AS p
        )
        """;

    private sealed record Module(string? ObjectName, string? SchemaName, string? DatabaseName);

    [Fact]
    public async Task ModuleLookup_SortsNothing_KeepsOneGroupPerRankedHandle_AndReturnsTheOldAnswer()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8549 module lookup test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var newest = NewestRow;

        /* The retained history: 150 procedures, one row a minute for ten hours each (90,000 rows of the server). */
        await ExecAsync(connection,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                           sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time)
              SELECT (p::bigint * 100000) + s, $1 - make_interval(mins => s), $2, 'module-lookup', 'db' || (p % 3), 'dbo', 'usp_' || p, 'PROCEDURE',
                     '0xH' || p, '0xP' || p || '_' || s, 1, 1, 1
              FROM generate_series(1, $3) AS p, generate_series(0, $4 - 1) AS s",
            ct, newest, ServerId, Procs, SnapshotsPerProc);

        async Task Plant(int serverId, string handle, DateTime at, long collectionId, string? obj, string? schema, string? db, string plan) =>
            await ExecAsync(connection,
                @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                               sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time)
                  VALUES ($1, $2, $3, 'module-lookup', $4, $5, $6, 'PROCEDURE', $7, $8, 1, 1, 1)",
                ct, collectionId, at, serverId, db!, schema!, obj!, handle, plan);

        /* A tie on the newest collection_time with the same module (a handle with two cached plans): any pick answers the same. */
        await Plant(ServerId, "0xHTIE", newest, 1, "usp_tie", "dbo", "dbtie", "0xPA");
        await Plant(ServerId, "0xHTIE", newest, 2, "usp_tie", "dbo", "dbtie", "0xPB");
        await Plant(ServerId, "0xHTIE", newest.AddMinutes(-5), 3, "usp_tie_old", "dbo", "dbtie", "0xPC");
        /* A tie with two different modules: the row collected last (the higher collection_id) wins, as it does in Lite. The old statement left it unspecified. */
        await Plant(ServerId, "0xHTIEX", newest, 5, "usp_tie_low", "dbo", "dbx", "0xPA");
        await Plant(ServerId, "0xHTIEX", newest, 9, "usp_tie_high", "dbo", "dbx", "0xPB");
        /* The newest row has no module name: the answer is the NULLs, not the older named row. */
        await Plant(ServerId, "0xHNULL", newest.AddMinutes(-30), 11, "usp_named", "dbo", "dbn", "0xPA");
        await Plant(ServerId, "0xHNULL", newest, 12, null, null, null, "0xPB");
        /* A handle whose only row is a year old still names its module (a time bound would turn it into ad hoc). */
        await Plant(ServerId, "0xHANCIENT", newest.AddDays(-365), 13, "usp_ancient", "dbo", "dba", "0xPA");
        /* A handle whose newest row is older than the window start still names its module from the older history (the fallback), and the
           newest of its rows wins, not the first one found. */
        await Plant(ServerId, "0xHOLD", newest.AddHours(-5), 15, "usp_old_oldest", "dbo", "dbd", "0xPA");
        await Plant(ServerId, "0xHOLD", newest.AddHours(-3), 16, "usp_old_newest", "dbo", "dbd", "0xPB");
        /* A tie on the newest time inside the older history: the higher collection_id wins there too. */
        await Plant(ServerId, "0xHOLDTIE", newest.AddHours(-4), 17, "usp_oldtie_low", "dbo", "dbd", "0xPA");
        await Plant(ServerId, "0xHOLDTIE", newest.AddHours(-4), 18, "usp_oldtie_high", "dbo", "dbd", "0xPB");
        /* A handle with rows on both sides of the window start takes the recent one. */
        await Plant(ServerId, "0xHBOTH", newest.AddHours(-6), 19, "usp_both_old", "dbo", "dbb", "0xPA");
        await Plant(ServerId, "0xHBOTH", newest.AddMinutes(-10), 20, "usp_both_recent", "dbo", "dbb", "0xPB");
        /* Another server's newer row for a ranked handle must not leak in. */
        await Plant(OtherServerId, "0xH1", newest.AddHours(1), 14, "usp_other_server", "dbo", "dbo2", "0xPA");

        await ExecAsync(connection, "ANALYZE procedure_stats", ct);

        var ranked = Enumerable.Range(1, NormalRanked).Select(p => "0xH" + p)
            .Concat(new[] { "0xHTIE", "0xHTIEX", "0xHNULL", "0xHANCIENT", "0xHOLD", "0xHOLDTIE", "0xHBOTH", "0xHADHOC" }).ToArray();

        /* A small work_mem makes the old sort spill to temp files on this small seed, as it does on the rig's 126,732 rows at the default; no parallel
           workers keeps the plan shape the same on every runner. */
        await ExecAsync(connection, "SET work_mem = '64kB'", ct);
        await ExecAsync(connection, "SET max_parallel_workers_per_gather = 0", ct);

        var oldSql = Standalone(OldModuleCte);
        var newSql = Standalone(ViewerDataService.TopQueriesModuleCte);

        var oldRows = await ReadModulesAsync(connection, oldSql, ranked, ct);
        var newRows = await ReadModulesAsync(connection, newSql, ranked, ct);

        /* The answer: the same module per handle, the NULL module kept, no row for the ad hoc handle, nothing from the other server. */
        Assert.Equal(oldRows.Keys.OrderBy(k => k, StringComparer.Ordinal), newRows.Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var handle in oldRows.Keys.Where(k => k != "0xHTIEX" && k != "0xHOLDTIE"))
        {
            Assert.Equal(oldRows[handle], newRows[handle]);
        }

        Assert.DoesNotContain("0xHADHOC", newRows.Keys);
        Assert.Equal(new Module("usp_1", "dbo", "db1"), newRows["0xH1"]);
        Assert.Equal(new Module("usp_tie", "dbo", "dbtie"), newRows["0xHTIE"]);
        Assert.Equal(new Module("usp_tie_high", "dbo", "dbx"), newRows["0xHTIEX"]);
        Assert.Contains(oldRows["0xHTIEX"].ObjectName, new[] { "usp_tie_low", "usp_tie_high" });
        Assert.Contains(oldRows["0xHOLDTIE"].ObjectName, new[] { "usp_oldtie_low", "usp_oldtie_high" });
        Assert.Equal(new Module(null, null, null), newRows["0xHNULL"]);
        Assert.Equal(new Module("usp_ancient", "dbo", "dba"), newRows["0xHANCIENT"]);
        Assert.Equal(new Module("usp_old_newest", "dbo", "dbd"), newRows["0xHOLD"]);
        Assert.Equal(new Module("usp_oldtie_high", "dbo", "dbd"), newRows["0xHOLDTIE"]);
        Assert.Equal(new Module("usp_both_recent", "dbo", "dbb"), newRows["0xHBOTH"]);
        Assert.Equal(NormalRanked + 7, newRows.Count);

        /* The cost: temp blocks and the aggregate's groups, from EXPLAIN (ANALYZE, BUFFERS). */
        var oldPlan = await ExplainAsync(connection, oldSql, ranked, ct);
        var newPlan = await ExplainAsync(connection, newSql, ranked, ct);

        var oldTemp = MaxTempWritten(oldPlan);
        Assert.True(oldTemp > 0,
            $"the old statement wrote {oldTemp} temp blocks; the seed no longer forces its sort to spill, so this test would prove nothing: {oldPlan.GetRawText()}");

        var newTemp = MaxTempWritten(newPlan);
        Assert.True(newTemp == 0,
            $"the module lookup wrote {newTemp} temp blocks (the old statement's sort wrote {oldTemp}); it should aggregate, not sort: {newPlan.GetRawText()}");

        var groups = HandleAggregateRows(newPlan);
        Assert.True(groups.Count > 0, "no aggregate grouped on sql_handle was found in the module lookup's plan: " + newPlan.GetRawText());
        Assert.True(groups.All(g => g <= ranked.Length),
            $"the module lookup's aggregate kept {string.Join(", ", groups)} groups for {ranked.Length} ranked handles; it should key on the ranked handles, not on the {Procs} the server retains: {newPlan.GetRawText()}");

        /* The rows read: with every ranked handle recent, the older history is never read (its scan is a "never executed" node). A ranked
           handle with no recent row (ad hoc, or a handle whose newest row is old) is the only reason to read it, and then it is read once,
           filtered to those handles. Rows scanned, not time or blocks: the probes read a whole snapshot's rows each, so a block count scales with
           the seed, but the history's row count does not hide. */
        var retained = Procs * SnapshotsPerProc;
        var recentOnly = Enumerable.Range(1, NormalRanked).Select(p => "0xH" + p).Concat(new[] { "0xHTIE", "0xHBOTH" }).ToArray();
        var recentOnlyPlan = await ExplainAsync(connection, newSql, recentOnly, ct);
        var oldRecentOnlyPlan = await ExplainAsync(connection, oldSql, recentOnly, ct);
        var recentOnlyRows = ProcedureStatsRowsScanned(recentOnlyPlan);
        var oldRecentOnlyRows = ProcedureStatsRowsScanned(oldRecentOnlyPlan);
        var withFallbackRows = ProcedureStatsRowsScanned(newPlan);
        Assert.True(oldRecentOnlyRows >= retained,
            $"the old statement scanned {oldRecentOnlyRows} procedure_stats rows for {retained} retained; the seed no longer makes it read the history, so this test would prove nothing: {oldRecentOnlyPlan.GetRawText()}");
        Assert.True(withFallbackRows >= retained * 0.9,
            $"with handles that need the older history the lookup scanned {withFallbackRows} procedure_stats rows for {retained} retained; the fallback did not read it: {newPlan.GetRawText()}");
        Assert.True(recentOnlyRows < retained / 4,
            $"with every ranked handle recent the lookup scanned {recentOnlyRows} procedure_stats rows for {retained} retained; the fallback should cost nothing when it has no handles: {recentOnlyPlan.GetRawText()}");
    }

    /// <summary>
    /// #8549: the grid shows a group's host_object_name when it has one and the stitch's module only otherwise. A query_stats row of the server with a
    /// host_object_name in the hour before the window start proves the build collecting then fills the column, so every NULL host in the window is
    /// ad hoc and the lookup reads nothing. Without such a row it reads the NULL-host groups' handles only. What the grid shows is the same as
    /// the statement that read the stitch for every ranked handle.
    /// </summary>
    [Fact]
    public async Task ModuleLookup_ReadsNoProcedureStatsWithAHostNamedRowBeforeTheWindow_AndTheGridShowsTheSameModule()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8549 module attribution test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const int snapshots = 200;
        await ExecAsync(connection,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                           sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time)
              SELECT (p::bigint * 100000) + s, $1 - make_interval(mins => s), $2, 'module-gate', 'db' || (p % 3), 'dbo', 'usp_' || p, 'PROCEDURE',
                     '0xH' || p, '0xP' || p || '_' || s, 1, 1, 1
              FROM generate_series(1, $3) AS p, generate_series(0, $4 - 1) AS s",
            ct, NewestRow, ServerId, Procs, snapshots);
        /* A handle whose only row is five hours old: before the window start, so only the older-history read can name it. */
        await ExecAsync(connection,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                           sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time)
              VALUES (1, $1, $2, 'module-gate', 'dbold', 'dbo', 'usp_old_only', 'PROCEDURE', '0xHOLDONLY', '0xPOLD', 1, 1, 1)",
            ct, NewestRow.AddHours(-5), ServerId);
        await ExecAsync(connection, "ANALYZE procedure_stats", ct);
        await ExecAsync(connection, "SET max_parallel_workers_per_gather = 0", ct);

        var retained = Procs * snapshots + 1;
        var before = Standalone(BeforeGatesModuleCte);
        var after = Standalone(ViewerDataService.TopQueriesModuleCte);

        /* The query_stats rows the lookup's probe reads: one per entry, collected at the given time for the given server, with or without a host. */
        async Task SetQueryStatsAsync(params (DateTime At, int Server, string? Host)[] rows)
        {
            await ExecAsync(connection, "DELETE FROM query_stats", ct);
            var i = 0;
            foreach (var (at, server, host) in rows)
            {
                await ExecAsync(connection,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                                               last_execution_time, creation_time, sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                               delta_logical_reads, delta_rows, delta_logical_writes, delta_physical_reads, delta_spills, total_clr_time, plan_generation_num,
                                               query_text, host_object_name)
                      VALUES ($1, $2, $3, 'module-gate', 'db', '0xQ' || $1::text, '0xPLANHASH', '0xHQS' || $1::text, '0xPLANHANDLE', $2, $2, 60, 5, 1, 1, 10, 0, 0, 0, 0, 0, 1, 'SELECT 1', $4)",
                    ct, (long)++i, at, server, host!);
            }

            await ExecAsync(connection, "ANALYZE query_stats", ct);
        }

        /* What the grid shows for each ranked group: its host when it has one, else the stitch's module, else 'ad hoc' (ViewerQueryStatsRow.ModuleName). */
        static List<string> Shown(Dictionary<string, Module> modules, (string Handle, string? Host)[] groups) =>
            groups.Select(g => g.Host is not null
                ? "host " + g.Host
                : modules.TryGetValue(g.Handle, out var m) ? $"{m.DatabaseName}.{m.SchemaName}.{m.ObjectName}" : "ad hoc").ToList();

        /* A host-named row of this server in the hour before the window start: ad hoc groups (handles with no procedure_stats row) and host-named
           groups, which is every group a collector that fills host_object_name can produce. The statement before the gate read the history to
           prove the ad hoc handles absent. */
        await SetQueryStatsAsync((WindowStart.AddMinutes(-40), ServerId, "dbo.usp_collected"));
        var withProof = new (string Handle, string? Host)[]
        {
            ("0xHADHOC1", null), ("0xHADHOC2", null), ("0xH1", "dbo.usp_1"), ("0xH2", "dbo.usp_2"), ("0xHOLDONLY", "dbo.usp_old_only"),
        };
        var proofHandles = withProof.Select(g => g.Handle).ToArray();
        var proofHosts = withProof.Select(g => g.Host).ToArray();
        var proofBeforeRows = await ReadModulesAsync(connection, before, proofHandles, ct, proofHosts);
        var proofAfterRows = await ReadModulesAsync(connection, after, proofHandles, ct, proofHosts);
        Assert.NotEmpty(proofBeforeRows);
        Assert.Empty(proofAfterRows);
        Assert.Equal(Shown(proofBeforeRows, withProof), Shown(proofAfterRows, withProof));
        var proofBeforePlan = await ExplainAsync(connection, before, proofHandles, ct, proofHosts);
        var proofAfterPlan = await ExplainAsync(connection, after, proofHandles, ct, proofHosts);
        Assert.True(ProcedureStatsRowsScanned(proofBeforePlan) >= retained * 0.9,
            $"the statement before the gate scanned {ProcedureStatsRowsScanned(proofBeforePlan)} procedure_stats rows for {retained} retained; the seed no longer makes it read the history: {proofBeforePlan.GetRawText()}");
        Assert.True(ProcedureStatsRowsScanned(proofAfterPlan) == 0,
            $"a window with a host-named row before it still read {ProcedureStatsRowsScanned(proofAfterPlan)} procedure_stats rows: {proofAfterPlan.GetRawText()}");

        /* No host-named row in the hour before: groups with a NULL host still get their module, from the recent rows (0xH5) and from the older
           history (0xHOLDONLY), and an ad hoc group has none. The host-named group's handle (0xH1) is no longer read. Each of these stores has
           query_stats rows that are NOT proof: none at all, a NULL-host row in the hour, a host-named row older than the hour or inside the
           window, and a host-named row of another server in the hour. */
        var noProof = new (string Handle, string? Host)[]
        {
            ("0xH5", null), ("0xHOLDONLY", null), ("0xHADHOC1", null), ("0xH1", "dbo.usp_1"),
        };
        var noProofHandles = noProof.Select(g => g.Handle).ToArray();
        var noProofHosts = noProof.Select(g => g.Host).ToArray();
        foreach (var rows in new (DateTime At, int Server, string? Host)[][]
        {
            Array.Empty<(DateTime, int, string?)>(),
            new[] { (WindowStart.AddMinutes(-40), ServerId, (string?)null) },
            new[] { (WindowStart.AddMinutes(-61), ServerId, (string?)"dbo.usp_collected") },
            new[] { (WindowStart.AddMinutes(1), ServerId, (string?)"dbo.usp_collected") },
            new[] { (WindowStart.AddMinutes(-40), OtherServerId, (string?)"dbo.usp_collected") },
        })
        {
            await SetQueryStatsAsync(rows);
            var beforeRows = await ReadModulesAsync(connection, before, noProofHandles, ct, noProofHosts);
            var afterRows = await ReadModulesAsync(connection, after, noProofHandles, ct, noProofHosts);
            var shown = Shown(afterRows, noProof);
            Assert.Equal(Shown(beforeRows, noProof), shown);
            Assert.Equal(new[] { "db2.dbo.usp_5", "dbold.dbo.usp_old_only", "ad hoc", "host dbo.usp_1" }, shown);
            var beforePlan = await ExplainAsync(connection, before, noProofHandles, ct, noProofHosts);
            var plan = await ExplainAsync(connection, after, noProofHandles, ct, noProofHosts);
            Assert.True(ProcedureStatsRowsScanned(plan) > 0, "a window with no host-named row before it must read procedure_stats: " + plan.GetRawText());
            Assert.True(HandleAggregateRows(plan).Sum() < HandleAggregateRows(beforePlan).Sum(),
                $"the host-named group's handle should no longer be aggregated: {string.Join(", ", HandleAggregateRows(plan))} groups against {string.Join(", ", HandleAggregateRows(beforePlan))} before: {plan.GetRawText()}");
        }
    }

    private static string Standalone(string moduleCte) =>
        "WITH ranked AS MATERIALIZED (SELECT sql_handle, host_object_name FROM unnest($3::text[], $4::text[]) AS u(sql_handle, host_object_name)),\n" + moduleCte
        + "\nSELECT sql_handle, object_name, schema_name, database_name FROM module";

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, string[] ranked, string?[]? hosts = null)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = WindowStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = ranked });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = hosts ?? new string?[ranked.Length] });
        return command;
    }

    private static async Task<Dictionary<string, Module>> ReadModulesAsync(NpgsqlConnection connection, string sql, string[] ranked, CancellationToken ct, string?[]? hosts = null)
    {
        var rows = new Dictionary<string, Module>(StringComparer.Ordinal);
        await using var command = Command(connection, sql, ranked, hosts);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0), new Module(
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return rows;
    }

    private static async Task<JsonElement> ExplainAsync(NpgsqlConnection connection, string sql, string[] ranked, CancellationToken ct, string?[]? hosts = null)
    {
        await using var command = Command(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, ranked, hosts);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        return JsonDocument.Parse(json).RootElement[0].GetProperty("Plan").Clone();
    }

    private static IEnumerable<JsonElement> Walk(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var below in Walk(child))
                {
                    yield return below;
                }
            }
        }
    }

    /// <summary>The most temp blocks any node wrote: a node's count includes its children, so the root's is the largest, and a node that spilled is found wherever it sits.</summary>
    private static long MaxTempWritten(JsonElement plan) =>
        Walk(plan).Max(n => n.TryGetProperty("Temp Written Blocks", out var v) ? v.GetInt64() : 0);

    /// <summary>The procedure_stats rows the plan's scan nodes produced (rows times loops); a node that never ran counts as none.</summary>
    private static double ProcedureStatsRowsScanned(JsonElement plan) =>
        Walk(plan)
            .Where(n => n.TryGetProperty("Relation Name", out var r) && r.GetString() == "procedure_stats")
            .Sum(n => n.GetProperty("Actual Rows").GetDouble() * n.GetProperty("Actual Loops").GetDouble());

    /// <summary>The rows each aggregate grouped on sql_handle produced (rows times loops).</summary>
    private static List<double> HandleAggregateRows(JsonElement plan) =>
        Walk(plan)
            .Where(n => n.TryGetProperty("Node Type", out var t) && t.GetString() == "Aggregate"
                && n.TryGetProperty("Group Key", out var keys)
                && keys.EnumerateArray().Any(k => (k.GetString() ?? string.Empty).Contains("sql_handle", StringComparison.Ordinal)))
            .Select(n => n.GetProperty("Actual Rows").GetDouble() * n.GetProperty("Actual Loops").GetDouble())
            .ToList();

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
