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
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that adds the hourly row-count ledger of <c>collect.query_stats</c> (#4605): two plain collect
/// tables, the one-row state table's starting hour, and the two seams the next lanes build on, <c>UpsertSql</c> and
/// <c>RecountSql</c>. Every fact finds the rung by NAME. The live facts mint their own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStatsHourLedgerTests
{
    public const string RungName = "query-stats-hour-ledger";

    private const int ProbeOrdinal = 139;

    private const int ServerA = -945101;
    private const int ServerB = -945102;
    private const string NameA = "LedgerServerA\\INST";
    private const string NameB = "ledgerserverb";

    /// <summary>Fixed anchor, never wall-clock relative: whole hours.</summary>
    private static readonly DateTime H10 = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    public static int RungVersion => Rung.Version;

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    private static List<(string Name, string Type)> ColumnsOf(string table)
    {
        var sql = Statements();
        var header = "CREATE TABLE IF NOT EXISTS " + table + "\n(";
        var start = sql.IndexOf(header, StringComparison.Ordinal);
        Assert.True(start >= 0, "no CREATE TABLE for " + table);
        var end = sql.IndexOf("\n)", start, StringComparison.Ordinal);
        var columns = new List<(string, string)>();
        foreach (var raw in sql[(start + header.Length)..end].Split('\n'))
        {
            var line = raw.Trim().TrimEnd(',');
            if (line.Length == 0 || line.StartsWith("CONSTRAINT", StringComparison.Ordinal))
            {
                continue;
            }

            var space = line.IndexOf(' ', StringComparison.Ordinal);
            columns.Add((line[..space], line[(space + 1)..].Trim()));
        }

        return columns;
    }

    [Fact]
    public void TheRungIsRegisteredOnce_InADenseLadder_AsTheTopRung()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(164, Rung.Version);
        Assert.Equal(QueryStatsHourLedger.RungVersion, Rung.Version); // the runner's below-the-rung check reads this constant
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    [Fact]
    public void TheTablesArePlain_SchemaQualified_Guarded_AndOnlyTheStateRowIsInserted()
    {
        var sql = Statements();

        Assert.DoesNotContain("create_hypertable", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("timescaledb", sql, StringComparison.OrdinalIgnoreCase);
        foreach (var match in Regex.Matches(sql, @"(?:CREATE TABLE IF NOT EXISTS|INSERT INTO)\s+(\S+)").Cast<Match>())
        {
            Assert.Matches(@"^collect\.", match.Groups[1].Value);
        }

        foreach (var create in Regex.Matches(sql, @"CREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX)\s+(?!IF NOT EXISTS)", RegexOptions.IgnoreCase).Cast<Match>())
        {
            Assert.Fail("an unguarded CREATE: " + create.Value);
        }

        foreach (var verb in new[] { "ALTER ", "UPDATE ", "DELETE ", "GRANT ", "DROP ", "CREATE INDEX" })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.OrdinalIgnoreCase);
        }

        /* The census treats an INSERT ... SELECT on an earlier rung's table as data-moving; this one is a VALUES row into a
           table the same rung creates, and a re-run must keep the first counted_since. */
        Assert.Single(Regex.Matches(sql, @"INSERT\s+INTO", RegexOptions.IgnoreCase));
        Assert.Contains("INSERT INTO collect.query_stats_hour_ledger_state (id, counted_since)", sql, StringComparison.Ordinal);
        Assert.Contains("VALUES (1, date_trunc('hour', now() AT TIME ZONE 'UTC') + interval '1 hour')", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (id) DO NOTHING;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheColumnsTypesAndKeys_ArePinned()
    {
        Assert.Equal(
            ["server_id:integer NOT NULL", "server_name:text NOT NULL", "bucket:timestamp NOT NULL", "n:bigint NOT NULL"],
            ColumnsOf(QueryStatsHourLedger.LedgerTable).Select(c => c.Name + ":" + c.Type).ToArray());
        Assert.Equal(
            ["id:integer NOT NULL", "counted_since:timestamp NOT NULL"],
            ColumnsOf(QueryStatsHourLedger.StateTable).Select(c => c.Name + ":" + c.Type).ToArray());

        var sql = Statements();
        /* Hour first: the guard reads a range of hours across every server and the prune deletes below one hour. */
        Assert.Contains("CONSTRAINT pk_query_stats_hour_ledger PRIMARY KEY (bucket, server_id, server_name)", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT pk_query_stats_hour_ledger_state PRIMARY KEY (id)", sql, StringComparison.Ordinal);
        Assert.Contains("CHECK (id = 1)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void NeitherTableIsACollectorTable()
    {
        foreach (var table in new[] { "query_stats_hour_ledger", "query_stats_hour_ledger_state" })
        {
            Assert.DoesNotContain(CollectorCatalog.All, c => c.TargetTable == table);
        }
    }

    [Fact]
    public void TheSeams_CountTheRollupsPopulation_ByTheHour_AndNeedNoTimescale()
    {
        /* The rollup counts rows under this exact predicate; the recount says the same. The guard has no raw side any more: it
           reads the ledger, so it names this table and has no scan of collect.query_stats (the lookahead keeps
           query_stats_hour_ledger and query_stats_interval_hourly from counting as one). */
        const string predicate = "sample_interval_seconds IS DISTINCT FROM 0";
        Assert.Contains(predicate, TimescaleSupport.CreateQueryStatsIntervalHourlySql, StringComparison.Ordinal);
        Assert.Contains(predicate, QueryStatsHourLedger.RecountSql, StringComparison.Ordinal);
        Assert.Contains($"FROM {QueryStatsHourLedger.LedgerTable}", IntervalRollupCountGuard.QueryStatsSql, StringComparison.Ordinal);
        Assert.Contains($"FROM {QueryStatsHourLedger.StateTable}", IntervalRollupCountGuard.QueryStatsSql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bcollect\.query_stats(?![\w$])", IntervalRollupCountGuard.QueryStatsSql);
        Assert.DoesNotContain(predicate, IntervalRollupCountGuard.QueryStatsSql, StringComparison.Ordinal);

        foreach (var sql in new[] { QueryStatsHourLedger.UpsertSql, QueryStatsHourLedger.RecountSql })
        {
            Assert.Contains("date_trunc('hour'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("time_bucket", sql, StringComparison.Ordinal);
        }

        Assert.Contains("ON CONFLICT (bucket, server_id, server_name) DO UPDATE SET n = ledger.n + EXCLUDED.n", QueryStatsHourLedger.UpsertSql, StringComparison.Ordinal);
        Assert.Contains("WHERE $4::bigint > 0", QueryStatsHourLedger.UpsertSql, StringComparison.Ordinal);
        Assert.Contains("DO UPDATE SET n = EXCLUDED.n", QueryStatsHourLedger.RecountSql, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM collect.query_stats_hour_ledger", QueryStatsHourLedger.RecountSql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_stats_hour_ledger_state", QueryStatsHourLedger.RecountSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeCarriesTheLedgerTable_AsTheTopArm_AndMapsFullyMigratedToThisRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = "to_regclass('collect.query_stats_hour_ledger') IS NOT NULL";
        Assert.Contains(arm, probe, StringComparison.Ordinal);
        Assert.True(probe.LastIndexOf("EXISTS", StringComparison.Ordinal) < probe.IndexOf(arm, StringComparison.Ordinal),
            "the new arm is the probe's last EXISTS, so it reads at the next ordinal");

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal(ProbeOrdinal, parameters.Length - 1);
        Assert.Equal("hasQueryStatsHourLedger", parameters[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(Rung.Version, (int)method.Invoke(null, all)!);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(Rung.Version - 1, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasQueryStatsHourLedger)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasStoreStatementHistory)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0 && thisArm < previousArm, "this arm sits above the previous rung's");
        Assert.Contains($"return {Rung.Version};", viewer[thisArm..previousArm], StringComparison.Ordinal);

        var armProseStart = viewer.LastIndexOf("/* V164 (#4605)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain("query_stats_hour_ledger", viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigratingAFreshStore_CreatesBothTables_SeedsTheStateRowAtTheNextWholeHour_AndRerunningKeepsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the hour ledger rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var before = DateTime.UtcNow;
        await PgMigrations.MigrateAsync(connection, ct);
        var after = DateTime.UtcNow;

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(2L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relkind = 'r' AND c.relname IN ('query_stats_hour_ledger','query_stats_hour_ledger_state')", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger_state", ct));

            var countedSince = (DateTime)(await ScalarAsync(connection, "SELECT counted_since FROM collect.query_stats_hour_ledger_state WHERE id = 1", ct))!;
            Assert.Equal(new DateTime(countedSince.Year, countedSince.Month, countedSince.Day, countedSince.Hour, 0, 0), countedSince);
            Assert.True(countedSince > before, "counted_since is after the moment the rung ran: the hour it ran in is only partly counted");
            Assert.True(countedSince <= after.AddHours(1), "counted_since is the NEXT whole hour, no later");

            /* A re-run (the rung is applied again) keeps whatever counted_since says, so a backfill's earlier value survives. */
            await ExecAsync(connection, "UPDATE collect.query_stats_hour_ledger_state SET counted_since = counted_since - interval '5 days'", ct);
            var moved = (DateTime)(await ScalarAsync(connection, "SELECT counted_since FROM collect.query_stats_hour_ledger_state", ct))!;
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 164", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger_state", ct));
            Assert.Equal(moved, (DateTime)(await ScalarAsync(connection, "SELECT counted_since FROM collect.query_stats_hour_ledger_state", ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task UpsertSql_AddsToAnExistingBucket_StartsANewOne_AndIgnoresACountOfZeroOrLess()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the hour ledger's live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* A batch lands mid-hour: the bucket is the whole hour, and the first count inserts. */
            Assert.Equal(1, await UpsertAsync(connection, ServerA, NameA, H10.AddMinutes(20).AddSeconds(30), 3, ct));
            Assert.Equal(3L, await LedgerAsync(connection, ServerA, NameA, H10, ct));

            /* Another batch in the same hour, at its last microsecond, adds to it. */
            Assert.Equal(1, await UpsertAsync(connection, ServerA, NameA, H10.AddHours(1).AddTicks(-10), 4, ct));
            Assert.Equal(7L, await LedgerAsync(connection, ServerA, NameA, H10, ct));

            /* The next whole hour, another server id, and another spelling of the name are each their own row. */
            await UpsertAsync(connection, ServerA, NameA, H10.AddHours(1), 2, ct);
            await UpsertAsync(connection, ServerB, NameA, H10, 5, ct);
            await UpsertAsync(connection, ServerA, NameB, H10, 6, ct);
            Assert.Equal(2L, await LedgerAsync(connection, ServerA, NameA, H10.AddHours(1), ct));
            Assert.Equal(5L, await LedgerAsync(connection, ServerB, NameA, H10, ct));
            Assert.Equal(6L, await LedgerAsync(connection, ServerA, NameB, H10, ct));
            Assert.Equal(7L, await LedgerAsync(connection, ServerA, NameA, H10, ct));

            /* A count of 0 (a restart-only batch) or less writes nothing: the rollup has no row for such an hour. */
            Assert.Equal(0, await UpsertAsync(connection, ServerA, NameA, H10.AddHours(5), 0, ct));
            Assert.Equal(0, await UpsertAsync(connection, ServerA, NameA, H10, -3, ct));
            Assert.Equal(7L, await LedgerAsync(connection, ServerA, NameA, H10, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger WHERE bucket = '2026-01-05 15:00:00'", ct));
            Assert.Equal(4L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task RecountSql_RecomputesARangeFromRaw_SkipsRestartRows_CountsNullIntervals_AndKeepsTheRawNameSpelling()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the hour ledger's live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, NameB, ct);

        var bodySucceeded = false;
        try
        {
            /* Server A, hour 10: two measured rows, one restart row (0, not counted), one NULL-interval row (counted) = 3.
               Hour 11: a row exactly on the hour (the start is inclusive) = 1. Server B, hour 11: two rows = 2, and in
               hour 10 only a restart row, so no ledger row at all. A row exactly at 12:00 is outside [10:00, 12:00). */
            await PlantAsync(connection, ct, ServerA, NameA, H10.AddMinutes(5), 3600);
            await PlantAsync(connection, ct, ServerA, NameA, H10.AddMinutes(25), 3600);
            await PlantAsync(connection, ct, ServerA, NameA, H10.AddMinutes(45), 0);
            await PlantAsync(connection, ct, ServerA, NameA, H10.AddMinutes(50), null);
            await PlantAsync(connection, ct, ServerA, NameA, H10.AddHours(1), 3600);
            await PlantAsync(connection, ct, ServerB, NameB, H10.AddMinutes(30), 0);
            await PlantAsync(connection, ct, ServerB, NameB, H10.AddHours(1).AddMinutes(10), 3600);
            await PlantAsync(connection, ct, ServerB, NameB, H10.AddHours(1).AddMinutes(40), 3600);
            await PlantAsync(connection, ct, ServerA, NameA, H10.AddHours(2), 3600);

            /* A ledger row outside the range must survive the recount untouched. */
            await UpsertAsync(connection, ServerA, NameA, H10.AddHours(2), 9, ct);

            var first = await RecountAsync(connection, H10, H10.AddHours(2), ct);
            Assert.Equal((3L, 0L), first);
            Assert.Equal(3L, await LedgerAsync(connection, ServerA, NameA, H10, ct));
            Assert.Equal(1L, await LedgerAsync(connection, ServerA, NameA, H10.AddHours(1), ct));
            Assert.Equal(2L, await LedgerAsync(connection, ServerB, NameB, H10.AddHours(1), ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger WHERE server_id = -945102 AND bucket = '2026-01-05 10:00:00'", ct));
            Assert.Equal(9L, await LedgerAsync(connection, ServerA, NameA, H10.AddHours(2), ct));

            /* The name is stored exactly as raw holds it, backslash and case included, so it joins the rollup's row. */
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger WHERE server_name = 'LedgerServerA\\INST' AND bucket = '2026-01-05 10:00:00'", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger WHERE server_name = 'ledgerserverb'", ct));

            /* A recount SETS the count from raw, it does not add to it: a new raw row, a second recount, 4 and not 7. */
            await PlantAsync(connection, ct, ServerA, NameA, H10.AddMinutes(55), 3600);
            Assert.Equal((3L, 0L), await RecountAsync(connection, H10, H10.AddHours(2), ct));
            Assert.Equal(4L, await LedgerAsync(connection, ServerA, NameA, H10, ct));

            /* A ragged range is widened to whole hours, so no partial hour is ever written: from 10:20 to 11:40 still
               counts every row of hours 10 and 11 (a count from 10:20 would give 3 for hour 10). */
            await ExecAsync(connection, "DELETE FROM collect.query_stats_hour_ledger WHERE bucket < '2026-01-05 12:00:00'", ct);
            Assert.Equal((3L, 0L), await RecountAsync(connection, H10.AddMinutes(20), H10.AddHours(1).AddMinutes(40), ct));
            Assert.Equal(4L, await LedgerAsync(connection, ServerA, NameA, H10, ct));
            Assert.Equal(9L, await LedgerAsync(connection, ServerA, NameA, H10.AddHours(2), ct));

            /* A ledger row whose raw rows are gone is deleted by a recount of its hour. */
            await ExecAsync(connection, "DELETE FROM collect.query_stats WHERE server_id = -945102", ct);
            Assert.Equal((2L, 1L), await RecountAsync(connection, H10, H10.AddHours(2), ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger WHERE server_id = -945102", ct));

            /* The recount never touches the state row. */
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger_state", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheViewerRolesProvisionedGrants_ReadBothTables_AndWriteNeither()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the hour ledger's live pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;
        var roleName = "ledger_ro_" + Guid.NewGuid().ToString("N")[..8];

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var viewerStatements = ViewerGrantReplay.StatementsFor(roleName);
        Assert.Contains(viewerStatements, x => x.Contains("SELECT ON ALL TABLES IN SCHEMA collect", StringComparison.Ordinal));

        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD 'ledger-pin-password'", ct);
            foreach (var statement in viewerStatements)
            {
                await ExecAsync(connection, statement, ct);
            }

            /* No new GRANT exists for these tables: the collect schema's blanket SELECT (provisioning re-asserts it at every
               start, after migration) covers them. The role reads both and cannot write either. */
            var asViewer = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = roleName, Password = "ledger-pin-password" }.ConnectionString;
            await using var viewer = new NpgsqlConnection(asViewer);
            await viewer.OpenAsync(ct);
            Assert.Equal(0L, await ScalarAsync(viewer, "SELECT count(*) FROM collect.query_stats_hour_ledger", ct));
            Assert.Equal(1L, await ScalarAsync(viewer, "SELECT count(*) FROM collect.query_stats_hour_ledger_state", ct));

            var write = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(viewer,
                "INSERT INTO collect.query_stats_hour_ledger (server_id, server_name, bucket, n) VALUES (1, 'x', '2026-01-05 10:00:00', 1)", ct));
            Assert.Equal("42501", write.SqlState);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<int> UpsertAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, long n, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(QueryStatsHourLedger.UpsertSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = serverName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = at });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = n });
        return await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(long Written, long Deleted)> RecountAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(QueryStatsHourLedger.RecountSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = from });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = to });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private static async Task<long> LedgerAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime bucket, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT n FROM collect.query_stats_hour_ledger WHERE server_id = $1 AND server_name = $2 AND bucket = $3", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = serverName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = bucket });
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? -1L : Convert.ToInt64(value);
    }

    private static async Task PlantAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, int? intervalSeconds)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'LedgerDb', '0xLEDGER', '0xLEDGER', 1000, 900, 1, $5)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DarlingMcpTestData.TruncateToSeconds(at) });
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = intervalSeconds.HasValue ? intervalSeconds.Value : DBNull.Value });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
