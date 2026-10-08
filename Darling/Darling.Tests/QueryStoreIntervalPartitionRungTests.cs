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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact here mints its own scratch database through ScratchPostgres (the roles fact also a
   role with a per-run name, dropped in its finally) and never touches another test's rows, so the class is
   deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5571: V171 and V172 turn <c>collect.query_store_interval_wide</c> and <c>collect.query_store_interval_latest</c> into
/// tables partitioned by day, with the table that holds every row attached whole as <c>X_legacy</c>. The rung must be
/// catalog-only (no data copy, no scan, no index build), idempotent, and must leave every writer, reader and trigger
/// working through the new parent. Each fact starts from a store migrated to the top, rewound to the V170 shape by
/// <see cref="RewindToV170Async"/> (detach the legacy table, drop the parent, rename the table and its indexes back, delete the
/// two version rows), seeded across five days, and then migrated again, so the rung under test is the shipped one.
/// </summary>
public sealed class QueryStoreIntervalPartitionRungTests
{
    private const string Wide = "query_store_interval_wide";

    private const string Latest = "query_store_interval_latest";

    internal const int WideRungVersion = 171;

    internal const int LatestRungVersion = 172;

    private const int RowsPerTable = 100;

    private const int ServerId = -5571900;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration WideRung => PgMigrations.Scripts.Single(m => m.Version == WideRungVersion);

    private static PgMigrations.Migration LatestRung => PgMigrations.Scripts.Single(m => m.Version == LatestRungVersion);

    /// <summary>V168's trigger, as the rewind puts it back on the plain latest table. The rung's copy is checked against this text.</summary>
    private const string LatestTriggerSql = @"
CREATE TRIGGER trg_plan_regression_daily_late
    AFTER INSERT OR UPDATE ON collect.query_store_interval_latest
    FOR EACH ROW
    WHEN (NEW.first_execution_time < date_trunc('day', now() AT TIME ZONE 'UTC') - interval '1 day'
          AND NEW.first_execution_time >= date_trunc('day', now() AT TIME ZONE 'UTC') - interval '17 days')
    EXECUTE FUNCTION collect.plan_regression_daily_mark_late();";

    [Fact]
    public void TheRungsAreRegistered_DenseAndTheTop_AndShareOneBuilder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();
        Assert.Equal(new[] { 170, 171, 172 }, versions.OrderBy(v => v).TakeLast(3).ToArray());
        Assert.Equal("query-store-interval-wide-partitioned", WideRung.Name);
        Assert.Equal("query-store-interval-latest-partitioned", LatestRung.Name);
        Assert.Equal(LatestRungVersion, StorageVersion.SchemaVersion);
        Assert.Equal(LatestRungVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(WideRungVersion, PgMigrations.Scripts[^2].Version);

        /* Every statement is catalog-only, in the shipped text: the indexes are ON ONLY, nothing copies rows, and the
           parent has no WITH clause (PostgreSQL refuses storage parameters on a partitioned table). */
        foreach (var rung in new[] { WideRung, LatestRung })
        {
            var sql = rung.Sql;
            Assert.Contains("FOR VALUES FROM (MINVALUE) TO (MAXVALUE)", sql, StringComparison.Ordinal);
            Assert.Contains("SET LOCAL lock_timeout = '280s'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("INSERT INTO", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CONCURRENTLY", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("fillfactor", sql, StringComparison.OrdinalIgnoreCase);
            foreach (var line in sql.Split('\n').Select(l => l.Trim()).Where(l => l.StartsWith("CREATE ", StringComparison.Ordinal) && l.Contains(" INDEX ", StringComparison.Ordinal)))
            {
                Assert.Contains(" ON ONLY ", line, StringComparison.Ordinal);
            }
        }

        Assert.DoesNotContain("trg_plan_regression_daily_late", WideRung.Sql, StringComparison.Ordinal);
        Assert.Contains("DROP TRIGGER IF EXISTS trg_plan_regression_daily_late ON collect.query_store_interval_latest_legacy", LatestRung.Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time_brin", WideRung.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("brin", LatestRung.Sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TheRung_OnAStoreSeededAcrossFiveDays_PartitionsBothTablesAndKeepsEveryRow()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            var wideBefore = await CountAsync(connection, "collect." + Wide, ct);
            var latestBefore = await CountAsync(connection, "collect." + Latest, ct);
            Assert.Equal(RowsPerTable, wideBefore);
            Assert.Equal(RowsPerTable, latestBefore);
            Assert.Equal("r", await TextAsync(connection, "SELECT relkind::text FROM pg_class WHERE oid = 'collect." + Wide + "'::regclass", ct));

            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));

            foreach (var table in new[] { Wide, Latest })
            {
                Assert.Equal("p", await TextAsync(connection, "SELECT relkind::text FROM pg_class WHERE oid = 'collect." + table + "'::regclass", ct));
                Assert.Equal("r", await TextAsync(connection, "SELECT relkind::text FROM pg_class WHERE oid = 'collect." + table + "_legacy'::regclass", ct));
                Assert.Equal("RANGE (first_execution_time)", await TextAsync(connection, "SELECT pg_get_partkeydef('collect." + table + "'::regclass)", ct));

                /* The legacy table is the parent's only partition and takes every possible key. */
                Assert.Equal("query_store_interval_" + table.Split('_')[^1] + "_legacy|FOR VALUES FROM (MINVALUE) TO (MAXVALUE)", await TextAsync(connection,
                    "SELECT c.relname || '|' || pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'collect." + table + "'::regclass", ct));
                Assert.Equal(1, await CountAsync(connection, "pg_inherits WHERE inhparent = 'collect." + table + "'::regclass", ct));

                /* Every row is readable through the parent and still physically in the legacy table. */
                Assert.Equal(RowsPerTable, await CountAsync(connection, "collect." + table, ct));
                Assert.Equal(RowsPerTable, await CountAsync(connection, "collect." + table + "_legacy", ct));
                Assert.Equal(0, await CountAsync(connection, "ONLY collect." + table, ct));
                Assert.Equal(5, await CountAsync(connection, "(SELECT DISTINCT first_execution_time::date FROM collect." + table + ") AS d", ct));

                /* The legacy table keeps its fillfactor; the parent, as a partitioned table, has none. */
                Assert.Equal("{fillfactor=50}", await TextAsync(connection, "SELECT reloptions::text FROM pg_class WHERE oid = 'collect." + table + "_legacy'::regclass", ct));

                /* The unique index and the first-execution-time index exist on the parent, are valid, and have the legacy table's index as their child. */
                foreach (var index in new[] { "ux_" + table, "idx_" + table + "_first_exec" })
                {
                    Assert.Equal("I|true", await TextAsync(connection,
                        "SELECT c.relkind::text || '|' || i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indexrelid = 'collect." + index + "'::regclass", ct));
                    Assert.Equal(index + "_legacy", await TextAsync(connection,
                        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'collect." + index + "'::regclass", ct));
                }

                Assert.Equal(true, await ScalarAsync(connection,
                    "SELECT pg_get_indexdef('collect.ux_" + table + "'::regclass) LIKE '%UNIQUE%NULLS NOT DISTINCT%'", ct));
            }

            Assert.Equal(LatestRungVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT max(version) FROM darling_schema_version", ct), CultureInfo.InvariantCulture));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The attach must not scan. The column is NOT NULL, so the unbounded range's constraint (<c>IS NOT NULL</c>) is implied, and
    /// PostgreSQL says so at DEBUG1. Two instruments, plus a control that proves they can see a scan: the same attach with a
    /// bound that is NOT implied scans, counts a <c>seq_scan</c>, and emits no such line.
    /// </summary>
    [Fact]
    public async Task TheAttach_SkipsItsScan_ProvedByTheDebugLineAndTheSeqScanCounter()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var arrange = await ArrangeV170StoreAsync(scratch, ct))
            {
                await FlushStatsAsync(arrange, ct);
            }

            var seqBefore = new Dictionary<string, long>();
            await using (var reader = new NpgsqlConnection(scratch.ConnectionString))
            {
                await reader.OpenAsync(ct);
                foreach (var table in new[] { Wide, Latest })
                {
                    seqBefore[table] = await SeqScanAsync(reader, table, ct);
                }
            }

            /* The control first: an attach whose bound is not implied reads the table, and the instruments see it. */
            var controlNotices = new List<string>();
            long controlSeqBefore;
            long controlSeqAfter;
            await using (var control = new NpgsqlConnection(scratch.ConnectionString))
            {
                await control.OpenAsync(ct);
                control.Notice += (_, e) => controlNotices.Add(e.Notice.MessageText);
                await ExecAsync(control, "CREATE TABLE collect.qsp_ctl_parent (LIKE collect.query_store_interval_latest INCLUDING DEFAULTS) PARTITION BY RANGE (first_execution_time)", ct);
                await ExecAsync(control, "CREATE TABLE collect.qsp_ctl_child (LIKE collect.query_store_interval_latest INCLUDING DEFAULTS INCLUDING INDEXES)", ct);
                await ExecAsync(control, @"
INSERT INTO collect.qsp_ctl_child
(server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count, query_text)
SELECT 1, 'db', g, 1, NULL, g, TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-01', 'ph', 'qh', 1, 1, 1, TIMESTAMP '2026-01-01', false, 0, 'select 1'
FROM generate_series(1, 50) AS g", ct);
                await FlushStatsAsync(control, ct);
                controlSeqBefore = await SeqScanAsync(control, "qsp_ctl_child", ct);
                await ExecAsync(control, "SET client_min_messages = debug1", ct);
                await ExecAsync(control, "ALTER TABLE collect.qsp_ctl_parent ATTACH PARTITION collect.qsp_ctl_child FOR VALUES FROM ('2000-01-01') TO ('2100-01-01')", ct);
                await ExecAsync(control, "RESET client_min_messages", ct);
                await FlushStatsAsync(control, ct);
                controlSeqAfter = await SeqScanAsync(control, "qsp_ctl_child", ct);
                await ExecAsync(control, "DROP TABLE collect.qsp_ctl_parent, collect.qsp_ctl_child", ct);
            }

            Assert.True(controlSeqAfter > controlSeqBefore, "the control attach scanned, and the seq_scan counter must show it, or the counter proves nothing");
            Assert.DoesNotContain(controlNotices, n => n.Contains("is implied by existing constraints", StringComparison.Ordinal));

            var notices = new List<string>();
            await using (var migrator = new NpgsqlConnection(scratch.ConnectionString))
            {
                await migrator.OpenAsync(ct);
                migrator.Notice += (_, e) => notices.Add(e.Notice.MessageText);
                await ExecAsync(migrator, "SET client_min_messages = debug1", ct);
                Assert.Equal(2, await PgMigrations.MigrateAsync(migrator, ct));
                await FlushStatsAsync(migrator, ct);
            }

            foreach (var table in new[] { Wide, Latest })
            {
                Assert.Contains(notices, n => n.Contains("partition constraint for table \"" + table + "_legacy\" is implied by existing constraints", StringComparison.Ordinal));
            }

            await using (var reader = new NpgsqlConnection(scratch.ConnectionString))
            {
                await reader.OpenAsync(ct);
                foreach (var table in new[] { Wide, Latest })
                {
                    Assert.Equal(seqBefore[table], await SeqScanAsync(reader, table + "_legacy", ct));
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ReRunningTheRungs_ChangesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));
            var before = await CatalogSnapshotAsync(connection, ct);
            Assert.Contains("p:query_store_interval_wide", before, StringComparison.Ordinal);

            /* The migrator again does nothing at all. */
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

            /* And the rungs' own text, run again by hand, hits its relkind guard and changes nothing. */
            foreach (var rung in new[] { WideRung, LatestRung })
            {
                await using var transaction = await connection.BeginTransactionAsync(ct);
                await using (var command = new NpgsqlCommand(rung.Sql, connection, transaction))
                {
                    await command.ExecuteNonQueryAsync(ct);
                }

                await transaction.CommitAsync(ct);
            }

            Assert.Equal(before, await CatalogSnapshotAsync(connection, ct));
            Assert.Equal(RowsPerTable, await CountAsync(connection, "collect." + Wide, ct));
            Assert.Equal(RowsPerTable, await CountAsync(connection, "collect." + Latest, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The upgrade from V170 with the #5507 leftovers: the wide table's background btree is INVALID and its BRIN is missing, the
    /// latest table's btree is valid, and the latest table carries V168's trigger. The rung succeeds, drops the INVALID index,
    /// builds nothing for the missing one, attaches the valid one, and moves the trigger to the parent.
    /// </summary>
    [Fact]
    public async Task UpgradeFromV170_WithAnInvalidBtree_AMissingBrin_AndTheTrigger_SucceedsAndTidiesUp()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            await ExecAsync(connection, "CREATE INDEX ix_query_store_interval_wide_server_first_exec ON collect.query_store_interval_wide (server_id, first_execution_time)", ct);
            await ExecAsync(connection, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'collect.ix_query_store_interval_wide_server_first_exec'::regclass", ct);
            await ExecAsync(connection, "CREATE INDEX ix_query_store_interval_latest_server_first_exec ON collect.query_store_interval_latest (server_id, first_execution_time)", ct);
            Assert.Equal(1, await CountAsync(connection, "pg_trigger WHERE tgrelid = 'collect.query_store_interval_latest'::regclass AND NOT tgisinternal", ct));
            var triggerBefore = await TextAsync(connection, "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid = 'collect.query_store_interval_latest'::regclass AND NOT tgisinternal", ct);

            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));

            /* The INVALID btree is gone from both the legacy table and the parent; the missing BRIN was not built. */
            Assert.Null(await ScalarAsync(connection, "SELECT to_regclass('collect.ix_query_store_interval_wide_server_first_exec')::text", ct));
            Assert.Null(await ScalarAsync(connection, "SELECT to_regclass('collect.ix_query_store_interval_wide_server_first_exec_legacy')::text", ct));
            Assert.Equal(0, await CountAsync(connection, "pg_indexes WHERE schemaname = 'collect' AND tablename LIKE 'query_store_interval_wide%' AND indexdef ILIKE '%brin%'", ct));

            /* The valid one is attached, on a valid parent index. */
            Assert.Equal("I|true", await TextAsync(connection,
                "SELECT c.relkind::text || '|' || i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indexrelid = 'collect.ix_query_store_interval_latest_server_first_exec'::regclass", ct));
            Assert.Equal("ix_query_store_interval_latest_server_first_exec_legacy", await TextAsync(connection,
                "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'collect.ix_query_store_interval_latest_server_first_exec'::regclass", ct));

            /* The trigger exists once on the parent, with the text it had, and is cloned onto the legacy table. */
            Assert.Equal(1, await CountAsync(connection, "pg_trigger WHERE tgrelid = 'collect.query_store_interval_latest'::regclass AND tgname = 'trg_plan_regression_daily_late' AND tgparentid = 0", ct));
            Assert.Equal(1, await CountAsync(connection, "pg_trigger WHERE tgrelid = 'collect.query_store_interval_latest_legacy'::regclass AND tgname = 'trg_plan_regression_daily_late' AND tgparentid <> 0", ct));
            Assert.Equal(1, await CountAsync(connection, "pg_trigger WHERE tgname = 'trg_plan_regression_daily_late'::name AND tgparentid = 0", ct));
            var triggerAfter = await TextAsync(connection, "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid = 'collect.query_store_interval_latest'::regclass AND NOT tgisinternal", ct);
            Assert.Equal(triggerBefore.Replace("query_store_interval_latest", "X", StringComparison.Ordinal), triggerAfter.Replace("query_store_interval_latest", "X", StringComparison.Ordinal));
            Assert.Equal(0, await CountAsync(connection, "pg_trigger WHERE tgrelid = 'collect.query_store_interval_wide'::regclass AND NOT tgisinternal", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The valid-copy side of step 6: a valid btree and a valid autosummarize BRIN on the wide table both attach, with no build.</summary>
    [Fact]
    public async Task UpgradeFromV170_WithAValidBtreeAndAValidBrin_AttachesBothWithoutBuildingAnything()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            await ExecAsync(connection, "CREATE INDEX ix_query_store_interval_wide_server_first_exec ON collect.query_store_interval_wide (server_id, first_execution_time)", ct);
            await ExecAsync(connection, "CREATE INDEX ix_query_store_interval_wide_collection_time_brin ON collect.query_store_interval_wide USING brin (collection_time) WITH (autosummarize = on)", ct);
            const string names = "('ux_query_store_interval_wide', 'ix_query_store_interval_wide_server_first_exec', 'ix_query_store_interval_wide_collection_time_brin', 'idx_query_store_interval_wide_first_exec')";
            var filesBefore = await TextAsync(connection, "SELECT string_agg(relfilenode::text, ',' ORDER BY relname) FROM pg_class WHERE relname IN " + names, ct);
            Assert.Equal(4, filesBefore.Split(',').Length);

            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));

            foreach (var index in new[] { "ix_query_store_interval_wide_server_first_exec", "ix_query_store_interval_wide_collection_time_brin" })
            {
                Assert.Equal("I|true", await TextAsync(connection,
                    "SELECT c.relkind::text || '|' || i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indexrelid = 'collect." + index + "'::regclass", ct));
                Assert.Equal(index + "_legacy", await TextAsync(connection,
                    "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'collect." + index + "'::regclass", ct));
            }

            /* The same physical files under the renamed indexes: nothing was rebuilt. */
            Assert.Equal(filesBefore, await TextAsync(connection,
                "SELECT string_agg(relfilenode::text, ',' ORDER BY replace(relname, '_legacy', '')) FROM pg_class WHERE relname IN ('ux_query_store_interval_wide_legacy', 'ix_query_store_interval_wide_server_first_exec_legacy', 'ix_query_store_interval_wide_collection_time_brin_legacy', 'idx_query_store_interval_wide_first_exec_legacy')", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>Column order matters: the positional writers (V155's note) depend on it. The parent has the legacy table's columns in the legacy table's order.</summary>
    [Fact]
    public async Task TheParent_HasTheLegacyTablesColumnsInTheSameOrder_AndTheSameNullability()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            var legacyShape = new Dictionary<string, string>();
            foreach (var table in new[] { Wide, Latest })
            {
                legacyShape[table] = await ColumnShapeAsync(connection, "collect." + table, ct);
            }

            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));

            foreach (var table in new[] { Wide, Latest })
            {
                var parent = await ColumnShapeAsync(connection, "collect." + table, ct);
                Assert.Equal(legacyShape[table], parent);
                Assert.Equal(legacyShape[table], await ColumnShapeAsync(connection, "collect." + table + "_legacy", ct));
                Assert.Contains("first_execution_time:timestamp without time zone:notnull", parent, StringComparison.Ordinal);
            }

            /* V155's column is appended last and the writers rely on that. */
            Assert.EndsWith("interval_end_time_utc:timestamp without time zone:null", await ColumnShapeAsync(connection, "collect." + Wide, ct), StringComparison.Ordinal);
            Assert.Equal(58, (await ColumnShapeAsync(connection, "collect." + Wide, ct)).Split(',').Length);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// A role that could read the tables before the rung can read the new parents after it. The grants come from the schema's default
    /// privileges, set here as DarlingManagedRoles section 4 sets them for the owner.
    /// </summary>
    [Fact]
    public async Task ReaderRoles_CanSelectTheNewParents()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var role = "qsp_reader_" + Guid.NewGuid().ToString("N")[..12];
        var bodySucceeded = false;
        NpgsqlConnection? connection = null;
        try
        {
            connection = await ArrangeV170StoreAsync(scratch, ct);
            await ExecAsync(connection, "CREATE ROLE " + role + " NOLOGIN", ct);
            await ExecAsync(connection, "GRANT USAGE ON SCHEMA collect TO " + role, ct);
            await ExecAsync(connection, "GRANT SELECT ON ALL TABLES IN SCHEMA collect TO " + role, ct);
            await ExecAsync(connection, "ALTER DEFAULT PRIVILEGES FOR ROLE " + await TextAsync(connection, "SELECT current_user::text", ct) + " IN SCHEMA collect GRANT SELECT ON TABLES TO " + role, ct);

            await ExecAsync(connection, "SET ROLE " + role, ct);
            Assert.Equal(RowsPerTable, await CountAsync(connection, "collect." + Wide, ct));
            await ExecAsync(connection, "RESET ROLE", ct);

            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));

            await ExecAsync(connection, "SET ROLE " + role, ct);
            foreach (var table in new[] { Wide, Latest })
            {
                Assert.Equal(RowsPerTable, await CountAsync(connection, "collect." + table, ct));
                Assert.Equal(RowsPerTable, await CountAsync(connection, "collect." + table + "_legacy", ct));
            }

            /* Read-only: the role gets no write on the parent. */
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "DELETE FROM collect." + Latest, ct));
            await ExecAsync(connection, "RESET ROLE", ct);
            bodySucceeded = true;
        }
        finally
        {
            if (connection is not null)
            {
                try
                {
                    await ExecAsync(connection, "RESET ROLE", CancellationToken.None);
                    await ExecAsync(connection, "DROP OWNED BY " + role, CancellationToken.None);
                    await ExecAsync(connection, "DROP ROLE IF EXISTS " + role, CancellationToken.None);
                }
                finally
                {
                    await connection.DisposeAsync();
                }
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The writers' upsert, through the parent: a new row inserts, the same identity with a higher count updates (the ON CONFLICT
    /// target is the parent's unique index, which must be valid), and nothing duplicates, in both tables, whose first
    /// execution times fall in the legacy partition.
    /// </summary>
    [Fact]
    public async Task TheWritersUpsert_InsertsThenUpdates_ThroughTheParent()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));
            Assert.True(await IndexIsValidAsync(connection, "ux_" + Wide, ct));
            Assert.True(await IndexIsValidAsync(connection, "ux_" + Latest, ct));

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
            var context = new CollectorContext { ServerId = ServerId, ServerName = "qsp-5571-host", CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator() };
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "qsp-5571", Host = "qsp-5571-host" },
                ConnectionString = "Server=qsp-5571-host",
                Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                StorageName = "qsp-5571-host",
                ServerId = ServerId,
                EngineEdition = 3,
            };

            var first = new DateTime(2026, 9, 1, 0, 0, 0, 123, DateTimeKind.Unspecified).AddTicks(4560);
            QueryStoreCollector.Row RowOf(long executions) => new()
            {
                DatabaseName = "qsp",
                QueryId = 1,
                PlanId = 11,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(9),
                QueryHash = "0x00000001",
                QueryPlanHash = "0x0000000B",
                ExecutionCount = executions,
                AvgCpuTimeUs = 500,
                AvgDurationUs = 1000,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = 100,
            };

            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { RowOf(10) }, server, first.AddMinutes(10), context, ct);
            foreach (var table in new[] { Wide, Latest })
            {
                Assert.Equal(1, await CountAsync(connection, "collect." + table + " WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), ct));
                Assert.Equal(10L, Convert.ToInt64(await ScalarAsync(connection, "SELECT execution_count FROM collect." + table + " WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), ct), CultureInfo.InvariantCulture));
            }

            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { RowOf(20) }, server, first.AddMinutes(40), context, ct);
            foreach (var table in new[] { Wide, Latest })
            {
                Assert.Equal(1, await CountAsync(connection, "collect." + table + " WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), ct));
                Assert.Equal(20L, Convert.ToInt64(await ScalarAsync(connection, "SELECT execution_count FROM collect." + table + " WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), ct), CultureInfo.InvariantCulture));

                /* Microseconds are kept: the sub-millisecond part of the key survived the round trip through the parent. */
                Assert.Equal(456L, Convert.ToInt64(await ScalarAsync(connection,
                    "SELECT (extract(microseconds FROM first_execution_time)::bigint % 1000) FROM collect." + table + " WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), ct), CultureInfo.InvariantCulture));

                /* The rows live in the legacy partition. */
                Assert.Equal(1, await CountAsync(connection, "collect." + table + "_legacy WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The latest table's late-row trigger still fires for a row written through the parent: a steady row does not run it, a row from a closed day does.</summary>
    [Fact]
    public async Task TheLatestTablesTrigger_FiresForARowWrittenThroughTheParent()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));
            await ExecAsync(connection, "TRUNCATE collect.plan_regression_daily_built", ct);

            const string insert = @"
INSERT INTO collect.query_store_interval_latest
(server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count, query_text)
SELECT -5571901, 'trg', 900000, 1, NULL, 900000, {0}, {0}, 'ph', 'qh', 1, 1, 1, {0}, false, 0, 'select 1'";

            /* Steady: now. */
            await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, insert, "(now() AT TIME ZONE 'UTC')"), ct);
            Assert.Equal(0, await CountAsync(connection, "collect.plan_regression_daily_built", ct));

            /* Late: five days ago, inside the trigger's window. */
            await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, insert, "(now() AT TIME ZONE 'UTC') - interval '5 days'"), ct);
            Assert.Equal(2, await CountAsync(connection, "collect.plan_regression_daily_built", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheViewerProbe_MapsAPartitionedStoreToTheTopRung_AndAStoreBehindItToTheOneBelow()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 rung's live facts.");
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ArrangeV170StoreAsync(scratch, ct);
            Assert.Equal(170, await ProbedVersionAsync(connection, ct));
            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal(LatestRungVersion, await ProbedVersionAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /* ---- the arrangement ---------------------------------------------------------------------------------- */

    /// <summary>
    /// Migrates a scratch store to the top, rewinds both interval tables to the V170 shape, and seeds each across five days (an
    /// <see cref="RowsPerTable"/>-row table, 20 rows a day, 60 days back so the latest table's trigger window is not touched).
    /// </summary>
    private static async Task<NpgsqlConnection> ArrangeV170StoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await RewindToV170Async(connection, ct);

        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count, replica_role, runtime_stats_interval_id)
SELECT d + (g % 1440) * INTERVAL '1 minute' + INTERVAL '1 minute', 1, 'db', g, 1, 'exec',
       d + (g % 1440) * INTERVAL '1 minute' + (g * INTERVAL '1 microsecond'), d + (g % 1440) * INTERVAL '1 minute' + INTERVAL '1 minute',
       'mod', 'select 1', 'qh', 1, NULL, g
FROM generate_series(1, " + RowsPerTable.ToString(CultureInfo.InvariantCulture) + @") AS g
CROSS JOIN LATERAL (SELECT date_trunc('day', now() AT TIME ZONE 'UTC') - interval '60 days' + (g % 5) * interval '1 day' AS d) AS x", ct);
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_latest
(server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count, query_text)
SELECT (g % 10) + 1, 'db' || (g % 3), g, g % 1000, NULL, g,
       d + (g % 1440) * INTERVAL '1 minute' + (g * INTERVAL '1 microsecond'), d + INTERVAL '1 minute',
       'ph', 'qh', 1, 1, 1, d + INTERVAL '1 minute', false, 0, 'select 1'
FROM generate_series(1, " + RowsPerTable.ToString(CultureInfo.InvariantCulture) + @") AS g
CROSS JOIN LATERAL (SELECT date_trunc('day', now() AT TIME ZONE 'UTC') - interval '60 days' + (g % 5) * interval '1 day' AS d) AS x", ct);
        return connection;
    }

    /// <summary>Puts both tables back to their V170 shape and deletes the rung rows. See <see cref="RewindTableAsync"/>.</summary>
    private static async Task RewindToV170Async(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { Wide, Latest })
        {
            await RewindTableAsync(connection, table, ct);
        }

        await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + WideRungVersion.ToString(CultureInfo.InvariantCulture), ct);
    }

    /// <summary>
    /// One table back to its pre-rung shape: detach the legacy table, drop the parent, rename the table and its indexes back, and (latest)
    /// put V168's trigger back on the plain table. The version rows are the caller's. Also used by the viewer-probe tests that simulate an older store.
    /// </summary>
    internal static async Task RewindTableAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await ExecAsync(connection, @"
ALTER TABLE collect.@T@ DETACH PARTITION collect.@T@_legacy;
DROP TABLE collect.@T@;
ALTER TABLE collect.@T@_legacy RENAME TO @T@;
ALTER INDEX collect.ux_@T@_legacy RENAME TO ux_@T@;
ALTER INDEX collect.idx_@T@_first_exec_legacy RENAME TO idx_@T@_first_exec;".Replace("@T@", table, StringComparison.Ordinal), ct);

        if (table == Latest)
        {
            await ExecAsync(connection, "DROP TRIGGER IF EXISTS trg_plan_regression_daily_late ON collect.query_store_interval_latest", ct);
            await ExecAsync(connection, LatestTriggerSql, ct);
        }
    }

    /* ---- small helpers ------------------------------------------------------------------------------------ */

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    private static async Task<string> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        Convert.ToString(await ScalarAsync(connection, sql, ct), CultureInfo.InvariantCulture) ?? "";

    /// <summary><c>SELECT count(*) FROM &lt;from&gt;</c>, where <paramref name="from"/> may carry a WHERE.</summary>
    private static async Task<long> CountAsync(NpgsqlConnection connection, string from, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM " + from, ct), CultureInfo.InvariantCulture);

    private static async Task FlushStatsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* The statistics collector flushes at transaction end only when asked to; the second statement is the transaction end. */
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
        await ExecAsync(connection, "SELECT 1", ct);
    }

    private static async Task<long> SeqScanAsync(NpgsqlConnection connection, string table, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, "SELECT seq_scan FROM pg_stat_user_tables WHERE schemaname = 'collect' AND relname = '" + table + "'", ct), CultureInfo.InvariantCulture);

    private static async Task<bool> IndexIsValidAsync(NpgsqlConnection connection, string index, CancellationToken ct) =>
        (await ScalarAsync(connection, "SELECT indisvalid FROM pg_index WHERE indexrelid = 'collect." + index + "'::regclass", ct)) is true;

    private static async Task<string> ColumnShapeAsync(NpgsqlConnection connection, string table, CancellationToken ct) =>
        await TextAsync(connection,
            "SELECT string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod) || ':' || CASE WHEN a.attnotnull THEN 'notnull' ELSE 'null' END, ',' ORDER BY a.attnum) "
            + "FROM pg_attribute a WHERE a.attrelid = '" + table + "'::regclass AND a.attnum > 0 AND NOT a.attisdropped", ct);

    private static async Task<string> CatalogSnapshotAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await TextAsync(connection, @"
SELECT string_agg(x, E'\n' ORDER BY x) FROM (
    SELECT c.relkind::text || ':' || c.relname AS x FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'collect' AND c.relname LIKE '%query_store_interval_%' AND c.relkind IN ('r', 'p', 'i', 'I')
    UNION ALL SELECT 'def:' || indexdef FROM pg_indexes WHERE schemaname = 'collect' AND tablename LIKE 'query_store_interval_%'
    UNION ALL SELECT 'trg:' || c.relname || ':' || t.tgname || ':' || (t.tgparentid <> 0)::text FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE c.relname LIKE 'query_store_interval_%' AND NOT t.tgisinternal
    UNION ALL SELECT 'ver:' || version FROM darling_schema_version WHERE version >= 170
) s", ct);

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var flags = new object[reader.FieldCount];
        for (var i = 0; i < flags.Length; i++)
        {
            flags[i] = reader.GetBoolean(i);
        }

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (int)method.Invoke(null, flags)!;
    }
}
