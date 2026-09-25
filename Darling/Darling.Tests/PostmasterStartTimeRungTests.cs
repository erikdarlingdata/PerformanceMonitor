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
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V139 (#3955): ONE nullable <c>timestamp</c> column, <c>postmaster_start_time</c>, on two
/// existing tables — the <c>checkpointer</c> row of <c>collect.store_metrics</c> and every row of
/// <c>collect.pg_write_stats</c> — so each sample of a server's cumulative checkpointer counters says which
/// postmaster produced it. PostgreSQL counts a shutdown checkpoint as requested and keeps the count across the
/// restart, so without it every service restart raised a false Store Checkpointer Pressure warning and a monitored
/// server's restart read as a WAL-forced checkpoint. No new table, no new hypertable
/// (<see cref="TimescaleSupport.HypertableCount"/> stays 72), no DEFAULT, no backfill, no passthrough (neither table
/// has a <c>v_</c> view), no Lite twin (Lite stores neither table).
///
/// <para>This file carried the "I am the top rung" claims that moved off <see cref="PgServerConfigScopeRungTests"/>
/// (V138) when this rung landed, and handed them on to <see cref="CheckpointsTimedRungTests"/> (V140, #4037) when
/// that one did. What stays is everything true of this rung wherever it sits.</para>
///
/// <para>The rule the column exists for is <see cref="PostmasterRestart"/>, pinned where each reader lives: the
/// store's own interval and self-alert in <see cref="StoreToastAndCheckpointerTests"/>, the monitored-target fact, the
/// window read and the tool in <see cref="PgTargetWriteTests"/>, the collector's SQL in
/// <c>Lite.Tests.PgWriteStatsCollectorDefinitionTests</c>. This file is the RUNG: the ladder, the DDL, the V88
/// restatement, the probe, and the live climb with both writers.</para>
/// </summary>
public sealed class PostmasterStartTimeRungTests
{
    private const int RungVersion = 139;
    private const int PreviousVersion = 138;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V140 (#4037, the
    /// timed-checkpoint count) appended its own — so this is a position within the signature rather than its end,
    /// the handoff <see cref="PgServerConfigScopeRungTests"/> made to this file one rung ago.</summary>
    private const int ProbeOrdinal = 114;

    private const string Column = "postmaster_start_time";

    private static PgMigrations.Migration V139 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("postmaster-start-time", V139.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V140 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// Two ALTERs, one column each — nullable, no DEFAULT, no backfill, no table, no index, no view, nothing else —
    /// the store's own table first (the alert the issue is about), then the monitored targets'. The collector's
    /// column is rendered from its own declaration, so a type here that differed from <c>PayloadColumns</c> would
    /// fail rather than ship two populations.
    /// </summary>
    [Fact]
    public void TheRungAddsOneNullableTimestampToEachOfTwoTables_AndNothingElse()
    {
        var sql = V139.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"ALTER TABLE collect.store_metrics\n    ADD COLUMN IF NOT EXISTS {Column} timestamp;", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER TABLE collect.pg_write_stats\n    ADD COLUMN IF NOT EXISTS {Column} timestamp;", sql, StringComparison.Ordinal);
        Assert.True(sql.IndexOf("collect.store_metrics", StringComparison.Ordinal) < sql.IndexOf("collect.pg_write_stats", StringComparison.Ordinal));
        Assert.Equal(2, Regex.Matches(sql, "ALTER TABLE").Count);
        Assert.Equal(2, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "DEFAULT", "NOT NULL", "UPDATE ", "INSERT ", "DELETE ", "CREATE ", "DROP ", "VIEW" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }

        /* Neither table has a passthrough, so there is no view to refresh — a CREATE OR REPLACE VIEW here would
           CREATE one the generator does not know about. */
        Assert.DoesNotContain("v_store_metrics", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain("v_pg_write_stats", PgSchemaGenerator.AllPassthroughViews);

        var declared = PgWriteStatsCollector.Instance.PayloadColumns[^1];
        Assert.Equal(Column, declared.Name);
        Assert.Equal(CollectorColumnType.Timestamp, declared.Type);
        Assert.Equal("timestamp", PgSchemaGenerator.TypeFor(declared));
    }

    /// <summary>
    /// The V101 rule: V88's CREATE text carries the column too, LAST, so a FRESH store builds the table with it from
    /// the generated schema and this rung's ALTER no-ops, while a store that climbed through V88 before V139 existed
    /// gets it from the ALTER. <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c> holds
    /// V88's text to the generator, and the COPY writer names the column last, where the ALTER puts it.
    /// </summary>
    [Fact]
    public void V88sCreateTextCarriesTheColumn_AndTheGeneratorAndTheCopyAgree()
    {
        var v88 = PgMigrations.Scripts.Single(m => m.Version == 88).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains($"    wal_stats_reset timestamp,\n    {Column} timestamp\n);", v88, StringComparison.Ordinal);

        var generated = PgSchemaGenerator.CreateTable(PgWriteStatsCollector.Instance).Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains($"    wal_stats_reset timestamp,\n    {Column} timestamp\n", generated, StringComparison.Ordinal);

        Assert.EndsWith($", wal_stats_reset, {Column}) FROM STDIN (FORMAT BINARY)", PgCollectorRowWriter.CopyCommandFor(PgWriteStatsCollector.Instance), StringComparison.Ordinal);

        /* store_metrics is not a collector table: V53 creates it by hand and nothing generated walks it. */
        Assert.DoesNotContain(CollectorCatalog.All, c => c.TargetTable == "store_metrics");
    }

    /// <summary>
    /// The rung's doc block says what the other rungs' say, in the same voice: the lie it ends, why a stored column
    /// rather than a live read, naive UTC, nullable with the rule's reading of NULL, the V101 rule, and what it does
    /// NOT do. And the censuses did not move: no hypertable, no collector, one payload column.
    /// </summary>
    [Fact]
    public void TheRungDocSaysWhatItDoesAndWhatItDoesNot_AndNoCensusMoved()
    {
        /* Every phrase below sits on one line, so the plain reader (RepoFileAdoptionTests' rule: the LF reader is for
           pins whose anchors cross a line break). */
        var storage = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = storage.IndexOf("/// V139 —", StringComparison.Ordinal);
        Assert.True(start >= 0, "the V139 rung has no doc block in the V138 voice");
        var doc = storage[start..storage.IndexOf("private const string V139Sql", StringComparison.Ordinal)];

        foreach (var phrase in new[]
        {
            "#3955", "stays 72", "no Lite twin", "The lie this ends.", "0|1|0", "shutdown immediate",
            "Why a stored column rather than a read of <c>pg_postmaster_start_time()</c> at read time.",
            "Naive UTC", "Nullable, no DEFAULT, no backfill", "The V101 rule applies", "neither table has a <c>v_</c> view",
            "What this rung deliberately does NOT do.", "It does not backfill",
        })
        {
            Assert.Contains(phrase, doc, StringComparison.Ordinal);
        }

        Assert.Equal(72, TimescaleSupport.HypertableCount);
        Assert.Equal(71, CollectorCatalog.All.Count);
        Assert.Equal(27, PgWriteStatsCollector.Instance.PayloadColumns.Count);
    }

    /* ---- the probe (top arm) -------------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel at its own ordinal, and the map's arm for it
    /// returns 139. The probe asks the question, the caller reads the answer, the map has the parameter — a
    /// sentinel present at only some of them shifts every LATER ordinal onto the wrong column. The top-arm half of
    /// this claim moved to <see cref="CheckpointsTimedRungTests"/> (V140, #4037) with the top. The gate is
    /// load-bearing here: the write-side panel's shared reader names the column.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        Assert.Contains(
            $"table_name = 'pg_write_stats'\n                                                     AND   column_name = '{Column}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains(Column, DarlingPgWriteStatsReader.PgWriteStatsSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        /* The "nothing past me" half of this claim moved to V140's test with the top ordinal; what stays is
           that this rung's sentinel is read at its OWN ordinal, which is what keeps every later one on the
           right column. */
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPostmasterStartTime", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is the
           NEWEST sentinel, which stopped being true the moment V140 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);
        Assert.Equal("hasPostmasterStartTime", method.GetParameters()[ProbeOrdinal].Name);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns this rung's version. */
        var thisArm = viewer.IndexOf("if (hasPostmasterStartTime)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasPgServerConfigDatabaseRoleOverrides)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V139 sentinel arm — a store stopped here would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V139 arm sits below the previous rung's, so a V139 store maps one rung low");
        /* This rung's own literal, not the build's version: the "returns StorageVersion.SchemaVersion" half of
           the top-arm claim moved to V140's test with the top. */
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the tables and the column are named in the probe line and nowhere in the arm's prose —
           the coverage ratchet strips information_schema lines but cannot strip a comment. The arm's comment block
           sits ABOVE the `if`, so the prose searched is the span from the previous arm's end to this one's. */
        var armProseStart = viewer.LastIndexOf("/* V139 (#3955)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the V139 arm has no comment block saying why it exists");
        var prose = viewer[armProseStart..previousArm];
        foreach (var name in new[] { "pg_write_stats", "store_metrics", Column })
        {
            Assert.DoesNotContain(name, prose, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// The rung against a real PostgreSQL + TimescaleDB store (<c>DARLING_TEST_PG</c>): a store stopped one rung short
/// (both columns gone, the stamp gone) climbs EXACTLY this rung and puts the column back LAST on both tables,
/// nullable and default-less; then BOTH writers fill it with the server's own <c>pg_postmaster_start_time()</c> as
/// naive UTC — the store's checkpointer sweep statement, and the shared collector's REAL query run against the same
/// cluster as a monitored target, read by its real <c>ReadAsync</c> and written through the real binary COPY. On the
/// fleet <c>pg_write_stats</c> is a compressed hypertable; the fixture store converts it, so this is the ALTER the
/// fleet runs. Serialized against every other live class because it shares the store.
/// </summary>
[Collection("live-postgres")]
public sealed class PostmasterStartTimeLivePostgresTests
{
    private const int ServerId = -139139;
    private const string ServerName = "pg-v139-postmaster-start-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheStoreClimbsToTheRung_AndBothWritersStoreTheServersOwnStartInUtc_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V139 round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        /* Far outside the sweep's 400-day retention and any real run, so the cleanup's equality delete finds exactly
           this run's row. */
        var metricTime = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddYears(-50));
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* A store that stopped one rung short: the column gone from both tables, the stamp gone. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.store_metrics DROP COLUMN IF EXISTS postmaster_start_time");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.pg_write_stats DROP COLUMN IF EXISTS postmaster_start_time");
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 139");
            /* Every rung from 139 up re-applies (V140-V142 are idempotent: ADD COLUMN IF NOT EXISTS or CREATE
               TABLE/INDEX IF NOT EXISTS), so the count is the distance to the top rung, not 1. */
            Assert.Equal(StorageVersion.SchemaVersion - 138, await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            await AssertLastColumnAsync(connection, "store_metrics", ct);
            await AssertLastColumnAsync(connection, "pg_write_stats", ct);

            DateTime serverStart;
            using (var live = new NpgsqlCommand("SELECT pg_postmaster_start_time() AT TIME ZONE 'UTC'", connection))
            {
                serverStart = (DateTime)(await live.ExecuteScalarAsync(ct))!;
            }

            /* The store's writer: the checkpointer sweep statement for this server's major, verbatim. */
            var checkpointerSql = connection.PostgreSqlVersion.Major >= StoreSelfMetrics.CheckpointerViewMajorVersion
                ? StoreSelfMetrics.CheckpointerInsertSql
                : StoreSelfMetrics.CheckpointerBgwriterInsertSql;
            using (var sweep = new NpgsqlCommand(checkpointerSql, connection))
            {
                sweep.Parameters.AddWithValue(metricTime);
                Assert.Equal(1, await sweep.ExecuteNonQueryAsync(ct));
            }

            using (var stored = new NpgsqlCommand(
                $"SELECT postmaster_start_time FROM collect.store_metrics WHERE metric_time = $1 AND object_kind = '{StoreSelfMetrics.CheckpointerObjectKind}'", connection))
            {
                stored.Parameters.AddWithValue(metricTime);
                Assert.Equal(serverStart, (DateTime)(await stored.ExecuteScalarAsync(ct))!);
            }

            /* The monitored targets' writer: the shared collector's real query against this same cluster, its real
               ReadAsync, and the real COPY (DarlingCollectorRunner's loop in shape). */
            var definition = PgWriteStatsCollector.Instance;
            var collectionTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            var context = new CollectorContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                CollectionTime = collectionTime,
                Deltas = null!,
                Target = new CollectorTargetInfo
                {
                    Engine = CollectorTargetEngine.PostgreSql,
                    PostgresMajorVersion = connection.PostgreSqlVersion.Major,
                },
                ExcludedDatabases = Array.Empty<string>(),
            };

            List<PgWriteStatsCollector.Row> rows;
            using (var query = new NpgsqlCommand(definition.BuildQuery(context).Text, connection))
            using (var reader = await query.ExecuteReaderAsync(ct))
            {
                rows = await definition.ReadAsync(reader, context, ct);
            }

            var row = Assert.Single(rows);
            Assert.Equal(serverStart, row.PostmasterStartTime);

            var writer = new PgCollectorRowWriter();
            using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
            {
                writer.Importer = importer;
                await importer.StartRowAsync(ct);
                writer.Value(CollectionIdGenerator.Next());
                writer.Value(DarlingMcpTestData.Naive(collectionTime)).Value(ServerId).Value(ServerName);
                writer.BeginPayload();
                definition.WritePayload(row, writer, context);
                writer.EndPayload(definition.PayloadColumns.Count);
                await importer.CompleteAsync(ct);
            }

            using (var stored = new NpgsqlCommand("SELECT postmaster_start_time, num_requested FROM collect.pg_write_stats WHERE server_id = $1", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                using var reader = await stored.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), "the COPY wrote no row");
                Assert.Equal(serverStart, reader.GetDateTime(0));
                Assert.Equal(row.NumRequested, reader.GetInt64(1));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var metrics = new NpgsqlCommand("DELETE FROM collect.store_metrics WHERE metric_time = $1", cleanup);
                metrics.Parameters.AddWithValue(metricTime);
                await metrics.ExecuteNonQueryAsync(cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task AssertLastColumnAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        using var describe = new NpgsqlCommand(
            "SELECT data_type, is_nullable, column_default, ordinal_position = (SELECT MAX(ordinal_position) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1) " +
            "FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1 AND column_name = 'postmaster_start_time'", connection);
        describe.Parameters.AddWithValue(table);
        using var reader = await describe.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{table} has no postmaster_start_time column after MigrateAsync");
        Assert.Equal("timestamp without time zone", reader.GetString(0));
        Assert.Equal("YES", reader.GetString(1));
        Assert.True(reader.IsDBNull(2), $"{table}.postmaster_start_time has a DEFAULT, which a compressed hypertable would have to rewrite to honour");
        Assert.True(reader.GetBoolean(3), $"{table}.postmaster_start_time is not the LAST column, so an upgraded store and a fresh one disagree on where it sits");
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM pg_write_stats WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
