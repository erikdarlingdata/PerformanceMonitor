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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V133 / #3691: <c>numbackends</c> on <c>collect.pg_database_stats</c> and <c>sampled_ms</c> on
/// <c>collect.pg_wait_sampling</c> — the ONE migration rung of the v2 wave. The first is the connection-saturation
/// NUMERATOR (<c>pg_stat_database.numbackends</c>, a client-only level sampled every minute, universal since
/// PostgreSQL 8.x) the v1 design wanted and <c>pg_session_states</c> could not honestly supply; the second is the
/// service-side sampler's duty-cycle denominator — the milliseconds a collection actually observed — without
/// which a rate read over the interval understates that arm ~10×. Both are written by their collectors from this
/// rung on and READ BY NOTHING yet; the consumers are follow-on lanes.
///
/// <para>The "I am the top rung" claims this class carried moved to <c>TimeHonestyRungTests</c> (V134) when that
/// rung landed, the same handoff this class received from <c>PerfmonCounterTypeRungTests</c> (V132). What stays
/// here is the one-rung-behind half: a store carrying this and not V134 maps to 133, which is the honest answer
/// for it and what makes the upgrade banner correct in both directions.</para>
///
/// <para>The collectors' write shapes (the tenth slot of a <c>pg_database_stats</c> row is the level or NULL,
/// never 0; the sampler arm's seventh slot is snapshots × period and the extension arm's is NULL) are pinned
/// value-by-value in <c>Lite.Tests/PgDatabaseStatsCollectorDefinitionTests</c> and
/// <c>Lite.Tests/PgWaitSamplerArmTests</c>, which run off Windows. What is here is the PostgreSQL side.</para>
/// </summary>
public sealed class PgNumbackendsAndSampledMsRungTests
{
    private const int RungVersion = 133;
    private const int PreviousVersion = 132;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V134 appended
    /// its own — so the invariant that outlives the handoff is that the ordinal is FIXED: a later rung
    /// appends after it and never shifts it.</summary>
    private const int ProbeOrdinal = 108;

    private const string DatabaseStatsTable = "pg_database_stats";
    private const string DatabaseStatsColumn = "numbackends";
    private const string WaitSamplingTable = "pg_wait_sampling";
    private const string WaitSamplingColumn = "sampled_ms";

    private static PgMigrations.Migration V133 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-numbackends-and-sampled-ms", V133.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* One below the top since V134 landed; the "RungVersion == StorageVersion.SchemaVersion" half of
           the top-arm claim moved to TimeHonestyRungTests with the top. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion, "V133 is expected to sit below the ladder's top now that V134 has landed");
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// Two nullable, default-less <c>ADD COLUMN IF NOT EXISTS</c>, one per table, schema-qualified, in the type each
    /// collector declares (rendered by the generator's own mapping so this pin cannot disagree with
    /// <c>PgSchemaGeneratorTests</c> about what "the type" is); NO view refresh, because neither PostgreSQL
    /// collector table has a <c>v_</c> passthrough to freeze a column list; no backfill, no index, no table, no
    /// data movement; and the rung doc carrying the argument.
    /// </summary>
    [Fact]
    public void TheRungAddsOneNullableIntegerColumnToEachTable_RefreshesNoView_AndNothingElse()
    {
        var sql = V133.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        var declaredBackends = PgDatabaseStatsCollector.Instance.PayloadColumns[^1];
        Assert.Equal(DatabaseStatsColumn, declaredBackends.Name);
        var renderedBackends = PgSchemaGenerator.TypeFor(declaredBackends);
        Assert.Equal("integer", renderedBackends);

        var declaredSampled = PgWaitSamplingCollector.Instance.PayloadColumns[^1];
        Assert.Equal(WaitSamplingColumn, declaredSampled.Name);
        var renderedSampled = PgSchemaGenerator.TypeFor(declaredSampled);
        Assert.Equal("integer", renderedSampled);

        Assert.Contains($"ALTER TABLE collect.{DatabaseStatsTable}\n    ADD COLUMN IF NOT EXISTS {DatabaseStatsColumn} {renderedBackends};", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER TABLE collect.{WaitSamplingTable}\n    ADD COLUMN IF NOT EXISTS {WaitSamplingColumn} {renderedSampled};", sql, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(sql, "ALTER TABLE").Count);
        Assert.Equal(2, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);

        /* Neither table has a passthrough, so there is nothing to refresh — and a CREATE OR REPLACE VIEW here
           would CREATE one the generator does not know about. */
        Assert.DoesNotContain("VIEW", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain($"v_{DatabaseStatsTable}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{WaitSamplingTable}", PgSchemaGenerator.AllPassthroughViews);

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DROP ", sql, StringComparison.Ordinal);

        /* A fresh store gets each column from the generated CREATE TABLE, LAST — the positional COPY writer and an
           upgraded store's ALTER agree on where it sits. */
        AssertGeneratedLast(PgDatabaseStatsCollector.Instance, DatabaseStatsColumn, renderedBackends, "stats_reset ");
        AssertGeneratedLast(PgWaitSamplingCollector.Instance, WaitSamplingColumn, renderedSampled, "backend_count ");

        /* The rung doc carries the argument in the words the next reader will look for. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V133 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V133Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the V133 rung doc is missing or sits after its constant");
        var doc = source[start..end];
        foreach (var phrase in new[]
        {
            "#3691", "ONE rung", "READ BY", "NOTHING yet", "saturation NUMERATOR", "pg_stat_database.numbackends",
            "duty-cycle denominator", "SamplerSnapshotsPerCycle", "extension arm writes NULL", "Per row rather than cumulative",
            "Nullable, no DEFAULT, no backfill", "compressed hypertable", "No view to refresh", "Lite is unchanged",
            "PgNumbackendsAndSampledMsRungTests",
        })
        {
            Assert.Contains(phrase, doc, StringComparison.Ordinal);
        }

        /* No table, no collector: the censuses did not move; each payload grew by exactly one. */
        /* 72 since V136 (#3691) added pg_database_size_stats; this rung itself added none. Restated as the current
           census figure rather than as "unchanged from before", which is the claim the line makes. */
        Assert.Equal(72, TimescaleSupport.HypertableCount);
        Assert.Equal(10, PgDatabaseStatsCollector.Instance.PayloadColumns.Count);
        Assert.Equal(7, PgWaitSamplingCollector.Instance.PayloadColumns.Count);
    }

    private static void AssertGeneratedLast(ICollectorSchemaInfo definition, string column, string rendered, string expectedBefore)
    {
        var generated = PgSchemaGenerator.CreateTable(definition);
        var lines = generated.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, generated);
        Assert.Equal($"{column} {rendered}", lines[closing - 1]);
        Assert.StartsWith(expectedBefore, lines[closing - 2], StringComparison.Ordinal);
        Assert.EndsWith($", {column}) FROM STDIN (FORMAT BINARY)", PgCollectorRowWriter.CopyCommandFor(definition), StringComparison.Ordinal);
    }

    /// <summary>
    /// Lite stores no <c>pg_*</c> table, so there is no twin column and no DuckDB schema bump for this rung — the
    /// rung doc says so, and this pin is what makes "Lite is unchanged" a checked claim rather than an assumption:
    /// neither collector is in the set the DuckDB generator stores.
    /// </summary>
    [Fact]
    public void LiteStoresNeitherTable_SoTheRungHasNoDuckDbTwin()
    {
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgDatabaseStatsCollector.Instance.TargetEngine);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgWaitSamplingCollector.Instance.TargetEngine);
    }

    /* ---- the probe (three sites, one rung behind the top) -------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map has an arm for it one rung behind
    /// the top. The probe asks the question, the caller reads the answer, the map has the parameter — a sentinel
    /// present at only some of them shifts every LATER ordinal onto the wrong column, and a missing arm maps a
    /// store that stopped here one rung short.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains(
            $"table_name = '{DatabaseStatsTable}'\n                                                     AND   column_name = '{DatabaseStatsColumn}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgNumbackendsAndSampledMs", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* This rung's sentinel sits strictly BELOW the last argument now that V134 has appended its own; the
           "is the last argument" claim moved to TimeHonestyRungTests with the top. */
        Assert.True(ProbeOrdinal < arity - 1, "V133's sentinel is expected to sit below the top rung's now that V134 has landed");

        /* Every sentinel true = a fully-migrated store, which must map to exactly the ladder's top. Stated
           against StorageVersion rather than this rung's number, so it survives every later rung. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns THIS rung's version. */
        var thisArm = viewer.IndexOf("if (hasPgNumbackendsAndSampledMs)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf(PreviousArmSource, StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V133 sentinel arm — a store that stopped here would map to 132");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V133 arm sits below the previous rung's, so a store that stopped here maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the tables are named in the probe line and nowhere in the arm's prose. */
        Assert.DoesNotContain(DatabaseStatsTable, viewer[thisArm..previousArm], StringComparison.Ordinal);
        Assert.DoesNotContain(WaitSamplingTable, viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>The previous rung's arm as the viewer spells it — V132's sentinel.</summary>
    private const string PreviousArmSource = "if (hasPerfmonCounterType)";

    /* ---- the consumers, as they landed ------------------------------------------------------------- */

    /// <summary>
    /// The exit criterion's last clause, re-shaped as the consumers landed (the rung's "no reader yet" was the
    /// promise; each arrival moves its line here deliberately). <c>sampled_ms</c>: the wait-sampling MCP reader's
    /// estimate keeps its <c>samples × period</c> arithmetic and names the column only in its doc as the denominator a
    /// rate read must use — the analysis collector (<c>PgTargetFactCollector.Waits.cs</c>) is that rate read.
    /// <c>numbackends</c>: read by the saturation fact (lane 25, summed at one instant) and, since the third
    /// between-waves batch (#3791), by <c>DarlingPgDatabaseReader.PgDatabaseSql</c> as the per-database window PEAK —
    /// pinned here in the shape the rung doc demanded of a consumer: <c>MAX</c>, never a <c>LAG</c> difference, and
    /// never a <c>SUM</c> across databases (the peaks land at different instants). The reader's doc says the same.
    /// </summary>
    [Fact]
    public void TheWaitReaderStillEstimates_AndTheDatabaseReaderReadsNumbackendsAsAnUndifferencedPeak()
    {
        Assert.DoesNotContain(WaitSamplingColumn, DarlingPgWaitSamplingReader.PgWaitSamplingSql, StringComparison.Ordinal);
        Assert.Contains($"MAX({DatabaseStatsColumn})", DarlingPgDatabaseReader.PgDatabaseSql, StringComparison.Ordinal);
        Assert.DoesNotContain($"LAG({DatabaseStatsColumn})", DarlingPgDatabaseReader.PgDatabaseSql, StringComparison.Ordinal);
        Assert.DoesNotContain($"SUM({DatabaseStatsColumn})", DarlingPgDatabaseReader.PgDatabaseSql, StringComparison.Ordinal);

        var waits = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingPgWaitSamplingReader.cs");
        var estimated = waits.IndexOf("<param name=\"EstimatedWaitMs\">", StringComparison.Ordinal);
        Assert.True(estimated >= 0, "EstimatedWaitMs lost its param doc");
        var estimatedDoc = waits[estimated..waits.IndexOf("</param>", estimated, StringComparison.Ordinal)];
        Assert.Contains("pg_wait_sampling.sampled_ms", estimatedDoc, StringComparison.Ordinal);
        Assert.Contains("does NOT yet make that", estimatedDoc, StringComparison.Ordinal);
        Assert.Contains("the whole", estimatedDoc, StringComparison.Ordinal);

        var databases = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingPgDatabaseReader.cs");
        Assert.Contains("rides along as a LEVEL, never differenced", databases, StringComparison.Ordinal);
        Assert.Contains("are NOT summed into a cluster figure here", databases, StringComparison.Ordinal);
    }
}

/// <summary>
/// The rung against a real PostgreSQL + TimescaleDB store (<c>DARLING_TEST_PG</c>): both columns present, nullable,
/// default-less and LAST after <c>MigrateAsync</c>; a simulated climb from 132 through this rung and every later one; then one
/// row per table with the new column populated and one with NULL through each collector's real
/// <c>WritePayload</c> over a real binary COPY — <c>DarlingCollectorRunner</c>'s loop in shape — read back
/// verbatim. Serialized against every other live class because it shares the store.
/// </summary>
[Collection("live-postgres")]
public sealed class PgNumbackendsAndSampledMsLivePostgresTests
{
    private const int ServerId = -133133;
    private const string ServerName = "pg-v133-columns-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheStoreClimbsToTheRung_AndBothCollectorsWriteTheColumnsOrNull_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V133 round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* A store that stopped one rung short of THIS one: both columns gone, the stamp for this rung and
               every later one gone. MigrateAsync runs every rung above MAX(version), so the climb goes from 132
               through this rung AND every rung that has landed since (their ADD COLUMN IF NOT EXISTS no-op over
               columns the store still has) — the count is stated against the ladder rather than as 1, which was
               only true while this was the top; the exact single-rung climb belongs to the top rung's own test
               (TimeHonestyRungTests today). This rung's two columns must come back. Both tables are hypertables
               here (the fixture store converts them), so this is the ADD COLUMN the fleet will run. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.pg_database_stats DROP COLUMN IF EXISTS numbackends");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.pg_wait_sampling DROP COLUMN IF EXISTS sampled_ms");
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 133");
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 133), await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            await AssertColumnAsync(connection, ct, "pg_database_stats", "numbackends");
            await AssertColumnAsync(connection, ct, "pg_wait_sampling", "sampled_ms");

            var t1 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            var t2 = t1.AddMinutes(5);

            /* pg_database_stats through the REAL writer: a populated level and the shared-relations row's NULL. */
            await WriteThroughTheCollectorAsync(connection, PgDatabaseStatsCollector.Instance, t1, new[]
            {
                new PgDatabaseStatsCollector.Row("appdb", 1_000, 10, 500, 90_000, 4, 8_192, 0, null, 37),
                new PgDatabaseStatsCollector.Row(null, 0, 0, 12, 300, 0, 0, 0, null, null),
            }, ct);

            /* pg_wait_sampling through the REAL writer, two cycles so the existing differencing read has an
               oldest and a newest: a sampler-arm key carrying its window and an extension-arm key carrying NULL,
               as the two arms' ReadAsync produce them. */
            await WriteThroughTheCollectorAsync(connection, PgWaitSamplingCollector.Instance, t1, new[]
            {
                new PgWaitSamplingCollector.Row("Lock", "relation", 111, 20, PgWaitSamplingCollector.SamplerPeriodMs, 1,
                    PgWaitSamplingCollector.SamplerSnapshotsPerCycle * PgWaitSamplingCollector.SamplerPeriodMs),
                new PgWaitSamplingCollector.Row("IO", "DataFileRead", 222, 3_000, 10, 3, null),
            }, ct);
            await WriteThroughTheCollectorAsync(connection, PgWaitSamplingCollector.Instance, t2, new[]
            {
                new PgWaitSamplingCollector.Row("Lock", "relation", 111, 27, PgWaitSamplingCollector.SamplerPeriodMs, 1,
                    PgWaitSamplingCollector.SamplerSnapshotsPerCycle * PgWaitSamplingCollector.SamplerPeriodMs),
                new PgWaitSamplingCollector.Row("IO", "DataFileRead", 222, 4_000, 10, 3, null),
            }, ct);

            using (var stored = new NpgsqlCommand(
                "SELECT database_name, numbackends FROM pg_database_stats WHERE server_id = $1 ORDER BY database_name NULLS LAST", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<(string? Database, int? Backends)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt32(1)));
                }

                Assert.Equal(new (string?, int?)[] { ("appdb", 37), (null, null) }, rows);
            }

            using (var stored = new NpgsqlCommand(
                "SELECT event_type, sample_count, profile_period_ms, sampled_ms FROM pg_wait_sampling WHERE server_id = $1 ORDER BY event_type, collection_time", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<(string Type, long Samples, int Period, int? Sampled)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetInt32(3)));
                }

                Assert.Equal(new (string, long, int, int?)[]
                {
                    ("IO", 3_000, 10, null),                                                     // the extension arm: the whole interval was observed
                    ("IO", 4_000, 10, null),
                    ("Lock", 20, PgWaitSamplingCollector.SamplerPeriodMs, 30_000),               // the sampler arm: 30 snapshots × 1,000 ms, every cycle
                    ("Lock", 27, PgWaitSamplingCollector.SamplerPeriodMs, 30_000),
                }, rows);
            }

            /* The existing differencing read still works over rows that carry the new column, and it neither
               publishes nor divides by it: the sampler key's estimate is Δ7 × 1,000 ms, reported as observed. The
               denominator a rate read would use is the 30,000 ms the newer row stored, not the five-minute
               interval — which is the whole point of the column, and the consumer lane's change, not this one's. */
            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var page = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(postgres, ServerId, t1.AddMinutes(-1), t2.AddMinutes(1), 10, ct);
                var lockRow = Assert.Single(page.Rows, r => r.EventType == "Lock");
                Assert.Equal(7, lockRow.SampleCount);
                Assert.Equal(7L * PgWaitSamplingCollector.SamplerPeriodMs, lockRow.EstimatedWaitMs);
                Assert.False(lockRow.CounterReset);
                var ioRow = Assert.Single(page.Rows, r => r.EventType == "IO");
                Assert.Equal(1_000, ioRow.SampleCount);
                Assert.Equal(10_000, ioRow.EstimatedWaitMs);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task AssertColumnAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string table, string column)
    {
        using var describe = new NpgsqlCommand(
            "SELECT data_type, is_nullable, column_default, ordinal_position = (SELECT MAX(ordinal_position) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1) " +
            "FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1 AND column_name = $2", connection);
        describe.Parameters.AddWithValue(table);
        describe.Parameters.AddWithValue(column);
        using var reader = await describe.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{table} has no {column} column after MigrateAsync");
        Assert.Equal("integer", reader.GetString(0));
        Assert.Equal("YES", reader.GetString(1));
        Assert.True(reader.IsDBNull(2), $"{table}.{column} has a DEFAULT, which a compressed hypertable would have to rewrite to honour");
        Assert.True(reader.GetBoolean(3), $"{table}.{column} is not the LAST column, so the positional COPY writer would land it in the wrong slot");
    }

    /// <summary>DarlingCollectorRunner's COPY loop, verbatim in shape: prefix columns through the writer, then
    /// BeginPayload / WritePayload / EndPayload per row, so each collector's positional contract is exercised
    /// against the real, migrated table.</summary>
    private static async Task WriteThroughTheCollectorAsync<TRow>(NpgsqlConnection connection, ICollectorDefinition<TRow> definition, DateTime collectionTime,
        IReadOnlyList<TRow> rows, System.Threading.CancellationToken ct)
    {
        /* Neither collector differences at write time (both store raw cumulative counters, and the new columns are
           levels), so no calculator is ever consulted; null! is the standing idiom for that. */
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName, CollectionTime = collectionTime, Deltas = null! };
        var writer = new PgCollectorRowWriter();

        using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(DarlingMcpTestData.Naive(collectionTime)).Value(ServerId).Value(ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }

        await importer.CompleteAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "pg_database_stats", "pg_wait_sampling" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
            cleanup.Parameters.AddWithValue(ServerId);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
