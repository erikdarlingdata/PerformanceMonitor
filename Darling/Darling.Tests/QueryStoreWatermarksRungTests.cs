/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that adds the Query Store watermark side tables (V178, #5684): its place in the ladder below V179 (#5640) and V180 (#5654), the shape of its
/// script (guarded creates, the one coverage insert, nothing moved or dropped), the schema-version probe's sentinel and its mapping arm.
/// V176 (#5638) and V177 (#5641) are on dev below this rung, so V176-V180 are contiguous and the dense-ladder pins hold. The probe
/// ordinal below counts the probe's columns; it moves up by one for each rung that lands below this one.
/// </summary>
public sealed class QueryStoreWatermarksRungTests
{
    public const string RungName = "query-store-watermarks";

    private const int ProbeOrdinal = 153;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_BelowTheTopOfTheLadder_AtTheVersionTheTablesNameThemselves()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(178, QueryStoreWatermarks.RungVersion);
        Assert.Equal(QueryStoreWatermarks.RungVersion, Rung.Version);
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions[^1]);
        /* V179 (#5640), V180 (#5654), V181 (#5750), V182 (#5506), V183 (#5793), V184 (#5632), V185 (#5812), V186 (#5810) and V187 (#5821) sit above this rung,
           so it is the tenth from the top. */
        Assert.Equal(new[] { 178, 179, 180, 181, 182, 183, 184, 185, 186, 187 }, versions.TakeLast(10).ToArray());
        Assert.Equal(QueryStoreWatermarks.CreateSql, Rung.Sql);
    }

    [Fact]
    public void TheScript_IsGuardedCreates_AndOneCoverageInsertThatKeepsTheFirstValue_WithNothingMovedOrDropped()
    {
        var sql = Statements();

        Assert.Equal(2, Regex.Matches(sql, @"CREATE\s+TABLE\b", RegexOptions.IgnoreCase).Count);
        Assert.Equal(2, Regex.Matches(sql, @"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b", RegexOptions.IgnoreCase).Count);
        Assert.Single(Regex.Matches(sql, @"CREATE\s+INDEX\b", RegexOptions.IgnoreCase));
        Assert.Contains("CREATE INDEX IF NOT EXISTS ix_query_store_watermarks_time", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT pk_query_store_watermarks PRIMARY KEY (server_id, database_name, collection_time)", sql, StringComparison.Ordinal);

        /* Types mirror query_store_stats: naive timestamps and bigint ids. */
        Assert.Contains("collection_time timestamp NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("batch_max timestamp,", sql, StringComparison.Ordinal);
        Assert.Contains("max_query_id bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("max_plan_id bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("database_name text NOT NULL", sql, StringComparison.Ordinal);

        /* The coverage start is inserted once and a re-run keeps the first value, in the store's own UTC clock. */
        Assert.Single(Regex.Matches(sql, @"\bINSERT\b"));
        Assert.Contains("ON CONFLICT (singleton) DO NOTHING", sql, StringComparison.Ordinal);
        Assert.Contains("now() AT TIME ZONE 'UTC'", sql, StringComparison.Ordinal);

        foreach (var verb in new[] { "UPDATE", "DELETE", "GRANT", "REVOKE", "TRUNCATE", "DROP", "ALTER" })
        {
            Assert.DoesNotMatch(@"(?m)^\s*" + verb + @"\b", sql);
        }
    }

    [Fact]
    public void TheProbeNamesTheTable_AndMapsAFullyMigratedStoreToTheTopRung_AndThisRungBelowTheLaterOnes()
    {
        Assert.Contains("to_regclass('collect.query_store_watermarks')", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        /* V179's (#5640) and V180's (#5654) sentinels are appended after this one, so this rung is no longer the top. */
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 2})", viewer, StringComparison.Ordinal);
        /* V181's (#5750), V182's (#5506) and V183's (#5793) sentinels are next; V184's (#5632) and V185's (#5812) are next, and V186's (#5810) is next; and V187's (#5821) is the probe's last ordinal. */
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 3})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 4})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 5})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 6})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 7})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 8})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasQueryStoreWatermarks", parameters[ProbeOrdinal].Name);
        Assert.Equal("hasRunnableQueueSettings", parameters[ProbeOrdinal - 1].Name);
        Assert.Equal("hasClientTimeoutSettings", parameters[ProbeOrdinal + 1].Name);
        Assert.Equal("hasExternalAlerts", parameters[ProbeOrdinal + 2].Name);
        Assert.Equal("hasNotificationRouteScope", parameters[ProbeOrdinal + 3].Name);
        Assert.Equal("hasReadLatencyCapacityFields", parameters[ProbeOrdinal + 4].Name);
        Assert.Equal("hasReadCaller", parameters[ProbeOrdinal + 5].Name);
        Assert.Equal("hasInvestigations", parameters[ProbeOrdinal + 6].Name);
        Assert.Equal("hasPlanRegressionPartialDays", parameters[ProbeOrdinal + 7].Name);
        Assert.Equal("hasCollectionLogHourly", parameters[ProbeOrdinal + 8].Name);
        Assert.Equal("hasInvestigationAlertFacts", parameters[ProbeOrdinal + 9].Name);
        Assert.Equal(ProbeOrdinal + 9, parameters.Length - 1);

        /* Every sentinel present maps to exactly this build's version; with V180's (the top) absent the answer is V179's; with V179's
           absent too this rung is the answer; with this sentinel absent as well, one rung back. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* With V182's sentinel (#5506) absent too the answer is V181's. */
        /* With V187's (#5821) sentinel absent the answer is V186's (#5810); with V186's absent too, V185's (#5812). */
        var withoutV187 = (object[])all.Clone();
        withoutV187[ProbeOrdinal + 9] = false;
        Assert.Equal(186, (int)method.Invoke(null, withoutV187)!);

        var withoutV186 = (object[])withoutV187.Clone();
        withoutV186[ProbeOrdinal + 8] = false;
        Assert.Equal(185, (int)method.Invoke(null, withoutV186)!);

        /* With V185's (#5812) sentinel absent too the answer is V184's (#5632); with V184's absent too, V183's. */
        var withoutV185 = (object[])withoutV186.Clone();
        withoutV185[ProbeOrdinal + 7] = false;
        Assert.Equal(184, (int)method.Invoke(null, withoutV185)!);

        var withoutV184 = (object[])withoutV185.Clone();
        withoutV184[ProbeOrdinal + 6] = false;
        Assert.Equal(183, (int)method.Invoke(null, withoutV184)!);

        var withoutV183 = (object[])withoutV184.Clone();
        withoutV183[ProbeOrdinal + 5] = false;
        Assert.Equal(182, (int)method.Invoke(null, withoutV183)!);

        var withoutV182 = (object[])withoutV183.Clone();
        withoutV182[ProbeOrdinal + 4] = false;
        Assert.Equal(181, (int)method.Invoke(null, withoutV182)!);

        var withoutV181 = (object[])withoutV182.Clone();
        withoutV181[ProbeOrdinal + 3] = false;
        Assert.Equal(180, (int)method.Invoke(null, withoutV181)!);

        var withoutV180 = (object[])withoutV181.Clone();
        withoutV180[ProbeOrdinal + 2] = false;
        Assert.Equal(179, (int)method.Invoke(null, withoutV180)!);

        var withoutTop = (object[])withoutV180.Clone();
        withoutTop[ProbeOrdinal + 1] = false;
        Assert.Equal(QueryStoreWatermarks.RungVersion, (int)method.Invoke(null, withoutTop)!);

        var behind = (object[])withoutTop.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(177, (int)method.Invoke(null, behind)!);

        /* In the source the arm sits ABOVE V177's and returns this rung's version. */
        var thisArm = viewer.IndexOf("if (hasQueryStoreWatermarks)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasRunnableQueueSettings)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0 && previousArm > thisArm, "the V178 arm sits above V177's");
        Assert.Contains("return " + QueryStoreWatermarks.RungVersion + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAFreshStore_TheProbeMapsToTheNewestRung_AndWithoutTheTableToTheOneBelow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the watermark rung's live probe pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            await RungRewindArtifacts.DropV187SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV186SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV185SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV184SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV183SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV182SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV181SentinelAsync(connection, ct);
            /* V180's (#5654) and V179's (#5640) sentinels are above this rung's; removed, this rung's table is the newest sentinel left. */
            await RungRewindArtifacts.DropV180SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV179SentinelAsync(connection, ct);
            Assert.Equal(QueryStoreWatermarks.RungVersion, await ProbedVersionAsync(connection, ct));

            await ExecAsync(connection, "DROP TABLE collect.query_store_watermarks, collect.query_store_watermarks_coverage", ct);
            Assert.Equal(177, await ProbedVersionAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
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
