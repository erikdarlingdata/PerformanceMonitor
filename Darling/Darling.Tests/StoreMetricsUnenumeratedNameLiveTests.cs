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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8817: <c>get_store_metrics</c> names the largest relations it does not enumerate (<c>inventory.largest_unenumerated</c>), and
/// asking for one of them by <c>object_name</c> used to answer <c>empty</c>, because the series has no row for a relation the
/// sweep sums into the one <c>other</c> row. Every relation the tool names can now be asked for by the name it gives.
/// </summary>
public sealed class StoreMetricsUnenumeratedNameLiveTests
{
    /* A plain table no named row sizes, created by the test and a couple of MB so it is always among the ten largest un-enumerated relations. (The oversized plan backlog used to be the example here and has its own row since the second part of #8817.) */
    private const string Backlog = "collect.zz_unenumerated_probe";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<NpgsqlDataSource> SeededStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using (var probe = new NpgsqlCommand("CREATE TABLE collect.zz_unenumerated_probe AS SELECT g, repeat('x', 1000) AS filler FROM generate_series(1, 4000) AS g", connection))
        {
            await probe.ExecuteNonQueryAsync(ct);
        }

        /* One sweep, so the tool has self-metrics to answer from: the store row and the catch-all rows. No row for the relation under test. */
        await using var insert = new NpgsqlCommand($@"
INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes, size_estimated) VALUES
    ($1, 'store',  '{StoreSelfMetrics.StoreObjectKind}',  10000000000, NULL),
    ($1, '{StoreSelfMetrics.OtherObjectName}', '{StoreSelfMetrics.OtherObjectKind}', 9000000000, NULL),
    ($1, 'system', '{StoreSelfMetrics.SystemObjectKind}', 1000000000, NULL)", connection);
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-2), DateTimeKind.Unspecified));
        await insert.ExecuteNonQueryAsync(ct);

        return NpgsqlDataSource.Create(scratch.ConnectionString);
    }

    [Theory]
    [InlineData("zz_unenumerated_probe")]
    [InlineData("collect.zz_unenumerated_probe")]
    [InlineData("COLLECT.ZZ_Unenumerated_Probe")]
    public async Task AnUnenumeratedRelation_IsFoundByTheNameTheToolGives_AgainstDevPostgres(string objectName)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8817 test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var dataSource = await SeededStoreAsync(scratch, ct);

        using var document = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: objectName));
        var root = document.RootElement;
        Assert.False(root.TryGetProperty("status", out var status) && status.GetString() == "empty",
            $"the tool answered empty for '{objectName}', a relation it names in inventory.largest_unenumerated: {root}");
        Assert.Equal("unenumerated_relation", root.GetProperty("view").GetString());
        Assert.Equal("none", root.GetProperty("history").GetString());
        Assert.Equal(1, root.GetProperty("relations_matched").GetInt32());
        var relation = root.GetProperty("relations").EnumerateArray().Single();
        Assert.Equal(Backlog, relation.GetProperty("relation").GetString());
        Assert.True(relation.GetProperty("total_bytes").GetInt64() > 0);
        Assert.Contains("no growth history", root.GetProperty("note").GetString(), StringComparison.Ordinal);

        /* as_of is the last sweep's time and total_bytes is read live, so the reply carries the time of that read under its own name. */
        var asOf = DateTime.Parse(root.GetProperty("as_of").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind);
        var sizeReadAt = DateTime.Parse(root.GetProperty("size_read_at").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind);
        Assert.True(sizeReadAt > asOf, $"size_read_at {sizeReadAt:o} is not after the sweep time as_of {asOf:o}");
        Assert.True(sizeReadAt <= DateTime.UtcNow);
    }

    [Theory]
    [InlineData("zz_unenumerated_probe", 1, "collect.zz_unenumerated_probe")]
    [InlineData("collect.zz_unenumerated_probe", 1, "collect.zz_unenumerated_probe")]
    [InlineData("zz_unenumerated_pro", 2, null)]
    [InlineData("zz%probe", 0, null)]
    public async Task ThePlainStoreLookup_FindsTheSameRowsAsTheTimescaleDbLookup_AgainstDevPostgres(string objectName, int expectedMatched, string? expectedFirst)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8817 test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var dataSource = await SeededStoreAsync(scratch, ct);

        /* A second table whose name contains the first one's: an exact name must still answer with the one table, a partial name with both. */
        await using (var connection = await dataSource.OpenConnectionAsync(ct))
        await using (var second = new NpgsqlCommand("CREATE TABLE collect.zz_unenumerated_probe_two AS SELECT g FROM generate_series(1, 10) AS g", connection))
        {
            await second.ExecuteNonQueryAsync(ct);
        }

        var plain = new DarlingStoreMetricsReader.JobExecutionLoggingReading(DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered, null, null, null);
        var timescale = new DarlingStoreMetricsReader.JobExecutionLoggingReading(DarlingStoreMetricsReader.JobExecutionLoggingStatus.On, null, null, null);

        var plainFound = await DarlingStoreMetricsReader.GetUnenumeratedByNameAsync(dataSource, plain, objectName, 10, ct);
        var timescaleFound = await DarlingStoreMetricsReader.GetUnenumeratedByNameAsync(dataSource, timescale, objectName, 10, ct);

        Assert.NotNull(plainFound);
        Assert.NotNull(timescaleFound);
        Assert.Equal(expectedMatched, plainFound!.Matched);
        Assert.Equal(expectedMatched, plainFound.Relations.Count);
        if (expectedFirst is not null)
        {
            Assert.Equal(expectedFirst, plainFound.Relations.Single().Relation);
        }

        Assert.Equal(
            timescaleFound!.Relations.Select(r => (r.Relation, r.RelKind)).ToArray(),
            plainFound.Relations.Select(r => (r.Relation, r.RelKind)).ToArray());
        Assert.Equal(timescaleFound.Matched, plainFound.Matched);
    }

    [Fact]
    public async Task EveryRelationTheInventoryNames_CanBeAskedForByThatName_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8817 test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var dataSource = await SeededStoreAsync(scratch, ct);

        string[] named;
        using (var summary = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource)))
        {
            named = summary.RootElement.GetProperty("inventory").GetProperty("largest_unenumerated").EnumerateArray()
                .Select(r => r.GetProperty("relation").GetString()!).ToArray();
        }

        Assert.Contains(Backlog, named);
        foreach (var relation in named)
        {
            using var asked = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: relation));
            Assert.Equal("unenumerated_relation", asked.RootElement.GetProperty("view").GetString());
            Assert.Contains(asked.RootElement.GetProperty("relations").EnumerateArray(),
                r => string.Equals(r.GetProperty("relation").GetString(), relation, StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task ANameThatMatchesNothing_StillAnswersEmpty_AndSaysTheUnenumeratedRelationsWereChecked_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8817 test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var dataSource = await SeededStoreAsync(scratch, ct);

        /* The percent sign is a literal character, not a pattern: the name does not match the relation. */
        foreach (var name in new[] { "no_such_relation_anywhere", "zz%probe" })
        {
            using var none = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: name));
            Assert.Equal("empty", none.RootElement.GetProperty("status").GetString());
            Assert.Contains("no un-enumerated relation's name matches it either", none.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }

        /* An object_kind other than 'other' asked for a different kind: the reply does not claim a check it skipped. */
        using var otherKind = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(
            dataSource, object_kind: StoreSelfMetrics.TableObjectKind, object_name: "zz_unenumerated_probe"));
        Assert.Equal("empty", otherKind.RootElement.GetProperty("status").GetString());
        Assert.DoesNotContain("un-enumerated relation", otherKind.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

        /* object_kind other is where the relation's bytes live, so the name is looked up there. */
        using var inOther = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(
            dataSource, object_kind: StoreSelfMetrics.OtherObjectKind, object_name: "zz_unenumerated_probe"));
        Assert.Equal("unenumerated_relation", inOther.RootElement.GetProperty("view").GetString());
    }
}
