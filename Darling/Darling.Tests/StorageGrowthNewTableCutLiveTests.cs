/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// A table with no row at the database's earliest snapshot in the window was created inside it, so it counts its
/// whole size as growth. The grid's summary and the heatmap's series must agree on that, and cut the same top-N.
/// </summary>
public sealed class StorageGrowthNewTableCutLiveTests
{
    private const string ServerName = "darling-sg-newtable-cut";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static readonly string[] Queries =
    [
        DarlingFinOpsStorageGrowthReader.ObjectGrowthSummarySql,
        DarlingFinOpsStorageGrowthReader.ObjectGrowthSeriesSql,
    ];

    [Fact]
    public void BothQueries_CountAMissingEarliestRowAsZero_AndOrderTheSameWay()
    {
        foreach (var sql in Queries)
        {
            Assert.Contains("COALESCE(e.e_reserved_mb, 0)", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("COALESCE(e.e_reserved_mb, l.cur_reserved_mb)", sql, StringComparison.Ordinal);
            Assert.Contains("growth_mb DESC NULLS LAST, l.schema_name, l.table_name", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void LiteQueries_CountAMissingEarliestRowAsZero_AndOrderTheSameWay()
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "Lite", "Services", "LocalDataService.FinOps.IndexObjects.cs")))
            dir = Path.GetDirectoryName(dir);
        Assert.NotNull(dir);
        var source = File.ReadAllText(Path.Combine(dir, "Lite", "Services", "LocalDataService.FinOps.IndexObjects.cs"));

        Assert.Equal(2, CountOf(source, "COALESCE(e.e_reserved_mb, 0)"));
        Assert.DoesNotContain("COALESCE(e.e_reserved_mb, l.cur_reserved_mb)", source, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(source, "growth_mb DESC NULLS LAST, l.schema_name, l.table_name"));
    }

    [Fact]
    public async Task TheGridAndTheHeatmap_CutTheSameTopN_AndRankATableCreatedInTheWindowByItsWholeSize()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live storage growth test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        var earliest = new DateTime(2026, 3, 1, 0, 10, 0, DateTimeKind.Unspecified);
        var latest = new DateTime(2026, 3, 31, 0, 10, 0, DateTimeKind.Unspecified);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);

            /* Two growing, three unchanged, two shrinking, one table that exists only at the latest instant and is the
               biggest, and one whose latest size is unknown. */
            foreach (var (table, before, after) in new (string, decimal?, decimal?)[]
                     {
                         ("Grow1", 100m, 150m), ("Grow2", 100m, 130m),
                         ("Flat1", 50m, 50m), ("Flat2", 50m, 50m), ("Flat3", 50m, 50m),
                         ("Shrink1", 200m, 150m), ("Shrink2", 200m, 100m),
                         ("NewBig", null, 400m),
                         ("NoSize", 10m, null),
                     })
            {
                if (before != null) await SeedAsync(c, table, earliest, before, ct);
                await SeedAsync(c, table, latest, after, ct);
            }
        }

        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        var (objects, samples) = await DarlingFinOpsStorageGrowthReader.GetObjectGrowthHeatmapDataAsync(
            ds, ServerId, "cutdb", earliest.Date, 30, 5, 30, ct);

        var summaryTables = objects.Select(o => o.TableName).ToArray();
        var seriesTables = samples.Select(s => s.ObjectKey.Split('.')[1]).Distinct().OrderBy(t => t, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "NewBig", "Grow1", "Grow2", "Flat1", "Flat2" }, summaryTables);
        Assert.Equal(summaryTables.OrderBy(t => t, StringComparer.Ordinal).ToArray(), seriesTables);

        var added = objects[0];
        Assert.Equal("NewBig", added.TableName);
        Assert.Equal(400m, added.Growth30dMb!.Value);
        Assert.Null(added.GrowthPct30d);
        Assert.DoesNotContain(summaryTables, t => t.StartsWith("Shrink", StringComparison.Ordinal));
        Assert.DoesNotContain(seriesTables, t => t.StartsWith("Shrink", StringComparison.Ordinal));
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal)) count++;
        return count;
    }

    private static Task SeedAsync(NpgsqlConnection c, string table, DateTime at, decimal? reservedMb, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates)
              VALUES ($1,$2,$3,$4,'cutdb','dbo',$5,$6,1,$7,'CLUSTERED',$8,$8,1000,0,0,0,0)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, 100 + table.Length, table, "PK_" + table, (object?)reservedMb ?? DBNull.Value);
}
