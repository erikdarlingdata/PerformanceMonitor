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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V257 (#8464): ONE new index, <c>idx_collection_log_status_time</c> on
/// <c>collect.collection_log (server_id, status, collection_time DESC)</c>, the supporting index for every "last collected" read's
/// newest-SUCCESS descent (<c>CollectionRunSql.SuccessfulCollectorRun</c> under <c>ORDER BY collection_time DESC LIMIT 1</c>). The
/// descent's behaviour on a server whose every collector fails is <c>ServerListAndSummaryPlanShapeTests</c>'s to prove; this file pins
/// the rung itself and its viewer probe sentinel.
/// </summary>
public sealed class CollectionLogStatusTimeIndexRungTests
{
    private const int RungVersion = 257;

    /// <summary>This rung's sentinel ordinal in the viewer probe (the <c>reader.GetBoolean</c> index and the parameter position).</summary>
    private const int ProbeOrdinal = 232;

    private const string IndexName = "idx_collection_log_status_time";

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegisteredOnADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();
        Assert.Contains(RungVersion, versions);
        Assert.Equal("collection-log-status-time", Rung.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.True(RungVersion <= StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>ONE additive, idempotent index, status between the server and the time: no table, no column, no GRANT, no view.</summary>
    [Fact]
    public void TheRungCreatesOneIndex_OnServerIdThenStatusThenCollectionTimeDescending_AndNothingElse()
    {
        var sql = Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains(
            $"CREATE INDEX IF NOT EXISTS {IndexName} ON collect.collection_log (server_id, status, collection_time DESC);",
            sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE INDEX"));
        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "GRANT", "CREATE TABLE", "VIEW", "DROP ", "ALTER TABLE", "CONCURRENTLY" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }
    }

    /// <summary>The viewer probes the index by name, and the sentinel maps to this rung: all sentinels present is the ladder's top, this one
    /// absent is the rung below.</summary>
    [Fact]
    public void TheViewerProbe_FindsTheIndex_AndMapsItToThisRung()
    {
        Assert.Contains($"EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{IndexName}')", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.True(ProbeOrdinal < parameters.Length, "the sentinel ordinal lies inside the probe");
        Assert.Equal("hasCollectionLogStatusTimeIndex", parameters[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(PgMigrations.Scripts[^1].Version, (int)method.Invoke(null, all)!);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var atThisRung = Enumerable.Range(0, parameters.Length).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(RungVersion - 1, (int)method.Invoke(null, behind)!);
    }
}
