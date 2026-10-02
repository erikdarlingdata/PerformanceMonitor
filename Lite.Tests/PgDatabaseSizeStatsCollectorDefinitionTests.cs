/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the per-database size collector (#3691, Darling V136): the identity, the gate that gates on nothing,
/// the two privilege tests the query makes BEFORE it sizes a database, the BYTES-only column contract, the
/// templates-are-collected rule, and the one decision the collector makes that the query does not — the
/// instance total is NULL on every row when any database's size is, because a partial sum is not a total.
/// </summary>
public class PgDatabaseSizeStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(ICollectorDeltaCalculator? deltas = null)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas ?? s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned_AndTheEngineIsStructural()
    {
        Assert.Equal("pg_database_size_stats", PgDatabaseSizeStatsCollector.Instance.Name);
        Assert.Equal("pg_database_size_stats", PgDatabaseSizeStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgDatabaseSizeStatsCollector.Instance.TargetEngine);
        Assert.Null(PgDatabaseSizeStatsCollector.Instance.WatermarkColumn);
    }

    /// <summary>
    /// Every PostgreSQL target: stock or Aurora, writer or replica, any major. <c>pg_database</c> is a shared
    /// catalog and <c>pg_database_size()</c> is core, so there is nothing to gate on — and no per-database
    /// fan-out, because one connection sees every database.
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public void AppliesToEveryPostgresTarget_AndRunsOnce(bool isAurora, bool inRecovery)
    {
        var target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = 15,
            IsAurora = isAurora,
            IsInRecovery = inRecovery,
        };

        Assert.True(PgDatabaseSizeStatsCollector.Instance.AppliesTo(target));
        Assert.False(PgDatabaseSizeStatsCollector.Instance.RunsPerDatabase(target));
    }

    /// <summary>
    /// The query sizes a database only after BOTH privilege tests <c>dbsize.c</c> makes have been asked —
    /// <c>CONNECT</c> on the database, or the privileges of <c>pg_read_all_stats</c> — so a denied database
    /// is a NULL row rather than a raised error that takes the whole collection down. Templates are NOT
    /// filtered: every database is a row, and <c>is_template</c> is how a consumer sets them aside.
    /// </summary>
    [Fact]
    public void TheQuery_GuardsTheSizeCallWithBothPrivilegeTests_AndFiltersNothing()
    {
        var sql = PgDatabaseSizeStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("has_database_privilege(d.oid, 'CONNECT')", sql, StringComparison.Ordinal);
        Assert.Contains("pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')", sql, StringComparison.Ordinal);
        Assert.Contains("THEN pg_database_size(d.oid)", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULL", sql, StringComparison.Ordinal);

        /* The privilege tests come BEFORE the size call in the CASE — the whole point is that the call is
           never evaluated for a database that fails them. */
        Assert.True(
            sql.IndexOf("has_database_privilege", StringComparison.Ordinal)
                < sql.IndexOf("pg_database_size(d.oid)", StringComparison.Ordinal));

        Assert.Contains("FROM pg_database AS d", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("datistemplate = false", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("datallowconn", sql.Split("FROM")[1], StringComparison.Ordinal);

        /* No total in SQL: that decision belongs to ReadAsync, once, where "NULL if any is NULL" can be
           stated rather than left to an aggregate's NULL-skipping. */
        Assert.DoesNotContain("SUM(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("OVER (", sql, StringComparison.OrdinalIgnoreCase);

        /* No clock: StoreSqlClockDisciplineTests' rule, restated where the query lives. */
        Assert.DoesNotContain("now()", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// BYTES, bigint, both size columns — the naming contract a later join against the store's own
    /// <c>collect.store_metrics.total_bytes</c> or the SQL Server side rests on. No column here says MB or GiB.
    /// </summary>
    [Fact]
    public void ThePayload_IsBytesOnly_InThisOrder()
    {
        var columns = PgDatabaseSizeStatsCollector.Instance.PayloadColumns;

        Assert.Equal(
            new[] { "database_name", "size_bytes", "total_bytes", "is_template", "allows_connections" },
            columns.Select(c => c.Name).ToArray());

        Assert.Equal(CollectorColumnType.Varchar, columns[0].Type);
        Assert.Equal(CollectorColumnType.BigInt, columns[1].Type);
        Assert.Equal(CollectorColumnType.BigInt, columns[2].Type);
        Assert.Equal(CollectorColumnType.Boolean, columns[3].Type);
        Assert.Equal(CollectorColumnType.Boolean, columns[4].Type);

        Assert.DoesNotContain(columns, c => c.Name.Contains("_mb", StringComparison.Ordinal));
        Assert.DoesNotContain(columns, c => c.Name.Contains("_gb", StringComparison.Ordinal));
    }

    /// <summary>
    /// Every database sizeable → every row carries the same instance total, and it is the sum.
    /// </summary>
    [Fact]
    public async Task ReadsEveryDatabase_AndStampsTheInstanceTotalOnEveryRow()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { "appdb", 9_663_676_416L, false, true },
            new object[] { "postgres", 8_192_000L, false, true },
            new object[] { "template0", 7_500_000L, true, false },
            new object[] { "template1", 7_600_000L, true, true });

        var rows = await PgDatabaseSizeStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        Assert.Equal(4, rows.Count);
        Assert.All(rows, r => Assert.Equal(9_663_676_416L + 8_192_000L + 7_500_000L + 7_600_000L, r.TotalBytes));

        var template0 = Assert.Single(rows, r => r.DatabaseName == "template0");
        Assert.True(template0.IsTemplate);
        Assert.False(template0.AllowsConnections);
        Assert.Equal(7_500_000L, template0.SizeBytes);

        var app = Assert.Single(rows, r => r.DatabaseName == "appdb");
        Assert.False(app.IsTemplate);
        Assert.True(app.AllowsConnections);
    }

    /// <summary>
    /// The denied-database arm: the vendor-owned database on a managed target arrives with a NULL size, the
    /// row is KEPT (never dropped, never 0), the other databases keep their own sizes — and the instance total
    /// is NULL on EVERY row, because a sum over the databases this role can see is not the instance total and
    /// a consumer trending it would read the denied database's growth as the others' shrinkage.
    /// </summary>
    [Fact]
    public async Task ADeniedDatabase_IsANullSize_AndMakesTheInstanceTotalNullOnEveryRow()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { "appdb", 9_663_676_416L, false, true },
            new object[] { "rdsadmin", DBNull.Value, false, true },
            new object[] { "template1", 7_600_000L, true, true });

        var rows = await PgDatabaseSizeStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        Assert.Equal(3, rows.Count);
        var denied = Assert.Single(rows, r => r.DatabaseName == "rdsadmin");
        Assert.Null(denied.SizeBytes);
        Assert.Equal(9_663_676_416L, Assert.Single(rows, r => r.DatabaseName == "appdb").SizeBytes);

        Assert.All(rows, r => Assert.Null(r.TotalBytes));
    }

    /// <summary>The total rule in isolation: sum when every size is measured, NULL everywhere otherwise, and
    /// a zero-sized database is a measurement that counts (0 is not NULL).</summary>
    [Fact]
    public void WithInstanceTotal_SumsOnlyWhenEverySizeIsMeasured()
    {
        static PgDatabaseSizeStatsCollector.Row Row(string name, long? size) => new(name, size, null, false, true);

        var complete = PgDatabaseSizeStatsCollector.WithInstanceTotal(new List<PgDatabaseSizeStatsCollector.Row>
        {
            Row("a", 10), Row("b", 0), Row("c", 5),
        });
        Assert.All(complete, r => Assert.Equal(15L, r.TotalBytes));

        var partial = PgDatabaseSizeStatsCollector.WithInstanceTotal(new List<PgDatabaseSizeStatsCollector.Row>
        {
            Row("a", 10), Row("b", null), Row("c", 5),
        });
        Assert.All(partial, r => Assert.Null(r.TotalBytes));

        Assert.Empty(PgDatabaseSizeStatsCollector.WithInstanceTotal(new List<PgDatabaseSizeStatsCollector.Row>()));
    }

    [Fact]
    public async Task ReturnsNoRowsWhenTheCatalogReturnsNone()
    {
        var rows = await PgDatabaseSizeStatsCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>Size is a LEVEL; the growth rate is a difference over the stored series. No delta at write.</summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();

        PgDatabaseSizeStatsCollector.Instance.WritePayload(
            new PgDatabaseSizeStatsCollector.Row("appdb", 1, 1, false, true),
            new RecordingCollectorRowWriter(),
            MakeContext(deltas: deltas));

        Assert.Empty(deltas.Calls);
    }

    /// <summary>
    /// Positional, in <c>PayloadColumns</c> order, and a NULL size and total are written as null — never 0 —
    /// so the COPY lands "not measured" in the store as "not measured".
    /// </summary>
    [Fact]
    public void WritesEveryPayloadColumnInOrder_AndNullStaysNull()
    {
        var writer = new RecordingCollectorRowWriter();

        PgDatabaseSizeStatsCollector.Instance.WritePayload(
            new PgDatabaseSizeStatsCollector.Row("appdb", 4_096L, 8_192L, false, true),
            writer,
            MakeContext());

        Assert.Equal(PgDatabaseSizeStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal("appdb", writer.Values[0]);
        Assert.Equal(4_096L, writer.Values[1]);
        Assert.Equal(8_192L, writer.Values[2]);
        Assert.Equal(false, writer.Values[3]);
        Assert.Equal(true, writer.Values[4]);

        var denied = new RecordingCollectorRowWriter();
        PgDatabaseSizeStatsCollector.Instance.WritePayload(
            new PgDatabaseSizeStatsCollector.Row("rdsadmin", null, null, false, true),
            denied,
            MakeContext());

        Assert.Null(denied.Values[1]);
        Assert.Null(denied.Values[2]);
    }
}
