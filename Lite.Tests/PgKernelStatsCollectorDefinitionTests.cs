/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the kcache collector on the four things that produce plausible, wrong output when they are missed.
/// </summary>
public class PgKernelStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170000,
            },
        };

    private static string Sql => PgKernelStatsCollector.Instance.BuildQuery(MakeContext()).Text;

    [Fact]
    public void Identity_IsTheTableAndEngineTheStoreExpects()
    {
        Assert.Equal("pg_kernel_stats", PgKernelStatsCollector.Instance.Name);
        Assert.Equal("pg_kernel_stats", PgKernelStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgKernelStatsCollector.Instance.TargetEngine);
    }

    /// <summary>
    /// Only top-level statements. With <c>pg_stat_statements.track = 'all'</c> a nested statement appears
    /// both on its own row and inside its caller's, so summing them double-counts every function body on
    /// the server. The rig ran <c>track = 'top'</c>, where the filter is inert — which is exactly why it
    /// needs a test rather than a memory.
    /// </summary>
    [Fact]
    public void OnlyTopLevelStatements_AreCounted()
    {
        Assert.Matches(new Regex(@"WHERE\s+k\.top\b"), Sql);
    }

    /// <summary>
    /// An extension function lives in the schema the extension was created in, not <c>pg_catalog</c> —
    /// <c>pg_catalog.pgstatindex</c> did not resolve at all when #2561 tried it.
    /// </summary>
    [Fact]
    public void TheExtensionFunction_IsQualifiedWhereItActuallyLives()
    {
        Assert.Contains("public.pg_stat_kcache()", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_catalog.pg_stat_kcache", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Seconds in, milliseconds out, and the column names say which. The function reports seconds; storing
    /// them under a name ending in <c>_ms</c> without the conversion would be off by a thousand and look
    /// entirely plausible.
    /// </summary>
    [Fact]
    public void TimesAreConvertedToMilliseconds_AndNamedThatWay()
    {
        foreach (var column in new[] { "exec_user_time_ms", "exec_system_time_ms", "plan_cpu_time_ms" })
        {
            Assert.Contains(column, PgKernelStatsCollector.Instance.PayloadColumns.Select(c => c.Name));
        }

        Assert.Contains("* 1000.0", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// I/O columns are named <c>_bytes</c>. These are bytes that reached the device, and a column called
    /// <c>exec_reads</c> sitting in a SQL Server codebase would be read as a logical-read count — a
    /// different measure entirely, and one this cannot be compared against.
    /// </summary>
    [Fact]
    public void IoColumns_AreNamedAsBytes()
    {
        var names = PgKernelStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("exec_read_bytes", names);
        Assert.Contains("exec_write_bytes", names);
        Assert.DoesNotContain("exec_reads", names);
        Assert.DoesNotContain("exec_writes", names);
    }

    /// <summary>
    /// Ranked by CPU. Elapsed time already lives in <c>pg_statement_stats</c>; CPU is what this adds, and
    /// ranking by bytes would answer a different question than the panel asks.
    /// </summary>
    [Fact]
    public void ItRanksByCpu_NotByBytes()
    {
        Assert.Matches(new Regex(@"ORDER BY\s+sum\(k\.exec_user_time \+ k\.exec_system_time\) DESC"), Sql);
    }

    /// <summary>
    /// <c>stats_since</c> is collected so a reset is a recorded fact rather than an inference from a
    /// counter that moved backwards — the improvement this table can afford over the wait profile.
    /// </summary>
    [Fact]
    public void TheResetStamp_IsCollected()
    {
        Assert.Contains("stats_since", PgKernelStatsCollector.Instance.PayloadColumns.Select(c => c.Name));
    }

    /// <summary>
    /// Per-database attribution is legitimate here and the collector takes it: the function exposes
    /// <c>dbid</c> and <c>pg_database</c> is a SHARED catalog, so the name resolves from whichever database
    /// the collector connected to. Contrast <c>pg_wait_sampling</c>, which has no such column and therefore
    /// claims no such thing (#2599).
    /// </summary>
    [Fact]
    public void ItAttributesToADatabase_BecauseTheCatalogSupportsIt()
    {
        Assert.Contains("database_name", PgKernelStatsCollector.Instance.PayloadColumns.Select(c => c.Name));
        Assert.Contains("pg_catalog.pg_database", Sql, StringComparison.Ordinal);

        /* Cluster-wide from one connection, so running per database would collect it N times over. */
        Assert.False(PgKernelStatsCollector.Instance.RunsPerDatabase(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 }));
    }
}
