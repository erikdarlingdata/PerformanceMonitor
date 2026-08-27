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
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2219: PostgreSQL statement text, stored once per <c>(server_id, queryid)</c>.
///
/// <para><b>The gap.</b> <c>pg_statement_stats</c> identifies queries by <c>queryid</c> and stores no text —
/// <c>aurora_stat_statements</c>'s <c>showtext</c> is a real per-collection cost and normalized text is highly
/// repetitive. But <c>queryid</c> is NOT stable across a major version upgrade, so afterwards the stored history
/// joins to nothing readable: a list of integers that used to be your slowest queries, and unrecoverable, because
/// the live view no longer holds the old ids and anything else on the instance may have reset
/// <c>pg_stat_statements</c> out from under us.</para>
///
/// <para><b>Why inline text rather than the <c>query_text_dim</c> digest V64's comment promised.</b> The dimension
/// route is blocked and expensive to unblock — V38 is generated from <c>PayloadDimensions.All</c>, so registering
/// the fact table makes V38 <c>ALTER</c> a table it has not created yet, and it would break V64's own ladder diff.
/// More importantly the dimension needs the liveness interlock <see cref="QueryStorePlanMap"/> documents at
/// length, whose failure mode is SILENTLY missing text. Inline cannot dangle. The cost is cross-server dedup, a
/// few hundred MB on the measured fleet against a store whose Query Store plan XML alone was 43 GB.</para>
/// </summary>
public sealed class PgStatementTextTests
{
    /// <summary>
    /// The rung and the helper's own DDL must agree, or a fresh store and an upgraded one get different tables —
    /// the same discipline the ladder diff enforces for collector tables, applied by hand here because a
    /// non-collector table is outside that generator.
    /// </summary>
    [Fact]
    public void TheRungMatchesTheHelpersCreateTableSql()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == 73);

        Assert.Equal("pg-statement-text", rung.Name);
        Assert.Equal(Normalize(PgStatementText.CreateTableSql), Normalize(rung.Sql));
    }

    /// <summary>
    /// The ladder's own invariants, restated for this rung: it is the top, it is dense, and the build's schema
    /// version tracks it. A gap is skipped SILENTLY on every upgraded store, so the objects would never exist and
    /// no later upgrade would repair it.
    /// </summary>
    [Fact]
    public void TheRungIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        /* #2150 added V74, so this rung is no longer the top — the "I am the top" claim moves to the newest
           rung's own test (QueryStoreTextStoreTests) and this one keeps the invariants that stay true
           forever: the rung is PRESENT, the ladder is ordered and dense, and the build's schema version
           tracks the maximum. A gap is skipped SILENTLY on every upgraded store, so the objects would never
           exist and no later upgrade would repair it. */
        Assert.Contains(73, versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        /* Dense above the one sanctioned historical hole at V45. */
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The table is keyed on <c>(server_id, queryid)</c> — one row per statement per server, which is what makes
    /// "stored once" true and the upsert idempotent. Without the primary key the upsert has nothing to conflict
    /// on and every refresh would append a copy, which is precisely the per-snapshot duplication being avoided.
    /// </summary>
    [Fact]
    public void TheTableIsKeyedOnServerAndQueryId()
    {
        Assert.Contains("PRIMARY KEY (server_id, queryid)", PgStatementText.CreateTableSql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, queryid)", PgStatementText.UpsertSql, StringComparison.Ordinal);
        /* Pruned on last_seen, so it needs the index the prune scans. */
        Assert.Contains("idx_pg_statement_text_last_seen", PgStatementText.CreateTableSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>first_seen</c> is PRESERVED on conflict and <c>query_text</c> is advanced.
    ///
    /// <para>That asymmetry is the point: <c>first_seen</c> records when this statement shape was first seen on
    /// this server, which is the one fact that survives a major-version re-key and cannot be reconstructed
    /// afterwards — overwriting it would quietly turn every row's age into "since the last refresh". The text, by
    /// contrast, should track what the server says now.</para>
    /// </summary>
    [Fact]
    public void TheUpsertKeepsFirstSeenAndAdvancesTheText()
    {
        var setClause = PgStatementText.UpsertSql[PgStatementText.UpsertSql.IndexOf("DO UPDATE SET", StringComparison.Ordinal)..];

        Assert.Contains("query_text = EXCLUDED.query_text", setClause, StringComparison.Ordinal);
        Assert.Contains("last_seen = EXCLUDED.last_seen", setClause, StringComparison.Ordinal);
        Assert.DoesNotContain("first_seen = EXCLUDED", setClause, StringComparison.Ordinal);
    }

    /// <summary>
    /// The upsert is ORDERED by the conflict key. Concurrent batch upserts that take row locks in different
    /// relative orders deadlock (#1801), and this runs per server across a fleet — the same reason
    /// <see cref="QueryStorePlanMap.UpsertSql"/> carries its ORDER BY. Cheap to keep, expensive to rediscover.
    /// </summary>
    [Fact]
    public void TheUpsertIsOrderedByTheConflictKey()
    {
        var beforeConflict = PgStatementText.UpsertSql[..PgStatementText.UpsertSql.IndexOf("ON CONFLICT", StringComparison.Ordinal)];
        Assert.Contains("ORDER BY server_id, queryid", beforeConflict, StringComparison.Ordinal);
    }

    /// <summary>
    /// The fetch asks for text (<c>showtext = true</c>) — which is the entire reason it is a separate query from
    /// <c>pg_statement_stats</c>', which passes <c>false</c> every minute and must keep doing so.
    ///
    /// <para>Capped and ordered by total execution time, so a catalog larger than the cap keeps the text for the
    /// queries anyone would look at rather than an arbitrary slice; and parameterized rather than a hardcoded
    /// LIMIT, which the repo has had to correct once before.</para>
    /// </summary>
    [Fact]
    public void TheFetchAsksForTextAndIsBoundedByRank()
    {
        Assert.Contains("aurora_stat_statements(true)", PgStatementText.FetchSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY s.total_exec_time DESC", PgStatementText.FetchSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $1", PgStatementText.FetchSql, StringComparison.Ordinal);

        /* Every output column aliased, per the house rule — an unaliased expression comes back named after the
           function and the query stops being debuggable in psql. */
        Assert.Contains("AS queryid", PgStatementText.FetchSql, StringComparison.Ordinal);
        Assert.Contains("AS query_text", PgStatementText.FetchSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Due-ness is asked of the STORE, not remembered in the service — so a restart cannot re-fetch the fleet and
    /// two hosts writing one store cannot disagree about when text was last written. <c>COALESCE(..., TRUE)</c> is
    /// what makes a server with no rows yet due rather than never due.
    /// </summary>
    [Fact]
    public void DuenessComesFromTheStoreAndAFirstFetchIsDue()
    {
        Assert.Contains("max(last_seen)", PgStatementText.IsDueSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", PgStatementText.IsDueSql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(", PgStatementText.IsDueSql, StringComparison.Ordinal);
        Assert.Contains("TRUE)", PgStatementText.IsDueSql, StringComparison.Ordinal);
        /* The caller's clock, not the store's — the same value stamps the rows, so the cadence cannot drift
           against its own timestamps. */
        Assert.Contains("$2::timestamp", PgStatementText.IsDueSql, StringComparison.Ordinal);
        Assert.DoesNotContain("now()", PgStatementText.IsDueSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The prune margin makes text OUTLIVE the statistics that reference it, which is the opposite direction from
    /// the plan map's — and the asymmetry is the reason. Text kept past its facts is a few dead bytes; facts kept
    /// past their text is a top-queries answer that reads as a list of integers, the exact failure this table
    /// exists to fix.
    /// </summary>
    [Fact]
    public void ThePruneMarginKeepsTextAliveLongerThanItsFacts()
    {
        Assert.True(PgStatementText.PruneMarginDays > 0);

        var sql = PgStatementText.PruneSql(7);
        Assert.Contains("DELETE FROM collect.pg_statement_text", sql, StringComparison.Ordinal);
        Assert.Contains("last_seen < $1", sql, StringComparison.Ordinal);
        /* Time-sliced like every sibling purge, so one sweep cannot take an unbounded lock. */
        Assert.Contains("INTERVAL '7 days'", sql, StringComparison.Ordinal);
        /* Timestamp-driven, never an anti-join against the fact table — that is the cost this shape avoids. */
        Assert.DoesNotContain("pg_statement_stats", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="PgStatementText.Naive"/> strips the Kind without shifting the value. The #1969 trap is silent:
    /// Npgsql infers <c>timestamptz</c> from a Utc Kind, PostgreSQL converts into the session zone on the way into
    /// a naive column, and <c>last_seen</c> lands at the wrong hour — ageing text out ahead of the facts that
    /// reference it, which is this design's own failure mode arrived at through a timezone.
    /// </summary>
    [Fact]
    public void NaiveStripsTheKindWithoutShiftingTheValue()
    {
        var utc = new DateTime(2026, 8, 15, 4, 30, 0, DateTimeKind.Utc);
        var naive = PgStatementText.Naive(utc);

        Assert.Equal(DateTimeKind.Unspecified, naive.Kind);
        Assert.Equal(utc.Ticks, naive.Ticks);
    }

    /// <summary>
    /// The reader joins the text on <c>(server_id, queryid)</c> and LEFT joins it, so a statement whose text has
    /// not been captured yet still ranks — it simply reads as null. An inner join would silently drop exactly the
    /// newest and most interesting statements.
    /// </summary>
    [Fact]
    public void TheReaderLeftJoinsTheTextSoUncapturedStatementsStillRank()
    {
        var sql = DarlingPgStatementReader.PgTopQueriesSql;

        Assert.Contains("LEFT JOIN collect.pg_statement_text", sql, StringComparison.Ordinal);
        Assert.Contains("t.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("t.queryid = differenced.queryid", sql, StringComparison.Ordinal);
        /* MAX, because the read's grain is (queryid, database_id) while text is keyed on queryid alone —
           one text per group by construction, so this picks it without widening the GROUP BY. */
        Assert.Contains("MAX(t.query_text) AS query_text", sql, StringComparison.Ordinal);
        /* #2554: qualified, because THIS join is what made it ambiguous. */
        Assert.Contains("GROUP BY differenced.queryid, database_id", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The text refresh is hung off the statement-stats collector's success, keyed on the collector's OWN declared
    /// name so renaming it cannot silently unhook the text path — and it is best-effort, because losing text is a
    /// degraded read while losing a collection is lost data.
    /// </summary>
    [Fact]
    public void TheRefreshRidesTheStatsCollectorAndCannotCostACollection()
    {
        var source = ReadWorkerSource();

        Assert.Contains("PgStatementStatsCollector.Instance.Name", source, StringComparison.Ordinal);
        Assert.Contains("TryRefreshPgStatementTextAsync", source, StringComparison.Ordinal);
        /* Gated on the engine as well, rather than trusting the collector's own gate. */
        Assert.Contains("runtime.Target.Engine != CollectorTargetEngine.PostgreSql", source, StringComparison.Ordinal);
        /* A failure warns and leaves the statistics alone. */
        Assert.Contains("statistics are unaffected", source, StringComparison.Ordinal);
    }

    private static string Normalize(string sql) => string.Join(" ", sql.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
