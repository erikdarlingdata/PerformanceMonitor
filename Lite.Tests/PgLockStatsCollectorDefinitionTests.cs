/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// #2544, the locks slice. The assertions are about what this collector must capture that
/// <see cref="PgBlockingCollector"/> structurally cannot — the lock MODE and the RELATION — and about the
/// scope mismatch between the two catalogs it reads.
/// </summary>
public class PgLockStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgLockStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
            },
            ExcludedDatabases = Array.Empty<string>(),
        }).Text;

    /// <summary>
    /// The whole reason this collector exists. <c>PgBlockingCollector</c> reads <c>pg_blocking_pids()</c>
    /// and never touches <c>pg_locks</c>, so the mode and the relation are unavailable today — and the mode
    /// is what decides the remedy.
    /// </summary>
    [Fact]
    public void ItReadsPgLocks_WhichTheBlockingCollectorDoesNot()
    {
        Assert.Contains("pg_catalog.pg_locks", Sql(), StringComparison.Ordinal);

        var blocking = PgBlockingCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 },
            ExcludedDatabases = Array.Empty<string>(),
        }).Text;

        /* Asserted rather than assumed: if the blocking collector ever grows a pg_locks read, these two
           overlap and that is a decision to make deliberately, not to discover. */
        Assert.DoesNotContain("pg_locks", blocking, StringComparison.Ordinal);
    }

    /// <summary>
    /// Mode and granted must both be captured and grouped on. Either alone is uninterpretable: a mode
    /// without granted cannot distinguish a queue from ordinary held locks, and granted without the mode
    /// cannot distinguish a DDL queue from write concurrency.
    /// </summary>
    [Fact]
    public void ModeAndGranted_AreBothGroupedOn()
    {
        var sql = Sql();

        Assert.Matches(new Regex(@"GROUP BY[^;]*l\.mode"), sql);
        Assert.Matches(new Regex(@"GROUP BY[^;]*l\.granted"), sql);
    }

    /// <summary>
    /// Both relation columns are stored. <c>pg_locks</c> is cluster-wide and <c>pg_class</c> is
    /// per-database, so a lock in another database has an OID and no name — measured, not assumed. Keeping
    /// only the name would drop the row; keeping only the OID would make the common case unreadable.
    /// </summary>
    [Fact]
    public void BothRelationColumns_AreStored_BecauseTheNameCanBeUnresolvable()
    {
        var names = PgLockStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("relation_oid", names);
        Assert.Contains("relation_name", names);
    }

    /// <summary>
    /// The relation join must be LEFT. An inner join silently drops every non-relation lock —
    /// <c>transactionid</c>, <c>virtualxid</c>, <c>advisory</c> — and every lock held in another database,
    /// which is the contention most worth seeing.
    /// </summary>
    [Fact]
    public void TheRelationJoin_IsLeft_SoNonRelationLocksSurvive()
        => Assert.Matches(new Regex(@"LEFT JOIN pg_catalog\.pg_class"), Sql());

    /// <summary>
    /// <c>relation_oid</c> is <c>bigint</c>, not <c>integer</c>. PostgreSQL OIDs are UNSIGNED 32-bit, so one
    /// past 2^31 lands negative in a signed int — rare, and silently wrong when it happens.
    /// </summary>
    [Fact]
    public void RelationOid_IsBigInt_BecauseOidsAreUnsigned32Bit()
        => Assert.Equal(
            CollectorColumnType.BigInt,
            PgLockStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "relation_oid").Type);

    /// <summary>
    /// The collector excludes its own backend. Without it every snapshot reports the AccessShareLocks the
    /// collector itself holds on the catalogs it is reading — the same self-marker rule the statement
    /// collector applies.
    /// </summary>
    [Fact]
    public void ItExcludesItsOwnBackend()
        => Assert.Matches(new Regex(@"l\.pid\s*<>\s*pg_catalog\.pg_backend_pid\(\)"), Sql());

    /// <summary>
    /// Wait time is measured from <c>state_change</c>, not <c>query_start</c>. A backend waiting on a lock
    /// has been in its current STATE since it began waiting, whereas <c>query_start</c> also covers the work
    /// it did before hitting the lock — which would overstate every wait by however long the statement had
    /// already been running.
    /// </summary>
    [Fact]
    public void WaitTime_ComesFromStateChange_NotQueryStart()
    {
        var sql = Sql();

        Assert.Contains("a.state_change", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("a.query_start", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The wait is only computed for ungranted rows. A granted lock has no wait, and reporting 0 would read
    /// as "granted instantly" — a measurement — where NULL is the absence of one.
    /// </summary>
    [Fact]
    public void WaitIsComputedOnlyForUngrantedRows()
        => Assert.Matches(new Regex(@"CASE WHEN NOT l\.granted"), Sql());

    /// <summary>
    /// Ungranted sorts first. A granted lock is not a finding — every working server holds thousands — so a
    /// grid ordered any other way buries the only rows worth reading.
    /// </summary>
    [Fact]
    public void UngrantedSortsFirst()
        => Assert.Matches(new Regex(@"ORDER BY l\.granted"), Sql());

    /// <summary>
    /// Catalog reads are schema-qualified: <c>pg_catalog</c> is searched implicitly but not necessarily
    /// FIRST, so an unqualified read can resolve to an object a user created in a schema earlier in the
    /// monitoring login's search_path.
    /// </summary>
    [Fact]
    public void EveryCatalogRead_IsSchemaQualified()
    {
        /* Comments stripped first: the query explains the cluster-wide/per-database split in prose that
           necessarily names the catalogs, and an unstripped scan matches the explanation rather than the
           code. This repo has hit that trap repeatedly. */
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        foreach (var view in new[] { "pg_locks", "pg_database", "pg_class", "pg_stat_activity" })
        {
            foreach (Match match in Regex.Matches(sql, $@"(\S*)\b{Regex.Escape(view)}\b"))
            {
                Assert.Equal("pg_catalog.", match.Groups[1].Value);
            }
        }
    }

    [Fact]
    public void AppliesTo_EveryPostgresTarget()
    {
        foreach (var major in new[] { 13, 14, 16, 17, 18 })
        {
            Assert.True(PgLockStatsCollector.Instance.AppliesTo(
                new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major }));
        }
    }

    /// <summary>One SELECT alias per payload column, in order — a mismatch is a silently shifted binary
    /// COPY, which writes every value into the wrong column rather than failing.</summary>
    [Fact]
    public void SelectAliases_MatchThePayloadOrder()
    {
        var expected = PgLockStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        var selected = Sql()
            .Split('\n')
            .Where(line => !line.TrimStart().StartsWith("FROM", StringComparison.Ordinal)
                        && !line.TrimStart().StartsWith("LEFT JOIN", StringComparison.Ordinal))
            .Select(line => Regex.Match(line, @"\bAS\s+([a-z_]+),?\s*$"))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value)
            .ToArray();

        Assert.Equal(expected, selected);
    }
}
