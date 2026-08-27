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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the autovacuum read: ranking by each table's own threshold ratio rather than by raw dead-tuple
/// count, the three-part per-database key, and a severity that separates autovacuum losing a race from
/// autovacuum not running.
/// </summary>
public class DarlingPgAutovacuumReaderTests
{
    private static string Sql => DarlingPgAutovacuumReader.PgAutovacuumSql;

    /// <summary>
    /// The key is (database, schema, table), not table alone. This collector fans out per database, so two
    /// databases on one server routinely hold same-named tables — keying on table alone would collapse
    /// them and report one database's state as the other's.
    /// </summary>
    [Fact]
    public void KeysOnDatabaseSchemaAndTable()
    {
        Assert.Contains("DISTINCT ON (database_name, schema_name, table_name)", Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name, schema_name, table_name, collection_time DESC", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The ranking crux. Ordering by raw dead_tuples would put the biggest tables on top permanently —
    /// they always have the most dead tuples and are usually fine — and bury the small hot table that is
    /// fifty times past its line. Dividing by the table's own threshold is what makes them comparable.
    /// <para>Asserted by STRUCTURE rather than by an exact substring: the previous version matched
    /// <c>"l.dead_tuples::numeric / NULLIF(l.vacuum_threshold, 0) DESC"</c> verbatim and broke on a
    /// reformat that left the behaviour intact, which is a test failing for the wrong reason. The ordering
    /// SEMANTICS are what matter; they were hand-verified on live Aurora during the #2213 rounds
    /// (no live test carries that name - an earlier draft of this comment asserted one into
    /// existence).</para>
    /// </summary>
    [Fact]
    public void RanksByThresholdRatioNotRawDeadTupleCount()
    {
        var orderAt = Sql.IndexOf("ORDER BY", StringComparison.Ordinal);
        Assert.True(orderAt > 0, "the read must have an ORDER BY at all");

        var orderClause = Sql[orderAt..];

        /* Both ratios divide by the table's OWN threshold, which is what makes tables comparable. */
        Assert.Contains("l.dead_tuples::numeric", orderClause, StringComparison.Ordinal);
        Assert.Contains("NULLIF(l.vacuum_threshold, 0)", orderClause, StringComparison.Ordinal);
        Assert.Contains("l.inserts_since_vacuum::numeric", orderClause, StringComparison.Ordinal);
        Assert.Contains("l.insert_vacuum_threshold", orderClause, StringComparison.Ordinal);

        /* Both ratios must be considered TOGETHER — the worse one decides — rather than one after the other,
           which would let a table with zero dead tuples sort below every table that has any. Pinned on
           the RATIO GREATEST specifically: the raw tie-break below is itself GREATEST(dead, inserts),
           so a bare "GREATEST(" token is present even after the exact regression this guards
           (unwrapping the ratios into two sequential ORDER BY terms). */
        var collapsed = Regex.Replace(orderClause, @"\s+", "");
        Assert.Contains(
            "GREATEST(l.dead_tuples::numeric/NULLIF(l.vacuum_threshold,0),",
            collapsed, StringComparison.Ordinal);

        /* The disabled flag outranks everything, then the ratio, then the raw counts as a tie-break. */
        var disabledAt = orderClause.IndexOf("l.autovacuum_disabled DESC", StringComparison.Ordinal);
        var ratioAt = orderClause.IndexOf("l.dead_tuples::numeric", StringComparison.Ordinal);
        var rawAt = orderClause.LastIndexOf("GREATEST(l.dead_tuples, l.inserts_since_vacuum) DESC", StringComparison.Ordinal);
        Assert.True(
            disabledAt >= 0 && ratioAt > disabledAt && rawAt > ratioAt,
            $"ORDER BY must be disabled, then ratio, then raw counts (got {disabledAt}/{ratioAt}/{rawAt})");
    }

    /// <summary>
    /// A table with autovacuum switched off sorts to the top regardless of its count: it will never be
    /// vacuumed no matter how bad it gets, which is a configuration finding rather than a workload one.
    /// </summary>
    [Fact]
    public void SortsAutovacuumDisabledTablesFirst()
    {
        var orderAt = Sql.IndexOf("ORDER BY\n", StringComparison.Ordinal);
        var disabledAt = Sql.IndexOf("l.autovacuum_disabled DESC", StringComparison.Ordinal);
        var ratioAt = Sql.IndexOf("dead_tuples::numeric / NULLIF", StringComparison.Ordinal);

        Assert.True(orderAt < disabledAt && disabledAt < ratioAt);
    }

    /// <summary>
    /// NULLIF guards the never-analyzed table, whose threshold can be 0. Without it the division raises
    /// division_by_zero and the whole read fails because of one table.
    /// </summary>
    [Fact]
    public void GuardsDivisionByAZeroThreshold()
    {
        Assert.Contains("NULLIF(l.vacuum_threshold, 0)", Sql, StringComparison.Ordinal);
        Assert.Contains("NULLS LAST", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Growth separates two conditions with the same count and different fixes: climbing means autovacuum
    /// is losing a race, flat at ten times the threshold usually means it is blocked or switched off.
    /// </summary>
    [Fact]
    public void CarriesTheEarliestDeadTupleCountForGrowthComparison()
    {
        Assert.Contains("ORDER BY database_name, schema_name, table_name, collection_time ASC", Sql, StringComparison.Ordinal);
        Assert.Contains("first_dead_tuples", Sql, StringComparison.Ordinal);
        Assert.Contains("first_seen_at", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The join must use IS NOT DISTINCT FROM: database_name / schema_name / table_name are nullable
    /// columns, and a plain equality join silently drops every row where any of them is NULL.
    /// </summary>
    [Fact]
    public void JoinsTheTwoBranchesNullSafely()
    {
        Assert.Contains("e.database_name IS NOT DISTINCT FROM l.database_name", Sql, StringComparison.Ordinal);
        Assert.Contains("e.schema_name   IS NOT DISTINCT FROM l.schema_name", Sql, StringComparison.Ordinal);
        Assert.Contains("e.table_name    IS NOT DISTINCT FROM l.table_name", Sql, StringComparison.Ordinal);
        Assert.Equal(2, Sql.Split("server_id = $1").Length - 1);
        Assert.Equal(2, Sql.Split("collection_time >= $2").Length - 1);
    }

    [Fact]
    public void ReadsTheAutovacuumTableAndBoundsTheRowCount()
    {
        Assert.Contains("FROM pg_autovacuum_stats", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// autovacuum_enabled = false outranks any ratio, including a healthy one: the table is excluded from
    /// autovacuum entirely, so a low count today says nothing about tomorrow.
    /// </summary>
    [Fact]
    public void DisabledAutovacuumOutranksEveryRatio()
    {
        Assert.Equal(
            "critical_autovacuum_disabled_on_table",
            DarlingMcpPgAutovacuumTools.Classify(true, 0.01, false));
        Assert.Equal(
            "critical_autovacuum_disabled_on_table",
            DarlingMcpPgAutovacuumTools.Classify(true, null, false));
    }

    /// <summary>
    /// A missing threshold must read as unknown, never as healthy. The threshold is -1 (not applicable) or
    /// 0 (never analyzed), and calling either "ok" would hide a table nothing is known about.
    /// </summary>
    [Fact]
    public void MissingThresholdIsUnknownNotHealthy()
    {
        Assert.Equal("unknown_no_threshold", DarlingMcpPgAutovacuumTools.Classify(false, null, false));
        Assert.Equal("unknown_no_threshold", DarlingMcpPgAutovacuumTools.Classify(false, null, true));
    }

    /// <summary>The severity bands, and that growth only refines the middle one.</summary>
    [Theory]
    [InlineData(50.0, false, "critical_far_past_threshold")]
    [InlineData(10.0, false, "critical_far_past_threshold")]
    [InlineData(9.99, true, "warning_past_threshold_and_growing")]
    [InlineData(2.0, true, "warning_past_threshold_and_growing")]
    [InlineData(2.0, false, "warning_past_threshold")]
    [InlineData(1.0, false, "info_at_threshold")]
    [InlineData(1.99, false, "info_at_threshold")]
    [InlineData(0.5, true, "ok")]
    [InlineData(0.0, false, "ok")]
    public void ClassifiesByRatioBand(double ratio, bool growing, string expected)
    {
        Assert.Equal(expected, DarlingMcpPgAutovacuumTools.Classify(false, ratio, growing));
    }

    /// <summary>
    /// Ten times past the threshold is critical whether or not it is still growing — a pile that large and
    /// static is the WORSE case, because it means nothing is draining it at all.
    /// </summary>
    [Fact]
    public void FarPastThresholdIsCriticalEvenWhenFlat()
    {
        Assert.Equal("critical_far_past_threshold", DarlingMcpPgAutovacuumTools.Classify(false, 25.0, false));
        Assert.Equal("critical_far_past_threshold", DarlingMcpPgAutovacuumTools.Classify(false, 25.0, true));
    }

    /// <summary>Every severity is a distinct string, so a caller can switch on it.</summary>
    [Fact]
    public void SeveritiesAreDistinct()
    {
        var severities = new[]
        {
            DarlingMcpPgAutovacuumTools.Classify(true, 1.0, false),
            DarlingMcpPgAutovacuumTools.Classify(false, null, false),
            DarlingMcpPgAutovacuumTools.Classify(false, 20.0, false),
            DarlingMcpPgAutovacuumTools.Classify(false, 3.0, true),
            DarlingMcpPgAutovacuumTools.Classify(false, 3.0, false),
            DarlingMcpPgAutovacuumTools.Classify(false, 1.2, false),
            DarlingMcpPgAutovacuumTools.Classify(false, 0.1, false),
        };

        Assert.Equal(severities.Length, severities.Distinct().Count());
    }
}
