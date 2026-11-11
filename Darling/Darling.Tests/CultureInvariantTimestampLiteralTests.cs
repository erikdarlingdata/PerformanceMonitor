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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8604: a read that splices a <c>TIMESTAMP '...'</c> literal into its SQL must format the instant with the
/// invariant culture. In a custom .NET format the colon is the culture's own time separator, so on a host whose
/// separator is a dot (da-DK) the old text came out as <c>2026-11-11 02.00.00.000000</c>, which PostgreSQL rejects.
/// Each test builds a real statement under da-DK and reads every literal back.
/// </summary>
public sealed class CultureInvariantTimestampLiteralTests
{
    private static readonly Regex Literal = new(@"TIMESTAMP '([^']*)'", RegexOptions.CultureInvariant);

    /* Date-only literals (the fixed 2000-01-01 date_bin origin) are constants; every other literal carries a time. */
    private static readonly Regex Shape = new(
        @"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d{1,6})?)?$", RegexOptions.CultureInvariant);

    private const string Legacy = TimescaleSupport.QueryStatsHourlyView;
    private const string Successor = TimescaleSupport.QueryStatsIntervalHourlyView;

    private static readonly DateTime Now = new(2026, 11, 11, 2, 0, 0, DateTimeKind.Utc);

    private static RollupCoverage StitchedCoverage(string legacy, DateTime successorFloor)
    {
        var successor = TimescaleSupport.SuccessorOf(legacy)!;
        return new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [legacy] = Now.AddDays(-80), [successor] = successorFloor },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);
    }

    private static void UnderDanishCulture(Action body)
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            var danish = new CultureInfo("da-DK");

            /* The defect needs a host whose time separator is not a colon; without that this proves nothing. */
            Assert.NotEqual(":", danish.DateTimeFormat.TimeSeparator);

            CultureInfo.CurrentCulture = danish;
            body();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    private static string Text(DateTime value) =>
        value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    /// <summary>Every literal in <paramref name="sql"/> has the PostgreSQL timestamp shape, and each instant in
    /// <paramref name="expected"/> appears as a literal that parses back to exactly that value.</summary>
    private static void AssertLiterals(string sql, params DateTime[] expected)
    {
        var literals = Literal.Matches(sql).Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(literals);

        foreach (var literal in literals)
        {
            Assert.Matches(Shape, literal);
        }

        var parsed = literals
            .Where(l => l.Contains(' ', StringComparison.Ordinal))
            .Select(l => DateTime.ParseExact(
                l, l.Contains('.', StringComparison.Ordinal) ? "yyyy-MM-dd HH:mm:ss.FFFFFF" : "yyyy-MM-dd HH:mm:ss",
                CultureInfo.InvariantCulture, DateTimeStyles.None))
            .ToList();

        foreach (var instant in expected)
        {
            Assert.Contains(instant, parsed);
            Assert.Contains(Text(instant), literals);
        }
    }

    [Fact]
    public void StitchedRelationSql_SplitsAtAnInvariantLiteral_OnADotTimeSeparatorHost()
    {
        var successorFloor = Now; // 02:00:00, the instant from the field report
        var coverage = StitchedCoverage(Legacy, successorFloor);

        UnderDanishCulture(() =>
        {
            var sql = coverage.StitchedRelationSql(Legacy, "f", Now.AddDays(-10), RollupCoverage.StitchTier.Hourly);

            AssertLiterals(sql, successorFloor);
        });
    }

    [Fact]
    public void DailySummaryRangeSql_SplitsAtAnInvariantBoundaryDay_OnADotTimeSeparatorHost()
    {
        var successorFloor = Now; // mid-day, so the probe's boundary is the next whole day
        var coverage = StitchedCoverage(Legacy, successorFloor);

        UnderDanishCulture(() =>
        {
            var sql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, Now.AddDays(-10));

            AssertLiterals(sql, successorFloor.Date.AddDays(1));
        });
    }

    [Fact]
    public void ServerInventoryIdleCheck_UsesInvariantLiterals_OnADotTimeSeparatorHost()
    {
        var coverage = StitchedCoverage(TimescaleSupport.QueryStatsDbHourlyView, Now.AddDays(-3));
        var idleCutoff = new DateTime(2026, 11, 4, 4, 27, 0, DateTimeKind.Unspecified);
        var watermark = new DateTime(2026, 11, 10, 22, 0, 0, DateTimeKind.Unspecified);

        UnderDanishCulture(() =>
        {
            var sql = DarlingFinOpsInventoryReader.ServerMetricsSqlFor(coverage, idleCutoff, watermark);

            /* The ceiling hour of the cutoff (04:27 rounds up to 05:00), its floor hour (the idle-coverage
               oldest-bucket check), the watermark, and the stitch boundary. */
            AssertLiterals(
                sql,
                new DateTime(2026, 11, 4, 5, 0, 0),
                new DateTime(2026, 11, 4, 4, 0, 0),
                watermark,
                Now.AddDays(-3));
        });
    }

    [Fact]
    public void IdleDatabasesSql_UsesInvariantLiterals_OnADotTimeSeparatorHost()
    {
        var coverage = StitchedCoverage(TimescaleSupport.QueryStatsDbHourlyView, Now.AddDays(-3));
        var cutoff = new DateTime(2026, 11, 4, 4, 27, 0, DateTimeKind.Unspecified);
        var watermark = new DateTime(2026, 11, 10, 22, 0, 0, DateTimeKind.Unspecified);

        UnderDanishCulture(() =>
        {
            var sql = DarlingFinOpsOptimizationReader.IdleDatabasesSqlFor(coverage, cutoff, watermark);

            AssertLiterals(sql, new DateTime(2026, 11, 4, 5, 0, 0), watermark, Now.AddDays(-3));
        });
    }

    [Fact]
    public void IdleCoverageSql_UsesInvariantLiterals_OnADotTimeSeparatorHost()
    {
        var coverage = StitchedCoverage(TimescaleSupport.QueryStatsDbHourlyView, Now.AddDays(-3));
        var coverageStart = new DateTime(2026, 11, 4, 0, 0, 0, DateTimeKind.Unspecified);
        var watermark = new DateTime(2026, 11, 10, 22, 0, 0, DateTimeKind.Unspecified);

        UnderDanishCulture(() =>
        {
            var sql = DarlingFinOpsOptimizationReader.IdleCoverageSqlFor(coverage, coverageStart, watermark);

            AssertLiterals(sql, watermark, Now.AddDays(-3));
        });
    }

    [Fact]
    public void ExpensiveQueriesSqlFor_UsesInvariantLiterals_OnADotTimeSeparatorHost()
    {
        var ceiling = new DateTime(2026, 11, 4, 5, 0, 0, DateTimeKind.Unspecified);
        var trailing = new DateTime(2026, 11, 10, 22, 0, 0, DateTimeKind.Unspecified);

        UnderDanishCulture(() =>
        {
            var sql = DarlingFinOpsOptimizationReader.ExpensiveQueriesSqlFor(
                new DarlingFinOpsOptimizationReader.ExpensiveQueriesRoute(ceiling, trailing));

            AssertLiterals(sql, ceiling, trailing);
        });
    }
}
