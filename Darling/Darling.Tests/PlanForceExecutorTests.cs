/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The write path's exact SQL (#2138) — pinned as constants because these are the only statements
/// the bot may ever send at a monitored server, and every character of them was chosen against a
/// measured trap (#1914's argument-surface probing, the unforce replica asymmetry, DBCC's inability
/// to be parameterized set-based).
/// </summary>
public sealed class PlanForceExecutorTests
{
    [Fact]
    public void ForceSql_IsTheNamedTwoArgumentForm_WithNoReplicaOrOptimizedForcingArguments()
    {
        /* #1914: the documented four-argument order fails with 12463 on 2022 AND 2025 unless
           @disable_optimized_plan_forcing = 1; the plain named call works on both. And
           @replica_group_id must stay absent — a secondary-evidence target never reaches the
           executor (secondary_replica_evidence is a hard blocker), so the primary-default is always
           the right scope. */
        Assert.Equal(
            "EXEC sys.sp_query_store_force_plan @query_id = @query_id, @plan_id = @plan_id;",
            SqlServerPlanForceExecutor.ForceSql);
        Assert.DoesNotContain("replica_group_id", SqlServerPlanForceExecutor.ForceSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("disable_optimized_plan_forcing", SqlServerPlanForceExecutor.ForceSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void UnforceSql_MirrorsTheForce()
    {
        Assert.Equal(
            "EXEC sys.sp_query_store_unforce_plan @query_id = @query_id, @plan_id = @plan_id;",
            SqlServerPlanForceExecutor.UnforceSql);
    }

    [Fact]
    public void VerifySql_JudgesTheWholeQuery_SinceTheForce_WithRecompileAndADivideGuard()
    {
        var sql = SqlServerPlanForceExecutor.VerifySql;

        /* The cost aggregate must cover ALL plans for the query (join on query_id, not plan_id
           equality to the forced plan) — grading only the forced plan's executions would let a force
           that pushed work onto worse plans look like a win. */
        Assert.Contains("WHERE qsp.query_id = @query_id", sql, StringComparison.Ordinal);
        Assert.Contains("qsrsi.end_time >= @since_utc", sql, StringComparison.Ordinal);
        Assert.Contains("NULLIF(SUM(qsrs.count_executions), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("force_failure_count", sql, StringComparison.Ordinal);
        Assert.Contains("last_force_failure_reason_desc", sql, StringComparison.Ordinal);

        /* House collector-query conventions: AS table aliases, name = expression column aliases,
           OPTION(RECOMPILE) on each statement. */
        Assert.Contains("FROM sys.query_store_plan AS qsp", sql, StringComparison.Ordinal);
        Assert.Contains("cpu_per_exec_us =", sql, StringComparison.Ordinal);
        Assert.Equal(2, sql.Split("OPTION(RECOMPILE);", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public void NoStatementCarriesAUseClause_TheDatabaseTravelsAsChangeDatabase()
    {
        /* The database name is operator/collected data; it must never be concatenated into a
           statement. ChangeDatabase carries it out-of-band, so the constants must not smuggle a USE
           back in. */
        foreach (var sql in new[]
        {
            SqlServerPlanForceExecutor.ForceSql,
            SqlServerPlanForceExecutor.UnforceSql,
            SqlServerPlanForceExecutor.VerifySql,
            SqlServerPlanForceExecutor.EvictHandleLookupSql,
            SqlServerPlanForceExecutor.EvictSql,
        })
        {
            Assert.DoesNotContain("USE ", sql, StringComparison.OrdinalIgnoreCase);
            /* TRY_CAST over TRY_CONVERT is the house rule; here neither belongs — every value is a
               bound parameter, so any cast appearing is a smell. */
            Assert.DoesNotContain("TRY_CONVERT", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CONVERT(", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void EvictLookup_IsBounded_AndKeyedOnTheBoundPlanHash()
    {
        var sql = SqlServerPlanForceExecutor.EvictHandleLookupSql;

        Assert.Contains("SELECT TOP (16)", sql, StringComparison.Ordinal);
        Assert.Contains("deqs.query_plan_hash = @query_plan_hash", sql, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE);", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EvictSql_IsTheTargetedHandleForm_NeverTheWholeCache()
    {
        var sql = SqlServerPlanForceExecutor.EvictSql;

        Assert.Contains("DBCC FREEPROCCACHE(@handle)", sql, StringComparison.Ordinal);
        Assert.Contains("WITH NO_INFOMSGS", sql, StringComparison.Ordinal);
        /* A bare DBCC FREEPROCCACHE (no argument) clears the ENTIRE plan cache — pin that the
           argumentless form cannot appear. */
        Assert.DoesNotContain("FREEPROCCACHE;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FREEPROCCACHE()", sql, StringComparison.Ordinal);
    }

    /* ---------------- the hash parse ---------------- */

    [Theory]
    [InlineData("0x1A2B3C4D5E6F7081", 8)]
    [InlineData("1A2B3C4D5E6F7081", 8)]
    [InlineData("0xAB", 1)]
    public void TryParsePlanHash_AcceptsHexWithOrWithoutThePrefix(string hex, int expectedBytes)
    {
        Assert.True(SqlServerPlanForceExecutor.TryParsePlanHash(hex, out var bytes));
        Assert.Equal(expectedBytes, bytes.Length);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("0x")]
    [InlineData("0x123")]
    [InlineData("0xZZZZ")]
    [InlineData("0x1A2B3C4D5E6F708192")]
    public void TryParsePlanHash_RejectsGarbage_RatherThanEvictingByAccident(string? hex)
    {
        Assert.False(SqlServerPlanForceExecutor.TryParsePlanHash(hex, out _));
    }
}
