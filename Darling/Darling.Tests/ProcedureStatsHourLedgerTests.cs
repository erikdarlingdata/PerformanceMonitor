/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9357: the V299 rung and the procedure_stats side of the count guard. The ledger is the query_stats ledger (V164) for the
/// procedure rollup, so the table text must be the query_stats text with the source's name, and the guard must read each
/// source's own ledger, state table and rollup. The live behaviour (the writer, the guard's verdicts) is in
/// <c>ProcedureStatsHourLedgerLiveTests</c>.
/// </summary>
public sealed class ProcedureStatsHourLedgerTests
{
    [Fact]
    public void TheRungIsRegisteredOnce_UnderTheConstantTheRunnerReads()
    {
        Assert.Equal(299, ProcedureStatsHourLedger.RungVersion);
        var rung = Assert.Single(PgMigrations.Scripts, m => m.Name == "procedure-stats-hour-ledger");
        Assert.Equal(ProcedureStatsHourLedger.RungVersion, rung.Version);
        Assert.Equal(ProcedureStatsHourLedger.CreateSql, rung.Sql);
    }

    [Fact]
    public void TheTablesAreTheQueryStatsLedgerTablesWithTheProcedureStatsName()
    {
        string Swap(string text) => text.Replace("query_stats", "procedure_stats", StringComparison.Ordinal);

        Assert.Equal(ProcedureStatsHourLedger.CreateSql, Swap(QueryStatsHourLedger.CreateSql));
        Assert.Equal(ProcedureStatsHourLedger.UpsertSql, Swap(QueryStatsHourLedger.UpsertSql));
        Assert.Equal(ProcedureStatsHourLedger.PruneSql, Swap(QueryStatsHourLedger.PruneSql));
        Assert.Equal(ProcedureStatsHourLedger.RecountSql, Swap(QueryStatsHourLedger.RecountSql));
        Assert.Equal(ProcedureStatsHourLedger.LedgerTable, Swap(QueryStatsHourLedger.LedgerTable));
        Assert.Equal(ProcedureStatsHourLedger.StateTable, Swap(QueryStatsHourLedger.StateTable));
    }

    [Fact]
    public void TheGuardReadsEachSourcesOwnLedgerStateAndRollup()
    {
        var procedure = IntervalRollupCountGuard.SqlFor("procedure_stats", withUnregisteredServers: false);
        Assert.Equal(IntervalRollupCountGuard.ProcedureStatsSql, procedure);
        Assert.Contains(ProcedureStatsHourLedger.LedgerTable, procedure, StringComparison.Ordinal);
        Assert.Contains(ProcedureStatsHourLedger.StateTable, procedure, StringComparison.Ordinal);
        Assert.Contains("collect.procedure_stats_interval_hourly", procedure, StringComparison.Ordinal);
        Assert.DoesNotContain("query_stats", procedure, StringComparison.Ordinal);

        var query = IntervalRollupCountGuard.SqlFor("query_stats", withUnregisteredServers: false);
        Assert.Equal(IntervalRollupCountGuard.QueryStatsSql, query);
        Assert.Equal(IntervalRollupCountGuard.SqlFor(false), query);
        Assert.DoesNotContain("procedure_stats", query, StringComparison.Ordinal);

        /* Apart from the three relation names, the two statements are one statement. */
        Assert.Equal(
            query,
            procedure
                .Replace(ProcedureStatsHourLedger.LedgerTable, QueryStatsHourLedger.LedgerTable, StringComparison.Ordinal)
                .Replace(ProcedureStatsHourLedger.StateTable, QueryStatsHourLedger.StateTable, StringComparison.Ordinal)
                .Replace("collect.procedure_stats_interval_hourly", "collect.query_stats_interval_hourly", StringComparison.Ordinal));

        Assert.Equal(QueryStatsHourLedger.RungVersion, IntervalRollupCountGuard.RungVersionFor("query_stats"));
        Assert.Equal(ProcedureStatsHourLedger.RungVersion, IntervalRollupCountGuard.RungVersionFor("procedure_stats"));
        Assert.Throws<ArgumentException>(() => IntervalRollupCountGuard.SqlFor("wait_stats", withUnregisteredServers: false));
        Assert.Throws<ArgumentException>(() => IntervalRollupCountGuard.RungVersionFor("wait_stats"));
    }

    [Fact]
    public void TheUnregisteredNameArm_IsTheSameForBothSources()
    {
        var procedure = IntervalRollupCountGuard.SqlFor("procedure_stats", withUnregisteredServers: true);
        var query = IntervalRollupCountGuard.SqlFor("query_stats", withUnregisteredServers: true);
        Assert.Contains("$4", procedure, StringComparison.Ordinal);
        Assert.Equal(query.Count(c => c == '$'), procedure.Count(c => c == '$'));
    }
}
