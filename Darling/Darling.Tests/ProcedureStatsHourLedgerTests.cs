/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9357: the V300 rung. The ledger statements both sources run come from <c>HourLedgerStatements</c>, and the behaviour
/// (the writer, the guard's verdicts, each source's own ledger) is in <c>ProcedureStatsHourLedgerLiveTests</c>.
/// </summary>
public sealed class ProcedureStatsHourLedgerTests
{
    [Fact]
    public void TheRungIsRegisteredOnce_UnderTheConstantTheRunnerReads()
    {
        Assert.Equal(300, ProcedureStatsHourLedger.RungVersion);
        var rung = Assert.Single(PgMigrations.Scripts, m => m.Name == "procedure-stats-hour-ledger");
        Assert.Equal(ProcedureStatsHourLedger.RungVersion, rung.Version);
        Assert.Equal(ProcedureStatsHourLedger.CreateSql, rung.Sql);
    }
}
