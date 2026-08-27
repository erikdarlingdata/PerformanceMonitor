/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2512: the permission-denial number set, and the Azure sentence that rides on 262.
///
/// <para><b>Why this set is a shared predicate rather than three literals, and why that is what gets
/// tested.</b> The numbers lived in three hand-maintained places — <c>SqlServerTargetProvider.Classify</c>,
/// Darling's worker catch filter, and Lite's <c>RunCollectorAsync</c> catch — and they had already
/// drifted apart (916 was in the first and neither of the other two). None of those three call sites can be
/// unit-tested directly, because <c>SqlException</c> cannot be constructed: the only way to pin them is to
/// give them one set to read and pin the set. That is what these do.</para>
///
/// <para><b>What 262 costs when it is missing.</b> "%ls permission denied in database '%.*ls'" is what a
/// database-scoped DMV read raises when the login does not hold the permission IN the named database. It is
/// unmistakably a permission denial, and it was classified <c>Unclassified</c> — which means ERROR, on
/// every collection cycle, forever. That is what the #2150 field report looked like (11x consecutive on an
/// Azure SQL Database elastic pool), and it is why <see cref="TempDbStatsCollector"/> was gated off the
/// entire Azure SQL Database tier rather than allowed to degrade. The gate is gone; this is what makes that
/// safe.</para>
/// </summary>
public sealed class SqlServerPermissionErrorsTests
{
    /// <summary>
    /// 262 is the number this issue adds; the other five are the pre-existing set, asserted so the
    /// extraction cannot have quietly dropped one on its way into a shared method.
    /// </summary>
    [Theory]
    [InlineData(229)]  /* EXECUTE/SELECT permission denied on an object */
    [InlineData(262)]  /* permission denied IN a database — the #2150/#2512 tempdb case */
    [InlineData(297)]  /* the user does not have permission to perform this action */
    [InlineData(300)]  /* VIEW SERVER STATE denied (a service-objective limit on Azure SQL DB) */
    [InlineData(916)]  /* the principal cannot access the database under the current security context */
    [InlineData(8189)] /* sys.traces' own denial, ALTER TRACE missing (#1823) */
    public void PermissionDenials_AreClassifiedAsPermissions(int number)
        => Assert.True(SqlServerPermissionErrors.IsPermissionDenied(number));

    /// <summary>
    /// The other side of the set. These must NOT degrade to PERMISSIONS: a missing object wants an install
    /// or an upgrade rather than a grant, a lock-timeout yield is evidence about the monitored server, and a
    /// timeout or an unrecognized number has to stay loud. A predicate that swallowed them would turn the
    /// non-fatal bucket into a place failures go to be ignored.
    /// </summary>
    [Theory]
    [InlineData(208)]   /* invalid object name -> ObjectMissing, not Permissions */
    [InlineData(1222)]  /* lock request timeout -> LockTimeoutYield for collectors that declare it */
    [InlineData(-2)]    /* command timeout */
    [InlineData(207)]   /* invalid column name (version drift) */
    [InlineData(40615)] /* Azure firewall rejection */
    [InlineData(0)]
    public void NonPermissionFailures_StayLoud(int number)
        => Assert.False(SqlServerPermissionErrors.IsPermissionDenied(number));

    /// <summary>
    /// 262 in TEMPDB on Azure SQL Database gets the same treatment 300 already got (#1631): the raw error
    /// names tempdb and reads as a missing GRANT, and there is no grant to issue there. Empty off Azure,
    /// where a 262 IS a missing grant and the fix is to issue it — the same asymmetry the 300 hint draws.
    /// </summary>
    [Fact]
    public void AzureDmvPermissionHint_ExplainsTempDb262_OnAzureOnly()
    {
        var azure262 = AzureDmvPermissionHint.For(262, isAzureSqlDb: true, TempDbDenial);

        Assert.Contains("TEMPDB", azure262, System.StringComparison.Ordinal);
        Assert.Contains("##MS_ServerStateReader##", azure262, System.StringComparison.Ordinal);
        Assert.Contains("no grant to issue", azure262, System.StringComparison.Ordinal);

        Assert.Empty(AzureDmvPermissionHint.For(262, isAzureSqlDb: false, TempDbDenial));

        /* The 300 arm is untouched by the switch that replaced its if-guard, and 229 still says nothing. */
        Assert.Contains("SERVICE OBJECTIVE", AzureDmvPermissionHint.For(300, isAzureSqlDb: true), System.StringComparison.Ordinal);
        Assert.Empty(AzureDmvPermissionHint.For(229, isAzureSqlDb: true));
    }

    /// <summary>
    /// The review catch on #2512, and the reason 262 reads the message where 300 does not. 300 is
    /// server-scoped, so its number settles it. 262 names a DATABASE, and the advice inverts on which one:
    /// in tempdb there is no grant to issue, in a user database there is and the raw error already names
    /// it. Keying purely off the number appended tempdb guidance — "reach tempdb's space DMVs
    /// through server-level state access" — to a denial in someone's user database, which is
    /// worse than appending nothing, because it sends them after a role membership that would not have
    /// helped. No collector raises that today, but the per-database loop collectors run against arbitrary
    /// user databases on Azure SQL DB and are one permission change away from it.
    /// <para>A null message stays silent rather than guessing, so a call site that forgets to pass it
    /// loses a helpful sentence instead of gaining a wrong one.</para>
    /// </summary>
    [Theory]
    [InlineData("VIEW DATABASE PERFORMANCE STATE permission denied in database 'AdventureWorks'.")]
    [InlineData("VIEW DATABASE STATE permission denied in database 'reporting_tempdb_stage'.")]
    [InlineData("")]
    [InlineData(null)]
    public void AzureDmvPermissionHint_SaysNothingAbout262_WhenTheDenialIsNotTempDb(string? message)
        => Assert.Empty(AzureDmvPermissionHint.For(262, isAzureSqlDb: true, message));

    /// <summary>The name is matched QUOTED, and the collation of the server decides its casing.</summary>
    [Theory]
    [InlineData("permission denied in database 'tempdb'.")]
    [InlineData("permission denied in database 'TempDB'.")]
    [InlineData("permission denied in database 'TEMPDB'.")]
    public void AzureDmvPermissionHint_MatchesTempDb_WhateverTheCasing(string message)
        => Assert.NotEmpty(AzureDmvPermissionHint.For(262, isAzureSqlDb: true, message));

    private const string TempDbDenial =
        "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";
}
