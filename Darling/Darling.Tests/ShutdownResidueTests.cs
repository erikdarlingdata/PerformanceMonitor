/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2299 shutdown-residue classifier. The distinction it must preserve: <i>unfinished because we asked
/// it to stop</i> versus <i>unfinished because something broke</i>. A clean stop used to log seven ERRORs in
/// 800 ms — 7 of the day's 9 ERROR lines — because the analysis pass was still computing baselines when the
/// managed postmaster went down and the data source was disposed under it. Both directions of the two-factor
/// gate are pinned here, because each failing direction is a distinct real defect: classify too little and
/// every restart inflates the ERROR count again; classify too much and a data source disposed mid-run — a
/// genuine bug whose only evidence is these lines — logs as a shrug.
/// </summary>
public sealed class ShutdownResidueTests
{
    private static PostgresException Pg(string sqlState) => new(
        messageText: "terminating connection due to administrator command",
        severity: "FATAL",
        invariantSeverity: "FATAL",
        sqlState: sqlState);

    private static CancellationToken Signalled()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts.Token;
    }

    /// <summary>The three shapes the dogfood burst actually contained, plus the wrapped forms.</summary>
    [Fact]
    public void TheStopShapesAreShutdownShaped()
    {
        Assert.True(ShutdownResidue.IsShutdownShaped(Pg("57P01")));
        Assert.True(ShutdownResidue.IsShutdownShaped(new ObjectDisposedException("NpgsqlDataSource")));
        Assert.True(ShutdownResidue.IsShutdownShaped(new OperationCanceledException()));
        /* Npgsql routinely wraps the server's FATAL in its own envelope — the inner chain must be walked. */
        Assert.True(ShutdownResidue.IsShutdownShaped(
            new NpgsqlException("Exception while reading from stream", Pg("57P01"))));
    }

    /// <summary>
    /// 57014 (query_canceled) is deliberately NOT shutdown-shaped: that is the command-timeout signature
    /// <c>PgBaselineProvider.IsCommandTimeout</c> names specially, and reclassifying it here would hide a
    /// query outgrowing its deadline behind "we were stopping anyway".
    /// </summary>
    [Fact]
    public void ACommandTimeoutIsNotShutdownShaped()
    {
        Assert.False(ShutdownResidue.IsShutdownShaped(Pg("57014")));
        Assert.False(ShutdownResidue.IsShutdownShaped(new TimeoutException("timed out")));
    }

    /// <summary>A garden-variety fault stays a fault, stopping or not.</summary>
    [Fact]
    public void AGenuineFaultIsNeverAbandoned()
    {
        var fault = new NpgsqlException("Exception while reading from stream");

        Assert.False(ShutdownResidue.IsShutdownShaped(fault));
        Assert.False(ShutdownResidue.ShouldAbandon(fault, Signalled()));
    }

    /// <summary>
    /// The direction that guards the REAL bug: a disposed data source while the service is meant to be
    /// running must keep its ERROR — the token is the second factor, not decoration.
    /// </summary>
    [Fact]
    public void AShutdownShapeWithoutTheTokenKeepsItsError()
    {
        Assert.False(ShutdownResidue.ShouldAbandon(new ObjectDisposedException("NpgsqlDataSource"), CancellationToken.None));
        Assert.False(ShutdownResidue.ShouldAbandon(Pg("57P01"), CancellationToken.None));
    }

    [Fact]
    public void AShutdownShapeWithTheTokenIsAbandoned()
    {
        Assert.True(ShutdownResidue.ShouldAbandon(Pg("57P01"), Signalled()));
        Assert.True(ShutdownResidue.ShouldAbandon(new ObjectDisposedException("NpgsqlDataSource"), Signalled()));
    }

    /// <summary>The conversion keeps the real cause attached — the abandonment must never eat the evidence.</summary>
    [Fact]
    public void AbandonCarriesTheResidueAsTheInnerException()
    {
        var residue = Pg("57P01");
        var oce = ShutdownResidue.Abandon(residue, Signalled());

        Assert.Same(residue, oce.InnerException);
    }
}
