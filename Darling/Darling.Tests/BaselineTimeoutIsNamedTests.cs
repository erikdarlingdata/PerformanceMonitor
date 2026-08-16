/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A baseline query that ran out of time must SAY so, instead of reading as a broken connection.
///
/// <para><b>Observed on the dogfood box, 2026-08-16.</b> The service logged
/// <c>Failed to compute baselines for io_latency: Exception while reading from stream</c> — which reads as
/// a network fault. The store's own log, in a different file, showed
/// <c>ERROR: canceling statement due to user request</c> 267 ms earlier. Npgsql enforces its command
/// timeout by CANCELLING the statement, so the server reports the cancellation and the client is left
/// holding a torn stream. The real cause was a query outgrowing its deadline on a store that had grown to
/// 184 GB, and finding that out took correlating two logs by timestamp.</para>
///
/// <para>The consequence of mislabelling it is not cosmetic: "connection broke" invites someone to look at
/// the network, while "did not finish within its command timeout" points at the query and the window it
/// scans. The metric also silently loses its baseline for that pass, so anomaly detection for it goes quiet
/// while the collected data looks perfectly healthy.</para>
/// </summary>
public sealed class BaselineTimeoutIsNamedTests
{
    /// <summary>
    /// 57014 is <c>query_canceled</c> — the server telling us it cancelled the statement, which for this
    /// caller only ever happens because Npgsql's own deadline asked it to.
    /// </summary>
    [Fact]
    public void APostgresCancellationCountsAsATimeout()
        => Assert.True(PgBaselineProvider.IsCommandTimeout(new PostgresException(
            messageText: "canceling statement due to user request",
            severity: "ERROR",
            invariantSeverity: "ERROR",
            sqlState: "57014")));

    /// <summary>Npgsql's own client-side deadline, direct and wrapped.</summary>
    [Fact]
    public void ATimeoutExceptionCountsEitherDirectlyOrWrapped()
    {
        Assert.True(PgBaselineProvider.IsCommandTimeout(new TimeoutException("timed out")));
        Assert.True(PgBaselineProvider.IsCommandTimeout(
            new NpgsqlException("Exception while reading from stream", new TimeoutException())));
    }

    /// <summary>
    /// The other direction matters just as much: a GENUINE connection fault must keep saying so. Labelling
    /// it a timeout would be the identical defect aimed the other way, and it would send the next
    /// investigation at the query instead of the network.
    /// </summary>
    [Fact]
    public void ARealConnectionFaultIsNotCalledATimeout()
    {
        /* The bare message with no timeout in the chain — a reset socket, not a deadline. */
        Assert.False(PgBaselineProvider.IsCommandTimeout(
            new NpgsqlException("Exception while reading from stream", new System.IO.IOException("reset"))));
        Assert.False(PgBaselineProvider.IsCommandTimeout(new NpgsqlException("Exception while reading from stream")));

        /* And an ordinary SQL error is neither. */
        Assert.False(PgBaselineProvider.IsCommandTimeout(new PostgresException(
            messageText: "column does not exist",
            severity: "ERROR",
            invariantSeverity: "ERROR",
            sqlState: "42703")));

        Assert.False(PgBaselineProvider.IsCommandTimeout(new InvalidOperationException("something else")));
    }

    /// <summary>
    /// Classified STRUCTURALLY, not by message text. The message is the very thing that was ambiguous, so a
    /// guard that matched on it would be pinning the symptom that caused the confusion.
    /// </summary>
    [Fact]
    public void TheMessageTextAloneDecidesNothing()
    {
        const string ambiguous = "Exception while reading from stream";

        Assert.True(PgBaselineProvider.IsCommandTimeout(new NpgsqlException(ambiguous, new TimeoutException())));
        Assert.False(PgBaselineProvider.IsCommandTimeout(new NpgsqlException(ambiguous)));
    }
}
