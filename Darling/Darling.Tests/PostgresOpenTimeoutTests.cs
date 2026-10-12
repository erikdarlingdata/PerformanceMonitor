// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5016: every shape a timed-out store open takes answers "could not get a store connection in time", and
/// every failure that is not a timeout keeps "could not open a store connection". Each exception below is built
/// the way Npgsql 10.0.3 builds it (the source file is named on each test), so no socket, timer or sleep is
/// involved. The silent-store test in <see cref="ComposeOpenPhaseTimeoutTests"/> drives the real driver once;
/// this class pins the shapes a loaded machine can produce instead of the one a quiet machine usually does.
/// </summary>
public sealed class PostgresOpenTimeoutTests
{
    private const string InTime = "Error running query: could not get a store connection in time: ";
    private const string NotOpened = "Error running query: could not open a store connection: ";

    private static string Answer(Exception inner)
    {
        var outcome = DarlingWebEndpoints.FromRunException(new DarlingWebEndpoints.ComposeStoreOpenException(inner));
        Assert.True(outcome.IsServerError);
        Assert.Null(outcome.AuthorSqlState);
        return outcome.Error!;
    }

    private static void AssertTimedOut(Exception inner) => Assert.Equal(InTime + inner.Message, Answer(inner));

    private static void AssertNotATimeout(Exception inner) => Assert.Equal(NotOpened + inner.Message, Answer(inner));

    // ---- timed-out shapes ----

    [Fact] // NpgsqlReadBuffer.cs:362-367, 380-382: a startup or login read whose timer fired
    public void AStartupReadThatTimedOut_IsATimeout() =>
        AssertTimedOut(new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt")));

    [Fact] // NpgsqlWriteBuffer.cs:172-178: a startup send whose timer fired
    public void AStartupWriteThatTimedOut_IsATimeout() =>
        AssertTimedOut(new NpgsqlException("Exception while writing to stream", new TimeoutException("Timeout during writing attempt")));

    [Fact] // NpgsqlTimeout.cs:23-27 and ThrowHelper.cs:108-109: the budget was already spent (also the driver's retry after a first attempt)
    public void ABudgetAlreadySpent_IsATimeout() =>
        AssertTimedOut(new NpgsqlException("The operation has timed out", new TimeoutException()));

    [Fact] // NpgsqlConnector.cs:1451-1459: the TCP connect did not finish
    public void ATcpConnectThatTimedOut_IsATimeout() =>
        AssertTimedOut(new NpgsqlException("Failed to connect to 10.0.0.1:5432", new TimeoutException("Timeout during connection attempt")));

    [Fact] // PoolingDataSource.cs:176-179: the pool wait ended
    public void APoolWaitThatEnded_IsATimeout() =>
        AssertTimedOut(new NpgsqlException(
            "The connection pool has been exhausted, either raise 'Max Pool Size' (currently 100) or 'Timeout' (currently 15 seconds) in your connection string.",
            new TimeoutException()));

    [Fact] // NpgsqlConnector.cs:812-814 over :362-367: the read timeout inside the GSS step, wrapped once more
    public void AReadTimeoutWrappedByTheGssStep_IsATimeout() =>
        AssertTimedOut(new NpgsqlException(
            "Exception while performing GSS encryption",
            new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"))));

    [Fact] // NpgsqlConnector.cs:1451-1459 with the operating system's error: it gave up before the driver's timer
    public void ATcpConnectTheOperatingSystemTimedOut_IsATimeout() =>
        AssertTimedOut(new NpgsqlException("Failed to connect to 10.0.0.1:5432", new SocketException((int)SocketError.TimedOut)));

    [Fact] // NpgsqlReadBuffer.cs:176 and :235: a stream read that failed with the operating system's timed-out error
    public void AStreamReadTheOperatingSystemTimedOut_IsATimeout() =>
        AssertTimedOut(new NpgsqlException(
            "Exception while reading from stream",
            new IOException(
                "Unable to read data from the transport connection: A connection attempt failed because the connected party did not properly respond after a period of time.",
                new SocketException((int)SocketError.TimedOut))));

    [Fact] // NpgsqlDataSource.cs:296-298: the wait for the first-time setup ended; a bare TimeoutException, not an NpgsqlException
    public void AFirstTimeSetupWaitThatEnded_IsATimeout() =>
        AssertTimedOut(new TimeoutException());

    // ---- failures that are not timeouts ----

    [Fact] // NpgsqlConnector.cs:1459 over a refused connect
    public void ARefusedConnection_IsNotATimeout() =>
        AssertNotATimeout(new NpgsqlException("Failed to connect to 127.0.0.1:5432", new SocketException((int)SocketError.ConnectionRefused)));

    [Fact] // NpgsqlReadBuffer.cs:371: the server reset the connection during startup
    public void AConnectionTheServerReset_IsNotATimeout() =>
        AssertNotATimeout(new NpgsqlException(
            "Exception while reading from stream",
            new IOException(
                "Unable to read data from the transport connection: An existing connection was forcibly closed by the remote host.",
                new SocketException((int)SocketError.ConnectionReset))));

    [Fact] // NpgsqlReadBuffer.cs:297-298: the server closed the connection (a read of zero bytes)
    public void AConnectionTheServerClosed_IsNotATimeout() =>
        AssertNotATimeout(new NpgsqlException("Exception while reading from stream", new EndOfStreamException("Attempted to read past the end of the stream.")));

    [Fact] // NpgsqlConnector.Auth.cs:147: authentication cannot proceed
    public void AnAuthenticationFailure_IsNotATimeout() =>
        AssertNotATimeout(new NpgsqlException("No password has been provided but the backend requires one (in SASL/SCRAM-SHA-256)"));

    [Fact] // NpgsqlConnector.cs:1398-1400: the name does not resolve
    public void ANameThatDoesNotResolve_IsNotATimeout() =>
        AssertNotATimeout(new NpgsqlException("No such host is known.", new SocketException((int)SocketError.HostNotFound)));

    [Fact] // NpgsqlConnector.cs:366-367: a caller's own cancellation is never the open running out of time
    public void ACallersCancellation_IsNotATimeout()
    {
        using var source = new CancellationTokenSource();
        source.Cancel();
        AssertNotATimeout(new NpgsqlException(
            "Exception while reading from stream",
            new OperationCanceledException("Query was cancelled", new TimeoutException("Timeout during reading attempt"), source.Token)));
    }

    [Fact]
    public void TheHelper_AnswersTheSameAsTheSentence()
    {
        Assert.True(PostgresOpenTimeout.IsTimedOutOpen(new TimeoutException()));
        Assert.True(PostgresOpenTimeout.IsTimedOutOpen(new NpgsqlException("x", new TimeoutException())));
        Assert.False(PostgresOpenTimeout.IsTimedOutOpen(new OperationCanceledException()));
        Assert.False(PostgresOpenTimeout.IsTimedOutOpen(new TaskCanceledExceptionStandIn()));
        Assert.False(PostgresOpenTimeout.IsTimedOutOpen(new NpgsqlException("x", new PostgresException("down", "FATAL", "FATAL", "57P01"))));
        Assert.False(PostgresOpenTimeout.IsTimedOutOpen(new NpgsqlException("boom")));
    }

    private sealed class TaskCanceledExceptionStandIn : OperationCanceledException
    {
    }

    // ---- #5016 follow-up: the clock decides when the driver's own timer tore the socket down ----

    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    private static NpgsqlException AbortedRead() =>
        new("Exception while reading from stream",
            new System.IO.IOException("Unable to read data from the transport connection", new SocketException((int)SocketError.OperationAborted)));

    [Fact] // the shape rules alone do not see this one; that is the gap
    public void AnAbortedSocketRead_IsNotATimeoutByShape() => Assert.False(PostgresOpenTimeout.IsTimedOutOpen(AbortedRead()));

    [Fact]
    public void AnAbortedSocketRead_AtTheTimeout_IsATimedOutOpen()
    {
        Assert.True(PostgresOpenTimeout.IsTimedOutByClock(AbortedRead(), OneSecond, OneSecond, callerCancelled: false));
        Assert.True(PostgresOpenTimeout.IsTimedOutByClock(AbortedRead(), TimeSpan.FromMilliseconds(950), OneSecond, callerCancelled: false));
    }

    [Fact]
    public void AnAbortedSocketRead_WellBeforeTheTimeout_IsNotATimedOutOpen() =>
        Assert.False(PostgresOpenTimeout.IsTimedOutByClock(AbortedRead(), TimeSpan.FromMilliseconds(50), OneSecond, callerCancelled: false));

    [Fact]
    public void AServerReply_AtTheTimeout_IsNotATimedOutOpen() =>
        Assert.False(PostgresOpenTimeout.IsTimedOutByClock(
            new NpgsqlException("x", new PostgresException("down", "FATAL", "FATAL", "57P01")), OneSecond, OneSecond, callerCancelled: false));

    [Fact]
    public void ACallerCancellation_AtTheTimeout_IsNotATimedOutOpen()
    {
        Assert.False(PostgresOpenTimeout.IsTimedOutByClock(AbortedRead(), OneSecond, OneSecond, callerCancelled: true));
        Assert.False(PostgresOpenTimeout.IsTimedOutByClock(new NpgsqlException("x", new OperationCanceledException()), OneSecond, OneSecond, callerCancelled: true));
    }

    [Fact]
    public void ANestedCancellation_WithTheCallerLive_AtTheTimeout_IsTheDriversTimer_AndTimedOut()
    {
        Assert.True(PostgresOpenTimeout.IsTimedOutByClock(
            new NpgsqlException("Exception while reading from stream", new OperationCanceledException()), OneSecond, OneSecond, callerCancelled: false));
        Assert.True(PostgresOpenTimeout.IsTimedOutByClock(
            new NpgsqlException("x", new System.IO.IOException("io", new OperationCanceledException())), OneSecond, OneSecond, callerCancelled: false));
    }

    [Fact]
    public void AnOpenThatFailedAfterTheTimeout_AnswersInTime_AndOneThatDidNotKeepsCouldNotOpen()
    {
        var inner = AbortedRead();
        var late = DarlingWebEndpoints.FromRunException(new DarlingWebEndpoints.ComposeStoreOpenException(inner) { FailedAfterTimeout = true });
        Assert.Equal(InTime + inner.Message, late.Error);
        Assert.Equal(NotOpened + inner.Message, Answer(inner));
    }
}
