/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net.Sockets;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The one test for "this store open ran out of time" (#5016), so every place that words an open failure asks
/// the same question. A timed-out open does not have one shape. Read against Npgsql 10.0.3, an open that ran
/// out of time throws one of these:
///
/// <list type="bullet">
/// <item>A startup or login read (SSL request, GSS request, the authentication exchange) whose timer fired:
/// <see cref="NpgsqlException"/> "Exception while reading from stream" over a <see cref="TimeoutException"/>
/// ("Timeout during reading attempt"), thrown from <c>NpgsqlReadBuffer</c>.</item>
/// <item>The same read timing out inside the GSS step, which wraps what it caught: <see cref="NpgsqlException"/>
/// "Exception while performing GSS encryption" over that read-timeout <see cref="NpgsqlException"/>, so the
/// <see cref="TimeoutException"/> sits TWO levels down.</item>
/// <item>A startup send whose timer fired: <see cref="NpgsqlException"/> "Exception while writing to stream" over a
/// <see cref="TimeoutException"/> ("Timeout during writing attempt").</item>
/// <item>The budget already spent when the next step began, which includes the retry the driver makes after a
/// failed first attempt (it retries without GSS, and the budget is shared): <see cref="NpgsqlException"/> "The
/// operation has timed out" over a bare <see cref="TimeoutException"/>. This is the shape a silent store
/// normally ends with.</item>
/// <item>A TCP connect or DNS lookup that did not finish: <see cref="NpgsqlException"/> "Failed to connect to
/// {endpoint}" (or "The operation has timed out" for DNS) over a <see cref="TimeoutException"/> ("Timeout during
/// connection attempt"). When the operating system gives up before the driver's timer, the inner is a
/// <see cref="SocketException"/> with <see cref="SocketError.TimedOut"/> instead.</item>
/// <item>A pool wait that ended: <see cref="NpgsqlException"/> "The connection pool has been exhausted, ..." over a
/// bare <see cref="TimeoutException"/>.</item>
/// <item>A wait for the data source's first-time setup that ended: a bare <see cref="TimeoutException"/>, not an
/// <see cref="NpgsqlException"/> at all (<c>NpgsqlDataSource.Bootstrap</c>).</item>
/// </list>
///
/// <para>Each of those carries a <see cref="TimeoutException"/> (or the operating system's timed-out socket
/// error) somewhere in its chain and no outer type or message says so, because the same
/// "Exception while reading from stream" text also covers a connection the server reset. So the chain is
/// walked, as <see cref="PostgresTransportFault"/> walks it, and the answer is the first thing that decides
/// it.</para>
///
/// <para>Three things answer false. A <see cref="PostgresException"/> is the backend answering (a refused
/// login is not a slow one). An <see cref="OperationCanceledException"/> is somebody cancelling, and the one
/// who can cancel an open is the caller; the driver turns its own timer's cancellation into a
/// <see cref="TimeoutException"/> before it reaches us, so a cancellation left over is never the open running
/// out of time. And every other failure: a refused connection, a reset by the server, a connection the server
/// closed (<see cref="System.IO.EndOfStreamException"/>), an authentication error, a name that does not
/// resolve.</para>
/// </summary>
internal static class PostgresOpenTimeout
{
    /// <summary>
    /// True when <paramref name="exception"/>, the exception a store open threw, means the open ran out of time.
    /// The chain is walked from the outside in, and the first <see cref="PostgresException"/> or
    /// <see cref="OperationCanceledException"/> answers false before any deeper level is read.
    /// </summary>
    internal static bool IsTimedOutOpen(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            switch (current)
            {
                case PostgresException:
                case OperationCanceledException:
                    return false;
                case TimeoutException:
                case SocketException { SocketErrorCode: SocketError.TimedOut }:
                    return true;
            }
        }

        return false;
    }
}
