/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net.Sockets;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The one predicate behind every "is this store fault worth another attempt" decision in this project:
/// a transport-level fault (socket reset, stream read/write failure, client-side deadline) rather than a
/// definitive answer from a working server.
///
/// <para><b>A <see cref="PostgresException"/> anywhere in the chain is never a transport fault.</b> It
/// means the backend received the statement and replied — a duplicate key, a missing relation, bad input
/// syntax, a full disk, or its own <c>statement_timeout</c> cancelling us at SQLSTATE 57014. Every one of
/// those returns the identical answer to an identical second attempt, so re-attempting buys nothing and
/// costs a second round trip against a store that just said no. The reply is also the evidence: a caller
/// that re-attempts past a SQLSTATE turns a legible, actionable error into a silent one.</para>
///
/// <para><b>Two callers, one predicate, deliberately.</b>
/// <see cref="DarlingCollectorRunner"/>'s store write re-attempts once, immediately, on a fresh
/// connection; <see cref="DarlingManagedPostgres"/>'s first-connection-after-start loop re-attempts six
/// times with a two-second pause. The POLICIES differ because the regimes do — one runs mid-sweep on a
/// live service holding a sweep permit, the other runs once during startup with nothing else in flight —
/// but the question each asks of the exception is the same question, and two copies of it would be free
/// to disagree the moment either grew an arm. What varies belongs at the call site; this does not.</para>
///
/// <para>The chain is walked rather than the outermost type tested, because Npgsql wraps: a client-side
/// command deadline arrives as an <see cref="NpgsqlException"/> whose inner exception is a
/// <see cref="TimeoutException"/>, and a lost connection as one wrapping an <see cref="IOException"/>.
/// Both render as the same "Exception while reading from stream" text, which is why the outermost type
/// and the message can distinguish neither from the other nor from a server reply.</para>
/// </summary>
internal static class PostgresTransportFault
{
    /// <summary>
    /// True when <paramref name="exception"/> is a transport-level fault. The <see cref="PostgresException"/>
    /// test comes FIRST at every level of the chain, so a server reply wrapped in transport machinery still
    /// answers false.
    /// </summary>
    internal static bool IsTransportFault(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException)
            {
                return false;
            }

            if (current is SocketException or IOException or TimeoutException)
            {
                return true;
            }
        }

        return exception is NpgsqlException;
    }
}
