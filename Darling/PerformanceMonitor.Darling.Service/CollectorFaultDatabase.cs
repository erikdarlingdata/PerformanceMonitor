/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Carries the database a per-database collection fault happened in, ON the exception, so a fault handler
/// upstream can name it (#2997).
///
/// <para><b>Why this is needed at all.</b> A <c>RunsPerDatabase</c> collector does not use the runtime's
/// own connection: the per-database loop opens one connection per database. When EVERY database fails the
/// loop rethrows only the first failure, bare, and the handler's only other source of a database name is
/// <c>ServerRuntime.ConnectedDatabase</c> - an <c>init</c>-only field stamped once during the initial
/// connect-and-probe, from whatever database the probe landed on. For a per-database collector that is
/// simply a different database from the one that failed, so a message built on it names the wrong one with
/// full confidence. Seven collectors declare <c>RunsPerDatabase =&gt; true</c>, <c>pg_index_bloat</c>
/// among them.</para>
///
/// <para><b>Why <see cref="Exception.Data"/> and not a wrapper exception.</b> Fault classification keys on
/// the exception's TYPE and SQLSTATE - <c>PostgresTargetProvider.Classify</c> asks whether it is a
/// <c>PostgresException</c>, an <c>NpgsqlException</c>, or wraps a <c>TimeoutException</c>. Wrapping the
/// failure would change the type every one of those checks sees and silently re-route the fault, which is
/// a far larger change than adding a name to a message. <c>Data</c> rides along and alters nothing.</para>
///
/// <para>Reading is total: an unstamped exception, or one whose payload is not a string, falls back to
/// whatever the caller had before. So a non-per-database collector behaves exactly as it always did.</para>
/// </summary>
internal static class CollectorFaultDatabase
{
    /// <summary>The <see cref="Exception.Data"/> key. Named, so a test asserts the same identity the
    /// producer uses rather than retyping the string and proving only its own transcription.</summary>
    internal const string DataKey = "PerformanceMonitor.FaultedDatabase";

    /// <summary>
    /// Records <paramref name="databaseName"/> on <paramref name="exception"/>. Best-effort: an exception
    /// type can override <see cref="Exception.Data"/> to be read-only or fixed-size, and losing a name from
    /// a diagnostic message must never turn into a second fault on the failure path.
    /// </summary>
    internal static void Stamp(Exception? exception, string? databaseName)
    {
        if (exception is null || string.IsNullOrWhiteSpace(databaseName))
        {
            return;
        }

        try
        {
            exception.Data[DataKey] = databaseName;
        }
        catch
        {
            /* Intentionally empty - see the summary. A read-only or fixed-size Data bag costs the message
               its database name and nothing else; throwing here would replace a classified fault with an
               unclassified one, which is the opposite of what this exists to do. */
        }
    }

    /// <summary>
    /// The stamped database, or <paramref name="fallback"/> when the exception carries none - which is the
    /// normal case for every collector that does not fan out per database.
    /// </summary>
    internal static string? For(Exception? exception, string? fallback)
    {
        try
        {
            if (exception?.Data[DataKey] is string stamped && !string.IsNullOrWhiteSpace(stamped))
            {
                return stamped;
            }
        }
        catch
        {
            /* Same reasoning as Stamp: fall back rather than fault while reporting a fault. */
        }

        return fallback;
    }
}
