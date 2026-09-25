/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #4277: pins every STORE session's <c>timezone</c> to UTC, client-side — defence in depth alongside the
/// managed runtime's own v9 <c>postgresql.conf</c> block (<c>DarlingManagedPostgres.BuildTimeZoneConfAppend</c>),
/// which only reaches a MANAGED store. A bring-your-own store keeps whatever zone its owner set, or whatever
/// initdb took from its host, and every timestamp column in both stores is <c>timestamp without time zone</c>
/// holding naive UTC. A naive-to-<c>timestamptz</c> comparison — a bare <c>now()</c> in the SQL text, or a
/// bound <c>DateTime</c> with <c>Kind=Utc</c> — is resolved by PostgreSQL at the SESSION's <c>TimeZone</c>, so a
/// non-UTC store session silently shifts those reads (measured on #4277: the same one-hour predicate returned
/// 66 rows under UTC and 343 under America/New_York — five hours of data under a one-hour window).
///
/// <para>Every STORE call site — the worker's own store connection, the MCP/web host connections, the CLI
/// verbs, the managed-runtime bootstrap/upgrade, the viewer, the store connection self-test — routes its
/// resolved connection string through this ONE helper, right before the connection or data source is created.
/// A MONITORED target's connection string never does: its own session zone is read on purpose (see
/// <c>PgTargetBaselineProvider.Clock.cs</c>, which resolves a target's local clock from its collected
/// <c>pg_server_config</c> snapshot, not from a live session), and pinning it there would make that resolver's
/// own read wrong.</para>
///
/// <para>Overwrites a caller-supplied <c>Timezone</c> rather than leaving it — unlike
/// <c>DarlingWorker.EnsureStoreSearchPath</c>'s set-if-absent for <c>SearchPath</c>, and unlike the viewer's
/// connect <c>Timeout</c>, where an operator's explicit value wins. The store's columns are naive UTC
/// regardless of what a caller's connection string says, so there is no reading of "the caller meant it";
/// a store connection string that names another zone is always a mistake this pin corrects.</para>
/// </summary>
public static class DarlingStoreConnection
{
    /// <summary>
    /// The pin itself: Npgsql's <c>Timezone</c> connection-string keyword, forced to <c>UTC</c> — by APPENDING
    /// it to the caller's own string, never by rewriting the string through a builder. A builder round trip
    /// writes an explicit empty value (<c>Password=''</c>, <c>Root Certificate=""</c>, ...) as a bare
    /// <c>Key=</c>, and its own reader treats that as "not set" — silently dropping the keyword and letting
    /// Npgsql fall back to a <c>PG*</c> environment variable or a default file where the caller's empty value
    /// meant "never fall back" (round-1 review on #4285's PR). Parsing once first keeps today's exception on a
    /// malformed string at this exact statement; the parsed builder is then discarded, so every OTHER keyword
    /// in the caller's string survives byte for byte. Npgsql keeps only the LAST value for a repeated keyword,
    /// so appending still overwrites any <c>Timezone</c>/<c>TimeZone</c> the caller's string already sets,
    /// wherever it appears and in whatever case — measured against trailing <c>;</c>, trailing spaces, and an
    /// odd-case existing key, all of which still parse to a single UTC <c>Timezone</c> after the append.
    /// </summary>
    public static string PinSessionTimeZoneUtc(string connectionString)
    {
        /* Parse once, so a malformed string fails here exactly as Npgsql's own constructor would — the
           discarded builder exists only for that validation, never for re-serializing the string. */
        _ = new NpgsqlConnectionStringBuilder(connectionString);

        return connectionString + ";Timezone=UTC";
    }
}
