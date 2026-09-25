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
    /// <summary>The pin itself: Npgsql's <c>Timezone</c> connection-string keyword, forced to <c>UTC</c>.</summary>
    public static string PinSessionTimeZoneUtc(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Timezone = "UTC",
        };

        return builder.ConnectionString;
    }
}
