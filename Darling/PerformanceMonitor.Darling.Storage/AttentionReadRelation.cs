/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The relation text every read of <c>collect.attention_events</c> uses (#9024). Store version 277 renames a populated table to
/// <c>collect.attention_events_legacy</c> and puts an empty table in its place. <see cref="AttentionLegacyCopy"/> then copies the legacy rows
/// across and never deletes from the legacy table, which it drops when the copy ends. Until then the older rows exist only in the legacy
/// table, so a read of the plain table alone sees an empty week and the Client Timeouts baseline falls to zero.
///
/// <para>While the legacy table exists a read uses <see cref="DerivedRelation"/>: every legacy row, plus the rows of the plain table that are
/// LATER than the server's newest legacy row. Every row written after the swap is later than every legacy row of the same server, so each row
/// counts once at any copy progress (a plain <c>UNION ALL</c> would count each copied row twice). The text binds <c>$1</c> = server_id, which
/// every statement of <see cref="PerformanceMonitor.Common.AttentionWindowSql"/> already does, and the outer server_id and time predicates reach
/// both branches, so each branch uses its own <c>(server_id, collection_time)</c> index. A server with no legacy row has no bound, so all its
/// rows of the plain table count.</para>
///
/// <para>Writes, the copy, retention and table setup keep the plain table. They never use this class.</para>
/// </summary>
public static class AttentionReadRelation
{
    /// <summary>The relation the reads used before the swap, and still use once the legacy table is gone.</summary>
    public const string PlainRelation = "attention_events";

    private const string LegacyTable = "collect.attention_events_legacy";

    /* The existence check is one catalog lookup. */
    private const int LegacyCheckTimeoutSeconds = 15;

    private const string Columns =
        "attention_event_id, collection_time, server_id, event_time, event_kind, attention_count, database_name, client_app_name, "
        + "client_hostname, session_id, interrupt_duration_us, statement_fingerprint, statement_text";

    /// <summary>The derived table that stands in for the plain relation while the legacy table exists. Binds <c>$1</c> = server_id.</summary>
    public const string DerivedRelation =
        "(SELECT " + Columns + " FROM " + LegacyTable
        + " UNION ALL SELECT " + Columns + " FROM collect.attention_events"
        + " WHERE collection_time > COALESCE((SELECT MAX(l.collection_time) FROM " + LegacyTable + " AS l WHERE l.server_id = $1), '-infinity'::timestamp)"
        + ") AS attention_events";

    /// <summary>
    /// Runs <paramref name="read"/> with the relation text to read. A read that hits an undefined table (SQLSTATE 42P01) on the derived text
    /// runs once more with the plain relation: the copy dropped the legacy table between the check and the read, which happens once per store.
    /// </summary>
    public static async Task<T> ReadAsync<T>(
        NpgsqlConnection connection, Func<string, Task<T>> read, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(read);

        if (!await LegacyExistsAsync(connection, cancellationToken).ConfigureAwait(false))
        {
            return await read(PlainRelation).ConfigureAwait(false);
        }

        try
        {
            return await read(DerivedRelation).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return await read(PlainRelation).ConfigureAwait(false);
        }
    }

    private static async Task<bool> LegacyExistsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT to_regclass('" + LegacyTable + "') IS NOT NULL", connection) { CommandTimeout = LegacyCheckTimeoutSeconds };
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }
}
