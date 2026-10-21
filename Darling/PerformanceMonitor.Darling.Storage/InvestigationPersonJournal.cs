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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The journal row a person's change writes (#6274): a profile saved or removed, a route's agent level, limits, scope or match changed.
/// The row is dated by the STORE's clock (<c>now()</c>), never the caller's: a desktop with a skewed clock must not write a backdated or
/// out-of-order audit row. The Viewer and the web share this one statement, so a change journals the same way from either.
/// </summary>
public static class InvestigationPersonJournal
{
    /// <summary>One journal row dated by the store. $1 request, $2 route, $3 server, $4 event, $5 actor, $6 reason, $7 detail (JSON).</summary>
    public const string InsertSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail)
VALUES (now(), $1, $2, $3, $4, $5, $6, $7::jsonb)";

    /// <summary>Writes the row inside the caller's transaction, so the change and its trace commit or fail together.</summary>
    public static async Task InsertAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, Guid? requestId, int? routeId, int? serverId, string journalEvent,
        string actor, string? reason, string? detailJson, int timeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(InsertSql, connection, transaction) { CommandTimeout = timeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid });
        command.Parameters.Add(new NpgsqlParameter<int?> { TypedValue = routeId, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = journalEvent });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = actor });
        command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = reason, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = detailJson, NpgsqlDbType = NpgsqlDbType.Text });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
