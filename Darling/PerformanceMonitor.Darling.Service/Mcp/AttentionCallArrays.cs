/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The captured calls of one range, read once with <see cref="AttentionWindowSql.Calls"/> or <see cref="AttentionWindowSql.BaselineCalls"/>
/// and handed to the Client Timeouts reads as array parameters (#9881). A week on a busy server holds millions of under-floor samples and a
/// few dozen calls; bound as arrays, the calls reach the planner with their true count, so it hashes them and passes over the samples in
/// parallel workers. One array per column of the calls statement, in its column order.
/// </summary>
internal sealed class AttentionCallArrays
{
    private readonly List<long> _ids = [];
    private readonly List<int> _sessionIds = [];
    private readonly List<string?> _databaseNames = [];
    private readonly List<DateTime> _eventTimes = [];
    private readonly List<bool> _azdataMetadata = [];
    private readonly List<bool> _dbatoolsMetadata = [];

    /// <summary>How many calls the range holds.</summary>
    public int Count => _ids.Count;

    /// <summary>Runs <paramref name="callsSql"/> with <c>$1</c> = <paramref name="serverId"/>, <c>$2</c> = <paramref name="lower"/> and
    /// <c>$3</c> = <paramref name="upper"/> (naive UTC, bound as <c>timestamp</c>) and keeps its rows.</summary>
    public static async Task<AttentionCallArrays> ReadAsync(
        NpgsqlConnection connection, string callsSql, int serverId, DateTime lower, DateTime upper, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(callsSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(lower, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(upper, DateTimeKind.Unspecified));

        var calls = new AttentionCallArrays();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            calls._ids.Add(reader.GetInt64(0));
            calls._sessionIds.Add(reader.GetInt32(1));
            calls._databaseNames.Add(reader.IsDBNull(2) ? null : reader.GetString(2));
            calls._eventTimes.Add(DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Unspecified));
            calls._azdataMetadata.Add(reader.GetBoolean(4));
            calls._dbatoolsMetadata.Add(reader.GetBoolean(5));
        }

        return calls;
    }

    /// <summary>Adds the <see cref="AttentionWindowSql.CallArrayCount"/> array parameters after the ones <paramref name="command"/> already
    /// has, so the statement's <c>callsAt</c> is that count plus one.</summary>
    public void AddTo(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = _ids.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = _sessionIds.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = _databaseNames.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = _eventTimes.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Boolean, Value = _azdataMetadata.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Boolean, Value = _dbatoolsMetadata.ToArray() });
    }
}
