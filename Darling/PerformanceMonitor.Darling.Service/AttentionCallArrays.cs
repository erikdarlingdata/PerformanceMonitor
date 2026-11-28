/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The captured calls of one range, the rows of one <see cref="AttentionWindowSql.Calls"/> or <see cref="AttentionWindowSql.BaselineCalls"/>
/// statement, handed to the Client Timeouts reads (the MCP read and the alert baseline) as array parameters (#9881). A week on a busy server holds millions of under-floor samples and a
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

    /// <summary>Keeps the row <paramref name="reader"/> is on, one row of the calls statement, and returns the call's id. The statement
    /// itself is run by the caller that holds the store connection.</summary>
    public long Add(NpgsqlDataReader reader)
    {
        var id = reader.GetInt64(0);
        _ids.Add(id);
        _sessionIds.Add(reader.GetInt32(1));
        _databaseNames.Add(reader.IsDBNull(2) ? null : reader.GetString(2));
        _eventTimes.Add(DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Unspecified));
        _azdataMetadata.Add(reader.GetBoolean(4));
        _dbatoolsMetadata.Add(reader.GetBoolean(5));
        return id;
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

    /// <summary>Adds <paramref name="ids"/> as one <c>bigint[]</c> parameter after the ones <paramref name="command"/> already has: the
    /// matched call ids <see cref="AttentionWindowSql.MatchedCallRows"/> lists.</summary>
    public static void AddIds(NpgsqlCommand command, IReadOnlyCollection<long> ids) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = ids.ToArray() });
}
