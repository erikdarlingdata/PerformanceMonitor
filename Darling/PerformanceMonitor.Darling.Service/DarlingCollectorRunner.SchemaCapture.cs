/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>#7643: what the schema pass (<see cref="DarlingSchemaCapture"/>) takes from the runner: its store, its logger and its test seams.</summary>
public sealed partial class DarlingCollectorRunner
{
    private ISchemaCaptureSource? _schemaSource;

    /// <summary>The store the runner writes to.</summary>
    internal NpgsqlDataSource Store => _postgres;

    internal ILogger? Logger => _logger;

    /// <summary>Where the schema pass opens its connections to a monitored server. Replaced by a test; the SQL Server one otherwise.</summary>
    internal ISchemaCaptureSource SchemaSource
    {
        get => _schemaSource ??= new SqlSchemaCaptureSource(this);
        set => _schemaSource = value;
    }

    /// <summary>The pass budget and the per-database budget of the schema pass, replaced by a test. Null in production: five minutes and 120 seconds.</summary>
    internal (TimeSpan Pass, TimeSpan Database)? SchemaBudgetsForTests { get; set; }

    /// <summary>
    /// The servers whose disabled collector was last found to leave no session to drop (#7643): the store showed none, or the drop ran. The
    /// disable path asks the store once per server and then not again, until a pass runs for it and forgets the entry.
    /// </summary>
    private readonly ConcurrentDictionary<int, byte> _schemaSessionSettled = new();

    internal bool SchemaSessionSettled(int serverId) => _schemaSessionSettled.ContainsKey(serverId);

    internal void SettleSchemaSession(int serverId) => _schemaSessionSettled[serverId] = 0;

    internal void UnsettleSchemaSession(int serverId) => _schemaSessionSettled.TryRemove(serverId, out _);

    /// <summary>
    /// When the schema pass last started each database's work, by server and database name (#7643). The pass orders its databases by
    /// this and the stored "seen in scope" stamp, oldest first, so a database whose read fails every pass, which no pass stamps, still
    /// goes to the back once it was tried. Kept for the process only: after a restart the stored stamps alone order the databases.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database), DateTime> _schemaAttempts = new();

    internal DateTime? SchemaAttemptedAt(int serverId, string database) =>
        _schemaAttempts.TryGetValue((serverId, database), out var at) ? at : null;

    internal void NoteSchemaAttempt(int serverId, string database, DateTime at) => _schemaAttempts[(serverId, database)] = at;

    /// <summary>Writes the pass rows of one run with the same writer every collector's rows go through.</summary>
    internal Task<int> WriteSchemaPassRowsAsync(
        NpgsqlConnection pgConnection, List<SchemaObjectsCollector.PassRow> rows, ServerRuntime server, DateTime collectionTime, CancellationToken cancellationToken)
    {
        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = collectionTime,
            Deltas = _deltas,
            Target = server.Target,
        };

        return WriteBatchAsync(pgConnection, SchemaObjectsCollector.Instance, rows, server, collectionTime, context, cancellationToken);
    }
}
