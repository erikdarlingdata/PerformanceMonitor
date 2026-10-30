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
/// The connection (and transaction) a store method runs on (#5915). Standalone, it opens its own connection from the data
/// source, as the stores always have. Handed an ambient connection and transaction (a Level 2 agent change, which must
/// write inside the gate's transaction so the re-check, the change and its <c>action_done</c> row commit together), it uses
/// those and owns neither: <see cref="CommitAsync"/> and <see cref="RollbackAsync"/> do nothing, and disposing closes nothing.
/// </summary>
internal sealed class PgStoreScope : IAsyncDisposable
{
    private readonly NpgsqlConnection? _ownedConnection;
    private readonly NpgsqlTransaction? _ownedTransaction;

    private PgStoreScope(NpgsqlConnection connection, NpgsqlTransaction? transaction, NpgsqlConnection? ownedConnection,
        NpgsqlTransaction? ownedTransaction)
    {
        Connection = connection;
        Transaction = transaction;
        _ownedConnection = ownedConnection;
        _ownedTransaction = ownedTransaction;
    }

    internal NpgsqlConnection Connection { get; }

    /// <summary>The transaction commands run in: the ambient one, the one this scope began, or null for a plain connection.</summary>
    internal NpgsqlTransaction? Transaction { get; }

    /// <summary>Opens the scope: the ambient connection when one is given, else a new connection (with a transaction when <paramref name="transactional"/>).</summary>
    internal static async Task<PgStoreScope> OpenAsync(NpgsqlDataSource? dataSource, NpgsqlConnection? ambientConnection,
        NpgsqlTransaction? ambientTransaction, bool transactional, CancellationToken cancellationToken = default)
    {
        if (ambientConnection is not null)
            return new PgStoreScope(ambientConnection, ambientTransaction, null, null);

        var connection = await dataSource!.OpenConnectionAsync(cancellationToken);
        if (!transactional)
            return new PgStoreScope(connection, null, connection, null);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        return new PgStoreScope(connection, transaction, connection, transaction);
    }

    internal Task CommitAsync(CancellationToken cancellationToken) =>
        _ownedTransaction is null ? Task.CompletedTask : _ownedTransaction.CommitAsync(cancellationToken);

    internal Task RollbackAsync(CancellationToken cancellationToken) =>
        _ownedTransaction is null ? Task.CompletedTask : _ownedTransaction.RollbackAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        if (_ownedTransaction is not null)
            await _ownedTransaction.DisposeAsync();
        if (_ownedConnection is not null)
            await _ownedConnection.DisposeAsync();
    }
}
