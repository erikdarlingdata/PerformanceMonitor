/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Wraps a DuckDBConnection with a read lock that is released when the connection is disposed.
/// Ensures UI reads hold the lock for their entire duration, preventing CHECKPOINT or compaction
/// from reorganizing the database file while a reader has stale file offsets.
/// </summary>
public sealed class LockedConnection : IDisposable, IAsyncDisposable
{
    private readonly DuckDBConnection _connection;
    private readonly IDisposable _readLock;
    private bool _disposed;

    public LockedConnection(DuckDBConnection connection, IDisposable readLock)
    {
        _connection = connection;
        _readLock = readLock;
    }

    /// <summary>
    /// Creates a command on the underlying connection.
    /// This is the only method callers need — all 50 call sites use CreateCommand() exclusively.
    /// </summary>
    public DuckDBCommand CreateCommand() => _connection.CreateCommand();

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
        _readLock.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _connection.DisposeAsync();
        _readLock.Dispose();
    }
}
