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
/// Wraps a DuckDBConnection with a lock (read or write) that is released when the connection is disposed.
/// Ensures operations hold the lock for their entire duration, preventing CHECKPOINT or compaction
/// from reorganizing the database file while the connection is active.
/// </summary>
public sealed class LockedConnection : IDisposable, IAsyncDisposable
{
    private readonly DuckDBConnection _connection;
    private readonly IDisposable _lock;
    private bool _disposed;

    public LockedConnection(DuckDBConnection connection, IDisposable @lock)
    {
        _connection = connection;
        _lock = @lock;
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
        _lock.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _connection.DisposeAsync();
        _lock.Dispose();
    }
}
