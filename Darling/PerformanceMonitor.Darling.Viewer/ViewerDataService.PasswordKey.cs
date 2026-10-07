/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's read of the key the service publishes for sealing secrets (V165, #5366): the current public key and
/// the newest service state row. Read-only; the key tables are written by the store owner alone.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Reads the published password key and the newest service state row over one pooled connection. Either part is
    /// null when the store holds no such row; both are null on a store the service has not started against. A store
    /// below V165 has no tables, which surfaces as the store's own error, so callers check the schema version first.
    /// </summary>
    public async Task<(PublishedPasswordKey? Key, PasswordKeyServiceState? State)> GetPublishedPasswordKeyAsync(CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        var key = await PasswordKeyTables.ReadCurrentAsync(connection, ct);
        var state = await PasswordKeyTables.ReadNewestServiceStateAsync(connection, ct);
        return (key, state);
    }
}
