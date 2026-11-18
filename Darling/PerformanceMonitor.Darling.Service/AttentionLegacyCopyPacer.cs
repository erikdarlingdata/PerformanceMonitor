/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8967: gives the attention legacy copy the retention purge's WAL pacer (#4823), so the copy writes no more WAL a second than the purge
/// does. The copy lives in the Storage project, which cannot see <see cref="RetentionWalPacer"/>.
/// </summary>
internal sealed class AttentionLegacyCopyPacer : IBatchWalPacer
{
    private readonly RetentionWalPacer _pacer;

    internal AttentionLegacyCopyPacer(RetentionWalPacer pacer) => _pacer = pacer;

    public Task<long?> ReadWalPositionAsync(NpgsqlConnection connection, CancellationToken cancellationToken) =>
        _pacer.ReadWalPositionAsync(connection, cancellationToken);

    public async Task AfterBatchAsync(NpgsqlConnection connection, long? before, CancellationToken cancellationToken) =>
        await _pacer.AfterBatchAsync(await _pacer.WalWrittenSinceAsync(connection, before, cancellationToken), cancellationToken);
}
