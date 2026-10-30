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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's access to <c>config.config_threshold_overrides</c> (#5916 part 4), over the statements the
/// <c>set_alert_threshold_override</c> tool and the web route use (<see cref="ThresholdOverrideStore"/>). A write on the read-only
/// <c>viewer</c> role degrades to <see cref="ViewerReadOnlyException"/> as every viewer write does. The table is on the
/// <c>config_version</c> beacon, so the running service re-reads it on its next reload.
/// </summary>
public sealed partial class ViewerDataService : IThresholdOverrideGateway
{
    public async Task<IReadOnlyList<ThresholdOverrideRow>> ListThresholdOverridesAsync(CancellationToken cancellationToken = default) =>
        await ThresholdOverrideStore.ListAsync(
            _dataSource, k => DarlingThresholdKeys.TryGet(k, out var entry) ? entry.Column : null, cancellationToken);

    public async Task SaveThresholdOverrideAsync(
        int serverId, string? database, string key, int value, string setBy, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await ThresholdOverrideStore.UpsertAsync(connection, transaction, serverId, database, key, value, setBy, null, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    public async Task SaveThresholdOverrideForTagAsync(
        int tagId, string key, int value, string setBy, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await ThresholdOverrideStore.UpsertTagAsync(connection, transaction, tagId, key, value, setBy, null, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    public async Task RemoveThresholdOverrideForTagAsync(int tagId, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await ThresholdOverrideStore.DeleteTagAsync(connection, transaction, tagId, key, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    public async Task RemoveThresholdOverrideAsync(
        int serverId, string? database, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await ThresholdOverrideStore.DeleteAsync(connection, transaction, serverId, database, key, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }
}
