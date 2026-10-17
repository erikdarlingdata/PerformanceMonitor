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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The generic webhook signing key columns (#5998, V193). They are written by their own statements, never by the Settings
/// upsert or the route update, which leave them alone. A key is sealed under the signing slot for the row, the settings row's
/// generic proxy and the stored generic URL text; the write is guarded on that URL and proxy still being what was read, so a
/// key can not be stored for a destination that changed under it. Reading needs the admin seat: the read roles are denied these
/// columns, so a read-only seat sees no key block.
/// </summary>
public sealed partial class ViewerDataService
{
    private const string SettingsSigningStateSql =
        "SELECT signing_key, previous_signing_key, previous_signing_key_until, generic_url, generic_proxy FROM config_notification WHERE id = 1";

    private const string RouteSigningStateSql =
        "SELECT r.signing_key, r.previous_signing_key, r.previous_signing_key_until, r.generic_url, " +
        "COALESCE((SELECT n.generic_proxy FROM config_notification n WHERE n.id = 1), '') " +
        "FROM config_notification_routes r WHERE r.route_id = $1";

    /// <summary>Makes or replaces the settings row's key: the current key moves to the previous slot with a window of
    /// <see cref="WebhookSigning.RotationOverlapDays"/> days when there was one. $1 sealed key, $2 stored URL, $3 proxy, $4 days.</summary>
    public const string SettingsSigningKeyWriteSql = @"
UPDATE config_notification SET
    previous_signing_key = signing_key,
    previous_signing_key_until = CASE WHEN signing_key <> '' THEN now() + make_interval(days => $4) END,
    signing_key = $1
WHERE id = 1 AND generic_url = $2 AND generic_proxy = $3";

    /// <summary>The route form of <see cref="SettingsSigningKeyWriteSql"/>; $5 is the route id, $3 the settings row's generic proxy.</summary>
    public const string RouteSigningKeyWriteSql = @"
UPDATE config_notification_routes SET
    previous_signing_key = signing_key,
    previous_signing_key_until = CASE WHEN signing_key <> '' THEN now() + make_interval(days => $4) END,
    signing_key = $1
WHERE route_id = $5 AND generic_url = $2
  AND COALESCE((SELECT n.generic_proxy FROM config_notification n WHERE n.id = 1), '') = $3";

    public const string SettingsSigningKeyClearSql =
        "UPDATE config_notification SET signing_key = '', previous_signing_key = '', previous_signing_key_until = NULL WHERE id = 1";

    public const string RouteSigningKeyClearSql =
        "UPDATE config_notification_routes SET signing_key = '', previous_signing_key = '', previous_signing_key_until = NULL WHERE route_id = $1";

    /// <summary>The settings row's key columns and the URL and proxy they are bound to, or null when a read-only seat can not read
    /// them or the row does not exist yet.</summary>
    public async Task<ViewerSigningKeyState?> GetSettingsSigningKeyStateAsync(CancellationToken cancellationToken = default)
    {
        if (IsReadOnly)
        {
            return null;
        }

        await using var command = _dataSource.CreateCommand(SettingsSigningStateSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        return await ReadSigningStateAsync(command, cancellationToken);
    }

    /// <summary>A route's key columns and the URL and proxy they are bound to, or null (read-only seat, or no such route).</summary>
    public async Task<ViewerSigningKeyState?> GetRouteSigningKeyStateAsync(int routeId, CancellationToken cancellationToken = default)
    {
        if (IsReadOnly)
        {
            return null;
        }

        await using var command = _dataSource.CreateCommand(RouteSigningStateSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        return await ReadSigningStateAsync(command, cancellationToken);
    }

    private static async Task<ViewerSigningKeyState?> ReadSigningStateAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ViewerSigningKeyState(
            reader.GetString(0), reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTime>(2).ToUniversalTime(),
            reader.GetString(3), reader.GetString(4));
    }

    /// <summary>The ids of the routes that hold a signing key. A read-only seat can not read the column, so it reports none.</summary>
    public async Task<IReadOnlyList<int>> GetRouteIdsHoldingSigningKeysAsync(CancellationToken cancellationToken = default)
    {
        var ids = new List<int>();
        if (IsReadOnly)
        {
            return ids;
        }

        await using var command = _dataSource.CreateCommand("SELECT route_id FROM config_notification_routes WHERE signing_key <> '' ORDER BY route_id");
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids;
    }

    /// <summary>
    /// Makes the settings row's key, or rotates it when one is stored, and returns the new key to show once. Throws
    /// <see cref="ViewerPasswordRefusedException"/> with a sentence when the URL is not saved or changed meanwhile.
    /// </summary>
    public async Task<string> GenerateSettingsSigningKeyAsync(ViewerPasswordSealer sealer, CancellationToken cancellationToken = default)
    {
        var state = await GetSettingsSigningKeyStateAsync(cancellationToken)
            ?? throw new ViewerPasswordRefusedException(ViewerWebhookSigningKeys.SaveUrlFirstText);
        var (plain, sealedKey) = ViewerWebhookSigningKeys.MakeKey(PasswordBinding.WebhookSettingsRow, state, sealer);
        await using var command = _dataSource.CreateCommand(SettingsSigningKeyWriteSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddSigningWriteParameters(command, sealedKey, state);
        if (await ExecuteWriteAsync(command, cancellationToken) == 0)
        {
            throw new ViewerPasswordRefusedException(ViewerWebhookSigningKeys.ChangedMeanwhileText);
        }

        return plain;
    }

    /// <summary>The route form of <see cref="GenerateSettingsSigningKeyAsync"/>.</summary>
    public async Task<string> GenerateRouteSigningKeyAsync(int routeId, ViewerPasswordSealer sealer, CancellationToken cancellationToken = default)
    {
        var state = await GetRouteSigningKeyStateAsync(routeId, cancellationToken)
            ?? throw new ViewerPasswordRefusedException(ViewerWebhookSigningKeys.SaveUrlFirstText);
        var (plain, sealedKey) = ViewerWebhookSigningKeys.MakeKey(PasswordBinding.WebhookRouteRow(routeId), state, sealer);
        await using var command = _dataSource.CreateCommand(RouteSigningKeyWriteSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddSigningWriteParameters(command, sealedKey, state);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });   // $5
        if (await ExecuteWriteAsync(command, cancellationToken) == 0)
        {
            throw new ViewerPasswordRefusedException(ViewerWebhookSigningKeys.ChangedMeanwhileText);
        }

        return plain;
    }

    private static void AddSigningWriteParameters(NpgsqlCommand command, string sealedKey, ViewerSigningKeyState state)
    {
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = sealedKey });                         // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = state.StoredUrl });                   // $2
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = state.Proxy });                       // $3
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = WebhookSigning.RotationOverlapDays });   // $4
    }

    /// <summary>Removes the settings row's key and its previous key.</summary>
    public async Task RemoveSettingsSigningKeyAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(SettingsSigningKeyClearSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Removes a route's key and its previous key.</summary>
    public async Task RemoveRouteSigningKeyAsync(int routeId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(RouteSigningKeyClearSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        await ExecuteWriteAsync(command, cancellationToken);
    }
}
