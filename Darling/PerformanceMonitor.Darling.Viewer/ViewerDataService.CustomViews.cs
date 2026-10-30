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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's Custom Views manager (#6390 S5): list, create, edit, rename, move and delete over <c>config.custom_views</c>, the same
/// rows the web dashboard's Views pages write. Every statement is <see cref="CustomViewStore"/>'s (Storage), and every definition passes
/// <see cref="CustomViewValidation.ValidateDefinition(string?)"/> first, so a view the viewer saves reads back identically on the web and
/// a definition the web refuses gets the same sentence here. There are no personal views in the viewer: a write is shared, stamped with
/// the Windows user name, and the list shows shared views only.
///
/// <para><b>The write gate is the login's own privilege on the table</b>, not <see cref="IsReadOnly"/>: a locked-down
/// <c>viewer</c> seat holds the narrow INSERT/UPDATE/DELETE grant on <c>custom_views</c> that the config tables do not give it
/// (<see cref="GetCustomViewWriteAccessAsync"/>).</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The first store schema version that has the folder, personal and created_by columns every custom view read selects (V204).</summary>
    public const int CustomViewsStoreSchemaVersion = 204;

    /// <summary>What the manager says on a store older than <see cref="CustomViewsStoreSchemaVersion"/>.</summary>
    public const string CustomViewsNeedNewerStoreMessage =
        "Custom views need a newer store (schema v204 or later). Update or restart the Darling service so it migrates the store, then reopen the viewer.";

    /// <summary>The login's own INSERT, UPDATE and DELETE privileges on the views table, in one round trip. The bare name resolves through the
    /// store's search_path to <c>config.custom_views</c>, as the other probes here do.</summary>
    public const string CustomViewsPrivilegeProbeSql =
        "SELECT has_table_privilege('custom_views', 'INSERT'), has_table_privilege('custom_views', 'UPDATE'), has_table_privilege('custom_views', 'DELETE')";

    /// <summary>True when a store probed at <paramref name="probedVersion"/> has the V204 columns. An unanswered probe (null) fails open, as
    /// every gate on the schema probe does.</summary>
    public static bool CustomViewsAvailable(int? probedVersion) =>
        probedVersion is not int version || version >= CustomViewsStoreSchemaVersion;

    /// <summary>Whether this store has the V204 custom view columns, from the probed schema version (cached per instance).</summary>
    public async Task<bool> CustomViewsSupportedAsync(CancellationToken cancellationToken = default)
    {
        var storeVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        return CustomViewsAvailable(storeVersion);
    }

    /// <summary>The name a write stamps into <c>updated_by</c> (and <c>created_by</c> on a create): the Windows user running the viewer.</summary>
    public static string CustomViewWriter => Environment.UserName;

    /// <summary>What the store login may do to the views table. A probe that cannot run grants nothing.</summary>
    public readonly record struct CustomViewWriteAccess(bool CanInsert, bool CanUpdate, bool CanDelete)
    {
        public bool CanWrite => CanInsert && CanUpdate && CanDelete;

        /// <summary>The sentence for the manager's banner, or null when every write is allowed.</summary>
        public string? Banner
        {
            get
            {
                var missing = new List<string>();
                if (!CanInsert)
                {
                    missing.Add("INSERT");
                }

                if (!CanUpdate)
                {
                    missing.Add("UPDATE");
                }

                if (!CanDelete)
                {
                    missing.Add("DELETE");
                }

                if (missing.Count == 0)
                {
                    return null;
                }

                return $"This store login has no {string.Join(" or ", missing)} privilege on the custom_views table, so custom views can be viewed but not changed here.";
            }
        }
    }

    /// <summary>Probes the store login's INSERT, UPDATE and DELETE privileges on the views table. A server answer that cannot be read (the
    /// table missing, a quirk) grants nothing, so the buttons stay off rather than dead-click; an unreachable store throws.</summary>
    public async Task<CustomViewWriteAccess> GetCustomViewWriteAccessAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(CustomViewsPrivilegeProbeSql);
            command.CommandTimeout = ViewerCommandDeadlines.ConnectGateSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                return new CustomViewWriteAccess(reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2));
            }
        }
        catch (Exception ex) when (IsConnectionFailure(ex))
        {
            throw new ViewerStoreUnreachableException(ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ViewerLogger.Warn("CustomViews", $"The custom view privilege probe failed, so writes are off: {ex.Message}");
        }

        return new CustomViewWriteAccess(false, false, false);
    }

    private CustomViewStore NewCustomViewStore() => new(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);

    /// <summary>Every shared view, name order (no definition bodies).</summary>
    public async Task<IReadOnlyList<CustomViewSummary>> GetCustomViewsAsync(CancellationToken cancellationToken = default) =>
        await CustomViewSkewAsync(() => NewCustomViewStore().ListAsync(cancellationToken: cancellationToken));

    /// <summary>One view in full, or <see cref="CustomViewResult.NotFound"/>.</summary>
    public async Task<CustomViewResult> GetCustomViewAsync(long id, CancellationToken cancellationToken = default) =>
        await CustomViewSkewAsync(() => NewCustomViewStore().GetAsync(id, cancellationToken: cancellationToken));

    /// <summary>The refusal text for a definition, or null when it is valid: the one validator the web uses, with the read-parameter table in Storage.</summary>
    public static string? ValidateCustomViewDefinition(string? definitionJson)
    {
        var validation = CustomViewValidation.ValidateDefinition(definitionJson is null ? null : CustomViewsEditorModel.CompactDefinition(definitionJson));
        return validation.IsValid ? null : validation.Error;
    }

    /// <summary>Creates a shared view at version 1. A definition the validator refuses comes back as <see cref="CustomViewResult.Invalid"/> with the
    /// web's sentence; nothing is written.</summary>
    public async Task<CustomViewResult> CreateCustomViewAsync(
        string name, string? description, string definitionJson, string? folder, CancellationToken cancellationToken = default)
    {
        if (ValidateCustomViewDefinition(definitionJson) is string refusal)
        {
            return new CustomViewResult.Invalid(refusal);
        }

        /* The web sends the definition compact (JsonNode.ToJsonString), so the size cap counts the same bytes here. */
        definitionJson = CustomViewsEditorModel.CompactDefinition(definitionJson);

        return await CustomViewSkewAsync(() => NewCustomViewStore().CreateAsync(
            name?.Trim() ?? string.Empty, description, definitionJson, CustomViewWriter, folder, personal: false, ownerKey: null, cancellationToken));
    }

    /// <summary>Saves a view under the version the caller loaded. A stale version comes back as <see cref="CustomViewResult.Conflict"/> with the
    /// store's own sentence; a refused definition as <see cref="CustomViewResult.Invalid"/>. The view stays shared.</summary>
    public async Task<CustomViewResult> UpdateCustomViewAsync(
        long id, string name, string? description, string definitionJson, int expectedVersion, string? folder, CancellationToken cancellationToken = default)
    {
        if (ValidateCustomViewDefinition(definitionJson) is string refusal)
        {
            return new CustomViewResult.Invalid(refusal);
        }

        /* The web sends the definition compact (JsonNode.ToJsonString), so the size cap counts the same bytes here. */
        definitionJson = CustomViewsEditorModel.CompactDefinition(definitionJson);

        return await CustomViewSkewAsync(() => NewCustomViewStore().UpdateAsync(
            id, name?.Trim() ?? string.Empty, description, definitionJson, expectedVersion, CustomViewWriter, folder, personal: false, actorKey: null, cancellationToken));
    }

    /// <summary>Deletes a shared view. A personal view somebody owns is Forbidden (the viewer has no owner key), as the web answers a non-owner.</summary>
    public async Task<CustomViewResult> DeleteCustomViewAsync(long id, CancellationToken cancellationToken = default) =>
        await CustomViewSkewAsync(() => NewCustomViewStore().DeleteAsync(id, actorKey: null, anyPersonal: false, cancellationToken));

    /// <summary>A store that lacks the table or a column reads as the house schema-skew message.</summary>
    private static async Task<T> CustomViewSkewAsync<T>(Func<Task<T>> call)
    {
        try
        {
            return await call();
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }
}
