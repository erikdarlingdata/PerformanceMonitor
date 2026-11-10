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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>An explicit data scope; an empty tag grant never means All servers.</summary>
public enum WebAccessGroupScope { Tags, AllServers }

/// <summary>Safe administrative/session metadata. No credential is returned.</summary>
public sealed record WebAccessGroup(
    Guid GroupId, string Name, WebAccessGroupScope Scope, bool Enabled, Guid TokenGeneration,
    long Revision, string ActivationState, string? RefusalCode, IReadOnlyList<int> TagIds);

/// <summary>Owner-only credential material for service activation. Never log this object or its value.</summary>
public sealed record WebAccessGroupCredential(WebAccessGroup Group, string SealedToken)
{
    public override string ToString() => "WebAccessGroupCredential { GroupId = " + Group.GroupId + " }";
}

/// <summary>
/// Reader-group persistence. Mutations call owner-defined functions so a Viewer admin can never directly
/// change the credential tables or their audit trail. PostgreSQL enforces validation and revision conflicts
/// even when the caller bypasses this adapter. No plaintext token is accepted by this layer.
/// </summary>
public sealed class WebAccessGroupStore
{
    private readonly NpgsqlDataSource _postgres;
    private readonly int _timeout;

    public WebAccessGroupStore(NpgsqlDataSource postgres, int commandTimeoutSeconds = 15)
    {
        _postgres = postgres;
        _timeout = commandTimeoutSeconds;
    }

    private const string SafeColumns = """
        g.group_id,g.name,g.scope_kind,g.enabled,g.token_generation,g.revision,g.activation_state,g.refusal_code,
        ARRAY(SELECT t.tag_id FROM config.web_access_group_tags t WHERE t.group_id=g.group_id ORDER BY t.tag_id)
        """;

    /// <summary>Safe metadata under one statement snapshot, suitable for the admin list and web authorization.</summary>
    public async Task<IReadOnlyList<WebAccessGroup>> ListAsync(CancellationToken ct = default)
    {
        await using var command = Command("SELECT " + SafeColumns + " FROM config.web_access_groups g ORDER BY g.name,g.group_id");
        var result = new List<WebAccessGroup>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) result.Add(Read(reader));
        return result;
    }

    /// <summary>Current identity, generation and grants in one snapshot; absence means revoked.</summary>
    public async Task<WebAccessGroup?> ReadAsync(Guid groupId, CancellationToken ct = default)
    {
        await using var command = Command("SELECT " + SafeColumns + " FROM config.web_access_groups g WHERE g.group_id=$1");
        command.Parameters.AddWithValue(groupId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Read(reader) : null;
    }

    public async Task<Guid?> ReadAdvancedSessionEpochAsync(CancellationToken ct = default)
    {
        await using var command = Command("SELECT session_epoch FROM config.web_access_advanced WHERE id=1 AND enabled");
        return await command.ExecuteScalarAsync(ct) as Guid?;
    }

    public async Task SetAdvancedEnabledAsync(bool enabled, CancellationToken ct = default)
    {
        await using var command = Command("SELECT config.set_web_access_advanced($1)");
        command.Parameters.AddWithValue(enabled);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task<(WebAccessGroup Group, Guid Epoch)?> ReadEnabledGroupAsync(Guid groupId, CancellationToken ct = default)
    {
        await using var command = Command("SELECT " + SafeColumns + ",a.session_epoch FROM config.web_access_groups g " +
            "JOIN config.web_access_advanced a ON a.id=1 AND a.enabled WHERE g.group_id=$1");
        command.Parameters.AddWithValue(groupId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (Read(reader), reader.GetGuid(9)) : null;
    }

    /// <summary>The service owner's activation read. All consumer roles are denied the secret column.</summary>
    public async Task<IReadOnlyList<WebAccessGroupCredential>> ReadCredentialsAsync(CancellationToken ct = default)
    {
        await using var command = Command("SELECT " + SafeColumns + ",g.sealed_token FROM config.web_access_groups g ORDER BY g.group_id");
        var result = new List<WebAccessGroupCredential>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) result.Add(new(Read(reader), reader.GetString(9)));
        return result;
    }

    public Task<long> CreateAsync(Guid id, string name, WebAccessGroupScope scope, int[] tagIds,
        Guid generation, string sealedToken, bool enabled, string actor, CancellationToken ct = default) =>
        ManageAsync("create", id, null, name, WireScope(scope), tagIds, generation, sealedToken, enabled, actor, ct);

    public Task<long> UpdateAsync(Guid id, long revision, WebAccessGroupScope scope, int[] tagIds,
        bool enabled, string actor, CancellationToken ct = default) =>
        ManageAsync("update", id, revision, null, WireScope(scope), tagIds, null, null, enabled, actor, ct);

    public Task<long> RotateAsync(Guid id, long revision, Guid generation, string sealedToken,
        string actor, CancellationToken ct = default) =>
        ManageAsync("rotate", id, revision, null, null, null, generation, sealedToken, null, actor, ct);

    public Task<long> DeleteAsync(Guid id, long revision, string actor, CancellationToken ct = default) =>
        ManageAsync("delete", id, revision, null, null, null, null, null, null, actor, ct);

    /// <summary>Owner only: activate pending credentials or refuse active ones. False means the state/revision changed.</summary>
    public async Task<bool> ActivateAsync(Guid id, long revision, Guid generation, string? refusalCode, CancellationToken ct = default)
    {
        await using var command = Command("SELECT config.activate_web_access_group($1,$2,$3,$4)");
        command.Parameters.AddWithValue(id);
        command.Parameters.AddWithValue(revision);
        command.Parameters.AddWithValue(generation);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)refusalCode ?? DBNull.Value);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private async Task<long> ManageAsync(string action, Guid id, long? revision, string? name, string? scope,
        int[]? tags, Guid? generation, string? sealedToken, bool? enabled, string actor, CancellationToken ct,
        NpgsqlTransaction? transaction = null)
    {
        const string sql = "SELECT config.manage_web_access_group($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)";
        await using var command = transaction is null ? Command(sql) : new NpgsqlCommand(sql, transaction.Connection, transaction)
        { CommandTimeout = _timeout };
        command.Parameters.AddWithValue(action);
        command.Parameters.AddWithValue(id);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, (object?)revision ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)name ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)scope ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, (object?)tags ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Uuid, (object?)generation ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)sealedToken ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Boolean, (object?)enabled ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, actor);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private NpgsqlCommand Command(string sql)
    {
        var command = _postgres.CreateCommand(sql);
        command.CommandTimeout = _timeout;
        return command;
    }

    private static string WireScope(WebAccessGroupScope scope) => scope switch
    {
        WebAccessGroupScope.Tags => "tags",
        WebAccessGroupScope.AllServers => "all_servers",
        _ => throw new ArgumentOutOfRangeException(nameof(scope)),
    };

    private static WebAccessGroup Read(NpgsqlDataReader r) => new(r.GetGuid(0),r.GetString(1),
        r.GetString(2) == "all_servers" ? WebAccessGroupScope.AllServers : WebAccessGroupScope.Tags,
        r.GetBoolean(3),r.GetGuid(4),r.GetInt64(5),r.GetString(6),
        r.IsDBNull(7) ? null : r.GetString(7),r.GetFieldValue<int[]>(8));
}
