/* Copyright (c) 2026 Erik Darling, Darling Data LLC
 * Licensed under the MIT License. See LICENSE in the project root. */
using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

public sealed partial class ViewerDataService
{
    public async Task<(string Description, string Address)> GetWebAccessHandoverAsync(WebAccessGroup group, CancellationToken ct)
    {
        await RequireWebGroupAdminAsync(ct);
        var json = await ExportCommandAsync("read", group, null, null, null, ct);
        if (json is null) return ("", "");
        using var document = JsonDocument.Parse(json);
        return (document.RootElement.GetProperty("description").GetString() ?? "",
            document.RootElement.GetProperty("address").GetString() ?? "");
    }

    public async Task SaveWebAccessHandoverAsync(WebAccessGroup group, string description, string address, CancellationToken ct)
    {
        await RequireWebGroupAdminAsync(ct);
        ValidateWebAccessAddress(address);
        await ExportCommandAsync("metadata", group, null, description, address, ct);
    }

    public async Task UpdateWebAccessGroupWithHandoverAsync(WebAccessGroup group, WebAccessGroupScope scope,
        int[] tags, bool enabled, string description, string address, CancellationToken ct)
    {
        ValidateWebAccessAddress(address);
        await RequireWebGroupAdminAsync(ct);
        await WebGroupWriteAsync(() => WebGroups.UpdateWithHandoverAsync(group, scope, tags.Distinct().ToArray(),
            enabled, description, address, "viewer", ct));
    }

    internal static void ValidateWebAccessAddress(string address)
    {
        if (address.Length > 0 && (!Uri.TryCreate(address, UriKind.Absolute, out var uri) ||
            (uri.Scheme != "http" && uri.Scheme != "https") || !string.IsNullOrEmpty(uri.UserInfo)))
            throw new InvalidOperationException("Enter an HTTP or HTTPS dashboard address without credentials.");
    }

    public async Task<string> ExportWebAccessTokenAsync(WebAccessGroup group, CancellationToken ct)
    {
        await RequireWebGroupAdminAsync(ct);
        var envelope = await ExportCommandAsync("export", group, null, null, null, ct);
        if (envelope is null)
            throw new InvalidOperationException("This token predates recoverable storage. Rotate it once to enable repeated export.");
        var parts = envelope.Split('.');
        var key = Convert.FromBase64String(parts[0]);
        var plain = new byte[Convert.FromBase64String(parts[2]).Length];
        try
        {
            using var aes = new AesGcm(key, 16);
            aes.Decrypt(Convert.FromBase64String(parts[1]), Convert.FromBase64String(parts[2]),
                Convert.FromBase64String(parts[3]), plain, Encoding.UTF8.GetBytes(group.GroupId + ":" + group.TokenGeneration));
            return Encoding.UTF8.GetString(plain);
        }
        finally { CryptographicOperations.ZeroMemory(key); CryptographicOperations.ZeroMemory(plain); }
    }

    private static string CreateWebAccessRecovery(ViewerWebToken token)
    {
        var key = RandomNumberGenerator.GetBytes(32);
        var nonce = RandomNumberGenerator.GetBytes(12);
        var plain = Encoding.UTF8.GetBytes(token.Plain);
        var cipher = new byte[plain.Length];
        var tag = new byte[16];
        try
        {
            using var aes = new AesGcm(key, 16);
            aes.Encrypt(nonce, plain, cipher, tag, Encoding.UTF8.GetBytes(token.GroupId + ":" + token.Generation));
            var envelope = string.Join('.', Convert.ToBase64String(key), Convert.ToBase64String(nonce),
                Convert.ToBase64String(cipher), Convert.ToBase64String(tag));
            return envelope;
        }
        finally { CryptographicOperations.ZeroMemory(key); CryptographicOperations.ZeroMemory(plain); }
    }

    private async Task<long> PersistRecoverableTokenAsync(ViewerWebToken token, string sealedToken,
        WebAccessGroup? previous, string? name, WebAccessGroupScope scope, int[] tags, CancellationToken ct,
        (string Description, string Address)? handover = null)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var command = new NpgsqlCommand("SELECT config.manage_web_access_group_recoverable($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", connection, transaction);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.AddWithValue(previous is null ? "create" : "rotate");
        command.Parameters.AddWithValue(token.GroupId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, (object?)previous?.Revision ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)name ?? DBNull.Value);
        command.Parameters.AddWithValue(scope == WebAccessGroupScope.AllServers ? "all_servers" : "tags");
        command.Parameters.AddWithValue(tags);
        command.Parameters.AddWithValue(token.Generation);
        command.Parameters.AddWithValue(sealedToken);
        command.Parameters.AddWithValue(true);
        command.Parameters.AddWithValue(CreateWebAccessRecovery(token));
        var revision = (long)(await command.ExecuteScalarAsync(ct))!;
        if (handover is { } details)
            await ExportCommandAsync("metadata", token.GroupId, token.Generation, null, details.Description, details.Address, ct, transaction);
        await transaction.CommitAsync(ct);
        return revision;
    }

    private Task<string?> ExportCommandAsync(string action, WebAccessGroup group, string? envelope,
        string? description, string? address, CancellationToken ct) =>
        ExportCommandAsync(action, group.GroupId, group.TokenGeneration, envelope, description, address, ct);

    private async Task<string?> ExportCommandAsync(string action, Guid groupId, Guid generation, string? envelope,
        string? description, string? address, CancellationToken ct, NpgsqlTransaction? transaction = null)
    {
        const string sql = "SELECT config.web_access_export($1,$2,$3,$4,$5,$6)";
        await using var command = transaction is null ? _dataSource.CreateCommand(sql) : new NpgsqlCommand(sql, transaction.Connection, transaction);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.AddWithValue(action);
        command.Parameters.AddWithValue(groupId);
        command.Parameters.AddWithValue(generation);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)envelope ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)description ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)address ?? DBNull.Value);
        return await command.ExecuteScalarAsync(ct) as string;
    }
}
