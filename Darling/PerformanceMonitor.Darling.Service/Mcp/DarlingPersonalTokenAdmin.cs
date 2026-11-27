/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The one place a personal MCP token is made, revoked and shown as a row (#8737, #9643). The web routes (<c>/api/mcp-tokens</c>) and the MCP
/// tools (<c>mint_personal_token</c>, <c>revoke_personal_token</c>, <c>get_personal_tokens</c>) both call it, so the name rule, the store write, the
/// log line and the snapshot refresh cannot drift apart. The token value is returned to the caller and appears in no log line and no row.
/// </summary>
internal static class DarlingPersonalTokenAdmin
{
    /// <summary>What a mint made: the store's outcome, and for <see cref="PersonalTokenCreateOutcome.Created"/> the value (shown once) and the row.</summary>
    internal sealed record MintResult(PersonalTokenCreateOutcome Outcome, string? Token, PersonalTokenRow? Record);

    /// <summary>Whether a token name is allowed: the token-name rule, and not one of the built-in stamps.</summary>
    internal static bool IsAcceptableName(string? name) => DarlingMcpTokenSet.IsValidName(name) && !DarlingWebSeat.IsBuiltInStamp(name);

    /// <summary>
    /// Makes a token for <paramref name="ownerKey"/>. The caller has checked the name, the lifetime and the role. A created token is logged
    /// (never its value), then the snapshot is refreshed before this returns, so the MCP gate holds it before the caller sees the value.
    /// <paramref name="refreshToken"/> is the host's stopping token: the row has committed by then, so a caller that goes away cannot skip the refresh.
    /// </summary>
    internal static async Task<MintResult> MintAsync(NpgsqlDataSource owner, StoreOwnerSource ownerSource, DarlingPersonalTokenSnapshot snapshot,
        PersonalTokenSettings settings, string ownerKey, string subject, string name, string role, int lifetimeDays, string actor,
        TimeProvider clock, ILogger logger, CancellationToken ct, CancellationToken refreshToken)
    {
        var minted = PersonalTokenFormat.Mint();
        var now = clock.GetUtcNow();
        var outcome = await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, ownerKey, subject, name, role,
            now, lifetimeDays, settings.MaxTokensPerPerson, actor, ct);
        if (outcome != PersonalTokenCreateOutcome.Created)
        {
            return new MintResult(outcome, null, null);
        }

        logger.LogInformation("Personal MCP token {TokenName} ({TokenId}) was created for {Subject} with role {Role}", name, minted.TokenId, subject, role);
        await snapshot.RefreshAsync(ownerSource, logger, refreshToken);
        var record = new PersonalTokenRow(minted.TokenId, ownerKey, subject, name, role, now, now.AddDays(lifetimeDays), null, null, null, null);
        return new MintResult(PersonalTokenCreateOutcome.Created, minted.Value, record);
    }

    /// <summary>
    /// Revokes one live token (see <see cref="PersonalTokenStore.RevokeAsync"/> for the owner arguments). True when it was revoked: it is logged and
    /// the snapshot refreshed before this returns, so the gate refuses the token now. False when no live token has that id.
    /// </summary>
    internal static async Task<bool> RevokeAsync(NpgsqlDataSource owner, StoreOwnerSource ownerSource, DarlingPersonalTokenSnapshot snapshot,
        string tokenId, string? requiredOwnerKey, string? callerOwnerKey, string ownerRevokedBy, string adminRevokedBy, string actor,
        string logSubject, TimeProvider clock, ILogger logger, CancellationToken ct, CancellationToken refreshToken)
    {
        var revoked = await PersonalTokenStore.RevokeAsync(owner, tokenId, requiredOwnerKey, callerOwnerKey, ownerRevokedBy, adminRevokedBy, actor,
            clock.GetUtcNow(), ct);
        if (!revoked)
        {
            return false;
        }

        logger.LogInformation("Personal MCP token {TokenId} was revoked by {Subject}", tokenId, logSubject);
        await snapshot.RefreshAsync(ownerSource, logger, refreshToken);
        return true;
    }

    /// <summary>One token as the page and the MCP read show it: never the value and never its hash. <c>subject</c> only when asked for.</summary>
    internal static JsonObject RowJson(PersonalTokenRow row, DateTimeOffset now, bool withSubject)
    {
        var json = new JsonObject
        {
            ["id"] = row.TokenId,
            ["name"] = row.TokenName,
            ["role"] = row.Role,
            ["created_at"] = Iso(row.CreatedAt),
            ["expires_at"] = Iso(row.ExpiresAt),
            ["last_used_at"] = row.LastUsedAt is { } used ? Iso(used) : null,
            ["state"] = row.RevokedAt is not null ? "revoked" : row.ExpiresAt <= now ? "expired" : "live",
            ["revoked_by"] = row.RevokedBy,
            ["revoke_reason"] = row.RevokeReason,
        };
        if (withSubject)
        {
            json["subject"] = row.Subject;
        }

        return json;
    }

    private static string Iso(DateTimeOffset value) => value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
}
