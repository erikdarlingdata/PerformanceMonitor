/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// What the web host does to a person's personal MCP tokens (#8737) when the OIDC sign-in lowers their seat, and the sweep at start that applies
/// lowered settings. Best-effort: a store that cannot be reached is logged as a warning, and the sign-in's outcome is never changed.
/// </summary>
internal static class DarlingPersonalTokenSignIn
{
    internal const string SignInActor = "sign-in";

    /// <summary>
    /// Revokes the person's personal tokens the seat no longer allows, then refreshes the snapshot so the MCP gate sees it before the sign-in
    /// answers: a refused sign-in loses every live token, a read-only seat and an admin under <c>adminRole: reader</c> lose the operator ones.
    /// Does nothing without settings or an owner key. A store failure is a warning; cancellation of <paramref name="ct"/> propagates.
    /// </summary>
    internal static async Task OnOidcSignInAsync(PersonalTokenSettings? settings, StoreOwnerSource owner, DarlingPersonalTokenSnapshot snapshot,
        string? ownerKey, WebOidcRole role, ILogger logger, CancellationToken ct)
    {
        if (settings is null || ownerKey is null)
        {
            return;
        }

        bool operatorOnly;
        string reason;
        switch (role)
        {
            case WebOidcRole.Denied:
                operatorOnly = false;
                reason = PersonalTokenStore.SignInRefusedReason;
                break;
            case WebOidcRole.Viewer:
                operatorOnly = true;
                reason = PersonalTokenStore.SeatLoweredReason;
                break;
            case WebOidcRole.Admin when !settings.AdminMayMintOperator:
                operatorOnly = true;
                reason = PersonalTokenStore.OperatorsDisabledReason;
                break;
            default:
                return;
        }

        try
        {
            var source = owner.Read();
            if (source is null)
            {
                logger.LogWarning("MCP personal tokens were not revoked at sign-in ({Reason}): the store owner connection is not available yet.", reason);
                return;
            }

            var revoked = await PersonalTokenStore.RevokeForOwnerAsync(source, ownerKey, operatorOnly, SignInActor, reason, DateTimeOffset.UtcNow, ct);
            if (revoked > 0)
            {
                logger.LogInformation("Revoked {Count} MCP personal token(s) at sign-in ({Reason}).", revoked, reason);
                await snapshot.RefreshAsync(owner, logger, ct);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            if (PersonalTokenStore.IsMissingTable(ex))
            {
                logger.LogDebug("MCP personal tokens were not revoked at sign-in ({Reason}): the store has no personal token table.", reason);
            }
            else
            {
                logger.LogWarning(ex, "MCP personal tokens could not be revoked at sign-in ({Reason}); the sign-in outcome is unchanged.", reason);
            }
        }
    }

    /// <summary>
    /// Applies the settings to the live personal tokens once: with <c>adminRole: reader</c> the operator ones are revoked, and any that outlives
    /// <c>maxLifetimeDays</c> is cut back to it (journal actor <c>config</c>). True when done or when there is nothing to do; false when it should
    /// be tried again (no owner connection yet, a store below the table's version, or a store failure).
    /// </summary>
    internal static async Task<bool> ApplySettingsAtStartAsync(PersonalTokenSettings? settings, StoreOwnerSource owner, DarlingPersonalTokenSnapshot snapshot,
        ILogger logger, CancellationToken ct)
    {
        if (settings is null)
        {
            return true;
        }

        var source = owner.Read();
        if (source is null)
        {
            return false;
        }

        try
        {
            var (revoked, cut) = await PersonalTokenStore.ApplySettingsAsync(
                source, settings.MaxLifetimeDays, settings.AdminMayMintOperator, DateTimeOffset.UtcNow, ct);
            if (revoked > 0 || cut > 0)
            {
                logger.LogInformation(
                    "MCP personal token settings applied at start: {Revoked} operator token(s) revoked, {Cut} expiry(ies) cut to {Days} days.",
                    revoked, cut, settings.MaxLifetimeDays);
                await snapshot.RefreshAsync(owner, logger, ct);
            }

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            if (PersonalTokenStore.IsMissingTable(ex))
            {
                logger.LogDebug("MCP personal token settings not applied yet: the store has no personal token table (it is below the version that adds it).");
            }
            else
            {
                logger.LogWarning(ex, "MCP personal token settings could not be applied at start; the next supervisor tick retries.");
            }

            return false;
        }
    }
}
