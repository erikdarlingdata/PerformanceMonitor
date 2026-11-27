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
    /// Does nothing without settings or an owner key. When the revoke cannot be made (no owner connection yet, or the store write throws), it is
    /// queued in <paramref name="pending"/> for <see cref="DrainPendingAsync"/>, with one warning per owner. Cancellation of <paramref name="ct"/>
    /// propagates.
    /// </summary>
    internal static async Task OnOidcSignInAsync(PersonalTokenSettings? settings, StoreOwnerSource owner, DarlingPersonalTokenSnapshot snapshot,
        PersonalTokenPendingRevokes pending, string? ownerKey, WebOidcRole role, ILogger logger, CancellationToken ct)
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
                /* The seat is not lowered: nothing to revoke now, and a queued revoke from an earlier sign-in of this person (demoted during an
                   outage, restored since) is moot, so it must not revoke tokens made after the restore. */
                pending.Remove(ownerKey);
                return;
        }

        var revokedRows = 0;
        try
        {
            var source = owner.Read();
            if (source is null)
            {
                Queue(pending, ownerKey, operatorOnly, reason, "the store owner connection is not available yet", null, logger);
                return;
            }

            revokedRows = await PersonalTokenStore.RevokeForOwnerAsync(source, ownerKey, operatorOnly, SignInActor, reason, DateTimeOffset.UtcNow, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            if (PersonalTokenStore.IsMissingTable(ex))
            {
                logger.LogDebug("MCP personal tokens were not revoked at sign-in ({Reason}): the store has no personal token table.", reason);
            }
            else
            {
                Queue(pending, ownerKey, operatorOnly, reason, "the store write failed", ex, logger);
            }

            return;
        }

        if (revokedRows > 0)
        {
            logger.LogInformation("Revoked {Count} MCP personal token(s) at sign-in ({Reason}).", revokedRows, reason);
            await RefreshAfterRevokeAsync(owner, snapshot, logger, ct);
        }
    }

    /// <summary>The revoke has committed, so a failed refresh is not a reason to revoke again: it is logged, and the snapshot's own tick reloads
    /// within 5 seconds. True when the refresh loaded the rows, so the snapshot now shows the revoke.</summary>
    private static async Task<bool> RefreshAfterRevokeAsync(StoreOwnerSource owner, DarlingPersonalTokenSnapshot snapshot, ILogger logger, CancellationToken ct)
    {
        try
        {
            return await snapshot.RefreshAsync(owner, logger, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "MCP personal tokens were revoked, but the snapshot refresh failed; the next snapshot tick picks the revoke up.");
            return false;
        }
    }

    /// <summary>Remembers a revoke the sign-in could not make. The first entry for an owner is a warning; a later one for the same owner, and a
    /// drop on a full queue after the first, are debug lines, so a store outage does not log once per sign-in or per tick.</summary>
    private static void Queue(PersonalTokenPendingRevokes pending, string ownerKey, bool operatorOnly, string reason, string why, Exception? ex, ILogger logger)
    {
        switch (pending.Add(ownerKey, operatorOnly, reason))
        {
            case PersonalTokenPendingAdd.Queued:
                logger.LogWarning(ex, "MCP personal tokens were not revoked at sign-in ({Reason}): {Why}. The revoke is queued and retried on the next supervisor tick; the sign-in outcome is unchanged.", reason, why);
                break;
            case PersonalTokenPendingAdd.DroppedFirst:
                logger.LogWarning("MCP personal token revoke queue is full ({Max} owners); a revoke at sign-in ({Reason}) was dropped, and so will the newest ones be until it drains.", PersonalTokenPendingRevokes.MaxOwners, reason);
                break;
            default:
                logger.LogDebug(ex, "MCP personal token revoke at sign-in ({Reason}) not made ({Why}); already queued or the queue is full.", reason, why);
                break;
        }
    }

    /// <summary>
    /// Makes the revokes a sign-in could not make: one store write per queued owner. An entry leaves the queue only after its revoke commits;
    /// a store failure stops the pass and leaves the rest for the next tick (a debug line, the pending case was already warned about). A store with no personal token table
    /// has no tokens to revoke, so its entries are dropped. An entry whose revoke changed no rows leaves at once. An entry whose revoke changed rows
    /// stays, and keeps refusing the owner's tokens, until the snapshot refresh after the pass succeeds, so the snapshot never lags a token the queue
    /// has already stopped refusing. A failed refresh leaves those entries for the next pass, where the same revoke changes nothing and they leave.
    /// Every call first retries a queue file save that failed. With the feature off (<paramref name="settings"/> null) nothing is drained and the queue is kept.
    /// </summary>
    internal static async Task DrainPendingAsync(PersonalTokenSettings? settings, StoreOwnerSource owner, DarlingPersonalTokenSnapshot snapshot,
        PersonalTokenPendingRevokes pending, ILogger logger, CancellationToken ct)
    {
        pending.SaveIfDirty();
        if (settings is null || pending.Count == 0)
        {
            return;
        }

        var source = owner.Read();
        if (source is null)
        {
            return;
        }

        var awaitingRefresh = new List<PersonalTokenPendingRevokes.Entry>();
        foreach (var entry in pending.Snapshot())
        {
            try
            {
                var revoked = await PersonalTokenStore.RevokeForOwnerAsync(
                    source, entry.OwnerKey, entry.OperatorOnly, SignInActor, entry.Reason, DateTimeOffset.UtcNow, ct);
                if (revoked > 0)
                {
                    awaitingRefresh.Add(entry);
                    logger.LogInformation("Revoked {Count} MCP personal token(s) from the queued sign-in revoke ({Reason}).", revoked, entry.Reason);
                }
                else
                {
                    pending.Complete(entry);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                if (PersonalTokenStore.IsMissingTable(ex))
                {
                    pending.Complete(entry);
                }
                else
                {
                    /* Stop the pass, on purpose, for any store failure other than the missing table. An outage would cost every further entry the
                       same connect timeout inside the supervisor loop, and a command failure that belongs to one entry (a lock timeout on that
                       person's rows, say) is treated like an outage: the rest wait for the next tick, and the loop pays at most one command
                       timeout per tick. No entry can fail forever: the owner key is a hash, the reason is a constant, and the journal payload has
                       the shape that creating the token already wrote. */
                    logger.LogDebug(ex, "A queued MCP personal token revoke ({Reason}) is still pending; the pass stops and the next supervisor tick retries.", entry.Reason);
                    break;
                }
            }
        }

        if (awaitingRefresh.Count > 0 && await RefreshAfterRevokeAsync(owner, snapshot, logger, ct))
        {
            foreach (var entry in awaitingRefresh)
            {
                pending.Complete(entry);
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
