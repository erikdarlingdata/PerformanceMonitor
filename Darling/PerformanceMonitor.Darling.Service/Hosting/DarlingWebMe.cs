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

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>#8737: the answer of <c>GET /api/me</c>, which describes only the calling session: how it signed in, its seat, who it is and when
/// it ends. The identity fields are display-only (nothing authorizes on them), and <c>/api/session</c> still carries no name.</summary>
internal static class DarlingWebMe
{
    /// <param name="seat">The calling session's seat.</param>
    /// <param name="oidcRoleMapping">Whether the OIDC sign-in configured on this host has a role mapping; null when none is configured.
    /// Reported as <c>role_mapping</c> for an OIDC session only: a null <c>granted_by</c> alone cannot say "no mapping", because a sign-in
    /// without an owner key carries no grant.</param>
    /// <param name="personalTokensServed">Whether this host serves personal MCP tokens: a store with the feature on, never a front door. The page
    /// shows its token list only when this is true and the session is an OIDC one or an administrator's.</param>
    internal static JsonObject Describe(DarlingWebSeat seat, bool? oidcRoleMapping, bool personalTokensServed)
    {
        var method = seat.GroupId is not null ? "managed_group"
            : seat.TokenName is not null ? "named_token"
            : seat.Subject is not null ? "oidc"
            : seat.CanEdit || seat.SessionExpiresAt is not null ? "shared_token"
            : "reads_only";
        var oidc = method == "oidc";
        return new JsonObject
        {
            ["method"] = method,
            ["seat"] = seat.CanEdit ? "admin" : "read_only",
            ["subject"] = oidc ? seat.Subject : null,
            ["display_name"] = oidc ? seat.DisplayName : null,
            ["granted_by"] = oidc ? seat.GrantedBy : null,
            ["role_mapping"] = oidc ? oidcRoleMapping : null,
            ["token_name"] = method == "named_token" ? seat.TokenName : null,
            ["group_name"] = method == "managed_group" ? seat.TokenName : null,
            ["expires_at"] = seat.SessionExpiresAt?.UtcDateTime.ToString("O", CultureInfo.InvariantCulture),
            ["personal_tokens"] = personalTokensServed && (oidc || seat.CanEdit),
        };
    }
}
