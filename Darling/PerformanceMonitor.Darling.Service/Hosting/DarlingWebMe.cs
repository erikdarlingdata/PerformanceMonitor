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
    internal static JsonObject Describe(DarlingWebSeat seat)
    {
        var method = seat.GroupId is not null ? "managed_group"
            : seat.TokenName is not null ? "named_token"
            : seat.Subject is not null ? "oidc"
            : seat.CanEdit ? "shared_token"
            : "reads_only";
        var oidc = method == "oidc";
        return new JsonObject
        {
            ["method"] = method,
            ["seat"] = seat.CanEdit ? "admin" : "read_only",
            ["subject"] = oidc ? seat.Subject : null,
            ["display_name"] = oidc ? seat.DisplayName : null,
            ["granted_by"] = oidc ? seat.GrantedBy : null,
            ["token_name"] = method == "named_token" ? seat.TokenName : null,
            ["group_name"] = method == "managed_group" ? seat.TokenName : null,
            ["expires_at"] = seat.SessionExpiresAt?.UtcDateTime.ToString("O", CultureInfo.InvariantCulture),
        };
    }
}
