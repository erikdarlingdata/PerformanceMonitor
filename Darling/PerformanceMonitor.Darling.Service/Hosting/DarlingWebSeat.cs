/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.AspNetCore.Http;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// WHO is making this request and WHAT they may do — the one seam between the auth middleware (which resolves
/// it once per request) and everything downstream (#2550). Downstream consumers read it from
/// <see cref="HttpContext.Items"/> via <see cref="FromContext"/>, deliberately NOT from per-endpoint
/// attributes: an endpoint added tomorrow — by anyone, on any branch — is covered by the middleware's
/// group-level write gate and inherits correct attribution without knowing this type exists.
///
/// <para><b>Three seats.</b> The shared token (<see cref="Subject"/> null, edit — the pre-#2550 reach,
/// unchanged); an OIDC admin (named, edit); an OIDC viewer (named, read-only). A loopback-only dashboard
/// registers no auth middleware at all, so <see cref="FromContext"/>'s absent-item default IS the shared-token
/// seat — the tokenless local operator keeps today's full reach.</para>
///
/// <para><b>How the role survives the round-trip.</b> The session cookie's subject slot (#2583) carries an
/// ENCODED seat — <c>o1:a:{subject}</c> / <c>o1:v:{subject}</c> — because the role decided at sign-in has to
/// come back on every later request, and the cookie is the only thing that does. The prefix rides INSIDE the
/// HMAC-signed region, so a viewer cannot promote themselves by editing it. A subject-bearing cookie whose
/// slot decodes to neither shape is REFUSED (the session reads as invalid, the user re-authenticates):
/// serving an un-decodable identity would put an unnamed principal behind the write paths this exists to
/// attribute.</para>
/// </summary>
internal sealed record DarlingWebSeat(string? Subject, bool CanEdit)
{
    /// <summary>The shared-token seat, and the default for a pipeline with no auth middleware (loopback-only
    /// mode). Full edit reach — exactly what the token granted before per-user identity existed.</summary>
    internal static readonly DarlingWebSeat SharedToken = new(null, true);

    /// <summary>The <see cref="HttpContext.Items"/> key the auth middleware stores the resolved seat under.</summary>
    internal const string HttpContextItemKey = "darling.web.seat";

    /// <summary>Seat-encoding prefixes inside the session cookie's subject slot. Versioned (<c>o1</c>) so a
    /// future shape can coexist with cookies already in browsers instead of signing everyone out.</summary>
    private const string AdminPrefix = "o1:a:";
    private const string ViewerPrefix = "o1:v:";

    /// <summary>The longest raw subject that still fits the cookie's <c>MaxSessionSubjectLength</c> once the
    /// seat prefix is prepended. Enforced at sign-in with a REFUSAL, mirroring the cookie's own rule.</summary>
    internal const int MaxSubjectLength = Mcp.DarlingWebHostService.MaxSessionSubjectLength - 5;

    /// <summary>What <c>updated_by</c> gets stamped with: the person when there is one, else the honest
    /// <c>web</c> constant the shared-token seat has always written.</summary>
    internal string EditorPrincipal => Subject ?? DarlingWebEndpoints.WebEditorPrincipal;

    /// <summary>The request's resolved seat. Absent item = <see cref="SharedToken"/>: the only pipeline that
    /// never stores one is loopback-only mode, where the local operator has today's full reach by design.</summary>
    internal static DarlingWebSeat FromContext(HttpContext context)
        => context.Items.TryGetValue(HttpContextItemKey, out var value) && value is DarlingWebSeat seat
            ? seat
            : SharedToken;

    /// <summary>Encodes an OIDC seat for the session cookie's subject slot.</summary>
    internal static string EncodeCookieSubject(string subject, WebOidcRole role) => role switch
    {
        WebOidcRole.Admin => AdminPrefix + subject,
        WebOidcRole.Viewer => ViewerPrefix + subject,
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, "A Denied sign-in never mints a cookie."),
    };

    /// <summary>
    /// Resolves a validated session cookie's subject slot to a seat. Null (the two-segment shared-token
    /// cookie) is the shared-token seat; a recognized <c>o1:</c> encoding is that OIDC seat; anything else is
    /// FALSE — treat the session as invalid and re-authenticate. Only sign-in ever writes this slot, so an
    /// unrecognized value is a version we do not speak, not a user to guess about.
    /// </summary>
    internal static bool TryResolveSeat(string? cookieSubject, out DarlingWebSeat seat)
    {
        if (cookieSubject is null)
        {
            seat = SharedToken;
            return true;
        }

        if (cookieSubject.StartsWith(AdminPrefix, StringComparison.Ordinal) && cookieSubject.Length > AdminPrefix.Length)
        {
            seat = new DarlingWebSeat(cookieSubject.Substring(AdminPrefix.Length), CanEdit: true);
            return true;
        }

        if (cookieSubject.StartsWith(ViewerPrefix, StringComparison.Ordinal) && cookieSubject.Length > ViewerPrefix.Length)
        {
            seat = new DarlingWebSeat(cookieSubject.Substring(ViewerPrefix.Length), CanEdit: false);
            return true;
        }

        seat = SharedToken;
        return false;
    }

    /// <summary>
    /// The group-level write gate: may THIS seat make THIS request? Pure over (seat, method, path) so the
    /// matrix pins without a server. An edit seat may do anything. A read-only seat gets every safe method,
    /// plus exactly one unsafe-method exception: <c>POST /api/compose/run</c>, which is read-only by
    /// construction (it compiles a panel spec into bound SELECTs on the viewer-role pool — the SPA runs every
    /// custom-view panel through it, so blocking it would break the read surface the viewer seat exists to
    /// grant). Everything else non-GET is refused — INCLUDING write endpoints that do not exist yet, which is
    /// the point of gating on the method instead of enumerating routes.
    /// </summary>
    internal static bool IsRequestAllowed(DarlingWebSeat seat, string method, string path)
    {
        if (seat.CanEdit)
        {
            return true;
        }

        if (HttpMethods.IsGet(method) || HttpMethods.IsHead(method) || HttpMethods.IsOptions(method))
        {
            return true;
        }

        return HttpMethods.IsPost(method) && string.Equals(path, "/api/compose/run", StringComparison.OrdinalIgnoreCase);
    }
}
