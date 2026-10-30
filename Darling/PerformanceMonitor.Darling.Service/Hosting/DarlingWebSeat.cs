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
/// <para><b>Seats.</b> The shared token (<see cref="Subject"/> null, edit — the pre-#2550 reach,
/// unchanged); an OIDC admin (named, edit); an OIDC viewer (named, read-only); and <see cref="ReadsOnly"/>
/// (<see cref="Subject"/> null, read-only), which the host stores for every request on a dashboard that
/// <c>web.allowTokenlessLoopback</c> serves with no token, and for a shared-token session when the token is under
/// the length floor.</para>
///
/// <para><b>How the role survives the round-trip.</b> The session cookie's subject slot (#2583) carries an
/// ENCODED seat — <c>o2:a:{ownerKey}:{subject}</c> / <c>o2:v:{ownerKey}:{subject}</c> (<c>o1:a:{subject}</c> /
/// <c>o1:v:{subject}</c> from an older build still reads, with no owner key) — because the role decided at sign-in has to
/// come back on every later request, and the cookie is the only thing that does. The prefix rides INSIDE the
/// HMAC-signed region, so a viewer cannot promote themselves by editing it. A subject-bearing cookie whose
/// slot decodes to neither shape is REFUSED (the session reads as invalid, the user re-authenticates):
/// serving an un-decodable identity would put an unnamed principal behind the write paths this exists to
/// attribute.</para>
/// </summary>
internal sealed record DarlingWebSeat(string? Subject, bool CanEdit, string? OwnerKey = null)
{
    /// <summary>The shared-token seat: full edit reach, exactly what the token granted before per-user identity
    /// existed.</summary>
    internal static readonly DarlingWebSeat SharedToken = new(null, true);

    /// <summary>The seat of a dashboard that serves reads only: <c>web.allowTokenlessLoopback</c> with no token, or a
    /// shared-token session whose token is under the length floor. Unnamed, so writes stamp nothing, and refused
    /// every change by <see cref="IsRequestAllowed"/>.</summary>
    internal static readonly DarlingWebSeat ReadsOnly = new(null, false);

    /// <summary>The <see cref="HttpContext.Items"/> key the auth middleware stores the resolved seat under.</summary>
    internal const string HttpContextItemKey = "darling.web.seat";

    /// <summary>The request header a front door sends with every request it forwards for a person whose seat cannot edit (#6840). The
    /// front door signs in to a store once, with the store's shared token, and shares that one session between its viewers and its
    /// editors, so the store would otherwise answer a viewer as the shared-token seat (the Admin server form's login name, the
    /// investigation export, the edit flags). See <see cref="ApplyFrontDoorRole"/>.</summary>
    internal const string FrontDoorRoleHeader = "X-Darling-Front-Door-Role";

    /// <summary>The response header a store sets when it applied <see cref="FrontDoorRoleHeader"/>. The front door refuses a viewer's
    /// answer that lacks it, so a store on a build without this support never answers a viewer as the shared-token seat.</summary>
    internal const string FrontDoorRoleAppliedHeader = "X-Darling-Front-Door-Role-Applied";

    /// <summary>The one value <see cref="FrontDoorRoleHeader"/> carries.</summary>
    internal const string FrontDoorViewerRole = "viewer";

    /// <summary>
    /// The seat a request runs as once <see cref="FrontDoorRoleHeader"/> is read (#6840), decided per request (a front door shares one
    /// session between viewers and editors). With the header set to <see cref="FrontDoorViewerRole"/>, a seat that can edit becomes
    /// <see cref="ReadsOnly"/> and the request is marked with <see cref="FrontDoorRoleAppliedHeader"/>. Every store handler then answers
    /// exactly what it answers a reads-only caller, with no list of routes to keep. The header can only lower a seat: it names
    /// nobody, a seat that cannot edit stays as it is, and any other value, or no header, changes nothing. Whoever sends it can
    /// already use the session it rides on with that session's full reach.
    /// </summary>
    internal static DarlingWebSeat ApplyFrontDoorRole(HttpContext context, DarlingWebSeat seat)
    {
        if (!string.Equals(context.Request.Headers[FrontDoorRoleHeader].ToString(), FrontDoorViewerRole, StringComparison.Ordinal))
        {
            return seat;
        }

        context.Response.Headers[FrontDoorRoleAppliedHeader] = FrontDoorViewerRole;
        return seat.CanEdit ? ReadsOnly : seat;
    }

    /// <summary>Seat-encoding prefixes inside the session cookie's subject slot. Versioned (<c>o1</c>, <c>o2</c>) so a
    /// future shape can coexist with cookies already in browsers instead of signing everyone out. <c>o1</c> carries no
    /// owner key and is only ever read; sign-in writes <c>o2</c> (#6214).</summary>
    private const string AdminPrefix = "o1:a:";
    private const string ViewerPrefix = "o1:v:";
    private const string AdminPrefixV2 = "o2:a:";
    private const string ViewerPrefixV2 = "o2:v:";

    /// <summary>The length of an owner key: base64url (no padding) of a SHA-256 hash is 43 characters. It has no <c>:</c>.</summary>
    internal const int OwnerKeyLength = 43;

    /// <summary>The longest raw subject that still fits the cookie's <c>MaxSessionSubjectLength</c> once the
    /// seat prefix, the owner key and its separator are prepended. Enforced at sign-in with a REFUSAL, mirroring the cookie's own rule.</summary>
    internal const int MaxSubjectLength = Mcp.DarlingWebHostService.MaxSessionSubjectLength - 5 - OwnerKeyLength - 1;

    /// <summary>
    /// The stable owner key of a signed-in person (#6214): base64url(SHA-256(issuer + "\n" + sub)) of the validated ID token. The display
    /// subject can change and an identity provider can hand the same one to somebody else, so it never decides who owns a personal view;
    /// the issuer and the provider's own <c>sub</c> do not change and are not reused. Null when either is missing.
    /// </summary>
    internal static string? ComputeOwnerKey(string? issuer, string? sub)
    {
        if (string.IsNullOrEmpty(issuer) || string.IsNullOrEmpty(sub))
        {
            return null;
        }

        var hash = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(issuer + "\n" + sub));
        return Convert.ToBase64String(hash).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    /// <summary>What <c>updated_by</c> gets stamped with: the person when there is one, else the honest
    /// <c>web</c> constant the shared-token seat has always written.</summary>
    internal string EditorPrincipal => Subject ?? DarlingWebEndpoints.WebEditorPrincipal;

    /// <summary>The request's resolved seat. Every pipeline the host builds stores one before routing (the auth gate,
    /// or the reads-only gate of a tokenless dashboard); an absent item (routes mapped without the
    /// host's pipeline, as some tests map them) reads as <see cref="ReadsOnly"/>, so a request nobody sat a seat for cannot write.</summary>
    internal static DarlingWebSeat FromContext(HttpContext context)
        => context.Items.TryGetValue(HttpContextItemKey, out var value) && value is DarlingWebSeat seat
            ? seat
            : ReadsOnly;

    /// <summary>Whether this request is a signed-in session whose seat may edit (#6236): the auth middleware stored a seat (it does so
    /// only for a request that holds a valid session) and that seat is the shared token's or an OIDC admin's. A dashboard with no
    /// sign-in configured registers no middleware, so nothing is stored and this is false, unlike <see cref="FromContext"/>, whose
    /// default is the reads-only seat.</summary>
    internal static bool IsSignedInAdmin(HttpContext context)
        => context.Items.TryGetValue(HttpContextItemKey, out var value) && value is DarlingWebSeat { CanEdit: true };

    /// <summary>Encodes an OIDC seat for the session cookie's subject slot. With an owner key it writes the <c>o2</c> shape
    /// (<c>o2:a:{ownerKey}:{subject}</c>); without one the <c>o1</c> shape an older build wrote.</summary>
    internal static string EncodeCookieSubject(string subject, WebOidcRole role, string? ownerKey = null)
    {
        if (role is not (WebOidcRole.Admin or WebOidcRole.Viewer))
        {
            throw new ArgumentOutOfRangeException(nameof(role), role, "A Denied sign-in never mints a cookie.");
        }

        var admin = role == WebOidcRole.Admin;
        return ownerKey is null
            ? (admin ? AdminPrefix : ViewerPrefix) + subject
            : (admin ? AdminPrefixV2 : ViewerPrefixV2) + ownerKey + ":" + subject;
    }

    /// <summary>
    /// Resolves a validated session cookie's subject slot to a seat. Null (the two-segment shared-token
    /// cookie) is the shared-token seat; a recognized <c>o1:</c> or <c>o2:</c> encoding is that OIDC seat (<c>o1</c> with no owner key); anything else is
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

        foreach (var (prefix, canEdit) in new[] { (AdminPrefixV2, true), (ViewerPrefixV2, false) })
        {
            if (!cookieSubject.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            /* o2:{a|v}:{ownerKey}:{subject}. The key is exactly OwnerKeyLength base64url characters, then ':' and a non-empty subject. */
            var rest = cookieSubject.AsSpan(prefix.Length);
            if (rest.Length > OwnerKeyLength + 1 && rest[OwnerKeyLength] == ':' && IsBase64Url(rest.Slice(0, OwnerKeyLength)))
            {
                seat = new DarlingWebSeat(rest.Slice(OwnerKeyLength + 1).ToString(), canEdit, rest.Slice(0, OwnerKeyLength).ToString());
                return true;
            }

            break;
        }

        seat = SharedToken;
        return false;
    }

    private static bool IsBase64Url(ReadOnlySpan<char> text)
    {
        foreach (var c in text)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c == '-' || c == '_'))
            {
                return false;
            }
        }

        return true;
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
