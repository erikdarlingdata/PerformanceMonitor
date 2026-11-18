/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
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
/// unchanged); an OIDC admin (named, edit); an OIDC viewer (named, read-only); a named web token (#7735: named by its
/// entry, <see cref="TokenName"/> set, read-only or edit as the entry's role says, decided from the live set on every request); a scoped
/// reader (a named reader whose entry carries <c>allowedTagIds</c>: <see cref="AllowedTagIds"/> set from the live set on every request, the
/// scope never in the cookie, and every server read held to the servers under those tags); and
/// <see cref="ReadsOnly"/> (<see cref="Subject"/> null, read-only), which the host stores for every request on a dashboard that
/// <c>web.allowTokenlessLoopback</c> serves with no token, and for a shared-token session when the token is under
/// the length floor.</para>
///
/// <para><b>How the role survives the round-trip.</b> The session cookie's subject slot (#2583) carries an
/// ENCODED seat — <c>o3:a:{ownerKey}:{b64u(displayName)}:{b64u(grantedBy)}:{subject}</c> / <c>o3:v:...</c> (an empty name or grant
/// segment is null; the base64url segments hold no <c>:</c>, so the subject, last, may). <c>o2:{a|v}:{ownerKey}:{subject}</c> and
/// <c>o1:a:{subject}</c> / <c>o1:v:{subject}</c> from older builds still read, with no display name or grant (and, for <c>o1</c>,
/// no owner key). The display name and the granting role value are display-only: nothing authorizes on them. Because the role decided at sign-in has to
/// come back on every later request, and the cookie is the only thing that does. The prefix rides INSIDE the
/// HMAC-signed region, so a viewer cannot promote themselves by editing it. A subject-bearing cookie whose
/// slot decodes to neither shape is REFUSED (the session reads as invalid, the user re-authenticates):
/// serving an un-decodable identity would put an unnamed principal behind the write paths this exists to
/// attribute.</para>
///
/// <para>A named web token's cookie (<c>t1:{name}</c>) carries the NAME only, no role: the auth middleware looks the name up in the
/// token set the start resolved and sits the seat with that entry's current role, so a token removed from the config ends its
/// sessions and a role change applies to them. <see cref="TryResolveSeat"/> hands such a cookie back read-only with
/// <see cref="TokenName"/> set, and the middleware's lookup decides the rest.</para>
/// </summary>
/// <param name="AllowedTagIds">The scoped reader's server tag ids, or null for every other seat. The tag tree and the tag map are read
/// on each request, so a tag change applies to the next request without a new sign-in.</param>
/// <param name="SessionExpiresAt">When the session cookie that carried this request ends (its signed expiry, whole seconds), or null for a request
/// admitted with no cookie.</param>
internal sealed record DarlingWebSeat(string? Subject, bool CanEdit, string? OwnerKey = null, string? TokenName = null, IReadOnlyList<int>? AllowedTagIds = null, Guid? GroupId = null, Guid? GroupGeneration = null, string? DisplayName = null, string? GrantedBy = null, DateTimeOffset? SessionExpiresAt = null)
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

    /// <summary>Seat-encoding prefixes inside the session cookie's subject slot. Versioned (<c>o1</c>, <c>o2</c>, <c>o3</c>) so a
    /// future shape can coexist with cookies already in browsers instead of signing everyone out. <c>o1</c> carries no
    /// owner key and <c>o2</c> no display name or grant; both are only ever read. Sign-in writes <c>o3</c> (#6214, #8737).</summary>
    private const string AdminPrefix = "o1:a:";
    private const string ViewerPrefix = "o1:v:";
    private const string AdminPrefixV2 = "o2:a:";
    private const string ViewerPrefixV2 = "o2:v:";
    private const string AdminPrefixV3 = "o3:a:";
    private const string ViewerPrefixV3 = "o3:v:";

    /// <summary>The prefix of a named web token's cookie subject (#7735): <c>t1:{name}</c>, the name alone.</summary>
    private const string TokenPrefix = "t1:";

    /// <summary>The length of an owner key: base64url (no padding) of a SHA-256 hash is 43 characters. It has no <c>:</c>.</summary>
    internal const int OwnerKeyLength = 43;

    /// <summary>The longest raw subject sign-in accepts, fixed at 207 characters (it was the room left in the 256-character cookie slot
    /// by the <c>o2</c> prefix, the owner key and its separator). It no longer follows <c>MaxSessionSubjectLength</c>, which grew to
    /// make room for the <c>o3</c> display name and grant: <c>MaxSessionSubjectLength</c> = 5 + 43 + 1 + 171 + 1 + 171 + 1 +
    /// <c>MaxSubjectLength</c>. Enforced at sign-in with a REFUSAL, mirroring the cookie's own rule.</summary>
    internal const int MaxSubjectLength = 207;

    /// <summary>The most UTF-8 bytes a display name or a granting role value keeps in the <c>o3</c> slot (base64url: 171 characters).
    /// See <see cref="CapDisplayField"/>.</summary>
    internal const int DisplayFieldMaxBytes = 128;

    /// <summary>Cuts a display-only field to at most <see cref="DisplayFieldMaxBytes"/> UTF-8 bytes at a text-element boundary
    /// (<see cref="StringInfo"/>), so a flag, a ZWJ emoji sequence or a letter with its combining mark is never split. Null stays null.
    /// Only for display fields: a subject is refused, never cut.</summary>
    internal static string? CapDisplayField(string? value)
    {
        if (value is null)
        {
            return null;
        }

        var bytes = 0;
        var end = 0;
        while (end < value.Length)
        {
            var length = StringInfo.GetNextTextElementLength(value, end);
            bytes += Encoding.UTF8.GetByteCount(value.AsSpan(end, length));
            if (bytes > DisplayFieldMaxBytes)
            {
                break;
            }

            end += length;
        }

        return end == value.Length ? value : value.Substring(0, end);
    }

    private static string EncodeSegment(string? value)
        => string.IsNullOrEmpty(value)
            ? string.Empty
            : Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(value)).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    /* An empty segment is null. False when the segment is not base64url or does not decode to valid UTF-8. */
    private static bool TryDecodeSegment(ReadOnlySpan<char> segment, out string? value)
    {
        value = null;
        if (segment.IsEmpty)
        {
            return true;
        }

        if (!IsBase64Url(segment) || segment.Length % 4 == 1)
        {
            return false;
        }

        var b64 = segment.ToString().Replace('-', '+').Replace('_', '/');
        b64 = (b64.Length % 4) switch
        {
            2 => b64 + "==",
            3 => b64 + "=",
            _ => b64,
        };
        try
        {
            value = new System.Text.UTF8Encoding(false, true).GetString(Convert.FromBase64String(b64));
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

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

    /// <summary>The names a built-in writer stamps on <c>updated_by</c>, <c>requested_by</c> and the <c>person:</c> journal actor as itself:
    /// the shared token's <c>web</c>, the MCP surface's <c>mcp</c>, the command line's <c>cli</c> and the Viewer's <c>viewer</c>. A named web
    /// token's name is its <see cref="EditorPrincipal"/>, so no token may take one of these (#7735): a person's edits would read as a surface's.</summary>
    internal static readonly string[] BuiltInStamps =
    {
        DarlingWebEndpoints.WebEditorPrincipal, DarlingWebEndpoints.McpEditorPrincipal, DarlingServiceConnectionTest.RequestedBy, "viewer",
    };

    /// <summary>True when <paramref name="name"/> is one of <see cref="BuiltInStamps"/>, compared ignoring case.</summary>
    internal static bool IsBuiltInStamp(string? name) =>
        name is not null && Array.Exists(BuiltInStamps, stamp => string.Equals(stamp, name, StringComparison.OrdinalIgnoreCase));

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

    /// <summary>Encodes an OIDC seat for the session cookie's subject slot. With an owner key it writes the <c>o3</c> shape
    /// (<c>o3:a:{ownerKey}:{b64u(displayName)}:{b64u(grantedBy)}:{subject}</c>, an empty segment meaning null); without one the <c>o1</c>
    /// shape an older build wrote (which has no room for a name or a grant). <c>o2</c> is no longer written. The caller caps the name and
    /// the grant with <see cref="CapDisplayField"/>.</summary>
    internal static string EncodeCookieSubject(string subject, WebOidcRole role, string? ownerKey = null, string? displayName = null, string? grantedBy = null)
    {
        if (role is not (WebOidcRole.Admin or WebOidcRole.Viewer))
        {
            throw new ArgumentOutOfRangeException(nameof(role), role, "A Denied sign-in never mints a cookie.");
        }

        var admin = role == WebOidcRole.Admin;
        return ownerKey is null
            ? (admin ? AdminPrefix : ViewerPrefix) + subject
            : (admin ? AdminPrefixV3 : ViewerPrefixV3) + ownerKey + ":" + EncodeSegment(displayName) + ":" + EncodeSegment(grantedBy) + ":" + subject;
    }

    /// <summary>Encodes a named web token's seat for the session cookie's subject slot (#7735): <c>t1:{name}</c>.</summary>
    internal static string EncodeTokenCookieSubject(string name) => TokenPrefix + name;

    internal static string EncodeGroupCookieSubject(Guid groupId, Guid generation) =>
        "g1:" + groupId.ToString("N") + ":" + generation.ToString("N");

    /// <summary>
    /// Resolves a validated session cookie's subject slot to a seat. Null (the two-segment shared-token
    /// cookie) is the shared-token seat; a recognized <c>o1:</c>, <c>o2:</c> or <c>o3:</c> encoding is that OIDC seat (<c>o1</c> with no owner key, <c>o1</c> and <c>o2</c> with no display name or grant; a malformed <c>o3</c> is refused); a
    /// <c>t1:</c> encoding is a named web token's seat, handed back READ-ONLY with <see cref="TokenName"/> set, for the middleware to look up
    /// in the live token set (an unknown name is then no seat at all); anything else is
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

        if (cookieSubject.StartsWith("g1:", StringComparison.Ordinal))
        {
            var parts=cookieSubject.Split(':');
            if(parts.Length==3 && Guid.TryParseExact(parts[1],"N",out var group) && group!=Guid.Empty &&
                Guid.TryParseExact(parts[2],"N",out var generation) && generation!=Guid.Empty)
            {
                seat=new DarlingWebSeat(null,false,GroupId:group,GroupGeneration:generation);
                return true;
            }
            seat=ReadsOnly;
            return false;
        }

        if (cookieSubject.StartsWith(TokenPrefix, StringComparison.Ordinal) && cookieSubject.Length > TokenPrefix.Length)
        {
            var name = cookieSubject.Substring(TokenPrefix.Length);
            seat = new DarlingWebSeat(name, CanEdit: false, TokenName: name);
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

        foreach (var (prefix, canEdit) in new[] { (AdminPrefixV3, true), (ViewerPrefixV3, false) })
        {
            if (!cookieSubject.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            /* o3:{a|v}:{ownerKey}:{name}:{grant}:{subject}. The key is exactly OwnerKeyLength base64url characters; name and grant are
               base64url (possibly empty, never containing ':'); the subject is whatever follows the fifth separator, non-empty, and may contain ':'. */
            var rest = cookieSubject.AsSpan(prefix.Length);
            if (rest.Length > OwnerKeyLength + 1 && rest[OwnerKeyLength] == ':' && IsBase64Url(rest.Slice(0, OwnerKeyLength)))
            {
                var tail = rest.Slice(OwnerKeyLength + 1);
                var nameEnd = tail.IndexOf(':');
                if (nameEnd >= 0)
                {
                    var afterName = tail.Slice(nameEnd + 1);
                    var grantEnd = afterName.IndexOf(':');
                    if (grantEnd >= 0 && afterName.Length > grantEnd + 1 &&
                        TryDecodeSegment(tail.Slice(0, nameEnd), out var displayName) &&
                        TryDecodeSegment(afterName.Slice(0, grantEnd), out var grantedBy))
                    {
                        seat = new DarlingWebSeat(afterName.Slice(grantEnd + 1).ToString(), canEdit, rest.Slice(0, OwnerKeyLength).ToString(),
                            DisplayName: displayName, GrantedBy: grantedBy);
                        return true;
                    }
                }
            }

            break;
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
    /// plus three unsafe-method exceptions, each a POST matched exactly. The first is <c>POST /api/compose/run</c>, which is read-only by
    /// construction (it compiles a panel spec into bound SELECTs on the viewer-role pool — the SPA runs every
    /// custom-view panel through it, so blocking it would break the read surface the viewer seat exists to
    /// grant). Everything else non-GET is refused — INCLUDING write endpoints that do not exist yet, which is
    /// the point of gating on the method instead of enumerating routes.
    /// <para>One more exception (#7109): a seat signed in with an <see cref="OwnerKey"/> may POST the two proposal presses,
    /// <c>/api/proposals/{id}/first-press</c> and <c>/api/proposals/{id}/confirm</c>, matched exactly (<see cref="IsProposalPressPath"/>).
    /// A grant gives the approve power to a person whatever the role of their sign-in, so the press endpoint, not this gate, decides: a seat
    /// that cannot edit presses only as the enabled person its key maps to, and the press API checks that person's grant. A seat with no
    /// owner key (the reads-only shared token, a front door's viewer, an older cookie) names nobody and stays refused here.</para>
    /// <para>The third (#8737): a seat signed in with an <see cref="OwnerKey"/> may POST <c>/api/mcp-tokens</c> and
    /// <c>/api/mcp-tokens/{id}/revoke</c>, matched exactly (<see cref="IsPersonalTokenWritePath"/>), to make and revoke their own personal MCP
    /// tokens. The route decides the rest: a read-only seat makes <c>reader</c> tokens only, revokes only its own, and is held by the per-person
    /// cap and the 24-hour create ceiling. A seat with no owner key stays refused here.</para>
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

        if (!HttpMethods.IsPost(method))
        {
            return false;
        }

        return string.Equals(path, "/api/compose/run", StringComparison.OrdinalIgnoreCase)
            || (seat.OwnerKey is not null && (IsProposalPressPath(path) || IsPersonalTokenWritePath(path)));
    }

    /// <summary>True for exactly <c>/api/mcp-tokens</c> or <c>/api/mcp-tokens/{id}/revoke</c>, the id 16 lowercase hex characters, with no
    /// other segment and no trailing slash (#8737). A person signed in with an owner key makes and revokes their own personal MCP tokens whatever
    /// the role of their sign-in; the route, not this gate, decides which role a token may take and whose token may be revoked.</summary>
    internal static bool IsPersonalTokenWritePath(string path)
    {
        const string Route = "/api/mcp-tokens";
        if (string.Equals(path, Route, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        const string Suffix = "/revoke";
        if (!path.StartsWith(Route + "/", StringComparison.OrdinalIgnoreCase) || !path.EndsWith(Suffix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var start = Route.Length + 1;
        var length = path.Length - Suffix.Length - start;
        return length == PersonalTokenIdLength && IsPersonalTokenId(path.Substring(start, length));
    }

    private const int PersonalTokenIdLength = 16;

    /// <summary>True for a personal token id: 16 lowercase hex characters.</summary>
    internal static bool IsPersonalTokenId(string? id)
    {
        if (id is null || id.Length != PersonalTokenIdLength)
        {
            return false;
        }

        foreach (var c in id)
        {
            if (!(c is >= '0' and <= '9' or >= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>True for exactly <c>/api/proposals/{id}/first-press</c> or <c>/api/proposals/{id}/confirm</c>, the id a GUID in its hyphenated
    /// form, with no other segment and no trailing slash (#7109).</summary>
    internal static bool IsProposalPressPath(string path)
    {
        const string Prefix = "/api/proposals/";
        if (!path.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var rest = path.AsSpan(Prefix.Length);
        var slash = rest.IndexOf('/');
        if (slash < 0 || !Guid.TryParseExact(rest[..slash], "D", out _))
        {
            return false;
        }

        var verb = rest[(slash + 1)..];
        return verb.Equals("first-press", StringComparison.OrdinalIgnoreCase) || verb.Equals("confirm", StringComparison.OrdinalIgnoreCase);
    }
}
