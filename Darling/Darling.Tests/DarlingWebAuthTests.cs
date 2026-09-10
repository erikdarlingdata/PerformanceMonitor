/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1562 web dashboard browser auth (network mode). The route decision is a PURE function over the resolved
/// facts (<see cref="DarlingWebHostService.DecideWebAuth"/>) so the whole matrix pins without a server, and the
/// HMAC session cookie's sign/verify/tamper/expiry round-trip pins the token→cookie exchange. The verdict enum
/// is internal, so the matrix compares its name (an internal type cannot appear in a public test signature).
/// </summary>
public sealed class DarlingWebAuthTests
{
    private static readonly byte[] Key = RandomNumberGenerator.GetBytes(32);
    private static readonly byte[] OtherKey = RandomNumberGenerator.GetBytes(32);

    private static IPNetwork Cidr => IPNetwork.Parse("192.168.1.0/24");

    /* ---- ROUTE AUTH MATRIX ---- */

    /* #1649: loopback no longer passes tokenless. This method only runs in NETWORK mode, and the surface
       stopped being read-only at Custom Views v2 (view CRUD + /api/compose/run), so a local process could
       read and mutate with no credential while LAN-exposed. Loopback is now exempt from the CIDR test ONLY
       — it authenticates like any other remote, mirroring the MCP host. The three loopback rows below are
       the behavior change: same inputs, ShowLogin instead of Allow. */
    [Theory]
    [InlineData("127.0.0.1", false, false, "ShowLogin")]              // loopback, no credential -> login form, NOT a free pass
    [InlineData("::1", false, false, "ShowLogin")]                    // IPv6 loopback
    [InlineData("::ffff:127.0.0.1", false, false, "ShowLogin")]       // IPv4-mapped loopback
    [InlineData("127.0.0.1", true, false, "Allow")]                   // loopback + valid cookie -> in
    [InlineData("127.0.0.1", false, true, "SetCookieAndRedirect")]    // loopback + valid token -> cookie + 302, same exchange as a LAN client
    [InlineData("::ffff:127.0.0.1", false, true, "SetCookieAndRedirect")] // the IPv4-mapped form takes the same path (no unwrap drift)
    [InlineData("192.168.1.50", false, true, "SetCookieAndRedirect")] // in-CIDR + valid token -> cookie + 302
    [InlineData("192.168.1.50", true, false, "Allow")]                // in-CIDR + valid cookie
    [InlineData("192.168.1.50", true, true, "Allow")]                 // cookie is checked before the token
    [InlineData("10.0.0.5", true, true, "Forbid")]                    // out-of-CIDR, even WITH a cookie+token
    [InlineData("192.168.1.50", false, false, "ShowLogin")]           // in-CIDR, no cookie/token (or expired/tampered cookie -> hasValidCookie=false)
    public void DecideWebAuth_RouteMatrix(string remote, bool hasValidCookie, bool hasValidToken, string expected)
    {
        var action = DarlingWebHostService.DecideWebAuth(IPAddress.Parse(remote), Cidr, hasValidCookie, hasValidToken);
        Assert.Equal(expected, action.ToString());
    }

    [Fact]
    public void DecideWebAuth_NullRemote_Forbids()
        => Assert.Equal("Forbid", DarlingWebHostService.DecideWebAuth(null, Cidr, hasValidCookie: true, hasValidToken: true).ToString());

    /* ---- Host-header allowlist: the DNS-rebinding guard (security review M1; #1576 extends it to BOTH modes) ----
       The pure decision is unchanged; #1576 wires it into the loopback-only pipeline too (null listen IP), which
       these null-listenIp rows pin: loopback Hosts pass, a rebound foreign Host fails. */

    [Theory]
    [InlineData("localhost", "192.168.1.205", true)]      // loopback name
    [InlineData("LOCALHOST", "192.168.1.205", true)]      // case-insensitive
    [InlineData("127.0.0.1", "192.168.1.205", true)]      // loopback IP
    [InlineData("::1", "192.168.1.205", true)]            // IPv6 loopback
    [InlineData("192.168.1.205", "192.168.1.205", true)] // the configured listen IP (the real operator, direct)
    [InlineData("", "192.168.1.205", true)]              // empty Host (HTTP/1.0 / probes) is allowed
    [InlineData("evil.com", "192.168.1.205", false)]      // rebound foreign hostname -> rejected
    [InlineData("192.168.1.205", null, false)]            // loopback-only mode: the listen IP is NOT an allowed host
    [InlineData("attacker.internal", null, false)]        // rebound name in loopback-only mode -> rejected
    /* #1576: loopback-only mode (null listen IP) — loopback Hosts pass, a rebound foreign Host is rejected. */
    [InlineData("localhost", null, true)]                 // loopback name passes in loopback-only mode
    [InlineData("127.0.0.1", null, true)]                 // loopback IP passes in loopback-only mode
    [InlineData("::1", null, true)]                       // IPv6 loopback passes in loopback-only mode
    [InlineData("", null, true)]                          // empty Host still allowed in loopback-only mode
    [InlineData("evil.com", null, false)]                 // rebound foreign host in loopback-only mode -> rejected
    public void IsAllowedHost_RejectsReboundForeignHosts(string host, string? listenIp, bool expected)
    {
        var ip = listenIp is null ? null : IPAddress.Parse(listenIp);
        Assert.Equal(expected, DarlingWebHostService.IsAllowedHost(host, ip));
    }

    /* ---- Open-redirect guard on the token-strip 302 (security review M2) ---- */

    [Theory]
    [InlineData("/dashboard", "/dashboard")]            // ordinary path untouched
    [InlineData("/", "/")]                              // root
    [InlineData("", "/")]                               // empty -> root
    [InlineData("//evil.com/", "/evil.com/")]           // protocol-relative -> collapsed single slash
    [InlineData("/\\evil.com", "/evil.com")]            // slash-backslash trick -> collapsed
    [InlineData("///a", "/a")]                          // longer leading run
    [InlineData("/fleet?x=1", "/fleet?x=1")]            // preserved query on a safe path
    /* #2730 review catch: the WHATWG URL parser strips tab/CR/LF from ANYWHERE in a URL before parsing —
       location.replace, <a href>, and Location headers alike — so "\t//evil.com" past a leading-slash-only
       guard becomes protocol-relative IN THE BROWSER. The guard strips C0 controls first, so it sees the
       same string the browser will. */
    [InlineData("\t//evil.com", "/evil.com")]           // tab-prefixed protocol-relative -> neutralized
    [InlineData("/\t/evil.com", "/evil.com")]           // embedded tab building a // after browser stripping
    [InlineData("\n//evil.com", "/evil.com")]           // LF variant
    [InlineData("/\r\n/evil.com", "/evil.com")]         // CRLF variant
    [InlineData("/dead\tlocks", "/deadlocks")]          // controls are stripped, the rest survives
    public void SanitizeRedirectPath_ForcesSingleSlashSiteRelative(string input, string expected)
        => Assert.Equal(expected, DarlingWebHostService.SanitizeRedirectPath(input));

    /* ---- SESSION COOKIE sign / verify / tamper / expiry ---- */

    [Fact]
    public void SessionCookie_SignedThenVerified_RoundTrips()
    {
        var now = DateTimeOffset.UtcNow;
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12));
        Assert.True(DarlingWebHostService.TryValidateSessionCookie(cookie, Key, now));
    }

    [Fact]
    public void SessionCookie_WrongKey_Fails()
    {
        var now = DateTimeOffset.UtcNow;
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12));
        /* A restart regenerates the signing key, which is exactly this case — old sessions no longer verify. */
        Assert.False(DarlingWebHostService.TryValidateSessionCookie(cookie, OtherKey, now));
    }

    [Fact]
    public void SessionCookie_Expired_Fails()
    {
        /* Signed 24h ago with a 12h life -> expired 12h before "now". */
        var signedAt = DateTimeOffset.UtcNow.AddHours(-24);
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, signedAt.AddHours(12));
        Assert.False(DarlingWebHostService.TryValidateSessionCookie(cookie, Key, DateTimeOffset.UtcNow));
    }

    [Fact]
    public void SessionCookie_TamperedSignature_Fails()
    {
        var now = DateTimeOffset.UtcNow;
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12));

        /* Tamper the FIRST character of the base64url signature (the char right after the '.'), not the last.
           The signature is base64url(HMAC-SHA256) = 43 chars for 32 bytes; the LAST char encodes only 4 data
           bits + 2 padding bits, so flipping it (e.g. 'A'->'B') can leave the decoded bytes identical — a
           no-op "tamper" that validation rightly accepts, which made the old last-char flip flaky ~1 run in 16.
           The first signature char is fully data-bearing (6 bits), so any flip always changes the decoded MAC. */
        var dot = cookie.IndexOf('.', StringComparison.Ordinal);
        var sigStart = dot + 1;
        var tampered = cookie[..sigStart] + (cookie[sigStart] == 'A' ? 'B' : 'A') + cookie[(sigStart + 1)..];
        Assert.False(DarlingWebHostService.TryValidateSessionCookie(tampered, Key, now));
    }

    [Fact]
    public void SessionCookie_TamperedExpiry_Fails()
    {
        var now = DateTimeOffset.UtcNow;
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(1));
        var signatureWithDot = cookie.Substring(cookie.IndexOf('.'));
        /* Extend the expiry 10 years out but keep the OLD signature — the HMAC is over the expiry, so it fails. */
        var forgedExpiry = now.AddYears(10).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        Assert.False(DarlingWebHostService.TryValidateSessionCookie(forgedExpiry + signatureWithDot, Key, now));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("no-dot")]
    [InlineData(".onlysignature")]
    [InlineData("12345.")]                 // empty signature part
    [InlineData("notanumber.YWJjZA")]      // non-numeric expiry
    public void SessionCookie_Malformed_Fails(string? value)
        => Assert.False(DarlingWebHostService.TryValidateSessionCookie(value, Key, DateTimeOffset.UtcNow));

    /* ---- SESSION SUBJECT: the cookie carries WHO, and the signature covers it (#2550) ---- */

    /// <summary>
    /// base64url of a subject, matching what the builder embeds — so a test can forge a segment that is
    /// well-formed in every respect EXCEPT being signed, which is the only interesting kind of forgery.
    /// </summary>
    private static string EncodeSubject(string subject)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(subject)).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    [Fact]
    public void SessionSubject_RoundTrips()
    {
        var now = DateTimeOffset.UtcNow;
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12), "erik@example.com");

        Assert.Equal(3, cookie.Split('.').Length);
        Assert.True(DarlingWebHostService.TryValidateSessionCookie(cookie, Key, now, out var subject));
        Assert.Equal("erik@example.com", subject);
    }

    /// <summary>
    /// THE property this shape exists for. Signing only the expiry and parking the subject beside it would
    /// leave identity unauthenticated: any holder of a valid cookie could rewrite the subject and be served as
    /// anyone. That is strictly worse than the shared token it replaces, because it would LOOK like identity.
    /// </summary>
    [Fact]
    public void SessionSubject_Rewritten_Fails()
    {
        var now = DateTimeOffset.UtcNow;
        var parts = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12), "reader@example.com").Split('.');
        var forged = $"{parts[0]}.{EncodeSubject("admin@example.com")}.{parts[2]}";

        Assert.False(DarlingWebHostService.TryValidateSessionCookie(forged, Key, now, out _));
    }

    /// <summary>
    /// The other half of the same property: a signature is bound to ITS payload, so one lifted off a different
    /// principal's cookie does not authenticate this one. Both cookies here are genuine and unexpired — only
    /// the pairing is wrong.
    /// </summary>
    [Fact]
    public void SessionSubject_GraftedSignatureFromAnotherPrincipal_Fails()
    {
        var now = DateTimeOffset.UtcNow;
        var expiry = now.AddHours(12);
        var mine = DarlingWebHostService.BuildSessionCookieValue(Key, expiry, "reader@example.com").Split('.');
        var theirs = DarlingWebHostService.BuildSessionCookieValue(Key, expiry, "admin@example.com").Split('.');

        Assert.False(DarlingWebHostService.TryValidateSessionCookie($"{mine[0]}.{mine[1]}.{theirs[2]}", Key, now, out _));
    }

    [Fact]
    public void SessionSubject_TamperedExpiry_Fails()
    {
        var now = DateTimeOffset.UtcNow;
        var parts = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(1), "erik@example.com").Split('.');
        var forgedExpiry = now.AddYears(10).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);

        /* The signed region grew to include the subject, so this pins that it still covers the expiry too —
           a construction that authenticated only the subject would trade one forgery for another. */
        Assert.False(DarlingWebHostService.TryValidateSessionCookie($"{forgedExpiry}.{parts[1]}.{parts[2]}", Key, now, out _));
    }

    /// <summary>
    /// The shared-token seat reports null, NOT empty string. "The shared token did this" and "somebody signed
    /// in whose name we failed to read" are different facts, and a caller stamping provenance has to be able
    /// to tell them apart.
    /// </summary>
    [Fact]
    public void SessionSubject_SharedTokenSeat_IsNullNotEmpty()
    {
        var now = DateTimeOffset.UtcNow;
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12));

        Assert.Equal(2, cookie.Split('.').Length);
        Assert.True(DarlingWebHostService.TryValidateSessionCookie(cookie, Key, now, out var subject));
        Assert.Null(subject);
    }

    /// <summary>
    /// An upgrade must not sign everybody out, so the subjectless cookie is byte-for-byte what this minted
    /// before per-user identity existed — asserted by equality against a freshly built one rather than by
    /// eyeballing the format.
    /// </summary>
    [Fact]
    public void SessionSubject_EmptySubject_ProducesTheSubjectlessShape()
    {
        var expiry = DateTimeOffset.UtcNow.AddHours(12);

        Assert.Equal(
            DarlingWebHostService.BuildSessionCookieValue(Key, expiry),
            DarlingWebHostService.BuildSessionCookieValue(Key, expiry, string.Empty));
    }

    /// <summary>
    /// Refuses rather than truncates. Truncation is the worse failure: two subjects sharing a 256-character
    /// prefix would collapse into one seat, silently attributing one person's writes to another.
    /// </summary>
    [Fact]
    public void SessionSubject_OverLong_ThrowsRatherThanTruncating()
    {
        var expiry = DateTimeOffset.UtcNow.AddHours(12);
        var tooLong = new string('x', DarlingWebHostService.MaxSessionSubjectLength + 1);

        Assert.Throws<ArgumentException>(() => DarlingWebHostService.BuildSessionCookieValue(Key, expiry, tooLong));
        Assert.Equal(3, DarlingWebHostService
            .BuildSessionCookieValue(Key, expiry, new string('x', DarlingWebHostService.MaxSessionSubjectLength))
            .Split('.').Length);
    }

    /// <summary>
    /// UTF-8, not ASCII: a subject can be an email or a display name with an accent in it, and mangling
    /// somebody's name into question marks is both wrong and a way for two people to become one seat.
    /// </summary>
    [Fact]
    public void SessionSubject_NonAscii_RoundTrips()
    {
        var now = DateTimeOffset.UtcNow;
        const string Subject = "yasm\u00edn.o'brien@example.com";
        var cookie = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12), Subject);

        Assert.True(DarlingWebHostService.TryValidateSessionCookie(cookie, Key, now, out var subject));
        Assert.Equal(Subject, subject);
    }

    /// <summary>
    /// The two shapes share a separator, so these pin that neither can be re-presented as the other: a
    /// 3-segment payload offered as a 2-segment cookie needs the subject to be a valid HMAC over the expiry,
    /// and a smuggled extra dot is refused outright so two distinct cookies can never parse to one subject.
    /// </summary>
    [Fact]
    public void SessionSubject_ShapeConfusion_Fails()
    {
        var now = DateTimeOffset.UtcNow;
        var parts = DarlingWebHostService.BuildSessionCookieValue(Key, now.AddHours(12), "erik@example.com").Split('.');

        Assert.False(DarlingWebHostService.TryValidateSessionCookie($"{parts[0]}.{parts[1]}", Key, now, out _));
        Assert.False(DarlingWebHostService.TryValidateSessionCookie($"{parts[0]}.{parts[1]}.{parts[1]}.{parts[2]}", Key, now, out _));
        Assert.False(DarlingWebHostService.TryValidateSessionCookie($"{parts[0]}..{parts[2]}", Key, now, out _));
    }

    /* ---- #2550: the per-request decision WITH the sign-in flow, composed AROUND DecideWebAuth ----
       The DecideWebAuth matrix above is the pinned pre-OIDC behavior and is untouched; these rows pin the
       composition rules: the CIDR stays outermost (it beats the OIDC endpoints too), a flow path is handled
       regardless of credential state, and every non-flow request decides exactly as before. */

    [Theory]
    [InlineData("10.0.0.5", true, true, true, "Forbid")]                 // out-of-CIDR beats the flow route — sign-in is not a way past the boundary
    [InlineData("192.168.1.50", true, false, false, "HandleAuthFlow")]   // in-CIDR flow route, no credential — the pre-auth login leg
    [InlineData("192.168.1.50", true, true, false, "HandleAuthFlow")]    // flow route wins over a valid cookie (re-login / logout)
    [InlineData("127.0.0.1", true, false, false, "HandleAuthFlow")]      // loopback may sign in too (localhost redirect URIs)
    [InlineData("192.168.1.50", false, true, false, "Allow")]            // non-flow rows delegate to the original matrix…
    [InlineData("192.168.1.50", false, false, true, "SetCookieAndRedirect")]
    [InlineData("192.168.1.50", false, false, false, "ShowLogin")]
    [InlineData("10.0.0.5", false, true, true, "Forbid")]
    public void DecideWebRequest_ComposesFlowAroundDecideWebAuth(
        string remote, bool isAuthFlowRoute, bool hasValidCookie, bool hasValidToken, string expected)
        => Assert.Equal(expected, DarlingWebHostService.DecideWebRequest(
            IPAddress.Parse(remote), Cidr, isAuthFlowRoute, hasValidCookie, hasValidToken).ToString());

    [Theory]
    [InlineData("/auth/logout", false, true)]                 // logout is a flow path even with OIDC off (token seats hold cookies too)
    [InlineData("/auth/logout", true, true)]
    [InlineData("/auth/oidc/login", true, true)]
    [InlineData("/auth/oidc/callback", true, true)]
    [InlineData("/auth/oidc/login", false, false)]            // OIDC off: the legs fall through to normal auth (login page, no squatting)
    [InlineData("/auth/oidc/callback", false, false)]
    [InlineData("/AUTH/OIDC/LOGIN", true, true)]              // paths are case-insensitive on this stack
    [InlineData("/api/fleet", true, false)]
    [InlineData("/auth/oidc/login/extra", true, false)]       // exact paths only
    public void IsAuthFlowPath_Matrix(string path, bool oidcEnabled, bool expected)
        => Assert.Equal(expected, DarlingWebHostService.IsAuthFlowPath(path, oidcEnabled));

    /* ---- #2550: the login page grows the SSO affordance exactly when OIDC is enabled ---- */

    [Fact]
    public void LoginPage_SsoLink_PresentOnlyWhenOidcEnabled()
    {
        var withOidc = DarlingWebHostService.BuildLoginPageHtml(oidcEnabled: true);
        var withoutOidc = DarlingWebHostService.BuildLoginPageHtml(oidcEnabled: false);

        Assert.Contains(DarlingWebHostService.OidcLoginPath, withOidc, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingWebHostService.OidcLoginPath, withoutOidc, StringComparison.Ordinal);

        /* The token form is NEVER removed — the shared token stays as the scripted-caller/break-glass path. */
        Assert.Contains("name='token'", withOidc, StringComparison.Ordinal);
        Assert.Contains("name='token'", withoutOidc, StringComparison.Ordinal);

        /* The placeholder must not leak into either rendering. */
        Assert.DoesNotContain("<!--SSO-->", withOidc, StringComparison.Ordinal);
        Assert.DoesNotContain("<!--SSO-->", withoutOidc, StringComparison.Ordinal);
    }
}
