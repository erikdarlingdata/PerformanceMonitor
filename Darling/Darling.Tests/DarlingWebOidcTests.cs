/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2550 OIDC sign-in: the protocol surface is PURE (<see cref="DarlingWebOidc"/>), so config validation,
/// PKCE, ID-token claim validation, subject resolution, role mapping, the seat codec, the read-only write
/// gate, and the transaction cookie all pin without a provider. The two network touches (discovery, code
/// exchange) are exercised against a live IdP, not here.
/// </summary>
public sealed class DarlingWebOidcTests
{
    private static WebOidcConfig ValidConfig() => new()
    {
        Authority = "https://login.example.com/tenant/v2.0",
        ClientId = "client-123",
    };

    /* ---- CONFIG VALIDATION ---- */

    [Fact]
    public void ValidateConfig_MinimalHttpsAuthorityAndClientId_Passes()
        => Assert.Null(DarlingWebOidc.ValidateConfig(ValidConfig()));

    [Fact]
    public void ValidateConfig_MissingAuthority_Fails()
        => Assert.Contains("authority", DarlingWebOidc.ValidateConfig(new WebOidcConfig { ClientId = "c" })!, StringComparison.Ordinal);

    [Fact]
    public void ValidateConfig_RelativeAuthority_Fails()
        => Assert.Contains("absolute", DarlingWebOidc.ValidateConfig(new WebOidcConfig { Authority = "login.example.com", ClientId = "c" })!, StringComparison.Ordinal);

    /// <summary>http:// toward a NON-loopback host would send the code exchange (client secret and all) in
    /// the clear; toward loopback it is the local test-IdP case and allowed.</summary>
    [Fact]
    public void ValidateConfig_HttpAuthority_LoopbackOnly()
    {
        Assert.Null(DarlingWebOidc.ValidateConfig(new WebOidcConfig { Authority = "http://localhost:8080/default", ClientId = "c" }));
        Assert.Null(DarlingWebOidc.ValidateConfig(new WebOidcConfig { Authority = "http://127.0.0.1:8080/default", ClientId = "c" }));
        Assert.Contains("clear", DarlingWebOidc.ValidateConfig(new WebOidcConfig { Authority = "http://idp.corp.local/oauth", ClientId = "c" })!, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateConfig_NonHttpScheme_Fails()
        => Assert.Contains("https", DarlingWebOidc.ValidateConfig(new WebOidcConfig { Authority = "ftp://idp.example.com", ClientId = "c" })!, StringComparison.Ordinal);

    [Fact]
    public void ValidateConfig_MissingClientId_Fails()
        => Assert.Contains("clientId", DarlingWebOidc.ValidateConfig(new WebOidcConfig { Authority = "https://idp.example.com" })!, StringComparison.Ordinal);

    /// <summary>Half a role mapping is refused BOTH ways: a claim with no lists would deny (or admit)
    /// everyone silently, lists with no claim would never be read.</summary>
    [Fact]
    public void ValidateConfig_HalfConfiguredRoleMapping_FailsBothWays()
    {
        var claimOnly = ValidConfig();
        claimOnly.RoleClaim = "groups";
        Assert.Contains("roleClaim", DarlingWebOidc.ValidateConfig(claimOnly)!, StringComparison.Ordinal);

        var listsOnly = ValidConfig();
        listsOnly.AdminRoles = new[] { "dba" };
        Assert.Contains("roleClaim", DarlingWebOidc.ValidateConfig(listsOnly)!, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateConfig_FullRoleMapping_Passes()
    {
        var config = ValidConfig();
        config.RoleClaim = "groups";
        config.AdminRoles = new[] { "dba" };
        config.ViewerRoles = new[] { "eng" };
        Assert.Null(DarlingWebOidc.ValidateConfig(config));
    }

    /* ---- SCOPES ---- */

    [Theory]
    [InlineData(null, "openid profile email")]                 // default
    [InlineData("", "openid profile email")]                   // blank -> default
    [InlineData("openid profile", "openid profile")]           // already carries openid -> untouched
    [InlineData("profile email", "openid profile email")]      // openid FORCED in (no ID token without it)
    [InlineData("  profile   email  ", "openid profile email")] // whitespace normalized
    public void EffectiveScopes_AlwaysIncludeOpenid(string? configured, string expected)
        => Assert.Equal(expected, DarlingWebOidc.EffectiveScopes(configured));

    /* ---- PKCE (RFC 7636) ---- */

    /// <summary>The S256 transform pinned against RFC 7636 Appendix B's own vector — not a re-derivation.</summary>
    [Fact]
    public void ComputeCodeChallenge_MatchesRfc7636AppendixB()
        => Assert.Equal(
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
            DarlingWebOidc.ComputeCodeChallenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"));

    [Fact]
    public void CreatePkce_VerifierShapeAndFreshness()
    {
        var (verifier, challenge) = DarlingWebOidc.CreatePkce();
        var (verifier2, _) = DarlingWebOidc.CreatePkce();

        /* RFC 7636: 43-128 chars from the unreserved alphabet. 32 bytes base64url = exactly 43. */
        Assert.Equal(43, verifier.Length);
        Assert.All(verifier, c => Assert.True(
            char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' or '~', $"'{c}' is outside the PKCE alphabet"));
        Assert.Equal(DarlingWebOidc.ComputeCodeChallenge(verifier), challenge);
        Assert.NotEqual(verifier, verifier2);
    }

    /* ---- AUTHORIZATION URL ---- */

    [Fact]
    public void BuildAuthorizationUrl_CarriesEveryParameterEscaped()
    {
        var url = DarlingWebOidc.BuildAuthorizationUrl(
            "https://idp.example.com/authorize",
            "client 123",
            "https://192.168.1.205:5153/auth/oidc/callback",
            "openid profile email",
            "state-x",
            "nonce-y",
            "challenge-z");

        Assert.StartsWith("https://idp.example.com/authorize?response_type=code&", url, StringComparison.Ordinal);
        Assert.Contains("&client_id=client%20123", url, StringComparison.Ordinal);
        Assert.Contains("&redirect_uri=https%3A%2F%2F192.168.1.205%3A5153%2Fauth%2Foidc%2Fcallback", url, StringComparison.Ordinal);
        Assert.Contains("&scope=openid%20profile%20email", url, StringComparison.Ordinal);
        Assert.Contains("&state=state-x", url, StringComparison.Ordinal);
        Assert.Contains("&nonce=nonce-y", url, StringComparison.Ordinal);
        Assert.Contains("&code_challenge=challenge-z", url, StringComparison.Ordinal);
        Assert.EndsWith("&code_challenge_method=S256", url, StringComparison.Ordinal);
    }

    /// <summary>Some IdPs publish parameterized authorization endpoints; a second '?' would corrupt them.</summary>
    [Fact]
    public void BuildAuthorizationUrl_EndpointWithExistingQuery_AppendsWithAmpersand()
        => Assert.Contains("?p=1&response_type=code", DarlingWebOidc.BuildAuthorizationUrl(
            "https://idp.example.com/authorize?p=1", "c", "https://h/cb", "openid", "s", "n", "ch"), StringComparison.Ordinal);

    /* ---- ID TOKEN: parse + claim validation ---- */

    private static string B64Url(string value)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(value)).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static string FakeJwt(string payloadJson)
        => $"{B64Url("{\"alg\":\"RS256\"}")}.{B64Url(payloadJson)}.{B64Url("signature")}";

    private const string Issuer = "https://idp.example.com";
    private const string ClientId = "client-123";
    private const string Nonce = "nonce-abc";

    private static JsonObject Payload(
        string? iss = Issuer, object? aud = null, long? exp = null, string? nonce = Nonce, string? azp = null)
    {
        var payload = new JsonObject();
        if (iss is not null)
        {
            payload["iss"] = iss;
        }

        switch (aud)
        {
            case null:
                payload["aud"] = ClientId;
                break;
            case string s:
                payload["aud"] = s;
                break;
            case string[] many:
                payload["aud"] = new JsonArray(many.Select(a => (JsonNode)a).ToArray());
                break;
        }

        payload["exp"] = exp ?? DateTimeOffset.UtcNow.AddMinutes(10).ToUnixTimeSeconds();
        if (nonce is not null)
        {
            payload["nonce"] = nonce;
        }

        if (azp is not null)
        {
            payload["azp"] = azp;
        }

        payload["sub"] = "sub-guid";
        return payload;
    }

    [Fact]
    public void TryParseJwtPayload_ThreeSegmentToken_Parses()
    {
        var payload = DarlingWebOidc.TryParseJwtPayload(FakeJwt("{\"sub\":\"abc\",\"iss\":\"x\"}"));
        Assert.NotNull(payload);
        Assert.Equal("abc", payload!["sub"]!.GetValue<string>());
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-jwt")]
    [InlineData("only.two")]
    [InlineData("a.b.c.d")]                                   // four segments
    [InlineData("a.!!!notbase64url!!!.c")]                    // undecodable payload
    public void TryParseJwtPayload_Malformed_ReturnsNull(string jwt)
        => Assert.Null(DarlingWebOidc.TryParseJwtPayload(jwt));

    [Fact]
    public void TryParseJwtPayload_PayloadNotAnObject_ReturnsNull()
        => Assert.Null(DarlingWebOidc.TryParseJwtPayload($"{B64Url("{}")}.{B64Url("[1,2]")}.{B64Url("s")}"));

    [Fact]
    public void ValidateIdTokenClaims_HappyPath_Passes()
        => Assert.Null(DarlingWebOidc.ValidateIdTokenClaims(Payload(), Issuer, ClientId, Nonce, DateTimeOffset.UtcNow));

    [Fact]
    public void ValidateIdTokenClaims_WrongIssuer_Fails()
        => Assert.Contains("issuer", DarlingWebOidc.ValidateIdTokenClaims(
            Payload(iss: "https://evil.example.com"), Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);

    [Fact]
    public void ValidateIdTokenClaims_WrongAudience_Fails()
        => Assert.Contains("audience", DarlingWebOidc.ValidateIdTokenClaims(
            Payload(aud: "another-client"), Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);

    /// <summary>A single-element aud ARRAY containing this client is fine without azp.</summary>
    [Fact]
    public void ValidateIdTokenClaims_SingleAudienceArray_Passes()
        => Assert.Null(DarlingWebOidc.ValidateIdTokenClaims(
            Payload(aud: new[] { ClientId }), Issuer, ClientId, Nonce, DateTimeOffset.UtcNow));

    /// <summary>OIDC Core §3.1.3.7 rules 4-5: a token minted for SEVERAL clients must name this one as the
    /// authorized party, or it does not say it was minted FOR this one.</summary>
    [Fact]
    public void ValidateIdTokenClaims_MultiAudience_RequiresAzp()
    {
        var withoutAzp = Payload(aud: new[] { ClientId, "other-client" });
        Assert.Contains("azp", DarlingWebOidc.ValidateIdTokenClaims(withoutAzp, Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);

        var withAzp = Payload(aud: new[] { ClientId, "other-client" }, azp: ClientId);
        Assert.Null(DarlingWebOidc.ValidateIdTokenClaims(withAzp, Issuer, ClientId, Nonce, DateTimeOffset.UtcNow));

        var wrongAzp = Payload(aud: new[] { ClientId, "other-client" }, azp: "other-client");
        Assert.Contains("azp", DarlingWebOidc.ValidateIdTokenClaims(wrongAzp, Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateIdTokenClaims_Expired_FailsBeyondSkew()
    {
        var now = DateTimeOffset.UtcNow;

        /* Just past exp but inside the 5-minute skew: still valid (clock drift is real). */
        Assert.Null(DarlingWebOidc.ValidateIdTokenClaims(
            Payload(exp: now.AddMinutes(-2).ToUnixTimeSeconds()), Issuer, ClientId, Nonce, now));

        /* Beyond the skew: expired. */
        Assert.Contains("expired", DarlingWebOidc.ValidateIdTokenClaims(
            Payload(exp: now.AddMinutes(-10).ToUnixTimeSeconds()), Issuer, ClientId, Nonce, now)!, StringComparison.Ordinal);
    }

    /// <summary>The nonce is what binds the token to the transaction THIS host started — without it a code
    /// minted for some other sign-in could be redeemed against this one.</summary>
    [Fact]
    public void ValidateIdTokenClaims_NonceMismatchOrMissing_Fails()
    {
        Assert.Contains("nonce", DarlingWebOidc.ValidateIdTokenClaims(
            Payload(nonce: "some-other-nonce"), Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);
        Assert.Contains("nonce", DarlingWebOidc.ValidateIdTokenClaims(
            Payload(nonce: null), Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);
    }

    /// <summary>A malformed token REFUSES, never throws — validation runs on IdP-authored input.</summary>
    [Fact]
    public void ValidateIdTokenClaims_NonStringClaimTypes_RefuseWithoutThrowing()
    {
        var payload = Payload();
        payload["iss"] = 12345; /* number where a string belongs */
        Assert.NotNull(DarlingWebOidc.ValidateIdTokenClaims(payload, Issuer, ClientId, Nonce, DateTimeOffset.UtcNow));

        var noAud = Payload();
        noAud.Remove("aud");
        Assert.Contains("aud", DarlingWebOidc.ValidateIdTokenClaims(noAud, Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);

        var noExp = Payload();
        noExp.Remove("exp");
        Assert.Contains("exp", DarlingWebOidc.ValidateIdTokenClaims(noExp, Issuer, ClientId, Nonce, DateTimeOffset.UtcNow)!, StringComparison.Ordinal);
    }

    /* ---- SUBJECT RESOLUTION ---- */

    [Fact]
    public void ResolveSubject_DefaultChain_PrefersHumanReadableNames()
    {
        var full = new JsonObject { ["preferred_username"] = "erik@example.com", ["email"] = "e2@example.com", ["sub"] = "guid" };
        Assert.Equal("erik@example.com", DarlingWebOidc.ResolveSubject(full, null));

        var emailOnly = new JsonObject { ["email"] = "e2@example.com", ["email_verified"] = true, ["sub"] = "guid" };
        Assert.Equal("e2@example.com", DarlingWebOidc.ResolveSubject(emailOnly, null));

        var subOnly = new JsonObject { ["sub"] = "guid" };
        Assert.Equal("guid", DarlingWebOidc.ResolveSubject(subOnly, null));

        Assert.Null(DarlingWebOidc.ResolveSubject(new JsonObject(), null));
    }

    /// <summary>
    /// Review catch on #2730: the default chain's email step trusts an address only when the IdP says it
    /// VERIFIED it. An IdP that lets users self-assert an email would otherwise let an authenticated
    /// stranger present a real operator's address and be stamped as them in updated_by and the audit log.
    /// An unverified email falls through to the opaque-but-honest sub; an operator who EXPLICITLY configures
    /// subjectClaim=email has stated they trust their directory, and that choice is honored ungated.
    /// </summary>
    [Fact]
    public void ResolveSubject_DefaultChainEmail_RequiresEmailVerified()
    {
        var unverified = new JsonObject { ["email"] = "victim@example.com", ["sub"] = "guid" };
        Assert.Equal("guid", DarlingWebOidc.ResolveSubject(unverified, null));

        var explicitlyFalse = new JsonObject { ["email"] = "victim@example.com", ["email_verified"] = false, ["sub"] = "guid" };
        Assert.Equal("guid", DarlingWebOidc.ResolveSubject(explicitlyFalse, null));

        /* OIDC says boolean; real providers have shipped the string form. Both count as verified. */
        var stringTrue = new JsonObject { ["email"] = "e@example.com", ["email_verified"] = "true", ["sub"] = "guid" };
        Assert.Equal("e@example.com", DarlingWebOidc.ResolveSubject(stringTrue, null));

        /* The explicit operator choice bypasses the gate. */
        Assert.Equal("victim@example.com", DarlingWebOidc.ResolveSubject(unverified, "email"));
    }

    /// <summary>A CONFIGURED subjectClaim is required, not preferred: falling back would stamp updated_by
    /// with a claim the operator did not choose, silently.</summary>
    [Fact]
    public void ResolveSubject_ConfiguredClaim_NeverFallsBack()
    {
        var payload = new JsonObject { ["preferred_username"] = "erik@example.com", ["sub"] = "guid" };
        Assert.Equal("erik@example.com", DarlingWebOidc.ResolveSubject(payload, "preferred_username"));
        Assert.Null(DarlingWebOidc.ResolveSubject(payload, "upn"));
    }

    /* ---- ROLE CLAIM EXTRACTION + MAPPING ---- */

    [Fact]
    public void ExtractClaimValues_StringAndArrayShapesBothFlatten()
    {
        Assert.Equal(new[] { "dba" }, DarlingWebOidc.ExtractClaimValues(new JsonObject { ["groups"] = "dba" }, "groups"));
        Assert.Equal(
            new[] { "dba", "eng" },
            DarlingWebOidc.ExtractClaimValues(new JsonObject { ["groups"] = new JsonArray("dba", "eng") }, "groups"));
        Assert.Empty(DarlingWebOidc.ExtractClaimValues(new JsonObject(), "groups"));
        /* Non-strings are ignored, not stringified — a guessed format is a mapping that half-works. */
        Assert.Equal(
            new[] { "dba" },
            DarlingWebOidc.ExtractClaimValues(new JsonObject { ["groups"] = new JsonArray("dba", 42, true) }, "groups"));
    }

    [Theory]
    [InlineData(new string[0], new string[0], new string[0], "Admin")]        // no mapping -> the shared token's reach
    [InlineData(new[] { "dba" }, new[] { "dba" }, new string[0], "Admin")]    // admin match
    [InlineData(new[] { "eng" }, new[] { "dba" }, new[] { "eng" }, "Viewer")] // viewer match
    [InlineData(new[] { "dba", "eng" }, new[] { "dba" }, new[] { "eng" }, "Admin")] // admin wins over viewer
    [InlineData(new[] { "other" }, new[] { "dba" }, new[] { "eng" }, "Denied")]     // matches neither -> refused
    [InlineData(new string[0], new[] { "dba" }, new[] { "eng" }, "Denied")]         // no claims at all -> refused
    [InlineData(new[] { "DBA" }, new[] { "dba" }, new string[0], "Denied")]         // ORDINAL: case matters (identifiers)
    [InlineData(new[] { "eng" }, new string[0], new[] { "eng" }, "Viewer")]         // viewer-only mapping works alone
    public void MapRole_Matrix(string[] claims, string[] admin, string[] viewer, string expected)
        => Assert.Equal(expected, DarlingWebOidc.MapRole(claims, admin, viewer).ToString());

    /* ---- THE SEAT CODEC (the role's round-trip through the session cookie's subject slot) ---- */

    /* The role enum is internal, so the rows carry admin-ness as a bool (the same convention as the verdict
       matrix in DarlingWebAuthTests, which compares enum NAMES for the same reason). */
    [Theory]
    [InlineData("erik@example.com", true)]
    [InlineData("reader@example.com", false)]
    public void SeatCodec_RoundTrips(string subject, bool admin)
    {
        var encoded = DarlingWebSeat.EncodeCookieSubject(subject, admin ? WebOidcRole.Admin : WebOidcRole.Viewer);
        Assert.True(DarlingWebSeat.TryResolveSeat(encoded, out var seat));
        Assert.Equal(subject, seat.Subject);
        Assert.Equal(admin, seat.CanEdit);
    }

    /// <summary>A subject containing the codec's own separator must not shift the parse.</summary>
    [Fact]
    public void SeatCodec_SubjectWithColons_RoundTrips()
    {
        var encoded = DarlingWebSeat.EncodeCookieSubject("o1:v:sneaky@example.com", WebOidcRole.Admin);
        Assert.True(DarlingWebSeat.TryResolveSeat(encoded, out var seat));
        Assert.Equal("o1:v:sneaky@example.com", seat.Subject);
        Assert.True(seat.CanEdit);
    }

    [Fact]
    public void SeatCodec_NullSubject_IsTheSharedTokenSeat()
    {
        Assert.True(DarlingWebSeat.TryResolveSeat(null, out var seat));
        Assert.Same(DarlingWebSeat.SharedToken, seat);
        Assert.Null(seat.Subject);
        Assert.True(seat.CanEdit);
    }

    /// <summary>An unrecognized subject slot is REFUSED (re-authenticate), never guessed into a seat —
    /// serving an identity we cannot read would put an unnamed principal behind the write paths.</summary>
    [Theory]
    [InlineData("bare-subject-no-prefix")]
    [InlineData("o1:x:unknown-role")]
    [InlineData("o1:a:")]  // empty subject
    [InlineData("o2:a:future-version")]
    public void SeatCodec_UnrecognizedShape_Refuses(string cookieSubject)
        => Assert.False(DarlingWebSeat.TryResolveSeat(cookieSubject, out _));

    [Fact]
    public void SeatCodec_DeniedRole_CannotBeEncoded()
        => Assert.Throws<ArgumentOutOfRangeException>(() => DarlingWebSeat.EncodeCookieSubject("x", WebOidcRole.Denied));

    /// <summary>
    /// The read half of the middleware's seam. The auth middleware WRITES the seat into HttpContext.Items;
    /// this is what every consumer reads it back with, and the absent-item default is load-bearing: a
    /// loopback-only dashboard registers no auth middleware at all, so "no item" has to mean the
    /// shared-token seat with today's full reach, never a denied one.
    /// </summary>
    [Fact]
    public void Seat_FromContext_ReadsWhatTheMiddlewareWrote_AndDefaultsToSharedToken()
    {
        var empty = new DefaultHttpContext();
        Assert.Same(DarlingWebSeat.SharedToken, DarlingWebSeat.FromContext(empty));

        var viewer = new DarlingWebSeat("rita@example.com", CanEdit: false);
        var carrying = new DefaultHttpContext();
        carrying.Items[DarlingWebSeat.HttpContextItemKey] = viewer;
        Assert.Same(viewer, DarlingWebSeat.FromContext(carrying));

        /* A foreign value under our key is not a seat — fail closed to the documented default rather than
           throwing inside the request pipeline. */
        var junk = new DefaultHttpContext();
        junk.Items[DarlingWebSeat.HttpContextItemKey] = "not-a-seat";
        Assert.Same(DarlingWebSeat.SharedToken, DarlingWebSeat.FromContext(junk));
    }

    [Fact]
    public void Seat_EditorPrincipal_SubjectOrTheWebConstant()
    {
        Assert.Equal("erik@example.com", new DarlingWebSeat("erik@example.com", true).EditorPrincipal);
        Assert.Equal(DarlingWebEndpoints.WebEditorPrincipal, DarlingWebSeat.SharedToken.EditorPrincipal);
    }

    /* ---- THE GROUP-LEVEL WRITE GATE ---- */

    [Theory]
    [InlineData(true, "POST", "/api/views", true)]              // edit seat: anything
    [InlineData(true, "DELETE", "/api/views/3", true)]
    [InlineData(false, "GET", "/api/fleet", true)]              // viewer: the whole read surface
    [InlineData(false, "HEAD", "/api/fleet", true)]
    [InlineData(false, "OPTIONS", "/api/views", true)]
    [InlineData(false, "POST", "/api/compose/run", true)]       // the ONE unsafe-method read (panel preview/run)
    [InlineData(false, "POST", "/api/views", false)]            // viewer: no create
    [InlineData(false, "PUT", "/api/views/3", false)]           // no update
    [InlineData(false, "DELETE", "/api/views/3", false)]        // no delete
    [InlineData(false, "POST", "/api/anything/new", false)]     // a write endpoint added TOMORROW is born gated
    [InlineData(false, "PATCH", "/api/compose/run", false)]     // the compose exception is POST-only
    public void IsRequestAllowed_Matrix(bool canEdit, string method, string path, bool expected)
        => Assert.Equal(expected, DarlingWebSeat.IsRequestAllowed(new DarlingWebSeat("who", canEdit), method, path));

    /* ---- THE TRANSACTION COOKIE ---- */

    private static readonly byte[] SessionKey = RandomNumberGenerator.GetBytes(32);

    private static DarlingWebOidc.OidcTransaction Transaction() => new(
        State: "state-1", Nonce: "nonce-1", CodeVerifier: "verifier-1",
        RedirectUri: "https://192.168.1.205:5153/auth/oidc/callback", ReturnPath: "/deadlocks?server=x");

    [Fact]
    public void TransactionCookie_RoundTrips()
    {
        var key = DarlingWebOidc.DeriveTransactionKey(SessionKey);
        var now = DateTimeOffset.UtcNow;
        var cookie = DarlingWebOidc.BuildTransactionCookie(key, Transaction(), now.AddMinutes(10));

        Assert.True(DarlingWebOidc.TryValidateTransactionCookie(cookie, key, now, out var roundTripped));
        Assert.Equal(Transaction(), roundTripped);
    }

    [Fact]
    public void TransactionCookie_ExpiredOrTampered_Fails()
    {
        var key = DarlingWebOidc.DeriveTransactionKey(SessionKey);
        var now = DateTimeOffset.UtcNow;

        var expired = DarlingWebOidc.BuildTransactionCookie(key, Transaction(), now.AddMinutes(-1));
        Assert.False(DarlingWebOidc.TryValidateTransactionCookie(expired, key, now, out _));

        var cookie = DarlingWebOidc.BuildTransactionCookie(key, Transaction(), now.AddMinutes(10));
        var dot = cookie.IndexOf('.', StringComparison.Ordinal);
        /* First payload char after the expiry dot is fully data-bearing (same reasoning as the session
           cookie's tamper test). */
        var tampered = cookie[..(dot + 1)] + (cookie[dot + 1] == 'A' ? 'B' : 'A') + cookie[(dot + 2)..];
        Assert.False(DarlingWebOidc.TryValidateTransactionCookie(tampered, key, now, out _));

        var wrongKey = DarlingWebOidc.DeriveTransactionKey(RandomNumberGenerator.GetBytes(32));
        Assert.False(DarlingWebOidc.TryValidateTransactionCookie(cookie, wrongKey, now, out _));
    }

    /// <summary>
    /// THE reason the transaction key is DERIVED instead of reused: both cookie shapes are
    /// <c>{payload}.{base64url(HMAC)}</c>, and the transaction cookie is minted for any unauthenticated
    /// in-CIDR caller who hits /auth/oidc/login. Under one shared key, pasting the transaction value into
    /// the SESSION cookie slot would validate as an authenticated session (three segments, valid HMAC, the
    /// transaction JSON as the subject). This pins the real construction end-to-end: a freshly minted
    /// transaction cookie presented AS a session cookie does not validate.
    /// </summary>
    [Fact]
    public void TransactionCookie_PresentedAsSessionCookie_DoesNotValidate()
    {
        var transactionKey = DarlingWebOidc.DeriveTransactionKey(SessionKey);
        var now = DateTimeOffset.UtcNow;
        var transactionCookie = DarlingWebOidc.BuildTransactionCookie(transactionKey, Transaction(), now.AddMinutes(10));

        Assert.False(DarlingWebHostService.TryValidateSessionCookie(transactionCookie, SessionKey, now, out _));
    }

    [Fact]
    public void DeriveTransactionKey_IsDeterministicPerSessionKey_AndNotTheSessionKey()
    {
        var derived = DarlingWebOidc.DeriveTransactionKey(SessionKey);
        Assert.Equal(derived, DarlingWebOidc.DeriveTransactionKey(SessionKey));
        Assert.NotEqual(SessionKey, derived);
    }

    /* ---- DISCOVERY: the issuer must BE the authority (OIDC Discovery §4.3, review catch on #2730) ----
       Without this rule, whatever answers the well-known URL could name any issuer it likes, and the
       ID-token 'iss' check would validate against the attacker's own claim. */

    [Theory]
    [InlineData("https://idp.example.com/realm", "https://idp.example.com/realm", true)]
    [InlineData("https://idp.example.com/realm/", "https://idp.example.com/realm", true)]  // trailing slash tolerated…
    [InlineData("https://idp.example.com/realm", "https://idp.example.com/realm/", true)]  // …in either direction
    [InlineData("https://evil.example.com", "https://idp.example.com/realm", false)]       // a substituted issuer is refused
    [InlineData("https://IDP.example.com/realm", "https://idp.example.com/realm", false)]  // ordinal: issuers are compared as identifiers
    [InlineData("https://idp.example.com/other", "https://idp.example.com/realm", false)]
    public void IssuerMatchesAuthority_Matrix(string issuer, string authority, bool expected)
        => Assert.Equal(expected, DarlingWebOidc.IssuerMatchesAuthority(issuer, authority));

    /// <summary>
    /// The discovery document's endpoints are held to the authority's own scheme rule (review catch on
    /// #2730): the token endpoint receives the client secret and the code, so a discovery answer naming an
    /// http:// endpoint must not be able to point that POST at cleartext — the loopback-only-http exception
    /// the operator's config is held to applies to the document too.
    /// </summary>
    [Theory]
    [InlineData("https://idp.example.com/token", true)]
    [InlineData("http://localhost:8080/default/token", true)]   // the local test-IdP exception
    [InlineData("http://127.0.0.1:8080/token", true)]
    [InlineData("http://idp.corp.local/token", false)]          // cleartext to a real host -> refused
    [InlineData("ftp://idp.example.com/token", false)]
    [InlineData("not-a-url", false)]
    [InlineData("/relative/token", false)]
    public void IsAcceptableEndpointUrl_Matrix(string url, bool expected)
        => Assert.Equal(expected, DarlingWebOidc.IsAcceptableEndpointUrl(url));

    /// <summary>A subject with control characters in it could forge lines in the audit trail these
    /// sign-ins feed (review catch on #2730); the sign-in refuses it, the same decision as the length
    /// cap \u2014 refusing is recoverable, laundering-then-attributing is not.</summary>
    [Theory]
    [InlineData("erik@example.com", false)]
    [InlineData("yasm\u00edn.o'brien@example.com", false)]  // non-ASCII is fine \u2014 only CONTROLS are hostile
    [InlineData("evil\r\nOIDC sign-in: admin@example.com as admin", true)]
    [InlineData("tab\tseparated", true)]
    [InlineData("nul\u0000byte", true)]
    [InlineData("del\u007fchar", true)]
    public void SubjectCarriesControlCharacters_Matrix(string subject, bool expected)
        => Assert.Equal(expected, DarlingWebOidc.SubjectCarriesControlCharacters(subject));
}
