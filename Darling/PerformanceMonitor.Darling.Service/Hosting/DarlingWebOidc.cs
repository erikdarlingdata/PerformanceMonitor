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
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// The seat an OIDC sign-in maps to (#2550). <see cref="Denied"/> is a real outcome, not an error state: a
/// user the IdP authenticated but the role mapping does not know gets NOTHING — that refusal is per-user
/// revocation working, and it must be distinguishable from a viewer seat (which gets the whole read surface).
/// </summary>
internal enum WebOidcRole { Admin, Viewer, Denied }

/// <summary>
/// The web dashboard's OIDC sign-in (#2550): authorization code + PKCE, resolved via the provider's discovery
/// document, mapping IdP role/group claims onto the dashboard's two seats (edit / read-only). Everything in
/// this static class is PURE — no clock it doesn't take, no I/O — so the whole protocol surface pins in unit
/// tests without a provider; <see cref="DarlingWebOidcClient"/> below owns the two network touches (discovery,
/// code exchange).
///
/// <para><b>Why the ID token's signature is not verified here, and why that is sound for THIS flow.</b> The
/// ID token is received by the confidential client DIRECTLY from the token endpoint over the TLS channel the
/// client itself opened — never through the browser. OIDC Core §3.1.3.7 rule 6 says exactly this case: "If the
/// ID Token is received via direct communication between the Client and the Token Endpoint … the TLS server
/// validation MAY be used to validate the issuer in place of checking the token signature." What MUST still be
/// checked — and is, in <see cref="ValidateIdTokenClaims"/> — is iss, aud (+azp on a multi-audience token),
/// exp, and the nonce binding the token to the transaction this host started. The alternative was the full
/// Microsoft.IdentityModel dependency tree for a signature check the spec makes redundant on this channel;
/// anyone extending this to validate BROWSER-delivered tokens (implicit/hybrid flow, a bearer JWT on the API)
/// must NOT reuse this parse — that channel requires the signature.</para>
/// </summary>
internal static class DarlingWebOidc
{
    /// <summary>Scopes used when the config names none. <c>openid</c> is what makes it OIDC (no ID token
    /// without it); profile/email are what make the subject a NAME instead of an opaque GUID.</summary>
    internal const string DefaultScopes = "openid profile email";

    /// <summary>The subject-claim fallback chain when <c>subjectClaim</c> is unset, in order. Chosen for
    /// attribution: <c>updated_by</c> stamped with an opaque <c>sub</c> GUID answers "who changed this
    /// dashboard" no better than the <c>web</c> constant did. The <c>email</c> step is additionally gated on
    /// <c>email_verified</c> — see <see cref="ResolveSubject"/>.</summary>
    internal static readonly string[] DefaultSubjectClaims = { "preferred_username", "email", "sub" };

    /// <summary>Clock-skew allowance on exp/nbf. IdP and host clocks genuinely drift; five minutes is the
    /// industry convention (it is Microsoft.IdentityModel's default too).</summary>
    internal static readonly TimeSpan ClockSkew = TimeSpan.FromMinutes(5);

    /* ---------------------------------------------------------------------------------------------------
       Config validation — pure, so the whole matrix pins without a host. A problem string, or null when the
       block is usable. The caller treats a problem as "OIDC disabled, token auth unchanged" (Critical log):
       unlike a TLS problem this is NOT a fail-closed-to-loopback matter, because OIDC is additive — the
       dashboard without it is exactly the dashboard that shipped yesterday, still behind the token gate.
       --------------------------------------------------------------------------------------------------- */

    internal static string? ValidateConfig(WebOidcConfig config)
    {
        if (string.IsNullOrWhiteSpace(config.Authority))
        {
            return "web.network.oidc.authority is required (the provider's issuer URL)";
        }

        if (!Uri.TryCreate(config.Authority.Trim(), UriKind.Absolute, out var authority))
        {
            return $"web.network.oidc.authority '{config.Authority}' is not an absolute URL";
        }

        if (authority.Scheme == Uri.UriSchemeHttp)
        {
            /* http:// is allowed ONLY toward loopback — a local test IdP in a container. Toward anything
               else the code exchange (carrying the client secret and returning the tokens) would cross the
               segment in the clear, which is the web dashboard's own TLS story re-broken one hop over. */
            if (!authority.IsLoopback)
            {
                return $"web.network.oidc.authority '{config.Authority}' is http:// to a non-loopback host — the code exchange would cross the wire in the clear; use https://";
            }
        }
        else if (authority.Scheme != Uri.UriSchemeHttps)
        {
            return $"web.network.oidc.authority '{config.Authority}' must be an https:// URL";
        }

        if (string.IsNullOrWhiteSpace(config.ClientId))
        {
            return "web.network.oidc.clientId is required";
        }

        var hasRoleClaim = !string.IsNullOrWhiteSpace(config.RoleClaim);
        var hasRoleLists = config.AdminRoles is { Length: > 0 } || config.ViewerRoles is { Length: > 0 };
        if (hasRoleClaim && !hasRoleLists)
        {
            /* Half a mapping is worse than none: roleClaim alone reads as "roles are being checked" while
               every sign-in would fall through to Denied (no list can match), locking everyone out — or to
               Admin, granting everyone edit — and either silent resolution betrays what the operator wrote. */
            return "web.network.oidc.roleClaim is set but neither adminRoles nor viewerRoles names any value — set at least one list, or remove roleClaim";
        }

        if (!hasRoleClaim && hasRoleLists)
        {
            return "web.network.oidc.adminRoles/viewerRoles are set but roleClaim names no claim to read them from — set roleClaim, or remove the lists";
        }

        return null;
    }

    /// <summary>The effective scope string: the configured set with <c>openid</c> guaranteed present (an
    /// OIDC request without it gets no ID token and the sign-in cannot complete — silently, on some IdPs).</summary>
    internal static string EffectiveScopes(string? configured)
    {
        if (string.IsNullOrWhiteSpace(configured))
        {
            return DefaultScopes;
        }

        var scopes = configured.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return Array.Exists(scopes, s => string.Equals(s, "openid", StringComparison.Ordinal))
            ? string.Join(' ', scopes)
            : "openid " + string.Join(' ', scopes);
    }

    /* ---------------------------------------------------------------------------------------------------
       PKCE (RFC 7636) — S256 only. "plain" exists in the RFC for clients that cannot hash; this one can.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>A fresh (verifier, challenge) pair: 32 random bytes base64url'd = 43 chars, comfortably
    /// inside the RFC's 43–128 window and all from its unreserved alphabet by construction.</summary>
    internal static (string Verifier, string Challenge) CreatePkce()
    {
        var verifier = Base64UrlEncode(RandomNumberGenerator.GetBytes(32));
        return (verifier, ComputeCodeChallenge(verifier));
    }

    /// <summary>S256: base64url(SHA256(ASCII(verifier))). Split out so a test can pin the transform against
    /// RFC 7636's own appendix vector.</summary>
    internal static string ComputeCodeChallenge(string verifier)
        => Base64UrlEncode(SHA256.HashData(Encoding.ASCII.GetBytes(verifier)));

    /* ---------------------------------------------------------------------------------------------------
       The authorization request.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>Builds the authorization-endpoint redirect URL. Every value is escaped; honors a '?' the
    /// endpoint may already carry (some IdPs publish parameterized authorization endpoints).</summary>
    internal static string BuildAuthorizationUrl(
        string authorizationEndpoint,
        string clientId,
        string redirectUri,
        string scopes,
        string state,
        string nonce,
        string codeChallenge)
    {
        var separator = authorizationEndpoint.Contains('?') ? '&' : '?';
        return authorizationEndpoint
            + separator
            + "response_type=code"
            + "&client_id=" + Uri.EscapeDataString(clientId)
            + "&redirect_uri=" + Uri.EscapeDataString(redirectUri)
            + "&scope=" + Uri.EscapeDataString(scopes)
            + "&state=" + Uri.EscapeDataString(state)
            + "&nonce=" + Uri.EscapeDataString(nonce)
            + "&code_challenge=" + Uri.EscapeDataString(codeChallenge)
            + "&code_challenge_method=S256";
    }

    /* ---------------------------------------------------------------------------------------------------
       ID-token claim validation. See the class doc for why the signature is not checked on this channel.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>Extracts a JWT's payload as a JSON object, or null for anything that is not a three-segment
    /// token with a base64url JSON-object middle. Parsing only — the claims mean nothing until
    /// <see cref="ValidateIdTokenClaims"/> has passed.</summary>
    internal static JsonObject? TryParseJwtPayload(string jwt)
    {
        if (string.IsNullOrEmpty(jwt))
        {
            return null;
        }

        var segments = jwt.Split('.');
        if (segments.Length != 3)
        {
            return null;
        }

        try
        {
            var payload = Base64UrlDecode(segments[1]);
            return JsonNode.Parse(payload) as JsonObject;
        }
        catch (FormatException)
        {
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// The OIDC Core §3.1.3.7 checks that remain mandatory on the direct token-endpoint channel: issuer,
    /// audience (with the azp rule on a multi-audience token), expiry (with <see cref="ClockSkew"/>), and the
    /// nonce binding this token to the transaction this host started (which is what stops a code minted for
    /// some OTHER sign-in being redeemed against this one). Returns the problem, or null when valid.
    /// </summary>
    internal static string? ValidateIdTokenClaims(
        JsonObject payload, string expectedIssuer, string clientId, string expectedNonce, DateTimeOffset now)
    {
        var iss = StringClaim(payload, "iss");
        if (!string.Equals(iss, expectedIssuer, StringComparison.Ordinal))
        {
            return $"issuer mismatch (token 'iss' does not equal the discovery document's issuer '{expectedIssuer}')";
        }

        /* aud is a string or an array. A single audience must BE this client; a multi-audience token must
           CONTAIN it and name it as the authorized party — a token minted for several clients with no azp
           does not say it was minted FOR this one. */
        var aud = payload["aud"];
        switch (aud)
        {
            case JsonValue single when single.TryGetValue<string>(out var audValue):
                if (!string.Equals(audValue, clientId, StringComparison.Ordinal))
                {
                    return "audience mismatch (token 'aud' is not this clientId)";
                }

                break;

            case JsonArray many:
                var found = false;
                foreach (var entry in many)
                {
                    if (entry is JsonValue v && v.TryGetValue<string>(out var s) && string.Equals(s, clientId, StringComparison.Ordinal))
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    return "audience mismatch (token 'aud' array does not contain this clientId)";
                }

                if (many.Count > 1)
                {
                    var azp = StringClaim(payload, "azp");
                    if (!string.Equals(azp, clientId, StringComparison.Ordinal))
                    {
                        return "multi-audience token without 'azp' naming this clientId";
                    }
                }

                break;

            default:
                return "token carries no 'aud' claim";
        }

        if (payload["exp"] is not JsonValue expValue || !TryGetUnixSeconds(expValue, out var exp))
        {
            return "token carries no usable 'exp' claim";
        }

        if (now > DateTimeOffset.FromUnixTimeSeconds(exp) + ClockSkew)
        {
            return "token is expired";
        }

        if (payload["nbf"] is JsonValue nbfValue && TryGetUnixSeconds(nbfValue, out var nbf)
            && now + ClockSkew < DateTimeOffset.FromUnixTimeSeconds(nbf))
        {
            return "token is not yet valid ('nbf' is in the future)";
        }

        var nonce = StringClaim(payload, "nonce");
        if (string.IsNullOrEmpty(nonce) || !string.Equals(nonce, expectedNonce, StringComparison.Ordinal))
        {
            return "nonce mismatch (the token is not bound to this sign-in transaction)";
        }

        return null;
    }

    /// <summary>A claim as a string, or null when absent or not a JSON string — validation must never THROW
    /// on a malformed token, only refuse it.</summary>
    private static string? StringClaim(JsonObject payload, string claim)
        => payload[claim] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;

    private static bool TryGetUnixSeconds(JsonValue value, out long seconds)
    {
        if (value.TryGetValue<long>(out seconds))
        {
            return true;
        }

        /* Some providers serialize numeric-date claims as JSON floats. */
        if (value.TryGetValue<double>(out var d))
        {
            seconds = (long)d;
            return true;
        }

        seconds = 0;
        return false;
    }

    /* ---------------------------------------------------------------------------------------------------
       Subject + role mapping.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// The seat's identity: the configured claim when <paramref name="subjectClaim"/> is set (REQUIRED then —
    /// a token without it returns null and the caller refuses, rather than silently stamping something the
    /// operator didn't ask for), else the first usable of <see cref="DefaultSubjectClaims"/>.
    ///
    /// <para><b>The default chain's <c>email</c> step requires <c>email_verified</c> to be true</b> (review
    /// catch on #2730): an IdP that lets users self-assert an address would otherwise let an
    /// authenticated-but-unverified user present a real operator's email and be STAMPED as them in
    /// <c>updated_by</c> and the audit log — identity theft by claim, one directory setting away. An
    /// unverified email falls through to the opaque-but-honest <c>sub</c>. An operator who EXPLICITLY sets
    /// <c>subjectClaim: "email"</c> is stating they trust their directory's addresses, and that explicit
    /// choice is honored without the gate.</para>
    /// </summary>
    internal static string? ResolveSubject(JsonObject payload, string? subjectClaim)
    {
        if (!string.IsNullOrWhiteSpace(subjectClaim))
        {
            return NonEmptyStringClaim(payload, subjectClaim.Trim());
        }

        var preferredUsername = NonEmptyStringClaim(payload, "preferred_username");
        if (preferredUsername is not null)
        {
            return preferredUsername;
        }

        var email = NonEmptyStringClaim(payload, "email");
        if (email is not null && IsEmailVerified(payload))
        {
            return email;
        }

        return NonEmptyStringClaim(payload, "sub");
    }

    /// <summary>OIDC's <c>email_verified</c> is a JSON boolean, but real providers have shipped it as the
    /// string "true"; both are accepted. Absent or anything else = not verified.</summary>
    private static bool IsEmailVerified(JsonObject payload)
        => payload["email_verified"] is JsonValue value
           && ((value.TryGetValue<bool>(out var b) && b)
               || (value.TryGetValue<string>(out var s) && string.Equals(s, "true", StringComparison.OrdinalIgnoreCase)));

    private static string? NonEmptyStringClaim(JsonObject payload, string claim)
        => payload[claim] is JsonValue value && value.TryGetValue<string>(out var s) && !string.IsNullOrWhiteSpace(s)
            ? s
            : null;

    /// <summary>The values of a role/group claim — a bare string and an array-of-strings both flatten to a
    /// list (Entra sends arrays; some IdPs send a lone string for a single membership). Non-strings are
    /// ignored rather than stringified: a numeric group id the operator didn't quote in config can't match a
    /// quoted one anyway, and guessing formats is how mappings half-work.</summary>
    internal static IReadOnlyList<string> ExtractClaimValues(JsonObject payload, string claimName)
    {
        switch (payload[claimName])
        {
            case JsonValue value when value.TryGetValue<string>(out var s):
                return new[] { s };

            case JsonArray array:
                var values = new List<string>(array.Count);
                foreach (var entry in array)
                {
                    if (entry is JsonValue v && v.TryGetValue<string>(out var s))
                    {
                        values.Add(s);
                    }
                }

                return values;

            default:
                return Array.Empty<string>();
        }
    }

    /// <summary>
    /// The role decision. No mapping configured ⇒ <see cref="WebOidcRole.Admin"/> — parity with the shared
    /// token's reach, not a new privilege (ValidateConfig already refused half-configured mappings, so "no
    /// mapping" here really means the operator wrote none). With a mapping: admin wins over viewer when a
    /// user carries both; matching neither is <see cref="WebOidcRole.Denied"/> — fail closed, because "the
    /// IdP knows you" and "this dashboard grants you a seat" are different facts. Ordinal, case-sensitive:
    /// role values are identifiers.
    /// </summary>
    internal static WebOidcRole MapRole(
        IReadOnlyList<string> claimValues, IReadOnlyList<string>? adminRoles, IReadOnlyList<string>? viewerRoles)
    {
        var hasAdmin = adminRoles is { Count: > 0 };
        var hasViewer = viewerRoles is { Count: > 0 };
        if (!hasAdmin && !hasViewer)
        {
            return WebOidcRole.Admin;
        }

        if (hasAdmin && Intersects(claimValues, adminRoles!))
        {
            return WebOidcRole.Admin;
        }

        if (hasViewer && Intersects(claimValues, viewerRoles!))
        {
            return WebOidcRole.Viewer;
        }

        return WebOidcRole.Denied;
    }

    private static bool Intersects(IReadOnlyList<string> values, IReadOnlyList<string> against)
    {
        foreach (var value in values)
        {
            foreach (var candidate in against)
            {
                if (string.Equals(value, candidate, StringComparison.Ordinal))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /* ---------------------------------------------------------------------------------------------------
       The sign-in transaction — state, nonce, PKCE verifier, and where to land afterwards — carried across
       the IdP round-trip in a SIGNED cookie rather than server memory: stateless, naturally bound to the
       browser that started the sign-in, and immune to an unauthenticated /auth/oidc/login being used to
       fill a server-side table.
       --------------------------------------------------------------------------------------------------- */

    internal const string TransactionCookieName = "darling_web_oidc_txn";
    internal static readonly TimeSpan TransactionLifetime = TimeSpan.FromMinutes(10);

    /// <summary>One in-flight sign-in. <see cref="RedirectUri"/> rides along because the token endpoint
    /// requires the EXACT redirect_uri the authorization request used — and this host answers on several
    /// names at once (localhost, the listen IP, publicHost), so re-deriving it from the CALLBACK request's
    /// Host would break any sign-in where the two differ.</summary>
    internal sealed record OidcTransaction(string State, string Nonce, string CodeVerifier, string RedirectUri, string ReturnPath);

    /// <summary>
    /// Derives the transaction-cookie signing key from the session signing key, and the derivation is
    /// LOAD-BEARING, not hygiene: both cookie shapes are <c>{payload}.{base64url(HMAC)}</c>, and the
    /// transaction cookie is minted for ANY unauthenticated in-CIDR caller who hits /auth/oidc/login. Under
    /// one shared key, that caller could paste the transaction value into the SESSION cookie slot — three
    /// segments, valid HMAC — and the transaction JSON would validate as an authenticated session with the
    /// JSON as its subject. A pinned test presents a transaction cookie to the session validator to hold
    /// this closed.
    /// </summary>
    internal static byte[] DeriveTransactionKey(byte[] sessionSigningKey)
        => HMACSHA256.HashData(sessionSigningKey, "darling-web-oidc-transaction-v1"u8);

    /// <summary>Builds the signed transaction cookie: <c>{expiryUnix}.{base64url(json)}.{base64url(HMAC)}</c>,
    /// the HMAC covering everything before the final dot (the #2583 rule: never sign less than you carry).</summary>
    internal static string BuildTransactionCookie(byte[] transactionKey, OidcTransaction transaction, DateTimeOffset expiry)
    {
        var json = JsonSerializer.Serialize(transaction);
        var payload = expiry.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)
            + "." + Base64UrlEncode(Encoding.UTF8.GetBytes(json));
        var signature = HMACSHA256.HashData(transactionKey, Encoding.ASCII.GetBytes(payload));
        return payload + "." + Base64UrlEncode(signature);
    }

    /// <summary>Validates and decodes a transaction cookie: false on a malformed/expired/tampered value.
    /// The JSON is decoded only AFTER the HMAC verifies, mirroring the session cookie's rule.</summary>
    internal static bool TryValidateTransactionCookie(
        string? cookieValue, byte[] transactionKey, DateTimeOffset now, out OidcTransaction? transaction)
    {
        transaction = null;

        if (string.IsNullOrEmpty(cookieValue))
        {
            return false;
        }

        var segments = cookieValue.Split('.');
        if (segments.Length != 3 || segments[0].Length == 0 || segments[1].Length == 0 || segments[2].Length == 0)
        {
            return false;
        }

        if (!long.TryParse(segments[0], NumberStyles.None, CultureInfo.InvariantCulture, out var expiryUnix))
        {
            return false;
        }

        if (now.ToUnixTimeSeconds() > expiryUnix)
        {
            return false;
        }

        byte[] presented;
        try
        {
            presented = Base64UrlDecode(segments[2]);
        }
        catch (FormatException)
        {
            return false;
        }

        var payload = segments[0] + "." + segments[1];
        var expected = HMACSHA256.HashData(transactionKey, Encoding.ASCII.GetBytes(payload));
        if (!CryptographicOperations.FixedTimeEquals(presented, expected))
        {
            return false;
        }

        try
        {
            transaction = JsonSerializer.Deserialize<OidcTransaction>(Base64UrlDecode(segments[1]));
        }
        catch (FormatException)
        {
            return false;
        }
        catch (JsonException)
        {
            return false;
        }

        return transaction is not null;
    }

    /// <summary>A fresh state/nonce value: 32 random bytes, base64url.</summary>
    internal static string CreateFlowValue() => Base64UrlEncode(RandomNumberGenerator.GetBytes(32));

    /// <summary>
    /// OIDC Discovery 1.0 §4.3: the discovery document's <c>issuer</c> MUST be identical to the URL the
    /// configuration was retrieved from (review catch on #2730) — without this check, whatever answers the
    /// well-known URL could name ANY issuer and the ID-token <c>iss</c> comparison would dutifully validate
    /// against the attacker's own claim, turning the one cross-input check into a self-affirmation. Ordinal
    /// except for a tolerated trailing slash, which real providers genuinely disagree on.
    /// </summary>
    internal static bool IssuerMatchesAuthority(string issuer, string authority)
        => string.Equals(issuer.TrimEnd('/'), authority.TrimEnd('/'), StringComparison.Ordinal);

    /// <summary>
    /// The scheme rule <see cref="ValidateConfig"/> enforces on the authority, applied to a URL the
    /// DISCOVERY DOCUMENT supplied (review catch on #2730): https, or http toward a loopback host. The
    /// authorization endpoint is where the browser gets sent; the token endpoint is where the CLIENT SECRET
    /// and the authorization code get POSTed — a discovery answer naming an http:// token endpoint would
    /// quietly bypass the loopback-only-http restriction the operator's own config is held to, and ship the
    /// secret in the clear.
    /// </summary>
    internal static bool IsAcceptableEndpointUrl(string url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            return false;
        }

        if (uri.Scheme == Uri.UriSchemeHttps)
        {
            return true;
        }

        return uri.Scheme == Uri.UriSchemeHttp && uri.IsLoopback;
    }

    internal static string Base64UrlEncode(byte[] bytes)
        => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    internal static byte[] Base64UrlDecode(string value)
    {
        var b64 = value.Replace('-', '+').Replace('_', '/');
        b64 = (b64.Length % 4) switch
        {
            2 => b64 + "==",
            3 => b64 + "=",
            _ => b64,
        };
        return Convert.FromBase64String(b64);
    }
}

/// <summary>
/// The two network touches of the sign-in — discovery and the code exchange — plus the resolved config they
/// run under. One instance per STARTED web server (created when the network block enables OIDC, disposed with
/// the listener), so the discovery document is fetched once per server lifetime, lazily on the first sign-in:
/// fetching at start would make the dashboard's availability depend on the IdP's, which is exactly backwards
/// for a monitoring tool.
/// </summary>
internal sealed class DarlingWebOidcClient : IDisposable
{
    /// <summary>Everything the middleware needs at request time, resolved ONCE at server start so a sign-in
    /// never re-reads config or re-decrypts the secret.</summary>
    internal sealed record ResolvedOptions(
        string Authority,
        string ClientId,
        string? ClientSecret,
        string Scopes,
        string? SubjectClaim,
        string? RoleClaim,
        IReadOnlyList<string> AdminRoles,
        IReadOnlyList<string> ViewerRoles);

    internal sealed record DiscoveryDocument(string Issuer, string AuthorizationEndpoint, string TokenEndpoint);

    /// <summary>The code exchange's outcome: an ID token, or the problem (already safe to log — no secret
    /// material is ever put in it).</summary>
    internal sealed record ExchangeResult(string? IdToken, string? Error);

    private readonly HttpClient _http;
    private readonly SemaphoreSlim _discoveryLock = new(1, 1);
    private DiscoveryDocument? _discovery;

    internal ResolvedOptions Options { get; }

    internal DarlingWebOidcClient(ResolvedOptions options, HttpMessageHandler? handler = null)
    {
        Options = options;
        _http = handler is null ? new HttpClient() : new HttpClient(handler);
        /* Short and deliberate: these calls sit inside an interactive sign-in; a hung IdP should read as
           "SSO unavailable, use the token" within seconds, not stall a browser for two minutes. */
        _http.Timeout = TimeSpan.FromSeconds(15);
    }

    /// <summary>The discovery document, fetched from <c>{authority}/.well-known/openid-configuration</c> on
    /// first use and cached for the server's lifetime. Null when the fetch/parse fails (the caller reports
    /// "SSO temporarily unavailable" and the NEXT sign-in retries — a transient IdP hiccup must not require a
    /// service restart, the #2677 lesson).</summary>
    internal async Task<DiscoveryDocument?> GetDiscoveryAsync(CancellationToken cancellationToken)
    {
        var cached = _discovery;
        if (cached is not null)
        {
            return cached;
        }

        await _discoveryLock.WaitAsync(cancellationToken);
        try
        {
            if (_discovery is not null)
            {
                return _discovery;
            }

            var url = Options.Authority.TrimEnd('/') + "/.well-known/openid-configuration";
            using var response = await _http.GetAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                return null;
            }

            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            if (JsonNode.Parse(body) is not JsonObject document)
            {
                return null;
            }

            var issuer = document["issuer"]?.GetValue<string>();
            var authorizationEndpoint = document["authorization_endpoint"]?.GetValue<string>();
            var tokenEndpoint = document["token_endpoint"]?.GetValue<string>();
            if (string.IsNullOrWhiteSpace(issuer)
                || string.IsNullOrWhiteSpace(authorizationEndpoint)
                || string.IsNullOrWhiteSpace(tokenEndpoint))
            {
                return null;
            }

            /* OIDC Discovery §4.3 (see IssuerMatchesAuthority): a document whose issuer is not the
               authority it was fetched from is refused outright — accepting it would let the discovery
               response choose which issuer the ID-token check validates against. Not cached: a poisoned
               answer must not stick for the server's lifetime. */
            if (!DarlingWebOidc.IssuerMatchesAuthority(issuer, Options.Authority))
            {
                return null;
            }

            /* And the endpoints the document names are held to the SAME scheme rule as the authority
               (see IsAcceptableEndpointUrl) — the token endpoint in particular receives the client secret
               and the code, and the discovery response must not be able to point that POST at cleartext. */
            if (!DarlingWebOidc.IsAcceptableEndpointUrl(authorizationEndpoint)
                || !DarlingWebOidc.IsAcceptableEndpointUrl(tokenEndpoint))
            {
                return null;
            }

            _discovery = new DiscoveryDocument(issuer, authorizationEndpoint, tokenEndpoint);
            return _discovery;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* The CALLER's cancellation propagates; an HttpClient TIMEOUT also surfaces as
               TaskCanceledException but with an un-tripped caller token, and falls through to the catch
               below — a hung IdP is "no document this attempt", not a crashed request. */
            throw;
        }
        catch (Exception)
        {
            /* HttpRequestException, JsonException, the timeout case above — all the same answer: no
               document this attempt, try again on the next sign-in. */
            return null;
        }
        finally
        {
            _discoveryLock.Release();
        }
    }

    /// <summary>
    /// Redeems the authorization code: <c>grant_type=authorization_code</c> with the PKCE verifier, the
    /// client secret going as <c>client_secret</c> POST-body auth (the most widely accepted placement) when
    /// one is configured, or nothing when running as a public client on PKCE alone.
    /// </summary>
    internal async Task<ExchangeResult> ExchangeCodeAsync(
        string tokenEndpoint, string code, string redirectUri, string codeVerifier, CancellationToken cancellationToken)
    {
        var form = new List<KeyValuePair<string, string>>
        {
            new("grant_type", "authorization_code"),
            new("code", code),
            new("redirect_uri", redirectUri),
            new("client_id", Options.ClientId),
            new("code_verifier", codeVerifier),
        };

        if (!string.IsNullOrEmpty(Options.ClientSecret))
        {
            form.Add(new("client_secret", Options.ClientSecret));
        }

        try
        {
            using var content = new FormUrlEncodedContent(form);
            using var response = await _http.PostAsync(tokenEndpoint, content, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);

            if (JsonNode.Parse(body) is not JsonObject document)
            {
                return new ExchangeResult(null, $"token endpoint answered HTTP {(int)response.StatusCode} with a non-JSON body");
            }

            if (!response.IsSuccessStatusCode)
            {
                /* error/error_description are the OAuth-standard fields; both are IdP-authored text, safe to
                   log (and worth logging verbatim — 'invalid_grant' vs 'invalid_client' is the whole
                   diagnosis). The raw body is NOT echoed beyond these named fields. */
                var error = document["error"]?.GetValue<string>() ?? $"HTTP {(int)response.StatusCode}";
                var description = document["error_description"]?.GetValue<string>();
                return new ExchangeResult(null, description is null ? error : $"{error}: {description}");
            }

            var idToken = document["id_token"]?.GetValue<string>();
            return string.IsNullOrEmpty(idToken)
                ? new ExchangeResult(null, "token endpoint's response carries no id_token (is the 'openid' scope enabled for this client?)")
                : new ExchangeResult(idToken, null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Same split as GetDiscoveryAsync: a caller abort propagates, a client TIMEOUT (also a
               TaskCanceledException) reports as an exchange failure. */
            throw;
        }
        catch (Exception ex)
        {
            return new ExchangeResult(null, ex.Message);
        }
    }

    public void Dispose()
    {
        _http.Dispose();
        _discoveryLock.Dispose();
    }
}
