/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Stamps the browser-facing headers on EVERY response the web host sends: the HTML shell, a static asset, an
/// API JSON reply, and every refusal or error page. It registers the stamp with <c>OnStarting</c>, so it
/// decides nothing about the request (no refusal, no read of the body) and it still lands on a response a gate
/// or the failure observer writes after this middleware has returned.
///
/// <para><b>The policy matches the single-page app as shipped.</b> The shell loads one module script and four
/// stylesheets from its own origin, talks only to its own origin (<c>fetch</c> to <c>/api/*</c>), and
/// builds every other piece of markup in script. The directives, and the one place each loosens the strictest
/// form, are:</para>
/// <list type="bullet">
/// <item><c>default-src 'self'</c>: nothing loads from another origin.</item>
/// <item><c>script-src 'self'</c>: no inline script, no <c>eval</c>. The pre-sign-in pages that must carry a
/// small inline script (the login form, the signed-in landing page) get a policy of their own through
/// <see cref="PolicyForInlinePage"/>: the same policy with the SHA-256 of exactly the inline blocks that page
/// holds, so a block is allowed by its text and nothing else inline is.</item>
/// <item><c>style-src 'self'</c> plus <c>style-src-attr 'unsafe-inline'</c>: style blocks and stylesheets are
/// same-origin only, but the page sets <c>style="..."</c> attributes on elements it builds (chart sizes, hidden
/// markers, colour swatches), and an inline style attribute cannot run script. Only the attribute form is
/// loosened.</item>
/// <item><c>img-src 'self' data:</c>: the notification-bell icon in <c>css/app.css</c> is a <c>data:</c> SVG
/// mask, and the chart image export draws its SVG from a <c>data:</c> URL.</item>
/// <item><c>connect-src 'self'</c>: the page fetches only its own <c>/api/*</c>.</item>
/// <item><c>object-src 'none'</c>, <c>base-uri 'none'</c>: no plugin content, no <c>&lt;base&gt;</c>.</item>
/// <item><c>form-action 'self'</c>: the login form submits to the same address.</item>
/// <item><c>frame-ancestors 'none'</c> (with <c>X-Frame-Options: DENY</c> for browsers that read only that
/// one): the dashboard is never shown inside another page.</item>
/// </list>
///
/// <para>A browser download made from a <c>blob:</c> link (plan, CSV and chart exports) is a navigation, which
/// none of these directives govern, so <c>blob:</c> is not listed.</para>
/// </summary>
internal sealed class DarlingWebSecurityHeaders
{
    internal const string ContentSecurityPolicyHeader = "Content-Security-Policy";

    internal const string ContentSecurityPolicy =
        "default-src 'self'; script-src 'self'; style-src 'self'; style-src-attr 'unsafe-inline'; "
        + "img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; "
        + "frame-ancestors 'none'";

    private static readonly Regex s_scriptBlock = new(
        @"<script\b[^>]*>(?<body>.*?)</script>", RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex s_styleBlock = new(
        @"<style\b[^>]*>(?<body>.*?)</style>", RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private readonly RequestDelegate _next;

    public DarlingWebSecurityHeaders(RequestDelegate next) => _next = next;

    public Task InvokeAsync(HttpContext context)
    {
        context.Response.OnStarting(static state =>
        {
            var headers = ((HttpContext)state).Response.Headers;

            /* A page that needs inline blocks sets its own policy before it writes (PolicyForInlinePage);
               every other response gets the default. */
            if (!headers.ContainsKey(ContentSecurityPolicyHeader))
            {
                headers[ContentSecurityPolicyHeader] = ContentSecurityPolicy;
            }

            headers.XFrameOptions = "DENY";
            headers.XContentTypeOptions = "nosniff";
            headers["Referrer-Policy"] = "no-referrer";
            return Task.CompletedTask;
        }, context);

        return _next(context);
    }

    /// <summary>
    /// The default policy with the SHA-256 of every inline <c>&lt;script&gt;</c> and <c>&lt;style&gt;</c> block
    /// in <paramref name="html"/> added to <c>script-src</c> and <c>style-src</c>. A browser compares the hash
    /// with the block's exact text, so the pages that build their own markup (the login form and the
    /// signed-in landing page, which render before the gated stylesheets and scripts are reachable) keep
    /// working while every other inline block stays blocked.
    ///
    /// <para>A browser hashes a block's text after its HTML parser has turned every line ending into LF, so the
    /// hash is taken over the LF form of the text, whatever line endings the source file was checked out with.
    /// Call this once per page, on constant text, and keep the result: the policy never depends on a value
    /// put into a particular response.</para>
    /// </summary>
    internal static string PolicyForInlinePage(string html)
    {
        ArgumentNullException.ThrowIfNull(html);
        var scriptHashes = HashBlocks(s_scriptBlock, html);
        var styleHashes = HashBlocks(s_styleBlock, html);
        var policy = ContentSecurityPolicy;
        if (scriptHashes.Length > 0)
        {
            policy = policy.Replace("script-src 'self'", "script-src 'self' " + scriptHashes, StringComparison.Ordinal);
        }

        if (styleHashes.Length > 0)
        {
            policy = policy.Replace("style-src 'self';", "style-src 'self' " + styleHashes + ";", StringComparison.Ordinal);
        }

        return policy;
    }

    /// <summary>The text with every CRLF and lone CR turned into LF: the form a browser's HTML parser hands to the
    /// policy check (HTML Standard, "preprocessing the input stream").</summary>
    internal static string NormalizeLineEndings(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        return text.Replace("\r\n", "\n", StringComparison.Ordinal).Replace('\r', '\n');
    }

    private static string HashBlocks(Regex pattern, string html)
    {
        var hashes = new List<string>();
        foreach (Match match in pattern.Matches(html))
        {
            var body = NormalizeLineEndings(match.Groups["body"].Value);
            if (body.Length == 0)
            {
                continue;
            }

            var token = "'sha256-" + Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(body))) + "'";
            if (!hashes.Contains(token))
            {
                hashes.Add(token);
            }
        }

        return string.Join(' ', hashes);
    }
}
