/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web host's browser-facing response headers (<c>Content-Security-Policy</c>, <c>X-Frame-Options</c>,
/// <c>X-Content-Type-Options</c>, <c>Referrer-Policy</c>), through the SAME <c>ConfigurePipeline</c> production
/// calls, over a <see cref="TestServer"/>: on the HTML shell, a static asset, an API JSON reply and an error
/// reply, in loopback mode and in network mode. A second group checks <c>wwwroot/</c> for any construct the policy
/// would block, so a later change to the page cannot start failing in the browser without a test going red.
/// </summary>
public sealed class DarlingWebSecurityHeadersTests
{
    private const string ListenIp = "192.168.1.205";
    private const string Token = "correct-token-value";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");

    private static async Task<TestServer> BuildServer(bool networkMode)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();
        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache());

        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: networkMode,
            networkListenIp: networkMode ? IPAddress.Parse(ListenIp) : null,
            allowedCidr: IPNetwork.Parse(networkMode ? "192.168.1.0/24" : "127.0.0.1/32"),
            accessToken: Token,
            oidcClient: null);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private static Task<HttpContext> Get(TestServer server, bool networkMode, string path, string? hostHeader = null, string? token = null)
    {
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = path;
            if (token is not null)
            {
                ctx.Request.QueryString = new QueryString("?token=" + Uri.EscapeDataString(token));
            }

            ctx.Request.Headers.Host = hostHeader ?? (networkMode ? ListenIp : "localhost");
            ctx.Connection.RemoteIpAddress = networkMode ? InCidrRemote : IPAddress.Loopback;
        });
    }

    private static void AssertDefaultHeaders(HttpContext ctx, bool expectDefaultPolicy = true)
    {
        var headers = ctx.Response.Headers;
        Assert.Equal("DENY", headers.XFrameOptions.ToString());
        Assert.Equal("nosniff", headers.XContentTypeOptions.ToString());
        Assert.Equal("no-referrer", headers["Referrer-Policy"].ToString());
        var csp = headers["Content-Security-Policy"].ToString();
        if (expectDefaultPolicy)
        {
            Assert.Equal(DarlingWebSecurityHeaders.ContentSecurityPolicy, csp);
        }

        Assert.Contains("frame-ancestors 'none'", csp, StringComparison.Ordinal);
        Assert.Contains("default-src 'self'", csp, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HtmlShell_CarriesTheHeaders(bool networkMode)
    {
        using var server = await BuildServer(networkMode);
        var ctx = await Get(server, networkMode, "/", token: networkMode ? Token : null);

        /* Network mode answers a right token with a 302 to the shell; the redirect carries the headers too. */
        Assert.True(ctx.Response.StatusCode is 200 or 302, $"unexpected status {ctx.Response.StatusCode}");
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task HtmlShell_Loopback_IsTheIndexPageItself()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Get(server, false, "/index.html");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.StartsWith("text/html", ctx.Response.ContentType, StringComparison.Ordinal);
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task StaticAsset_Loopback_CarriesTheHeaders()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Get(server, false, "/js/app.js");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task ApiJson_Loopback_CarriesTheHeaders()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Get(server, false, "/api/ping");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task ErrorResponse_Loopback_CarriesTheHeaders()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Get(server, false, "/api/this-route-does-not-exist");

        Assert.Equal(StatusCodes.Status404NotFound, ctx.Response.StatusCode);
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task RefusedHost_Loopback_CarriesTheHeaders()
    {
        using var server = await BuildServer(networkMode: false);
        var ctx = await Get(server, false, "/", hostHeader: "evil.example");

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task RefusedHost_Network_CarriesTheHeaders()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Get(server, true, "/", hostHeader: "evil.example");

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task Network_ApiAndStaticWithoutCredentials_CarryTheHeaders()
    {
        using var server = await BuildServer(networkMode: true);

        AssertDefaultHeaders(await Get(server, true, "/api/ping"), expectDefaultPolicy: false);
        AssertDefaultHeaders(await Get(server, true, "/js/app.js"), expectDefaultPolicy: false);
    }

    [Fact]
    public async Task Network_ApiAndStaticWithToken_CarryTheDefaultPolicy()
    {
        using var server = await BuildServer(networkMode: true);

        /* A right token is exchanged for a 302 and a cookie, so the headers are read on that answer; the
           shell, asset and API replies behind it are the loopback cases above (the same middleware). */
        var ctx = await Get(server, true, "/api/ping", token: Token);
        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        AssertDefaultHeaders(ctx);
    }

    [Fact]
    public async Task LoginPage_CarriesAPolicyThatAllowsItsOwnInlineBlocksByHash()
    {
        using var server = await BuildServer(networkMode: true);
        var ctx = await Get(server, true, "/");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        AssertDefaultHeaders(ctx, expectDefaultPolicy: false);
        var csp = ctx.Response.Headers["Content-Security-Policy"].ToString();
        var html = await new StreamReader(ctx.Response.Body).ReadToEndAsync();

        Assert.Contains("'sha256-", csp, StringComparison.Ordinal);
        AssertEveryInlineBlockIsHashed(html, csp);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PolicyForInlinePage_HashesEveryInlineBlockOfTheLoginPage(bool oidcEnabled)
    {
        var html = DarlingWebHostService.BuildLoginPageHtml(oidcEnabled);
        var csp = DarlingWebSecurityHeaders.PolicyForInlinePage(html);

        AssertEveryInlineBlockIsHashed(html, csp);
        Assert.Contains("object-src 'none'", csp, StringComparison.Ordinal);
        Assert.DoesNotContain("'unsafe-inline'", csp.Replace("style-src-attr 'unsafe-inline'", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Fact]
    public void PolicyForInlinePage_WithNoInlineBlock_IsTheDefaultPolicy()
        => Assert.Equal(
            DarlingWebSecurityHeaders.ContentSecurityPolicy,
            DarlingWebSecurityHeaders.PolicyForInlinePage("<html><body><script src=\"js/app.js\"></script></body></html>"));

    private static void AssertEveryInlineBlockIsHashed(string html, string csp)
    {
        var blocks = Regex.Matches(html, @"<(script|style)\b[^>]*>(?<body>.*?)</\1>", RegexOptions.Singleline | RegexOptions.IgnoreCase)
            .Select(m => (Kind: m.Groups[1].Value.ToLowerInvariant(), Body: m.Groups["body"].Value))
            .Where(b => b.Body.Length > 0)
            .ToList();
        Assert.NotEmpty(blocks);
        foreach (var (kind, body) in blocks)
        {
            var hash = "'sha256-" + Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(body))) + "'";
            var directive = csp.Split(';').Select(d => d.Trim()).First(d => d.StartsWith(kind + "-src ", StringComparison.Ordinal));
            Assert.Contains(hash, directive, StringComparison.Ordinal);
        }
    }

    /* ---- The page-side guard: nothing under wwwroot/ may use a construct the policy blocks. ---- */

    private static string WwwRoot => Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service", "wwwroot");

    /// <summary>Removes block comments and whole-line or trailing <c>//</c> comments (a <c>//</c> inside a string,
    /// as in <c>http://</c>, is left alone because it is not preceded by whitespace).</summary>
    private static string StripJsComments(string text)
    {
        text = Regex.Replace(text, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(text, @"(^|\s)//[^\r\n]*", "$1", RegexOptions.Multiline);
    }

    /// <summary>The constructs the default policy blocks. Inline <c>style="..."</c> attributes are NOT here: the
    /// policy allows them through <c>style-src-attr</c> (see <see cref="DarlingWebSecurityHeaders"/>).</summary>
    internal static List<string> FindBlockedConstructs(string fileName, string text)
    {
        var found = new List<string>();
        var isHtml = fileName.EndsWith(".html", StringComparison.OrdinalIgnoreCase);
        var code = isHtml
            ? Regex.Replace(text, @"<!--.*?-->", string.Empty, RegexOptions.Singleline)
            : StripJsComments(text);

        if (fileName.EndsWith(".css", StringComparison.OrdinalIgnoreCase))
        {
            if (Regex.IsMatch(code, @"@import\s+(url\()?\s*[""']?https?:", RegexOptions.IgnoreCase))
            {
                found.Add("a stylesheet @import from another origin");
            }

            return found;
        }

        if (Regex.IsMatch(code, @"<script\b(?![^>]*\bsrc\s*=)[^>]*>\s*\S", RegexOptions.IgnoreCase))
        {
            found.Add("an inline <script> body");
        }

        if (Regex.IsMatch(code, @"<style\b", RegexOptions.IgnoreCase))
        {
            found.Add("an inline <style> block");
        }

        if (Regex.IsMatch(code, @"<[a-zA-Z][^<>]*\son[a-zA-Z]{3,}\s*=", RegexOptions.Singleline))
        {
            found.Add("an on*= event-handler attribute");
        }

        if (Regex.IsMatch(code, @"setAttribute\(\s*[""']on[a-zA-Z]{3,}[""']", RegexOptions.IgnoreCase))
        {
            found.Add("setAttribute of an on* handler");
        }

        if (Regex.IsMatch(code, @"[""'`=]\s*javascript:", RegexOptions.IgnoreCase))
        {
            found.Add("a javascript: URL");
        }

        if (Regex.IsMatch(code, @"\beval\s*\(|\bnew\s+Function\s*\(|\bset(Timeout|Interval)\(\s*[""'`]"))
        {
            found.Add("eval / new Function / a string handed to a timer");
        }

        if (Regex.IsMatch(code, @"createElement\(\s*[""']style[""']"))
        {
            found.Add("a created <style> element");
        }

        if (Regex.IsMatch(code, @"\b(insertAdjacentHTML|document\.write)\s*\(.*<(script|style)\b", RegexOptions.IgnoreCase))
        {
            found.Add("markup written from script that holds a script or style block");
        }

        return found;
    }

    [Fact]
    public void Wwwroot_HoldsNothingTheDefaultPolicyBlocks()
    {
        var files = Directory.EnumerateFiles(WwwRoot, "*", SearchOption.AllDirectories)
            .Where(f => f.EndsWith(".html", StringComparison.OrdinalIgnoreCase)
                     || f.EndsWith(".js", StringComparison.OrdinalIgnoreCase)
                     || f.EndsWith(".css", StringComparison.OrdinalIgnoreCase))
            .ToList();
        Assert.True(files.Count > 60, "the check read only " + files.Count + " files; it is reading the wrong folder");
        Assert.Contains(files, f => f.EndsWith("index.html", StringComparison.OrdinalIgnoreCase));

        var problems = new List<string>();
        foreach (var file in files)
        {
            foreach (var construct in FindBlockedConstructs(Path.GetFileName(file), File.ReadAllText(file)))
            {
                problems.Add(Path.GetRelativePath(WwwRoot, file) + ": " + construct);
            }
        }

        Assert.True(problems.Count == 0,
            "These constructs are blocked by the web host's Content-Security-Policy (DarlingWebSecurityHeaders): "
            + string.Join("; ", problems));
    }

    [Fact]
    public void Wwwroot_IndexHtml_LoadsItsScriptAndStylesFromItsOwnOrigin()
    {
        var index = File.ReadAllText(Path.Combine(WwwRoot, "index.html"));
        Assert.DoesNotContain("<style", index, StringComparison.OrdinalIgnoreCase);
        foreach (Match m in Regex.Matches(index, @"<(script|link)\b[^>]*\b(src|href)\s*=\s*""([^""]*)""", RegexOptions.IgnoreCase))
        {
            var target = m.Groups[3].Value;
            Assert.False(target.StartsWith("http", StringComparison.OrdinalIgnoreCase) || Regex.IsMatch(target, "^/{2}"),
                "index.html loads " + target + " from another origin");
        }
    }

    [Theory]
    [InlineData("a.html", "<script>run()</script>", "an inline <script> body")]
    [InlineData("a.html", "<button onclick=\"go()\">x</button>", "an on*= event-handler attribute")]
    [InlineData("a.html", "<a href=\"javascript:go()\">x</a>", "a javascript: URL")]
    [InlineData("a.html", "<style>body{}</style>", "an inline <style> block")]
    [InlineData("a.js", "const f = new Function('return 1');", "eval / new Function / a string handed to a timer")]
    [InlineData("a.js", "x = eval(code);", "eval / new Function / a string handed to a timer")]
    [InlineData("a.js", "setTimeout('go()', 10);", "eval / new Function / a string handed to a timer")]
    [InlineData("a.js", "n.setAttribute('onclick', 'go()');", "setAttribute of an on* handler")]
    [InlineData("a.js", "const s = document.createElement('style');", "a created <style> element")]
    [InlineData("a.js", "const h = '<img src=x onerror=go()>';", "an on*= event-handler attribute")]
    [InlineData("a.css", "@import url(https://cdn.example/x.css);", "a stylesheet @import from another origin")]
    public void TheGuardCheck_SeesEachBlockedConstruct(string fileName, string text, string expected)
        => Assert.Contains(expected, FindBlockedConstructs(fileName, text));

    [Theory]
    [InlineData("a.html", "<script src=\"js/app.js\" type=\"module\"></script>")]
    [InlineData("a.html", "<!-- <script>run()</script> --><p>text</p>")]
    [InlineData("a.js", "/* eval(x) and onclick=\"y\" in a comment */ const a = 1; // javascript: in a comment")]
    [InlineData("a.js", "node.style.display = 'none'; el('div', { style: 'height:4px', onClick: go });")]
    [InlineData("a.js", "const ns = \"http://www.w3.org/2000/svg\";")]
    public void TheGuardCheck_LeavesAllowedCodeAlone(string fileName, string text)
        => Assert.Empty(FindBlockedConstructs(fileName, text));
}
