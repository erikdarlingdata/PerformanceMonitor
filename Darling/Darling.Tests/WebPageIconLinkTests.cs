/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5701: a page with no icon link makes the browser ask for <c>/favicon.ico</c> on its first load, and the host
/// answers 404, which the UI smoke check reads as a failed request. Every <c>&lt;head&gt;</c> the web host emits
/// therefore carries an icon link: the <c>wwwroot/*.html</c> files, the sign-in page, the signed-in landing page and
/// the sign-in error page. The three hand-built pages share <see cref="DarlingWebHostService.IconLinkHtml"/>.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class WebPageIconLinkTests
{
    private static readonly Regex s_iconLink = new(
        @"<link\b[^>]*\brel\s*=\s*[""']icon[""'][^>]*\bhref\s*=\s*[""'][^""']+",
        RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static string HeadOf(string html)
    {
        var m = Regex.Match(html, @"<head\b.*?</head>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        Assert.True(m.Success, "the page has no <head> element");
        return m.Value;
    }

    [Fact]
    public void EveryWwwrootHtmlFile_CarriesAnIconLinkInItsHead()
    {
        var root = Path.Combine(AppContext.BaseDirectory, "wwwroot");
        var files = Directory.GetFiles(root, "*.html", SearchOption.AllDirectories);
        Assert.NotEmpty(files);
        foreach (var file in files)
        {
            Assert.True(s_iconLink.IsMatch(HeadOf(File.ReadAllText(file))),
                $"{Path.GetRelativePath(root, file)} has no <link rel=\"icon\"> in its head, so a browser asks for /favicon.ico and gets a 404 (#5701).");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheSignInPage_CarriesAnIconLink(bool oidcEnabled)
    {
        var head = HeadOf(DarlingWebHostService.BuildLoginPageHtml(oidcEnabled));
        Assert.Contains(DarlingWebHostService.IconLinkHtml, head, StringComparison.Ordinal);
        Assert.Matches(s_iconLink, head);
    }

    [Fact]
    public async Task TheSignedInLandingPage_CarriesAnIconLink()
    {
        var html = await Render(ctx => DarlingWebHostService.WriteSignedInLandingAsync(ctx, "/"));
        Assert.Contains(DarlingWebHostService.IconLinkHtml, HeadOf(html), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheSignInErrorPage_CarriesAnIconLink()
    {
        var html = await Render(ctx => DarlingWebHostService.WriteSignInErrorAsync(ctx, 400, "A message"));
        Assert.Contains(DarlingWebHostService.IconLinkHtml, HeadOf(html), StringComparison.Ordinal);
    }

    /// <summary>The shared link is a <c>data:</c> URL (the default policy allows it through <c>img-src</c>) and is the
    /// same icon as the one in <c>index.html</c>.</summary>
    [Fact]
    public void TheSharedIconLink_IsADataUrl_AndMatchesTheOneInIndexHtml()
    {
        Assert.StartsWith("<link rel=\"icon\" href=\"data:", DarlingWebHostService.IconLinkHtml, StringComparison.Ordinal);
        var index = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "wwwroot", "index.html"));
        var href = Regex.Match(DarlingWebHostService.IconLinkHtml, "href=\"(?<h>[^\"]+)\"").Groups["h"].Value;
        Assert.Contains(href, index, StringComparison.Ordinal);
    }

    /// <summary>A census: the host source holds exactly three <c>&lt;head&gt;</c> literals (landing, sign-in error and
    /// sign-in page), and this class tests each. A fourth page with its own head fails here until it is added above.</summary>
    [Fact]
    public void TheHostSource_HoldsExactlyTheThreeKnownHeads()
    {
        var path = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs");
        var count = Regex.Matches(File.ReadAllText(path), @"<head\b", RegexOptions.IgnoreCase).Count;
        Assert.Equal(3, count);
    }

    private static async Task<string> Render(Func<HttpContext, Task> write)
    {
        var ctx = new DefaultHttpContext();
        ctx.Response.Body = new MemoryStream();
        await write(ctx);
        ctx.Response.Body.Position = 0;
        return await new StreamReader(ctx.Response.Body).ReadToEndAsync();
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
