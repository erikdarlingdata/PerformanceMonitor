/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8631: the web dashboard's sidebar at desktop window sizes, drawn by a real headless Chrome or Edge. The shipped index.html shell
/// (its scripts removed) and the shipped stylesheets are loaded in an iframe of the exact window size, filled with sixteen page links,
/// forty servers and a few views (<c>web-sidebar-height-probe.js</c>), and every entry is checked for being fully inside the viewport
/// and inside every scroll region that clips it, with no scrolling. One browser run draws all the sizes. Skipped when no Chrome or
/// Edge is installed.
/// </summary>
public sealed class WebSidebarHeightBehaviourTests
{
    private static readonly (int Width, int Height)[] Sizes = { (1920, 1080), (1280, 720) };

    private static readonly Lazy<Dictionary<string, JsonElement>> Drawn = new(Draw);

    private static string? FindBrowser()
    {
        var candidates = new List<string>();
        foreach (var root in new[] { Environment.GetEnvironmentVariable("ProgramFiles"), Environment.GetEnvironmentVariable("ProgramFiles(x86)") })
        {
            if (string.IsNullOrEmpty(root)) continue;
            candidates.Add(Path.Combine(root, "Google", "Chrome", "Application", "chrome.exe"));
            candidates.Add(Path.Combine(root, "Microsoft", "Edge", "Application", "msedge.exe"));
        }

        var onPath = (Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);
        foreach (var name in new[] { "google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge" })
        {
            candidates.AddRange(onPath.Select(d => Path.Combine(d, name)));
        }

        return candidates.FirstOrDefault(File.Exists);
    }

    private static Dictionary<string, JsonElement> Draw()
    {
        var browser = FindBrowser();
        if (browser is null)
        {
            Assert.Skip("No Chrome or Edge is installed, so the sidebar cannot be drawn.");
            return new();
        }

        var wwwroot = PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot");
        var dir = Path.Combine(Path.GetTempPath(), "pm-sidebar-8631-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var shell = Regex.Replace(File.ReadAllText(Path.Combine(wwwroot, "index.html")), @"<script[\s\S]*?</script>", "");
            var probe = File.ReadAllText(PathTo("Darling", "Darling.Tests", "web-sidebar-height-probe.js"));
            var baseHref = new Uri(wwwroot.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar).AbsoluteUri;
            shell = shell.Replace("<head>", "<head><base href=\"" + baseHref + "\">").Replace("</body>", "<script>" + probe + "</script></body>");
            File.WriteAllText(Path.Combine(dir, "page.html"), shell);

            var frames = string.Concat(Sizes.Select(s => $"<iframe src=\"page.html?{s.Width}x{s.Height}\" style=\"width:{s.Width}px;height:{s.Height}px;border:0;display:block\"></iframe>"));
            var wrapper = "<!doctype html><body>" + frames + "<script>var got=[];window.addEventListener('message',function(e){got.push(e.data);"
                + "if(got.length===" + Sizes.Length + "){var p=document.createElement('pre');p.id='pm-probe-result';p.textContent='['+got.join(',')+']';document.body.appendChild(p);}});</script></body>";
            File.WriteAllText(Path.Combine(dir, "wrapper.html"), wrapper);

            var psi = new ProcessStartInfo(browser) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            foreach (var arg in new[] { "--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check", "--user-data-dir=" + Path.Combine(dir, "profile"), "--virtual-time-budget=10000", "--dump-dom", new Uri(Path.Combine(dir, "wrapper.html")).AbsoluteUri })
            {
                psi.ArgumentList.Add(arg);
            }

            using var proc = Process.Start(psi)!;
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEndAsync();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the browser did not finish drawing the sidebar in 60 s");
            }

            var match = Regex.Match(output.Result, "<pre id=\"pm-probe-result\">(.*?)</pre>", RegexOptions.Singleline);
            Assert.True(match.Success, "the browser drew no result for the sidebar. stderr: " + error.Result[..Math.Min(300, error.Result.Length)]);
            using var doc = JsonDocument.Parse(WebUtility.HtmlDecode(match.Groups[1].Value));
            return doc.RootElement.EnumerateArray().ToDictionary(e => e.GetProperty("size").GetString()!, e => e.Clone());
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
        }
    }

    private static JsonElement At(int width, int height)
    {
        var all = Drawn.Value;
        var r = all[$"{width}x{height}"];
        Assert.Equal(width, r.GetProperty("innerWidth").GetInt32());
        Assert.Equal(height, r.GetProperty("innerHeight").GetInt32());
        Assert.Equal(17, r.GetProperty("pageLinks").GetInt32());
        return r;
    }

    /// <summary>A 1080-high window has room for every page link and the server list's five rows, so no page link sits behind a scroll.</summary>
    [Fact]
    public void ATallWindow_ShowsEveryPageLink_AndFiveServerRows_WithoutScrolling()
    {
        var r = At(1920, 1080);
        Assert.True(r.GetProperty("pageLinksVisible").GetInt32() == 17, "page links inside the viewport at 1920x1080: " + r);
        Assert.True(r.GetProperty("serversVisible").GetInt32() >= 5, "server rows inside the viewport at 1920x1080: " + r);
        Assert.True(r.GetProperty("viewsHeadingVisible").GetBoolean(), "the Views heading at 1920x1080: " + r);
    }

    /// <summary>A 720-high window cannot hold every link: the search box, five server rows and Alert History (the fifth link) still show.</summary>
    [Fact]
    public void AShortWindow_KeepsTheSearchBox_FiveServerRows_AndAlertHistoryInView()
    {
        var r = At(1280, 720);
        Assert.True(r.GetProperty("searchVisible").GetBoolean(), "the search box at 1280x720: " + r);
        Assert.True(r.GetProperty("serversVisible").GetInt32() >= 5, "server rows inside the viewport at 1280x720: " + r);
        Assert.True(r.GetProperty("alertHistoryVisible").GetBoolean(), "the Alert History link at 1280x720: " + r);
        Assert.True(r.GetProperty("viewsHeadingVisible").GetBoolean(), "the Views heading at 1280x720: " + r);
    }
}
