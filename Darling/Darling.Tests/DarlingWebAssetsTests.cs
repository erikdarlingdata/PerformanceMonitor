/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1562: the web dashboard's static assets must land in the build output. The service csproj declares
/// <c>&lt;Content Include="wwwroot\**\*" CopyToOutputDirectory="PreserveNewest" /&gt;</c>, and the web host
/// pins its content/web root to <see cref="AppContext.BaseDirectory"/> — so if the copy silently stops
/// working, a Windows service would 404 every static file in production only (the load-bearing footgun). This
/// test fails the build instead. The content flows transitively from the referenced service project into the
/// test's own output directory, so the assertion runs against the test bin.
/// </summary>
public sealed class DarlingWebAssetsTests
{
    [Fact]
    public void Wwwroot_IndexHtml_IsCopiedToTheBuildOutput()
    {
        var indexPath = Path.Combine(AppContext.BaseDirectory, "wwwroot", "index.html");
        Assert.True(File.Exists(indexPath),
            $"wwwroot/index.html was not copied to the build output ({indexPath}). Check the " +
            "<Content Include=\"wwwroot\\**\\*\"> item in PerformanceMonitor.Darling.Service.csproj.");
    }

    /// <summary>
    /// #1562: the frontend's CSS/JS entry files (Builder 3) must ALSO reach the build output — the recursive
    /// <c>wwwroot\**\*</c> glob copies subdirectories, so a broken glob or a moved/renamed entry file (which the
    /// SPA loads by exact path) would 404 in production only. This pins the module graph's roots.
    /// </summary>
    [Theory]
    [InlineData("css/theme.css")]
    [InlineData("css/app.css")]
    [InlineData("css/editor.css")]
    [InlineData("js/app.js")]
    [InlineData("js/util.js")]
    [InlineData("js/panels.js")]
    [InlineData("js/charts.js")]
    [InlineData("js/views-api.js")]
    [InlineData("js/derive.js")]
    [InlineData("js/editor.js")]
    [InlineData("js/compose.js")]
    [InlineData("js/markdown.js")]
    [InlineData("js/notebook.js")]
    [InlineData("js/pages/ag.js")]
    [InlineData("js/pages/fleet.js")]
    [InlineData("js/pages/server.js")]
    [InlineData("js/pages/alerts.js")]
    [InlineData("js/pages/views.js")]
    [InlineData("js/pages/triage.js")]
    public void Wwwroot_EntryAsset_IsCopiedToTheBuildOutput(string relativePath)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "wwwroot", relativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path),
            $"wwwroot/{relativePath} was not copied to the build output ({path}). The SPA loads it by exact path; " +
            "check the recursive <Content Include=\"wwwroot\\**\\*\"> glob in PerformanceMonitor.Darling.Service.csproj.");
    }
}
