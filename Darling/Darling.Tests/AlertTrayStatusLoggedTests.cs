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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2814 (follow-on to #2781): the Darling web is headless (no system tray), so a <c>"tray"</c> alert - Lite's
/// delivered-without-email taxonomy - was never delivered on this surface and must read a single neutral
/// "Logged", NOT a Sent/Not-sent derived from <c>alert_sent</c> (which the engine sets inconsistently: clears
/// true, problems false). Both status collapses - <c>statusCell</c> (alerts.js) and <c>alertStatusText</c>
/// (triage.js) - carry the rule, and the pages document that they stay in lockstep, so this pins both. No JS
/// runner in this repo, so it scans the frontend source the way the composer drift guards do.
/// </summary>
public class AlertTrayStatusLoggedTests
{
    [Theory]
    [InlineData("pages/alerts.js")]
    [InlineData("pages/triage.js")]
    public void TrayAlert_RendersLogged_NotSentOrNotSent(string relPath)
    {
        var src = FrontendSource(relPath);

        // The tray branch exists and maps to "Logged".
        Assert.Matches(new Regex(@"notification_type\s*===\s*""tray"""), src);
        Assert.Contains("\"Logged\"", src, StringComparison.Ordinal);

        // The tray check must sit BEFORE the alert_sent -> Sent/Not-sent collapse, or a tray alert would still
        // fall through to the misleading Sent/Not-sent. (Guards the ordering, not just the presence.)
        var trayIdx = src.IndexOf("=== \"tray\"", StringComparison.Ordinal);
        var sentIdx = src.IndexOf("alert_sent", StringComparison.Ordinal);
        Assert.True(trayIdx >= 0, $"{relPath}: tray branch not found");
        Assert.True(sentIdx >= 0, $"{relPath}: alert_sent branch not found");
        Assert.True(trayIdx < sentIdx, $"{relPath}: the tray->Logged branch must precede the alert_sent collapse");
    }

    private static string FrontendSource(string relPath, [CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Service", "wwwroot", "js", relPath));
        Assert.True(File.Exists(path), $"{relPath} not found at {path} (did the frontend move?)");
        return File.ReadAllText(path);
    }
}
