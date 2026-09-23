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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3013's Lite half: <c>get_collection_health</c> exists on BOTH SKUs, so a block added to one is this
/// repo's recurring parity failure (#3006 needed a test per SKU; #3017 found a fourth surface in the web
/// dashboard). These pins are written from Lite's side and are deliberately NOT a copy of the Darling
/// ones — they enumerate the SKU-paired surfaces and assert every one carries the block, rather than
/// confirming the phrases this change just wrote appear where it wrote them.
///
/// <para><b>What is NOT claimed.</b> #3013's mechanism is store latency crossing the alert pass's
/// Postgres command deadline, and Lite's alert reads hit a local DuckDB store, so that mechanism does not
/// transfer. What transfers is the SURFACE gap: a swallowed alert read on Lite also reached no health read.
/// The shared engine is where the counting happens, so Lite gets the same instrument for free and pays
/// nothing for the parts of #3013 that are Darling's.</para>
/// </summary>
public sealed class AlertReadFailureSurfaceTests
{
    [Fact]
    public void TheLiteSurface_DerivesTheSameServerKeyAsTheLiteAlertPass()
    {
        /* The silent-zero hazard, Lite's spelling of it. Lite's alert pass keys on
           summary.ServerId.ToString() with no explicit culture, so the reader must render the key the SAME
           way or it looks up a bucket nothing ever wrote and reports a confident zero. Same process, same
           culture, so the two agree by construction — but only while they stay the same expression, which
           is what this pins. Darling's pair uses InvariantCulture on both sides and is pinned from its own
           side; neither SKU's spelling is imposed on the other, because changing Lite's alert key would
           re-key its suppression, badge and watermark state as well. */
        var tool = ReadSource(Path.Combine("Lite", "Mcp", "McpHealthTools.cs"));
        var pass = ReadSource(Path.Combine("Lite", "MainWindow.AlertEngine.cs"));

        Assert.Contains(
            "AlertReadFailureCounter.Shared.ReadFor(resolved.ServerId.ToString())",
            tool,
            StringComparison.Ordinal);
        Assert.Contains("var key = summary.ServerId.ToString();", pass, StringComparison.Ordinal);

        /* The control: the same Contains form finds a deliberately wrong spelling nowhere, so its silence
           above is an absence rather than a matcher that never matches. */
        Assert.DoesNotContain(
            "ReadFor(resolved.ServerId.ToString(CultureInfo.InvariantCulture))",
            tool,
            StringComparison.Ordinal);
    }

    [Fact]
    public void TheCounter_IsWiredIntoLitesEngine_AtConstruction()
    {
        /* The wiring, which no behavioural test on this SKU can reach: the engine takes the counter as an
           optional constructor argument defaulting to null, so an unwired Lite would compile, run, and
           report a permanent zero. Exactly the #1648 middleware-ordering shape — a WIRING omission that a
           pure logic pin passes straight over. */
        var wiring = ReadSource(Path.Combine("Lite", "MainWindow.xaml.cs"));

        Assert.Contains("readFailures: AlertReadFailureCounter.Shared);", wiring, StringComparison.Ordinal);

        /* And the alias rather than a namespace import, because importing PerformanceMonitor.Alerting into
           this file collides with the app's own CpuAlertMode — the reason the AlertEngine reference here is
           an alias in the first place. */
        Assert.Contains(
            "using AlertReadFailureCounter = PerformanceMonitor.Alerting.AlertReadFailureCounter;",
            wiring,
            StringComparison.Ordinal);
    }

    /* ---------------- helpers ---------------- */

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#3013 scan target not found: {path}");

        return File.ReadAllText(path);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
