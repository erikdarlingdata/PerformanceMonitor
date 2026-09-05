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
    /// <summary>
    /// Every file in the tree that DEFINES a <c>get_collection_health</c> MCP tool. Discovered rather than
    /// listed: a positive check over the two files this change touched would confirm its own work and see
    /// nothing else, which is how two older sites survived a parity sweep earlier in this backlog.
    /// </summary>
    private static IReadOnlyList<string> CollectionHealthToolFiles()
    {
        var root = RepoRoot();

        var found = Directory
            .EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !p.Contains($"{Path.DirectorySeparatorChar}deprecated{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !p.Contains($"{Path.DirectorySeparatorChar}.git{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(p => File.ReadAllText(p).Contains(
                "[McpServerTool(Name = \"get_collection_health\")", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();

        /* The control for the sweep above, through the identical enumeration: it must find MORE than one
           file, or the whole-tree walk is silently reading nothing and every assertion built on it is
           vacuous. Two is today's answer and the count is asserted at the call site; this is the floor. */
        Assert.True(
            found.Count >= 2,
            $"the whole-tree walk for get_collection_health tool definitions found {found.Count} file(s) "
            + "under " + root + " — a walk that reaches one file or none cannot make a parity claim");

        return found;
    }

    [Fact]
    public void EverySkusCollectionHealthTool_CarriesTheAlertReadBlock()
    {
        var files = CollectionHealthToolFiles();

        Assert.Equal(2, files.Count);
        Assert.Contains(files, f => f.EndsWith("McpHealthTools.cs", StringComparison.Ordinal));
        Assert.Contains(files, f => f.EndsWith("DarlingMcpDataTools.cs", StringComparison.Ordinal));

        foreach (var file in files)
        {
            var text = File.ReadAllText(file);

            Assert.Contains("alert_read_health = new", text, StringComparison.Ordinal);
            Assert.Contains("AlertReadFailureCounter.Shared.ReadFor(", text, StringComparison.Ordinal);
            Assert.Contains("AlertReadFailureCounter.FormatFinding(", text, StringComparison.Ordinal);
            Assert.Contains("note = AlertReadFailureCounter.WindowNote", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BothSkusBlocks_CarryTheIdenticalFieldSet_AndEveryFieldOfTheReading()
    {
        /* The parity claim proper. Compared as SETS of field names extracted from each SKU's own
           initializer, so a field added to one and forgotten on the other fails here — and cross-checked
           against the Reading record by REFLECTION, so a field added to the record and rendered by neither
           SKU also fails. Two directions, because a payload nobody renders and a payload one SKU renders
           are different defects with the same cause. */
        var fieldsBySku = CollectionHealthToolFiles()
            .ToDictionary(
                f => Path.GetFileName(f),
                f => AlertReadFieldNames(File.ReadAllText(f)),
                StringComparer.Ordinal);

        var sets = fieldsBySku.Values.ToList();
        Assert.Equal(2, sets.Count);
        Assert.Equal(sets[0], sets[1]);

        var rendered = sets[0];

        /* Reflected off the record so a seventh member cannot be added without a surface for it. */
        var readingMembers = typeof(AlertReadFailureCounter.Reading)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .Where(n => n != "EqualityContract")
            .ToList();

        Assert.Equal(6, readingMembers.Count);

        /* Six from the record plus the two composed values. */
        Assert.Equal(8, rendered.Count);
        Assert.Equal(
            new[]
            {
                "counting_since", "finding", "instance_read_failures", "last_failure_at",
                "last_failure_read", "note", "server_alert_passes", "server_read_failures",
            },
            rendered.OrderBy(f => f, StringComparer.Ordinal).ToArray());
    }

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

    [Fact]
    public void BothSkusToolDescriptions_StayByteIdentical()
    {
        /* One tool, one contract: the two SKUs' descriptions of get_collection_health are byte-identical on
           origin/dev and must stay so, or a client learns different things about the same tool depending on
           which SKU answered. This change appended to both; the pin is that it appended the SAME bytes. */
        var descriptions = CollectionHealthToolFiles()
            .Select(f => ToolDescription(File.ReadAllText(f)))
            .ToList();

        Assert.Equal(2, descriptions.Count);
        Assert.Equal(descriptions[0], descriptions[1]);

        /* And that the appended paragraph is actually in there, so the equality above is not two copies of
           an unchanged string agreeing with each other. */
        Assert.Contains("alert_read_health", descriptions[0], StringComparison.Ordinal);
        Assert.Contains("counting_since", descriptions[0], StringComparison.Ordinal);
        Assert.Contains("failed to DELIVER", descriptions[0], StringComparison.Ordinal);
    }

    /* ---------------- helpers ---------------- */

    /// <summary>The field names inside a tool's <c>alert_read_health = new { … }</c> initializer.</summary>
    private static SortedSet<string> AlertReadFieldNames(string source)
    {
        var at = source.IndexOf("alert_read_health = new", StringComparison.Ordinal);
        Assert.True(at > 0, "a get_collection_health tool no longer builds an alert_read_health block");

        var open = source.IndexOf('{', at);
        Assert.True(open > 0, "alert_read_health has no initializer");

        var depth = 0;
        var end = -1;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    end = i;
                    break;
                }
            }
        }

        Assert.True(end > open, "alert_read_health's initializer never closes");

        var body = source[open..end];

        /* Assignments only, and only at the initializer's own level — the block contains explanatory
           comments with '=' in prose, so the pattern requires an identifier at the start of a line. */
        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(body, @"^\s*([a-z_][a-z0-9_]*)\s*=\s*[^=]", RegexOptions.Multiline))
        {
            names.Add(m.Groups[1].Value);
        }

        Assert.True(names.Count > 0, "no fields were extracted from an alert_read_health block");

        return names;
    }

    private static string ToolDescription(string source)
    {
        var m = Regex.Match(
            source,
            @"\[McpServerTool\(Name = ""get_collection_health""\), Description\(""(.*?)""\)\]",
            RegexOptions.Singleline);

        Assert.True(m.Success, "a get_collection_health tool has no Description attribute in the expected shape");

        return m.Groups[1].Value;
    }

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
