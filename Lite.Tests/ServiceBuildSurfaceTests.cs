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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3453's parity half: <c>get_collection_health</c> exists on BOTH SKUs with byte-identical descriptions
/// (pinned by <see cref="AlertReadFailureSurfaceTests.BothSkusToolDescriptions_StayByteIdentical"/>), so a
/// <c>service</c> block the shared text documents has to exist on both — a description that promises a
/// block only one SKU renders is the contract lying to the other SKU's callers, which is worse than the
/// gap #3453 closes. These censuses hold the block to the same discipline the #3013 family holds
/// <c>alert_read_health</c> to: discovered surfaces rather than the two files this change touched,
/// field sets compared rather than trusted, and each SKU's facts served by its OWN authorities — Lite's
/// version is its app assembly's, its rung is the DuckDB initializer's, because a Lite payload carrying
/// Darling's constants would be precisely wrong on the SKU boundary this tool exists to see across.
/// </summary>
public sealed class ServiceBuildSurfaceTests
{
    [Fact]
    public void EverySkusCollectionHealthTool_CarriesTheServiceBlock_WithTheIdenticalFieldSet()
    {
        /* Same shape as the #3013 census one file over: extracted from each SKU's own initializer and
           compared as SETS, so a field added to one SKU and forgotten on the other fails here rather than
           shipping as a payload-shape fork of one tool. */
        var files = CollectionHealthToolFiles();

        Assert.Equal(2, files.Count);

        var sets = files
            .Select(f => ServiceFieldNames(File.ReadAllText(f)))
            .ToList();

        Assert.Equal(sets[0], sets[1]);

        /* Three facts and deliberately no more: what build (version), since when (started_at), and what
           the build expects of its store (compiled_schema_version). A fourth field arriving here should
           have to argue for itself the way these three did in #3453. */
        Assert.Equal(
            new[] { "compiled_schema_version", "started_at", "version" },
            sets[0].ToArray());
    }

    [Fact]
    public void EachSkusVersionRead_IsItsOwnAssemblys_ThroughTheCliMechanics()
    {
        /* Darling serves the CLI's own method — pinned from Darling's side too, in that project's
           ServiceBuildSurfaceTests; repeated here because this census is the one that reads both files
           and can therefore say the two SKUs answer with the same KIND of fact. */
        var darling = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));
        Assert.Contains("version = DarlingCliCommands.ProductVersion()", darling, StringComparison.Ordinal);

        /* Lite has no version verb, so its helper is that read's twin: same attribute, same
           strip-build-metadata-keep-prerelease mechanics, off its OWN assembly. */
        var lite = ReadSource(Path.Combine("Lite", "Mcp", "McpHealthTools.cs"));
        Assert.Contains("version = ProductVersion()", lite, StringComparison.Ordinal);
        Assert.Contains(
            "GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion",
            lite,
            StringComparison.Ordinal);

        /* The drift refused by name: VersionText.Normalize is the server list's DISPLAY form, which
           collapses to Major.Minor.Build — stripping the nightly's prerelease stamp, the exact part that
           distinguishes one nightly from the next and the exact part this surface exists to expose. It is
           the nearest helper to hand in this SKU, which is why its absence is asserted rather than
           assumed. */
        Assert.DoesNotContain("version = VersionText", lite, StringComparison.Ordinal);
        Assert.DoesNotContain("version = VersionText", darling, StringComparison.Ordinal);

        /* And the helper's behaviour, from the value rather than the spelling: equal to the attribute read
           with build metadata stripped, prerelease kept, and a parseable leading component — the same
           floor DarlingCliCommandsTests holds for the verb this twins. */
        var assembly = typeof(McpHealthTools).Assembly;
        var informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        Assert.False(string.IsNullOrWhiteSpace(informational));

        var plus = informational!.IndexOf('+', StringComparison.Ordinal);
        var expected = plus >= 0 ? informational[..plus] : informational;

        var version = McpHealthTools.ProductVersion();

        Assert.Equal(expected, version);
        Assert.False(string.IsNullOrWhiteSpace(version));
        Assert.DoesNotContain('+', version);
        Assert.NotNull(Version.Parse(version.Split('-', '+')[0]));
    }

    [Fact]
    public void StartedAt_IsTheSameInstantAsCountingSince_OnBothSkus()
    {
        /* The decision #3453 asked to be made and then documented: started_at reuses the counter's own
           CountingSinceUtc rather than taking a stamp of its own. Both mean "when this process came up",
           and two clocks for one fact would put two near-identical stamps on one payload whose skew a
           reader has to explain away — so the pin is that BOTH fields render the IDENTICAL expression,
           which is a stronger claim than two stamps that happen to agree tonight. */
        foreach (var file in CollectionHealthToolFiles())
        {
            var text = File.ReadAllText(file);

            Assert.Contains(
                "started_at = alertReads.CountingSinceUtc.ToString(\"o\")",
                text,
                StringComparison.Ordinal);
            Assert.Contains(
                "counting_since = alertReads.CountingSinceUtc.ToString(\"o\")",
                text,
                StringComparison.Ordinal);
        }

        /* And the rendering that expression produces: ISO-8601, UTC-marked, round-trippable to the
           construction instant. Unspecified kind would make "since when" a function of the reader's local
           zone, which for the restart detector's own instant is a wrong answer by up to a day. */
        var started = new DateTime(2026, 9, 15, 1, 2, 3, 456, DateTimeKind.Utc);
        var counter = new AlertReadFailureCounter(() => started);

        var rendered = counter.ReadFor("never-seen").CountingSinceUtc.ToString("o");

        Assert.EndsWith("Z", rendered, StringComparison.Ordinal);

        var parsed = DateTime.Parse(rendered, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
        Assert.Equal(started, parsed);
    }

    [Fact]
    public void EachSkusCompiledRung_IsItsOwnConstant_NeverALiteral()
    {
        /* The rung must MOVE with every future migration, which only a reference to the compiled constant
           does by construction — Darling's StorageVersion.SchemaVersion is pinned to the highest migration
           by ScaffoldTests, and Lite's CurrentSchemaVersion is what its initializer migrates to. A numeral
           here would be correct on the day it is written and quietly wrong on the next rung, on the one
           field whose whole job is saying which rung the build carries. */
        var darling = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));
        Assert.Contains(
            "compiled_schema_version = StorageVersion.SchemaVersion",
            darling,
            StringComparison.Ordinal);

        var lite = ReadSource(Path.Combine("Lite", "Mcp", "McpHealthTools.cs"));
        Assert.Contains(
            "compiled_schema_version = DuckDbInitializer.CurrentSchemaVersion",
            lite,
            StringComparison.Ordinal);

        /* Neither SKU may serve the other's constant: the two rungs are different ladders (Postgres
           migrations versus DuckDB migrations), and a crossed reference would type-check, serialize, and
           lie. Matched as the ASSIGNMENT, because prose on both sides legitimately names the other
           ladder's constant when explaining that this one is its twin. */
        Assert.DoesNotContain("= StorageVersion", lite, StringComparison.Ordinal);
        Assert.DoesNotContain("= DuckDbInitializer", darling, StringComparison.Ordinal);

        foreach (var text in new[] { darling, lite })
        {
            Assert.False(
                Regex.IsMatch(text, @"compiled_schema_version\s*=\s*\d"),
                "a compiled rung is being served as a literal, which goes stale on the next migration");
        }

        /* A floor rather than an equality, deliberately: the equality claim belongs to each ladder's own
           pins, and an equality restated here would be the stale-literal defect wearing a test's clothes.
           The floor is today's rung, and rungs only rise. */
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 59);
    }

    [Fact]
    public void TheSharedDescription_DocumentsTheServiceBlock()
    {
        /* The description is THE build-attribution contract now — an MCP caller learns what the block is
           for from this text and nothing else — so its claims are censused the way the #3013 paragraph's
           are. Byte-identity across the SKUs is the sibling family's pin; this one holds that the shared
           bytes actually say the load-bearing things: what question version answers, that counting_since
           keeps its old job, and what the rung is NOT (a store read). Asserted inside the Description
           attribute's own span rather than against the whole file, so a sentence demoted to a code comment
           cannot keep this green. */
        foreach (var file in CollectionHealthToolFiles())
        {
            var description = DescriptionSpan(File.ReadAllText(file));

            foreach (var phrase in new[]
            {
                "is the running service the build that carries fix X",
                "counting_since remains the restart detector",
                "started_at is the SAME instant",
                "compiled_schema_version is the schema rung this BUILD expects",
                "not a read of the store's migrated rung",
            })
            {
                Assert.Contains(phrase, description, StringComparison.Ordinal);
            }

            /* The control: the identical Contains form finds a plausible-but-absent claim nowhere, so the
               silences above are absences rather than a matcher that never matches. */
            Assert.DoesNotContain(
                "the store's migrated rung is reported beside it",
                description,
                StringComparison.Ordinal);
        }
    }

    /* ---------------- helpers ---------------- */

    /// <summary>
    /// Every file in the tree that DEFINES a <c>get_collection_health</c> MCP tool — the same
    /// discovered-not-listed discipline as <see cref="AlertReadFailureSurfaceTests"/>' census, for the same
    /// reason: a positive check over the two files this change touched would confirm its own work and see
    /// nothing else.
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

        Assert.True(
            found.Count >= 2,
            $"the whole-tree walk for get_collection_health tool definitions found {found.Count} file(s) "
            + "under " + root + " — a walk that reaches one file or none cannot make a parity claim");

        return found;
    }

    /// <summary>The field names inside a tool's <c>service = new { … }</c> initializer, extracted the way
    /// the sibling census extracts <c>alert_read_health</c>'s: brace-balanced from the initializer's own
    /// opening, assignments matched only at line starts so prose in comments cannot join the set.</summary>
    private static SortedSet<string> ServiceFieldNames(string source)
    {
        var at = source.IndexOf("service = new", StringComparison.Ordinal);
        Assert.True(at > 0, "a get_collection_health tool no longer builds a service block");

        var open = source.IndexOf('{', at);
        Assert.True(open > 0, "the service block has no initializer");

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

        Assert.True(end > open, "the service block's initializer never closes");

        var body = source[open..end];

        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(body, @"^\s*([a-z_][a-z0-9_]*)\s*=\s*[^=]", RegexOptions.Multiline))
        {
            names.Add(m.Groups[1].Value);
        }

        Assert.True(names.Count > 0, "no fields were extracted from a service block");

        return names;
    }

    /// <summary>The raw span of the tool's <c>Description(...)</c> attribute argument — quoted literals and
    /// the concatenated constant's NAME, which is enough for phrases that live in the literals. The full
    /// client-visible reassembly (with the constant substituted) belongs to the sibling family's
    /// byte-identity pin; this lighter read exists so a claim census cannot be satisfied by the same words
    /// in a code comment.</summary>
    private static string DescriptionSpan(string source)
    {
        var call = Regex.Match(
            source,
            @"\[McpServerTool\(Name = ""get_collection_health""\), Description\((.*?)\)\]",
            RegexOptions.Singleline);

        Assert.True(call.Success, "a get_collection_health tool has no Description attribute in the expected shape");

        return call.Groups[1].Value;
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#3453 scan target not found: {path}");

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
