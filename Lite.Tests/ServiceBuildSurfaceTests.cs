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

    /* ---------------- helpers ---------------- */

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
