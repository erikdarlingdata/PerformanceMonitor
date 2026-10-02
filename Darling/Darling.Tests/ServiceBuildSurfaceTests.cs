/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3453: the MCP surface can say what build the service is, and each fact on the <c>service</c> block is
/// the authority's own read rather than a parallel one that can drift.
///
/// <para><b>The gap this closes.</b> <c>counting_since</c> moving proves a RESTART, not a new build — a
/// crash-restart reads identically to an install — and a capability fingerprint identifies a build only
/// when the change in question happens to move a fingerprintable surface. The one question an operator
/// watching an install actually asks, "is the running service the build that carries fix X", had no direct
/// read. Now it has one, and these pins hold the property that makes it trustworthy: every field is served
/// from the thing that already owns the fact. The version is <see cref="DarlingCliCommands.ProductVersion"/>
/// — the METHOD <c>--version</c> prints, not a re-derivation beside it — so the two surfaces cannot
/// disagree. The rung is the compiled constant, so the field moves with every future migration by
/// construction instead of waiting for someone to remember a literal. The start instant is the counter's
/// own <c>CountingSinceUtc</c>, so the payload carries ONE clock for "when this process came up" rather
/// than two near-identical stamps whose skew a reader would have to explain away.</para>
///
/// <para><b>Division of labour with the Lite side.</b> The cross-SKU censuses — both tools carry the block,
/// identical field sets, the shared description documents it — live in <c>Lite.Tests</c>'
/// <c>CrossSkuSurfaceSourceTests</c>, beside the #3013 parity family whose whole-tree discovery they reuse,
/// and this project compiles that file through a linked <c>Compile</c> item (#3938).
/// This file pins what only Darling can pin: its own surface's spellings, and the behaviour of the reads
/// those spellings name.</para>
/// </summary>
public sealed class ServiceBuildSurfaceTests
{
    [Fact]
    public void TheServiceBlock_ServesEachFact_FromTheAuthoritysOwnRead()
    {
        /* Spellings, not paraphrases: the pin is that the tool calls the SAME expression the authority
           exposes, because a re-derivation beside an authority is the drift #3222 documents one layer up
           (six version declarations, four values). */
        var tool = ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");

        Assert.Contains("service = new", tool, StringComparison.Ordinal);

        /* The CLI's method, not the attribute read repeated. DarlingCliCommandsTests holds what that method
           DOES (informational version, build metadata stripped); this holds that the MCP surface serves it
           rather than a sibling that could normalize differently. */
        Assert.Contains("version = DarlingCliCommands.ProductVersion()", tool, StringComparison.Ordinal);

        /* The counter's own instant, so started_at and counting_since are one clock by construction —
           the same reading, the same round-trip "o" rendering. */
        Assert.Contains(
            "started_at = alertReads.CountingSinceUtc.ToString(\"o\")",
            tool,
            StringComparison.Ordinal);

        /* The compiled constant. ScaffoldTests pins the constant to the highest migration, so through this
           reference the field equals the top rung on every future rung with no edit here — which is the
           property a numeral in this file could never have. */
        Assert.Contains(
            "compiled_schema_version = StorageVersion.SchemaVersion",
            tool,
            StringComparison.Ordinal);

        /* The two drifts worth refusing by name. A literal rung would be correct on the day it is written
           and stale on the next rung; VersionText.Normalize is the server list's DISPLAY form, which
           collapses to Major.Minor.Build and would strip the nightly's prerelease stamp — the exact part
           that distinguishes one nightly from the next, and so the exact part #3453 exists to expose. */
        Assert.False(
            Regex.IsMatch(tool, @"compiled_schema_version\s*=\s*\d"),
            "the compiled rung is being served as a literal, which goes stale on the next migration");
        Assert.DoesNotContain("version = VersionText", tool, StringComparison.Ordinal);
    }

    [Fact]
    public void TheVersionServed_IsTheAttributeRead_ExactlyAsTheCliPrintsIt()
    {
        /* The expectation is computed HERE from the assembly attribute, by the documented mechanics
           (informational version, any SemVer build-metadata suffix stripped, prerelease KEPT), so this pin
           fails if ProductVersion ever starts normalizing away the prerelease — the nightly stamp lives
           there, and stripping it would make every nightly of a release indistinguishable on the one
           surface built to distinguish them. */
        var assembly = typeof(DarlingCliCommands).Assembly;
        var informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        Assert.False(string.IsNullOrWhiteSpace(informational));

        var plus = informational!.IndexOf('+', StringComparison.Ordinal);
        var expected = plus >= 0 ? informational[..plus] : informational;

        var version = DarlingCliCommands.ProductVersion();

        Assert.Equal(expected, version);
        Assert.False(string.IsNullOrWhiteSpace(version));
        Assert.DoesNotContain('+', version);

        /* The leading component parses as a version — the same floor the CLI's own pin holds, restated
           here because this surface serializes the value into JSON a fleet scan will compare. */
        Assert.NotNull(Version.Parse(version.Split('-', '+')[0]));
    }

    [Fact]
    public void StartedAt_RendersTheCountingInstant_AsRoundTripUtc()
    {
        /* The block's stamp is alertReads.CountingSinceUtc.ToString("o") — pinned as that expression by the
           source scan above — so this holds the behaviour of that exact rendering: ISO-8601, UTC-marked,
           and round-trippable to the instant the counter was constructed with. A stamp that parsed to
           Unspecified kind would make "since when" depend on the reader's local zone, which for a
           restart detector is a wrong answer by up to a day. */
        var started = new DateTime(2026, 9, 15, 1, 2, 3, 456, DateTimeKind.Utc);
        var counter = new AlertReadFailureCounter(() => started);

        var rendered = counter.ReadFor("never-seen").CountingSinceUtc.ToString("o");

        Assert.EndsWith("Z", rendered, StringComparison.Ordinal);

        var parsed = DateTime.Parse(rendered, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
        Assert.Equal(started, parsed);
    }
}
