/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every build the nightly workflow publishes receives the derived nightly version as an msbuild
/// property, and no release build does (#3455).
///
/// <para><b>The defect this closes was a name without a stamp.</b> <c>nightly.yml</c> derives its
/// prerelease string once per run (#3390) and named every artifact with it, but no publish ever passed
/// it to msbuild, so the assemblies inside those artifacts stamped the clean release string
/// <c>Directory.Build.props</c> declares. The first live reading of #3453's <c>service</c> block —
/// built precisely to answer "is the running service the build that carries fix X" — came back bare
/// <c>3.7.0</c> on a <c>.438</c> nightly, on every store that had installed it. The informational
/// version is the one surface that read and the CLI's <c>--version</c> share, and it derives from the
/// version property at build time, so a publish that does not pass the property cannot stamp the
/// nightly, however correct the artifact's name is.</para>
///
/// <para><b>Why a pin and not the workflow comment alone.</b> The stamp's own #3390 rationale already
/// CLAIMED the binaries carried it while they did not — prose describing intent outlives the mechanics
/// it describes, which is the drift these source-parsing tests exist to catch. The routes enumerated:
/// a publish step arrives in <c>nightly.yml</c> without the flag, the ordinary fate of hand-copied
/// steps (#2384's checksum fix landing in one workflow and not the other is the local precedent); the
/// container's inner publish loses the arg threading on either side of the <c>docker build</c>
/// boundary, where each half alone reads as complete at its own file; and the inverse, a version
/// property leaking into <c>build.yml</c>, which would take the release binaries' stamp away from the
/// one declaration the release process bumps and check-version-bump.yml gates (#3222).</para>
/// </summary>
public sealed class NightlyVersionInjectionTests
{
    private const string NightlyWorkflow = ".github/workflows/nightly.yml";
    private const string ReleaseWorkflow = ".github/workflows/build.yml";
    private const string ContainerFile = "Darling/Dockerfile";

    /// <summary>The exact spelling every nightly publish must carry: the version step's own output,
    /// not a re-derivation beside it — the same one-authority rule the version-set steps already obey
    /// for the artifact names.</summary>
    private const string VersionFlag = "-p:Version=${{ steps.version.outputs.VERSION }}";

    [Fact]
    public void EveryNightlyPublish_PassesTheDerivedVersionToMsBuild()
    {
        var offending = PublishLines(NightlyWorkflow)
            .Where(line => !line.Contains(VersionFlag, StringComparison.Ordinal))
            .ToList();

        Assert.True(
            offending.Count == 0,
            "A nightly publish that does not pass the derived version to msbuild builds assemblies "
            + "stamping the clean release string inside an artifact named nightly, and two same-day "
            + "nightlies become indistinguishable at the read built to distinguish them (#3455):\n  "
            + string.Join("\n  ", offending));
    }

    [Fact]
    public void NoReleasePublish_PassesAVersionProperty()
    {
        var offending = PublishLines(ReleaseWorkflow)
            .Where(line => line.Contains("-p:Version", StringComparison.Ordinal))
            .ToList();

        Assert.True(
            offending.Count == 0,
            "A release publish passing a version property takes the release binaries' stamp away from "
            + "the one declaration the release process bumps (#3222). Release builds stay clean by "
            + "CONSTRUCTION — the property flows only where a prerelease stamp exists to flow:\n  "
            + string.Join("\n  ", offending));
    }

    [Fact]
    public void TheNightlyContainerBuild_ThreadsTheVersionThroughTheImageBoundary()
    {
        /* Both halves or neither. The workflow passing an arg the Dockerfile does not declare stamps
           nothing; the Dockerfile declaring an arg the workflow does not pass stamps the clean string.
           Either failure leaves a :nightly image whose service answers with the release string, and
           each half alone reads as complete at its own file. */
        var dockerBuilds = ReadRepoFileLf(NightlyWorkflow)
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("docker build", StringComparison.Ordinal))
            .ToList();

        var build = Assert.Single(dockerBuilds);
        Assert.Contains("--build-arg VERSION=\"${version}\"", build, StringComparison.Ordinal);

        /* Continuations joined before the line scan: the publish RUN is split across lines, and a
           per-line scan would read the flag's line as not containing a publish and vice versa. */
        var containerFile = ReadRepoFileLf(ContainerFile).Replace("\\\n", " ", StringComparison.Ordinal);

        Assert.Contains("ARG VERSION=", containerFile, StringComparison.Ordinal);

        var publishes = containerFile
            .Split('\n')
            .Where(line => line.Contains("dotnet publish", StringComparison.Ordinal))
            .ToList();

        var inner = Assert.Single(publishes);

        /* The conditional expansion, not a bare flag: an empty arg must add NOTHING, so a release or
           hand-run docker build keeps publishing against the declared version rather than against an
           empty property that would override it with nothing. */
        Assert.Contains("${VERSION:+-p:Version=$VERSION}", inner, StringComparison.Ordinal);
    }

    /// <summary>The dotnet publish commands in one workflow, comment lines excluded so a rationale
    /// quoting a command is not judged as one.</summary>
    private static List<string> PublishLines(string workflow)
    {
        var lines = ReadRepoFileLf(workflow)
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => !line.StartsWith("#", StringComparison.Ordinal))
            .Where(line => line.Contains("dotnet publish ", StringComparison.Ordinal))
            .ToList();

        /* Non-vacuity: nightly.yml publishes four windows trees and one linux tree, build.yml six.
           A scan reading nothing must fail here rather than satisfy every assertion above by having
           nothing to judge. */
        Assert.True(
            lines.Count >= 5,
            $"only {lines.Count} publish lines found in {workflow}; the scan is not reading the file");

        return lines;
    }
}
