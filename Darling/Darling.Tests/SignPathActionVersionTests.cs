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
/// Every SignPath signing step in the release workflow submits through
/// <c>signpath/github-action-submit-signing-request@v3</c> (#4218).
///
/// <para><b>Why v3, and why all five together.</b> v2 still works, but v3 is the current version of the
/// action: it sends signing requests to SignPath's Pipeline Connector instead of the connector v2 used.
/// The only functional difference is the default <c>connector-url</c>. PerformanceStudio moved first
/// (erikdarlingdata/PerformanceStudio#538) and its v1.27.0 release then failed to sign
/// (erikdarlingdata/PerformanceStudio#569): the Pipeline Connector needs the SignPath GitHub App
/// installed on the repository, which v2's connector did not need, and the app was not installed yet.
/// That app has been installed on this repository since 2026-09-24 (#4218), so the same move here does
/// not hit that failure. Five separate steps submit a signing request in <c>build.yml</c> — Lite,
/// Darling, Lite (Velopack), Darling Viewer (Velopack), and the executables Velopack generates — and
/// #4218 asks that all five move in one PR, so a release never sends some requests through the old
/// connector and others through the new one.</para>
///
/// <para><b>Why this reads <c>name:</c> and not just counts <c>uses:</c> lines.</b> Asserting a count
/// alone would pass just as well if two of the five steps were accidentally duplicated and the other
/// three deleted. Naming the five steps the issue names means a renamed or removed signing step fails
/// this test with a message that says which one, not just a wrong total.</para>
/// </summary>
public sealed class SignPathActionVersionTests
{
    private const string ReleaseWorkflow = ".github/workflows/build.yml";
    private const string ActionName = "signpath/github-action-submit-signing-request";
    private const string PinnedVersion = "@v3";

    private static readonly string[] ExpectedStepNames =
    [
        "Sign Lite",
        "Sign Darling",
        "Sign Lite (Velopack)",
        "Sign Darling Viewer (Velopack)",
        "Sign the executables Velopack generates",
    ];

    [Fact]
    public void ExactlyTheFiveDocumentedSteps_SubmitASigningRequest()
    {
        var steps = SigningSteps();

        var names = steps.Select(step => step.Name).OrderBy(name => name, StringComparer.Ordinal).ToList();
        var expected = ExpectedStepNames.OrderBy(name => name, StringComparer.Ordinal).ToList();

        Assert.True(
            names.SequenceEqual(expected, StringComparer.Ordinal),
            "the SignPath signing steps in build.yml no longer match the five #4218 names:\n"
            + $"  found:    {string.Join(", ", names)}\n"
            + $"  expected: {string.Join(", ", expected)}");
    }

    [Fact]
    public void EverySigningStep_SubmitsThroughActionVersionThree()
    {
        var steps = SigningSteps();

        var offending = steps
            .Where(step => step.UsesLine.Trim() != $"uses: {ActionName}{PinnedVersion}")
            .ToList();

        Assert.True(
            offending.Count == 0,
            $"a SignPath signing step is not pinned to {ActionName}{PinnedVersion}. v2's connector does "
            + "not need the SignPath GitHub App and a mixed pin would let some release requests bypass "
            + "the Pipeline Connector this repository now relies on (#4218):\n  "
            + string.Join("\n  ", offending.Select(step => $"{step.Name}: {step.UsesLine.Trim()}")));
    }

    /// <summary>Every step in the workflow whose <c>uses:</c> line names the SignPath signing action,
    /// paired with the <c>name:</c> line immediately above it. A structural scan over the raw lines
    /// rather than a full YAML parse, matching every other workflow-reading pin in this project
    /// (<see cref="NightlyVersionInjectionTests"/>).</summary>
    private static List<(string Name, string UsesLine)> SigningSteps()
    {
        var lines = ReadRepoFileLf(ReleaseWorkflow).Split('\n');

        var steps = new List<(string Name, string UsesLine)>();
        string? currentName = null;

        foreach (var rawLine in lines)
        {
            var trimmed = rawLine.TrimStart();

            if (trimmed.StartsWith("- name:", StringComparison.Ordinal))
            {
                currentName = trimmed["- name:".Length..].Trim().Trim('\'', '"');
                continue;
            }

            if (trimmed.StartsWith("uses:", StringComparison.Ordinal)
                && trimmed.Contains(ActionName, StringComparison.Ordinal))
            {
                Assert.True(
                    currentName is not null,
                    $"found a {ActionName} uses: line with no preceding name: line to attribute it to");

                steps.Add((currentName!, trimmed));
                currentName = null;
            }
        }

        /* Non-vacuity: a scan that finds nothing must fail here rather than pass every assertion above
           by having nothing to judge (the same guard NightlyVersionInjectionTests.PublishLines uses). */
        Assert.True(
            steps.Count > 0,
            $"no {ActionName} steps found in {ReleaseWorkflow}; the scan is not reading the file");

        return steps;
    }
}
