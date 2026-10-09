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
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every <c>actions/checkout</c> in <c>nightly.yml</c> checks out the commit the run was dispatched at,
/// never a branch name (#5616).
///
/// <para><b>The defect this closes was a moving target.</b> Seven checkouts carried
/// <c>ref: ${{ github.event_name == 'workflow_dispatch' &amp;&amp; github.ref_name || 'dev' }}</c>.
/// <c>github.ref_name</c> is the branch NAME, so each job checked out the branch tip at the moment that
/// job started. A merge to dev in the middle of a run made the later jobs (the test shards, publish)
/// build a different commit than the earlier ones, so dev had to freeze for the length of every nightly.
/// <c>github.sha</c> is fixed when the run is dispatched, so every job sees the same tree.</para>
///
/// <para><b>Why a pin.</b> Each checkout is a hand-copied block, and a new job copied from an old
/// workflow, or from the branch-name spelling in a doc, brings the moving target back with no visible
/// difference until two jobs of one run disagree about what they built.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class NightlyCheckoutCommitPinTests
{
    private const string NightlyWorkflow = ".github/workflows/nightly.yml";

    private static readonly Regex JobKey = new(@"^  ([A-Za-z0-9_-]+):\s*$", RegexOptions.Compiled);
    private static readonly Regex RefKey = new(@"^\s*ref:\s*(.*)$", RegexOptions.Compiled);
    private static readonly Regex BranchRef = new(@"github\.(ref_name|ref|head_ref|base_ref)\b", RegexOptions.Compiled);

    private sealed record Checkout(string Job, int Line, string? RefExpression);

    private static List<string> Lines() => ReadRepoFile(NightlyWorkflow).Split('\n').ToList();

    /// <summary>The job each line belongs to: the last 2-space-indented key under <c>jobs:</c>.</summary>
    private static string[] JobOfLine(List<string> lines)
    {
        var jobs = new string[lines.Count];
        var inJobs = false;
        var current = "";
        for (var i = 0; i < lines.Count; i++)
        {
            var line = lines[i].TrimEnd('\r');
            if (line.StartsWith("jobs:", StringComparison.Ordinal))
            {
                inJobs = true;
            }
            else if (inJobs)
            {
                var m = JobKey.Match(line);
                if (m.Success)
                {
                    current = m.Groups[1].Value;
                }
            }

            jobs[i] = current;
        }

        return jobs;
    }

    private static List<Checkout> Checkouts()
    {
        var lines = Lines();
        var jobs = JobOfLine(lines);
        var found = new List<Checkout>();
        for (var i = 0; i < lines.Count; i++)
        {
            if (!lines[i].Contains("uses: actions/checkout@", StringComparison.Ordinal))
            {
                continue;
            }

            /* The step's `with:` block: lines after the `uses:` line indented deeper than the step's own
               `- uses` dash, up to the next line that is not. */
            var stepIndent = lines[i].IndexOf('-');
            string? expression = null;
            for (var j = i + 1; j < lines.Count; j++)
            {
                var next = lines[j].TrimEnd('\r');
                if (next.Trim().Length == 0 || next.TrimStart().StartsWith('#'))
                {
                    continue;
                }

                var indent = next.Length - next.TrimStart().Length;
                if (indent <= stepIndent)
                {
                    break;
                }

                var m = RefKey.Match(next);
                if (m.Success)
                {
                    expression = m.Groups[1].Value.Trim();
                    break;
                }
            }

            found.Add(new Checkout(jobs[i], i + 1, expression));
        }

        return found;
    }

    [Fact]
    public void NightlyWorkflow_HasCheckouts_AndTheScanSeesThem()
    {
        /* A scan that finds nothing passes every pin below vacuously. Seven jobs check out a ref and two
           more check out the run's commit directly, so a parse that drops below that has lost its way. */
        Assert.True(
            Checkouts().Count >= 9,
            "The checkout scan found fewer than 9 actions/checkout steps in nightly.yml; the parser, not the "
            + "workflow, has probably broken.");
    }

    [Fact]
    public void EveryNightlyCheckout_ResolvesToTheRunsCommit()
    {
        var offending = Checkouts()
            .Where(c => c.RefExpression is null
                || !c.RefExpression.Contains("github.sha", StringComparison.Ordinal))
            .Select(c => $"job '{c.Job}' (line {c.Line}): ref = {c.RefExpression ?? "<none>"}")
            .ToList();

        Assert.True(
            offending.Count == 0,
            "A nightly checkout without a ref that resolves to github.sha builds the default branch's tip, "
            + "or the dispatched branch's tip at the moment that job starts, so a merge to dev in the middle "
            + "of a run makes later jobs build a different commit than earlier ones (#5616):\n  "
            + string.Join("\n  ", offending));
    }

    [Fact]
    public void NoNightlyRef_UsesABranchName()
    {
        var offending = Lines()
            .Select((text, index) => (text: text.TrimEnd('\r'), line: index + 1))
            .Where(x => RefKey.IsMatch(x.text) && BranchRef.IsMatch(x.text))
            .Select(x => $"line {x.line}: {x.text.Trim()}")
            .ToList();

        Assert.True(
            offending.Count == 0,
            "A `ref:` built from a branch name (github.ref_name, github.ref, github.head_ref, github.base_ref) "
            + "resolves to the branch tip when each job starts, not to the commit the run was dispatched at "
            + "(#5616):\n  " + string.Join("\n  ", offending));
    }

    [Fact]
    public void TheRedispatchJob_HasNoCheckout()
    {
        /* The redispatch job only runs `gh workflow run ... --ref dev`; the run it starts carries dev's
           tip as its github.sha. A checkout added there would be the one place the rule above does not
           reach, because the job runs from the scheduled event, where the dispatch expression falls to
           the 'dev' branch name. */
        var checkouts = Checkouts();
        Assert.Contains(checkouts, c => c.Job != "redispatch");
        Assert.DoesNotContain(checkouts, c => c.Job == "redispatch");
    }
}
