/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the list of classes that carry the <c>Cost</c> trait with the value <c>Slow</c> in Darling.Tests and Lite.Tests, and the CI wiring
/// that acts on the tag (#5459, change 5).
///
/// <para><b>What the tag does.</b> A pull request run leaves a tagged class out unless the change reaches it
/// (<c>ci-select.py slow-skip</c>: the class's own source file, a file whose name the class's source mentions, a
/// script or style file for the Node-driven classes, a test-project or build input). A push to dev, a merge-queue run,
/// the nightly and a release run every class. So the tag moves a class's cost off the pull request and onto the next
/// push, which is a fair trade only for a class that is slow, has never failed, and guards nothing a pull request
/// must stop at the door.</para>
///
/// <para><b>Why a pinned list.</b> A class must not gain or lose the tag silently. Adding a class means adding it
/// to <see cref="s_slow"/> with the reason, in the same change; the failure message names the class. The classes in
/// <see cref="s_keptOnPullRequests"/> were measured with the rest and stay on pull requests; tagging one fails
/// here, because each guards a security boundary, an upgrade path or a shared path.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class SlowClassCensusTests
{
    private const string Darling = "darling";
    private const string Lite = "lite";

    /// <summary>The tagged classes, each with the suite and why it may wait for the push to dev.</summary>
    private static readonly Dictionary<string, (string Suite, string Why)> s_slow = new(StringComparer.Ordinal)
    {
        ["WebDataStartNoteTests"] = (Darling, "no store: the note's list and the page code under Node; 0 failures in 30 days"),
        ["WebDataStartNoteLiveTests"] = (Darling, "the note's store side; 0 failures in 30 days"),
        ["ViewerTextLookupWindowBoundLiveTests"] = (Darling, "a plan-shape read bound on a scratch store; 0 failures in 30 days"),
        ["AdminServerEditBehaviourTests"] = (Darling, "the Admin page's edit flow under Node; 0 failures in 30 days"),
        ["DeadlockProcessRowsTests"] = (Darling, "a pure read shaper; 0 failures in 30 days"),
        ["ComposeHourlyRawEdgesGuardLiveTests"] = (Darling, "compose routing edges on a scratch store; 0 failures in 30 days"),
        ["QueryStoreIntervalWideBelowFloorLiveTests"] = (Darling, "a Query Store read bound on a scratch store; 0 failures in 30 days"),
        ["WebPerfmonMultiSelectBehaviourTests"] = (Darling, "a web picker under Node; 0 failures in 30 days"),
        ["WebChartAtTimeBehaviourTests"] = (Darling, "web chart drill-downs under Node; 0 failures in 30 days"),
        ["WebWaitStatsMultiSelectBehaviourTests"] = (Darling, "a web picker under Node; 0 failures in 30 days"),
        ["ComposeTimeoutPhaseLiveTests"] = (Darling, "both compose timeout phases on a real store; 0 failures in 30 days"),
        ["PgServerLogTailCsvJsonRotationLiveTests"] = (Darling, "log-tail rotation on dedicated clusters; 0 failures in 30 days"),
        ["ProcedureTrendRunModelLiveTests"] = (Darling, "a trend read on a scratch store; 0 failures in 30 days"),
        ["GridAdoptionBehaviourTests"] = (Darling, "web grid column filters under Node; 0 failures in 30 days"),
        ["PlanViewerRuntimeSummaryNestingTests"] = (Darling, "WPF margins read back through reflection; 0 failures in 30 days"),
        ["McpWindowNoticeSystemHealthToolTests"] = (Lite, "a window notice on one MCP tool family; 0 failures in 30 days"),
        ["McpWindowNoticeConfigAndLogToolTests"] = (Lite, "a window notice on one MCP tool family; 0 failures in 30 days"),
        ["McpWindowNoticeEventToolTests"] = (Lite, "a window notice on one MCP tool family; 0 failures in 30 days"),
        ["McpWindowNoticeToolTests"] = (Lite, "a window notice on one MCP tool family; 0 failures in 30 days"),
        ["McpWindowNoticeAggregateToolTests"] = (Lite, "a window notice on one MCP tool family; 0 failures in 30 days"),
        ["ServerClockDstReadTests"] = (Lite, "four read sites across a DST change; 0 failures in 30 days"),
        ["QueryStatsTextAfterRankingTests"] = (Lite, "the memory shape of two reads; 0 failures in 30 days"),
        ["ProcedureStatsIdleRowsReaderTests"] = (Lite, "idle-row handling in the readers; 0 failures in 30 days"),
        ["LongQueryTraceLifecycleLiteTests"] = (Lite, "trace session lifecycle on Azure SQL Database; 0 failures in 30 days"),
        ["DataStartBannerSurfaceTests"] = (Lite, "the Showing-since note per surface; 0 failures in 30 days"),
        ["DataStartBannerQueriesTabTests"] = (Lite, "the Showing-since note on the Queries tab; 0 failures in 30 days"),
        ["BlockingChartsDataStartTests"] = (Lite, "the Showing-since note on the Blocking charts; 0 failures in 30 days"),
    };

    /// <summary>Slow classes measured with the rest that stay on pull requests, each with the reason.</summary>
    private static readonly Dictionary<string, string> s_keptOnPullRequests = new(StringComparer.Ordinal)
    {
        ["DarlingPasswordKeyStoreLiveTests"] = "passwords and keys",
        ["ServerEditPasswordRuleViewerTests"] = "the password rules of the server edit",
        ["DarlingStoreUpgradeTests"] = "the store upgrade path",
        ["ManagedConfUpgradePathTests"] = "the managed configuration upgrade path",
        ["DarlingManagedPostgresTests"] = "the managed runtime, 13 failures in 30 days",
        ["AlertNotebookEndpointTests"] = "a census over the shared read-dispatch table",
        ["ArchiveResetRestoresStateTests"] = "archive reset, a data-loss path; 3 failures in 30 days",
        ["ArchiveGroupedWatermarkReadTests"] = "archive reads, a data-loss path",
        ["StoredEventCopiesTests"] = "1 failure in 30 days",
        ["FactCollectorTests"] = "1 failure in 30 days",
        ["ScenarioTests"] = "6 failures in 30 days",
        ["AnomalyDetectorTests"] = "4 failures in 30 days",
    };

    private static readonly Regex s_classDecl = new(
        @"^[ \t]*(?:(?:public|internal|private|protected|sealed|static|abstract|partial)\s+)*class\s+(\w+)",
        RegexOptions.Compiled);

    /// <summary>A line of the attribute and comment block above a declaration (the same lines ci-select.py reads).</summary>
    private static readonly Regex s_attributeOrComment = new(@"^(\[|//|/\*|\*)", RegexOptions.Compiled);

    private static string Root([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

    /// <summary>{class: (suite, trait values on its declaration)} for every class of both suites that carries any
    /// <c>Cost</c> trait; the attribute block above the declaration is read the way ci-select.py reads it.</summary>
    private static Dictionary<string, (string Suite, HashSet<string> Traits)> Scan()
    {
        var found = new Dictionary<string, (string, HashSet<string>)>(StringComparer.Ordinal);
        foreach (var (suite, rel) in new[] { (Darling, Path.Combine("Darling", "Darling.Tests")), (Lite, "Lite.Tests") })
        {
            var dir = Path.Combine(Root(), rel);
            foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar)
                         && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar)))
            {
                var lines = File.ReadAllLines(file);
                for (var n = 0; n < lines.Length; n++)
                {
                    var m = s_classDecl.Match(lines[n]);
                    if (!m.Success)
                    {
                        continue;
                    }

                    var traits = new HashSet<string>(StringComparer.Ordinal);
                    for (var j = n - 1; j >= 0; j--)
                    {
                        var s = lines[j].Trim();
                        if (!(s_attributeOrComment.IsMatch(s) || s.EndsWith("*/", StringComparison.Ordinal)))
                        {
                            break;
                        }

                        foreach (Match t in Regex.Matches(s, "Trait\\(\\s*\"(\\w+)\"\\s*,\\s*\"(\\w+)\"\\s*\\)"))
                        {
                            traits.Add(t.Groups[1].Value + "=" + t.Groups[2].Value);
                        }
                    }

                    if (found.TryGetValue(m.Groups[1].Value, out var prev))
                    {
                        prev.Item2.UnionWith(traits);
                    }
                    else
                    {
                        found[m.Groups[1].Value] = (suite, traits);
                    }
                }
            }
        }

        return found;
    }

    private static (int ExitCode, string Output) RunScript(params string[] args)
    {
        var psi = new ProcessStartInfo("python")
        {
            WorkingDirectory = Root(),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add(Path.Combine(Root(), ".github", "scripts", "ci-select.py"));
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi) ?? throw new InvalidOperationException("python did not start");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        Assert.True(p.WaitForExit(120_000), "ci-select.py did not finish in two minutes");
        return (p.ExitCode, stdout.Result);
    }

    private static string[] Lines(string output) =>
        output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    [Fact]
    public void TheTaggedClasses_AreExactlyThePinnedList()
    {
        var tagged = Scan().Where(kv => kv.Value.Traits.Contains("Cost=Slow")).ToDictionary(kv => kv.Key, kv => kv.Value.Suite);
        var pinned = s_slow.ToDictionary(kv => kv.Key, kv => kv.Value.Suite);

        var gained = tagged.Keys.Except(pinned.Keys).OrderBy(x => x, StringComparer.Ordinal).ToList();
        var lost = pinned.Keys.Except(tagged.Keys).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.True(gained.Count == 0,
            "classes carry the Cost=Slow trait without a row in SlowClassCensusTests.s_slow (add the row with the reason, "
            + "or remove the tag): " + string.Join(", ", gained));
        Assert.True(lost.Count == 0,
            "classes are pinned as Cost=Slow but no longer carry the tag (restore it, or remove the row): " + string.Join(", ", lost));
        foreach (var (name, suite) in tagged)
        {
            Assert.True(pinned[name] == suite, name + " is pinned under the wrong suite");
        }

        Assert.True(s_slow.Count >= 20, "anti-vacuity: the pinned list shrank below 20 classes");
    }

    [Fact]
    public void ATaggedClass_IsNeverAGuardClass_NorOneTheDarlingReadsLegChooses()
    {
        // The Guard stage runs on every pull request, and the Lite reads leg exists to run the Reads=Darling classes
        // when a Darling read changes. A Slow tag on either would be dropped or contradicted by the other rule.
        var scan = Scan();
        foreach (var name in s_slow.Keys)
        {
            var traits = scan[name].Traits;
            Assert.DoesNotContain("Stage=Guard", traits);
            Assert.DoesNotContain("Reads=Darling", traits);
        }
    }

    [Fact]
    public void TheKeptClasses_AreNotTagged()
    {
        var scan = Scan();
        foreach (var (name, why) in s_keptOnPullRequests)
        {
            Assert.True(scan.ContainsKey(name), name + " is listed as kept on pull requests but no such class exists; update the list");
            Assert.False(scan[name].Traits.Contains("Cost=Slow"), name + " must stay on pull requests (" + why + ") but carries the Slow tag");
        }
    }

    [Fact]
    public void ThePullRequestRule_LeavesOutEveryTaggedClass_ForAnUnrelatedChange_AndNoneOtherwise()
    {
        foreach (var suite in new[] { Darling, Lite })
        {
            var expected = s_slow.Where(kv => kv.Value.Suite == suite).Select(kv => kv.Key).OrderBy(x => x, StringComparer.Ordinal).ToArray();

            var (code, output) = RunScript("slow-skip", "--suite", suite, "--event", "pull_request", "SECURITY.md");
            Assert.Equal(0, code);
            Assert.Equal(expected, Lines(output).OrderBy(x => x, StringComparer.Ordinal).ToArray());

            // Every event but a pull request runs every class.
            foreach (var evt in new[] { "push", "merge_group", "release" })
            {
                Assert.Empty(Lines(RunScript("slow-skip", "--suite", suite, "--event", evt, "SECURITY.md").Output));
            }

            // An unknown change (no file list) skips nothing.
            Assert.Empty(Lines(RunScript("slow-skip", "--suite", suite, "--event", "pull_request").Output));

            // The workflow, the selector and a project file reach every class.
            foreach (var input in new[] { ".github/workflows/build.yml", "Lite/PerformanceMonitorLite.csproj" })
            {
                Assert.Empty(Lines(RunScript("slow-skip", "--suite", suite, "--event", "pull_request", input).Output));
            }
        }
    }

    [Fact]
    public void AChange_ThatReachesATaggedClass_PutsItBack()
    {
        // Its own test file.
        var own = Lines(RunScript("slow-skip", "--suite", Darling, "--event", "pull_request",
            "Darling/Darling.Tests/DeadlockProcessRowsTests.cs").Output);
        Assert.DoesNotContain("DeadlockProcessRowsTests", own);
        Assert.Contains("WebDataStartNoteTests", own);

        // A product file the test source names.
        var product = Lines(RunScript("slow-skip", "--suite", Lite, "--event", "pull_request",
            "Lite/Services/LocalDataService.cs").Output);
        Assert.DoesNotContain("McpWindowNoticeSystemHealthToolTests", product);
        Assert.Contains("LongQueryTraceLifecycleLiteTests", product);

        // A page script reaches the classes that run the shipped scripts under Node.
        var script = Lines(RunScript("slow-skip", "--suite", Darling, "--event", "pull_request",
            "Darling/PerformanceMonitor.Darling.Web/wwwroot/js/never-named-anywhere.js").Output);
        Assert.DoesNotContain("WebPerfmonMultiSelectBehaviourTests", script);
        Assert.DoesNotContain("GridAdoptionBehaviourTests", script);
        Assert.Contains("ComposeTimeoutPhaseLiveTests", script);

        // A non-source file inside the test project (a fixture, a harness) reaches every class of that suite.
        Assert.Empty(Lines(RunScript("slow-skip", "--suite", Darling, "--event", "pull_request",
            "Darling/Darling.Tests/web-perfmon-multi-harness.mjs").Output));
    }

    [Fact]
    public void TheWorkflow_AppliesTheRuleOnPullRequestsOnly_InEveryLegThatRunsTheseSuites()
    {
        var yml = File.ReadAllText(Path.Combine(Root(), ".github", "workflows", "build.yml"));

        // darling-pg shards and the build job's whole-suite pass (Darling); the Lite shards (Lite). The whole-tree guards job
        // is deliberately NOT here: CrossAppGuardCiGateTests pins its invocation whole, and the backstop exemptions rest on it.
        Assert.Equal(2, Regex.Matches(yml, @"slow-skip --suite darling --event \$env:SLOW_EVENT --repo \$env:SLOW_REPO --pr \$env:SLOW_PR").Count);
        Assert.Single(Regex.Matches(yml, @"slow-skip --suite lite --event \$env:SLOW_EVENT --repo \$env:SLOW_REPO --pr \$env:SLOW_PR"));

        // Each use sits inside an `$env:SLOW_EVENT -eq 'pull_request'` condition, so a push, a merge-queue run, the
        // nightly and a release run every class.
        Assert.Equal(3, Regex.Matches(yml, @"\$env:SLOW_EVENT -eq 'pull_request'").Count);

        // And the environment each of the three steps reads is set from the run.
        Assert.Equal(3, Regex.Matches(yml, @"SLOW_EVENT: \$\{\{ github\.event_name \}\}").Count);
        Assert.Equal(3, Regex.Matches(yml, @"SLOW_PR: \$\{\{ github\.event\.pull_request\.number \}\}").Count);
        Assert.Equal(3, Regex.Matches(yml, @"SLOW_REPO: \$\{\{ github\.repository \}\}").Count);
    }
}
