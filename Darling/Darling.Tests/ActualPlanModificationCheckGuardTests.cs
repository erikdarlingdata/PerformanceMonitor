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
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5639: every "Get Actual Plan" entry point, in Lite, the shared plan navigation and the Darling viewer, runs the
/// shared data-modification check (<see cref="QueryModificationDetector"/>) before it offers to run a query, builds the
/// prompt from that check's result with the shared <see cref="QueryModificationDetector.BuildConsentWarning"/>,
/// <see cref="QueryModificationDetector.BuildConsentTitle"/>, <see cref="QueryModificationDetector.BuildConsentTarget"/>
/// and <see cref="QueryModificationDetector.BuildConsentQueryText"/>, and runs the query only after the operator
/// answers OK. The prompts are WPF message boxes, so the wiring is read from source, the way
/// <see cref="ActualPlanConsentNoticeTests"/> reads it.
///
/// <para>The census is per CALL, not per file: every place that runs a query for its actual plan (the
/// <c>ExecuteForActualPlanAsync</c> executor, the shared navigation's executor delegate, the viewer's
/// <c>ExecuteActualPlanAsync</c> and the <c>execute_actual_plan</c> command) must either sit inside the argument list
/// of a <c>new PlanNavigationController(</c> (which runs the gate) or in a method that checks, prompts and tests the
/// answer BEFORE the call. A method added to any file that calls the executor without all three fails here.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class ActualPlanModificationCheckGuardTests
{
    private const string Controller = "PerformanceMonitor.Ui/PlanNavigationController.cs";
    private const string ViewerFlow = "Darling/PerformanceMonitor.Darling.Viewer/ViewerActualPlanFlow.cs";
    private const string ServerTabPlans = "Lite/Controls/ServerTab.Plans.cs";
    private const string FinOpsTab = "Lite/Controls/FinOpsTab.xaml.cs";

    /// <summary>The files that DEFINE an executor or handle the queued command on the service side: the consent is taken
    /// before the command is queued (the viewer flow), so these are not entry points.</summary>
    private static readonly string[] s_executorDefinitions =
    {
        "PerformanceMonitor.PlanAnalysis/ActualPlanExecutor.cs",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.ActualPlan.cs",
        "Darling/PerformanceMonitor.Darling.Service/ActualPlanRequest.cs",
        "Darling/PerformanceMonitor.Darling.Service/DarlingCommandExecutor.cs",
        "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs",
        "Darling/PerformanceMonitor.Darling.Service/ServiceCommandDeadlines.cs",
    };

    /// <summary>What runs a query for its actual plan.</summary>
    private static readonly Regex s_runCall = new(
        @"ExecuteForActualPlanAsync|\.ExecuteActualPlanAsync\b|(?<![A-Za-z0-9])_?executeActual\s*\(",
        RegexOptions.CultureInvariant);

    /// <summary>The queued-command name: a second way to run a query for its actual plan.</summary>
    private static readonly Regex s_commandName = new(
        @"CommandExecuteActualPlan|""execute_actual_plan""", RegexOptions.CultureInvariant);

    /// <summary>The answer test that ends a request: the OK/Cancel box answered other than OK, or the viewer's Confirm failing.</summary>
    private static readonly Regex s_answerTest = new(
        @"!=\s*MessageBoxResult\.OK\s*\)\s*(?:\{\s*)?return|if\s*\(\s*!Confirm\(", RegexOptions.CultureInvariant);

    [Fact]
    public void TheDetector_FlagsAWritingPlan_AndTheTitleSaysSo()
    {
        const string insertPlan = "<ShowPlanXML><StmtSimple StatementType=\"INSERT\" /></ShowPlanXML>";
        const string selectPlan = "<ShowPlanXML><StmtSimple StatementType=\"SELECT\" /></ShowPlanXML>";

        var writes = QueryModificationDetector.Detect(insertPlan, "INSERT dbo.t SELECT 1");
        var reads = QueryModificationDetector.Detect(selectPlan, "SELECT 1");
        var noPlan = QueryModificationDetector.Detect(null, "SELECT 1");

        Assert.True(writes.ModifiesData);
        Assert.False(reads.ModifiesData);
        Assert.True(noPlan.ModifiesData);
        Assert.True(noPlan.IsUncertain);

        Assert.Contains("DATA WILL BE MODIFIED", QueryModificationDetector.BuildConsentTitle(writes), StringComparison.Ordinal);
        Assert.Contains("DATA WILL BE MODIFIED", QueryModificationDetector.BuildConsentTitle(noPlan), StringComparison.Ordinal);
        Assert.DoesNotContain("DATA WILL BE MODIFIED", QueryModificationDetector.BuildConsentTitle(reads), StringComparison.Ordinal);
        Assert.Equal(string.Empty, QueryModificationDetector.BuildConsentWarning(reads, "db"));
    }

    /// <summary>
    /// The three gates, each read in its own method: the check, then the warning, the title, the target and the query
    /// text built from THAT check's result (not a second or hard-coded one), then the OK/Cancel box, then the answer
    /// test, and only then the run.
    /// </summary>
    [Theory]
    [InlineData(Controller, "GetActualPlanAsync")]
    [InlineData(ServerTabPlans, "GetActualPlan_Click")]
    public void EveryGate_ChecksThenPromptsFromTheCheckResult_ThenAsksBeforeItRuns(string relative, string method)
    {
        var body = MethodBody(StripComments(File.ReadAllText(Path.Combine(RepoRoot(), relative))), method);

        var detect = Regex.Match(body, @"var\s+(\w+)\s*=\s*QueryModificationDetector\.Detect\(", RegexOptions.CultureInvariant);
        Assert.True(detect.Success, $"{relative} {method} never calls QueryModificationDetector.Detect into a variable");
        var info = detect.Groups[1].Value;

        var warning = body.IndexOf($"QueryModificationDetector.BuildConsentWarning({info},", StringComparison.Ordinal);
        var title = body.IndexOf($"QueryModificationDetector.BuildConsentTitle({info})", StringComparison.Ordinal);
        var target = body.IndexOf("QueryModificationDetector.BuildConsentTarget(", StringComparison.Ordinal);
        var queryText = body.IndexOf("QueryModificationDetector.BuildConsentQueryText(", StringComparison.Ordinal);
        var prompt = body.IndexOf("MessageBoxButton.OKCancel", StringComparison.Ordinal);
        var answer = s_answerTest.Match(body);
        var run = s_runCall.Match(body);

        Assert.True(warning > detect.Index, $"{relative} {method}: the prompt's warning is not built from the Detect result '{info}'");
        Assert.True(title > detect.Index, $"{relative} {method}: the prompt's title is not built from the Detect result '{info}'");
        Assert.True(target > 0, $"{relative} {method}: the prompt does not name the server and database with BuildConsentTarget");
        Assert.True(queryText > 0, $"{relative} {method}: the prompt does not show the query text with BuildConsentQueryText");
        Assert.True(prompt > warning && prompt > title && prompt > target && prompt > queryText,
            $"{relative} {method}: the OK/Cancel box is shown before the prompt text is built");
        Assert.True(answer.Success && answer.Index > prompt, $"{relative} {method}: the answer is not tested right after the OK/Cancel box");
        Assert.True(run.Success && run.Index > answer.Index, $"{relative} {method}: the query runs before the answer is tested");
    }

    /// <summary>The viewer's gate: Detect, then its Confirm helper (given the check result), then the answer test, then the run.</summary>
    [Fact]
    public void TheViewerGate_ChecksThenConfirmsWithTheCheckResult_BeforeItQueuesTheRun()
    {
        var source = StripComments(File.ReadAllText(Path.Combine(RepoRoot(), ViewerFlow)));
        var body = MethodBody(source, "RequestActualPlanAsync");

        var detect = Regex.Match(body, @"var\s+(\w+)\s*=\s*QueryModificationDetector\.Detect\(", RegexOptions.CultureInvariant);
        Assert.True(detect.Success, "RequestActualPlanAsync never calls QueryModificationDetector.Detect into a variable");
        var info = detect.Groups[1].Value;

        var confirm = Regex.Match(body, @"if\s*\(\s*!Confirm\([^;]*\b" + Regex.Escape(info) + @"\s*\)\s*\)", RegexOptions.CultureInvariant);
        var run = body.IndexOf(".ExecuteActualPlanAsync(", StringComparison.Ordinal);
        Assert.True(confirm.Success && confirm.Index > detect.Index,
            $"RequestActualPlanAsync does not pass the Detect result '{info}' to Confirm before running");
        Assert.True(run > confirm.Index, "RequestActualPlanAsync queues the run before the Confirm answer is tested");

        /* The Confirm helper builds its prompt from the result it is handed. */
        var helper = MethodBody(source, "bool Confirm");
        var param = Regex.Match(helper, @"QueryModificationInfo\s+(\w+)", RegexOptions.CultureInvariant);
        Assert.True(param.Success, "Confirm takes no QueryModificationInfo");
        var p = param.Groups[1].Value;
        Assert.Contains($"QueryModificationDetector.BuildConsentWarning({p},", helper, StringComparison.Ordinal);
        Assert.Contains($"QueryModificationDetector.BuildConsentTitle({p})", helper, StringComparison.Ordinal);
        Assert.Contains("QueryModificationDetector.BuildConsentTarget(", helper, StringComparison.Ordinal);
        Assert.Contains("QueryModificationDetector.BuildConsentQueryText(", helper, StringComparison.Ordinal);
        Assert.Contains("MessageBoxButton.OKCancel", helper, StringComparison.Ordinal);
        Assert.Contains("MessageBoxResult.OK", helper, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every call that runs a query for its actual plan, in any shipped project: inside a controller's argument list, or
    /// in a method that checks, prompts and tests the answer first. Command-name references stay in the files that queue
    /// and handle the command.
    /// </summary>
    [Fact]
    public void EveryCallThatRunsAQueryForItsActualPlan_SitsBehindAGate()
    {
        var offenders = new List<string>();
        foreach (var file in ShippedSourceFiles())
        {
            var relative = Path.GetRelativePath(RepoRoot(), file).Replace('\\', '/');
            var source = StripComments(File.ReadAllText(file));
            var definesExecutor = s_executorDefinitions.Contains(relative, StringComparer.Ordinal);

            foreach (Match name in s_commandName.Matches(source))
            {
                if (!definesExecutor)
                {
                    offenders.Add($"{relative}: names the execute_actual_plan command outside the files that queue and handle it");
                }
            }

            if (definesExecutor)
            {
                continue;
            }

            /* Both are only ever run by the shared navigation's gate: the controller's constructor arguments, and the
               per-click target a host hands to GetActualPlanAsync. */
            var controllerSpans = ArgumentSpans(source, "new PlanNavigationController(")
                .Concat(ArgumentSpans(source, "new ActualPlanRunTarget(")).ToList();
            foreach (Match run in s_runCall.Matches(source))
            {
                if (controllerSpans.Any(span => run.Index >= span.Start && run.Index < span.End))
                {
                    continue; /* handed to the shared navigation, which gates it (pinned below) */
                }

                var member = EnclosingMember(source, run.Index);
                var at = run.Index - member.Start;
                var text = member.Text;
                var detect = text.IndexOf("QueryModificationDetector.Detect(", StringComparison.Ordinal);
                var prompt = text.IndexOf("MessageBoxButton.OKCancel", StringComparison.Ordinal);
                var answer = s_answerTest.Match(text);
                var line = source.Take(run.Index).Count(c => c == '\n') + 1;

                var gated = detect >= 0 && detect < at && answer.Success && answer.Index < at
                    && (prompt < 0 ? text.Contains("Confirm(", StringComparison.Ordinal) : prompt < answer.Index && detect < prompt);
                if (!gated)
                {
                    offenders.Add($"{relative}:{line}: runs a query for its actual plan ('{run.Value.Trim()}') in a method that does not " +
                        "check, prompt and test the OK answer first");
                }
            }
        }

        Assert.True(offenders.Count == 0, "Call sites without the data-modification check and the OK/Cancel prompt before them:\n" +
            string.Join("\n", offenders));
    }

    /// <summary>The shared navigation has ONE place that runs the executor, and it is the gated method.</summary>
    [Fact]
    public void TheSharedNavigation_RunsItsExecutorInExactlyOnePlace_AfterTheGate()
    {
        var source = StripComments(File.ReadAllText(Path.Combine(RepoRoot(), Controller)));

        var runs = s_runCall.Matches(source).Where(m => !m.Value.StartsWith("Execute", StringComparison.Ordinal)).ToList();
        Assert.True(runs.Count == 1, $"PlanNavigationController invokes its executor in {runs.Count} places; it must be exactly one, in GetActualPlanAsync");

        var gate = MethodBody(source, "GetActualPlanAsync");
        var gateStart = source.IndexOf(gate, StringComparison.Ordinal);
        Assert.True(runs[0].Index > gateStart && runs[0].Index < gateStart + gate.Length,
            "PlanNavigationController invokes its executor outside GetActualPlanAsync");
    }

    /// <summary>The shared navigation looks up the stored plan and stops on a changed server BEFORE the check and the prompt, one request at a time.</summary>
    [Fact]
    public void TheSharedNavigation_ReadsThePlanFirst_StopsOnAChangedServer_AndTakesOneRequestAtATime()
    {
        var gate = MethodBody(StripComments(File.ReadAllText(Path.Combine(RepoRoot(), Controller))), "GetActualPlanAsync");

        var inProgress = gate.IndexOf("if (_actualPlanInProgress) return;", StringComparison.Ordinal);
        var fetch = gate.IndexOf("await fetchPlanXml()", StringComparison.Ordinal);
        var busy = gate.IndexOf("SetBusy()", StringComparison.Ordinal);
        var current = gate.IndexOf("target.IsCurrent()", StringComparison.Ordinal);
        var detect = gate.IndexOf("QueryModificationDetector.Detect(", StringComparison.Ordinal);

        Assert.True(inProgress >= 0 && inProgress < fetch, "a second request is not turned away before the plan lookup starts");
        Assert.True(busy >= 0 && busy < fetch, "the plan lookup runs without the busy cursor");
        Assert.True(fetch > 0 && fetch < current && current < detect, "the plan lookup and the changed-server stop must come before the check");
        Assert.Contains("_actualPlanInProgress = false;", gate, StringComparison.Ordinal);
        Assert.Matches(@"catch\s*\(Exception\)\s*\{\s*estimatedPlanXml\s*=\s*null;", gate); /* a failed lookup is no plan */
    }

    /// <summary>Lite FinOps captures its server and connection string before any await, and runs against them.</summary>
    [Fact]
    public void LiteFinOps_CapturesItsServerBeforeTheFirstAwait_AndPassesItToEveryRequest()
    {
        var source = StripComments(File.ReadAllText(Path.Combine(RepoRoot(), FinOpsTab)));
        var handler = MethodBody(source, "FinOpsGetActualPlan_Click");

        var capture = handler.IndexOf("ServerSelector.SelectedItem is not ServerConnection", StringComparison.Ordinal);
        var connection = handler.IndexOf("GetSelectedConnectionString()", StringComparison.Ordinal);
        var firstAwait = handler.IndexOf("await ", StringComparison.Ordinal);
        Assert.True(capture >= 0 && capture < firstAwait, "FinOps reads the selected server after its first await");
        Assert.True(connection >= 0 && connection < firstAwait, "FinOps reads the connection string after its first await");
        Assert.Contains("new ActualPlanRunTarget(", handler, StringComparison.Ordinal);

        var requests = CallsOf(handler, ".GetActualPlanAsync(").ToList();
        Assert.NotEmpty(requests);
        Assert.All(requests, call => Assert.Contains("target:", call, StringComparison.Ordinal));

        /* The plan lookup takes the captured server, not the selector. */
        var fetch = MethodBody(source, "FetchFinOpsHighImpactPlanAsync");
        Assert.DoesNotContain("ServerSelector", fetch, StringComparison.Ordinal);
        Assert.DoesNotContain("GetSelected", fetch, StringComparison.Ordinal);
        Assert.Contains("QueryModificationDetector.IsUsableQueryHash(", fetch, StringComparison.Ordinal);

        /* The database named in the prompt is the row's own (the one picked when a hash is in several). */
        Assert.Contains("hi.FullQueryText, hi.DatabaseName,", handler, StringComparison.Ordinal);
    }

    /// <summary>Every Lite call into the shared navigation hands over a plan lookup for the controller to run, not a plan it fetched itself.</summary>
    [Fact]
    public void EveryCallIntoTheSharedNavigation_PassesItsPlanLookupToTheController()
    {
        var calls = new List<string>();
        foreach (var file in SourceFiles("Lite"))
        {
            foreach (var call in CallsOf(StripComments(File.ReadAllText(file)), ".GetActualPlanAsync("))
            {
                if (!call.Contains("fetchPlanXml:", StringComparison.Ordinal))
                {
                    calls.Add($"{Path.GetRelativePath(RepoRoot(), file)}: {call.Replace('\n', ' ').Trim()}");
                }
            }
        }

        Assert.True(calls.Count == 0,
            "These calls into PlanNavigationController.GetActualPlanAsync do not pass fetchPlanXml:\n" + string.Join("\n", calls));
    }

    [Fact]
    public void TheCensus_SeesTheKnownEntryPoints()
    {
        var found = SourceFiles("Lite")
            .Where(f => CallsOf(StripComments(File.ReadAllText(f)), ".GetActualPlanAsync(").Any())
            .Select(f => Path.GetFileName(f))
            .ToHashSet(StringComparer.Ordinal);

        foreach (var expected in new[]
        {
            "QueryStatsHistoryWindow.xaml.cs", "QueryStoreHistoryWindow.xaml.cs", "WaitDrillDownWindow.xaml.cs", "FinOpsTab.xaml.cs",
        })
        {
            Assert.Contains(expected, found);
        }

        /* And the per-call scan sees the executor calls and the one controller delegate it is meant to police. */
        var runCalls = ShippedSourceFiles()
            .Sum(f => s_runCall.Matches(StripComments(File.ReadAllText(f))).Count);
        Assert.True(runCalls >= 8, $"the per-call census found only {runCalls} executor references; its scan has stopped seeing them");
    }

    /// <summary>The lookups that decide the check by query hash skip an empty or all-zero one (no plan: the warning shows).</summary>
    [Theory]
    [InlineData("Lite/Windows/QueryStatsHistoryWindow.xaml.cs")]
    [InlineData("Lite/Controls/ServerTab.Plans.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/QueryStatsHistoryWindow.xaml.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.ActualPlan.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/FinOpsTab.Loaders.cs")]
    public void ThePlanLookupByQueryHash_SkipsAnEmptyOrAllZeroHash(string relative)
        => Assert.Contains("QueryModificationDetector.IsUsableQueryHash(",
            File.ReadAllText(Path.Combine(RepoRoot(), relative)), StringComparison.Ordinal);

    // ── source reading ──

    /// <summary>Removes // and /* */ comments so a word in a comment is not read as code.</summary>
    private static string StripComments(string source)
    {
        var sb = new StringBuilder(source.Length);
        var i = 0;
        while (i < source.Length)
        {
            if (source[i] == '"' && (i == 0 || source[i - 1] != '\\'))
            {
                /* keep string literals (the command name is one) untouched */
                var end = i + 1;
                while (end < source.Length && !(source[end] == '"' && source[end - 1] != '\\') && source[end] != '\n') end++;
                var stop = Math.Min(end + 1, source.Length);
                sb.Append(source, i, stop - i);
                i = stop;
            }
            else if (source[i] == '/' && i + 1 < source.Length && source[i + 1] == '/')
            {
                while (i < source.Length && source[i] != '\n') i++;
            }
            else if (source[i] == '/' && i + 1 < source.Length && source[i + 1] == '*')
            {
                var end = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var stop = end < 0 ? source.Length : end + 2;
                for (var k = i; k < stop; k++)
                {
                    if (source[k] == '\n') sb.Append('\n'); /* keep line numbers */
                }

                i = stop;
            }
            else
            {
                sb.Append(source[i]);
                i++;
            }
        }

        return sb.ToString();
    }

    private readonly record struct Member(int Start, string Text);

    private static readonly Regex s_memberStart = new(
        @"^    (?:\[[^\]\r\n]*\]\s*)*(?:public|private|internal|protected|static|async|override|sealed|partial|const|readonly)\b[^\r\n]*",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    /// <summary>The type member (method, field, property) holding <paramref name="index"/>: from its 4-space-indented declaration to the next one.</summary>
    private static Member EnclosingMember(string source, int index)
    {
        var starts = s_memberStart.Matches(source).Select(m => m.Index).ToList();
        var start = starts.LastOrDefault(s => s <= index);
        var next = starts.FirstOrDefault(s => s > index, source.Length);
        return new Member(start, source.Substring(start, next - start));
    }

    /// <summary>The argument list (open parenthesis to its close) of each <paramref name="marker"/> call.</summary>
    private static List<(int Start, int End)> ArgumentSpans(string source, string marker)
    {
        var spans = new List<(int, int)>();
        var at = 0;
        while ((at = source.IndexOf(marker, at, StringComparison.Ordinal)) >= 0)
        {
            var start = at + marker.Length;
            var depth = 1;
            var i = start;
            for (; i < source.Length && depth > 0; i++)
            {
                if (source[i] == '(') depth++;
                else if (source[i] == ')') depth--;
            }

            spans.Add((start, i));
            at = start;
        }

        return spans;
    }

    /// <summary>The text of <paramref name="method"/>'s body (brace-matched), from its declaration to the closing brace.</summary>
    private static string MethodBody(string source, string method)
    {
        var decl = Regex.Match(source, @"\b" + Regex.Escape(method) + @"\s*\(", RegexOptions.CultureInvariant);
        while (decl.Success)
        {
            var open = source.IndexOf('{', decl.Index);
            var semi = source.IndexOf(';', decl.Index);
            var arrow = source.IndexOf("=>", decl.Index, StringComparison.Ordinal);
            if (open >= 0 && (semi < 0 || open < semi) && (arrow < 0 || open < arrow))
            {
                var depth = 0;
                for (var i = open; i < source.Length; i++)
                {
                    if (source[i] == '{') depth++;
                    else if (source[i] == '}' && --depth == 0)
                    {
                        return source.Substring(decl.Index, i - decl.Index + 1);
                    }
                }
            }

            decl = decl.NextMatch();
        }

        Assert.Fail($"No method body found for {method}");
        return string.Empty;
    }

    /// <summary>Each call's argument text, from the open parenthesis to its matching close.</summary>
    private static IEnumerable<string> CallsOf(string source, string marker)
    {
        foreach (var (start, end) in ArgumentSpans(source, marker))
        {
            yield return source.Substring(start, Math.Max(0, end - start - 1));
        }
    }

    private static IEnumerable<string> SourceFiles(string relativeRoot)
    {
        var sep = Path.DirectorySeparatorChar;
        return Directory.EnumerateFiles(Path.Combine(RepoRoot(), relativeRoot), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{sep}obj{sep}", StringComparison.Ordinal) && !f.Contains($"{sep}bin{sep}", StringComparison.Ordinal));
    }

    /// <summary>Every shipped project's source: Lite, the PerformanceMonitor.* libraries and apps, and the Darling projects (not tests, not deprecated).</summary>
    private static IEnumerable<string> ShippedSourceFiles()
    {
        var root = RepoRoot();
        var roots = new List<string> { "Lite" };
        roots.AddRange(Directory.EnumerateDirectories(root, "PerformanceMonitor.*").Select(d => Path.GetFileName(d)!));
        roots.AddRange(Directory.EnumerateDirectories(Path.Combine(root, "Darling"), "PerformanceMonitor.Darling.*")
            .Select(d => "Darling/" + Path.GetFileName(d)));
        return roots.SelectMany(SourceFiles);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
