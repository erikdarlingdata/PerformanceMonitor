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
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
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
public sealed class ActualPlanModificationCheckGuardTests
{
    private const string Controller = "PerformanceMonitor.Ui/PlanNavigationController.cs";
    private const string ViewerFlow = "Darling/PerformanceMonitor.Darling.Viewer/ViewerActualPlanFlow.cs";
    private const string ServerTabPlans = "Lite/Controls/ServerTab.Plans.cs";

    /// <summary>The files that DEFINE an executor or handle the queued command on the service side: the consent is taken
    /// before the command is queued (the viewer flow), so these are not entry points.</summary>
    private static readonly string[] s_executorDefinitions =
    {
        "PerformanceMonitor.PlanAnalysis/ActualPlanExecutor.cs",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.ActualPlan.cs",
        "Darling/PerformanceMonitor.Darling.Service/ActualPlanRequest.cs",
        "Darling/PerformanceMonitor.Darling.Service/ServiceCommandDeadlines.cs",
    };

    /// <summary>
    /// The two service files that hold the command's handlers, and the only methods in them that may run the executor or
    /// name the command. The rest of each file is held to the same rule as any other file: a new method that runs a query
    /// for its actual plan there (an MCP or web handler, say) is not a command handler and is reported.
    /// </summary>
    private static readonly Dictionary<string, string[]> s_serviceHandlers = new(StringComparer.Ordinal)
    {
        /* #7024: the handler and the nested command host that calls it now sit in different DarlingWorker.<Area>.cs files. */
        ["Darling/PerformanceMonitor.Darling.Service/DarlingWorker.Targets.cs"] = new[] { "RunExecuteActualPlanAsync" },
        ["Darling/PerformanceMonitor.Darling.Service/DarlingWorker.Passes.cs"] = new[] { "ExecuteActualPlanAsync" },
        ["Darling/PerformanceMonitor.Darling.Service/DarlingCommandExecutor.cs"] = new[] { "RunAsync", "ResolvePlan" },
    };

    /// <summary>
    /// What runs a query for its actual plan: the executor itself, the viewer's and the service's wrappers, a call or an
    /// <c>Invoke</c> through the <c>executeActual</c> delegate, and a <c>.Execute(</c> call on a run target.
    /// </summary>
    private static readonly Regex s_runCall = new(
        @"ExecuteForActualPlanAsync|\.ExecuteActualPlanAsync\b|\bRunExecuteActualPlanAsync\b|(?<![A-Za-z0-9])_?executeActual\s*(?:\??\s*\.\s*Invoke\s*)?\(|\.Execute\s*\(",
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
        var body = MethodBody(StripComments(RepoFile.ReadRepoFile(Path.Combine(RepoRoot(), relative))), method);

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
            offenders.AddRange(CensusOffenders(relative, StripComments(File.ReadAllText(file))));
        }

        Assert.True(offenders.Count == 0, "Call sites without the data-modification check and the OK/Cancel prompt before them:\n" +
            string.Join("\n", offenders));
    }

    /// <summary>
    /// The per-call census of one file (its comment-stripped source): the problems found with how it names the command or
    /// runs a query for its actual plan. Takes the source as text so the checks can be exercised on small sources.
    /// </summary>
    private static List<string> CensusOffenders(string relative, string source)
    {
        var offenders = new List<string>();
        var literal = LiteralMask(source);
        var definesExecutor = s_executorDefinitions.Contains(relative, StringComparer.Ordinal);
        var handlers = s_serviceHandlers.TryGetValue(relative, out var names) ? names : null;
        var handlerSpans = handlers == null
            ? new List<(int Start, int End)>()
            : handlers.SelectMany(n => MethodSpans(source, literal, n)).ToList();

        foreach (Match name in s_commandName.Matches(source))
        {
            if (!definesExecutor && !handlerSpans.Any(span => name.Index >= span.Start && name.Index < span.End))
            {
                offenders.Add($"{relative}:{LineOf(source, name.Index)}: names the execute_actual_plan command outside the files and methods that queue and handle it");
            }
        }

        if (definesExecutor)
        {
            return offenders;
        }

        if (handlers != null)
        {
            foreach (Match run in s_runCall.Matches(source))
            {
                if (!handlerSpans.Any(span => run.Index >= span.Start && run.Index < span.End))
                {
                    offenders.Add($"{relative}:{LineOf(source, run.Index)}: runs a query for its actual plan ('{run.Value.Trim()}') outside the " +
                        $"command handler methods ({string.Join(", ", handlers)})");
                }
            }

            return offenders;
        }

        /* Both are only ever run by the shared navigation's gate: the controller's constructor arguments, and the
           per-click target a host hands to GetActualPlanAsync. */
        var controllerSpans = ArgumentSpans(source, "new PlanNavigationController(")
            .Concat(ArgumentSpans(source, "new PlanNavigationController.ActualPlanRunTarget("))
            .Concat(ArgumentSpans(source, "new ActualPlanRunTarget(")).ToList();
        var uncovered = s_runCall.Matches(source)
            .Where(run => !controllerSpans.Any(span => run.Index >= span.Start && run.Index < span.End)) /* handed to the shared navigation, which gates it (pinned below) */
            .ToList();
        if (uncovered.Count == 0)
        {
            return offenders;
        }

        /* A file that runs a query itself is split into members by indentation and modifier; a file that cannot be split
           that way fails rather than being read as one member. */
        var members = Members(source, literal, out var problems);
        offenders.AddRange(problems.Select(p => $"{relative}: {p}"));
        if (problems.Count > 0)
        {
            return offenders;
        }

        foreach (var group in uncovered.GroupBy(run => members.LastOrDefault(m => m.Start <= run.Index)))
        {
            var first = group.First();
            if (group.Key.Text == null)
            {
                offenders.Add($"{relative}:{LineOf(source, first.Index)}: runs a query for its actual plan ('{first.Value.Trim()}') before any member starts");
                continue;
            }

            if (s_typeDeclaration.IsMatch(group.Key.Text.Split('\n')[0]))
            {
                offenders.Add($"{relative}:{LineOf(source, first.Index)}: runs a query for its actual plan ('{first.Value.Trim()}') inside a nested type, " +
                    "whose methods cannot be told apart by indentation");
                continue;
            }

            foreach (var problem in GateProblems(group.Key.Text, group.Select(run => run.Index - group.Key.Start).ToList()))
            {
                offenders.Add($"{relative}:{LineOf(source, first.Index)}: runs a query for its actual plan ('{first.Value.Trim()}') in a method that {problem}");
            }
        }

        return offenders;
    }

    /// <summary>
    /// What a method that runs a query for its actual plan must do, read from its own text: call the check once into a
    /// variable; show ONE prompt (an OK/Cancel box, or the viewer's Confirm helper) built from that variable; test the
    /// answer straight after the prompt; and have exactly one run call, after that test.
    /// </summary>
    private static List<string> GateProblems(string member, List<int> runs)
    {
        var problems = new List<string>();
        var detect = Regex.Match(member, @"var\s+(\w+)\s*=\s*QueryModificationDetector\.Detect\(", RegexOptions.CultureInvariant);
        if (!detect.Success || Regex.Matches(member, @"QueryModificationDetector\.Detect\(", RegexOptions.CultureInvariant).Count != 1)
        {
            problems.Add("does not call QueryModificationDetector.Detect exactly once into a variable");
            return problems;
        }

        var info = detect.Groups[1].Value;
        if (runs.Count != 1)
        {
            problems.Add($"runs the query in {runs.Count} places; a gated method has exactly one run call");
        }

        var boxes = Regex.Matches(member, @"MessageBoxButton\.OKCancel", RegexOptions.CultureInvariant).Count;
        var confirms = Regex.Matches(member, @"(?<![A-Za-z0-9_])Confirm\(", RegexOptions.CultureInvariant).Count;
        var answerEnd = -1;
        if (boxes == 1 && confirms == 0)
        {
            var prompt = member.IndexOf("MessageBoxButton.OKCancel", StringComparison.Ordinal);
            var warning = member.IndexOf($"QueryModificationDetector.BuildConsentWarning({info},", StringComparison.Ordinal);
            var title = member.IndexOf($"QueryModificationDetector.BuildConsentTitle({info})", StringComparison.Ordinal);
            var target = member.IndexOf("QueryModificationDetector.BuildConsentTarget(", StringComparison.Ordinal);
            var queryText = member.IndexOf("QueryModificationDetector.BuildConsentQueryText(", StringComparison.Ordinal);
            if (warning < detect.Index) problems.Add($"does not build the prompt's warning from the Detect result '{info}'");
            if (title < detect.Index) problems.Add($"does not build the prompt's title from the Detect result '{info}'");
            if (target < 0 || queryText < 0 || target > prompt || queryText > prompt || warning > prompt)
            {
                problems.Add("does not build the prompt text (warning, target, query text) before the OK/Cancel box");
            }

            /* The answer test is the statement straight after the one that shows the box, and tests that box's result. */
            var shown = Regex.Match(member[..prompt], @"var\s+(\w+)\s*=\s*MessageBox\.Show\(", RegexOptions.CultureInvariant | RegexOptions.RightToLeft);
            var statementEnd = IndexOfCode(member, ';', prompt);
            var answer = shown.Success && statementEnd >= 0
                ? Regex.Match(member.Substring(statementEnd + 1),
                    @"\A\s*if\s*\(\s*" + Regex.Escape(shown.Groups[1].Value) + @"\s*!=\s*MessageBoxResult\.OK\s*\)\s*(?:\{\s*)?return",
                    RegexOptions.CultureInvariant)
                : Match.Empty;
            if (!answer.Success)
            {
                problems.Add("does not test the OK/Cancel answer straight after the box that shows it");
            }
            else
            {
                answerEnd = statementEnd + 1 + answer.Index + answer.Length;
            }
        }
        else if (confirms == 1 && boxes == 0)
        {
            var answer = Regex.Match(member, @"if\s*\(\s*!Confirm\([^;]*\b" + Regex.Escape(info) + @"\s*\)\s*\)", RegexOptions.CultureInvariant);
            if (!answer.Success || answer.Index < detect.Index)
            {
                problems.Add($"does not pass the Detect result '{info}' to its one Confirm call and test the answer");
            }
            else
            {
                answerEnd = answer.Index + answer.Length;
            }
        }
        else
        {
            problems.Add($"shows {boxes} OK/Cancel boxes and calls Confirm {confirms} times; it must show exactly one prompt");
        }

        if (answerEnd >= 0 && runs.Any(r => r < answerEnd))
        {
            problems.Add("runs the query before the answer is tested");
        }

        return problems;
    }

    /// <summary>
    /// The compiler, not a text scan, keeps the run target's executor to the controller: no public or internal member of
    /// <see cref="PlanNavigationController.ActualPlanRunTarget"/> runs it or hands it out. (<c>internal</c> alone would not
    /// do: the Ui assembly grants internals to both host apps.)
    /// </summary>
    [Fact]
    public void TheRunTarget_ExposesNoMemberThatRunsOrHandsOutItsExecutor()
    {
        const BindingFlags all = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;
        var type = typeof(PlanNavigationController.ActualPlanRunTarget);

        var reachable = type.GetMembers(all).Where(m => m switch
        {
            MethodBase method => method.IsPublic || method.IsAssembly || method.IsFamily || method.IsFamilyOrAssembly,
            FieldInfo field => field.IsPublic || field.IsAssembly || field.IsFamily || field.IsFamilyOrAssembly,
            PropertyInfo or EventInfo => false, /* their accessors are methods, checked above */
            Type nested => nested.IsNestedPublic || nested.IsNestedAssembly || nested.IsNestedFamily || nested.IsNestedFamORAssem,
            _ => false,
        }).Select(m => m.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();

        Assert.Equal(new[] { ".ctor", "get_IsCurrent", "get_ServerName" }, reachable);
        Assert.True(type.IsSealed);
    }

    // ── the census's own checks, on small sources ──

    private static List<string> Offenders(string members, string relative = "Lite/Controls/Fake.cs")
        => CensusOffenders(relative, StripComments("public sealed class Host\n{\n" + members + "\n}\n"));

    private const string GatedMember = """
        private async Task Go(string q, string plan)
        {
            var info = QueryModificationDetector.Detect(plan, q);
            var warning = QueryModificationDetector.BuildConsentWarning(info, "db");
            var where = QueryModificationDetector.BuildConsentTarget("s", "db");
            var text = QueryModificationDetector.BuildConsentQueryText(q);
            var result = MessageBox.Show(warning + where + text, QueryModificationDetector.BuildConsentTitle(info),
                MessageBoxButton.OKCancel, MessageBoxImage.Warning);
            if (result != MessageBoxResult.OK) return;
            await ActualPlanExecutor.ExecuteForActualPlanAsync(q);
        }
    """;

    [Fact]
    public void TheCensus_AcceptsAMethodThatChecksPromptsAndTestsTheAnswerBeforeItRuns()
        => Assert.Empty(Offenders(GatedMember));

    /// <summary>(a) The executor delegate and a run target's Execute are run calls too, not only the executor class.</summary>
    [Theory]
    [InlineData("target.Execute(db, q, null, null, ct)")]
    [InlineData("_executeActual.Invoke(db, q, null, null, ct)")]
    [InlineData("executeActual?.Invoke(db, q, null, null, ct)")]
    [InlineData("executeActual(db, q, null, null, ct)")]
    public void TheCensus_SeesTheExecuteAndInvokeCallForms(string call)
    {
        var offenders = Offenders($$"""
            private async Task Run(string db, string q, CancellationToken ct)
            {
                await {{call}};
            }
        """);

        Assert.Single(offenders);
        Assert.Contains("exactly once into a variable", offenders[0], StringComparison.Ordinal);
    }

    /// <summary>(b) A method with no modifier does not merge into the gated method above it.</summary>
    [Fact]
    public void TheCensus_FailsAFileWithAMemberThatHasNoModifier()
    {
        var offenders = Offenders(GatedMember + "\n\n" + """
            Task Sneaky(string q)
            {
                return ActualPlanExecutor.ExecuteForActualPlanAsync(q);
            }
        """);

        Assert.Contains(offenders, o => o.Contains("without an explicit modifier", StringComparison.Ordinal));
    }

    /// <summary>(b) A run call inside a nested type, or in a file with a block-scoped namespace, is not read as part of one method.</summary>
    [Fact]
    public void TheCensus_FailsARunCallInANestedTypeOrABlockScopedNamespace()
    {
        var nested = Offenders("""
            private sealed class Inner
            {
                private Task Go(string q) => ActualPlanExecutor.ExecuteForActualPlanAsync(q);
            }
        """);
        Assert.Contains(nested, o => o.Contains("nested type", StringComparison.Ordinal));

        var blockScoped = CensusOffenders("Lite/Controls/Fake.cs", StripComments(
            "namespace Fake\n{\n    public sealed class Host\n    {\n        private Task Go(string q) => ActualPlanExecutor.ExecuteForActualPlanAsync(q);\n    }\n}\n"));
        Assert.Contains(blockScoped, o => o.Contains("nested type", StringComparison.Ordinal));
    }

    [Fact]
    public void TheCensus_FailsAFileWhoseMembersCannotBeFound()
    {
        var offenders = CensusOffenders("Lite/Controls/Fake.cs", StripComments(
            "public sealed class Host\n{\n  private Task Go(string q) => ActualPlanExecutor.ExecuteForActualPlanAsync(q);\n}\n"));

        Assert.Contains(offenders, o => o.Contains("no member start was found", StringComparison.Ordinal));
    }

    /// <summary>(c) Every accepted method is held to the same checks: one run call, one prompt, built from the Detect result, answered straight away.</summary>
    [Fact]
    public void TheCensus_FailsAGatedMethodWithASecondRunCall()
    {
        var offenders = Offenders(GatedMember.Replace(
            "await ActualPlanExecutor.ExecuteForActualPlanAsync(q);",
            "await ActualPlanExecutor.ExecuteForActualPlanAsync(q);\n        await ActualPlanExecutor.ExecuteForActualPlanAsync(plan);", StringComparison.Ordinal));

        Assert.Contains(offenders, o => o.Contains("runs the query in 2 places", StringComparison.Ordinal));
    }

    [Fact]
    public void TheCensus_FailsAGatedMethodWithAnUnrelatedSecondOkCancelBox()
    {
        var offenders = Offenders(GatedMember.Replace(
            "if (result != MessageBoxResult.OK) return;",
            "if (result != MessageBoxResult.OK) return;\n        var other = MessageBox.Show(\"Continue?\", \"t\", MessageBoxButton.OKCancel);", StringComparison.Ordinal));

        Assert.Contains(offenders, o => o.Contains("exactly one prompt", StringComparison.Ordinal));
    }

    [Fact]
    public void TheCensus_FailsAPromptThatIsNotBuiltFromTheDetectResult()
    {
        var offenders = Offenders(GatedMember.Replace("BuildConsentTitle(info)", "BuildConsentTitle(null)", StringComparison.Ordinal));
        Assert.Contains(offenders, o => o.Contains("title from the Detect result", StringComparison.Ordinal));

        offenders = Offenders(GatedMember.Replace("BuildConsentWarning(info,", "BuildConsentWarning(other,", StringComparison.Ordinal));
        Assert.Contains(offenders, o => o.Contains("warning from the Detect result", StringComparison.Ordinal));
    }

    [Fact]
    public void TheCensus_FailsAnAnswerTestThatIsNotStraightAfterThePrompt()
    {
        var offenders = Offenders(GatedMember.Replace(
            "if (result != MessageBoxResult.OK) return;",
            "Log(q);\n        if (result != MessageBoxResult.OK) return;", StringComparison.Ordinal));

        Assert.Contains(offenders, o => o.Contains("straight after the box", StringComparison.Ordinal));
    }

    [Fact]
    public void TheCensus_FailsAViewerMethodWhoseConfirmDoesNotGetTheDetectResult()
    {
        const string viewer = """
            private async Task Request(string q, string plan)
            {
                var info = QueryModificationDetector.Detect(plan, q);
                if (!Confirm(owner, q, CHECK))
                {
                    return;
                }

                await dataService.ExecuteActualPlanAsync(1, q, ct);
            }
        """;

        Assert.Empty(Offenders(viewer.Replace("CHECK", "info", StringComparison.Ordinal)));
        Assert.Contains(Offenders(viewer.Replace("CHECK", "null", StringComparison.Ordinal)),
            o => o.Contains("to its one Confirm call", StringComparison.Ordinal));
    }

    /// <summary>(d) A parenthesis inside a string or character literal does not stretch the exempt argument list.</summary>
    [Theory]
    [InlineData("\"(\"")]
    [InlineData("'('")]
    [InlineData("$\"({1 + 1}\"")]
    [InlineData("@\"(\"\"(\"")]
    public void TheCensus_IgnoresParenthesesInsideLiteralsWhenMatchingAnArgumentList(string literal)
    {
        var offenders = Offenders($$"""
            private object Build()
            {
                return new PlanNavigationController(owner, {{literal}}, (db, q, e, i, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(q));
            }

            private async Task Bad(string q)
            {
                await ActualPlanExecutor.ExecuteForActualPlanAsync(q);
            }
        """);

        Assert.Single(offenders);
        Assert.Contains(":10:", offenders[0], StringComparison.Ordinal); /* the Bad method's call; Build's is inside the controller's argument list */
    }

    /// <summary>(e) In the service's two handler files only the named handler methods may run the executor or name the command.</summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.Targets.cs", "RunExecuteActualPlanAsync")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingCommandExecutor.cs", "RunAsync")]
    public void TheCensus_AllowsOnlyTheNamedHandlerMethodsInTheServiceFiles(string relative, string handler)
    {
        var members = $$"""
            private async Task {{handler}}(string q)
            {
                await ActualPlanExecutor.ExecuteForActualPlanAsync(q);
                Log("execute_actual_plan");
            }

            private async Task HandleSomethingElse(string q)
            {
                await ActualPlanExecutor.ExecuteForActualPlanAsync(q);
                await _worker.RunExecuteActualPlanAsync(q);
                Log("execute_actual_plan");
            }
        """;

        var offenders = Offenders(members, relative);

        /* Only the second method's three references (its run call, its call of the handler and its command name) are reported. */
        Assert.Equal(3, offenders.Count);
        foreach (var line in new[] { ":11:", ":12:", ":13:" })
        {
            Assert.Contains(offenders, o => o.Contains(line, StringComparison.Ordinal));
        }
    }

    /// <summary>The scanner keeps comments out and finds literals the way the compiler does.</summary>
    [Fact]
    public void TheScanner_ReadsCharVerbatimInterpolatedAndRawLiterals()
    {
        const string source = """"
            var a = '"'; var b = @"x""y("; var c = $"{Format("(")}z)"; var d = "q\"(";
            var e = """raw " ( raw"""; // ( not code
            var f = (1 + 2);
            """";
        var stripped = StripComments(source);
        var literal = LiteralMask(stripped);

        Assert.DoesNotContain("not code", stripped, StringComparison.Ordinal);
        var parens = Enumerable.Range(0, stripped.Length).Where(i => (stripped[i] == '(' || stripped[i] == ')') && !literal[i]).Select(i => stripped[i]).ToArray();
        /* Format( ... ) inside the interpolation hole, then (1 + 2) */
        Assert.Equal(new[] { '(', ')', '(', ')' }, parens);
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

    // ── source reading ──

    /// <summary>Removes // and /* */ comments so a word in a comment is not read as code. String and character literals are kept (the command name is one).</summary>
    private static string StripComments(string source) => Lex(source, out _);

    /// <summary>
    /// For each character of <paramref name="source"/> (already comment-stripped), whether it sits inside a string or
    /// character literal. The braces of an interpolation hole count as literal; what is inside the hole counts as code.
    /// </summary>
    private static bool[] LiteralMask(string source)
    {
        Lex(source, out var mask);
        return mask;
    }

    private static string Lex(string source, out bool[] mask)
    {
        var text = new StringBuilder(source.Length);
        var literal = new List<bool>(source.Length);
        var i = 0;
        LexCode(source, ref i, text, literal, inHole: false);
        mask = literal.ToArray();
        return text.ToString();
    }

    private static void Put(StringBuilder text, List<bool> mask, char c, bool inLiteral)
    {
        text.Append(c);
        mask.Add(inLiteral);
    }

    /// <summary>Code up to the end of the input, or (inside an interpolation hole) up to the brace that closes the hole.</summary>
    private static void LexCode(string s, ref int i, StringBuilder text, List<bool> mask, bool inHole)
    {
        var depth = 0;
        while (i < s.Length)
        {
            var c = s[i];
            if (c == '/' && i + 1 < s.Length && s[i + 1] == '/')
            {
                while (i < s.Length && s[i] != '\n') i++;
            }
            else if (c == '/' && i + 1 < s.Length && s[i + 1] == '*')
            {
                var end = s.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var stop = end < 0 ? s.Length : end + 2;
                for (var k = i; k < stop; k++)
                {
                    if (s[k] == '\n') Put(text, mask, '\n', false); /* keep line numbers */
                }

                i = stop;
            }
            else if (c == '"')
            {
                LexString(s, ref i, text, mask);
            }
            else if (c == '\'')
            {
                LexChar(s, ref i, text, mask);
            }
            else
            {
                if (inHole)
                {
                    if (c == '{') depth++;
                    else if (c == '}')
                    {
                        if (depth == 0) return;
                        depth--;
                    }
                }

                Put(text, mask, c, false);
                i++;
            }
        }
    }

    /// <summary>A character literal ('x', '\n', '\'', '"'): a quote inside it does not start a string.</summary>
    private static void LexChar(string s, ref int i, StringBuilder text, List<bool> mask)
    {
        Put(text, mask, '\'', true);
        i++;
        if (i < s.Length && s[i] == '\\')
        {
            Put(text, mask, s[i++], true);
            if (i < s.Length) Put(text, mask, s[i++], true);
            while (i < s.Length && s[i] != '\'' && s[i] != '\n') Put(text, mask, s[i++], true);
        }
        else if (i < s.Length && s[i] != '\n' && s[i] != '\'')
        {
            Put(text, mask, s[i++], true);
            if (i < s.Length && char.IsLowSurrogate(s[i])) Put(text, mask, s[i++], true);
        }

        if (i < s.Length && s[i] == '\'')
        {
            Put(text, mask, '\'', true);
            i++;
        }
    }

    /// <summary>A string literal at its opening quote: regular, verbatim (@), interpolated ($, with its holes read as code) or raw ("""..."""). The prefix was already written out.</summary>
    private static void LexString(string s, ref int i, StringBuilder text, List<bool> mask)
    {
        var verbatim = false;
        var interpolated = false;
        for (var k = text.Length - 1; k >= 0 && (text[k] == '@' || text[k] == '$'); k--)
        {
            verbatim |= text[k] == '@';
            interpolated |= text[k] == '$';
        }

        if (i + 2 < s.Length && s[i + 1] == '"' && s[i + 2] == '"')
        {
            var opening = 0;
            while (i + opening < s.Length && s[i + opening] == '"') opening++;
            for (var k = 0; k < opening; k++) Put(text, mask, '"', true);
            i += opening;
            while (i < s.Length)
            {
                if (s[i] != '"')
                {
                    Put(text, mask, s[i++], true);
                    continue;
                }

                var run = 0;
                while (i + run < s.Length && s[i + run] == '"') run++;
                for (var k = 0; k < run; k++) Put(text, mask, '"', true);
                i += run;
                if (run >= opening) return;
            }

            return;
        }

        Put(text, mask, '"', true);
        i++;
        while (i < s.Length)
        {
            var c = s[i];
            var next = i + 1 < s.Length ? s[i + 1] : '\0';
            if (verbatim)
            {
                if (c == '"')
                {
                    if (next == '"')
                    {
                        Put(text, mask, c, true);
                        Put(text, mask, next, true);
                        i += 2;
                        continue;
                    }

                    Put(text, mask, c, true);
                    i++;
                    return;
                }
            }
            else
            {
                if (c == '\\')
                {
                    Put(text, mask, c, true);
                    i++;
                    if (i < s.Length && s[i] != '\n') Put(text, mask, s[i++], true);
                    continue;
                }

                if (c == '"')
                {
                    Put(text, mask, c, true);
                    i++;
                    return;
                }

                if (c == '\n')
                {
                    return; /* unterminated: stop at the line end */
                }
            }

            if (interpolated && (c == '{' || c == '}') && next == c)
            {
                Put(text, mask, c, true);
                Put(text, mask, next, true);
                i += 2;
                continue;
            }

            if (interpolated && c == '{')
            {
                Put(text, mask, c, true);
                i++;
                LexCode(s, ref i, text, mask, inHole: true);
                if (i < s.Length && s[i] == '}')
                {
                    Put(text, mask, '}', true);
                    i++;
                }

                continue;
            }

            Put(text, mask, c, true);
            i++;
        }
    }

    private static int LineOf(string source, int index) => source.Take(index).Count(c => c == '\n') + 1;

    /// <summary>The first <paramref name="ch"/> at or after <paramref name="from"/> that is code, not inside a string or character literal.</summary>
    private static int IndexOfCode(string source, char ch, int from)
    {
        var literal = LiteralMask(source);
        for (var i = from; i < source.Length; i++)
        {
            if (source[i] == ch && !literal[i]) return i;
        }

        return -1;
    }

    private readonly record struct Member(int Start, string Text);

    /// <summary>A member's first line: four spaces, optional attributes, then an explicit modifier.</summary>
    private static readonly Regex s_memberStart = new(
        @"^    (?:\[[^\]\r\n]*\]\s*)*(?:public|private|internal|protected|static|async|override|sealed|partial|const|readonly|abstract|virtual|extern|unsafe|volatile|required|file)\b",
        RegexOptions.CultureInvariant);

    /// <summary>A type declaration line (a nested type, or a type inside a block-scoped namespace).</summary>
    private static readonly Regex s_typeDeclaration = new(
        @"^\s*(?:\[[^\]\r\n]*\]\s*)*(?:(?:public|private|internal|protected|static|sealed|abstract|partial|readonly|unsafe|file)\s+)*(?:class|struct|interface|enum|record)\b",
        RegexOptions.CultureInvariant);

    /// <summary>
    /// Splits a file that runs a query for its actual plan into its type members, each from its four-space-indented
    /// declaration (with an explicit modifier) to the next one. It fails closed instead of guessing: a four-space-indented
    /// line that starts something without a modifier (a method that would merge into the one above it), and a file with no
    /// member start at all (a block-scoped namespace, say), are returned as problems. Darling.Tests does not reference
    /// Roslyn, and this does not add it.
    /// </summary>
    private static List<Member> Members(string source, bool[] literal, out List<string> problems)
    {
        problems = new List<string>();
        var starts = new List<int>();
        var offset = 0;
        var lineNo = 0;
        while (offset <= source.Length)
        {
            lineNo++;
            var nl = source.IndexOf('\n', offset);
            var end = nl < 0 ? source.Length : nl;
            var line = source.Substring(offset, end - offset).TrimEnd('\r');
            if (line.Length > 4 && line.StartsWith("    ", StringComparison.Ordinal) && line[4] != ' ' && line[4] != '\t'
                && !literal[offset + 4] && "{}[#)].?:+-&|=,*<>/".IndexOf(line[4]) < 0)
            {
                if (s_memberStart.IsMatch(line))
                {
                    starts.Add(offset);
                }
                else
                {
                    problems.Add($"line {lineNo}: a four-space-indented member without an explicit modifier ('{line.Trim()}'); give it public, private, internal or another modifier so its start can be found");
                }
            }

            if (nl < 0) break;
            offset = nl + 1;
        }

        if (starts.Count == 0 && problems.Count == 0)
        {
            problems.Add("holds a call that runs a query for its actual plan but no member start was found (members are read at four spaces of indentation with an explicit modifier)");
        }

        var members = new List<Member>();
        for (var i = 0; i < starts.Count; i++)
        {
            var stop = i + 1 < starts.Count ? starts[i + 1] : source.Length;
            members.Add(new Member(starts[i], source.Substring(starts[i], stop - starts[i])));
        }

        return members;
    }

    /// <summary>The argument list (open parenthesis to its close) of each <paramref name="marker"/> call. Parentheses inside string and character literals do not count.</summary>
    private static List<(int Start, int End)> ArgumentSpans(string source, string marker)
    {
        var literal = LiteralMask(source);
        var spans = new List<(int, int)>();
        var at = 0;
        while ((at = source.IndexOf(marker, at, StringComparison.Ordinal)) >= 0)
        {
            var start = at + marker.Length;
            if (literal[at])
            {
                at = start;
                continue;
            }

            var depth = 1;
            var i = start;
            for (; i < source.Length && depth > 0; i++)
            {
                if (literal[i]) continue;
                if (source[i] == '(') depth++;
                else if (source[i] == ')') depth--;
            }

            spans.Add((start, i));
            at = start;
        }

        return spans;
    }

    /// <summary>The span of each declaration of <paramref name="method"/> (from its name to the end of a brace body, or to the semicolon of an expression body). Calls of the same name are not declarations and are skipped.</summary>
    private static List<(int Start, int End)> MethodSpans(string source, bool[] literal, string method)
    {
        var spans = new List<(int, int)>();
        foreach (Match decl in Regex.Matches(source, @"(?<![\w.])" + Regex.Escape(method) + @"\s*\(", RegexOptions.CultureInvariant))
        {
            if (literal[decl.Index])
            {
                continue;
            }

            var depth = 0;
            var i = decl.Index + decl.Length - 1;
            for (; i < source.Length; i++)
            {
                if (literal[i]) continue;
                if (source[i] == '(') depth++;
                else if (source[i] == ')' && --depth == 0) break;
            }

            var j = i + 1;
            while (j < source.Length && char.IsWhiteSpace(source[j])) j++;
            if (j + 1 < source.Length && source[j] == '=' && source[j + 1] == '>')
            {
                var semi = j;
                while (semi < source.Length && !(source[semi] == ';' && !literal[semi])) semi++;
                spans.Add((decl.Index, Math.Min(semi + 1, source.Length)));
            }
            else if (j < source.Length && source[j] == '{')
            {
                var braces = 0;
                for (var k = j; k < source.Length; k++)
                {
                    if (literal[k]) continue;
                    if (source[k] == '{') braces++;
                    else if (source[k] == '}' && --braces == 0)
                    {
                        spans.Add((decl.Index, k + 1));
                        break;
                    }
                }
            }
        }

        return spans;
    }

    /// <summary>The text of <paramref name="method"/>'s body (brace-matched), from its declaration to the closing brace.</summary>
    private static string MethodBody(string source, string method)
    {
        var literal = LiteralMask(source);
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
                    if (literal[i]) continue;
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
