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
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348, Lite's live plan display. The plans a Lite window fetches live from the monitored server
/// (<c>FetchQueryPlanOnDemandAsync</c>, <c>FetchProcedurePlanOnDemandAsync</c>, <c>FetchQueryStorePlanAsync</c>) are
/// not collected, so the collection-time filter never sees them: each display site passes the result through
/// <see cref="LivePlanDisplay.Filter"/> at the call. Pinned here: the helper, a scan of every caller in the app, and
/// the state the plan viewer is left in for a plan the filter withholds whole.
/// </summary>
public sealed class StatementScrubLivePlanDisplayTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    [Fact]
    public void ALiveFetchedCanaryPlan_ReachesTheDisplayStringWithTheMarker()
    {
        var shown = LivePlanDisplay.Filter(StatementScrubCanary.CanaryPlan());

        Assert.NotNull(shown);
        Assert.Contains(Marker, shown!, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, shown!, StringComparison.Ordinal);
        }

        foreach (var kept in StatementScrubCanary.KeptNeedles)
        {
            Assert.Contains(kept, shown!, StringComparison.Ordinal);
        }

        /* What the viewer loads: the plan still parses, statement 1 reads as the marker, statement 2 is untouched. */
        var parsed = ShowPlanParser.Parse(shown!);
        Assert.Null(parsed.ParseError);
    }

    [Fact]
    public void APlanTheFilterFindsNothingIn_IsTheSameInstance_AndNullAndEmptyPassThrough()
    {
        var plain = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + StatementScrubCanary.PlainStatement + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

        Assert.Same(plain, LivePlanDisplay.Filter(plain));
        Assert.Null(LivePlanDisplay.Filter(null));
        Assert.Equal(string.Empty, LivePlanDisplay.Filter(string.Empty));
    }

    /// <summary>
    /// The shared parser still refuses the marker (it is not a plan), which is why the viewer must not hand it to the
    /// parser: <c>PlanViewerControl.LoadPlan</c> answers a whole-plan marker with the withheld sentence instead of the
    /// parse error ("The plan XML could not be read"), and the next test holds that.
    /// </summary>
    [Fact]
    public void TheParserRefusesTheMarker_SoTheViewerMustNotParseIt()
    {
        var parsed = ShowPlanParser.Parse(Marker);

        Assert.False(string.IsNullOrEmpty(PlanDisplayText.ParseErrorMessage(parsed)));
        Assert.DoesNotContain(PlanStatements.EnumerateAll(parsed), s => s.RootNode != null);
    }

    [Fact]
    public void APlanWithheldWhole_ShowsTheWithheldSentenceInTheViewer_NotAParseError()
    {
        OnStaThread(() =>
        {
            var control = new PlanViewerControl();
            try
            {
                control.LoadPlan(Marker, "Stored Plan").GetAwaiter().GetResult();

                var title = (TextBlock)control.FindName("EmptyStateTitle");
                Assert.Equal(SensitiveStatements.WithheldPlanSentence, title.Text);
                Assert.Equal("This plan was withheld by the statement filter (#4348).", title.Text);
                Assert.DoesNotContain("could not be read", title.Text, StringComparison.Ordinal);
                Assert.Equal(Visibility.Visible, ((UIElement)control.FindName("EmptyState")).Visibility);
                Assert.Equal(Visibility.Collapsed, ((UIElement)control.FindName("EmptyStateDetail")).Visibility);
                Assert.Equal(Visibility.Collapsed, ((UIElement)control.FindName("PlanScrollViewer")).Visibility);
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void TheWithheldGuard_RefusesOnlyTheMarker_AndARealPlanIsUnchanged()
    {
        Assert.Equal(SensitiveStatements.WithheldPlanSentence, WithheldPlanGuard.WithheldSentence(Marker));
        Assert.Equal(SensitiveStatements.WithheldPlanSentence, WithheldPlanGuard.WithheldSentence("  " + Marker + "\r\n"));

        var plain = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + StatementScrubCanary.PlainStatement + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

        Assert.Null(WithheldPlanGuard.WithheldSentence(plain));
        Assert.Null(WithheldPlanGuard.WithheldSentence(StatementScrubCanary.CanaryPlan()));
        Assert.Null(WithheldPlanGuard.WithheldSentence(null));
        Assert.Null(WithheldPlanGuard.WithheldSentence(string.Empty));

        /* A real plan is not refused, and nothing is shown for it (a refusal would put up a message box). */
        Assert.False(WithheldPlanGuard.RefuseSave(plain));
        Assert.False(WithheldPlanGuard.RefuseSave(null));
        Assert.Null(ShowPlanParser.Parse(plain).ParseError);
    }

    /// <summary>
    /// Every place in the app that writes a plan to a <c>.sqlplan</c> file asks <c>WithheldPlanGuard.RefuseSave</c>
    /// first, so the marker is never saved as a plan. A new save site fails here until it does.
    /// </summary>
    [Fact]
    public void EveryPlanSaveSite_RefusesTheWithheldMarkerFirst()
    {
        var problems = new List<string>();
        var sites = 0;

        foreach (var root in new[] { "Lite", "PerformanceMonitor.Ui" })
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(RepoRoot(), root), "*.cs", SearchOption.AllDirectories))
            {
                var relative = Path.GetRelativePath(RepoRoot(), file).Replace('\\', '/');
                if (relative.Contains("/obj/", StringComparison.Ordinal) || relative.Contains("/bin/", StringComparison.Ordinal))
                {
                    continue;
                }

                var text = File.ReadAllText(file);
                if (!text.Contains("new SaveFileDialog", StringComparison.Ordinal) || !text.Contains(".sqlplan\"", StringComparison.Ordinal))
                {
                    continue;
                }

                sites++;
                if (!text.Contains("WithheldPlanGuard.RefuseSave(", StringComparison.Ordinal))
                {
                    problems.Add(relative + " writes a .sqlplan without WithheldPlanGuard.RefuseSave");
                }
            }
        }

        /* The three history windows, the shared SavePlanFile helper and the viewer's own Save button. */
        Assert.True(sites >= 5, "expected at least 5 plan save sites, found " + sites);
        Assert.Empty(problems);
    }

    private static readonly Regex LiveFetchCall = new(
        @"LocalDataService\s*\.\s*Fetch\w*Plan\w*\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex Wrapped = new(
        @"LivePlanDisplay\s*\.\s*Filter\s*\(\s*await\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Every caller of a live plan fetch in the app passes the result through the helper at the call. The MCP plan
    /// tools are the one exception: they answer through <c>AnalyzeFilteredPlan</c> (see
    /// <c>StoredPlanAnalysisFilterTests</c>). A new caller fails here until it is wrapped.
    /// </summary>
    [Fact]
    public void EveryLiveFetchCaller_PassesThePlanThroughTheHelperAtTheCall()
    {
        var liteRoot = Path.Combine(RepoRoot(), "Lite");
        var problems = new List<string>();
        var sites = 0;
        var files = new HashSet<string>(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(liteRoot, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(liteRoot, file).Replace('\\', '/');
            if (relative.StartsWith("obj/", StringComparison.Ordinal)
                || relative.StartsWith("bin/", StringComparison.Ordinal)
                || relative.StartsWith("Services/LocalDataService", StringComparison.Ordinal)
                || relative == "Mcp/McpPlanTools.cs")
            {
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            foreach (Match match in LiveFetchCall.Matches(code))
            {
                sites++;
                files.Add(relative);
                /* The call is `LivePlanDisplay.Filter(await LocalDataService.Fetch...(`, so what precedes `LocalDataService`
                   must end in the wrapper and the `await`. */
                var awaitIndex = code.LastIndexOf("await", match.Index, StringComparison.Ordinal);
                var before = awaitIndex < 0 ? string.Empty : code[..(awaitIndex + "await".Length)];
                var between = awaitIndex < 0 ? "x" : code[(awaitIndex + "await".Length)..match.Index];
                if (awaitIndex < 0 || between.Trim().Length != 0 || !Wrapped.IsMatch(before))
                {
                    var line = code[..match.Index].Count(c => c == '\n') + 1;
                    problems.Add(relative + ":" + line + " calls " + match.Value.TrimEnd('(') + " without LivePlanDisplay.Filter(await ...)");
                }
            }
        }

        /* A scan that found nothing would pass vacuously: the display sites are in these six files. The pattern takes any
           fetch-a-plan name (the by-sql_handle fetch the blocked-process and deadlock actions use included, #5320), so a
           fetch added under a new name fails here until it is wrapped. */
        Assert.True(sites >= 18, "expected at least 18 live plan fetch call sites, found " + sites);
        Assert.Equal(6, files.Count);
        Assert.Empty(problems);
    }

    /// <summary>
    /// #5367: a withheld deadlock graph or blocked process report is not "a plan", so its save refusal names what is
    /// being saved; the plan viewers keep their existing sentence word for word.
    /// </summary>
    [Fact]
    public void TheWithheldSentence_NamesWhatIsSaved_AndThePlanSentenceIsUnchanged()
    {
        Assert.Equal("This plan was withheld by the statement filter (#4348).", WithheldPlanGuard.WithheldSentenceFor("plan"));
        Assert.Equal(SensitiveStatements.WithheldPlanSentence, WithheldPlanGuard.WithheldSentenceFor("plan"));
        Assert.Equal("This deadlock graph was withheld by the statement filter (#4348).", WithheldPlanGuard.WithheldSentenceFor("deadlock graph"));
        Assert.Equal("This blocked process report was withheld by the statement filter (#4348).", WithheldPlanGuard.WithheldSentenceFor("blocked process report"));
    }

    /// <summary>
    /// #5320: a deadlock graph or blocked process report the filter withheld whole is the marker, not XML. The shared
    /// <c>FileSaveHelper.SaveXmlToFile</c> (the one save path for both reports) asks the guard before it opens the save
    /// dialog, so the marker is never written to an <c>.xml</c> file and the user is told why.
    /// </summary>
    [Fact]
    public void SaveXmlToFile_RefusesAWithheldReportBeforeTheSaveDialog_AndLitesDownloadsUseIt()
    {
        var helper = File.ReadAllText(Path.Combine(RepoRoot(), "PerformanceMonitor.Ui", "FileSaveHelper.cs"));
        var start = helper.IndexOf("public static void SaveXmlToFile(", StringComparison.Ordinal);
        Assert.True(start >= 0, "SaveXmlToFile not found");
        var dialog = helper.IndexOf("new SaveFileDialog", start, StringComparison.Ordinal);
        var guard = helper.IndexOf("WithheldPlanGuard.RefuseSave(xml, withheldSubject)", start, StringComparison.Ordinal);
        Assert.True(guard >= 0 && dialog >= 0 && guard < dialog, "SaveXmlToFile must call WithheldPlanGuard.RefuseSave(xml, withheldSubject) before it opens the dialog");

        var plans = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "ServerTab.Plans.cs"));
        Assert.Contains("SaveXmlToFile(row.DeadlockGraphXml", plans, StringComparison.Ordinal);
        Assert.Contains("SaveXmlToFile(row.BlockedProcessReportXml", plans, StringComparison.Ordinal);

        /* #5367: the sentence names what is saved. A deadlock graph is not "a plan"; each download passes its own subject. */
        Assert.Contains("SaveXmlToFile(row.DeadlockGraphXml, $\"deadlock_{row.DeadlockTime:yyyyMMdd_HHmmss}.xml\", \"deadlock XML\", \"deadlock graph\")", plans, StringComparison.Ordinal);
        Assert.Contains("\"blocked process XML\", \"blocked process report\")", plans, StringComparison.Ordinal);
        Assert.DoesNotContain("File.WriteAllText", plans, StringComparison.Ordinal);
    }

    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
