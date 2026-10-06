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
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320 (the plan viewers half of #4348), the Darling viewer's side. A stored or live plan the statement filter
/// withheld whole is the marker, not a plan. The viewer shows it as withheld
/// (<see cref="SensitiveStatements.WithheldPlanSentence"/>) instead of the parser's "The plan XML could not be read",
/// and no site that saves a plan to a <c>.sqlplan</c> file writes it. The control and the guard are the shared
/// <c>PerformanceMonitor.Ui</c> ones Lite uses too; Lite.Tests holds the same pins for Lite's own save sites.
/// </summary>
public sealed class StatementScrubViewerPlanWithheldTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

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
    }

    /// <summary>
    /// Every place the Darling viewer (and the shared plan control it hosts) writes a plan to a <c>.sqlplan</c> file
    /// asks <c>WithheldPlanGuard.RefuseSave</c> first. A new save site fails here until it does.
    /// </summary>
    [Fact]
    public void EveryPlanSaveSite_RefusesTheWithheldMarkerFirst()
    {
        var problems = new List<string>();
        var sites = 0;

        foreach (var root in new[] { "Darling/PerformanceMonitor.Darling.Viewer", "PerformanceMonitor.Ui" })
        {
            foreach (var file in Directory.EnumerateFiles(RepoFile.PathTo(root), "*.cs", SearchOption.AllDirectories))
            {
                var relative = file.Replace('\\', '/');
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

        /* The three history windows, the shared SavePlanFile helper and the plan control's own Save button. */
        Assert.True(sites >= 5, "expected at least 5 plan save sites, found " + sites);
        Assert.Empty(problems);
    }

    /// <summary>
    /// #5320: a deadlock graph or blocked process report the filter withheld whole is the marker, not XML. The viewer's
    /// two XML downloads go through the shared <c>FileSaveHelper.SaveXmlToFile</c>, which asks the guard before it
    /// opens the save dialog, so the marker is never written to an <c>.xml</c> file and the user is told why.
    /// </summary>
    [Fact]
    public void SaveXmlToFile_RefusesAWithheldReportBeforeTheSaveDialog_AndTheViewersDownloadsUseIt()
    {
        var helper = File.ReadAllText(RepoFile.PathTo("PerformanceMonitor.Ui/FileSaveHelper.cs"));
        var start = helper.IndexOf("public static void SaveXmlToFile(", StringComparison.Ordinal);
        Assert.True(start >= 0, "SaveXmlToFile not found");
        var dialog = helper.IndexOf("new SaveFileDialog", start, StringComparison.Ordinal);
        var guard = helper.IndexOf("WithheldPlanGuard.RefuseSave(xml)", start, StringComparison.Ordinal);
        Assert.True(guard >= 0 && dialog >= 0 && guard < dialog, "SaveXmlToFile must call WithheldPlanGuard.RefuseSave(xml) before it opens the dialog");

        var export = File.ReadAllText(RepoFile.PathTo("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.CopyExport.cs"));
        Assert.Contains("using static PerformanceMonitor.Ui.FileSaveHelper;", export, StringComparison.Ordinal);
        Assert.Contains("SaveXmlToFile(row.DeadlockGraphXml", export, StringComparison.Ordinal);
        Assert.Contains("SaveXmlToFile(row.BlockedProcessReportXml", export, StringComparison.Ordinal);
        Assert.DoesNotContain("File.WriteAllText", export, StringComparison.Ordinal);
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
}
