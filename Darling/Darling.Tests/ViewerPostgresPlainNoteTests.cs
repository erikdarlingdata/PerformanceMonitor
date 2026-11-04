/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7410: a Darling Viewer PostgreSQL panel with no rows shows one plain sentence. The longer explanation (the collector's design, setting
/// names, what else an empty grid can mean) is one hover away, not the panel's whole note.
/// </summary>
public sealed class ViewerPostgresPlainNoteTests
{
    private const string Short = "This server has no replication slots.";
    private const string Full = "This server has no replication slots, so nothing is retaining WAL. Zero rows is the healthy answer here, not a missing read.";

    [Fact]
    public void AnEmptyPanel_ShowsTheShortSentence_AndKeepsTheFullTextForTheHover()
    {
        var note = ViewerServerTab.EmptyNoteParts(null, offline: false, rowCount: 0, Short, Full);

        Assert.Equal(Short, note.Text);
        Assert.Equal(Full, note.Detail);
        Assert.DoesNotContain("not a missing read", note.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOfflineServer_AnEngineGap_AndAPanelWithRows_CarryNoHoverText()
    {
        Assert.Equal(ViewerServerTab.OfflineEmptyText, ViewerServerTab.EmptyNoteParts(null, offline: true, rowCount: 0, Short, Full).Text);
        Assert.Null(ViewerServerTab.EmptyNoteParts(null, offline: true, rowCount: 0, Short, Full).Detail);
        Assert.Equal(("gap", (string?)null), (ViewerServerTab.EmptyNoteParts("gap", offline: false, rowCount: 0, Short, Full).Text,
            ViewerServerTab.EmptyNoteParts("gap", offline: false, rowCount: 0, Short, Full).Detail));
        var withRows = ViewerServerTab.EmptyNoteParts(null, offline: false, rowCount: 4, Short, Full);
        Assert.Equal(string.Empty, withRows.Text);
        Assert.Null(withRows.Detail);
    }

    [Fact]
    public void PlanCaptureReadiness_NamesTheUnmetSteps_InPlainWords_NotTheFacetFields()
    {
        var note = ViewerServerTab.PlanCaptureReadinessNote(new (string, bool)[]
        {
            ("extension_available", true),
            ("library_loaded", false),
            ("plan_attribution", false),
        });

        Assert.Equal("Plan capture is not ready on this server. Not met: auto_explain loaded at server start, query ids in the log.", note.Text);
        Assert.DoesNotContain("library_loaded", note.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_attribution", note.Text, StringComparison.Ordinal);
        Assert.Contains("plan_attribution is the one that is easy to miss", note.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCaptureReadiness_WhenEverythingIsMet_OrNothingWasCollected_SaysSoInOneLine()
    {
        var met = ViewerServerTab.PlanCaptureReadinessNote(new (string, bool)[] { ("extension_available", true), ("message_locale", true) });
        Assert.Equal("Every precondition for plan capture is met on this server.", met.Text);
        Assert.NotNull(met.Detail);

        var none = ViewerServerTab.PlanCaptureReadinessNote(Array.Empty<(string, bool)>());
        Assert.Equal("Plan capture readiness has not been collected for this server yet.", none.Text);
        Assert.NotNull(none.Detail);
    }
}
