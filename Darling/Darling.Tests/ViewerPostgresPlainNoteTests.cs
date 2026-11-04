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

        Assert.Equal("Plan capture is not ready on this server. Not met: the auto_explain library loaded at server start, query ids in the log.", note.Text);
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

    [Fact]
    public void LockStats_WithRows_SaysHowManyQueuesInOneLine_AndKeepsTheReadingGuideForTheHover()
    {
        var granted = ViewerServerTab.LockStatsRowsNote(queuedCount: 0, totalCaptures: 1200);
        Assert.Equal("No lock was waiting in any of 1,200 captures. Every lock held in this window was granted.", granted.Text);
        Assert.DoesNotContain("healthy answer", granted.Text, StringComparison.Ordinal);
        Assert.Contains("healthy answer", granted.Detail, StringComparison.Ordinal);

        var one = ViewerServerTab.LockStatsRowsNote(queuedCount: 1, totalCaptures: 60);
        Assert.Equal("1 lock queue across 60 captures.", one.Text);
        var many = ViewerServerTab.LockStatsRowsNote(queuedCount: 3, totalCaptures: 60);
        Assert.Equal("3 lock queues across 60 captures.", many.Text);
        Assert.DoesNotContain("SAMPLES", many.Text, StringComparison.Ordinal);
        Assert.Contains("SAMPLES", many.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void AutovacuumIoAndWaitTrend_ShowOneShortSentence_WithTheReplicaOrVersionNoteOnHover()
    {
        const string AutovacuumShort = "No table reported an autovacuum backlog in this window.";
        const string AutovacuumFull = AutovacuumShort + " This collector runs on the WRITER only — pg_stat_user_tables reports zeros on a replica — so an empty grid on a reader is expected rather than informative.";
        var autovacuum = ViewerServerTab.EmptyNoteParts(null, offline: false, rowCount: 0, AutovacuumShort, AutovacuumFull);
        Assert.Equal(AutovacuumShort, autovacuum.Text);
        Assert.Contains("replica", autovacuum.Detail, StringComparison.Ordinal);

        var io = ViewerServerTab.EmptyNoteParts(null, offline: false, rowCount: 0, "No I/O was recorded in this window.",
            "No backend / object / context combination did any I/O in this window. This view needs PostgreSQL 16 or newer.");
        Assert.Equal("No I/O was recorded in this window.", io.Text);
        Assert.Contains("PostgreSQL 16", io.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCaptureFacetLabel_IsPlain_CapitalisedForTheGrid_AndKeepsAnUnknownFacetsName()
    {
        Assert.Equal("The auto_explain extension", ViewerServerTab.PlanCaptureFacetLabel("extension_available", capitalize: true));
        Assert.Equal("the auto_explain extension", ViewerServerTab.PlanCaptureFacetLabel("extension_available"));
        Assert.Equal("English log messages", ViewerServerTab.PlanCaptureFacetLabel("message_locale", capitalize: true));
        Assert.Equal("future_facet", ViewerServerTab.PlanCaptureFacetLabel("future_facet", capitalize: true));
    }
}
