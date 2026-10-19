/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4789: the Darling viewer's migration and its import from another viewer's registry never leave out a server
/// silently because a DIFFERENT server already holds its id.
///
/// <para><b>The defect.</b> Both wrote with <c>InsertMonitoredServerIfAbsentAsync</c>, whose <c>ON CONFLICT
/// (server_id) DO NOTHING</c> answers "nothing written" for a taken id, and the two imports counted that as "already
/// there". A server whose id is only SHARED with a different one (the id is a 32-bit hash of the identity) was
/// therefore dropped with no message, and it read as a server the store already had. Both now go through
/// <see cref="ViewerDataService.AddMonitoredServerAsync"/>, the write the add dialogs use, and count a refused
/// collision apart: <see cref="ViewerServerImportResult.Collided"/>.</para>
///
/// <para><b>What is pinned here</b> is everything that needs no store: how each outcome of the add is counted,
/// what the warning says, and the wiring (both imports add through the guarded write, and the import message
/// reports the collided count). The insert-then-read against a real store runs in
/// <see cref="ViewerImportServerIdCollisionLiveTests"/> in the PostgreSQL CI job.</para>
/// </summary>
public sealed class ViewerImportServerIdCollisionTests
{
    /* The synthetic pair the add tests use: two hosts whose storage names hash to ONE server_id. */
    private const string HolderHost = "sql9jocsv";
    private const string CollidingHost = "sqlsvvqew";

    private static MonitoredServerRow Row(string host, string name) => new()
    {
        ServerId = ViewerDataService.ComputeServerId(host, null, false),
        Name = name,
        Host = host,
    };

    private static MonitoredServerAddResult Answer(MonitoredServerAddOutcome outcome, MonitoredServerRow? occupant = null) =>
        new(outcome, occupant);

    [Fact]
    public void ThePairThePinsUse_StillDerivesOneId()
    {
        Assert.Equal(Row(HolderHost, "a").ServerId, Row(CollidingHost, "b").ServerId);
    }

    [Fact]
    public void ARowIsCountedByWhatTheAddAnswered_AndOnlyACollisionIsCountedApart()
    {
        var holder = Row(HolderHost, "Payroll");
        var colliding = Row(CollidingHost, "Reporting");
        var start = new ViewerServerImportResult(Imported: 0, Collided: 0);

        var added = ViewerServerMigration.Record(start, colliding, Answer(MonitoredServerAddOutcome.Added));
        var collided = ViewerServerMigration.Record(start, colliding, Answer(MonitoredServerAddOutcome.Collides, holder));
        var duplicate = ViewerServerMigration.Record(start, colliding, Answer(MonitoredServerAddOutcome.Duplicate, colliding));
        var notSaved = ViewerServerMigration.Record(start, colliding, Answer(MonitoredServerAddOutcome.NotSaved));

        Assert.Equal(new ViewerServerImportResult(Imported: 1, Collided: 0), added);
        Assert.Equal(new ViewerServerImportResult(Imported: 0, Collided: 1), collided);
        Assert.Equal(start, duplicate);
        Assert.Equal(start, notSaved);
    }

    [Fact]
    public void TheCounts_AccumulateAcrossRows()
    {
        var holder = Row(HolderHost, "Payroll");
        var colliding = Row(CollidingHost, "Reporting");

        var total = new ViewerServerImportResult(0, 0);
        total = ViewerServerMigration.Record(total, holder, Answer(MonitoredServerAddOutcome.Added));
        total = ViewerServerMigration.Record(total, colliding, Answer(MonitoredServerAddOutcome.Collides, holder));
        total = ViewerServerMigration.Record(total, holder, Answer(MonitoredServerAddOutcome.Duplicate, holder));
        total = ViewerServerMigration.Record(total, Row("other01", "Other"), Answer(MonitoredServerAddOutcome.Added));

        Assert.Equal(new ViewerServerImportResult(Imported: 2, Collided: 1), total);
    }

    [Fact]
    public void TheCollisionWarning_NamesTheRowAndTheServerItCollidesWith()
    {
        var text = ViewerServerMigration.DescribeCollision(Row(CollidingHost, "Reporting"), Row(HolderHost, "Payroll"));

        Assert.StartsWith("Not imported", text, StringComparison.Ordinal);
        Assert.Contains("'Reporting' (" + CollidingHost + ")", text, StringComparison.Ordinal);
        Assert.Contains("'Payroll' (" + HolderHost + ")", text, StringComparison.Ordinal);
        Assert.Contains("collides", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCollisionWarning_SaysItTheWayTheAddDialogDoes()
    {
        var holder = Row(HolderHost, "Payroll");
        var dialog = AddServerDialog.DescribeRefusedAdd(Answer(MonitoredServerAddOutcome.Collides, holder));
        var warning = ViewerServerMigration.DescribeCollision(Row(CollidingHost, "Reporting"), holder);

        const string shared = "collides with 'Payroll'";
        Assert.Contains(shared, dialog, StringComparison.Ordinal);
        Assert.Contains(shared, warning, StringComparison.Ordinal);
        Assert.Contains("a different server that is already monitored", dialog, StringComparison.Ordinal);
        Assert.Contains("a different server that is already monitored", warning, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both imports write through the guarded add and not the bare insert. Held at the source level because the
    /// suite cannot stand up a store here, and a swap back to <c>InsertMonitoredServerIfAbsentAsync</c> is
    /// invisible to every other store-free test.
    /// </summary>
    [Fact]
    public void TheMigration_AndTheImportFromAStore_AddThroughTheGuardedAdd_NotTheBareInsert()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerMigration.cs");

        var migrate = BodyBetween(source, "public async Task<int> MigrateAsync(", "public static async Task<ViewerServerImportResult> ImportFromStoreAsync(");
        var import = BodyBetween(source, "public static async Task<ViewerServerImportResult> ImportFromStoreAsync(", "public (MonitoredServerRow? Row, string? SkipReason) TryProjectEntry(");

        foreach (var body in new[] { migrate, import })
        {
            Assert.Contains("AddMonitoredServerAsync(row, cancellationToken)", body, StringComparison.Ordinal);
            Assert.Contains("Record(", body, StringComparison.Ordinal);
            Assert.DoesNotContain("InsertMonitoredServerIfAbsentAsync", body, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheImportMessage_ReportsTheCollidedCount()
    {
        var window = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.ServerManagement.cs");

        Assert.Contains(
            "(pushedToStore, collidedInStore) = await ViewerServerMigration.ImportFromStoreAsync(",
            window,
            StringComparison.Ordinal);
        Assert.Contains("if (collidedInStore > 0)", window, StringComparison.Ordinal);
    }

    private static string BodyBetween(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        var to = source.IndexOf(end, StringComparison.Ordinal);
        Assert.True(from >= 0 && to > from, $"'{start}' must be there, before '{end}'");
        return source[from..to];
    }
}
