/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Windows.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4766: a viewer grid time column that shows the UTC offset in the repeated autumn hour must sort by the row's true UTC
/// DateTime, not by its text and not by a DateTime already converted to the display zone.
///
/// <para>The text sorts wrong east of UTC: "02:30 +01:00" sorts before "02:30 +02:00", so the second pass of the hour
/// comes out before the first. A display-zone DateTime (a <c>...Local</c> value made by <c>ForDisplay</c>) is no better:
/// the second pass at 01:15 (06:15Z) sorts before the first pass at 01:45 (05:45Z). Only the UTC instant puts both passes
/// in order, so each column carries the row's UTC member in <c>SortMemberPath</c>. The filter button's <c>Tag</c> is a
/// separate contract and is left alone.</para>
///
/// <para>A row read from a stored server wall clock (job history, default trace, the query and procedure stats creation
/// and execution times) keeps its text sort, and a row with no UTC member of its own (the System Events rows) is not in
/// the table.</para>
/// </summary>
/* Serialized with the other classes that flip the process-wide ViewerTimeHelper statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerGridTimeColumnSortMemberTests
{
    private const string ViewerFolder = "PerformanceMonitor.Darling.Viewer";

    /// <summary>The row type is carried as its assembly-qualified name, so each case stays a serializable, separately reported row.</summary>
    private static string Name(Type type) => type.AssemblyQualifiedName!;

    /// <summary>(xaml in the viewer project, the DataGrid's x:Name, the bound text property, the row type, the SortMemberPath).</summary>
    public static TheoryData<string, string, string, string, string> Columns => new()
    {
        { "FinOpsTab.xaml", "FinOpsApplicationConnectionsDataGrid", "FirstSeenText", Name(typeof(ApplicationConnectionRow)), "FirstSeenUtc" },
        { "FinOpsTab.xaml", "FinOpsApplicationConnectionsDataGrid", "LastSeenText", Name(typeof(ApplicationConnectionRow)), "LastSeenUtc" },
        { "FinOpsTab.xaml", "FinOpsServerInventoryDataGrid", "InventoryAsOfText", Name(typeof(ServerPropertyRow)), "InventoryAsOfUtc" },
        { "FinOpsTab.xaml", "FinOpsServerInventoryDataGrid", "LastCollectedText", Name(typeof(ServerPropertyRow)), "LastCollectedUtc" },
        { "ViewerServerTab.xaml", "QueryStoreClutterGrid", "OptionsCapturedText", Name(typeof(ViewerDataService.QueryStoreClutterRow)), "OptionsCapturedUtc" },
        { "ViewerServerTab.xaml", "QueryStoreOverheadGrid", "LastObservedText", Name(typeof(ViewerDataService.QueryStoreOverheadWaitRow)), "LastObservedUtc" },
        { "ViewerServerTab.xaml", "QuerySnapshotsGrid", "CollectionTimeLocal", Name(typeof(ViewerQuerySnapshotRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "CurrentActiveQueriesGrid", "CollectionTimeLocal", Name(typeof(ViewerQuerySnapshotRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "QueryStoreGrid", "LastExecutionTimeLocal", Name(typeof(ViewerQueryStoreRow)), "LastExecutionTime" },
        { "ViewerServerTab.xaml", "QueryStoreGrid", "FirstExecutionTimeLocal", Name(typeof(ViewerQueryStoreRow)), "FirstExecutionTime" },
        { "ViewerServerTab.xaml", "QueryStoreRegressionsGrid", "LastExecutionTimeLocal", Name(typeof(ViewerQueryStoreRegressionRow)), "LastExecutionTime" },
        { "ViewerServerTab.xaml", "PlanCorrectionGrid", "CollectionTimeLocal", Name(typeof(PlanCorrectionRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "PlanCorrectionGrid", "ValidSinceLocal", Name(typeof(PlanCorrectionRow)), "ValidSince" },
        { "ViewerServerTab.xaml", "PlanCorrectionGrid", "LastRefreshLocal", Name(typeof(PlanCorrectionRow)), "LastRefresh" },
        { "ViewerServerTab.xaml", "PlanCorrectionGrid", "ExecuteActionInitiatedTimeLocal", Name(typeof(PlanCorrectionRow)), "ExecuteActionInitiatedTime" },
        { "ViewerServerTab.xaml", "PlanCorrectionGrid", "RevertActionInitiatedTimeLocal", Name(typeof(PlanCorrectionRow)), "RevertActionInitiatedTime" },
        { "ViewerServerTab.xaml", "BlockedProcessReportGrid", "EventTimeLocal", Name(typeof(ViewerBlockedProcessRow)), "EventTime" },
        { "ViewerServerTab.xaml", "DeadlockGrid", "DeadlockTimeLocal", Name(typeof(DeadlockProcessDetail)), "DeadlockTime" },
        { "ViewerServerTab.xaml", "AutomaticTuningGrid", "CollectionTimeLocal", Name(typeof(AutomaticTuningRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "ServerConfigChangesGrid", "ChangeTimeDisplay", Name(typeof(ServerConfigChangeRow)), "ChangeTime" },
        { "ViewerServerTab.xaml", "DatabaseConfigChangesGrid", "ChangeTimeDisplay", Name(typeof(DatabaseConfigChangeRow)), "ChangeTime" },
        { "ViewerServerTab.xaml", "TraceFlagChangesGrid", "ChangeTimeDisplay", Name(typeof(TraceFlagChangeRow)), "ChangeTime" },
        { "ViewerServerTab.xaml", "CollectionCaveatsGrid", "FirstSeenFormatted", Name(typeof(CollectionCaveatRow)), "FirstSeenUtc" },
        { "ViewerServerTab.xaml", "CollectionCaveatsGrid", "LastSeenFormatted", Name(typeof(CollectionCaveatRow)), "LastSeenUtc" },
        { "ViewerServerTab.xaml", "PgCollectionCaveatsGrid", "FirstSeenFormatted", Name(typeof(CollectionCaveatRow)), "FirstSeenUtc" },
        { "ViewerServerTab.xaml", "PgCollectionCaveatsGrid", "LastSeenFormatted", Name(typeof(CollectionCaveatRow)), "LastSeenUtc" },
        { "ViewerServerTab.xaml", "CollectionHealthGrid", "LastSuccessFormatted", Name(typeof(CollectorHealthRow)), "LastSuccessTime" },
        { "ViewerServerTab.xaml", "CollectionHealthGrid", "LastRunFormatted", Name(typeof(CollectorHealthRow)), "LastRunTime" },
        { "ViewerServerTab.xaml", "CollectionHealthGrid", "LastErrorFormatted", Name(typeof(CollectorHealthRow)), "LastErrorTime" },
        { "ViewerServerTab.xaml", "CollectionLogGrid", "CollectionTimeFormatted", Name(typeof(CollectionLogRow)), "CollectionTime" },
        { "CollectionLogWindow.xaml", "LogDataGrid", "CollectionTimeFormatted", Name(typeof(CollectionLogRow)), "CollectionTime" },
        { "WaitDrillDownWindow.xaml", "ResultsDataGrid", "CollectionTimeLocal", Name(typeof(ViewerQuerySnapshotRow)), "CollectionTime" },
        { "QueryStoreHistoryWindow.xaml", "HistoryDataGrid", "FirstExecutionTimeLocal", Name(typeof(ViewerQueryStoreHistoryRow)), "FirstExecutionTime" },
        { "QueryStoreHistoryWindow.xaml", "HistoryDataGrid", "LastExecutionTimeLocal", Name(typeof(ViewerQueryStoreHistoryRow)), "LastExecutionTime" },
        { "ManageServersWindow.xaml", "ServersGrid", "LastCollectedDisplay", Name(typeof(ManagedServerListItem)), "LastCollectedUtc" },
        { "NotificationRoutesWindow.xaml", "RoutesGrid", "ModifiedDisplay", Name(typeof(NotificationRouteRow)), "ModifiedAtUtc" },
    };

    /// <summary>
    /// The column bound to the text property carries the row's UTC member in <c>SortMemberPath</c>, and that member is a
    /// public <see cref="DateTime"/> or <c>DateTime?</c> on the row type. A wrong name sorts nothing and raises nothing,
    /// so the name is checked against the row type the grid is bound to. A name ending in <c>Local</c> or <c>Display</c>
    /// would be a display-zone value and is refused.
    /// </summary>
    [Theory]
    [MemberData(nameof(Columns))]
    public void ATimeColumn_SortsByItsRowsUtcDateTime(string xamlFile, string grid, string textProperty, string rowTypeName, string sortMember)
    {
        var xaml = ReadRepoFile("Darling", ViewerFolder, xamlFile);

        var column = FindColumn(xaml, grid, textProperty);
        var declared = Regex.Match(column, @"\sSortMemberPath=""(?<path>[^""]*)""");
        Assert.True(declared.Success, $"{xamlFile}: the {grid} column bound to {textProperty} has no SortMemberPath, so it sorts by its text.");
        Assert.Equal(sortMember, declared.Groups["path"].Value);

        var rowType = Type.GetType(rowTypeName, throwOnError: true)!;
        var property = rowType.GetProperty(sortMember, BindingFlags.Public | BindingFlags.Instance);
        Assert.True(property != null, $"{rowType.Name} has no public property {sortMember}.");
        Assert.True(
            (Nullable.GetUnderlyingType(property!.PropertyType) ?? property.PropertyType) == typeof(DateTime),
            $"{rowType.Name}.{sortMember} is a {property.PropertyType.Name}, not a DateTime.");

        Assert.False(sortMember.EndsWith("Local", StringComparison.Ordinal) || sortMember.EndsWith("Display", StringComparison.Ordinal),
            $"{sortMember} reads as a display-zone member; the column must sort by the UTC one.");
    }

    /// <summary>
    /// The two passes of the repeated hour, sorted the way a header click sorts them (a <see cref="ListCollectionView"/>
    /// on the column's <c>SortMemberPath</c>). US Eastern falls back at 2026-11-01 06:00Z, so 05:45Z is 01:45 -04:00 (the
    /// first pass) and 06:15Z is 01:15 -05:00 (the second). The UTC member puts the first pass first; the display-zone
    /// DateTime and the text both put the second pass first, which is what the columns did before.
    /// </summary>
    [Fact]
    public void TheRepeatedHour_SortsFirstPassBeforeSecond_OnTheUtcMember_ButNotOnTheDisplayDateTimeOrTheText()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        var savedCulture = CultureInfo.CurrentCulture;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            ViewerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

            /* The reader sets the UTC instant and the display-zone DateTime side by side. */
            ApplicationConnectionRow Row(string name, DateTime utc) => new()
            {
                ApplicationName = name,
                FirstSeenUtc = utc,
                FirstSeenLocal = ViewerTimeHelper.ForDisplay(utc),
            };
            var firstPass = Row("first", Utc(2026, 11, 1, 5, 45));
            var secondPass = Row("second", Utc(2026, 11, 1, 6, 15));
            var rows = new[] { secondPass, firstPass };

            /* The member the column is declared to sort by, read from the XAML rather than restated here. */
            var declared = Regex.Match(
                FindColumn(ReadRepoFile("Darling", ViewerFolder, "FinOpsTab.xaml"), "FinOpsApplicationConnectionsDataGrid", "FirstSeenText"),
                @"\sSortMemberPath=""(?<path>[^""]*)""").Groups["path"].Value;

            Assert.Equal(new[] { "first", "second" }, Sorted(rows, declared));

            /* Why the display-zone value and the text are refused as sort keys. */
            Assert.Equal(new DateTime(2026, 11, 1, 1, 45, 0), firstPass.FirstSeenLocal);
            Assert.Equal(new DateTime(2026, 11, 1, 1, 15, 0), secondPass.FirstSeenLocal);
            Assert.Equal(new[] { "second", "first" }, Sorted(rows, nameof(ApplicationConnectionRow.FirstSeenLocal)));
            Assert.Equal(new[] { "second", "first" }, Sorted(rows, nameof(ApplicationConnectionRow.FirstSeenText)));
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
            CultureInfo.CurrentCulture = savedCulture;
        }
    }

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    /// <summary>The application names in the order a header click on that sort member ascending gives.</summary>
    private static string[] Sorted(ApplicationConnectionRow[] rows, string sortMemberPath)
    {
        var view = new ListCollectionView(rows);
        view.SortDescriptions.Add(new SortDescription(sortMemberPath, ListSortDirection.Ascending));
        return view.Cast<ApplicationConnectionRow>().Select(r => r.ApplicationName).ToArray();
    }

    /// <summary>The start tag of the one DataGridTextColumn inside the named grid whose binding is the text property.</summary>
    private static string FindColumn(string xaml, string grid, string textProperty)
    {
        var gridStart = Regex.Match(xaml, @"<DataGrid\b[^>]*?x:Name=""" + Regex.Escape(grid) + @"""");
        Assert.True(gridStart.Success, $"the grid {grid} is not in the file.");
        var gridEnd = xaml.IndexOf("</DataGrid>", gridStart.Index, StringComparison.Ordinal);
        Assert.True(gridEnd > gridStart.Index, $"the end of the grid {grid} was not found.");

        var columns = Regex.Matches(xaml[gridStart.Index..gridEnd], @"<DataGridTextColumn\b(?:""[^""]*""|[^>""])*>")
            .Where(m => Regex.IsMatch(m.Value, @"\sBinding=""\{Binding " + Regex.Escape(textProperty) + @"(?=[,}\s])"))
            .ToList();
        Assert.True(columns.Count == 1, $"{grid} has {columns.Count} columns bound to {textProperty}; the pin expects exactly one.");
        return columns[0].Value;
    }
}
