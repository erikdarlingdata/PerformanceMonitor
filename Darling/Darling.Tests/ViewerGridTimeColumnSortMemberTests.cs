/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Windows.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
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
/// <para>Every column that shows a time as text sorts by the row's UTC DateTime where it has one. A row with none
/// (the System Events rows, the PostgreSQL panels) gets a get-only DateTime member for the value its text is formatted
/// from. A value that only ever existed as the monitored server's own wall clock (the dm_exec_* stamps of the query and
/// procedure stats, Agent's running-job start, a transaction's begin time, the default trace) sorts by the row's
/// DateTime for that value, and the rows below marked as a stored wall clock are those. Such a value can misorder inside
/// the repeated autumn hour, because the wall time occurs twice and the clock cannot say which pass a stamp belongs to;
/// it still orders by time, where the text ordered by its characters.</para>
///
/// <para>The scan at the end reads every DataGrid column in the viewer's XAML and fails on a column that shows a time as
/// text with no SortMemberPath, or one that names its own text, so a column added later cannot slip back to sorting by
/// its text.</para>
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

        /* Columns whose row already had a UTC (or display-zone) DateTime and only lacked the SortMemberPath. */
        { "AlertsHistoryTab.xaml", "AlertsDataGrid", "TimeLocal", Name(typeof(ViewerAlertRow)), "AlertTime" },
        { "JobHistoryTab.xaml", "JobHistoryDataGrid", "RunTimeLocal", Name(typeof(ViewerJobHistoryRow)), "RunDateTimeUtc" },
        { "JobHistoryTab.xaml", "JobHistoryDataGrid", "LastSuccessfulRunLocal", Name(typeof(ViewerJobHistoryRow)), "LastSuccessfulRunUtc" },
        { "MuteRulesWindow.xaml", "RulesGrid", "ExpiresDisplay", Name(typeof(MuteRule)), "ExpiresAtUtc" },
        { "ProcedureHistoryWindow.xaml", "HistoryDataGrid", "CollectionTimeLocal", Name(typeof(ViewerProcedureStatsHistoryRow)), "CollectionTime" },
        { "QueryStatsHistoryWindow.xaml", "HistoryDataGrid", "CollectionTimeLocal", Name(typeof(ViewerQueryStatsHistoryRow)), "CollectionTime" },
        { "QueryStoreHistoryWindow.xaml", "HistoryDataGrid", "CollectionTimeLocal", Name(typeof(ViewerQueryStoreHistoryRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "LongQueryCompletionsGrid", "EventTimeLocal", Name(typeof(ViewerLongQueryRow)), "EventTime" },

        /* The System Events rows carry the raw naive-UTC XE @timestamp as EventTime (the same member Lite's rows carry). */
        { "ViewerServerTab.xaml", "SchedulerIssuesGrid", "EventTimeLocal", Name(typeof(SchedulerIssueRow)), "EventTime" },
        { "ViewerServerTab.xaml", "SevereErrorsGrid", "EventTimeLocal", Name(typeof(SevereErrorRow)), "EventTime" },
        { "ViewerServerTab.xaml", "MemoryConditionsGrid", "EventTimeLocal", Name(typeof(MemoryConditionsRow)), "EventTime" },
        { "ViewerServerTab.xaml", "MemoryBrokerGrid", "EventTimeLocal", Name(typeof(MemoryBrokerRow)), "EventTime" },
        { "ViewerServerTab.xaml", "MemoryNodeOomGrid", "EventTimeLocal", Name(typeof(MemoryNodeOomRow)), "EventTime" },
        { "ViewerServerTab.xaml", "SignificantWaitsGrid", "EventTimeLocal", Name(typeof(SignificantWaitRow)), "EventTime" },
        { "ViewerServerTab.xaml", "CpuTasksGrid", "EventTimeLocal", Name(typeof(CpuTasksRow)), "EventTime" },
        { "ViewerServerTab.xaml", "IoIssuesGrid", "EventTimeLocal", Name(typeof(IoIssuesRow)), "EventTime" },

        /* A stored wall clock: the default trace's StartTime is the server's own clock, converted to an instant with the
           server's clock, so the two passes of a repeated local hour share one instant. Sorted by that value. */
        { "ViewerServerTab.xaml", "DefaultTraceGrid", "EventTimeLocal", Name(typeof(DefaultTraceEventRow)), "EventTimeUtc" },

        /* Stored wall clocks: sys.dm_exec_* stamps, Agent's start_execution_date and a transaction's begin time are the
           monitored server's own local clock and have no UTC member. Each sorts by the row's DateTime for that value. */
        { "ViewerServerTab.xaml", "QueryStatsGrid", "LastExecutionTimeLocal", Name(typeof(ViewerQueryStatsRow)), "LastExecutionTime" },
        { "ViewerServerTab.xaml", "QueryStatsGrid", "CreationTimeLocal", Name(typeof(ViewerQueryStatsRow)), "CreationTime" },
        { "ViewerServerTab.xaml", "ProcedureStatsGrid", "LastExecutionTimeLocal", Name(typeof(ViewerProcedureStatsRow)), "LastExecutionTime" },
        { "ViewerServerTab.xaml", "ProcedureStatsGrid", "CachedTimeFormatted", Name(typeof(ViewerProcedureStatsRow)), "CachedTime" },
        { "ViewerServerTab.xaml", "RunningJobsGrid", "StartTimeLocal", Name(typeof(RunningJobRow)), "StartTime" },

        /* The last "Collected" column of the four newest-snapshot grids (#4966): naive-UTC CollectionTime, shown in the display zone. */
        { "ViewerServerTab.xaml", "RunningJobsGrid", "CollectionTimeLocal", Name(typeof(RunningJobRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "CpuSchedulerGrid", "CollectionTimeLocal", Name(typeof(CpuSchedulerGridRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "LatchStatsGrid", "CollectionTimeLocal", Name(typeof(LatchStatsSnapshotRow)), "CollectionTime" },
        { "ViewerServerTab.xaml", "SpinlockStatsGrid", "CollectionTimeLocal", Name(typeof(SpinlockStatsSnapshotRow)), "CollectionTime" },
        { "ProcedureHistoryWindow.xaml", "HistoryDataGrid", "LastExecutionTimeLocal", Name(typeof(ViewerProcedureStatsHistoryRow)), "LastExecutionTime" },
        { "ProcedureHistoryWindow.xaml", "HistoryDataGrid", "CachedTimeLocal", Name(typeof(ViewerProcedureStatsHistoryRow)), "CachedTime" },
        { "QueryStatsHistoryWindow.xaml", "HistoryDataGrid", "LastExecutionTimeLocal", Name(typeof(ViewerQueryStatsHistoryRow)), "LastExecutionTime" },
        { "QueryStatsHistoryWindow.xaml", "HistoryDataGrid", "CreationTimeLocal", Name(typeof(ViewerQueryStatsHistoryRow)), "CreationTime" },
        { "WaitDrillDownWindow.xaml", "ResultsDataGrid", "TranStartTimeLocal", Name(typeof(ViewerQuerySnapshotRow)), "TranStartTime" },

        /* A calendar day the row keeps as a DateTime; the text is "ddd MM/dd", which sorts by the weekday's name. */
        { "FinOpsTab.xaml", "FinOpsProvisioningTrendGrid", "DayDisplay", Name(typeof(ProvisioningTrendRow)), "Day" },
        { "FinOpsTab.xaml", "FinOpsMemoryGrantEfficiencyDataGrid", "DayDisplay", Name(typeof(MemoryGrantEfficiencyRow)), "Day" },

        /* The PostgreSQL panels: every timestamp is naive UTC in the store, and each display row now carries the UTC
           instant its text is formatted from. */
        { "ViewerServerTab.xaml", "PgCollectorHealthGrid", "LastRun", Name(typeof(ViewerDataService.PostgresCollectorHealthRow)), "LastRunUtc" },
        { "ViewerServerTab.xaml", "PgCpuGrid", "Time", Name(typeof(ViewerDataService.PgCpuUtilizationRow)), "SampleTimeUtc" },
        { "ViewerServerTab.xaml", "PgBlockingChainsGrid", "CapturedAt", Name(typeof(PgDisplay.ChainRow)), "CapturedAtUtc" },
        { "ViewerServerTab.xaml", "PgBlockingCyclesGrid", "CapturedAt", Name(typeof(PgDisplay.CycleRow)), "CapturedAtUtc" },
        { "ViewerServerTab.xaml", "PgSessionStatesGrid", "FirstSeenAt", Name(typeof(PgDisplay.SessionStateRow)), "FirstSeenAtUtc" },
        { "ViewerServerTab.xaml", "PgSessionStatesGrid", "LastSeenAt", Name(typeof(PgDisplay.SessionStateRow)), "LastSeenAtUtc" },
        { "ViewerServerTab.xaml", "PgAutovacuumGrid", "LastAutovacuum", Name(typeof(PgDisplay.AutovacuumRow)), "LastAutovacuumUtc" },
        { "ViewerServerTab.xaml", "PgAutovacuumGrid", "LastVacuum", Name(typeof(PgDisplay.AutovacuumRow)), "LastVacuumUtc" },
        { "ViewerServerTab.xaml", "PgAutovacuumGrid", "LastAutoanalyze", Name(typeof(PgDisplay.AutovacuumRow)), "LastAutoanalyzeUtc" },
        { "ViewerServerTab.xaml", "PgAutovacuumGrid", "LastAnalyze", Name(typeof(PgDisplay.AutovacuumRow)), "LastAnalyzeUtc" },
        { "ViewerServerTab.xaml", "PgAutovacuumGrid", "MeasuredAt", Name(typeof(PgDisplay.AutovacuumRow)), "MeasuredAtUtc" },
        { "ViewerServerTab.xaml", "PgXminHorizonGrid", "MeasuredAt", Name(typeof(PgDisplay.XminRow)), "MeasuredAtUtc" },
        { "ViewerServerTab.xaml", "PgWraparoundGrid", "MeasuredAt", Name(typeof(PgDisplay.WraparoundRow)), "MeasuredAtUtc" },
        { "ViewerServerTab.xaml", "PgReplicationSlotsGrid", "InactiveSince", Name(typeof(PgDisplay.SlotRow)), "InactiveSinceUtc" },
        { "ViewerServerTab.xaml", "PgReplicationSlotsGrid", "MeasuredAt", Name(typeof(PgDisplay.SlotRow)), "MeasuredAtUtc" },
        { "ViewerServerTab.xaml", "PgIndexUsageGrid", "LastScan", Name(typeof(PgDisplay.IndexUsageRow)), "LastScanUtc" },
        { "ViewerServerTab.xaml", "PgDatabaseStatsGrid", "StatsReset", Name(typeof(PgDisplay.DatabaseRow)), "StatsResetUtc" },
        { "ViewerServerTab.xaml", "PgIoStatsGrid", "StatsReset", Name(typeof(PgDisplay.IoRow)), "StatsResetUtc" },

        /* The five grids that used to bind the shared reader's row, and so showed the raw UTC DateTime in every display mode
           (#4766): each now binds a display row that words the time from the mode and keeps the instant in its Utc member. */
        { "ViewerServerTab.xaml", "PgLockStatsGrid", "LastSeen", Name(typeof(PgDisplay.LockStatRow)), "LastSeenUtc" },
        { "ViewerServerTab.xaml", "PgWaitSamplingGrid", "CaptureTime", Name(typeof(PgDisplay.WaitSamplingRow)), "CaptureTimeUtc" },
        { "ViewerServerTab.xaml", "PgColumnStatsGrid", "CaptureTime", Name(typeof(PgDisplay.ColumnStatRow)), "CaptureTimeUtc" },
        { "ViewerServerTab.xaml", "PgReplicationStatsGrid", "BackendStart", Name(typeof(PgDisplay.ReplicationStatRow)), "BackendStartUtc" },
        { "ViewerServerTab.xaml", "PgIndexBloatGrid", "MeasuredAt", Name(typeof(PgDisplay.IndexBloatRow)), "MeasuredAtUtc" },
        { "ViewerServerTab.xaml", "PgIndexBloatGrid", "EstimatedAt", Name(typeof(PgDisplay.IndexBloatRow)), "EstimatedAtUtc" },
    };

    /// <summary>
    /// Columns whose header or bound property reads as a time to the scan but are not one, each with the reason.
    /// A key that matches no column in the XAML fails the scan, so an entry cannot outlive its column.
    /// </summary>
    private static readonly Dictionary<(string File, string Grid, string Property), string> NotTimes = new()
    {
        [("CollectionLogWindow.xaml", "LogDataGrid", "RowsCollected")] = "a row count",
        [("ViewerServerTab.xaml", "QuerySnapshotsGrid", "ElapsedTimeFormatted")] = "a duration, sorted by its millisecond count",
        [("ViewerServerTab.xaml", "CurrentActiveQueriesGrid", "ElapsedTimeFormatted")] = "a duration, sorted by its millisecond count",
        [("WaitDrillDownWindow.xaml", "ResultsDataGrid", "ElapsedTimeFormatted")] = "a duration, sorted by its millisecond count",
        [("ViewerServerTab.xaml", "BlockedProcessReportGrid", "WaitTimeFormatted")] = "a wait duration, sorted by its millisecond count",
        [("ViewerServerTab.xaml", "PgCollectorHealthGrid", "RowsCollected")] = "a row count",
        [("ViewerServerTab.xaml", "PgBlockingChainsGrid", "SamplesAsRoot")] = "a sample count",
        [("ViewerServerTab.xaml", "PgDeadlocksGrid", "TimesSeen")] = "a count of sightings",
        [("ViewerServerTab.xaml", "PgLogEventsGrid", "TimesSeen")] = "a count of sightings",
        [("ViewerServerTab.xaml", "PgLockStatsGrid", "Captures")] = "a count of captures",
        [("ViewerServerTab.xaml", "PgReplicationStatsGrid", "Samples")] = "a count of samples",
        [("ViewerServerTab.xaml", "PgKernelStatsGrid", "CounterReset")] = "a yes/no flag that the counters were reset, not when",
        [("ViewerServerTab.xaml", "PgWaitSamplingGrid", "CounterReset")] = "a yes/no flag that the counters were reset, not when",
        [("ViewerServerTab.xaml", "PgAutovacuumGrid", "InsertsSinceVacuum")] = "a row count",
        [("ViewerServerTab.xaml", "PgAutovacuumGrid", "ModsSinceAnalyze")] = "a row count",
        [("ViewerServerTab.xaml", "PgPlanCaptureGrid", "Observed")] = "the setting value the readiness check observed, not a time",
        [("ViewerServerTab.xaml", "PgIndexBloatGrid", "EstimatedReclaimableBytes")] = "a byte count",
        [("ViewerServerTab.xaml", "PgIndexBloatGrid", "SkippedReason")] = "the reason a measurement was skipped, text",
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

    /// <summary>
    /// Every DataGrid column in the viewer's XAML that shows a time as text sorts by a DateTime, not by that text
    /// (#4766). A column counts as a time when its header or its bound property reads as one (<see cref="TimeWords"/>),
    /// unless the header carries a unit (a duration or a size) or it binds a DateTime through a StringFormat, which
    /// sorts as the value it formats. What is left must either sit in <see cref="Columns"/> with a SortMemberPath that
    /// the reflection check above resolves to a DateTime of its row, or be named in <see cref="NotTimes"/> with the reason
    /// it is not a time. A column that binds a DateTime with no text in between is not let through: it prints the raw UTC
    /// value in every display mode, which is the defect the display rows exist to remove, so it fails here too.
    /// A column added later that does none of these fails here with its file, its header and the fix.
    /// </summary>
    [Fact]
    public void EveryTimeColumnInTheViewer_SortsByADateTime_NotByItsText()
    {
        var pinned = Columns
            .Select(row => ((Xunit.ITheoryDataRow)row).GetData())
            .Select(d => (File: (string)d![0]!, Grid: (string)d[1]!, Property: (string)d[2]!))
            .ToHashSet();

        var failures = new List<string>();
        var seen = new HashSet<(string File, string Grid, string Property)>();
        var viewerRoot = PathTo("Darling", ViewerFolder);
        foreach (var path in Directory.EnumerateFiles(viewerRoot, "*.xaml", SearchOption.AllDirectories).OrderBy(p => p, StringComparer.Ordinal))
        {
            if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            var file = Path.GetRelativePath(viewerRoot, path).Replace('\\', '/');
            foreach (var column in ReadColumns(file))
            {
                var key = (column.File, column.Grid, column.BoundProperty);
                seen.Add(key);
                if (!ReadsAsATime(column) || column.BoundPropertyHasStringFormat || NotTimes.ContainsKey(key))
                {
                    continue;
                }

                var label = $"{column.File}: the {column.Grid} column '{column.Header}' (bound to {column.BoundProperty})";
                var declared = column.SortMemberPath;
                if (declared.Length == 0)
                {
                    failures.Add($"{label} shows a time as text and has no SortMemberPath, so it sorts by that text. "
                        + "Fix: set SortMemberPath to the row's UTC DateTime member (else the row's own DateTime for that value, "
                        + "else add a get-only DateTime property the text is formatted from) and add the column to Columns.");
                }
                else if (declared == column.BoundProperty || Regex.IsMatch(declared, "(?:Local|Display|Text|Formatted)$"))
                {
                    failures.Add($"{label} sorts by {declared}, which is text. Fix: sort by the row's DateTime member instead.");
                }
                else if (!pinned.Contains(key))
                {
                    failures.Add($"{label} sorts by {declared}, which nothing checks. Fix: add the column to Columns so the row type resolves {declared} to a DateTime.");
                }
            }
        }

        foreach (var entry in NotTimes.Keys.Where(k => !seen.Contains(k)))
        {
            failures.Add($"{entry.File}: the {entry.Grid} column bound to {entry.Property} is listed as a non-time but is not in the XAML. Fix: remove the entry.");
        }

        Assert.True(failures.Count == 0, Environment.NewLine + string.Join(Environment.NewLine, failures));
    }

    /// <summary>The words that make a header or a bound property name read as a time. Tuned against every column in the viewer.</summary>
    private static readonly Regex TimeWords = new(
        @"\b(?:time|date|when|seen|expires?|expiry|created|creation|modified|collected|captured|observed|started|start|ended|since|measured|estimated|reset|day|cached"
        + @"|as of|(?:last|first) (?:run|success|scan|vacuum|autovacuum|analyze|autoanalyze|execution|refresh|access|cleanup end)|(?:executed|reverted|updated|last error) at)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>A header that carries a unit is a measure (a duration, a size, a rate), not a time of day.</summary>
    private static readonly Regex UnitInHeader = new(@"\((?:ms|s|sec|secs|us|min|mins|kb|mb|gb|kb/s|%)\)|\bms\b|%", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static bool ReadsAsATime(XamlColumn column)
    {
        if (UnitInHeader.IsMatch(column.Header))
        {
            return false;
        }

        var propertyWords = Regex.Replace(column.BoundProperty, "(?<=[a-z0-9])(?=[A-Z])", " ");
        return TimeWords.IsMatch(column.Header) || TimeWords.IsMatch(propertyWords);
    }

    /// <summary>One DataGrid column of the viewer's XAML, as the scan sees it.</summary>
    private sealed record XamlColumn(string File, string Grid, string Header, string BoundProperty, bool BoundPropertyHasStringFormat, string SortMemberPath);

    private static readonly Regex GridStart = new(@"<DataGrid(?![\w.])(?:""[^""]*""|[^>""])*>", RegexOptions.CultureInvariant);

    private static readonly Regex ColumnStart = new(@"<(?<tag>DataGrid\w*Column)(?![\w.])(?:""[^""]*""|[^>""])*>", RegexOptions.CultureInvariant);

    private static string Attribute(string startTag, string name) =>
        Regex.Match(startTag, @"\s" + name + @"=""(?<v>[^""]*)""").Groups["v"].Value;

    /// <summary>Every DataGrid column in the file: its grid, header text, bound property and SortMemberPath.</summary>
    private static List<XamlColumn> ReadColumns(string xamlFile)
    {
        var xaml = ReadRepoFile("Darling", ViewerFolder, xamlFile);
        var grids = GridStart.Matches(xaml)
            .Select(m => (m.Index, Name: Attribute(m.Value, "x:Name") is { Length: > 0 } name ? name : "(unnamed)"))
            .ToList();

        var columns = new List<XamlColumn>();
        foreach (Match start in ColumnStart.Matches(xaml))
        {
            var body = "";
            if (!start.Value.EndsWith("/>", StringComparison.Ordinal))
            {
                var end = xaml.IndexOf("</" + start.Groups["tag"].Value + ">", start.Index, StringComparison.Ordinal);
                body = end < 0 ? "" : xaml[(start.Index + start.Length)..end];
            }

            var header = Attribute(start.Value, "Header");
            if (header.Length == 0)
            {
                header = Regex.Match(body, @"<TextBlock\b[^>]*?\sText=""(?<t>[^""]*)""").Groups["t"].Value;
            }

            var binding = Attribute(start.Value, "Binding");
            if (binding.Length == 0)
            {
                binding = Regex.Match(body, @"\{Binding\s+[A-Za-z_][^}]*\}").Value;
            }

            columns.Add(new XamlColumn(
                xamlFile,
                grids.LastOrDefault(g => g.Index < start.Index).Name ?? "(none)",
                WebUtility.HtmlDecode(header),
                Regex.Match(binding, @"\{Binding\s+(?:Path=)?(?<p>[A-Za-z_][\w.]*)").Groups["p"].Value,
                binding.Contains("StringFormat", StringComparison.Ordinal),
                Attribute(start.Value, "SortMemberPath")));
        }

        return columns;
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
