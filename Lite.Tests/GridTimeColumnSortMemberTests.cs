/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a grid time column that shows the UTC offset in the repeated autumn hour must sort by the row's true UTC
/// DateTime, not by its text and not by a DateTime already converted to the display zone.
///
/// <para>The text sorts wrong east of UTC: "02:30 +01:00" sorts before "02:30 +02:00", so the second pass of the hour
/// comes out before the first. A display-zone DateTime is no better: the second pass at 01:15 (06:15Z) sorts before the
/// first pass at 01:45 (05:45Z). Only the naive-UTC instant puts both passes in order, so each column carries the row's
/// UTC member in <c>SortMemberPath</c>. The filter button's <c>Tag</c> is a separate contract (the filter manager reads
/// it) and is left alone.</para>
///
/// <para>A row read from a stored server wall clock (job history, running jobs, the query and procedure stats creation
/// and execution times, the open transaction's start) has no UTC member of its own. Its column sorts by the row's own
/// <see cref="DateTime"/> for that value, the stored wall clock, and never by the text: a "g" or "ddd MM/dd" text puts
/// "10/1/2026" before "9/30/2026" and "Fri" before "Mon". The wall clock can misorder inside the repeated autumn hour,
/// which its own text cannot say either.</para>
///
/// <para>Every column in any Lite .xaml that shows a time as text is held to that, whether it is in the table or was
/// added later: <see cref="EveryTimeColumn_SortsByADateTime_NotByItsText"/> reads the .xaml files themselves.</para>
/// </summary>
/* The behaviour check installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics;
   the class joins the collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class GridTimeColumnSortMemberTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
    }

    /// <summary>The row type is carried as its assembly-qualified name, so each case stays a serializable, separately reported row.</summary>
    private static string Name(Type type) => type.AssemblyQualifiedName!;

    /// <summary>(xaml under Lite/, the DataGrid's x:Name, the bound text property, the row type, the SortMemberPath).</summary>
    public static TheoryData<string, string, string, string, string> Columns => new()
    {
        { "Controls/FinOpsTab.xaml", "ApplicationConnectionsDataGrid", "FirstSeenText", Name(typeof(ApplicationConnectionRow)), "FirstSeen" },
        { "Controls/FinOpsTab.xaml", "ApplicationConnectionsDataGrid", "LastSeenText", Name(typeof(ApplicationConnectionRow)), "LastSeen" },
        { "Controls/AlertsHistoryTab.xaml", "AlertsDataGrid", "TimeLocal", Name(typeof(AlertHistoryRow)), "AlertTime" },
        { "Controls/ServerTab.xaml", "QuerySnapshotsGrid", "CollectionTimeLocal", Name(typeof(QuerySnapshotRow)), "CollectionTime" },
        { "Controls/ServerTab.xaml", "QueryStoreGrid", "LastExecutionTimeLocal", Name(typeof(QueryStoreRow)), "LastExecutionTime" },
        { "Controls/ServerTab.xaml", "QueryStoreGrid", "FirstExecutionTimeLocal", Name(typeof(QueryStoreRow)), "FirstExecutionTime" },
        { "Controls/ServerTab.xaml", "PlanCorrectionGrid", "CollectionTimeLocal", Name(typeof(PlanCorrectionRow)), "CollectionTime" },
        { "Controls/ServerTab.xaml", "PlanCorrectionGrid", "ValidSinceLocal", Name(typeof(PlanCorrectionRow)), "ValidSince" },
        { "Controls/ServerTab.xaml", "PlanCorrectionGrid", "LastRefreshLocal", Name(typeof(PlanCorrectionRow)), "LastRefresh" },
        { "Controls/ServerTab.xaml", "PlanCorrectionGrid", "ExecuteActionInitiatedTimeLocal", Name(typeof(PlanCorrectionRow)), "ExecuteActionInitiatedTime" },
        { "Controls/ServerTab.xaml", "PlanCorrectionGrid", "RevertActionInitiatedTimeLocal", Name(typeof(PlanCorrectionRow)), "RevertActionInitiatedTime" },
        { "Controls/ServerTab.xaml", "BlockedProcessReportGrid", "EventTimeLocal", Name(typeof(BlockedProcessReportRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "DeadlockGrid", "DeadlockTimeLocal", Name(typeof(DeadlockProcessDetail)), "DeadlockTime" },
        { "Controls/ServerTab.xaml", "AutomaticTuningGrid", "CollectionTimeLocal", Name(typeof(AutomaticTuningRow)), "CollectionTime" },
        /* #4966: the Collected column of every grid that draws the newest snapshot: the snapshot's collection (or capture) time, sorted by the stored instant. */
        { "Controls/ServerTab.xaml", "CpuSchedulerGrid", "CollectionTimeLocal", Name(typeof(CpuSchedulerGridRow)), "CollectionTime" },
        { "Controls/ServerTab.xaml", "LatchStatsGrid", "CollectionTimeLocal", Name(typeof(LatchStatsSnapshotRow)), "CollectionTime" },
        { "Controls/ServerTab.xaml", "SpinlockStatsGrid", "CollectionTimeLocal", Name(typeof(SpinlockStatsSnapshotRow)), "CollectionTime" },
        { "Controls/ServerTab.xaml", "ServerConfigGrid", "CaptureTimeLocal", Name(typeof(ServerConfigRow)), "CaptureTime" },
        { "Controls/ServerTab.xaml", "DatabaseConfigGrid", "CaptureTimeLocal", Name(typeof(DatabaseConfigRow)), "CaptureTime" },
        { "Controls/ServerTab.xaml", "DatabaseScopedConfigGrid", "CaptureTimeLocal", Name(typeof(DatabaseScopedConfigRow)), "CaptureTime" },
        { "Controls/ServerTab.xaml", "QueryStoreHealthGrid", "CaptureTimeLocal", Name(typeof(QueryStoreHealthRow)), "CaptureTime" },
        { "Controls/ServerTab.xaml", "TraceFlagsGrid", "CaptureTimeLocal", Name(typeof(TraceFlagRow)), "CaptureTime" },
        { "Controls/ServerTab.xaml", "RunningJobsGrid", "CollectionTimeLocal", Name(typeof(RunningJobRow)), "CollectionTime" },
        { "Controls/FinOpsTab.xaml", "ObjectIndexDetailGrid", "CollectionTimeLocal", Name(typeof(IndexUsageRow)), "CollectionTime" },
        { "Controls/ServerTab.xaml", "SchedulerIssuesGrid", "EventTimeLocal", Name(typeof(SchedulerIssueRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "SevereErrorsGrid", "EventTimeLocal", Name(typeof(SevereErrorRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "MemoryConditionsGrid", "EventTimeLocal", Name(typeof(MemoryConditionsRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "MemoryBrokerGrid", "EventTimeLocal", Name(typeof(MemoryBrokerRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "MemoryNodeOomGrid", "EventTimeLocal", Name(typeof(MemoryNodeOomRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "SignificantWaitsGrid", "EventTimeLocal", Name(typeof(SignificantWaitRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "CpuTasksGrid", "EventTimeLocal", Name(typeof(CpuTasksRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "IoIssuesGrid", "EventTimeLocal", Name(typeof(IoIssuesRow)), "EventTime" },
        { "Controls/ServerTab.xaml", "ServerConfigChangesGrid", "ChangeTimeLocal", Name(typeof(ServerConfigChangeRow)), "ChangeTime" },
        { "Controls/ServerTab.xaml", "DatabaseConfigChangesGrid", "ChangeTimeLocal", Name(typeof(DatabaseConfigChangeRow)), "ChangeTime" },
        { "Controls/ServerTab.xaml", "TraceFlagChangesGrid", "ChangeTimeLocal", Name(typeof(TraceFlagChangeRow)), "ChangeTime" },
        { "Controls/ServerTab.xaml", "CollectionHealthGrid", "LastSuccessFormatted", Name(typeof(CollectorHealthRow)), "LastSuccessTime" },
        { "Controls/ServerTab.xaml", "CollectionHealthGrid", "LastRunFormatted", Name(typeof(CollectorHealthRow)), "LastRunTime" },
        { "Controls/ServerTab.xaml", "CollectionHealthGrid", "LastErrorFormatted", Name(typeof(CollectorHealthRow)), "LastErrorTime" },
        { "Controls/ServerTab.xaml", "CollectionLogGrid", "CollectionTimeFormatted", Name(typeof(CollectionLogRow)), "CollectionTime" },
        { "Windows/CollectionLogWindow.xaml", "LogDataGrid", "CollectionTimeFormatted", Name(typeof(CollectionLogRow)), "CollectionTime" },
        { "Windows/ProcedureHistoryWindow.xaml", "HistoryDataGrid", "CollectionTimeLocal", Name(typeof(ProcedureStatsHistoryRow)), "CollectionTime" },
        { "Windows/QueryStatsHistoryWindow.xaml", "HistoryDataGrid", "CollectionTimeLocal", Name(typeof(QueryStatsHistoryRow)), "CollectionTime" },
        { "Windows/QueryStoreHistoryWindow.xaml", "HistoryDataGrid", "CollectionTimeLocal", Name(typeof(QueryStoreHistoryRow)), "CollectionTime" },
        { "Windows/QueryStoreHistoryWindow.xaml", "HistoryDataGrid", "FirstExecutionTimeLocal", Name(typeof(QueryStoreHistoryRow)), "FirstExecutionTime" },
        { "Windows/QueryStoreHistoryWindow.xaml", "HistoryDataGrid", "LastExecutionTimeLocal", Name(typeof(QueryStoreHistoryRow)), "LastExecutionTime" },
        { "Windows/WaitDrillDownWindow.xaml", "ResultsDataGrid", "CollectionTimeLocal", Name(typeof(QuerySnapshotRow)), "CollectionTime" },

        /* The row's UTC member: the Default Trace's de-skewed event time, the long-query event time, the Mute Rules expiry, the UTC day of the daily FinOps rows. */
        { "Controls/ServerTab.xaml", "DefaultTraceGrid", "EventTimeLocal", Name(typeof(DefaultTraceEventRow)), "EventTimeUtc" },
        { "Controls/ServerTab.xaml", "LongQueryCompletionsGrid", "EventTimeLocal", Name(typeof(LongQueryCompletionRow)), "EventTime" },
        { "Windows/ManageMuteRulesWindow.xaml", "RulesGrid", "ExpiresDisplay", Name(typeof(MuteRule)), "ExpiresAtUtc" },
        { "Controls/FinOpsTab.xaml", "MemoryGrantEfficiencyDataGrid", "DayDisplay", Name(typeof(MemoryGrantEfficiencyRow)), "Day" },
        { "Controls/FinOpsTab.xaml", "ProvisioningTrendGrid", "DayDisplay", Name(typeof(ProvisioningTrendRow)), "Day" },

        /* The row's own DateTime, a stored server wall clock (no UTC member exists for these): msdb job history, the running
           job's start, the plan cache's creation, cached and last-execution times, the open transaction's begin time. */
        { "Controls/JobHistoryTab.xaml", "JobHistoryDataGrid", "RunTimeLocal", Name(typeof(JobHistoryRow)), "RunDateTime" },
        { "Controls/JobHistoryTab.xaml", "JobHistoryDataGrid", "LastSuccessfulRunLocal", Name(typeof(JobHistoryRow)), "LastSuccessfulRun" },
        { "Controls/ServerTab.xaml", "RunningJobsGrid", "StartTimeLocal", Name(typeof(RunningJobRow)), "StartTime" },
        { "Controls/ServerTab.xaml", "QueryStatsGrid", "LastExecutionTimeLocal", Name(typeof(QueryStatsRow)), "LastExecutionTime" },
        { "Controls/ServerTab.xaml", "QueryStatsGrid", "CreationTimeLocal", Name(typeof(QueryStatsRow)), "CreationTime" },
        { "Controls/ServerTab.xaml", "ProcedureStatsGrid", "LastExecutionTimeLocal", Name(typeof(ProcedureStatsRow)), "LastExecutionTime" },
        { "Controls/ServerTab.xaml", "ProcedureStatsGrid", "CachedTimeFormatted", Name(typeof(ProcedureStatsRow)), "CachedTime" },
        { "Windows/QueryStatsHistoryWindow.xaml", "HistoryDataGrid", "LastExecutionTimeLocal", Name(typeof(QueryStatsHistoryRow)), "LastExecutionTime" },
        { "Windows/QueryStatsHistoryWindow.xaml", "HistoryDataGrid", "CreationTimeLocal", Name(typeof(QueryStatsHistoryRow)), "CreationTime" },
        { "Windows/ProcedureHistoryWindow.xaml", "HistoryDataGrid", "LastExecutionTimeLocal", Name(typeof(ProcedureStatsHistoryRow)), "LastExecutionTime" },
        { "Windows/ProcedureHistoryWindow.xaml", "HistoryDataGrid", "CachedTimeLocal", Name(typeof(ProcedureStatsHistoryRow)), "CachedTime" },
        { "Windows/WaitDrillDownWindow.xaml", "ResultsDataGrid", "TranStartTimeLocal", Name(typeof(QuerySnapshotRow)), "TranStartTime" },
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
        var xaml = ReadLite(xamlFile);

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
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);

        var firstPass = new ApplicationConnectionRow { ApplicationName = "first", FirstSeen = Utc(2026, 11, 1, 5, 45), LastSeen = Utc(2026, 11, 1, 5, 45) };
        var secondPass = new ApplicationConnectionRow { ApplicationName = "second", FirstSeen = Utc(2026, 11, 1, 6, 15), LastSeen = Utc(2026, 11, 1, 6, 15) };
        var rows = new[] { secondPass, firstPass };

        /* The member the column is declared to sort by, read from the XAML rather than restated here. */
        var declared = Regex.Match(FindColumn(ReadLite("Controls/FinOpsTab.xaml"), "ApplicationConnectionsDataGrid", "FirstSeenText"),
            @"\sSortMemberPath=""(?<path>[^""]*)""").Groups["path"].Value;

        Assert.Equal(new[] { "first", "second" }, Sorted(rows, declared));

        /* Why the display-zone value and the text are refused as sort keys. */
        Assert.Equal(new DateTime(2026, 11, 1, 1, 45, 0), firstPass.FirstSeenLocal);
        Assert.Equal(new DateTime(2026, 11, 1, 1, 15, 0), secondPass.FirstSeenLocal);
        Assert.Equal(new[] { "second", "first" }, Sorted(rows, nameof(ApplicationConnectionRow.FirstSeenLocal)));
        Assert.Equal(new[] { "second", "first" }, Sorted(rows, nameof(ApplicationConnectionRow.FirstSeenText)));
    }

    /// <summary>
    /// The texts that started this: a "g" date puts "10/1/2026" before "9/30/2026", and "ddd MM/dd" puts "Fri 10/02" before
    /// "Mon 09/28". Sorted by the member the column declares, both come out in time order; sorted by the text they do not.
    /// The text is worded in the current culture, so the test pins en-US for its length.
    /// </summary>
    [Fact]
    public void ATextThatSortsOutOfTimeOrder_ComesOutInTimeOrder_OnTheDeclaredMember()
    {
        var savedCulture = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("en-US");
        try
        {
            /* Sep 1 and Oct 9 at 12:00Z: in any zone the "g" text reads "9/..." and "10/...", and as text "10/" sorts first. */
            var september = new MuteRule { Reason = "september", ExpiresAtUtc = new DateTime(2099, 9, 1, 12, 0, 0, DateTimeKind.Utc) };
            var october = new MuteRule { Reason = "october", ExpiresAtUtc = new DateTime(2099, 10, 9, 12, 0, 0, DateTimeKind.Utc) };
            var rules = new[] { october, september };
            var expires = DeclaredSortMember("Windows/ManageMuteRulesWindow.xaml", "RulesGrid", "ExpiresDisplay");
            Assert.Equal(new[] { "september", "october" }, SortedBy(rules, expires, r => r.Reason!));
            Assert.Equal(new[] { "october", "september" }, SortedBy(rules, nameof(MuteRule.ExpiresDisplay), r => r.Reason!));

            /* 2026-09-28 is a Monday and 2026-10-02 a Friday: "Fri 10/02" sorts before "Mon 09/28" as text. */
            var days = new[] { new MemoryGrantEfficiencyRow { Day = new DateTime(2026, 10, 2) }, new MemoryGrantEfficiencyRow { Day = new DateTime(2026, 9, 28) } };
            var day = DeclaredSortMember("Controls/FinOpsTab.xaml", "MemoryGrantEfficiencyDataGrid", "DayDisplay");
            Assert.Equal(new[] { "Mon 09/28", "Fri 10/02" }, SortedBy(days, day, r => r.DayDisplay));
            Assert.Equal(new[] { "Fri 10/02", "Mon 09/28" }, SortedBy(days, nameof(MemoryGrantEfficiencyRow.DayDisplay), r => r.DayDisplay));
        }
        finally
        {
            CultureInfo.CurrentCulture = savedCulture;
        }
    }

    /// <summary>
    /// #4766: a column that shows a time as text sorts by the time, not by the text, whichever column it is and whenever it
    /// was added. This reads every column of every DataGrid in Lite's .xaml, and where its header or bound property reads as
    /// a time (see <see cref="ReadsAsATime"/>) and the bound property is a string, its <c>SortMemberPath</c> must name a
    /// public <see cref="DateTime"/> or <c>DateTime?</c> on the row type. A column that binds a DateTime directly is fine as
    /// it is, and a column that matches the words but shows no time (a duration) is listed in
    /// <see cref="NotTimeColumns"/> with its reason.
    /// </summary>
    [Fact]
    public void EveryTimeColumn_SortsByADateTime_NotByItsText()
    {
        var problems = new List<string>();
        var exemptionsInUse = new HashSet<(string, string, string)>();

        foreach (var column in ReadGridColumns())
        {
            if (!ShowsATimeAsText(column, out var rowTypes))
            {
                continue;
            }

            var key = (column.File, column.Grid, column.BoundProperty ?? column.Header);
            if (NotTimeColumns.ContainsKey(key))
            {
                exemptionsInUse.Add(key);
                continue;
            }

            var where = $"{column.File}: the {column.Grid} column \"{column.Header}\" (bound to {column.BoundProperty ?? "a template"})";
            if (string.IsNullOrWhiteSpace(column.SortMemberPath))
            {
                problems.Add($"{where} has no SortMemberPath, so it sorts by its text. {HowToFix}");
                continue;
            }

            /* The row types that declare the bound property are the ones this column can come from. A sort member is refused when any
               of them declares it as a string (it sorts by text), and when none of them declares it as a DateTime (a wrong name
               sorts nothing and raises nothing). With no such row type (a template), it must be a DateTime on some row type. */
            var sort = column.SortMemberPath;
            var members = rowTypes.Count == 0
                ? RowProperties.Value.Where(p => p.Property.Name == sort).Select(p => p.Property).ToList()
                : rowTypes.Select(t => t.GetProperty(sort, BindingFlags.Public | BindingFlags.Instance)).OfType<PropertyInfo>().ToList();
            var textMember = members.FirstOrDefault(m => m.PropertyType == typeof(string));
            if (textMember is not null)
            {
                problems.Add($"{where} sorts by {textMember.DeclaringType!.Name}.{sort}, a string, so it sorts by text. {HowToFix}");
            }
            else if (!members.Any(m => IsDateTime(m.PropertyType)))
            {
                problems.Add($"{where} sorts by {sort}, which is not a DateTime on {(rowTypes.Count == 0 ? "any row type" : "any row type that has " + column.BoundProperty)}, so the sort does nothing useful. {HowToFix}");
            }
        }

        foreach (var stale in NotTimeColumns.Keys.Where(k => !exemptionsInUse.Contains(k)))
        {
            problems.Add($"NotTimeColumns lists {stale.File} / {stale.Grid} / {stale.Property}, but no column there reads as a time shown as text any more. Remove the entry.");
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems.Distinct()));
    }

    /// <summary>
    /// The scan above is only as good as the words it reads times by, so every column the table pins must be one it reads
    /// as a time. A pinned column the scan skipped would mean the scan is checking less than the table.
    /// </summary>
    [Fact]
    public void TheScan_ReadsEveryPinnedColumnAsATime()
    {
        var read = ReadGridColumns()
            .Where(c => ShowsATimeAsText(c, out _))
            .Select(c => (c.File, c.Grid, Property: c.BoundProperty))
            .ToHashSet();

        var missed = Columns
            .Select(row => ((ITheoryDataRow)row).GetData())
            .Select(data => (File: (string)data[0]!, Grid: (string)data[1]!, Property: (string?)data[2]))
            .Where(pinned => !read.Contains(pinned))
            .ToList();

        Assert.True(missed.Count == 0,
            "The scan does not read these pinned columns as times, so it would not catch a regression in them: "
            + string.Join("; ", missed.Select(m => $"{m.File} / {m.Grid} / {m.Property}"))
            + ". Add the word that says they are times to ReadsAsATime.");
    }

    /// <summary>
    /// Columns whose header or bound property matches <see cref="ReadsAsATime"/> and whose property is a string, but which
    /// show no time. Keyed by the .xaml under Lite/, the grid and the bound property, each with the reason it is not a time.
    /// </summary>
    private static readonly Dictionary<(string File, string Grid, string Property), string> NotTimeColumns = new()
    {
        { ("Controls/ServerTab.xaml", "QuerySnapshotsGrid", "ElapsedTimeFormatted"), "a duration (how long the request has run), not a time" },
        { ("Windows/WaitDrillDownWindow.xaml", "ResultsDataGrid", "ElapsedTimeFormatted"), "a duration (how long the request has run), not a time" },
        { ("Controls/ServerTab.xaml", "BlockedProcessReportGrid", "WaitTimeFormatted"), "a wait duration in ms, not a time; it sorts by WaitTimeMs" },
        { ("Controls/ServerTab.xaml", "DeadlockGrid", "WaitTimeFormatted"), "a wait duration in ms, not a time; it sorts by WaitTime" },
        { ("Windows/CollectorScheduleEditorWindow.xaml", "ScheduleGrid", "RunAt"), "a 24-hour HH:MM time of day (#4938), not an instant: it is zero-padded, so its text order is its time-of-day order, and it has no date to sort by" },
    };

    private const string HowToFix =
        "Set its SortMemberPath to the row's UTC DateTime member; when the row has none, to the row's own DateTime for that value; "
        + "when it has no DateTime for it, add a get-only DateTime property holding the value the text is formatted from and sort by that. "
        + "Never point it at the text. If the column shows no time (a duration, a count), add it to NotTimeColumns with the reason.";

    /// <summary>
    /// The whole words (a header's words, and a property name's words split at each capital) that make a column read as a
    /// time: the time nouns, the verbs a stamp is named for ("Created", "Cached"), and "Local", which Lite puts on
    /// every display-zone string it makes from a DateTime.
    /// </summary>
    private static readonly Regex ReadsAsATime = new(
        @"\b(time|datetime|date|day|when|seen|expires|expiry|created|creation|modified|collected|captured|observed|started|start|ended|"
        + @"cached|refreshed|refresh|since|timestamp|stamp|at|local|updated|connected|checked|logged|occurred|"
        + @"last (run|success|successful|seen|access|refresh|update|connect))\b",
        RegexOptions.Compiled);

    /// <summary>Every public instance property of every plain class in Lite and in the notification library its Mute Rules grid binds.</summary>
    private static readonly Lazy<List<(Type Row, PropertyInfo Property)>> RowProperties = new(() =>
        new[] { typeof(ApplicationConnectionRow).Assembly, typeof(MuteRule).Assembly }
            .SelectMany(LoadableTypes)
            .Where(t => t.IsClass && !t.Name.StartsWith('<') && !typeof(DependencyObject).IsAssignableFrom(t))
            .SelectMany(t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance), (t, p) => (Row: t, Property: p))
            .ToList());

    private static IEnumerable<Type> LoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException e)
        {
            return e.Types.OfType<Type>();
        }
    }

    private static bool IsDateTime(Type type) => (Nullable.GetUnderlyingType(type) ?? type) == typeof(DateTime);

    /// <summary>
    /// Whether the column shows a time as text: its header or bound property reads as a time, and the property is a string on
    /// some row type (<paramref name="rowTypes"/>), or is one the scan cannot find (a template, or a row outside these
    /// assemblies), which it treats as text. A property that is a DateTime or a number on every row type is shown directly.
    /// </summary>
    private static bool ShowsATimeAsText(GridColumn column, out List<Type> rowTypes)
    {
        rowTypes = [];
        var words = Words(column.Header) + " | " + (column.BoundProperty is null ? "" : Words(column.BoundProperty));
        if (!ReadsAsATime.IsMatch(words))
        {
            return false;
        }

        if (column.BoundProperty is null)
        {
            return true;
        }

        var declared = RowProperties.Value.Where(p => p.Property.Name == column.BoundProperty).ToList();
        if (declared.Count == 0)
        {
            return true;
        }

        rowTypes = declared.Where(p => p.Property.PropertyType == typeof(string)).Select(p => p.Row).Distinct().ToList();
        return rowTypes.Count > 0;
    }

    /// <summary>Lower-case words of a header or a property name, split at each capital: "LastSuccessfulRunLocal" reads "last successful run local".</summary>
    private static string Words(string text) =>
        string.Join(' ', Regex.Matches(text, @"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|[0-9]+").Select(m => m.Value.ToLowerInvariant()));

    /// <summary>One column of one DataGrid: its file (under Lite/), grid name, header text, bound property and SortMemberPath.</summary>
    private sealed record GridColumn(string File, string Grid, string Header, string? BoundProperty, string? SortMemberPath);

    /// <summary>
    /// Every column of every DataGrid in Lite's .xaml, in file order. The header is the column's <c>Header</c> attribute, or the
    /// first TextBlock of its <c>Header</c> element (the filter button and title Lite puts in each header). The bound property is
    /// the first name in a <c>Binding</c>.
    /// </summary>
    private static List<GridColumn> ReadGridColumns([CallerFilePath] string thisFile = "")
    {
        var liteRoot = LiteRoot(thisFile);
        var columns = new List<GridColumn>();

        foreach (var path in Directory.EnumerateFiles(liteRoot, "*.xaml", SearchOption.AllDirectories).OrderBy(p => p, StringComparer.Ordinal))
        {
            var file = Path.GetRelativePath(liteRoot, path).Replace('\\', '/');
            if (file.StartsWith("bin/", StringComparison.Ordinal) || file.StartsWith("obj/", StringComparison.Ordinal))
            {
                continue;
            }

            var xaml = File.ReadAllText(path);
            var grids = Regex.Matches(xaml, @"<DataGrid(?=[\s>])(?<attrs>(?:""[^""]*""|[^>""])*)>");
            var starts = Regex.Matches(xaml, @"<DataGrid(?:Text|Template|CheckBox|ComboBox|Hyperlink)?Column(?=[\s/>])(?<attrs>(?:""[^""]*""|[^>""])*)>");

            for (var i = 0; i < starts.Count; i++)
            {
                var start = starts[i];
                var attrs = start.Groups["attrs"].Value;

                /* The column runs to the next column, or to the end of the grid's columns. */
                var end = i + 1 < starts.Count ? starts[i + 1].Index : xaml.Length;
                var closeColumns = xaml.IndexOf("</DataGrid.Columns>", start.Index, StringComparison.Ordinal);
                if (closeColumns >= 0 && closeColumns < end)
                {
                    end = closeColumns;
                }

                var region = attrs.TrimEnd().EndsWith('/') ? start.Value : xaml[start.Index..end];
                var header = AttributeOf(attrs, "Header")
                    ?? Regex.Match(region, @"<TextBlock\b(?:""[^""]*""|[^>""])*?\sText=""(?<text>[^""]*)""").Groups["text"].Value;
                var grid = grids.Cast<Match>().LastOrDefault(g => g.Index < start.Index);
                var gridName = grid is null ? "(no grid)" : AttributeOf(grid.Groups["attrs"].Value, "x:Name") ?? "(unnamed grid)";
                var bound = Regex.Match(attrs, @"\sBinding=""\{Binding\s+(?:Path=)?(?<path>[^,}\s""]+)").Groups["path"];

                columns.Add(new GridColumn(file, gridName, header, bound.Success ? bound.Value : null, AttributeOf(attrs, "SortMemberPath")));
            }
        }

        return columns;
    }

    private static string? AttributeOf(string attrs, string name)
    {
        var match = Regex.Match(attrs, @"(?<![\w:.])" + Regex.Escape(name) + @"=""(?<value>[^""]*)""");
        return match.Success ? match.Groups["value"].Value : null;
    }

    /// <summary>The member the named column is declared to sort by, read from the XAML rather than restated in the test.</summary>
    private static string DeclaredSortMember(string xamlFile, string grid, string textProperty) =>
        Regex.Match(FindColumn(ReadLite(xamlFile), grid, textProperty), @"\sSortMemberPath=""(?<path>[^""]*)""").Groups["path"].Value;

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    /// <summary>The application names in the order a header click on that sort member ascending gives.</summary>
    private static string[] Sorted(ApplicationConnectionRow[] rows, string sortMemberPath) =>
        SortedBy(rows, sortMemberPath, r => r.ApplicationName);

    /// <summary>The label of each row in the order a header click on that sort member ascending gives (a <see cref="ListCollectionView"/>).</summary>
    private static string[] SortedBy<T>(T[] rows, string sortMemberPath, Func<T, string> label)
    {
        var view = new ListCollectionView(rows);
        view.SortDescriptions.Add(new SortDescription(sortMemberPath, ListSortDirection.Ascending));
        return view.Cast<T>().Select(label).ToArray();
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

    private static string LiteRoot(string thisFile) =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite"));

    private static string ReadLite(string relative, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(LiteRoot(thisFile), relative));
}
