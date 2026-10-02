/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: a time read from a STORED server wall-clock column prints as the plain wall time in the repeated autumn
/// hour, never with a UTC offset.
///
/// <para>Text that shows one instant appends the instant's offset there (<c>ViewerTimeHelper.FormatForDisplay</c>), and
/// that is only true for a real UTC instant. A job step, a Default Trace event or a next scheduled run comes in as the
/// server's own wall clock and goes to UTC through <see cref="ServerClock.ToUtc"/>, which maps BOTH passes of a
/// repeated local time to the first ("A stored server-local time cannot say which one it was"). Formatted with the
/// offset, the second pass would print the first pass's: a step that ran at 01:30 the second time (06:30Z, -05:00) on a
/// US Eastern server printed "-04:00". US Eastern falls back on 2026-11-01 at 06:00Z.</para>
/// </summary>
/* Serialized with the classes that flip the process-wide ViewerTimeHelper statics: every row renders through them. */
[Collection("viewer-time-statics")]
public sealed class StoredServerClockRowTextTests
{
    private const string EasternWindowsId = "Eastern Standard Time";
    private const string Format = "yyyy-MM-dd HH:mm:ss";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern => ServerClock.Resolve(EasternWindowsId, -300);

    /// <summary>Server display mode on <paramref name="clock"/> and the invariant culture for its lifetime, for a test
    /// that reads rows (a row renders its time text when it is built) and then asserts on them. Restores all three.</summary>
    private sealed class ServerModeScope : IDisposable
    {
        private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
        private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;
        private readonly System.Globalization.CultureInfo _savedCulture = System.Globalization.CultureInfo.CurrentCulture;

        public ServerModeScope(ServerClock clock)
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            ViewerTimeHelper.ActiveServerClock = clock;
            System.Globalization.CultureInfo.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
        }

        public void Dispose()
        {
            ViewerTimeHelper.CurrentDisplayMode = _savedMode;
            ViewerTimeHelper.ActiveServerClock = _savedClock;
            System.Globalization.CultureInfo.CurrentCulture = _savedCulture;
        }
    }

    /// <summary>A system_health row: its event time is the XE <c>@timestamp</c>, a real UTC instant.</summary>
    private static string TrueUtcRowText(DateTime naiveUtc) =>
        new SchedulerIssueRow(new SchedulerIssueRecord { EventTime = naiveUtc }).EventTimeLocal;

    // ── Job History ──

    private static DataTable NewJobTable()
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("server_name", typeof(string));
        table.Columns.Add("instance_id", typeof(long));
        table.Columns.Add("job_id", typeof(string));
        table.Columns.Add("job_name", typeof(string));
        table.Columns.Add("job_enabled", typeof(bool));
        table.Columns.Add("category_name", typeof(string));
        table.Columns.Add("step_id", typeof(int));
        table.Columns.Add("step_name", typeof(string));
        table.Columns.Add("run_status", typeof(int));
        table.Columns.Add("run_status_desc", typeof(string));
        table.Columns.Add("run_datetime_local", typeof(DateTime));
        table.Columns.Add("run_duration_seconds", typeof(long));
        table.Columns.Add("retries_attempted", typeof(int));
        table.Columns.Add("message", typeof(string));
        table.Columns.Add("last_success_run_local", typeof(DateTime));
        table.Columns.Add("is_long_running", typeof(bool));
        return table;
    }

    private static ViewerJobHistoryRow ReadJobRow(DateTime runLocal, DateTime? lastSuccessLocal)
    {
        var table = NewJobTable();
        table.Rows.Add(1, "srv", 10L, "job", "Job", true, "cat", 0, "step", 1, "Succeeded", runLocal, 5L, 0, "ok",
            lastSuccessLocal.HasValue ? lastSuccessLocal.Value : DBNull.Value, false);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return ViewerDataService.ReadJobHistoryRow(reader, new Dictionary<int, ServerClock> { [1] = Eastern });
    }

    [Fact]
    public void AJobStepReadFromTheStoredWallClock_PrintsNoOffset_InBothPassesOfTheRepeatedHour_WhileARealInstantStillDoes()
    {
        using var scope = new ServerModeScope(Eastern);

        /* The Agent history stores 01:30 for a step that ran in either pass; ToUtc reads both as 05:30Z. */
        var firstPass = ReadJobRow(Naive(2026, 11, 1, 1, 30), Naive(2026, 11, 1, 1, 45));
        var secondPass = ReadJobRow(Naive(2026, 11, 1, 1, 30), Naive(2026, 11, 1, 1, 45));
        Assert.Equal(Naive(2026, 11, 1, 5, 30), firstPass.RunDateTimeUtc);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), secondPass.RunDateTimeUtc);

        Assert.Equal("2026-11-01 01:30:00", firstPass.RunTimeLocal);
        Assert.Equal("2026-11-01 01:30:00", secondPass.RunTimeLocal);
        Assert.Equal("2026-11-01 01:45:00", firstPass.LastSuccessfulRunLocal);
        Assert.Equal("2026-11-01 01:45:00", secondPass.LastSuccessfulRunLocal);

        /* A real instant in the same test keeps the offset: the two passes of 01:30 are 05:30Z and 06:30Z. */
        Assert.Equal("2026-11-01 01:30:00 -04:00", TrueUtcRowText(Naive(2026, 11, 1, 5, 30)));
        Assert.Equal("2026-11-01 01:30:00 -05:00", TrueUtcRowText(Naive(2026, 11, 1, 6, 30)));
    }

    [Fact]
    public void AJobStepOutsideTheRepeatedHour_ReadsAsTheDisplayTextItAlwaysDid()
    {
        using var scope = new ServerModeScope(Eastern);

        var beforeTheHour = ReadJobRow(Naive(2026, 11, 1, 0, 59), null);
        var afterTheHour = ReadJobRow(Naive(2026, 11, 1, 2, 0), null);
        var summer = ReadJobRow(Naive(2026, 7, 6, 12, 0), Naive(2026, 7, 6, 11, 0));

        Assert.Equal("2026-11-01 00:59:00", beforeTheHour.RunTimeLocal);
        Assert.Equal("2026-11-01 02:00:00", afterTheHour.RunTimeLocal);
        Assert.Equal("2026-07-06 12:00:00", summer.RunTimeLocal);
        Assert.Equal("2026-07-06 11:00:00", summer.LastSuccessfulRunLocal);
        Assert.Equal("Never", beforeTheHour.LastSuccessfulRunLocal);
        Assert.Equal("", new ViewerJobHistoryRow().RunTimeLocal);
    }

    // ── Agent status ──

    private static async Task<ViewerAgentStatusRow> ReadAgentRowAsync(DateTime? nextRunLocal)
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("server_name", typeof(string));
        table.Columns.Add("agent_running", typeof(bool));
        table.Columns.Add("agent_status_desc", typeof(string));
        table.Columns.Add("agent_startup_desc", typeof(string));
        table.Columns.Add("next_scheduled_run_local", typeof(DateTime));
        table.Rows.Add(1, "srv", true, "Running", "Auto", nextRunLocal.HasValue ? nextRunLocal.Value : DBNull.Value);

        using var reader = table.CreateDataReader();
        var rows = await ViewerDataService.ReadAgentStatusRowsAsync(
            reader, new Dictionary<int, ServerClock> { [1] = Eastern }, TestContext.Current.CancellationToken);
        return Assert.Single(rows);
    }

    [Fact]
    public async Task ANextScheduledRunReadFromTheStoredWallClock_PrintsNoOffset_InTheRepeatedHour_WhileARealInstantStillDoes()
    {
        using var scope = new ServerModeScope(Eastern);

        var repeated = await ReadAgentRowAsync(Naive(2026, 11, 1, 1, 30));
        var ordinary = await ReadAgentRowAsync(Naive(2026, 11, 2, 8, 0));
        var none = await ReadAgentRowAsync(null);

        Assert.Equal("2026-11-01 01:30:00", repeated.NextScheduledRunLocal);
        Assert.Equal("2026-11-02 08:00:00", ordinary.NextScheduledRunLocal);
        Assert.Equal("None scheduled", none.NextScheduledRunLocal);

        Assert.Equal("2026-11-01 01:30:00 -05:00", TrueUtcRowText(Naive(2026, 11, 1, 6, 30)));
    }

    // ── Default Trace ──

    private static DataTable NewDefaultTraceTable()
    {
        var table = new DataTable();
        table.Columns.Add("event_time_local", typeof(DateTime));
        table.Columns.Add("event_name", typeof(string));
        table.Columns.Add("database_name", typeof(string));
        table.Columns.Add("object_name", typeof(string));
        table.Columns.Add("login_name", typeof(string));
        table.Columns.Add("host_name", typeof(string));
        table.Columns.Add("application_name", typeof(string));
        table.Columns.Add("spid", typeof(int));
        table.Columns.Add("duration_us", typeof(long));
        table.Columns.Add("integer_data", typeof(long));
        table.Columns.Add("severity", typeof(int));
        table.Columns.Add("error_number", typeof(int));
        table.Columns.Add("text_data", typeof(string));
        return table;
    }

    [Fact]
    public async Task ADefaultTraceEventReadFromTheStoredWallClock_PrintsNoOffset_InBothPassesOfTheRepeatedHour_WhileARealInstantStillDoes()
    {
        using var scope = new ServerModeScope(Eastern);

        /* Two auto-grows, one in each pass of the hour: the trace file stores 01:30 for both (the label rides in
           database_name). */
        var table = NewDefaultTraceTable();
        foreach (var label in new[] { "first-pass", "second-pass" })
        {
            table.Rows.Add(Naive(2026, 11, 1, 1, 30), "Data File Auto Grow", label, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, 55, 1_500_000L, 256L, DBNull.Value, DBNull.Value, DBNull.Value);
        }

        table.Rows.Add(Naive(2026, 11, 1, 2, 30), "Data File Auto Grow", "after", DBNull.Value, DBNull.Value, DBNull.Value,
            DBNull.Value, 55, 1_500_000L, 256L, DBNull.Value, DBNull.Value, DBNull.Value);

        List<DefaultTraceEventRow> rows;
        using (var reader = table.CreateDataReader())
        {
            rows = await ViewerDataService.ReadDefaultTraceEventsAsync(
                reader, Eastern, Naive(2026, 11, 1, 0, 0), Naive(2026, 11, 1, 12, 0), TestContext.Current.CancellationToken);
        }

        var byLabel = rows.ToDictionary(r => r.DatabaseName!);
        Assert.Equal(3, byLabel.Count);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), byLabel["first-pass"].EventTimeUtc);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), byLabel["second-pass"].EventTimeUtc);

        Assert.Equal("2026-11-01 01:30:00", byLabel["first-pass"].EventTimeLocal);
        Assert.Equal("2026-11-01 01:30:00", byLabel["second-pass"].EventTimeLocal);
        Assert.Equal("2026-11-01 02:30:00", byLabel["after"].EventTimeLocal);

        /* A real instant in the same test keeps the offset. */
        Assert.Equal("2026-11-01 01:30:00 -04:00", TrueUtcRowText(Naive(2026, 11, 1, 5, 30)));
        Assert.Equal("2026-11-01 01:30:00 -05:00", TrueUtcRowText(Naive(2026, 11, 1, 6, 30)));
        Assert.Equal("", new DefaultTraceEventRow(
            null, DefaultTraceEventCategory.ErrorLog, "ErrorLog", null, null, null, null, null, null, null, null, 20, 823, "x").EventTimeLocal);
    }

    [Fact]
    public void TheStoredWallClockRenderer_IsTheForDisplayText_AndTheRealInstantRendererDiffersFromItOnlyInTheRepeatedHour()
    {
        using var scope = new ServerModeScope(Eastern);

        foreach (var instant in new[] { Naive(2026, 11, 1, 4, 59), Naive(2026, 11, 1, 7, 0), Naive(2026, 7, 6, 16, 0) })
        {
            Assert.Equal(ViewerTimeHelper.ForDisplay(instant).ToString(Format), SystemEventRowFormat.StoredWallClock(instant));
            Assert.Equal(SystemEventRowFormat.Local(instant), SystemEventRowFormat.StoredWallClock(instant));
        }

        var secondPass = Naive(2026, 11, 1, 6, 30);
        Assert.Equal("2026-11-01 01:30:00", SystemEventRowFormat.StoredWallClock(secondPass));
        Assert.Equal("2026-11-01 01:30:00 -05:00", SystemEventRowFormat.Local(secondPass));
        Assert.Equal("", SystemEventRowFormat.StoredWallClock(null));
    }
}
