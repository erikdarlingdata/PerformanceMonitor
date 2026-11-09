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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using DuckDB.NET.Data;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8342: the Job History tab's time range picker names where the stored job history starts, for the scope on screen: the oldest
/// run the store holds, archived or hot, fleet-wide at All Servers and the picked server's own at one server. A typed range wider
/// than the stored history previews that run, and a range inside the stored history shows no note. The note cannot come from the
/// grid's window-bounded coverage probe: a server collected for a month whose job history reaches back three days covers a week's
/// window, so that probe answers the window's own start and the note stays silent. Its own store, because it writes a Parquet
/// file and builds the archive views.
/// </summary>
public sealed class JobHistoryPickerDataStartTests : IDisposable
{
    private const int ServerA = 83421;
    private const int ServerB = 83422;

    /// <summary>How long a load may take to write the picker's note before the test gives up. The wait ends when the note is written.</summary>
    private static readonly TimeSpan LoadLimit = TimeSpan.FromSeconds(30);

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private DuckDbInitializer? _initializer;

    public JobHistoryPickerDataStartTests() => Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));

    public void Dispose()
    {
        _initializer?.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static string Ts(DateTime utc) => $"TIMESTAMP '{utc:yyyy-MM-dd HH:mm:ss}'";

    private const string JobColumns = "(job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, run_status, run_duration_seconds, retries_attempted, run_datetime)";

    /// <summary>The zone the servers' runs are read on: they have no clock row, so the service reads them on the machine's zone, which
    /// every test fixes here (east of UTC, where a run at its collection would otherwise read earlier than the collection).</summary>
    private static readonly TimeZoneInfo MachineZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    /// <summary>One job outcome, stored at <paramref name="utc"/> and run at it, the run written on <see cref="MachineZone"/>'s wall clock.</summary>
    private static string Run(long id, int serverId, DateTime utc) =>
        $"({id}, {Ts(utc)}, {serverId}, 'SERVER-{serverId}', {id}, 'job-{serverId}', 'Nightly', true, 0, 1, 5, 0, {Ts(TimeZoneInfo.ConvertTimeFromUtc(utc, MachineZone))})";

    private const string LogColumns = "(log_id, server_id, server_name, collector_name, collection_time, status)";

    private static string LogRow(long id, int serverId, DateTime utc) =>
        $"({id}, {serverId}, 'SERVER-{serverId}', 'job_history', {Ts(utc)}, 'SUCCESS')";

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private string ParquetPath(string name) => Path.Combine(_tempDir, "archive", name).Replace("\\", "/");

    /// <summary>The seeded instants, relative to the clock the picker's relative ranges resolve against.</summary>
    private sealed record Seeded(DateTime ArchivedA, DateTime HotA, DateTime FirstB);

    /// <summary>
    /// A store where both servers have been collected daily for a month (so a week's window is covered), whose archive holds server
    /// A's run from three days ago, and whose hot table holds a later run of A and server B's first run, both within the last day.
    /// </summary>
    private async Task<(LocalDataService Service, Seeded Seeded)> StoreAsync()
    {
        var now = DateTime.UtcNow;
        var nowUtc = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
        var seeded = new Seeded(nowUtc.AddDays(-3), nowUtc.AddHours(-20), nowUtc.AddHours(-10));

        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _initializer = new DuckDbInitializer(dbPath);
        await _initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            var logRows = new List<string>();
            long logId = 1;
            for (var day = 30; day >= 0; day--)
            {
                logRows.Add(LogRow(logId++, ServerA, nowUtc.AddDays(-day)));
                logRows.Add(LogRow(logId++, ServerB, nowUtc.AddDays(-day)));
            }

            await ExecuteAsync(connection, $"INSERT INTO collection_log {LogColumns} VALUES {string.Join(", ", logRows)}");

            await ExecuteAsync(connection, $"INSERT INTO job_history {JobColumns} VALUES {Run(1, ServerA, seeded.ArchivedA)}");
            await ExecuteAsync(connection, $"COPY job_history TO '{ParquetPath($"{seeded.ArchivedA:yyyyMMdd_HHmm}_job_history.parquet")}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, "DELETE FROM job_history");
            await ExecuteAsync(connection, $"INSERT INTO job_history {JobColumns} VALUES {Run(2, ServerA, seeded.HotA)}, {Run(3, ServerB, seeded.FirstB)}");
        }

        await _initializer.CreateArchiveViewsAsync();
        return (new LocalDataService(_initializer) { MachineZone = MachineZone }, seeded);
    }

    private static JobHistoryTab Tab(LocalDataService service)
    {
        var tab = new JobHistoryTab
        {
            OfferedServers = () => new Dictionary<int, string> { [ServerA] = $"SERVER-{ServerA}", [ServerB] = $"SERVER-{ServerB}" }
        };
        tab.Initialize(service);

        var host = new Border { Width = 1000, Height = 680, Child = tab };
        host.Measure(new Size(1000, 680));
        host.Arrange(new Rect(0, 0, 1000, 680));
        return tab;
    }

    /// <summary>Loads the grid over the picker's range and waits for the load to write the picker's note.</summary>
    private static async Task LoadAsync(JobHistoryTab tab)
    {
        tab.RangePicker.DataStartUtc = null;
        tab.RefreshJobs();

        var deadline = DateTime.UtcNow + LoadLimit;
        while (tab.RangePicker.DataStartUtc is null && DateTime.UtcNow < deadline)
        {
            await Task.Delay(25);
        }

        Assert.NotNull(tab.RangePicker.DataStartUtc);
    }

    private static TextBlock Note(TimeRangePicker picker) => Assert.IsType<TextBlock>(picker.FindName("NoteText"));

    [Fact]
    public async Task AWeekAtAllServers_NamesTheOldestArchivedRun_NotTheWindowStart()
    {
        var (service, seeded) = await StoreAsync();

        StaTestThread.Run(async () =>
        {
            var tab = Tab(service);
            tab.RangePicker.Value = TimeRangePresets.Find("1w")!;

            await LoadAsync(tab);

            Assert.Equal(seeded.ArchivedA, tab.RangePicker.DataStartUtc);
            var note = Note(tab.RangePicker);
            Assert.Equal(Visibility.Visible, note.Visibility);
            Assert.StartsWith("Data starts ", note.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task FourHoursInsideTheStoredHistory_ShowsNoNote()
    {
        var (service, seeded) = await StoreAsync();

        StaTestThread.Run(async () =>
        {
            var tab = Tab(service);
            tab.RangePicker.Value = TimeRangePresets.Find("4h")!;

            await LoadAsync(tab);

            Assert.Equal(seeded.ArchivedA, tab.RangePicker.DataStartUtc);
            Assert.Equal(Visibility.Collapsed, Note(tab.RangePicker).Visibility);
        });
    }

    [Fact]
    public async Task OneServer_NamesThatServersOwnFirstRun_NotAnotherServersArchivedOne()
    {
        var (service, seeded) = await StoreAsync();

        StaTestThread.Run(async () =>
        {
            var tab = Tab(service);
            tab.RangePicker.Value = TimeRangePresets.Find("1w")!;
            await LoadAsync(tab);

            /* The combo offers both servers after the first load; picking B scopes the next load, and the note, to it. */
            var pickedB = false;
            for (var i = 1; i < tab.ServerFilterComboBox.Items.Count; i++)
            {
                if ((tab.ServerFilterComboBox.Items[i] as ComboBoxItem)?.Tag as string == ServerB.ToString(System.Globalization.CultureInfo.InvariantCulture))
                {
                    tab.ServerFilterComboBox.SelectedIndex = i;
                    pickedB = true;
                }
            }

            Assert.True(pickedB, "the Server combo does not offer server B");
            await LoadAsync(tab);

            Assert.Equal(seeded.FirstB, tab.RangePicker.DataStartUtc);
            var note = Note(tab.RangePicker);
            Assert.Equal(Visibility.Visible, note.Visibility);
            Assert.StartsWith("Data starts ", note.Text, StringComparison.Ordinal);
        });
    }
}
