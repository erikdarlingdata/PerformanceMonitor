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
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls.Primitives;
using System.Windows.Threading;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis.Recommendations;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8542: the Recommendations tab's Generate now button renders the analysis it just ran, and its cards
/// follow the store's forcing state the way the stored load's cards do. The real tab is shown on the
/// shared STA thread over a store that holds two regressed queries and an automatic-plan-correction row
/// withdrawing the engine's recommendation for one best plan; the button is clicked and the realized
/// cards are read.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class RecommendationsGenerateNowForcePlanStateTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string Db = "AppDb";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -8_542_000;
    private DuckDBConnection? _seedConn;

    public RecommendationsGenerateNowForcePlanStateTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _tempDir = Path.Combine(Path.GetTempPath(), "GenerateNowForcePlan_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        /* Windows auth so AddServer never touches the credential store. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        /* The id the tab resolves the selected server to, so the rows below are the rows it reads. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task GenerateNow_GivesNoForceStatement_ForATargetTheStoreBlocks()
    {
        var now = DateTime.UtcNow;
        var windowEnd = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Utc).AddMinutes(-1);

        /* More than a day of wait history, so the analysis runs. */
        await PlantWaitAsync(windowEnd.AddHours(-30), 0L);
        for (var i = 0; i <= 16; i++)
            await PlantWaitAsync(windowEnd.AddHours(-4).AddMinutes(15 * i), i == 0 ? 0L : 900_000L);

        /* Two queries whose plan regressed inside the window: 201 from plan 11, 202 from plan 12. */
        foreach (var (queryId, bestPlanId, latestPlanId) in new[] { (201L, 11L, 19L), (202L, 12L, 29L) })
        {
            await PlantQueryStorePlanAsync(windowEnd, queryId, bestPlanId, "0xCHEAP" + queryId, cpuUs: 100_000,
                firstExec: windowEnd.AddDays(-6), lastExec: windowEnd.AddDays(-5));
            await PlantQueryStorePlanAsync(windowEnd, queryId, latestPlanId, "0xCOSTLY" + queryId, cpuUs: 500_000,
                firstExec: windowEnd.AddDays(-1), lastExec: windowEnd);
        }

        /* The engine recommended plan 12 for query 202 and withdrew it: apc_withdrew_it. */
        await PlantWithdrawnRecommendationAsync(now.AddMinutes(-20), queryId: 202, lastGoodPlanId: 12);

        var commands = StaTestThread.Run(() => WithTheme(() =>
        {
            var found = new List<string>();
            var tab = new RecommendationsTab();
            var window = new Window { Content = tab, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
            StaTestThread.ShowOffScreen(window);
            try
            {
                /* Under the dispatcher's own context, as on the app's UI thread, so the tab's handlers resume on this thread after they await. */
                StaTestThread.WithDispatcherContext(() =>
                {
                    tab.Initialize(_duckDb, _serverManager, scheduleManager: null, openTabClock: _ => null);
                    PumpUntilLoaded(tab, "the tab's first load");
                    StaTestThread.WaitForIdle(tab.Dispatcher, StaTestThread.IdleLimit);

                    tab.GenerateNowButton.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                    Assert.Equal("Generating…", tab.StatusText.Text);
                    PumpUntilLoaded(tab, "Generate now");
                    StaTestThread.WaitForIdle(tab.Dispatcher, StaTestThread.IdleLimit);
                    Assert.Equal(string.Empty, tab.StatusText.Text);

                    /* The cards the list realized. */
                    found.AddRange(ClippedTextProbe.Descendants(tab)
                        .OfType<FrameworkElement>()
                        .Select(element => element.DataContext)
                        .OfType<LiteRecommendationCardViewModel>()
                        .Distinct()
                        .Select(card => card.CopyPasteSql ?? string.Empty));
                });

                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        /* Every line SQL Server would run, across the cards Generate now rendered. */
        var runnable = commands
            .SelectMany(command => command.Split('\n'))
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("--", StringComparison.Ordinal))
            .ToList();

        Assert.True(Forces(runnable, 201, 11), "the target the store holds nothing against keeps its statement on the Generate now card");
        Assert.False(Forces(runnable, 202, 12), "plan 12 is apc_withdrew_it in the store, and the Generate now card must not give a runnable force statement for it");
    }

    private static bool Forces(IEnumerable<string> runnableLines, long queryId, long planId) =>
        runnableLines.Any(line =>
            line.Contains("sp_query_store_force_plan", StringComparison.Ordinal) &&
            line.Contains($"@query_id = {queryId},", StringComparison.Ordinal) &&
            line.Contains($"@plan_id = {planId};", StringComparison.Ordinal));

    /// <summary>
    /// Runs a dispatcher frame until the tab's loading overlay goes off, which every state the tab ends a load or a
    /// generate in does. The frame ends on the overlay's own change; the timer only ends a wait that would never finish.
    /// </summary>
    private static void PumpUntilLoaded(RecommendationsTab tab, string what)
    {
        if (!tab.LoadingOverlay.IsLoading)
        {
            return;
        }

        var frame = new DispatcherFrame();
        var finished = false;
        var isLoading = DependencyPropertyDescriptor.FromProperty(LoadingOverlay.IsLoadingProperty, typeof(LoadingOverlay));
        EventHandler changed = (_, _) =>
        {
            if (!tab.LoadingOverlay.IsLoading)
            {
                finished = true;
                frame.Continue = false;
            }
        };
        var expiry = new DispatcherTimer(DispatcherPriority.Send, tab.Dispatcher)
        {
            Interval = StaHost.WithinGuard(TimeSpan.FromSeconds(120), out _),
        };
        expiry.Tick += (_, _) => frame.Continue = false;
        isLoading.AddValueChanged(tab.LoadingOverlay, changed);
        try
        {
            expiry.Start();
            Dispatcher.PushFrame(frame);
        }
        finally
        {
            expiry.Stop();
            isLoading.RemoveValueChanged(tab.LoadingOverlay, changed);
        }

        Assert.True(finished, $"{what} did not finish");
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") });
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var dictionary in saved)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task PlantWaitAsync(DateTime at, long deltaWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, 'TestServer', 'SOS_SCHEDULER_YIELD', $4, $5, 0, $4, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(deltaWaitMs > 0 ? 10L : 0L);
        P(deltaWaitMs);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task PlantQueryStorePlanAsync(DateTime collectedAt, long queryId, long planId, string planHash, long cpuUs, DateTime firstExec, DateTime lastExec)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, 'TestServer', $4, $5, $6, 'Regular', $7, $8, $9, $10, 100, $11, $12, $13, false, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(collectedAt);
        P(_serverId);
        P(Db);
        P(queryId);
        P(planId);
        P(firstExec);
        P(lastExec);
        P("SELECT * FROM dbo.Orders WHERE CustomerId = @id" + queryId);
        P("0xQH" + queryId);
        P(cpuUs);
        P(cpuUs + 20_000);
        P(planHash);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task PlantWithdrawnRecommendationAsync(DateTime at, long queryId, long lastGoodPlanId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO plan_correction
    (collection_id, collection_time, server_id, server_name, database_name,
     force_last_good_plan_desired_state, force_last_good_plan_actual_state,
     recommendation_name, recommendation_state, recommendation_state_reason, score,
     query_id, regressed_plan_id, last_good_plan_id, last_good_plan_is_forced, execute_action_initiated_by)
VALUES ($1, $2, $3, 'TestServer', $4, 'OFF', 'OFF', $5, 'Expired', 'TempTableChanged', 50, $6, 7, $7, false, 'System')";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        P(_serverId);
        P(Db);
        P("PR_" + queryId);
        P(queryId);
        P(lastGoodPlanId);
        await cmd.ExecuteNonQueryAsync();
    }
}
