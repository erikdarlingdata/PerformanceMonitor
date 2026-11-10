/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8541: what the desktop viewer's Query Store panel says over a window with no rows, against a real store. The panel shows its
/// empty-window sentence and then runs <see cref="ViewerServerTab.ShowQueryStoreEmptyReasonAsync"/>, which asks the store why. Each
/// case seeds a server, runs those two steps on a real slicer whose message host is a real text block (the pair the tab's XAML
/// wires), and reads what the host shows.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */
public sealed class ViewerQueryStoreEmptyReasonLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] args)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var a in args)
        {
            cmd.Parameters.AddWithValue(a);
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static Task RegisterAsync(NpgsqlConnection c, int id, string name, CancellationToken ct) =>
        ExecAsync(c,
            """
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date)
            VALUES ($1, $2, $2, TRUE, $3, $4, $4)
            """, ct, id, name, MonitoredEngineKind.SqlServer, Naive(DateTime.UtcNow));

    /// <summary>The newest <c>server_properties</c> row's role, as the collector writes it.</summary>
    private static Task RoleAsync(NpgsqlConnection c, int id, string name, string role, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, ag_replica_role) VALUES ($1, $2, $3, $4, $5)",
            ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow.AddMinutes(-5)), id, name, role);

    /// <summary>One READ_WRITE database of a <c>query_store_health</c> capture taken <paramref name="age"/> ago.</summary>
    private static Task CaptureAsync(NpgsqlConnection c, int id, string name, TimeSpan age, CancellationToken ct) =>
        ExecAsync(c,
            """
            INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason,
                                            current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days,
                                            max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode)
            VALUES ($1, $2, $3, $4, 'db1', 'READ_WRITE', 'READ_WRITE', 0, 100, 1000, 'AUTO', 30, 200, 60, 'AUTO', 'ON')
            """, ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow - age), id, name);

    private static Task DisableHealthAsync(NpgsqlConnection c, int id, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES ($1, $2, 60, 30, FALSE)",
            ct, id, QueryStoreHealthCollector.Instance.Name);

    /// <summary>A schedule row that runs <c>query_store_health</c> on the server every <paramref name="minutes"/> minutes.</summary>
    private static Task ScheduleHealthAsync(NpgsqlConnection c, int id, int minutes, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES ($1, $2, $3, 30, TRUE)",
            ct, id, QueryStoreHealthCollector.Instance.Name, minutes);

    /// <summary>A successful <c>query_store_hints</c> run two minutes old that stored no row and logged <paramref name="note"/>.</summary>
    private static Task HintsRunAsync(NpgsqlConnection c, int id, string name, string note, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message) VALUES ($1, $2, $3, $4, 'query_store_hints', 'SUCCESS', 40, 0, $5)",
            ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow.AddMinutes(-2)), id, name, note);

    /// <summary>
    /// Seeds one server in a scratch store and returns what the Query Store panel's message host shows once the panel has said its
    /// window is empty and run its reason step: the host's text, or null when the host is hidden.
    /// </summary>
    private static async Task<string?> ShownAsync(int serverId, string serverName, Func<NpgsqlConnection, CancellationToken, Task> seed)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Query Store panel's empty-reason tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await RegisterAsync(connection, serverId, serverName, ct);
                await seed(connection, ct);
            }

            string? shown = null;
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var server = new DarlingServer(serverId, serverName, serverName, true, null);
                StaTestThread.Run(async () =>
                {
                    var host = new TextBlock { Visibility = Visibility.Collapsed };
                    var slicer = new TimeRangeSlicerControl { EmptyMessage = host };
                    slicer.ShowEmpty(ViewerServerTab.QueryStoreEmptyWindowText);
                    await ViewerServerTab.ShowQueryStoreEmptyReasonAsync(slicer, viewer, server);
                    shown = host.Visibility == Visibility.Visible ? host.Text : null;
                });
            }

            bodySucceeded = true;
            return shown;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task OnAForwarderWhoseHintsRunLoggedThatEveryDatabaseIsASecondary_ThePanelGivesThatReason_InPlaceOfTheEmptyWindowSentence()
    {
        /* The forwarder of a distributed availability group: its role reads PRIMARY, its health capture has aged out of retention,
           and its newest query_store_hints run stored no row and logged that every database is a secondary. */
        const string name = "viewer-qs-forwarder";
        var shown = await ShownAsync(854_181, name, async (connection, ct) =>
        {
            await RoleAsync(connection, 854_181, name, "PRIMARY", ct);
            await DisableHealthAsync(connection, 854_181, ct);
            await HintsRunAsync(connection, 854_181, name, CollectorRuntimePrecondition.QueryStoreAllSecondaryRunNote, ct);
        });

        Assert.NotNull(shown);
        Assert.Contains("Query Store is not collected for " + name, shown, StringComparison.Ordinal);
        Assert.Contains("readable secondary", shown, StringComparison.Ordinal);
        Assert.DoesNotContain(ViewerServerTab.QueryStoreEmptyWindowText, shown, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAServerWhoseHealthCaptureIsWeeksOld_AndItsCollectorDisabled_ThePanelSaysNoNewerCaptureIsTaken_AfterTheEmptyWindowSentence()
    {
        /* Not a secondary: one READ_WRITE database in a capture 19 days old, and query_store_health disabled for the server. */
        const string name = "viewer-qs-stale-capture";
        var shown = await ShownAsync(854_182, name, async (connection, ct) =>
        {
            await CaptureAsync(connection, 854_182, name, TimeSpan.FromDays(19), ct);
            await DisableHealthAsync(connection, 854_182, ct);
        });

        Assert.NotNull(shown);
        Assert.StartsWith(ViewerServerTab.QueryStoreEmptyWindowText, shown, StringComparison.Ordinal);
        Assert.Contains("19 days", shown, StringComparison.Ordinal);
        Assert.Contains("the query_store_health collector is disabled for this server", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("runs hourly", shown, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAServerWhoseHealthCaptureIsWeeksOld_AndItsCollectorRunsEveryTwoHours_ThePanelNamesThatCadence_NotTheShippedHourlyOne()
    {
        /* Not a secondary: one READ_WRITE database in a capture 19 days old, and query_store_health scheduled every two hours. */
        const string name = "viewer-qs-stale-two-hourly";
        var shown = await ShownAsync(854_184, name, async (connection, ct) =>
        {
            await CaptureAsync(connection, 854_184, name, TimeSpan.FromDays(19), ct);
            await ScheduleHealthAsync(connection, 854_184, 120, ct);
        });

        Assert.NotNull(shown);
        Assert.StartsWith(ViewerServerTab.QueryStoreEmptyWindowText, shown, StringComparison.Ordinal);
        Assert.Contains("the query_store_health collector runs every 2 hours", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("runs hourly", shown, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAServerWhoseHealthCaptureIsFresh_ThePanelKeepsTheEmptyWindowSentence()
    {
        /* A quiet window on a server the store has nothing to say about: a READ_WRITE database captured ten minutes ago. */
        const string name = "viewer-qs-quiet";
        var shown = await ShownAsync(854_183, name, (connection, ct) => CaptureAsync(connection, 854_183, name, TimeSpan.FromMinutes(10), ct));

        Assert.Equal(ViewerServerTab.QueryStoreEmptyWindowText, shown);
    }
}
