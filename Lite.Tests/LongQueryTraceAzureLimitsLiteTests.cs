/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7639: an Azure SQL Database server past its event-session count or memory limit refuses the long-query trace's create in
/// some databases on every cycle. The create is held for an hour in a database that refused it for a reason that will repeat
/// (a permission refusal is not held, so a grant works on the next cycle), the refusals are told at Warning once and at
/// Debug after, and the run's note names the databases without a session and why. The databases that did get a session are
/// still read. Each test drives the real reconcile with the work in each database replaced, so no server is needed.
///
/// <para>In <c>app-logger-statics</c> because the tests read <see cref="AppLogger"/>'s process-wide buffer.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class LongQueryTraceAzureLimitsLiteTests : IDisposable
{
    private const string Host = "lqlimits.database.windows.net";
    private const string Collector = "long_query_completions";

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _configDir;

    public LongQueryTraceAzureLimitsLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    private sealed class Rig
    {
        public required RemoteCollectorService Service { get; init; }
        public required ServerConnection Server { get; init; }
        public required DuckDbInitializer DuckDb { get; init; }
        public required ScheduleManager Schedules { get; init; }
        public required int ServerId { get; init; }

        public DateTime Clock { get; set; } = new(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);
        public List<string> Calls { get; } = new();
        public List<string> LegacyCalls { get; } = new();
        public Dictionary<string, int> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private async Task<Rig> BuildRigAsync()
    {
        var server = new ServerConnection { ServerName = Host, DisplayName = "lqlimits-" + Guid.NewGuid().ToString("N")[..8] };

        var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule(Collector, enabled: true);

        var rig = new Rig
        {
            Service = new RemoteCollectorService(duckDb, servers, schedules, installIdStore: new InstallIdStore(_configDir, "test-machine", null)),
            Server = server,
            Schedules = schedules,
            DuckDb = duckDb,
            ServerId = RemoteCollectorService.GetServerId(server),
        };

        rig.Service.LongQueryTraceUtcNowForTests = () => rig.Clock;
        rig.Service.LegacyLongQuerySessionExistsForTests = (_, _) => false;
        rig.Service.LongQueryTraceListOverrideForTests = (_, _, _) => Task.FromResult(new List<string> { "alpha", "beta", "gamma" });

        /* The statement a database would run. The shared step wrapper marks its failure as the Azure limits' (the caps sentence), as it does in production. */
        rig.Service.LongQueryTraceStepOverrideForTests = (_, database, _, _, sessionName, _) =>
        {
            if (sessionName == LongQueryCompletionsCollector.LegacyXeSessionName)
            {
                rig.LegacyCalls.Add(database);
                return Task.CompletedTask;
            }

            rig.Calls.Add(database);
            return rig.Refuse.TryGetValue(database, out var number)
                ? Task.FromException(SqlExceptionFactory.Create(number, errorClass: 14, message: $"The create was refused in {database}."))
                : Task.CompletedTask;
        };

        /* The read: a database with no session cannot be opened and is skipped by the Azure loop, the others read no rows, so the run succeeds. */
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha", "beta", "gamma" });
        rig.Service.AzureDatabaseReaderOverrideForTests = (database, _) =>
            rig.Refuse.ContainsKey(database)
                ? throw SqlExceptionFactory.Create(4060, errorClass: 11, message: $"Cannot open database \"{database}\".")
                : new System.Data.DataTable().CreateDataReader();

        return rig;
    }

    private static Task ReconcileAsync(Rig rig) =>
        rig.Service.ReconcileLongQueryCompletionsXeSessionAsync(rig.Server, CancellationToken.None);

    private static async Task WithDebugLoggingAsync(Func<Task> body)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();
            await body();
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    private static List<string> Lines(Rig rig) =>
        AppLogger.DrainBufferedLines().Where(line => line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)).ToList();

    private static int Count(IEnumerable<string> lines, string level, string text) =>
        lines.Count(line => line.Contains(level, StringComparison.Ordinal) && line.Contains(text, StringComparison.Ordinal));

    private async Task<(string Status, string? Message)> LastRunAsync(Rig rig)
    {
        using var connection = rig.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT status, error_message FROM collection_log WHERE server_id = {rig.ServerId} AND collector_name = '{Collector}' ORDER BY log_id DESC LIMIT 1";
        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    /// <summary>
    /// A database past the Azure limits refuses the create on every cycle. The first cycle sends it, the cycles inside the hour send
    /// nothing there while the other databases are still ensured, and the cycle after the hour tries again.
    /// </summary>
    [Fact]
    public async Task ACreateRefusedAtTheAzureLimits_IsHeldForAnHour_AndTheOtherDatabasesAreStillEnsured()
    {
        var rig = await BuildRigAsync();
        rig.Refuse["beta"] = 1105;

        await ReconcileAsync(rig);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Calls);

        rig.Calls.Clear();
        rig.Clock = rig.Clock.AddMinutes(10);
        await ReconcileAsync(rig);
        Assert.Equal(new[] { "alpha", "gamma" }, rig.Calls);

        rig.Calls.Clear();
        rig.Clock = rig.Clock.AddMinutes(61);
        await ReconcileAsync(rig);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Calls);
    }

    /// <summary>A permission refusal is not held: the grant that fixes it works on the next cycle.</summary>
    [Fact]
    public async Task APermissionRefusal_IsNotHeld()
    {
        var rig = await BuildRigAsync();
        rig.Refuse["beta"] = 15247;

        await ReconcileAsync(rig);
        rig.Calls.Clear();
        rig.Clock = rig.Clock.AddMinutes(10);
        await ReconcileAsync(rig);

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Calls);
    }

    /// <summary>A trace turned off and on again is not held by a refusal from before: turning it off dropped the sessions.</summary>
    [Fact]
    public async Task ATraceTurnedOffAndOnAgain_IsNotHeldByARefusalFromBefore()
    {
        var rig = await BuildRigAsync();
        rig.Refuse["beta"] = 1105;
        await ReconcileAsync(rig);

        rig.Schedules.UpdateSchedule(Collector, enabled: false);
        await ReconcileAsync(rig);
        rig.Schedules.UpdateSchedule(Collector, enabled: true);

        rig.Calls.Clear();
        await ReconcileAsync(rig);
        Assert.Contains("beta", rig.Calls);
    }

    /// <summary>
    /// A refusal in some databases is a failing cycle for the log: told at Warning on the first cycle and at Debug after, until a
    /// cycle has none. The refused database is held, so the later cycles' line is the held refusal and no connection.
    /// </summary>
    [Fact]
    public async Task APartialRefusal_IsToldAtWarningOnce_ThenDebug()
    {
        var rig = await BuildRigAsync();
        rig.Refuse["beta"] = 1105;

        await WithDebugLoggingAsync(async () =>
        {
            await ReconcileAsync(rig);
            var first = Lines(rig);
            Assert.Equal(1, Count(first, "WARN", "Failed to ensure long query completions XE session"));

            rig.Clock = rig.Clock.AddMinutes(61);
            await ReconcileAsync(rig);
            await ReconcileAsync(rig);
            var later = Lines(rig);
            Assert.Equal(0, Count(later, "WARN", "Failed to ensure long query completions XE session"));
            Assert.True(Count(later, "DEBUG", "Failed to ensure long query completions XE session") >= 2);
        });
    }

    /// <summary>
    /// The databases that got a session are read, and the run's note names the one that did not, with the server's reason and what fixes it.
    /// </summary>
    [Fact]
    public async Task APartialRefusal_PutsANoteOnTheRun_AndTheRunStillSucceeds()
    {
        var rig = await BuildRigAsync();
        rig.Refuse["beta"] = 1105;

        await ReconcileAsync(rig);
        await rig.Service.RunCollectorAsync(rig.Server, Collector, CancellationToken.None);

        var run = await LastRunAsync(rig);
        Assert.Equal("SUCCESS", run.Status);
        Assert.Contains("beta", run.Message, StringComparison.Ordinal);
        Assert.Contains("The create was refused in beta.", run.Message, StringComparison.Ordinal);
        Assert.Contains("ALTER ANY DATABASE EVENT SESSION", run.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// #7639: with "Databases that get Extended Events sessions" set to two of three databases, the long query trace creates in those
    /// two only. Empty creates in all three, which is how it worked before the setting.
    /// </summary>
    [Fact]
    public async Task TheXeSessionDatabaseList_NarrowsTheLongQueryCreates_AndEmptyMeansEveryDatabase()
    {
        var rig = await BuildRigAsync();
        rig.Server.XeSessionDatabases = new List<string> { "gamma", "ALPHA" };

        await ReconcileAsync(rig);
        Assert.Equal(new[] { "alpha", "gamma" }, rig.Calls);

        /* Cleared, the reconcile runs again (the list is part of its state key) and gives beta its session too. */
        rig.Calls.Clear();
        rig.Server.XeSessionDatabases = new List<string>();
        await ReconcileAsync(rig);
        Assert.Contains("beta", rig.Calls);
    }

    /// <summary>
    /// #7639: the list narrows the per-install creates only. The ownerless legacy session holds an Azure session slot that nothing owns,
    /// so its drop still reaches a database left off the list. This pins the behaviour Lite already had, so a change that narrowed the
    /// legacy drop by the list, as Darling's did before this fix, fails here.
    /// </summary>
    [Fact]
    public async Task TheXeSessionDatabaseList_DoesNotNarrowTheLegacyDrop_ADatabaseLeftOffStillGetsIt()
    {
        var rig = await BuildRigAsync();
        rig.Server.XeSessionDatabases = new List<string> { "alpha" };

        await ReconcileAsync(rig);

        Assert.Equal(new[] { "alpha" }, rig.Calls);
        Assert.Contains("beta", rig.LegacyCalls);
        Assert.Contains("gamma", rig.LegacyCalls);
    }

    /// <summary>
    /// #7639: a database left off the list is not refused, so it is not in the run's note. A database on the list that Azure refuses
    /// is still named.
    /// </summary>
    [Fact]
    public async Task ADatabaseOffTheList_IsNotInTheRunNote_AndARefusedOneOnTheListStillIs()
    {
        var rig = await BuildRigAsync();
        rig.Server.XeSessionDatabases = new List<string> { "alpha", "beta" };
        rig.Refuse["beta"] = 1105;

        await ReconcileAsync(rig);
        await rig.Service.RunCollectorAsync(rig.Server, Collector, CancellationToken.None);

        var run = await LastRunAsync(rig);
        Assert.Equal("SUCCESS", run.Status);
        Assert.Contains("The create was refused in beta.", run.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("gamma", run.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(rig.Calls, call => call == "gamma");
    }
}
