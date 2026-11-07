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
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8146: when the long-query read finds this install's own session below <c>xe_ring_buffer_max_events</c>, the host recreates
/// that session after the item's rows are written, at most once an hour. Drives the real
/// <see cref="RemoteCollectorService.RunCollectorDefinitionAsync"/> through the reader seams and records each lifecycle step
/// the recreate sends through <c>LongQueryTraceStepOverrideForTests</c>, the seam the long-query lifecycle tests use.
/// Shares the statics collection of the other tests that write <see cref="App.XeRingBufferMaxEvents"/>.
/// </summary>
[Collection("app-alert-statics")]
public sealed class LongQuerySessionRecreateLiteTests : IDisposable
{
    private static readonly DateTime Created = new(2026, 11, 7, 6, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime ReadAt = new(2026, 11, 7, 9, 0, 0, DateTimeKind.Utc);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pmlite-lqrecreate-" + Guid.NewGuid().ToString("N"));
    private readonly int _settingBefore = App.XeRingBufferMaxEvents;
    private readonly List<DuckDbInitializer> _initializers = new();

    public LongQuerySessionRecreateLiteTests()
    {
        Directory.CreateDirectory(_dir);
        App.XeRingBufferMaxEvents = 5000;
    }

    public void Dispose()
    {
        App.XeRingBufferMaxEvents = _settingBefore;
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private sealed class Rig
    {
        public required RemoteCollectorService Service { get; init; }
        public required ServerConnection Server { get; init; }
        public required string OwnSession { get; init; }
        public List<(string Database, LongQueryTraceStep Step, string Session)> Steps { get; } = new();
    }

    private async Task<Rig> BuildRigAsync(int engineEdition, string? database = null, bool readOnlyIntent = false)
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_dir, "lite.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var servers = new ServerManager(_dir);
        var server = new ServerConnection
        {
            ServerName = "lqrecreate-sql",
            DisplayName = "lqrecreate-" + Guid.NewGuid().ToString("N")[..8],
            IsEnabled = true,
            DatabaseName = database,
            ReadOnlyIntent = readOnlyIntent,
        };
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = engineEdition;

        var service = new RemoteCollectorService(duckDb, servers, new ScheduleManager(_dir), installIdStore: new InstallIdStore(_dir, "test-machine", null));
        var rig = new Rig
        {
            Service = service,
            Server = server,
            OwnSession = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.LiteProduct, service.GetInstallId()),
        };

        service.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
        {
            rig.Steps.Add((database, step, session));
            return Task.CompletedTask;
        };

        return rig;
    }

    /* One completed row (so the write runs), then the trailing row: the truncation columns, then the position with the
       session's limit (1000). */
    private static FakeCollectorDataReader LimitRead(string database) =>
        FakeCollectorDataReader.WithResultSets(
            new[]
            {
                new object[]
                {
                    ReadAt, "sql_batch_completed", 5_000_000L, 4_000_000L, 1L, 10L, 0L, 1L, "OK", "SELECT 1;", DBNull.Value,
                    "SqlClient", 1234, 7, database, 1L, DBNull.Value, "0xABCD", "sa", 55,
                },
            },
            new[] { new object[] { false, 0L, DBNull.Value, 40L, 10L, Created, 0L, 1000L, ReadAt } });

    private static List<string> Lines(Rig rig) =>
        AppLogger.DrainBufferedLines()
            .Where(line => line.Contains(rig.Server.DisplayName, StringComparison.Ordinal))
            .ToList();

    [Fact]
    public async Task OnPrem_ALimitBelowTheSetting_RecreatesTheOwnSession_AndNothingElse()
    {
        var rig = await BuildRigAsync(engineEdition: 3);
        rig.Service.ServerReaderOverrideForTests = _ => LimitRead("SO");
        AppLogger.DrainBufferedLines();

        await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(
            new[] { (string.Empty, LongQueryTraceStep.Drop, rig.OwnSession), (string.Empty, LongQueryTraceStep.CreateAndStart, rig.OwnSession) },
            rig.Steps);
        Assert.Contains(Lines(rig), line => line.Contains(
            $"Recreated long-query session {rig.OwnSession} to apply max_events_limit 5000 (was 1000)", StringComparison.Ordinal));
    }

    [Fact]
    public async Task OnPrem_ASecondRequestWithinTheHour_SendsNothing()
    {
        var rig = await BuildRigAsync(engineEdition: 3);
        rig.Service.ServerReaderOverrideForTests = _ => LimitRead("SO");

        await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);
        Assert.Equal(2, rig.Steps.Count);

        await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);
        Assert.Equal(2, rig.Steps.Count);
    }

    [Fact]
    public async Task OnPrem_AFailedRecreate_IsLoggedAtWarning_AndDoesNotFailTheRun()
    {
        var rig = await BuildRigAsync(engineEdition: 3);
        rig.Service.ServerReaderOverrideForTests = _ => LimitRead("SO");
        rig.Service.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
        {
            rig.Steps.Add((database, step, session));
            return step == LongQueryTraceStep.CreateAndStart
                ? Task.FromException(new InvalidOperationException("The create was refused."))
                : Task.CompletedTask;
        };
        AppLogger.DrainBufferedLines();

        var written = await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(1, written);
        Assert.Contains(Lines(rig), line => line.Contains("WARN", StringComparison.Ordinal)
            && line.Contains($"Failed to recreate long-query session {rig.OwnSession}", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Azure_ADatabaseWhoseWriteFailed_IsNotRecreated_WhileItsSiblingIs()
    {
        var rig = await BuildRigAsync(engineEdition: 5);
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha", "beta" });
        rig.Service.AzureDatabaseReaderOverrideForTests = (database, _) => LimitRead(database);
        rig.Service.PerDatabaseWriteFaultForTests = database =>
        {
            if (database == "beta")
            {
                throw new InvalidOperationException("injected write failure for beta");
            }
        };

        await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(
            new[] { ("alpha", LongQueryTraceStep.Drop, rig.OwnSession), ("alpha", LongQueryTraceStep.CreateAndStart, rig.OwnSession) },
            rig.Steps);
    }

    /// <summary>
    /// A registration with read-only intent reads a session that runs on its replica: the recreate stops it there, drops it
    /// on the primary, creates the definition there and starts it on the replica, the reconcile's own steps.
    /// </summary>
    [Fact]
    public async Task Azure_ReadOnlyIntent_StopsDropsCreatesAndStartsTheOwnSession()
    {
        var rig = await BuildRigAsync(engineEdition: 5, database: "alpha", readOnlyIntent: true);
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });
        rig.Service.AzureDatabaseReaderOverrideForTests = (database, _) => LimitRead(database);

        await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(
            new[]
            {
                ("alpha", LongQueryTraceStep.Stop, rig.OwnSession),
                ("alpha", LongQueryTraceStep.Drop, rig.OwnSession),
                ("alpha", LongQueryTraceStep.Check, rig.OwnSession),
                ("alpha", LongQueryTraceStep.CreateDefinition, rig.OwnSession),
                ("alpha", LongQueryTraceStep.Start, rig.OwnSession),
            },
            rig.Steps);
    }
}
