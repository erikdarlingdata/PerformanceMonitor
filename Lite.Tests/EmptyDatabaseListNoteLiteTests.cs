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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// A per-database run whose database list comes back empty records SUCCESS with 0 rows. Without a note that reads
/// as "nothing ran". Two things empty the list on an Azure SQL Database logical server: every user database is
/// monitored as its own server (the long-query read leaves those to their own registrations), or every database is
/// excluded. The run keeps SUCCESS and carries a note that says why nothing was read (#4961).
///
/// <para>The runs below go through the real per-database loop of the definition runner with the database list as
/// the only hook. An empty list opens no database connection, so no fake reader is needed.</para>
/// </summary>
public sealed class EmptyDatabaseListNoteLiteTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _configDir;

    public EmptyDatabaseListNoteLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /* ── the shared notes ── */

    [Fact]
    public void TheNotes_AreFixedText_InProductWords()
    {
        Assert.Equal("no database read: every user database is monitored as its own server", EmptyDatabaseListNote.EverySeparatelyMonitored);
        Assert.Equal("no database read: every database is excluded", EmptyDatabaseListNote.EveryExcluded);
    }

    [Theory]
    [InlineData(EmptyDatabaseListNote.EverySeparatelyMonitored)]
    [InlineData(EmptyDatabaseListNote.EveryExcluded)]
    public void AServerWhoseEveryRunCarriesTheNote_ReadsItWithItsRunCount_AndNoQualifier(string note)
    {
        /* index_object_stats is a collector the health bands expect to find user databases for, so the missing
           qualifier comes from the note, not from the collector being left out of that list. */
        Assert.True(CollectorHealthClassifier.ExpectsUserDatabases("index_object_stats"));
        Assert.DoesNotContain(CollectorHealthClassifier.EmptyEnumerationMarker, note, StringComparison.Ordinal);
        Assert.False(CollectorHealthClassifier.HasMeasurements(note));

        var formatted = CollectorHealthClassifier.FormatCollectionNote(
            note, noteCount: 96, totalRuns: 96, collectorName: "index_object_stats", targetHasUserDatabases: true);

        Assert.Equal(note + " (all 96 runs)", formatted);
        Assert.DoesNotContain(CollectorHealthClassifier.HasUserDatabasesQualifier, formatted, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, 0, false, false, false, null)]
    [InlineData(0, 0, false, true, false, EmptyDatabaseListNote.EveryExcluded)]
    [InlineData(0, 0, true, true, false, EmptyDatabaseListNote.EveryExcluded)]
    [InlineData(0, 0, false, true, true, null)]
    [InlineData(2, 0, true, false, false, EmptyDatabaseListNote.EverySeparatelyMonitored)]
    [InlineData(2, 0, true, true, true, EmptyDatabaseListNote.EverySeparatelyMonitored)]
    [InlineData(2, 1, true, false, false, null)]
    [InlineData(2, 2, false, true, false, null)]
    public void TheNote_NamesTheReasonTheListEmptied_AndNothingItCannotName(
        int listed, int read, bool skipsSeparatelyMonitored, bool exclusionsConfigured, bool databaseScoped, string? expected)
    {
        Assert.Equal(expected, EmptyDatabaseListNote.For(listed, read, skipsSeparatelyMonitored, exclusionsConfigured, databaseScoped));
    }

    /* ── the run ── */

    [Fact]
    public async Task EveryUserDatabaseMonitoredAsItsOwnServer_RecordsTheRunWithItsNote()
    {
        var rig = await BuildRigAsync(registeredDatabases: new[] { "alpha", "zeta" }, excludedDatabases: Array.Empty<string>());
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha", "zeta" });

        var written = await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Equal(EmptyDatabaseListNote.EverySeparatelyMonitored, rig.Service.TelemetryFor(rig.ServerId).Note);
    }

    [Fact]
    public async Task EveryDatabaseExcluded_RecordsTheRunWithItsNote()
    {
        var rig = await BuildRigAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: new[] { "alpha", "zeta" });
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string>());

        var written = await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Equal(EmptyDatabaseListNote.EveryExcluded, rig.Service.TelemetryFor(rig.ServerId).Note);
    }

    [Fact]
    public async Task EveryDatabaseExcluded_NotesACollectorThatReadsEveryDatabaseToo()
    {
        var rig = await BuildRigAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: new[] { "alpha", "zeta" });
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string>());

        await rig.Service.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(EmptyDatabaseListNote.EveryExcluded, rig.Service.TelemetryFor(rig.ServerId).Note);
    }

    [Fact]
    public async Task AnEmptyListWithNoExclusions_StaysWithoutANote()
    {
        var rig = await BuildRigAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: Array.Empty<string>());
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string>());

        var written = await rig.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, rig.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Null(rig.Service.TelemetryFor(rig.ServerId).Note);
    }

    /* ── the runner wiring ── */

    [Fact]
    public void TheRunner_TakesTheNoteFromTheSharedRule_AndCarriesNoTextOfItsOwn()
    {
        var runner = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"));

        Assert.Contains("EmptyDatabaseListNote.For(", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("no database read", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// The assignment that follows the per-database loop is unconditional, so a note assigned anywhere earlier
    /// would be erased by it. The note rides in that assignment instead.
    /// </summary>
    [Fact]
    public void TheNote_IsMergedIntoTheAssignmentThatFollowsThePerDatabaseLoop()
    {
        var runner = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"));

        var loopEnd = runner.IndexOf("context.CurrentDatabaseName = null;", StringComparison.Ordinal);
        Assert.True(loopEnd > 0, "the per-database loop is followed by a reset of the current database");
        var assignment = runner.IndexOf("telemetry.HostNote = EnumeratedCollectorDriver.MergeNotes(", loopEnd, StringComparison.Ordinal);
        Assert.True(assignment > loopEnd, "the run note is assigned after the per-database loop");

        var statement = runner[assignment..runner.IndexOf(';', assignment)];
        Assert.Contains("emptyListNote", statement, StringComparison.Ordinal);
    }

    private sealed record Rig(RemoteCollectorService Service, ServerConnection Server, int ServerId);

    /// <summary>
    /// A logical-server registration (no database named) on the Azure SQL Database engine edition, so the
    /// definition takes the per-database loop, plus one registration per named database on the same host.
    /// </summary>
    private async Task<Rig> BuildRigAsync(string[] registeredDatabases, string[] excludedDatabases)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var serverManager = new ServerManager(_configDir);
        var server = new ServerConnection
        {
            ServerName = "azure-test",
            DisplayName = "azure-test",
            ExcludedDatabases = excludedDatabases.ToList(),
        };
        serverManager.AddServer(server);
        foreach (var database in registeredDatabases)
        {
            serverManager.AddServer(new ServerConnection
            {
                ServerName = "azure-test",
                DisplayName = "azure-test " + database,
                DatabaseName = database,
            });
        }

        serverManager.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var service = new RemoteCollectorService(duckDb, serverManager, new ScheduleManager(_configDir));
        return new Rig(service, server, RemoteCollectorService.GetServerId(server));
    }

    private static string ReadLf(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath)).Replace("\r\n", "\n");
    }
}
