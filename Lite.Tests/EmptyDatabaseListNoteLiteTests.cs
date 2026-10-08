/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
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
    private readonly List<DuckDbInitializer> _initializers = [];

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
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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
        var fixture = await BuildFixtureAsync(registeredDatabases: new[] { "alpha", "zeta" }, excludedDatabases: Array.Empty<string>());
        fixture.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha", "zeta" });

        var written = await fixture.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Equal(EmptyDatabaseListNote.EverySeparatelyMonitored, fixture.Service.TelemetryFor(fixture.ServerId).Note);
    }

    /* A real logical server lists master beside its user databases, and the long-query trace never keeps a session
       in master (LongQueryTraceDatabases), so a list of master plus user databases that are all registered
       separately has nothing left to read either. The tests above list user databases only. */

    [Fact]
    public async Task EveryUserDatabaseMonitoredAsItsOwnServer_WithMasterListed_RecordsTheRunWithItsNote()
    {
        var fixture = await BuildFixtureAsync(registeredDatabases: new[] { "alpha", "zeta" }, excludedDatabases: Array.Empty<string>());
        var read = ListThenReadNothing(fixture, "master", "alpha", "zeta");

        var written = await fixture.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Equal(EmptyDatabaseListNote.EverySeparatelyMonitored, fixture.Service.TelemetryFor(fixture.ServerId).Note);
        Assert.Empty(read);
    }

    [Fact]
    public async Task OnlyMasterListed_WithEveryUserDatabaseExcluded_RecordsTheExcludedNote_AndReadsNothing()
    {
        var fixture = await BuildFixtureAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: new[] { "alpha", "zeta" });
        var read = ListThenReadNothing(fixture, "master");

        var written = await fixture.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Equal(EmptyDatabaseListNote.EveryExcluded, fixture.Service.TelemetryFor(fixture.ServerId).Note);
        Assert.Empty(read);
    }

    [Fact]
    public async Task OnlyMasterListed_WithNothingExcluded_StaysWithoutANote_AndReadsNothing()
    {
        /* No user database at all: nothing is monitored separately and nothing is excluded, so no note can name a reason. */
        var fixture = await BuildFixtureAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: Array.Empty<string>());
        var read = ListThenReadNothing(fixture, "master");

        var written = await fixture.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Null(fixture.Service.TelemetryFor(fixture.ServerId).Note);
        Assert.Empty(read);
    }

    [Fact]
    public async Task AUserDatabaseNotMonitoredSeparately_IsStillRead_WithMasterListed()
    {
        var fixture = await BuildFixtureAsync(registeredDatabases: new[] { "alpha" }, excludedDatabases: Array.Empty<string>());
        var read = ListThenReadNothing(fixture, "master", "alpha", "zeta");

        await fixture.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(new[] { "zeta" }, read);
        Assert.Null(fixture.Service.TelemetryFor(fixture.ServerId).Note);
    }

    /// <summary>
    /// Guard, not RED first: only the long-query read leaves master out. Every other per-database collector still reads
    /// every database the server lists, master included, and a collector that skips nothing never gets this note.
    /// </summary>
    [Fact]
    public async Task ACollectorThatSkipsNothing_StillReadsMaster_AndTheSeparatelyMonitoredDatabases()
    {
        var fixture = await BuildFixtureAsync(registeredDatabases: new[] { "alpha", "zeta" }, excludedDatabases: Array.Empty<string>());
        var read = ListThenReadNothing(fixture, "master", "alpha", "zeta");

        await fixture.Service.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(new[] { "master", "alpha", "zeta" }, read);
        Assert.Null(fixture.Service.TelemetryFor(fixture.ServerId).Note);
    }

    [Fact]
    public async Task EveryDatabaseExcluded_RecordsTheRunWithItsNote()
    {
        var fixture = await BuildFixtureAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: new[] { "alpha", "zeta" });
        fixture.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string>());

        var written = await fixture.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Equal(EmptyDatabaseListNote.EveryExcluded, fixture.Service.TelemetryFor(fixture.ServerId).Note);
    }

    [Fact]
    public async Task EveryDatabaseExcluded_NotesACollectorThatReadsEveryDatabaseToo()
    {
        var fixture = await BuildFixtureAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: new[] { "alpha", "zeta" });
        fixture.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string>());

        await fixture.Service.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(EmptyDatabaseListNote.EveryExcluded, fixture.Service.TelemetryFor(fixture.ServerId).Note);
    }

    [Fact]
    public async Task AnEmptyListWithNoExclusions_StaysWithoutANote()
    {
        var fixture = await BuildFixtureAsync(registeredDatabases: Array.Empty<string>(), excludedDatabases: Array.Empty<string>());
        fixture.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string>());

        var written = await fixture.Service.RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, fixture.Server, CancellationToken.None);

        Assert.Equal(0, written);
        Assert.Null(fixture.Service.TelemetryFor(fixture.ServerId).Note);
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

    private sealed record Fixture(RemoteCollectorService Service, ServerConnection Server, int ServerId);

    /// <summary>
    /// The server lists exactly these databases, and each database the run opens is recorded and read as an empty
    /// result, so a run never needs a connection.
    /// </summary>
    private static List<string> ListThenReadNothing(Fixture fixture, params string[] listed)
    {
        var read = new List<string>();
        fixture.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(listed.ToList());
        fixture.Service.AzureDatabaseReaderOverrideForTests = (database, _) =>
        {
            read.Add(database);
            return new DataTable().CreateDataReader();
        };

        return read;
    }

    /// <summary>
    /// A logical-server registration (no database named) on the Azure SQL Database engine edition, so the
    /// definition takes the per-database loop, plus one registration per named database on the same host.
    /// </summary>
    private async Task<Fixture> BuildFixtureAsync(string[] registeredDatabases, string[] excludedDatabases)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        _initializers.Add(duckDb);
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

        /* The long-query read names this install's session, so a run that reaches a database needs an install id to build
           its query. The runs that read nothing never get that far. */
        var service = new RemoteCollectorService(
            duckDb, serverManager, new ScheduleManager(_configDir),
            installIdStore: new InstallIdStore(_configDir, "test-machine", null));
        return new Fixture(service, server, RemoteCollectorService.GetServerId(server));
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
