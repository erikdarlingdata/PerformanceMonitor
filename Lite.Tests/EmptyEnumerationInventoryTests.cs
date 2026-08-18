/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1852, Lite half: "this collector has come back empty every run this week" is only interesting if the
/// target HAS anything to enumerate. #1837 shipped the persistence half of that sentence ("all 96 runs")
/// off the collection_log aggregate the grid already read; the other half needs a signal that aggregate
/// does not carry — whether the store has seen user databases on this server at all.
///
/// <para>The qualifier is informational and stays informational: it changes the DISPLAYED note text and
/// nothing else. No new status, no new band, and <c>Classify</c> never sees the flag — a target with no
/// user databases is legitimately empty and keeps reading HEALTHY, which is #1837's non-negotiable
/// constraint and is re-asserted here against the new input.</para>
///
/// <para>The live half runs against a REAL DuckDB through the real reader, because the whole change is a
/// cross-collector read: a source pin cannot tell an inventory subquery that matches from one that
/// silently never does. Darling.Tests pins the identical expectations against live Postgres on the same
/// query shape.</para>
/// </summary>
public sealed class EmptyEnumerationInventoryTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4343;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public EmptyEnumerationInventoryTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /* ── the two shared contracts the qualifier rests on ── */

    [Fact]
    public void The_Marker_Is_The_Head_Of_The_Shared_Empty_Enumeration_Message()
    {
        /* The classifier has to tell the empty-enumeration note apart from the probe-failure one, and it
           lives in PerformanceMonitor.Common, which deliberately does not reference
           PerformanceMonitor.Collectors — the same boundary that keeps the on-load collector NAMES an
           explicit set here rather than a catalog lookup. So the marker is one duplicated substring, and
           this is what makes the duplication safe: reword the driver's message alone and this fails.
           Darling.Tests pins the identical pair. */
        Assert.StartsWith(
            CollectorHealthClassifier.EmptyEnumerationMarker,
            EnumeratedCollectorDriver.EmptyEnumerationMessage,
            StringComparison.Ordinal);

        /* And the combined form the driver builds when an empty enumeration ALSO had probe failures still
           carries it, so that row is qualified too — it is an empty enumeration either way. */
        Assert.Contains(
            CollectorHealthClassifier.EmptyEnumerationMarker,
            EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: true, probeFailureCount: 4),
            StringComparison.Ordinal);
    }

    [Fact]
    public void The_Expectation_Set_Is_Exactly_The_Catalogs_Database_Enumerators()
    {
        /* ExpectsUserDatabases is a hand-kept name set, so it can only stay honest if something checks it
           against the collectors that actually enumerate. A collector that starts enumerating without an
           entry here would quietly never be qualified; one that stops enumerating would carry a mapping
           for a note it can no longer produce. Same shape as the on-load set's pin against the schedule
           table. Probed on an ON-PREM target: query_store and index_object_stats deliberately return no
           enumeration on Azure SQL DB, where the collector runs per-database instead. */
        var onPrem = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
        };

        /* Reflection because BuildEnumerationQuery lives on the GENERIC ICollectorDefinition<TRow> while
           the catalog is typed as the row-agnostic ICollectorSchemaInfo — there is no non-generic hook to
           ask "does this collector enumerate". The method lookup is asserted below, so a rename cannot
           quietly turn this pin into a comparison of two empty sets. */
        var enumerators = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var definition in CollectorCatalog.All)
        {
            var method = definition.GetType().GetMethod(
                nameof(CollectorDefinitionBase<object>.BuildEnumerationQuery),
                [typeof(CollectorContext)]);

            Assert.NotNull(method);
            if (method!.Invoke(definition, [onPrem]) is not null)
            {
                enumerators.Add(definition.Name);
            }
        }

        /* Named outright as well as compared, so the failure message names the drift rather than a set. */
        Assert.Equal(
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "query_store", "database_scoped_config", "index_object_stats", "plan_correction", "query_store_health" },
            enumerators);

        foreach (var name in CollectorCatalog.All.Select(c => c.Name))
        {
            Assert.Equal(enumerators.Contains(name), CollectorHealthClassifier.ExpectsUserDatabases(name));
        }
    }

    /* ── the four gates, on the shared formatter ── */

    [Theory]
    /* The case the issue exists for: every run of a database enumerator came back empty on a target the
       store has user databases for. Something is wrong and nothing else in the grid says so. */
    [InlineData("empty", 96L, 96L, "query_store", true, true)]
    /* No inventory to go on reads exactly like nothing to say. Silence over a false alarm: a server with
       genuinely no user databases is the ordinary install this must never nag. */
    [InlineData("empty", 96L, 96L, "query_store", false, false)]
    /* PERSISTENCE is a gate of its own. A sometimes-empty collector is normal for databases that go
       quiet, however much inventory the target has. */
    [InlineData("empty", 3L, 96L, "query_store", true, false)]
    /* A collector that does not enumerate user databases has no inventory to be surprised by. */
    [InlineData("empty", 96L, 96L, "wait_stats", true, false)]
    /* A probe-failure note already names its own cause — it says the enumeration REACHED items and could
       not check them, so "target has user databases" would only restate what was just read. */
    [InlineData("probe", 96L, 96L, "query_store", true, false)]
    /* Both at once IS an empty enumeration, so it qualifies. */
    [InlineData("both", 96L, 96L, "query_store", true, true)]
    /* No collector name at all — the three-argument call every pre-#1852 caller made, and what the fleet
       rollup still passes. Unmapped by construction, so never qualified. */
    [InlineData("empty", 96L, 96L, null, true, false)]
    public void Qualifier_Appears_Only_For_A_Persistent_Empty_Enumeration_On_A_Target_With_Databases(
        string noteKind, long noteCount, long totalRuns, string? collectorName, bool hasUserDatabases, bool expectQualified)
    {
        var note = NoteOf(noteKind);
        var formatted = CollectorHealthClassifier.FormatCollectionNote(
            note, noteCount, totalRuns, collectorName, hasUserDatabases);

        /* The note itself and the #1837 run qualifier are untouched in every case — this only ever ADDS. */
        Assert.StartsWith(note, formatted, StringComparison.Ordinal);
        Assert.Contains(
            noteCount >= totalRuns
                ? string.Format(CultureInfo.InvariantCulture, "(all {0} runs", totalRuns)
                : string.Format(CultureInfo.InvariantCulture, "({0} of {1} runs)", noteCount, totalRuns),
            formatted,
            StringComparison.Ordinal);

        Assert.Equal(expectQualified, formatted.Contains(CollectorHealthClassifier.HasUserDatabasesQualifier, StringComparison.Ordinal));

        /* The unqualified rendering must be BYTE-identical to what #1837 shipped, not merely similar —
           this text is what an operator greps a screenshot for. */
        if (!expectQualified)
        {
            Assert.Equal(CollectorHealthClassifier.FormatCollectionNote(note, noteCount, totalRuns), formatted);
        }
    }

    [Fact]
    public void The_Inventory_Never_Reaches_The_Banding()
    {
        /* #1837's constraint, re-asserted against #1852's new input: two collectors identical except for
           the note AND the inventory flag must band identically. The qualifier's whole premise is that a
           target with user databases and an empty enumeration is worth SAYING, not worth reddening —
           there are legitimate reasons (a filter, a feature nobody enabled) and a monitoring tool that
           cries wolf on them gets ignored. */
        var quiet = new CollectorHealthRow
        {
            CollectorName = "query_store",
            TotalRuns = 96,
            SuccessCount = 96,
            LastSuccessTime = DateTime.UtcNow.AddMinutes(-5),
            LastRunTime = DateTime.UtcNow.AddMinutes(-5),
        };
        var qualified = new CollectorHealthRow
        {
            CollectorName = "query_store",
            TotalRuns = 96,
            SuccessCount = 96,
            LastSuccessTime = quiet.LastSuccessTime,
            LastRunTime = quiet.LastRunTime,
            LastNote = EnumeratedCollectorDriver.EmptyEnumerationMessage,
            NoteCount = 96,
            TargetHasUserDatabases = true,
        };

        Assert.Equal(CollectorHealthClassifier.Healthy, quiet.HealthStatus);
        Assert.Equal(quiet.HealthStatus, qualified.HealthStatus);

        /* And the qualifier really is on the row that banded HEALTHY — otherwise this test would pass on
           an implementation that simply never qualified anything. */
        Assert.Contains(CollectorHealthClassifier.HasUserDatabasesQualifier, qualified.NoteFormatted, StringComparison.Ordinal);
    }

    /* ── live DuckDB: the inventory read itself ── */

    [Fact]
    public async Task A_Persistently_Empty_Enumeration_Is_Qualified_When_The_Store_Has_User_Databases()
    {
        var service = new LocalDataService(_duckDb);

        await SeedLogAsync("query_store", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
        await SeedLogAsync("query_store", MinutesAgo(20), EnumeratedCollectorDriver.EmptyEnumerationMessage);
        await SeedSizeAsync("AdventureWorks", databaseId: 7, MinutesAgo(25));

        var row = await ReadAsync(service, "query_store");

        Assert.True(row.TargetHasUserDatabases);
        Assert.Equal(
            EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 2 runs, " + CollectorHealthClassifier.HasUserDatabasesQualifier + ")",
            row.NoteFormatted);
    }

    [Fact]
    public async Task System_Databases_Are_Not_Inventory()
    {
        var service = new LocalDataService(_duckDb);

        /* database_size_stats takes EVERY online database, tempdb and the other system ones included, so
           a bare row check would be true on every server alive and the qualifier would mean nothing. The
           database_id > 4 screen is what makes it an answer about USER databases. */
        await SeedLogAsync("query_store", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
        await SeedSizeAsync("master", databaseId: 1, MinutesAgo(25));
        await SeedSizeAsync("tempdb", databaseId: 2, MinutesAgo(25));
        await SeedSizeAsync("model", databaseId: 3, MinutesAgo(25));
        await SeedSizeAsync("msdb", databaseId: 4, MinutesAgo(25));

        var row = await ReadAsync(service, "query_store");

        Assert.False(row.TargetHasUserDatabases);
        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)", row.NoteFormatted);
    }

    [Fact]
    public async Task An_Inventory_Older_Than_The_Health_Window_Says_Nothing()
    {
        var service = new LocalDataService(_duckDb);

        /* The staleness rule, and the reason it is the health read's OWN window rather than a second
           knob: an inventory that aged out of the seven days this grid summarizes is not evidence about
           what the target has TODAY, and a qualifier built on it would be an assertion the store can no
           longer support. Silence. */
        await SeedLogAsync("query_store", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
        await SeedSizeAsync("AdventureWorks", databaseId: 7, DateTime.UtcNow.AddDays(-8));

        var row = await ReadAsync(service, "query_store");

        Assert.False(row.TargetHasUserDatabases);
        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)", row.NoteFormatted);
    }

    [Fact]
    public async Task No_Inventory_At_All_Leaves_The_Note_Exactly_As_It_Read_Before()
    {
        var service = new LocalDataService(_duckDb);

        /* An install whose size collector is disabled, or a server whose first size collection has not
           run yet. The row must read BYTE-identically to what #1837 shipped rather than acquiring a
           hedge — the absence of a signal is not a finding. */
        await SeedLogAsync("query_store", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);

        var row = await ReadAsync(service, "query_store");

        Assert.False(row.TargetHasUserDatabases);
        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)", row.NoteFormatted);
    }

    [Fact]
    public async Task An_Unmapped_Collectors_Note_Is_Never_Qualified_However_Much_Inventory_There_Is()
    {
        var service = new LocalDataService(_duckDb);

        /* The inventory flag is a property of the SERVER, so every collector's row carries it — including
           collectors that enumerate nothing. Only the mapping keeps it off their notes. */
        await SeedLogAsync("wait_stats", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
        await SeedSizeAsync("AdventureWorks", databaseId: 7, MinutesAgo(25));

        var row = await ReadAsync(service, "wait_stats");

        Assert.True(row.TargetHasUserDatabases);
        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)", row.NoteFormatted);
    }

    [Fact]
    public async Task The_Inventory_Is_Scoped_To_The_Servers_Own_Row()
    {
        var service = new LocalDataService(_duckDb);

        /* A cross-collector join is a new chance to read another server's data. The size rows here belong
           to a DIFFERENT server, so this one still has no inventory. */
        await SeedLogAsync("query_store", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
        await SeedSizeAsync("AdventureWorks", databaseId: 7, MinutesAgo(25), serverId: ServerId + 1);

        var row = await ReadAsync(service, "query_store");

        Assert.False(row.TargetHasUserDatabases);
        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)", row.NoteFormatted);
    }

    /* ── helpers ── */

    private static string NoteOf(string kind) => kind switch
    {
        "empty" => EnumeratedCollectorDriver.EmptyEnumerationMessage,
        "probe" => EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 12)!,
        "both" => EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: true, probeFailureCount: 12)!,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown note kind"),
    };

    private async Task<CollectorHealthRow> ReadAsync(LocalDataService service, string collector) =>
        (await service.GetCollectionHealthAsync(ServerId)).Single(h => h.CollectorName == collector);

    /// <summary>Whole seconds, matching the sibling live suites' seeding convention.</summary>
    private static DateTime MinutesAgo(int minutes)
    {
        var t = DateTime.UtcNow.AddMinutes(-minutes);
        return new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);
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

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string? message) =>
        await ExecAsync(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            _nextId++, ServerId, "TestSrv", collector, collectionTimeUtc, 100, "SUCCESS", message, 0, 80, 20);

    private async Task SeedSizeAsync(string databaseName, int databaseId, DateTime collectionTimeUtc, int? serverId = null) =>
        await ExecAsync(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            _nextId++, collectionTimeUtc, serverId ?? ServerId, "TestSrv", databaseName, databaseId,
            1, "ROWS", databaseName + "_data", "C:\\data\\" + databaseName + ".mdf", 128.00m);
}
