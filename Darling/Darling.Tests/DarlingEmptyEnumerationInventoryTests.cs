/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1852, Darling half: "this collector has come back empty every run this week" is only interesting if
/// the target HAS anything to enumerate. #1837 shipped the persistence half of that sentence ("all 96
/// runs") off the collection_log aggregate the grid already read; the other half needs a signal that
/// aggregate does not carry — whether the store has seen user databases on this server at all.
///
/// <para>Lite.Tests (<c>EmptyEnumerationInventoryTests</c>) pins the identical expectations against live
/// DuckDB on the same query shape; parity is the point, and the inventory subquery is the first
/// cross-collector read either health query has ever done, so "it matches on one engine" is not evidence
/// it matches on the other. Run against a REAL store through BOTH Postgres readers — the Viewer's and the
/// MCP service's — and gated on DARLING_TEST_PG like every other live class.</para>
///
/// <para>The qualifier is informational and stays informational: no new status, no new band, and
/// <c>Classify</c> never sees the flag. A target with no user databases keeps reading HEALTHY, which is
/// #1837's non-negotiable constraint, re-asserted here against the new input.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingEmptyEnumerationInventoryTests
{
    private const int ServerId = -949552;
    private const string ServerName = "empty-enum-inventory";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* ── the two shared contracts the qualifier rests on ── */

    [Fact]
    public void The_Marker_Is_The_Head_Of_The_Shared_Empty_Enumeration_Message()
    {
        /* The classifier has to tell the empty-enumeration note apart from the probe-failure one, and it
           lives in PerformanceMonitor.Common, which deliberately does not reference
           PerformanceMonitor.Collectors — the same boundary that keeps the on-load collector NAMES an
           explicit set there rather than a catalog lookup. So the marker is one duplicated substring, and
           this is what makes the duplication safe: reword the driver's message alone and this fails.
           Lite.Tests pins the identical pair. */
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
           entry there would quietly never be qualified; one that stops enumerating would carry a mapping
           for a note it can no longer produce. Same shape as the on-load set's pin against the schedule
           table. Probed on an ON-PREM target: query_store and index_object_stats deliberately return no
           enumeration on Azure SQL DB, where the collector runs per-database instead. */
        var onPrem = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
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

    /* ── live Postgres: the inventory read itself, through BOTH readers ── */

    [Fact]
    public async Task BothReaders_QualifyOnlyWhenTheStoreHasUserDatabases_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live inventory-qualifier test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Two database enumerators that came back empty every run, and one collector that does not
               enumerate at all — the inventory flag is a property of the SERVER, so it reaches every
               row here and only the mapping keeps it off wait_stats's note. */
            await SeedLogAsync(connection, ct, "query_store", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
            await SeedLogAsync(connection, ct, "query_store", MinutesAgo(20), EnumeratedCollectorDriver.EmptyEnumerationMessage);
            await SeedLogAsync(connection, ct, "index_object_stats", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
            await SeedLogAsync(connection, ct, "wait_stats", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);

            /* System databases and a user one. database_size_stats takes EVERY online database, so a bare
               row check would be true on every server alive; database_id > 4 is what makes the flag an
               answer about USER databases. Seeded together so the screen is exercised, not assumed. */
            await SeedSizeAsync(connection, ct, "master", 1, MinutesAgo(25));
            await SeedSizeAsync(connection, ct, "tempdb", 2, MinutesAgo(25));
            await SeedSizeAsync(connection, ct, "AdventureWorks", 7, MinutesAgo(25));

            var qualified = EnumeratedCollectorDriver.EmptyEnumerationMessage
                + " (all 2 runs, " + CollectorHealthClassifier.HasUserDatabasesQualifier + ")";

            /* ── the Viewer's read (the Collection Health grid) ── */
            await using (var viewer = new ViewerDataService(cs!))
            {
                var health = await viewer.GetCollectionHealthAsync(ServerId, ct);

                var queryStore = health.Single(h => h.CollectorName == "query_store");
                Assert.True(queryStore.TargetHasUserDatabases);
                Assert.Equal(qualified, queryStore.NoteFormatted);

                /* A second mapped enumerator, so the qualifier is not accidentally keyed to one name. */
                var indexObject = health.Single(h => h.CollectorName == "index_object_stats");
                Assert.Contains(CollectorHealthClassifier.HasUserDatabasesQualifier, indexObject.NoteFormatted, StringComparison.Ordinal);

                var waitStats = health.Single(h => h.CollectorName == "wait_stats");
                Assert.True(waitStats.TargetHasUserDatabases);
                Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)", waitStats.NoteFormatted);
            }

            /* ── the MCP service's read (get_collection_health, and the web dashboard's table behind it) ── */
            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var health = await DarlingDataReader.GetCollectionHealthAsync(
                    postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct);

                var queryStore = health.Single(h => h.CollectorName == "query_store");
                Assert.True(queryStore.TargetHasUserDatabases);
                Assert.Equal(
                    qualified,
                    CollectorHealthClassifier.FormatCollectionNote(
                        queryStore.LastNote, queryStore.NoteCount, queryStore.TotalRuns,
                        queryStore.CollectorName, queryStore.TargetHasUserDatabases));

                /* The unmapped collector reads the same flag off the same row and still renders plain. */
                var waitStats = health.Single(h => h.CollectorName == "wait_stats");
                Assert.True(waitStats.TargetHasUserDatabases);
                Assert.Equal(
                    EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)",
                    CollectorHealthClassifier.FormatCollectionNote(
                        waitStats.LastNote, waitStats.NoteCount, waitStats.TotalRuns,
                        waitStats.CollectorName, waitStats.TargetHasUserDatabases));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task AnInventoryOutsideTheWindowAnotherServersOrSystemOnlySaysNothing_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live inventory-staleness test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await SeedLogAsync(connection, ct, "query_store", MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);

            /* Three ways the store can hold database_size_stats rows that say nothing about THIS read:
               - a USER database aged past the seven days the grid summarizes, so it is not evidence
                 about the target today and a qualifier built on it would be an assertion the store can
                 no longer support;
               - a user database belonging to ANOTHER server, which a cross-collector join is exactly
                 the kind of change that could leak;
               - the target's own SYSTEM databases, in window and its own. The size collector takes every
                 ONLINE database, so without the database_id > 4 screen this row alone would make the
                 flag true on every server alive and the qualifier would mean nothing. */
            await SeedSizeAsync(connection, ct, "AgedOut", 7, DateTime.UtcNow.AddDays(-8));
            await SeedSizeAsync(connection, ct, "SomeoneElses", 8, MinutesAgo(25), serverId: ServerId - 1);
            await SeedSizeAsync(connection, ct, "master", 1, MinutesAgo(25));
            await SeedSizeAsync(connection, ct, "tempdb", 2, MinutesAgo(25));
            await SeedSizeAsync(connection, ct, "model", 3, MinutesAgo(25));
            await SeedSizeAsync(connection, ct, "msdb", 4, MinutesAgo(25));

            await using (var viewer = new ViewerDataService(cs!))
            {
                var queryStore = (await viewer.GetCollectionHealthAsync(ServerId, ct))
                    .Single(h => h.CollectorName == "query_store");

                Assert.False(queryStore.TargetHasUserDatabases);

                /* Byte-identical to what #1837 shipped: the absence of a signal is not a finding. */
                Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 1 runs)", queryStore.NoteFormatted);
            }

            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var health = await DarlingDataReader.GetCollectionHealthAsync(
                    postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct);

                Assert.False(health.Single(h => h.CollectorName == "query_store").TargetHasUserDatabases);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task TheFleetReadStillCarriesNoInventory_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-inventory test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* The fleet read is scoped to the CONFIG registry, not the collector-side servers table. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            await SeedLogAsync(connection, ct, FleetProbeCollector, MinutesAgo(30), EnumeratedCollectorDriver.EmptyEnumerationMessage);
            await SeedSizeAsync(connection, ct, "AdventureWorks", 7, MinutesAgo(25));

            /* #1863's decision, extended: the fleet rollup projects the ordinal and NOT the value. It
               groups server_id INTO the result, so there is no single server to probe an inventory for,
               and its only caller counts bands — with last_note already NULL the formatter returns blank
               whatever the flag says. A real value here would be a second cross-collector join across
               every enabled server on a query the status bar re-runs on every aggregate-tab refresh,
               which is the cost #1855 measured and declined. Asserted against live data that WOULD
               qualify on the per-server read, so a fleet read that quietly grew the join fails here. */
            await using var viewer = new ViewerDataService(cs!);

            var fleet = (await viewer.GetFleetCollectionHealthAsync(ct))
                .Where(h => h.CollectorName == FleetProbeCollector)
                .ToList();

            var probe = Assert.Single(fleet);
            Assert.False(probe.TargetHasUserDatabases);
            Assert.Null(probe.LastNote);
            Assert.Equal(string.Empty, probe.NoteFormatted);

            /* The per-server read on the SAME rows does carry it — otherwise the assertion above would
               be satisfied by an inventory subquery that never matches anything. */
            var perServer = (await viewer.GetCollectionHealthAsync(ServerId, ct))
                .Single(h => h.CollectorName == FleetProbeCollector);
            Assert.True(perServer.TargetHasUserDatabases);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* ── helpers ── */

    /// <summary>
    /// The fleet read rolls up EVERY enabled server and its rows carry no server_id, so the fleet
    /// assertions scope themselves by a collector name no real install has — the sentinel-server
    /// discipline applied to the only key that projection exposes.
    /// </summary>
    private const string FleetProbeCollector = "inventory_fleet_probe";

    private static string NoteOf(string kind) => kind switch
    {
        "empty" => EnumeratedCollectorDriver.EmptyEnumerationMessage,
        "probe" => EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 12)!,
        "both" => EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: true, probeFailureCount: 12)!,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown note kind"),
    };

    /// <summary>Whole seconds so the assertions can compare against what Postgres stored.</summary>
    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SeedLogAsync(
        NpgsqlConnection connection, CancellationToken ct,
        string collector, DateTime collectionTimeUtc, string? message) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, "SUCCESS", message, 0, 80, 20);

    private static async Task SeedSizeAsync(
        NpgsqlConnection connection, CancellationToken ct,
        string databaseName, int databaseId, DateTime collectionTimeUtc, int? serverId = null) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), serverId ?? ServerId, ServerName,
            databaseName, databaseId, 1, "ROWS", databaseName + "_data", "C:\\data\\" + databaseName + ".mdf", 128.00m);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM database_size_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM database_size_stats WHERE server_id = $1", ServerId - 1);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
