/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5245 (part of #5244), PR2 lane R: the database filter on the two object-stats reads, get_object_locking and
/// get_index_usage. Each has an internal overload that takes a <see cref="DatabaseFilter"/>, with the list predicate
/// (<c>$2::text[] IS NULL OR database_name = ANY($2)</c>) in the row statement (and in the match-count statement of
/// get_index_usage), so an overload call with [A, B] over a seed of A, B and C returns only A's and B's rows. The
/// public methods still pass <see cref="DatabaseFilter.All"/> (get_object_locking) or one name (get_index_usage), so
/// there is no MCP schema, dispatch or tools/list change.
///
/// <para><b>What each test pins.</b> The page is the top N of the CHOSEN databases (the cap comes after the filter);
/// the snapshot anchor stays the server's newest capture (a filtered call never reads an older capture that a chosen
/// database was last seen in); and every one-name consumer on the empty, unavailable and truncated paths is list-aware
/// (#5245 M4): the empty sentences, the truncation notes, the optimized-locking note (only the chosen databases' flags
/// count), the Azure master "separately monitored" note (kept only when a chosen database is one of them), and the
/// echoed <c>database_name</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ObjectStatsDatabaseFilterLiveTests
{
    private const string A = "ObjFilterA";
    private const string B = "ObjFilterB";
    private const string C = "ObjFilterC";

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    private static List<string> Databases(IEnumerable<string> names) =>
        names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();

    private static List<string> JsonDatabases(JsonElement root, string property) =>
        Databases(root.GetProperty(property).EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!));

    /// <summary>
    /// One index_object_stats row. <paramref name="waitMs"/> is the row-lock wait (the contention the locking read
    /// ranks and filters on); <paramref name="reads"/> are the seeks the usage read classifies Unused (0) or Active by.
    /// </summary>
    private static Task PlantIndexAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture,
        string db, string index, long waitMs, long reads, decimal reservedMb = 100m) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                  row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms, user_seeks, user_scans, user_lookups, user_updates)
              VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,1,$8,'NONCLUSTERED',$9,$9,1000, 1,$10, 0,0, 0, 0,0, $11,0,0,0)",
            CollectionIdGenerator.Next(), capture, serverId, serverName, db, 1000 + index.Length, "T_" + index, index, reservedMb, waitMs, reads);

    private static Task PlantConfigAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture, string db, bool optimizedLocking) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level, is_optimized_locking_on)
              VALUES ($1,$2,$3,$4,$5,'ONLINE',170,$6)",
            CollectionIdGenerator.Next(), capture, serverId, serverName, db, optimizedLocking);

    /// <summary>The three-database seed: C is the most contended and the largest unused index, A and B are smaller.</summary>
    private static async Task SeedThreeAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture)
    {
        await PlantIndexAsync(connection, ct, serverId, serverName, capture, A, "IX_a1", waitMs: 300, reads: 0, reservedMb: 10m);
        await PlantIndexAsync(connection, ct, serverId, serverName, capture, A, "IX_a2", waitMs: 200, reads: 50, reservedMb: 20m);
        await PlantIndexAsync(connection, ct, serverId, serverName, capture, B, "IX_b1", waitMs: 100, reads: 0, reservedMb: 30m);
        await PlantIndexAsync(connection, ct, serverId, serverName, capture, C, "IX_c1", waitMs: 9_000, reads: 0, reservedMb: 900m);
        await PlantIndexAsync(connection, ct, serverId, serverName, capture, C, "IX_c2", waitMs: 8_000, reads: 0, reservedMb: 800m);
    }

    private const string Cleanup =
        "DELETE FROM index_object_stats WHERE server_id = {0}; DELETE FROM database_config WHERE server_id = {0}";

    // ---------------------------------------------------------------- get_object_locking

    [Fact]
    public Task ObjectLockingReader_AListOfDatabases_ReturnsOnlyThoseDatabases_AndTheCapAppliesAfterTheFilter() =>
        WithStoreAsync("obj-filter-locking-reader", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);

            var both = await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, Of(A, B), ct);
            Assert.Equal([A, B], Databases(both.Select(r => r.DatabaseName)));
            Assert.Equal(3, both.Count);

            /* Order of the names does not matter, and All is every database. */
            Assert.Equal(3, (await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, Of(B, A), ct)).Count);
            Assert.Equal([A, B, C], Databases((await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, DatabaseFilter.All, ct)).Select(r => r.DatabaseName)));
            Assert.Equal([A, B, C], Databases((await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, ct)).Select(r => r.DatabaseName)));

            /* One name: the same rows as a list of one. */
            Assert.Equal([B], Databases((await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, DatabaseFilter.One(B), ct)).Select(r => r.DatabaseName)));

            /* The cap comes AFTER the filter: unfiltered, the top 1 is C's most contended index; filtered to A and B, the top 1 is
               A's, never an empty page because C's rows took the slot. */
            var unfilteredTop = await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 1, DatabaseFilter.All, ct);
            Assert.Equal(C, Assert.Single(unfilteredTop).DatabaseName);
            var filteredTop = await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 1, Of(A, B), ct);
            Assert.Equal(A, Assert.Single(filteredTop).DatabaseName);

            /* A name that no row carries matches nothing, however it is cased. */
            Assert.Empty(await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, Of(A.ToUpperInvariant(), "Nope"), ct));
        }, Cleanup);

    [Fact]
    public Task ObjectLockingReader_TheSnapshotAnchorStaysServerWide() =>
        WithStoreAsync("obj-filter-locking-anchor", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            /* An older capture holds A and B; the newest holds only C. A filter on [A, B] must NOT slide back to the older
               capture A and B were last seen in: it reads the server's newest capture, finds none of theirs, and is empty. */
            await PlantIndexAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, "IX_old_a", waitMs: 500, reads: 0);
            await PlantIndexAsync(connection, ct, serverId, serverName, now.AddHours(-1), B, "IX_old_b", waitMs: 400, reads: 0);
            await PlantIndexAsync(connection, ct, serverId, serverName, now, C, "IX_new_c", waitMs: 100, reads: 0);

            Assert.Empty(await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, Of(A, B), ct));
            Assert.Equal(C, Assert.Single(await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, serverId, 50, DatabaseFilter.All, ct)).DatabaseName);

            /* The tool: the same capture stamps filtered and unfiltered pages. */
            var all = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: Of(C))).RootElement;
            var unfiltered = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName)).RootElement;
            Assert.Equal(unfiltered.GetProperty("captured_at").GetString(), all.GetProperty("captured_at").GetString());
        }, Cleanup);

    [Fact]
    public Task ObjectLockingTool_AListOfDatabases_PagesOverTheChosenDatabases_AndTheNotesNameTheFilter() =>
        WithStoreAsync("obj-filter-locking-tool", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);

            var both = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: Of(A, B))).RootElement;
            Assert.Equal([A, B], JsonDatabases(both, "objects"));
            Assert.Equal(3, both.GetProperty("objects_returned").GetInt32());
            Assert.False(both.GetProperty("truncated").GetBoolean());
            Assert.Equal("Complete: every index with lock/latch contention in the chosen databases at the latest snapshot is included.", both.GetProperty("note").GetString());

            var one = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: DatabaseFilter.One(B))).RootElement;
            Assert.Equal([B], JsonDatabases(one, "objects"));
            Assert.Equal($"Complete: every index with lock/latch contention in database '{B}' at the latest snapshot is included.", one.GetProperty("note").GetString());

            /* Truncated: the page is the top of the CHOSEN databases, and the note says which. */
            var capped = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 2, null, null, ct, databases: Of(A, B))).RootElement;
            Assert.True(capped.GetProperty("truncated").GetBoolean());
            Assert.Equal(2, capped.GetProperty("objects").GetArrayLength());
            Assert.DoesNotContain(C, JsonDatabases(capped, "objects"));
            Assert.Contains("lock/latch contention in the chosen databases at the latest snapshot", capped.GetProperty("note").GetString(), StringComparison.Ordinal);

            /* Unfiltered and the public method are unchanged: every database, today's words. */
            var every = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName)).RootElement;
            Assert.Equal([A, B, C], JsonDatabases(every, "objects"));
            Assert.Equal("Complete: every index with lock/latch contention at the latest snapshot is included.", every.GetProperty("note").GetString());
        }, Cleanup);

    [Fact]
    public Task ObjectLockingTool_AnEmptyFilteredRead_IsEmptyWhenTheServerHasContentionElsewhere_AndUnavailableWhenItHasNone() =>
        WithStoreAsync("obj-filter-locking-empty", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            /* No row at all: unfiltered and filtered both say there is no data, and the filtered one names its scope. */
            var bareUnfiltered = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName)).RootElement;
            var bareFiltered = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: Of(A, B))).RootElement;
            Assert.Equal(bareUnfiltered.GetProperty("status").GetString(), bareFiltered.GetProperty("status").GetString());
            Assert.Equal("unavailable", bareFiltered.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases, or for any other database on this server", bareFiltered.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain("chosen databases", bareUnfiltered.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* Contention exists, but only in C: a filter on A and B looked and found nothing, which is "empty", not "unavailable". */
            await PlantIndexAsync(connection, ct, serverId, serverName, now, C, "IX_c1", waitMs: 900, reads: 0);

            var many = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: Of(A, B))).RootElement;
            Assert.Equal("empty", many.GetProperty("status").GetString());
            var manyMessage = many.GetProperty("message").GetString()!;
            Assert.Contains("No lock/latch contention in the chosen databases", manyMessage, StringComparison.Ordinal);
            Assert.Contains("other databases on the server have some", manyMessage, StringComparison.Ordinal);

            var single = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: DatabaseFilter.One(A))).RootElement;
            Assert.Equal("empty", single.GetProperty("status").GetString());
            Assert.Contains($"No lock/latch contention in database '{A}'", single.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* The unfiltered read still finds C. */
            Assert.Equal([C], JsonDatabases(JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName)).RootElement, "objects"));
        }, Cleanup);

    [Fact]
    public Task ObjectLockingTool_TheOptimizedLockingNote_CountsOnlyTheChosenDatabases() =>
        WithStoreAsync("obj-filter-locking-optimized", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);
            await PlantConfigAsync(connection, ct, serverId, serverName, now, A, optimizedLocking: false);
            await PlantConfigAsync(connection, ct, serverId, serverName, now, B, optimizedLocking: false);
            await PlantConfigAsync(connection, ct, serverId, serverName, now, C, optimizedLocking: true);

            string? Note(JsonElement root) => root.GetProperty("optimized_locking_note").ValueKind == JsonValueKind.Null
                ? null : root.GetProperty("optimized_locking_note").GetString();

            var every = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName)).RootElement;
            Assert.Equal(OptimizedLockingNote.Text, Note(every));

            /* A and B have it off: a page of only A and B does not carry a warning about C's database. */
            var ab = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: Of(A, B))).RootElement;
            Assert.Null(Note(ab));

            var ac = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, null, null, ct, databases: Of(A, C))).RootElement;
            Assert.Equal(OptimizedLockingNote.Text, Note(ac));

            /* The reader the note comes from: the anchor is the server's capture, only the databases counted change. */
            Assert.Null(await DarlingObjectStatsReader.GetOptimizedLockingNoteAsync(postgres, serverId, Of(A, B), ct));
            Assert.Equal(OptimizedLockingNote.Text, await DarlingObjectStatsReader.GetOptimizedLockingNoteAsync(postgres, serverId, DatabaseFilter.All, ct));
            Assert.Equal(OptimizedLockingNote.Text, await DarlingObjectStatsReader.GetOptimizedLockingNoteAsync(postgres, serverId, ct));
        }, Cleanup);

    [Fact]
    public Task ObjectLockingTool_TheSeparatelyMonitoredNote_IsKeptOnlyWhenAChosenDatabaseIsOneOfThem() =>
        WithStoreAsync("obj-filter-locking-separate", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);
            var state = new MonitoredServerRegistryState();
            Task<IReadOnlyList<string>?> Resolver(int id, CancellationToken token) => Task.FromResult<IReadOnlyList<string>?>([C.ToLowerInvariant()]);

            async Task<string?> NoteFor(DatabaseFilter databases)
            {
                var root = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingCoreAsync(postgres, serverName, 50, state, Resolver, ct, databases)).RootElement;
                var note = root.GetProperty("separately_monitored_note");
                return note.ValueKind == JsonValueKind.Null ? null : note.GetString();
            }

            Assert.Equal(AzureMasterScope.SeparatelyMonitoredListNote, await NoteFor(DatabaseFilter.All));
            /* The page holds only A and B, none of the separately monitored databases: the note's claim would be false. */
            Assert.Null(await NoteFor(Of(A, B)));
            /* C is one of them (a differently cased spelling matches, as the SQL names compare there). */
            Assert.Equal(AzureMasterScope.SeparatelyMonitoredListNote, await NoteFor(Of(A, C)));
        }, Cleanup);

    // ---------------------------------------------------------------- get_index_usage

    [Fact]
    public Task IndexUsageReader_AListOfDatabases_ReturnsOnlyThoseDatabases_AndTheCountAndTheCapFollowTheFilter() =>
        WithStoreAsync("obj-filter-usage-reader", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);

            var both = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, serverId, 50, Of(A, B), ct);
            Assert.Equal([A, B], Databases(both.Select(r => r.DatabaseName)));
            Assert.Equal(3, both.Count);
            Assert.Equal(3, await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, serverId, Of(A, B), ct));

            Assert.Equal(5, await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, serverId, DatabaseFilter.All, ct));
            Assert.Equal(5, (await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, serverId, 50, DatabaseFilter.All, ct)).Count);

            /* One name: the list of one, the old string overload and the named-argument form all agree. */
            var oneFilter = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, serverId, 50, DatabaseFilter.One(B), ct);
            var oneName = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, serverId, 50, databaseName: B, cancellationToken: ct);
            Assert.Equal(oneFilter.Select(r => r.IndexName), oneName.Select(r => r.IndexName));
            Assert.Equal(1, await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, serverId, DatabaseFilter.One(B), ct));
            Assert.Equal(1, await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, serverId, databaseName: B, cancellationToken: ct));

            /* The cap is after the filter: unfiltered, the unused-first, largest-first top 1 is C's; filtered it is the largest unused of A and B. */
            Assert.Equal(C, Assert.Single(await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, serverId, 1, DatabaseFilter.All, ct)).DatabaseName);
            Assert.Equal("IX_b1", Assert.Single(await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, serverId, 1, Of(A, B), ct)).IndexName);
        }, Cleanup);

    [Fact]
    public Task IndexUsageTool_AListOfDatabases_PagesOverTheChosenDatabases_AndTheSentencesNameTheFilter() =>
        WithStoreAsync("obj-filter-usage-tool", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);

            var both = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, Of(A, B), 50, ct)).RootElement;
            Assert.Equal([A, B], JsonDatabases(both, "indexes"));
            Assert.Equal(3, both.GetProperty("matching_index_count").GetInt64());
            Assert.Equal("the chosen databases", both.GetProperty("database_name").GetString());
            Assert.False(both.GetProperty("truncated").GetBoolean());

            /* Truncated: the count is the chosen databases' (3, not the server's 5) and the note does not tell a filtered
               caller to "pass database_name". */
            var capped = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, Of(A, B), 2, ct)).RootElement;
            Assert.True(capped.GetProperty("truncated").GetBoolean());
            Assert.Equal(3, capped.GetProperty("matching_index_count").GetInt64());
            var note = capped.GetProperty("note").GetString()!;
            Assert.Contains("3 indexes match the chosen databases and 2 were returned", note, StringComparison.Ordinal);
            Assert.DoesNotContain("pass database_name", note, StringComparison.Ordinal);
            Assert.DoesNotContain("whole server", note, StringComparison.Ordinal);

            /* The public one-name method: its echo and sentences are unchanged. */
            var one = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, database_name: B, cancellationToken: ct)).RootElement;
            Assert.Equal(B, one.GetProperty("database_name").GetString());
            Assert.Equal([B], JsonDatabases(one, "indexes"));
            var every = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, cancellationToken: ct)).RootElement;
            Assert.Equal(JsonValueKind.Null, every.GetProperty("database_name").ValueKind);
            Assert.Equal(5, every.GetProperty("matching_index_count").GetInt64());
        }, Cleanup);

    [Fact]
    public Task IndexUsageTool_AnEmptyFilteredRead_IsEmptyWhenTheServerHasRowsElsewhere_AndKeepsTheUnfilteredAnswerWhenItHasNone() =>
        WithStoreAsync("obj-filter-usage-empty", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var bareUnfiltered = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, cancellationToken: ct)).RootElement;
            var bareFiltered = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, Of(A, B), 50, ct)).RootElement;
            Assert.Equal(bareUnfiltered.GetProperty("status").GetString(), bareFiltered.GetProperty("status").GetString());
            Assert.Equal("unavailable", bareFiltered.GetProperty("status").GetString());

            await PlantIndexAsync(connection, ct, serverId, serverName, now, C, "IX_c1", waitMs: 0, reads: 5);
            await PlantIndexAsync(connection, ct, serverId, serverName, now, C, "IX_c2", waitMs: 0, reads: 5);

            var many = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, Of(A, B), 50, ct)).RootElement;
            Assert.Equal("empty", many.GetProperty("status").GetString());
            var manyMessage = many.GetProperty("message").GetString()!;
            Assert.Contains("No index usage rows for the chosen databases on", manyMessage, StringComparison.Ordinal);
            Assert.Contains("the server has 2 across its other databases", manyMessage, StringComparison.Ordinal);
            Assert.Contains("Check the database names against get_database_sizes", manyMessage, StringComparison.Ordinal);

            var single = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, serverName, database_name: A, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", single.GetProperty("status").GetString());
            var singleMessage = single.GetProperty("message").GetString()!;
            Assert.Contains($"No index usage rows for database '{A}' on", singleMessage, StringComparison.Ordinal);
            Assert.Contains("Check the database name against get_database_sizes", singleMessage, StringComparison.Ordinal);
        }, Cleanup);

    // ---------------------------------------------------------------- #5231 PR2 lane W2: the web dispatch and the MCP parameter

    /// <summary>A read through the web dispatch, one <c>database_name</c> key per chosen database.</summary>
    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string read, string serverName, params string[] databases)
    {
        var query = new List<KeyValuePair<string, string?>> { new("server_name", serverName) };
        query.AddRange(databases.Select(d => new KeyValuePair<string, string?>("database_name", d)));
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    [Theory]
    [InlineData("get_object_locking", "objects")]
    [InlineData("get_index_usage", "indexes")]
    public Task WebDispatch_TwoRepeatedDatabaseNameKeys_ReturnOnlyThoseDatabasesRows(string read, string property) =>
        WithStoreAsync("obj-filter-web-" + read, async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);

            /* RED on base: the dispatch ignored the keys (get_object_locking passed All; get_index_usage read one name). */
            var two = JsonDocument.Parse(await WebReadAsync(postgres, read, serverName, A, B)).RootElement;
            Assert.Equal([A, B], JsonDatabases(two, property));

            var one = JsonDocument.Parse(await WebReadAsync(postgres, read, serverName, C)).RootElement;
            Assert.Equal([C], JsonDatabases(one, property));

            /* No key is every database; the same name twice is one database. */
            Assert.Equal([A, B, C], JsonDatabases(JsonDocument.Parse(await WebReadAsync(postgres, read, serverName)).RootElement, property));
            Assert.Equal([B], JsonDatabases(JsonDocument.Parse(await WebReadAsync(postgres, read, serverName, B, B)).RootElement, property));

            /* A blank-only list is refused, never widened to "all databases". */
            var refused = JsonDocument.Parse(await WebReadAsync(postgres, read, serverName, "  ", "")).RootElement;
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
        }, Cleanup);

    [Fact]
    public Task WebLockingDispatch_TheHeatBandsAreComputedOverTheFilteredRows_AndTheDetailSelectorIgnoresTheFilter() =>
        WithStoreAsync("obj-filter-web-heat", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);

            /* C holds the largest waits. Unfiltered, A and B's small waits band low; filtered to A and B, the largest of THEM
               takes a higher band, so the bands moved with the filter. */
            int TopBand(JsonElement root, string database) => root.GetProperty("objects").EnumerateArray()
                .Where(r => r.GetProperty("database_name").GetString() == database)
                .Max(r => r.GetProperty("heat")[0].GetInt32());
            var unfiltered = JsonDocument.Parse(await WebReadAsync(postgres, "get_object_locking", serverName)).RootElement;
            var filtered = JsonDocument.Parse(await WebReadAsync(postgres, "get_object_locking", serverName, A, B)).RootElement;
            Assert.True(TopBand(filtered, A) > TopBand(unfiltered, A),
                $"A's top row-lock band was {TopBand(unfiltered, A)} over every database and {TopBand(filtered, A)} over A and B");

            /* The detail read names its own database: a database_name key beside it is ignored. */
            var query = new List<KeyValuePair<string, string?>>
            {
                new("server_name", serverName), new("database_name", B),
                new("detail_database", A), new("detail_schema", "dbo"), new("detail_table", "T_IX_a1"), new("detail_index", "IX_a1"),
            };
            var context = new DefaultHttpContext();
            context.Request.QueryString = QueryString.Create(query);
            var detail = JsonDocument.Parse(await DarlingWebEndpoints.BuildReadDispatch()["get_object_locking"](context, postgres, null!)).RootElement;
            Assert.Equal(A, detail.GetProperty("detail").GetProperty("database_name").GetString());
        }, Cleanup);

    [Fact]
    public Task ObjectLockingTool_DatabaseName_LimitsTheAnswerToThatDatabase_BlankMeansAll() =>
        WithStoreAsync("obj-filter-locking-param", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedThreeAsync(connection, ct, serverId, serverName, now);

            var one = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName, database_name: A, cancellationToken: ct)).RootElement;
            Assert.Equal([A], JsonDatabases(one, "objects"));
            Assert.Equal(2, one.GetProperty("objects").GetArrayLength());

            foreach (var blank in new string?[] { null, "", "   " })
            {
                var all = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName, database_name: blank, cancellationToken: ct)).RootElement;
                Assert.Equal([A, B, C], JsonDatabases(all, "objects"));
            }

            /* The positional shape the tool has always had still means "every database". */
            Assert.Equal([A, B, C], JsonDatabases(JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName, 75)).RootElement, "objects"));
        }, Cleanup);

    private static async Task WithStoreAsync(
        string serverName, Func<NpgsqlConnection, NpgsqlDataSource, int, string, DateTime, CancellationToken, Task> body, string cleanupSql)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live object-stats database filter tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var cleanup = string.Format(System.Globalization.CultureInfo.InvariantCulture, cleanupSql, serverId)
                      + string.Format(System.Globalization.CultureInfo.InvariantCulture, "; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
            await body(connection, postgres, serverId, serverName, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-1), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}
