/*
 * Copyright (c) 2026 Erik Darling, Darling Data\b LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643 on a\b live store: the schema\b version writer. A baseline writes baseline versions, an unchanged read writes nothing, a\b changed
/// definition closes the old version and opens a\b new one with current children, a\b recreated object keeps its history, a\b rename and a\b
/// drop are versions of their own, and a\b history key shared by two servers of one availability group is one history.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class SchemaVersionStoreLiveTests
{
    private const string Db = "SalesDb";
    private static readonly DateTime s_t0 = new(2026, 11, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly SchemaHistoryKey s_key = new(1, null, null);

    private static async Task WithStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the schema\b version store live tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static SchemaObjectDefinition Table(int id, string name, params string[] columns)
    {
        return new SchemaObjectDefinition(
            id, "dbo", name, 'U', s_t0,
            new SchemaTableProperties(null, null, false, null, null, null, "TABLE"), null,
            columns.Select((n, i) => new SchemaColumn(i + 1, n, "int", 4, 10, 0, false, i == 0 ? 1m : null, i == 0 ? 1m : null, null, null, null, null, null, null, null)).ToList(),
            new List<SchemaIndex> { new(1, "PK_Main", "CLUSTERED", true, true, false, new[] { columns[0] }, Array.Empty<string>(), null, "NONE", false) },
            new List<SchemaConstraint> { new('P', "PK_Main", null, Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false) },
            new List<SchemaTrigger>());
    }

    private static SchemaDatabaseWrite Write(
        IReadOnlyList<SchemaObjectDefinition> read, DateTime at, bool baseline = false, bool complete = true,
        IReadOnlyList<SchemaEvent>? attribution = null, SchemaHistoryKey? key = null, int server = 1, string by = "reconcile",
        IReadOnlySet<int>? targeted = null)
    {
        return new SchemaDatabaseWrite(
            key ?? s_key, server, Db, at, by, baseline, complete, read, attribution ?? Array.Empty<SchemaEvent>(),
            new SchemaCaptureStateRow { ServerId = server, DatabaseName = Db, LastCompleteReadAt = complete ? at : null, BaselineAt = baseline ? at : null })
        {
            TargetedIds = targeted
        };
    }

    private static SchemaEvent Evt(string name, long seq, DateTime at, int? objectId, int? type = 8277, int? indexId = null, string? login = "app_login")
    {
        return new SchemaEvent(name, seq, at, Db, objectId, type, indexId, null, login, "App-" + seq, "host-" + seq, null);
    }

    private static Task<string> Scalar(NpgsqlConnection c, string sql, CancellationToken ct) => ActionProposalStoreLiveTests.ScalarAsync(c, sql, ct);

    [Fact]
    public async Task Apply_BaselineRead_WritesBaselineVersions()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            var read = new[] { Table(10, "Orders", "Id", "Total"), Table(11, "Customers", "Id") };
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(read, s_t0, baseline: true, by: "baseline"), ct);

            Assert.Equal(new SchemaWriteCounts(2, 0), counts);
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'baseline' AND detected_by = 'baseline' AND valid_to IS NULL AND server_id = 1", ct));
            Assert.Equal("3", await Scalar(c, "SELECT count(*) FROM collect.schema_columns WHERE is_current", ct));
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_indexes WHERE is_current AND NOT is_unique_constraint AND NOT is_disabled AND key_columns = 'Id'", ct));
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_constraints WHERE is_current AND kind = 'P' AND NOT is_system_named", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_capture_state WHERE server_id = 1 AND database_name = 'SalesDb' AND baseline_at IS NOT NULL", ct));
            var current = await SchemaVersionStore.LoadCurrentAsync(c, s_key, Db, ct);
            Assert.Equal(new[] { 10, 11 }, current.Keys.OrderBy(k => k).ToArray());
        });
    }

    [Fact]
    public async Task Apply_SameReadTwice_WritesNothingTheSecondTime()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            var read = new[] { Table(10, "Orders", "Id", "Total") };
            await SchemaVersionStore.ApplyAsync(c, Write(read, s_t0, baseline: true, by: "baseline"), ct);
            var again = await SchemaVersionStore.ApplyAsync(c, Write(read, s_t0.AddHours(1)), ct);

            Assert.Equal(new SchemaWriteCounts(0, 0), again);
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects", ct));
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_columns", ct));
        });
    }

    [Fact]
    public async Task Apply_AlteredObject_ClosesOldAndOpensNew_WithChildrenCurrentFlags()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id", "Total") }, s_t0, baseline: true, by: "baseline"), ct);
            var later = s_t0.AddHours(2);
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id", "Total", "Note") }, later, by: "xe_event"), ct);

            Assert.Equal(new SchemaWriteCounts(1, 0), counts);
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE object_id = 10", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'baseline' AND valid_to = timestamp '2026-11-01 12:00:00'", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'altered' AND detected_by = 'xe_event' AND valid_to IS NULL AND valid_from = timestamp '2026-11-01 12:00:00'", ct));
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_columns sc JOIN collect.schema_objects o USING (version_id) WHERE NOT sc.is_current AND o.change_kind = 'baseline'", ct));
            Assert.Equal("3", await Scalar(c, "SELECT count(*) FROM collect.schema_columns sc JOIN collect.schema_objects o USING (version_id) WHERE sc.is_current AND o.change_kind = 'altered'", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_indexes WHERE is_current", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_constraints WHERE is_current", ct));
        });
    }

    [Fact]
    public async Task Apply_DropAndRecreateWithSameDefinition_WritesNothing()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id", "Total") }, s_t0, baseline: true, by: "baseline"), ct);
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(77, "Orders", "Id", "Total") }, s_t0.AddHours(1)), ct);

            Assert.Equal(new SchemaWriteCounts(0, 0), counts);
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects", ct));
            Assert.Equal("77", await Scalar(c, "SELECT object_id FROM collect.schema_objects WHERE valid_to IS NULL", ct));
            Assert.Equal("baseline", await Scalar(c, "SELECT change_kind FROM collect.schema_objects WHERE valid_to IS NULL", ct));
        });
    }

    [Fact]
    public async Task Apply_DropAndRecreateWithChangedDefinition_ContinuesTheHistoryAsAltered()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id", "Total") }, s_t0, baseline: true, by: "baseline"), ct);
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(77, "Orders", "Id", "Total", "Note") }, s_t0.AddHours(1)), ct);

            Assert.Equal(new SchemaWriteCounts(1, 0), counts);
            Assert.Equal("0", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'dropped'", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'altered' AND object_id = 77 AND valid_to IS NULL AND object_name = 'Orders'", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE valid_to IS NULL", ct));
        });
    }

    [Fact]
    public async Task Apply_RenamedObject_IsRenamedVersion()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id", "Total") }, s_t0, baseline: true, by: "baseline"), ct);
            var at = s_t0.AddHours(1);
            var events = new[] { new SchemaEvent("module_end", 5, at, Db, null, null, null, "sp_rename", "renamer", "RenameApp", "renamer-host", null) };
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "SalesOrders", "Id", "Total") }, at, attribution: events), ct);

            Assert.Equal(new SchemaWriteCounts(1, 0), counts);
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'renamed' AND object_name = 'SalesOrders' AND valid_to IS NULL AND change_login = 'renamer' AND change_app = 'RenameApp' AND change_host = 'renamer-host'", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE object_name = 'Orders' AND valid_to IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task Apply_CompleteReadMissingObject_IsDropped()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id"), Table(11, "Customers", "Id") }, s_t0, baseline: true, by: "baseline"), ct);
            var at = s_t0.AddHours(3);
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id") }, at), ct);

            Assert.Equal(new SchemaWriteCounts(1, 1), counts);
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'dropped' AND object_id = 11 AND valid_from = valid_to AND valid_to = timestamp '2026-11-01 13:00:00'", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE valid_to IS NULL", ct));
            Assert.Equal("0", await Scalar(c, "SELECT count(*) FROM collect.schema_columns sc JOIN collect.schema_objects o USING (version_id) WHERE sc.is_current AND o.object_id = 11", ct));
        });
    }

    [Fact]
    public async Task Apply_IncompleteRead_RecordsNoDrops()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id"), Table(11, "Customers", "Id") }, s_t0, baseline: true, by: "baseline"), ct);
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id") }, s_t0.AddHours(1), complete: false), ct);

            Assert.Equal(new SchemaWriteCounts(0, 0), counts);
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE valid_to IS NULL", ct));
            Assert.Equal("0", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'dropped'", ct));
        });
    }

    [Fact]
    public async Task Apply_TargetedReadMissingRequestedObject_IsDropped_OthersUntouched()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id"), Table(11, "Customers", "Id"), Table(12, "Items", "Id") }, s_t0, baseline: true, by: "baseline"), ct);
            var targeted = new HashSet<int> { 11 };
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(Array.Empty<SchemaObjectDefinition>(), s_t0.AddHours(1), by: "xe_event", targeted: targeted), ct);

            Assert.Equal(new SchemaWriteCounts(1, 1), counts);
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'dropped' AND object_id = 11", ct));
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE valid_to IS NULL AND object_id IN (10, 12)", ct));
        });
    }

    [Fact]
    public async Task Apply_AgKeyedWrites_FromTwoServers_ShareOneHistory()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            var ag = new SchemaHistoryKey(null, Guid.Parse("76430000-0000-0000-0000-0000000000a1"), "AgOne");
            var read = new[] { Table(10, "Orders", "Id", "Total") };
            await SchemaVersionStore.ApplyAsync(c, Write(read, s_t0, baseline: true, key: ag, server: 1, by: "baseline"), ct);
            var same = await SchemaVersionStore.ApplyAsync(c, Write(read, s_t0.AddHours(1), key: ag, server: 2), ct);
            var altered = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id", "Total", "Note") }, s_t0.AddHours(2), key: ag, server: 2), ct);

            Assert.Equal(new SchemaWriteCounts(0, 0), same);
            Assert.Equal(new SchemaWriteCounts(1, 0), altered);
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE server_id IS NULL AND ag_group_id = '76430000-0000-0000-0000-0000000000a1' AND ag_name = 'AgOne'", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE detected_on_server_id = 2", ct));
            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_capture_state", ct));
            Assert.Single(await SchemaVersionStore.LoadCurrentAsync(c, ag, Db, ct));
            Assert.Empty(await SchemaVersionStore.LoadCurrentAsync(c, s_key, Db, ct));
        });
    }

    [Fact]
    public async Task Apply_Attribution_TakesNewestEventForTheObject()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id"), Table(11, "Customers", "Id") }, s_t0, baseline: true, by: "baseline"), ct);
            var at = s_t0.AddHours(1);
            var events = new[]
            {
                Evt("object_altered", 1, at.AddMinutes(-30), 10, login: "older"),
                Evt("object_created", 3, at.AddMinutes(-10), 10, type: 22601, indexId: 2, login: "newest_index_event"),
                Evt("object_altered", 2, at.AddMinutes(-20), 10, login: "middle"),
                Evt("object_altered", 4, at.AddMinutes(-5), 11, login: "other_object"),
                Evt("object_deleted", 6, at.AddMinutes(-2), 11, login: "dropper")
            };
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id", "Total") }, at, by: "xe_event", attribution: events), ct);

            Assert.Equal(new SchemaWriteCounts(2, 1), counts);
            Assert.Equal("newest_index_event", await Scalar(c, "SELECT change_login FROM collect.schema_objects WHERE object_id = 10 AND valid_to IS NULL", ct));
            Assert.Equal("App-3", await Scalar(c, "SELECT change_app FROM collect.schema_objects WHERE object_id = 10 AND valid_to IS NULL", ct));
            Assert.Equal("dropper", await Scalar(c, "SELECT change_login FROM collect.schema_objects WHERE object_id = 11 AND change_kind = 'dropped'", ct));
        });
    }

    [Fact]
    public async Task Apply_TwoConcurrentPassesForTheSameKey_LeaveOneCurrentVersionPerObject()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            var connectionString = c.ConnectionString;
            var ag = new SchemaHistoryKey(null, Guid.Parse("76430000-0000-0000-0000-0000000000a2"), "AgTwo");
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id") }, s_t0, baseline: true, key: ag, by: "baseline"), ct);

            var read = new[] { Table(10, "Orders", "Id", "Total"), Table(20, "Items", "Id") };
            async Task PassAsync(int server)
            {
                await using var other = new NpgsqlConnection(connectionString);
                await other.OpenAsync(ct);
                await SchemaVersionStore.ApplyAsync(other, Write(read, s_t0.AddHours(1), key: ag, server: server), ct);
            }

            await Task.WhenAll(PassAsync(1), PassAsync(2));

            Assert.Equal("2", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE valid_to IS NULL", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE valid_to IS NULL AND object_id = 10", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE valid_to IS NULL AND object_id = 20", ct));
            Assert.Equal("3", await Scalar(c, "SELECT count(*) FROM collect.schema_objects", ct));
        });
    }

    [Fact]
    public async Task Apply_ColumnNamesWithCommaAndBackslash_RoundTripThroughTheStoredLists()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            var t = Table(10, "Orders", "a,b", "a\\b", "plain");
            t = t with
            {
                Indexes = new List<SchemaIndex> { new(2, "IX_Odd", "NONCLUSTERED", false, false, false, new[] { "a,b", "a\\b" }, new[] { "plain" }, null, "NONE", false) }
            };
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { t }, s_t0, baseline: true, by: "baseline"), ct);

            var keys = await Scalar(c, "SELECT key_columns FROM collect.schema_indexes WHERE name = 'IX_Odd'", ct);
            Assert.Equal(new[] { "a,b", "a\\b" },SchemaCatalogReader.SplitList(keys));
            var included = await Scalar(c, "SELECT included_columns FROM collect.schema_indexes WHERE name = 'IX_Odd'", ct);
            Assert.Equal(new[] { "plain" }, SchemaCatalogReader.SplitList(included));
        });
    }

    [Fact]
    public async Task Apply_NamesDifferingOnlyByCase_AreNotTheSameObject()
    {
        await WithStoreAsync(async (c, ct) =>
        {
            await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(10, "Orders", "Id"), Table(11, "orders", "Id") }, s_t0, baseline: true, by: "baseline"), ct);
            var counts = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(30, "orders", "Id") }, s_t0.AddHours(1)), ct);

            /* Only the row with the exact name is adopted (same definition, new id); the other case variant is gone and dropped. */
            Assert.Equal(new SchemaWriteCounts(1, 1), counts);
            Assert.Equal("30", await Scalar(c, "SELECT object_id FROM collect.schema_objects WHERE object_name = 'orders' AND valid_to IS NULL", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'dropped' AND object_id = 10", ct));

            /* A read that differs from every gone row only by case adopts none of them: created, and the old row dropped. */
            var third = await SchemaVersionStore.ApplyAsync(c, Write(new[] { Table(40, "ORDERS", "Id") }, s_t0.AddHours(2)), ct);
            Assert.Equal(new SchemaWriteCounts(2, 1), third);
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'created' AND object_id = 40 AND valid_to IS NULL", ct));
            Assert.Equal("1", await Scalar(c, "SELECT count(*) FROM collect.schema_objects WHERE change_kind = 'dropped' AND object_id = 30", ct));
        });
    }
}
