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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643 on a live store: the three schema history reads over versions written through <see cref="SchemaVersionStore.ApplyAsync"/>.
/// The current definition with its columns, indexes, constraints and triggers; the version that held at an instant and the oldest one with
/// a note before the history; a wide table paged under the response budget; the group's history read through a secondary; the change list
/// with its diff, login, paging and baseline switch; the searches over current rows only; the expression text under the masked keys; and a
/// server the collector never ran for.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class DarlingMcpSchemaToolsLiveTests
{
    private const string Db = "SalesDb";
    private const string ServerName = "schema-srv";
    private static readonly DateTime s_t0 = TruncateToSeconds(DateTime.UtcNow.AddHours(-12));
    private static readonly int s_serverId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly SchemaHistoryKey s_key = new(s_serverId, null, null);

    private static DateTime TruncateToSeconds(DateTime value) => new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

    private static string Iso(DateTime value) => value.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);

    private sealed record Store(NpgsqlConnection Connection, NpgsqlDataSource Source);

    private static async Task WithStoreAsync(Func<Store, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the schema history MCP live tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingMcpTestData.RegisterServerAsync(connection, s_serverId, ServerName, ct);
            await body(new Store(connection, source), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static SchemaObjectDefinition Table(
        int id, string name, string[] columns, string? schema = null, IReadOnlyList<SchemaConstraint>? constraints = null,
        IReadOnlyList<SchemaIndex>? indexes = null, IReadOnlyList<SchemaTrigger>? triggers = null, Func<int, string, SchemaColumn, SchemaColumn>? shape = null, string? pk = null)
    {
        return new SchemaObjectDefinition(
            id, schema ?? "dbo", name, 'U', s_t0,
            new SchemaTableProperties(null, null, false, null, null, null, "TABLE"), null,
            columns.Select((n, i) =>
            {
                var column = new SchemaColumn(i + 1, n, "int", 4, 10, 0, false, i == 0 ? 1m : null, i == 0 ? 1m : null, null, null, null, null, null, null, null);
                return shape is null ? column : shape(i, n, column);
            }).ToList(),
            indexes ?? new List<SchemaIndex> { new(1, "PK_" + (pk ?? name), "CLUSTERED", true, true, false, new[] { columns[0] }, Array.Empty<string>(), null, "NONE", false) },
            constraints ?? new List<SchemaConstraint> { new('P', "PK_" + (pk ?? name), null, Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false) },
            triggers ?? new List<SchemaTrigger>());
    }

    private static SchemaObjectDefinition View(int id, string name, string text)
    {
        return new SchemaObjectDefinition(
            id, "dbo", name, 'V', s_t0, null, new SchemaViewProperties(false, false, text),
            new List<SchemaColumn> { new(1, "Id", "int", 4, 10, 0, false, null, null, null, null, null, null, null, null, null) },
            new List<SchemaIndex>(), new List<SchemaConstraint>(), new List<SchemaTrigger>());
    }

    private static SchemaDatabaseWrite Write(
        IReadOnlyList<SchemaObjectDefinition> read, DateTime at, bool baseline = false, string by = "reconcile",
        IReadOnlyList<SchemaEvent>? attribution = null, SchemaHistoryKey? key = null, int server = 0)
    {
        var serverId = server == 0 ? s_serverId : server;
        var historyKey = key ?? s_key;
        return new SchemaDatabaseWrite(
            historyKey, serverId, Db, at, by, baseline, true, read, attribution ?? Array.Empty<SchemaEvent>(),
            new SchemaCaptureStateRow
            {
                ServerId = serverId,
                DatabaseName = Db,
                HistoryServerId = historyKey.ServerId,
                HistoryAgGroupId = historyKey.AgGroupId,
                LastCompleteReadAt = at,
                BaselineAt = baseline ? at : null
            });
    }

    private static Task Baseline(Store s, CancellationToken ct, params SchemaObjectDefinition[] read) =>
        SchemaVersionStore.ApplyAsync(s.Connection, Write(read, s_t0, baseline: true, by: "baseline"), ct);

    private static async Task<JsonElement> Parse(Task<string> call)
    {
        var json = await call;
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    private static Task<string> Definition(Store s, CancellationToken ct, string name, string? asOf = null, int offset = 0, int limit = 200, string? server = null, string database = Db) =>
        DarlingMcpSchemaTools.ReadObjectDefinition(s.Source, server ?? ServerName, database, name, asOf, offset, limit, ct);

    private static Task<string> Changes(
        Store s, CancellationToken ct, int hoursBack = 24, int limit = 50, int offset = 0, bool baseline = false, string? name = null, string? database = null, string? asOf = null) =>
        DarlingMcpSchemaTools.ReadSchemaChanges(s.Source, ServerName, database, name, hoursBack, asOf, baseline, limit, offset, ct);

    private static Task<string> Find(
        Store s, CancellationToken ct, string? database = Db, string? prefix = null, string? contains = null, string? column = null, string? references = null,
        string? text = null, int limit = 50, McpDataDetail detail = McpDataDetail.Full) =>
        DarlingMcpSchemaTools.FindSchemaObjects(
            s.Source, ServerName, database, prefix, contains, column, references, text, limit, detail, DarlingMcpSchemaTools.BudgetMilliseconds, DateTime.UtcNow, ct);

    private static Task<string> FindWithBudget(Store s, CancellationToken ct, int budgetMilliseconds, string text) =>
        DarlingMcpSchemaTools.FindSchemaObjects(
            s.Source, ServerName, Db, null, null, null, null, text, 50, McpDataDetail.Full, budgetMilliseconds, DateTime.UtcNow, ct);

    private static string[] Names(JsonElement array, string property) =>
        array.EnumerateArray().Select(e => e.GetProperty(property).GetString()!).ToArray();

    [Fact]
    public async Task GetObjectDefinition_Current_ReturnsColumnsIndexesConstraintsTriggers()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            var orders = Table(
                10, "Orders", new[] { "Id", "CustomerId", "Total" },
                constraints: new List<SchemaConstraint>
                {
                    new('P', "PK_Orders", null, Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false),
                    new('F', "FK_Orders_Customers", null, new[] { "CustomerId" }, "dbo", "Customers", new[] { "Id" }, "NO_ACTION", "CASCADE", false, true, false),
                    new('C', "CK_Orders_Total", "([Total]>(0))", Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false),
                },
                indexes: new List<SchemaIndex>
                {
                    new(1, "PK_Orders", "CLUSTERED", true, true, false, new[] { "Id" }, Array.Empty<string>(), null, "NONE", false),
                    new(2, "IX_Orders_Customer", "NONCLUSTERED", false, false, false, new[] { "CustomerId", "Total DESC" }, new[] { "Id" }, "([Total]>(0))", "PAGE", false),
                },
                triggers: new List<SchemaTrigger> { new("trg_Orders_Audit", false, "INSERT,UPDATE") },
                shape: (i, n, c) => n == "Total" ? c with { TypeName = "decimal", Precision = 18, Scale = 2, DefaultName = "DF_Orders_Total", DefaultDefinition = "((0))" } : c);
            await Baseline(s, ct, orders);

            var answer = await Parse(Definition(s, ct, "dbo.Orders"));

            Assert.Equal(ServerName, answer.GetProperty("server").GetString());
            Assert.Equal(Db, answer.GetProperty("database_name").GetString());
            Assert.False(answer.GetProperty("resolved_from_ag").GetBoolean());
            Assert.Equal("current", answer.GetProperty("state").GetString());
            Assert.Equal("Orders", answer.GetProperty("object").GetProperty("name").GetString());
            Assert.Equal("table", answer.GetProperty("object").GetProperty("type").GetString());
            Assert.Equal("baseline", answer.GetProperty("version").GetProperty("change_kind").GetString());
            Assert.Equal(1, answer.GetProperty("versions_kept").GetInt32());
            Assert.NotNull(answer.GetProperty("history_starts_at").GetString());
            Assert.Equal(3, answer.GetProperty("columns_returned").GetInt32());
            Assert.False(answer.GetProperty("columns_has_more").GetBoolean());
            var columns = answer.GetProperty("columns");
            Assert.Equal(new[] { "Id", "CustomerId", "Total" }, Names(columns, "name"));
            Assert.Equal("1", columns[0].GetProperty("identity_seed").GetString());
            Assert.Equal("decimal", columns[2].GetProperty("type_name").GetString());
            Assert.Equal("((0))", columns[2].GetProperty("default_definition").GetString());
            var indexes = answer.GetProperty("indexes");
            Assert.Equal(new[] { "PK_Orders", "IX_Orders_Customer" }, Names(indexes, "name"));
            Assert.Equal(new[] { "CustomerId", "Total DESC" }, indexes[1].GetProperty("key_columns").EnumerateArray().Select(e => e.GetString()).ToArray());
            Assert.Equal("Id", indexes[1].GetProperty("included_columns")[0].GetString());
            Assert.Equal("([Total]>(0))", indexes[1].GetProperty("filter_definition").GetString());
            var constraints = answer.GetProperty("constraints");
            Assert.Equal(new[] { "check", "foreign_key", "primary_key" }, Names(constraints, "kind"));
            Assert.Equal("([Total]>(0))", constraints[0].GetProperty("check_definition").GetString());
            Assert.Equal("Customers", constraints[1].GetProperty("referenced_table").GetString());
            Assert.Equal("CASCADE", constraints[1].GetProperty("update_action").GetString());
            Assert.True(constraints[1].GetProperty("is_not_trusted").GetBoolean());
            Assert.Equal("trg_Orders_Audit", answer.GetProperty("triggers")[0].GetProperty("name").GetString());
            Assert.Equal("INSERT,UPDATE", answer.GetProperty("triggers")[0].GetProperty("events").GetString());

            var bare = await Parse(Definition(s, ct, "orders"));
            Assert.Equal("Orders", bare.GetProperty("object").GetProperty("name").GetString());
        });
    }

    [Fact]
    public async Task GetObjectDefinition_AsOf_ReturnsThatVersion()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id", "Total" }));
            await SchemaVersionStore.ApplyAsync(s.Connection, Write(new[] { Table(10, "Orders", new[] { "Id", "Total", "Note" }) }, s_t0.AddHours(2), by: "xe_event"), ct);

            var early = await Parse(Definition(s, ct, "dbo.Orders", asOf: Iso(s_t0.AddHours(1))));
            var late = await Parse(Definition(s, ct, "dbo.Orders", asOf: Iso(s_t0.AddHours(3))));
            var now = await Parse(Definition(s, ct, "dbo.Orders"));

            Assert.Equal(new[] { "Id", "Total" }, Names(early.GetProperty("columns"), "name"));
            Assert.Equal("as_of", early.GetProperty("state").GetString());
            Assert.Equal("baseline", early.GetProperty("version").GetProperty("change_kind").GetString());
            Assert.Equal(new[] { "Id", "Total", "Note" }, Names(late.GetProperty("columns"), "name"));
            Assert.Equal("altered", late.GetProperty("version").GetProperty("change_kind").GetString());
            Assert.Equal(new[] { "Id", "Total", "Note" }, Names(now.GetProperty("columns"), "name"));
            Assert.Equal(2, now.GetProperty("versions_kept").GetInt32());
        });
    }

    [Fact]
    public async Task GetObjectDefinition_AsOfBeforeHistory_ReturnsOldestWithNote()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id", "Total" }));
            await SchemaVersionStore.ApplyAsync(s.Connection, Write(new[] { Table(10, "Orders", new[] { "Id", "Total", "Note" }) }, s_t0.AddHours(2), by: "xe_event"), ct);

            var answer = await Parse(Definition(s, ct, "dbo.Orders", asOf: Iso(s_t0.AddDays(-30))));

            Assert.Equal(new[] { "Id", "Total" }, Names(answer.GetProperty("columns"), "name"));
            Assert.Equal("baseline", answer.GetProperty("version").GetProperty("change_kind").GetString());
            Assert.Contains("before the history", answer.GetProperty("note").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task GetObjectDefinition_AsOfBeforeTheObjectWasCreated_IsEmpty()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id" }));
            await SchemaVersionStore.ApplyAsync(
                s.Connection, Write(new[] { Table(10, "Orders", new[] { "Id" }), Table(12, "Returns", new[] { "Id" }) }, s_t0.AddHours(2), by: "xe_event"), ct);

            using var answer = JsonDocument.Parse(await Definition(s, ct, "dbo.Returns", asOf: Iso(s_t0.AddHours(1))));

            Assert.Equal("empty", answer.RootElement.GetProperty("status").GetString());
            Assert.Contains("created at", answer.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task GetObjectDefinition_HugeViewText_IsCutToTheBudgetAndSaysSo()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            var body = "SELECT 1 AS Id /*" + new string('x', 2_000_000) + "*/";
            await Baseline(s, ct, View(20, "BigView", body));

            var json = await Definition(s, ct, "dbo.BigView");
            var answer = await Parse(Task.FromResult(json));

            Assert.True(json.Length < McpResponseBudget.DefaultBytes, json.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var properties = answer.GetProperty("properties");
            Assert.True(properties.GetProperty("view_definition_truncated").GetBoolean());
            Assert.Equal(body.Length, properties.GetProperty("view_definition_length").GetInt32());
            Assert.StartsWith("SELECT 1 AS Id", properties.GetProperty("view_definition").GetString(), StringComparison.Ordinal);
            Assert.Equal(new[] { "Id" }, Names(answer.GetProperty("columns"), "name"));
        });
    }

    [Fact]
    public async Task GetObjectDefinition_DroppedObject_AnswersItsLastDefinitionWithDroppedAt()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id", "Total" }), Table(11, "Customers", new[] { "Id" }));
            await SchemaVersionStore.ApplyAsync(s.Connection, Write(new[] { Table(11, "Customers", new[] { "Id" }) }, s_t0.AddHours(2)), ct);

            var answer = await Parse(Definition(s, ct, "dbo.Orders"));

            Assert.Equal("dropped", answer.GetProperty("state").GetString());
            Assert.NotNull(answer.GetProperty("dropped_at").GetString());
            Assert.Equal(new[] { "Id", "Total" }, Names(answer.GetProperty("columns"), "name"));
        });
    }

    [Fact]
    public async Task GetObjectDefinition_AmbiguousBareName_IsRefused()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id" }), Table(12, "Orders", new[] { "Id" }, schema: "sales"));

            using var refused = JsonDocument.Parse(await Definition(s, ct, "Orders"));
            Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
            Assert.Equal("object_name", refused.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
            Assert.Contains("dbo.Orders, sales.Orders", refused.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            var qualified = await Parse(Definition(s, ct, "sales.Orders"));
            Assert.Equal("sales", qualified.GetProperty("object").GetProperty("schema").GetString());
        });
    }

    [Fact]
    public async Task GetObjectDefinition_WideTable_PagesColumnsUnderBudget()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            var names = Enumerable.Range(1, 600).Select(i => "Column_" + i.ToString("D4") + "_" + new string('x', 60)).ToArray();
            await Baseline(s, ct, Table(10, "Wide", names));

            var seen = new List<string>();
            var offset = 0;
            var pages = 0;
            while (true)
            {
                var json = await Definition(s, ct, "dbo.Wide", offset: offset, limit: 600);
                Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes, "a page is under the response budget");
                using var doc = JsonDocument.Parse(json);
                var page = doc.RootElement;
                seen.AddRange(Names(page.GetProperty("columns"), "name"));
                Assert.Equal(600, page.GetProperty("column_count").GetInt32());
                Assert.Equal(page.GetProperty("columns").GetArrayLength(), page.GetProperty("columns_returned").GetInt32());
                pages++;
                if (!page.GetProperty("columns_has_more").GetBoolean())
                {
                    Assert.Equal(JsonValueKind.Null, page.GetProperty("next_column_offset").ValueKind);
                    break;
                }

                Assert.True(page.GetProperty("truncated").GetBoolean());
                Assert.Contains("response budget", page.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
                offset = page.GetProperty("next_column_offset").GetInt32();
                Assert.Equal(seen.Count, offset);
            }

            Assert.True(pages > 1, "600 wide columns do not fit one page");
            Assert.Equal(names, seen.ToArray());
        });
    }

    [Fact]
    public async Task GetObjectDefinition_FromSecondary_ResolvesToAgHistory()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            var secondary = ServerIdHelper.GetDeterministicHashCode("schema-secondary");
            await DarlingMcpTestData.RegisterServerAsync(s.Connection, secondary, "schema-secondary", ct);
            var group = Guid.NewGuid();
            var agKey = new SchemaHistoryKey(null, group, "SalesAg");
            await SchemaVersionStore.ApplyAsync(s.Connection, Write(new[] { Table(10, "Orders", new[] { "Id", "Total" }) }, s_t0, baseline: true, by: "baseline", key: agKey), ct);
            /* The secondary has only its server row (it is never tracked) and the group the AG collector saw its database in. */
            await SchemaCaptureStateStore.SaveAsync(s.Connection, new SchemaCaptureStateRow { ServerId = secondary, DatabaseName = string.Empty, LastSeenInScopeAt = s_t0 }, ct);
            await DarlingMcpTestData.ExecAsync(
                s.Connection, ct,
                "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, group_id) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), secondary, "schema-secondary", "SalesAg", Db, group.ToString("D"));

            var viaSecondary = await Parse(Definition(s, ct, "dbo.Orders", server: "schema-secondary", database: Db.ToLowerInvariant()));
            var viaPrimary = await Parse(Definition(s, ct, "dbo.Orders"));

            Assert.True(viaSecondary.GetProperty("resolved_from_ag").GetBoolean());
            Assert.Equal("SalesAg", viaSecondary.GetProperty("ag_name").GetString());
            Assert.Equal(Db, viaSecondary.GetProperty("database_name").GetString());
            Assert.Equal(new[] { "Id", "Total" }, Names(viaSecondary.GetProperty("columns"), "name"));
            Assert.True(viaPrimary.GetProperty("resolved_from_ag").GetBoolean());

            var changes = await Parse(DarlingMcpSchemaTools.ReadSchemaChanges(s.Source, "schema-secondary", null, null, 24, null, true, 50, 0, ct));
            Assert.True(changes.GetProperty("resolved_from_ag").GetBoolean());
            Assert.Equal(1, changes.GetProperty("changes_returned").GetInt32());
        });
    }

    [Fact]
    public async Task GetSchemaChanges_ListsAlteredRenamedDroppedWithDiffAndLogin()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id", "Total" }), Table(11, "Customers", new[] { "Id" }), Table(12, "Archive", new[] { "Id" }));
            var altered = new[] { new SchemaEvent("object_altered", 1, s_t0.AddHours(1), Db, 10, 8277, null, null, "dev_login", "DevApp", "dev-host", null) };
            await SchemaVersionStore.ApplyAsync(
                s.Connection,
                Write(new[] { Table(10, "Orders", new[] { "Id", "Total", "Note" }), Table(11, "Customers", new[] { "Id" }), Table(12, "Archive", new[] { "Id" }) }, s_t0.AddHours(1), by: "xe_event", attribution: altered),
                ct);
            var renamed = new[] { new SchemaEvent("module_end", 2, s_t0.AddHours(2), Db, null, null, null, "sp_rename", "renamer", "RenameApp", "rename-host", null) };
            await SchemaVersionStore.ApplyAsync(
                s.Connection,
                Write(new[] { Table(10, "Orders", new[] { "Id", "Total", "Note" }), Table(11, "Clients", new[] { "Id" }, pk: "Customers"), Table(12, "Archive", new[] { "Id" }) }, s_t0.AddHours(2), attribution: renamed),
                ct);
            var dropped = new[] { new SchemaEvent("object_deleted", 3, s_t0.AddHours(3), Db, 12, 8277, null, null, "dropper", "DropApp", "drop-host", null) };
            await SchemaVersionStore.ApplyAsync(
                s.Connection,
                Write(new[] { Table(10, "Orders", new[] { "Id", "Total", "Note" }), Table(11, "Clients", new[] { "Id" }, pk: "Customers") }, s_t0.AddHours(3), attribution: dropped),
                ct);

            var answer = await Parse(Changes(s, ct));

            Assert.Equal(3, answer.GetProperty("changes_returned").GetInt32());
            Assert.False(answer.GetProperty("has_more").GetBoolean());
            var rows = answer.GetProperty("changes");
            Assert.Equal(new[] { "dropped", "renamed", "altered" }, Names(rows, "change_kind"));

            var drop = rows[0];
            Assert.Equal("Archive", drop.GetProperty("name").GetString());
            Assert.Equal("dropper", drop.GetProperty("login").GetString());
            Assert.Equal("drop-host", drop.GetProperty("host_name").GetString());
            Assert.Equal("Id", drop.GetProperty("diff").GetProperty("columns_removed")[0].GetString());
            Assert.Contains("PK_Archive", drop.GetProperty("diff").GetProperty("constraints_removed").EnumerateArray().Select(e => e.GetString()), StringComparer.Ordinal);

            var rename = rows[1];
            Assert.Equal("Clients", rename.GetProperty("name").GetString());
            Assert.Equal("renamer", rename.GetProperty("login").GetString());
            Assert.Equal("dbo.Customers", rename.GetProperty("diff").GetProperty("renamed").GetProperty("from").GetString());
            Assert.Equal("dbo.Clients", rename.GetProperty("diff").GetProperty("renamed").GetProperty("to").GetString());

            var alter = rows[2];
            Assert.Equal("Orders", alter.GetProperty("name").GetString());
            Assert.Equal("xe_event", alter.GetProperty("detected_by").GetString());
            Assert.Equal("dev_login", alter.GetProperty("login").GetString());
            Assert.Equal("DevApp", alter.GetProperty("application").GetString());
            Assert.Equal("Note", alter.GetProperty("diff").GetProperty("columns_added")[0].GetString());
            Assert.False(alter.GetProperty("diff").TryGetProperty("columns_removed", out _));

            var one = await Parse(Changes(s, ct, name: "dbo.orders"));
            Assert.Equal(new[] { "altered" }, Names(one.GetProperty("changes"), "change_kind"));
        });
    }

    [Fact]
    public async Task GetSchemaChanges_ExcludesBaselineByDefault()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id" }));

            using var none = JsonDocument.Parse(await Changes(s, ct));
            Assert.Equal("empty", none.RootElement.GetProperty("status").GetString());
            Assert.Contains("include_baseline", none.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            var withBaseline = await Parse(Changes(s, ct, baseline: true));
            Assert.Equal(new[] { "baseline" }, Names(withBaseline.GetProperty("changes"), "change_kind"));
            Assert.Equal(JsonValueKind.Null, withBaseline.GetProperty("changes")[0].GetProperty("diff").ValueKind);
        });
    }

    [Fact]
    public async Task GetSchemaChanges_RefusesHoursBackOver168()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            using var refused = JsonDocument.Parse(await Changes(s, ct, hoursBack: 169));
            Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
            Assert.Equal("hours_back", refused.RootElement.GetProperty("hints").GetProperty("parameter").GetString());

            var accepted = await Changes(s, ct, hoursBack: 168, baseline: true);
            Assert.NotEqual("invalid", JsonDocument.Parse(accepted).RootElement.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task GetSchemaChanges_AsOfInThePast_EndsTheWindowThere()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id" }), Table(11, "Customers", new[] { "Id" }));
            await SchemaVersionStore.ApplyAsync(s.Connection, Write(new[] { Table(10, "Orders", new[] { "Id", "A" }), Table(11, "Customers", new[] { "Id" }) }, s_t0.AddHours(1)), ct);
            await SchemaVersionStore.ApplyAsync(s.Connection, Write(new[] { Table(10, "Orders", new[] { "Id", "A" }), Table(11, "Customers", new[] { "Id", "B" }) }, s_t0.AddHours(5)), ct);

            var everything = await Parse(Changes(s, ct));
            Assert.Equal(new[] { "Customers", "Orders" }, Names(everything.GetProperty("changes"), "name"));

            var anchored = await Parse(Changes(s, ct, hoursBack: 6, asOf: Iso(s_t0.AddHours(2))));
            Assert.Equal(new[] { "Orders" }, Names(anchored.GetProperty("changes"), "name"));
            Assert.Equal(Iso(s_t0.AddHours(2)), Iso(DateTime.Parse(anchored.GetProperty("window_end").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal)));

            using var before = JsonDocument.Parse(await Changes(s, ct, hoursBack: 1, asOf: Iso(s_t0.AddMinutes(30))));
            Assert.Equal("empty", before.RootElement.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task GetSchemaChanges_PagesWithLimitPlusOne()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id" }), Table(11, "Customers", new[] { "Id" }), Table(12, "Archive", new[] { "Id" }));
            await SchemaVersionStore.ApplyAsync(
                s.Connection,
                Write(new[] { Table(10, "Orders", new[] { "Id", "A" }), Table(11, "Customers", new[] { "Id", "B" }), Table(12, "Archive", new[] { "Id", "C" }) }, s_t0.AddHours(1)),
                ct);

            var first = await Parse(Changes(s, ct, limit: 2));
            Assert.Equal(2, first.GetProperty("changes_returned").GetInt32());
            Assert.True(first.GetProperty("has_more").GetBoolean());
            Assert.Equal(2, first.GetProperty("next_offset").GetInt32());

            var second = await Parse(Changes(s, ct, limit: 2, offset: 2));
            Assert.Equal(1, second.GetProperty("changes_returned").GetInt32());
            Assert.False(second.GetProperty("has_more").GetBoolean());
            Assert.Equal(JsonValueKind.Null, second.GetProperty("next_offset").ValueKind);

            var exact = await Parse(Changes(s, ct, limit: 3));
            Assert.Equal(3, exact.GetProperty("changes_returned").GetInt32());
            Assert.False(exact.GetProperty("has_more").GetBoolean());
        });
    }

    [Fact]
    public async Task FindSchemaObjects_ByColumnName_ReadsCurrentOnly()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id", "Note" }), Table(11, "Customers", new[] { "Id", "NOTE" }));
            await SchemaVersionStore.ApplyAsync(
                s.Connection, Write(new[] { Table(10, "Orders", new[] { "Id" }), Table(11, "Customers", new[] { "Id", "NOTE" }) }, s_t0.AddHours(1)), ct);

            var found = await Parse(Find(s, ct, column: "note"));

            Assert.Equal(1, found.GetProperty("objects_returned").GetInt32());
            Assert.Equal("Customers", found.GetProperty("objects")[0].GetProperty("name").GetString());
            Assert.False(found.GetProperty("has_more").GetBoolean());

            using var none = JsonDocument.Parse(await Find(s, ct, column: "Nothing"));
            Assert.Equal("empty", none.RootElement.GetProperty("status").GetString());

            var byName = await Parse(Find(s, ct, prefix: "ord"));
            Assert.Equal(new[] { "Orders" }, Names(byName.GetProperty("objects"), "name"));
            var contains = await Parse(Find(s, ct, contains: "STOM", database: null));
            Assert.Equal(new[] { "Customers" }, Names(contains.GetProperty("objects"), "name"));
        });
    }

    [Fact]
    public async Task FindSchemaObjects_ReferencesTable_FindsForeignKeys()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            var fk = new List<SchemaConstraint>
            {
                new('P', "PK_Orders", null, Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false),
                new('F', "FK_Orders_Customers", null, new[] { "CustomerId" }, "dbo", "Customers", new[] { "Id" }, "NO_ACTION", "NO_ACTION", false, false, false),
            };
            await Baseline(s, ct, Table(10, "Orders", new[] { "Id", "CustomerId" }, constraints: fk), Table(11, "Customers", new[] { "Id" }));

            var qualified = await Parse(Find(s, ct, references: "dbo.customers"));
            var bare = await Parse(Find(s, ct, references: "Customers"));
            Assert.Equal(new[] { "Orders" }, Names(qualified.GetProperty("objects"), "name"));
            Assert.Equal(new[] { "Orders" }, Names(bare.GetProperty("objects"), "name"));

            using var otherSchema = JsonDocument.Parse(await Find(s, ct, references: "sales.Customers"));
            Assert.Equal("empty", otherSchema.RootElement.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task FindSchemaObjects_DefinitionContains_FindsTextInEachExpressionKind()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            var orders = Table(
                10, "Orders", new[] { "Id", "Status" },
                constraints: new List<SchemaConstraint> { new('C', "CK_Orders_Status", "([Status]<>'needle-check')", Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false) },
                shape: (i, n, c) => n == "Status" ? c with { DefaultDefinition = "('needle-default')" } : c);
            await Baseline(s, ct, orders, View(20, "AuditView", "select Id from dbo.Orders -- needle-view"));

            var found = await Parse(Find(s, ct, text: "NEEDLE"));

            Assert.Equal(new[] { "AuditView", "Orders" }, Names(found.GetProperty("objects"), "name"));
            Assert.Equal(new[] { "view_definition" }, found.GetProperty("objects")[0].GetProperty("matched_in").EnumerateArray().Select(e => e.GetString()).ToArray());
            Assert.Equal(new[] { "default_definition", "check_definition" }, found.GetProperty("objects")[1].GetProperty("matched_in").EnumerateArray().Select(e => e.GetString()).ToArray());

            using var none = JsonDocument.Parse(await Find(s, ct, text: "absent-text"));
            Assert.Equal("empty", none.RootElement.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task FindSchemaObjects_DefinitionContains_RefusedUnderMaskLiterals()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, View(20, "OrderView", "select 'needle-view' as Id"));

            using var masked = JsonDocument.Parse(await Find(s, ct, text: "needle", detail: McpDataDetail.MaskLiterals));
            Assert.Equal("invalid", masked.RootElement.GetProperty("status").GetString());
            Assert.Equal("definition_contains", masked.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
            using var hidden = JsonDocument.Parse(await Find(s, ct, text: "needle", detail: McpDataDetail.MetadataOnly));
            Assert.Equal("invalid", hidden.RootElement.GetProperty("status").GetString());

            var byName = await Parse(Find(s, ct, prefix: "Order", detail: McpDataDetail.MaskLiterals));
            Assert.Equal(new[] { "OrderView" }, Names(byName.GetProperty("objects"), "name"));
        });
    }

    [Fact]
    public async Task FindSchemaObjects_DefinitionContains_StatementTimeout_ReturnsStopped()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            /* Eight 2 MB view bodies that never hold the needle: the ILIKE has to read all 16 MB, which no 1 ms statement_timeout survives. */
            var body = new string('x', 2_000_000);
            await Baseline(s, ct, Enumerable.Range(1, 8).Select(i => View(100 + i, "BigView" + i, body)).ToArray());

            using var stopped = JsonDocument.Parse(await FindWithBudget(s, ct, budgetMilliseconds: 1, text: "needle"));
            Assert.Equal("stopped", stopped.RootElement.GetProperty("status").GetString());
            Assert.Contains("budget", stopped.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            using var answered = JsonDocument.Parse(await Find(s, ct, text: "needle"));
            Assert.Equal("empty", answered.RootElement.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task FindSchemaObjects_NamePatterns_AreLiteral_NotLikeWildcards()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(
                s, ct,
                Table(10, "Orders", new[] { "Id" }), Table(11, "Ord_Archive", new[] { "Id" }), Table(12, "Ord%Rate", new[] { "Id" }),
                Table(13, "Plain", new[] { "Id" }),
                View(20, "Discount50", "select 1 as Id -- 50 percent"), View(21, "Discount", "select 1 as Id -- 50% off"));

            var prefix = await Parse(Find(s, ct, prefix: "Ord_"));
            Assert.Equal(new[] { "Ord_Archive" }, Names(prefix.GetProperty("objects"), "name"));

            var percentPrefix = await Parse(Find(s, ct, prefix: "Ord%"));
            Assert.Equal(new[] { "Ord%Rate" }, Names(percentPrefix.GetProperty("objects"), "name"));

            var contains = await Parse(Find(s, ct, contains: "%"));
            Assert.Equal(new[] { "Ord%Rate" }, Names(contains.GetProperty("objects"), "name"));

            var underscore = await Parse(Find(s, ct, contains: "d_A"));
            Assert.Equal(new[] { "Ord_Archive" }, Names(underscore.GetProperty("objects"), "name"));

            var text = await Parse(Find(s, ct, text: "50%"));
            Assert.Equal(new[] { "Discount" }, Names(text.GetProperty("objects"), "name"));
        });
    }

    [Fact]
    public async Task FindSchemaObjects_DefinitionContains_RequiresDatabase()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            await Baseline(s, ct, View(20, "OrderView", "select 1 as Id"));

            using var refused = JsonDocument.Parse(await Find(s, ct, database: null, text: "select"));
            Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
            Assert.Equal("database_name", refused.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        });
    }

    [Fact]
    public async Task DataDetail_MaskLiterals_MasksDefaultsChecksFiltersAndViewText()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            var orders = Table(
                10, "Orders", new[] { "Id", "Status", "Doubled" },
                constraints: new List<SchemaConstraint> { new('C', "CK_Orders_Status", "([Status]<>'SECRET-CHECK')", Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false) },
                indexes: new List<SchemaIndex> { new(2, "IX_Orders_Status", "NONCLUSTERED", false, false, false, new[] { "Status" }, Array.Empty<string>(), "([Status]='SECRET-FILTER')", "NONE", false) },
                shape: (i, n, c) => n switch
                {
                    "Status" => c with { DefaultName = "DF_Status", DefaultDefinition = "('SECRET-DEFAULT')" },
                    "Doubled" => c with { ComputedDefinition = "(case when [Id]=(7) then 'SECRET-COMPUTED' end)" },
                    _ => c,
                });
            await Baseline(s, ct, orders, View(20, "OrderView", "select Id from dbo.Orders where Status = 'SECRET-VIEW'"));
            var secrets = new[] { "SECRET-CHECK", "SECRET-FILTER", "SECRET-DEFAULT", "SECRET-COMPUTED", "SECRET-VIEW" };

            var table = await Definition(s, ct, "dbo.Orders");
            var view = await Definition(s, ct, "dbo.OrderView");
            Assert.All(secrets.Take(4), secret => Assert.Contains(secret, table, StringComparison.Ordinal));
            Assert.Contains(secrets[4], view, StringComparison.Ordinal);

            foreach (var mode in new[] { McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly })
            {
                foreach (var json in new[] { table, view })
                {
                    var masked = McpDataDetailMasker.MaskJson(json, mode);
                    Assert.All(secrets, secret => Assert.DoesNotContain(secret, masked, StringComparison.Ordinal));
                    /* Names, types and structure are kept at every level. */
                    using var doc = JsonDocument.Parse(masked);
                    Assert.Equal(ServerName, doc.RootElement.GetProperty("server").GetString());
                    Assert.Equal(1, doc.RootElement.GetProperty("columns")[0].GetProperty("ordinal").GetInt32());
                    Assert.Equal("int", doc.RootElement.GetProperty("columns")[0].GetProperty("type_name").GetString());
                }

                var maskedTable = JsonDocument.Parse(McpDataDetailMasker.MaskJson(table, mode)).RootElement;
                Assert.Equal("IX_Orders_Status", maskedTable.GetProperty("indexes")[0].GetProperty("name").GetString());
                Assert.Equal("CK_Orders_Status", maskedTable.GetProperty("constraints")[0].GetProperty("name").GetString());
                Assert.Equal("DF_Status", maskedTable.GetProperty("columns")[1].GetProperty("default_name").GetString());
            }
        });
    }

    [Fact]
    public async Task NeverCollected_ReturnsNotCollected()
    {
        await WithStoreAsync(async (s, ct) =>
        {
            foreach (var json in new[]
            {
                await Definition(s, ct, "dbo.Orders"),
                await Changes(s, ct),
                await Find(s, ct, prefix: "Ord"),
            })
            {
                using var doc = JsonDocument.Parse(json);
                Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
                Assert.Contains("schema_objects", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }
        });
    }
}
