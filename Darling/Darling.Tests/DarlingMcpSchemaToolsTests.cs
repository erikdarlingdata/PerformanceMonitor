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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643: what the three schema history reads refuse before they touch the store: a window or page out of range, a missing or
/// malformed name, and a definition text search at a data-detail level that hides the text. Each refusal names the parameter and no
/// store is needed, so none of them can be answered by a read.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingMcpSchemaToolsTests
{
    private static readonly DateTime s_now = new(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);

    private static void AssertRefusal(string json, string parameter, string messagePart)
    {
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(parameter, doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains(messagePart, doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    private static Task<string> Definition(string? database = "SalesDb", string? name = "dbo.Orders", string? asOf = null, int offset = 0, int limit = 200) =>
        DarlingMcpSchemaTools.ReadObjectDefinition(null!, "srv", database, name, asOf, offset, limit, CancellationToken.None);

    private static Task<string> Changes(int hoursBack = 24, int limit = 50, int offset = 0, string? name = null) =>
        DarlingMcpSchemaTools.ReadSchemaChanges(null!, "srv", null, name, hoursBack, null, false, limit, offset, CancellationToken.None);

    private static Task<string> Find(
        string? database = null, string? prefix = "Ord", string? column = null, string? references = null, string? text = null, int limit = 50,
        McpDataDetail detail = McpDataDetail.Full) =>
        DarlingMcpSchemaTools.FindSchemaObjects(
            null!, "srv", database, prefix, null, column, references, text, limit, detail, DarlingMcpSchemaTools.BudgetMilliseconds, s_now, CancellationToken.None);

    [Fact]
    public async Task GetObjectDefinition_RefusesAMissingDatabaseOrName()
    {
        AssertRefusal(await Definition(database: " "), "database_name", "required");
        AssertRefusal(await Definition(name: null), "object_name", "required");
        AssertRefusal(await Definition(name: "dbo."), "object_name", "required");
    }

    [Fact]
    public async Task GetObjectDefinition_RefusesAnOutOfRangeColumnPageAndABadAsOf()
    {
        AssertRefusal(await Definition(offset: -1), "column_offset", "zero or a positive");
        AssertRefusal(await Definition(limit: 0), "column_limit", "positive integer");
        AssertRefusal(await Definition(limit: DarlingMcpSchemaTools.MaxColumnLimit + 1), "column_limit", "exceeds maximum");
        AssertRefusal(await Definition(asOf: "last tuesday"), "as_of", "Invalid as_of");
    }

    [Fact]
    public async Task GetSchemaChanges_RefusesAWindowOrPageOutOfRange()
    {
        AssertRefusal(await Changes(hoursBack: 169), "hours_back", "168");
        AssertRefusal(await Changes(hoursBack: 0), "hours_back", "Invalid");
        AssertRefusal(await Changes(limit: 0), "limit", "positive integer");
        AssertRefusal(await Changes(limit: DarlingMcpSchemaTools.MaxChangesLimit + 1), "limit", "exceeds maximum");
        AssertRefusal(await Changes(offset: -5), "offset", "zero or a positive");
        AssertRefusal(await Changes(name: "."), "object_name", "schema.name");
    }

    [Fact]
    public async Task FindSchemaObjects_RefusesDefinitionTextAtALevelThatHidesIt()
    {
        AssertRefusal(await Find(database: "SalesDb", text: "secret", detail: McpDataDetail.MaskLiterals), "definition_contains", "maskLiterals");
        AssertRefusal(await Find(database: "SalesDb", text: "secret", detail: McpDataDetail.MetadataOnly), "definition_contains", "metadataOnly");
    }

    [Fact]
    public async Task FindSchemaObjects_RefusesWhatItCannotServe()
    {
        AssertRefusal(await Find(text: "secret"), "database_name", "needs database_name");
        AssertRefusal(await Find(database: "SalesDb", text: "ab"), "definition_contains", "at least 3");
        AssertRefusal(await Find(limit: 0), "limit", "positive integer");
        AssertRefusal(await Find(limit: DarlingMcpSchemaTools.MaxFindLimit + 1), "limit", "exceeds maximum");
        AssertRefusal(await Find(prefix: null), "name_prefix", "at least one filter");
        AssertRefusal(await Find(prefix: null, references: "."), "references_table", "schema.table");
    }

    /* #8798: the selectivity shown beside a definition. The shape and the status are pure, so the rules run here without a store; the
       end-to-end reads over seeded rows are in DarlingMcpSchemaToolsLiveTests. */

    private static readonly DateTime s_read = new(2026, 11, 16, 3, 0, 0, DateTimeKind.Utc);

    private static SchemaColumn Col(int ordinal, string name) =>
        new(ordinal, name, "int", 4, 10, 0, false, null, null, null, null, null, null, null, null, null);

    private static SchemaIndex Index(string name, bool unique, bool pk, bool uniqueConstraint, string[] keys, string? filter = null, bool disabled = false) =>
        new(1, name, unique ? "NONCLUSTERED" : "CLUSTERED", unique, pk, uniqueConstraint, keys, Array.Empty<string>(), filter, "NONE", disabled);

    private static SchemaObjectDefinition Orders(params SchemaIndex[] indexes) =>
        new(10, "dbo", "Orders", 'U', s_read, null, null, [Col(1, "Id"), Col(2, "CustomerId"), Col(3, "OrderType")], indexes, [], []);

    private static StoredPrefix Prefix(
        string statistic, string[] columns, double density, long rows = 1000, long sampled = 1000, bool clustering = false, float? persisted = null, DateTime? updated = null) =>
        new(statistic, "index", columns, clustering, density, rows, sampled, updated ?? s_read, persisted, s_read.AddMinutes(5));

    private static JsonElement Json(object value)
    {
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(value, McpHelpers.JsonOptions));
        return doc.RootElement.Clone();
    }

    [Fact]
    public void Selectivity_OnColumns_AndCombinations()
    {
        var definition = Orders(Index("PK_Orders", true, true, false, ["Id"]));
        var view = ColumnSelectivity.Shape(definition, new[]
        {
            Prefix("PK_Orders", ["Id"], 0.001),
            Prefix("IX_Customer", ["CustomerId"], 0.1, sampled: 500),
            Prefix("IX_Customer", ["CustomerId", "OrderType"], 0.01, sampled: 500),
        });

        var id = Json(view.Columns["Id"]);
        Assert.Equal(1.0, id.GetProperty("selectivity").GetDouble());
        Assert.Equal(1000.0, id.GetProperty("distinct_estimate").GetDouble(), 6);
        Assert.Equal(1.0, id.GetProperty("rows_per_value").GetDouble(), 6);
        Assert.Equal("unique", id.GetProperty("label").GetString());
        Assert.Equal("PK_Orders", id.GetProperty("statistic").GetString());

        var team = Json(view.Columns["CustomerId"]);
        Assert.Equal(0.01, team.GetProperty("selectivity").GetDouble(), 6);
        Assert.Equal(10.0, team.GetProperty("distinct_estimate").GetDouble(), 6);
        Assert.Equal(100.0, team.GetProperty("rows_per_value").GetDouble(), 6);
        Assert.Equal(JsonValueKind.Null, team.GetProperty("label").ValueKind);
        Assert.Equal(50.0, team.GetProperty("sampled_percent").GetDouble(), 6);

        var combination = Assert.Single(view.Combinations);
        var json = Json(combination);
        Assert.Equal(new[] { "CustomerId", "OrderType" }, json.GetProperty("columns").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.False(json.GetProperty("includes_clustering_key").GetBoolean());
        Assert.Equal(0.1, json.GetProperty("selectivity").GetDouble(), 6);
        Assert.Equal(10.0, json.GetProperty("rows_per_value").GetDouble(), 6);
    }

    [Fact]
    public void SampledPercent_IsRowsSampledOverRows_NeverThePersistedDefault()
    {
        var view = ColumnSelectivity.Shape(Orders(), new[]
        {
            Prefix("IX_Customer", ["CustomerId"], 0.1, sampled: 250, persisted: 30f),
            Prefix("IX_Type", ["OrderType"], 0.1, sampled: 250),
            Prefix("IX_Region", ["Region"], 0.1, sampled: 500, persisted: 0f),
        });

        /* The persisted percent is the default for the next update; the numbers shown came from 250 of 1000 rows. */
        Assert.Equal(25.0, Json(view.Columns["CustomerId"]).GetProperty("sampled_percent").GetDouble(), 4);
        Assert.Equal(25.0, Json(view.Columns["OrderType"]).GetProperty("sampled_percent").GetDouble(), 4);
        Assert.Equal(50.0, Json(view.Columns["Region"]).GetProperty("sampled_percent").GetDouble(), 4);
    }

    [Fact]
    public void ZeroRowsOrZeroDensity_GiveNullNumbers_NotADivisionByZero()
    {
        var view = ColumnSelectivity.Shape(Orders(), new[]
        {
            Prefix("IX_Customer", ["CustomerId"], 0.1, rows: 0, sampled: 0),
            Prefix("IX_Type", ["OrderType"], 0.0),
        });

        foreach (var column in new[] { "CustomerId", "OrderType" })
        {
            var item = Json(view.Columns[column]);
            Assert.Equal(JsonValueKind.Null, item.GetProperty("selectivity").ValueKind);
            Assert.Equal(JsonValueKind.Null, item.GetProperty("distinct_estimate").ValueKind);
            Assert.Equal(JsonValueKind.Null, item.GetProperty("rows_per_value").ValueKind);
        }
    }

    [Fact]
    public void Unique_OnlyWhenUniqueIndexCoversExactlyThoseColumns()
    {
        /* A density of 1 / rows reads as unique on all of them, but only the PK on Id and the unique index on the two columns say so. */
        var definition = Orders(
            Index("PK_Orders", true, true, false, ["Id"]),
            Index("UQ_CustomerType", true, false, true, ["CustomerId", "OrderType DESC"]),
            Index("IX_Type", false, false, false, ["OrderType"]));
        var view = ColumnSelectivity.Shape(definition, new[]
        {
            Prefix("PK_Orders", ["Id"], 0.001),
            Prefix("IX_Customer", ["CustomerId"], 0.001),
            Prefix("IX_Type", ["OrderType"], 0.001),
            Prefix("UQ_CustomerType", ["CustomerId", "OrderType"], 0.001),
            Prefix("IX_Customer3", ["CustomerId", "Id"], 0.001),
            Prefix("UQ_TypeCustomer", ["OrderType", "CustomerId"], 0.001),
        });

        Assert.Equal("unique", Json(view.Columns["Id"]).GetProperty("label").GetString());
        Assert.Equal(JsonValueKind.Null, Json(view.Columns["CustomerId"]).GetProperty("label").ValueKind);
        Assert.Equal(JsonValueKind.Null, Json(view.Columns["OrderType"]).GetProperty("label").ValueKind);
        var labels = view.Combinations.Select(Json).ToDictionary(
            c => string.Join(",", c.GetProperty("columns").EnumerateArray().Select(e => e.GetString())),
            c => c.GetProperty("label").ValueKind == JsonValueKind.Null ? null : c.GetProperty("label").GetString());
        Assert.Equal("unique", labels["CustomerId,OrderType"]);
        Assert.Equal("unique", labels["OrderType,CustomerId"]);
        Assert.Null(labels["CustomerId,Id"]);
    }

    [Fact]
    public void Unique_IgnoresFilteredAndDisabledUniqueIndexes()
    {
        var definition = Orders(
            Index("UQ_Filtered", true, false, false, ["CustomerId"], filter: "([CustomerId]>(0))"),
            Index("UQ_Off", true, false, false, ["OrderType"], disabled: true));
        var view = ColumnSelectivity.Shape(definition, new[]
        {
            Prefix("IX_Customer", ["CustomerId"], 0.001),
            Prefix("IX_Type", ["OrderType"], 0.001),
        });

        Assert.Equal(JsonValueKind.Null, Json(view.Columns["CustomerId"]).GetProperty("label").ValueKind);
        Assert.Equal(JsonValueKind.Null, Json(view.Columns["OrderType"]).GetProperty("label").ValueKind);
    }

    [Fact]
    public void LowSelectivity_LabelsASingleColumnOnly()
    {
        var view = ColumnSelectivity.Shape(Orders(), new[]
        {
            Prefix("IX_Customer", ["CustomerId"], 0.5, rows: 1_000_000, sampled: 1_000_000),
            Prefix("IX_Customer", ["CustomerId", "OrderType"], 0.5, rows: 1_000_000, sampled: 1_000_000),
        });

        Assert.Equal("low selectivity", Json(view.Columns["CustomerId"]).GetProperty("label").GetString());
        Assert.Equal(JsonValueKind.Null, Json(Assert.Single(view.Combinations)).GetProperty("label").ValueKind);
    }

    [Fact]
    public void NoLeadingStatistic_GivesNullWithNote()
    {
        var view = ColumnSelectivity.Shape(Orders(), new[] { Prefix("IX_Customer", ["CustomerId"], 0.1) });

        Assert.True(view.Columns.ContainsKey("CustomerId"));
        Assert.False(view.Columns.ContainsKey("OrderType"));
        Assert.Equal("no statistic leads with this column", ColumnSelectivity.NoLeadingStatisticNote);
        Assert.Empty(view.Combinations);
    }

    [Fact]
    public void ClusteringKeyCombination_IsMarked()
    {
        var view = ColumnSelectivity.Shape(Orders(), new[]
        {
            Prefix("IX_Customer", ["CustomerId"], 0.1),
            Prefix("IX_Customer", ["CustomerId", "Id"], 0.001, clustering: true),
        });

        var combination = Json(Assert.Single(view.Combinations));
        Assert.True(combination.GetProperty("includes_clustering_key").GetBoolean());
        Assert.Equal(new[] { "CustomerId", "Id" }, combination.GetProperty("columns").EnumerateArray().Select(e => e.GetString()).ToArray());
    }

    [Fact]
    public void SeveralStatisticsForOneColumnList_TheLargestSampleWins_ThenTheNewest()
    {
        var view = ColumnSelectivity.Shape(Orders(), new[]
        {
            Prefix("_WA_Sys_CustomerId", ["CustomerId"], 0.5, sampled: 100),
            Prefix("IX_Customer", ["CustomerId"], 0.1, sampled: 900, updated: s_read.AddDays(-2)),
            Prefix("IX_Customer_New", ["CustomerId"], 0.2, sampled: 900, updated: s_read.AddDays(-1)),
        });

        Assert.Equal("IX_Customer_New", Json(view.Columns["CustomerId"]).GetProperty("statistic").GetString());
    }

    private static SelectivityFacts Facts(
        bool columnStatsOn = true, bool schemaOn = true, bool inScope = true, bool inOwnList = false, SelectivityRun? run = null,
        bool serverRunHadNoGrant = false, bool tf9485 = false, int rows = 3, DateTime? newestRead = null, DateTime? lastFullRead = null) =>
        new(columnStatsOn, schemaOn, inScope, inOwnList, run, serverRunHadNoGrant, tf9485, rows, newestRead ?? s_read, lastFullRead ?? s_read);

    [Fact]
    public void CollectorOff_StatusNotCollected()
    {
        var off = ColumnSelectivity.Decide(Facts(columnStatsOn: false), "SalesDb", null);
        Assert.Equal("not_collected", off.Status);
        Assert.Contains("column_stats", off.Note, StringComparison.Ordinal);

        Assert.Equal("not_collected", ColumnSelectivity.Decide(Facts(inScope: false), "SalesDb", null).Status);
        var schemaOff = ColumnSelectivity.Decide(Facts(schemaOn: false), "SalesDb", null);
        Assert.Equal("not_collected", schemaOff.Status);
        Assert.Contains("schema_objects", schemaOff.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void Current_CarriesTheCheckedAtStamp()
    {
        var current = ColumnSelectivity.Decide(Facts(run: new SelectivityRun(s_read.AddMinutes(10), null)), "SalesDb", null);

        Assert.Equal("current", current.Status);
        Assert.Equal(s_read.AddMinutes(10), current.CheckedAt);
        /* No run row (rows stored by a run whose record aged out): the newest stored read. */
        Assert.Equal(s_read, ColumnSelectivity.Decide(Facts(run: null), "SalesDb", null).CheckedAt);
    }

    [Fact]
    public void EveryStatusAfterTheSwitches_CarriesTheNewestRunTime()
    {
        var run = s_read.AddMinutes(20);
        foreach (var refused in new[] { "no_select", "tf9485" })
        {
            Assert.Equal(run, ColumnSelectivity.Decide(Facts(inOwnList: true, run: new SelectivityRun(run, refused), rows: 0), "SalesDb", null).CheckedAt);
            Assert.Equal(run, ColumnSelectivity.Decide(Facts(run: new SelectivityRun(run, refused), rows: 0), "SalesDb", null).CheckedAt);
        }

        Assert.Equal(run, ColumnSelectivity.Decide(Facts(run: new SelectivityRun(run, null), rows: 0, lastFullRead: null), "SalesDb", null).CheckedAt);
        Assert.Equal(run, ColumnSelectivity.Decide(Facts(run: new SelectivityRun(run, null)), "SalesDb", s_read.AddDays(-1)).CheckedAt);
        Assert.Null(ColumnSelectivity.Decide(Facts(columnStatsOn: false, run: new SelectivityRun(run, null)), "SalesDb", null).CheckedAt);
    }

    [Fact]
    public void TraceFlagRefusal_IsPrecondition_WithoutAGrantToRun()
    {
        /* Outside the own list too: SELECT is there, so another grant is no answer. */
        var refused = ColumnSelectivity.Decide(Facts(inOwnList: false, run: new SelectivityRun(s_read, "tf9485"), tf9485: true, rows: 0), "SalesDb", null);

        Assert.Equal("precondition", refused.Status);
        Assert.Contains(ColumnSelectivity.TraceFlagSentence, refused.Note, StringComparison.Ordinal);
        Assert.DoesNotContain("no SELECT", refused.Note, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT SELECT", refused.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void PastAsOf_StatusCurrentOnly_NoNumbers()
    {
        var past = ColumnSelectivity.Decide(Facts(run: new SelectivityRun(s_read, null)), "SalesDb", s_read.AddDays(-1));
        Assert.Equal("current_only", past.Status);
        Assert.Contains("current only", past.Note, StringComparison.Ordinal);

        Assert.Equal("current", ColumnSelectivity.Decide(Facts(run: new SelectivityRun(s_read, null)), "SalesDb", s_read.AddDays(1)).Status);
    }

    [Fact]
    public void PreconditionStatus_CarriesGrant()
    {
        /* A refused database named in column_stats' own list. */
        var named = ColumnSelectivity.Decide(Facts(inOwnList: true, run: new SelectivityRun(s_read, "no_select"), rows: 0), "Sales]Db", null);
        Assert.Equal("precondition", named.Status);
        Assert.Contains("GRANT SELECT", named.Note, StringComparison.Ordinal);
        /* A bare GRANT fails with 15151 where the login has no user: the statement makes the user first. */
        Assert.Contains("CREATE USER", named.Note, StringComparison.Ordinal);
        Assert.Contains("--column-stats-databases", named.Note, StringComparison.Ordinal);
        Assert.Contains("[Sales]]Db]", named.Note, StringComparison.Ordinal);
        /* The CLI hint passes the name as one quoted argument, so a name with a space or a comma survives the command line. */
        Assert.Contains("--column-stats-databases \"Sales]Db\"", named.Note, StringComparison.Ordinal);
        Assert.Contains(
            "--column-stats-databases \"Sales, \\\"Archive\\\" Db\"",
            ColumnSelectivity.Decide(Facts(inOwnList: true, run: new SelectivityRun(s_read, "no_select"), rows: 0), "Sales, \"Archive\" Db", null).Note,
            StringComparison.Ordinal);
        Assert.DoesNotContain("9485", named.Note, StringComparison.Ordinal);

        /* No database in scope had the grant on the newest run, though this one is not in the own list. */
        var nobody = ColumnSelectivity.Decide(Facts(run: new SelectivityRun(s_read, "no_select"), serverRunHadNoGrant: true, rows: 0), "SalesDb", null);
        Assert.Equal("precondition", nobody.Status);

        /* Trace flag 9485: the grant does not satisfy DBCC SHOW_STATISTICS. */
        var flagged = ColumnSelectivity.Decide(Facts(inOwnList: true, run: new SelectivityRun(s_read, "no_select"), tf9485: true, rows: 0), "SalesDb", null);
        Assert.Equal("precondition", flagged.Status);
        Assert.Contains("9485", flagged.Note, StringComparison.Ordinal);
        Assert.Contains("does not satisfy", flagged.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void RefusedOutsideTheOwnList_IsNotRead_NotPrecondition()
    {
        var skipped = ColumnSelectivity.Decide(Facts(inOwnList: false, run: new SelectivityRun(s_read, "no_select"), rows: 0), "SalesDb", null);

        Assert.Equal("not_read", skipped.Status);
        Assert.Contains("SELECT", skipped.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void NoRowsAndNoRunYet_IsNotReadYet()
    {
        Assert.Equal("not_read_yet", ColumnSelectivity.Decide(Facts(run: null, rows: 0, lastFullRead: null), "SalesDb", null).Status);
        /* A baseline cut short by the budget has a run row but no finished read: the table may simply not have been reached. */
        var unfinished = new SelectivityFacts(true, true, true, false, new SelectivityRun(s_read, null), false, false, 0, null, null);
        Assert.Equal("not_read_yet", ColumnSelectivity.Decide(unfinished, "SalesDb", null).Status);
    }

    [Fact]
    public void FinishedRead_WithNoStatisticOnTheTable_IsCurrent()
    {
        var done = ColumnSelectivity.Decide(Facts(run: new SelectivityRun(s_read, null), rows: 0, newestRead: null), "SalesDb", null);

        Assert.Equal("current", done.Status);
    }
}
