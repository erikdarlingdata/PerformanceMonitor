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
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7725: the mark FinOps Database Resources (rows, top by total CPU, top by average CPU) and High Impact put on a database the server no
/// longer lists, and the fill and envelope rules the top-N reads share. Mark only for FinOps: the order of its lists is untouched.
/// </summary>
public sealed class DroppedDatabaseMarkTests
{
    private static JsonObject Payload(params (string Key, string[] Databases)[] arrays)
    {
        var payload = new JsonObject { ["view"] = "database_resources" };
        foreach (var (key, databases) in arrays)
        {
            payload[key] = new JsonArray(databases.Select(db => (JsonNode)new JsonObject { ["database_name"] = db, ["cpu_ms"] = 1 }).ToArray());
        }

        return payload;
    }

    private static bool?[] Marks(JsonObject payload, string key) =>
        payload[key]!.AsArray().Select(r => r!["database_dropped"]?.GetValue<bool>()).ToArray();

    [Fact]
    public void MarkByName_MarksOnlyTheDroppedDatabases_InEveryNamedList_AndKeepsTheOrder()
    {
        var payload = Payload(("rows", new[] { "Sales", "OldSales", "Other" }), ("top_by_total", new[] { "OldSales", "Sales" }), ("top_by_avg", new[] { "Other", "OldSales" }));

        DroppedDatabaseRows.MarkByName(payload, new HashSet<string>(StringComparer.Ordinal) { "OldSales" }, "rows", "top_by_total", "top_by_avg");

        Assert.Equal(new bool?[] { null, true, null }, Marks(payload, "rows"));
        Assert.Equal(new bool?[] { true, null }, Marks(payload, "top_by_total"));
        Assert.Equal(new bool?[] { null, true }, Marks(payload, "top_by_avg"));
        Assert.Equal(new[] { "Sales", "OldSales", "Other" }, payload["rows"]!.AsArray().Select(r => r!["database_name"]!.GetValue<string>()).ToArray());
    }

    [Fact]
    public void MarkByName_LeavesEveryRowAlone_WhenNothingWasDropped_OrTheSetIsUnknown()
    {
        foreach (var dropped in new IReadOnlySet<string>?[] { null, new HashSet<string>() })
        {
            var payload = Payload(("rows", new[] { "Sales", "OldSales" }));
            DroppedDatabaseRows.MarkByName(payload, dropped, "rows");
            Assert.Equal(new bool?[] { null, null }, Marks(payload, "rows"));
        }
    }

    [Fact]
    public void MarkByName_SkipsAListThePayloadDoesNotCarry_AndARowWithNoDatabase()
    {
        var payload = Payload(("rows", new[] { "OldSales" }));
        payload["rows"]!.AsArray().Add(new JsonObject { ["cpu_ms"] = 2 });

        DroppedDatabaseRows.MarkByName(payload, new HashSet<string>(StringComparer.Ordinal) { "OldSales" }, "rows", "missing_list");

        Assert.Equal(new bool?[] { true, null }, Marks(payload, "rows"));
    }

    [Fact]
    public void Mark_SetsTheKeyOnTheDroppedRowsOnly()
    {
        var rows = new JsonArray(new JsonObject(), new JsonObject(), new JsonObject());

        DroppedDatabaseMark.Mark(rows, new List<string?> { "Live", "Gone", null }, new[] { "Gone" });

        Assert.Equal(new bool?[] { null, true, null }, rows.Select(r => r!["database_dropped"]?.GetValue<bool>()).ToArray());
    }

    [Fact]
    public void WithoutRows_NamesOnlyTheLeftOutDatabasesThatHaveNoRow_AndNullWhenEveryOneHasOne()
    {
        var leftOut = new[] { "GoneA", "GoneB" };

        Assert.Equal(new[] { "GoneB" }, DroppedDatabaseMark.WithoutRows(leftOut, new string?[] { "Live", "GoneA" }));
        Assert.Null(DroppedDatabaseMark.WithoutRows(leftOut, new string?[] { "GoneA", "GoneB" }));
    }

    [Fact]
    public async Task FillAsync_AppendsTheDroppedRowsAfterTheListedOnes_OnlyWhileTheLimitHasRoom()
    {
        var asked = new List<int>();
        Task<List<string>> ReadLeftOut(PerformanceMonitor.Darling.Storage.DatabaseFilter filter, int room)
        {
            asked.Add(room);
            return Task.FromResult(new List<string> { "dropped1", "dropped2", "dropped3" }.Take(room).ToList());
        }

        var filled = await DroppedDatabaseRows.FillAsync(new List<string> { "listed1" }, 3, new[] { "GoneDb" }, ReadLeftOut);
        Assert.Equal(new[] { "listed1", "dropped1", "dropped2" }, filled);
        Assert.Equal(new[] { 2 }, asked);

        /* A full page, nothing left out: no second read. */
        asked.Clear();
        Assert.Equal(new[] { "listed1" }, await DroppedDatabaseRows.FillAsync(new List<string> { "listed1" }, 1, new[] { "GoneDb" }, ReadLeftOut));
        Assert.Equal(new[] { "listed1" }, await DroppedDatabaseRows.FillAsync(new List<string> { "listed1" }, 5, null, ReadLeftOut));
        Assert.Empty(asked);
    }
    /// <summary>#7725: the Lite desktop grids' fill (the same rule as <c>DroppedDatabaseRows.FillAsync</c>): the listed rows first, the fill after
    /// them and only for the room the limit leaves, each fill row flagged, and the cell reads "name (dropped)" for a flagged row only.</summary>
    [Fact]
    public async Task GridFill_PutsTheListedRowsFirst_FlagsTheFill_AndLabelsOnlyTheFlaggedCell()
    {
        var askedTop = new List<int>();
        Task<List<string>> ReadLeftOut(IReadOnlyList<string> databases, int top)
        {
            Assert.Equal(new[] { "GoneDb" }, databases);
            askedTop.Add(top);
            return Task.FromResult(new List<string> { "dropped1", "dropped2" }.Take(top).ToList());
        }

        var flagged = new List<string>();
        var rows = await DroppedDatabaseMark.FillAsync(new List<string> { "listed1" }, 2, new[] { "GoneDb" }, ReadLeftOut, flagged.Add);
        Assert.Equal(new[] { "listed1", "dropped1" }, rows);
        Assert.Equal(new[] { "dropped1" }, flagged);
        Assert.Equal(new[] { 1 }, askedTop);

        /* A full page, or nothing left out: no second read and no flag. */
        askedTop.Clear();
        flagged.Clear();
        Assert.Equal(new[] { "listed1" }, await DroppedDatabaseMark.FillAsync(new List<string> { "listed1" }, 1, new[] { "GoneDb" }, ReadLeftOut, flagged.Add));
        Assert.Equal(new[] { "listed1" }, await DroppedDatabaseMark.FillAsync(new List<string> { "listed1" }, 5, null, ReadLeftOut, flagged.Add));
        Assert.Empty(askedTop);
        Assert.Empty(flagged);

        Assert.Equal("Sales (dropped)", DroppedDatabaseMark.Display("Sales", dropped: true));
        Assert.Equal("Sales", DroppedDatabaseMark.Display("Sales", dropped: false));
    }
}
