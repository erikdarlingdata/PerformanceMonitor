/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behaviour pins for the Queries grids' tag column and filter, assign-from-row and the by-tag view (#5993 part B), against a real
/// DuckDB: the grid reads, the context-menu assignment, one map pass, and the view's sections.
/// </summary>
public sealed class LiteQueryTagGridTests : IClassFixture<SharedDuckDbFixture>
{
    private const int Server = 9301;
    private const string HotHash = "0x00000000000000A1";
    private const string TaggedHash = "0x00000000000000B1";
    private const string ChildHash = "0x00000000000000C1";

    private readonly LocalDataService _service;
    private readonly SharedDuckDbFixture _fixture;
    private readonly DateTime _now = DateTime.UtcNow;
    private long _nextId = 3_000_000;

    public LiteQueryTagGridTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _service = new LocalDataService(fixture.DuckDb);
    }

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task PlanAsync(string hash, long cpuUs) => ExecAsync(
        "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', 'db1', $4, $2, 1, $5, $5, 10, 1)",
        _nextId++, _now.AddMinutes(-30), Server, hash, cpuUs);

    private Task MapAsync(string hash, int tagId) => ExecAsync(
        "INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ($1, 'hash', $2, $3, 'hand_hash')", Server, hash, tagId);

    private async Task<(int Parent, int Child)> SeedAsync()
    {
        var parent = await _service.CreateQueryTagAsync("A", null);
        var child = await _service.CreateQueryTagAsync("A2", parent);
        await PlanAsync(HotHash, 9_000_000);
        await PlanAsync(TaggedHash, 5_000_000);
        await PlanAsync(ChildHash, 1_000_000);
        await MapAsync(TaggedHash, parent);
        await MapAsync(ChildHash, child);
        return (parent, child);
    }

    private Task<List<QueryStatsRow>> GridAsync(string? choice, int top = 50) =>
        QueryTagGrid.ReadAsync(_service, Server, choice, tags => _service.GetTopQueriesByCpuAsync(Server, 24, top, tags: tags));

    [Fact]
    public async Task TagColumn_ShowsEachRowsPath_AndBlankForAnUntaggedRow()
    {
        await SeedAsync();

        var rows = await GridAsync(QueryTagGrid.AllChoice);

        Assert.Equal("A", rows.Single(r => r.QueryHash == TaggedHash).QueryTag);
        Assert.Equal("A / A2", rows.Single(r => r.QueryHash == ChildHash).QueryTag);
        Assert.Null(rows.Single(r => r.QueryHash == HotHash).QueryTag);
    }

    [Fact]
    public async Task TagFilter_SetToATag_ShowsOnlyThatTagsRows_AParentKeepsItsChildren_AndUntaggedShowsTheRest()
    {
        await SeedAsync();

        Assert.Equal([ChildHash], (await GridAsync("A / A2")).Select(r => r.QueryHash).ToArray());
        Assert.Equal([TaggedHash, ChildHash], (await GridAsync("A")).Select(r => r.QueryHash).ToArray());
        Assert.Equal([HotHash], (await GridAsync(QueryTagGrid.UntaggedChoice)).Select(r => r.QueryHash).ToArray());
        /* The filter applies before the top-N cut: the tagged row shows although the untagged one is hotter. */
        Assert.Equal([TaggedHash], (await GridAsync("A", top: 1)).Select(r => r.QueryHash).ToArray());
    }

    [Fact]
    public async Task TagFilter_AChoiceNoTagCarriesAnyMore_ReadsAsNoFilter()
    {
        await SeedAsync();

        Assert.Equal(3, (await GridAsync("Deleted / Gone")).Count);
    }

    [Fact]
    public async Task AssignFromRow_TagsTheRowAfterTheNextMapPass_AndNotBefore()
    {
        var tag = await _service.CreateQueryTagAsync("Reports", null);
        await PlanAsync(HotHash, 9_000_000);
        var buffer = new QueryTagSightingBuffer();
        buffer.Append(QuerySnapshotsCollector.Instance,
            [new QuerySnapshotsCollector.Row { QueryHash = HotHash, QueryText = "SELECT 1", DatabaseName = "db1", ProgramName = "app", LoginName = "u", HostName = "h" }], serverId: Server);
        buffer.Flush(_fixture.DuckDb);

        var row = (await GridAsync(null)).Single();
        var target = QueryTagGrid.TargetOf(row);
        Assert.NotNull(target);
        await QueryTagGrid.AssignAsync(_service, Server, tag, target);

        Assert.Null((await GridAsync(null)).Single().QueryTag);

        await _service.BuildQueryTagMapAsync();

        Assert.Equal("Reports", (await GridAsync(null)).Single().QueryTag);
        Assert.Contains("next pass", QueryTagGrid.AssignedMessage(target, "Reports"), StringComparison.Ordinal);
    }

    [Fact]
    public void TargetOf_AProcedureRowBindsItsObject_AndARowWithNoHashBindsNothing()
    {
        var proc = new ProcedureStatsRow { DatabaseName = "db1", SchemaName = "dbo", ObjectName = "p" };
        var target = QueryTagGrid.TargetOf(proc)!;
        Assert.Equal(QueryTagKeyKind.Object, target.Kind);
        Assert.Equal("db1.dbo.p", target.Display);
        Assert.Null(QueryTagGrid.TargetOf(new QueryStatsRow { QueryHash = "" }));
        Assert.Null(QueryTagGrid.TargetOf(new QueryStatsRow { QueryHash = "0x0000000000000000" }));
    }

    [Fact]
    public async Task ByTagView_TagRowsPlusUntaggedEqualEachSectionsTotal_AndSectionsAreNeverAdded()
    {
        await SeedAsync();
        var data = await _service.GetQueryTagRollupDataAsync([Server], _now.AddHours(-2), _now);

        var view = QueryTagUsageView.Build(data, [(Server, "s")], allServers: false, tagFilter: null);

        var plan = view.Tables.Single(t => t.Section == "plan_cache");
        Assert.Equal("Tag", plan.Columns[0]);
        double Cpu(QueryTagUsageRow r) => double.Parse(r.Cells[plan.Columns.ToList().IndexOf("CPU (ms)") - 1], System.Globalization.CultureInfo.InvariantCulture);
        var own = plan.Rows.Where(r => r.Kind == "tag").Sum(r => OwnCpu(r, plan));
        Assert.Equal(Cpu(plan.Rows.Single(r => r.Kind == "total")), own + Cpu(plan.Rows.Single(r => r.Kind == "untagged")), 3);
        Assert.Equal(15_000, Cpu(plan.Rows.Single(r => r.Kind == "total")), 3);
        Assert.Equal(6, view.Tables.Count);
        Assert.Equal("empty", view.Tables.Single(t => t.Section == "query_store").Status);
        Assert.DoesNotContain(view.Tables, t => t.Title.Contains("all sections", StringComparison.OrdinalIgnoreCase));
        /* The by-tag view writes a child's path with the same separator as the grids' Tag column and the reads (#6037 review). */
        Assert.Contains(plan.Rows, r => r.Kind == "tag" && r.Label == "A" + QueryTagReadScope.PathSeparator + "A2");
    }

    private static double OwnCpu(QueryTagUsageRow row, QueryTagUsageTable table) =>
        double.Parse(row.Cells[table.Columns.ToList().IndexOf("CPU (ms)") - 1], System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public async Task ByTagView_ATagThatMatchesNothing_SaysSoAtTheTop()
    {
        await SeedAsync();
        var empty = await _service.CreateQueryTagAsync("Idle", null);
        var data = await _service.GetQueryTagRollupDataAsync([Server], _now.AddHours(-2), _now);

        var view = QueryTagUsageView.Build(data, [(Server, "s")], false, empty);

        Assert.Contains("none of it is under query tag 'Idle'", view.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ByTagView_AllServers_GivesEachServerItsOwnTablePlusAFleetLine()
    {
        await SeedAsync();
        var data = await _service.GetQueryTagRollupDataAsync([Server, Server + 1], _now.AddHours(-2), _now);

        var view = QueryTagUsageView.Build(data, [(Server, "one"), (Server + 1, "two")], true, null);

        Assert.Contains(view.Tables, t => t.Title == "Plan cache - one");
        Assert.Contains(view.Tables, t => t.Title == "Plan cache - two" && t.Status == "empty");
        Assert.Contains(view.Tables, t => t.Title == "Plan cache - all servers");
    }

    [Fact]
    public void FilterChoices_ListAllUntaggedThenEveryTagByPath()
    {
        var tags = new List<QueryTagRow>
        {
            new(1, "B", null, 0, null, DateTime.UtcNow), new(2, "A", null, 0, null, DateTime.UtcNow), new(3, "A2", 2, 0, null, DateTime.UtcNow),
        };

        Assert.Equal(["All tags", "Untagged", "A", "A / A2", "B"], QueryTagGrid.FilterChoices(tags));
        Assert.Null(QueryTagGrid.FilterValue("All tags"));
        Assert.Equal("untagged", QueryTagGrid.FilterValue("Untagged"));
    }
}
