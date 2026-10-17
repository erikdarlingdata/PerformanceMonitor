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
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behaviour pins for the Lite resource-use-by-tag roll-up (#5992) against a real DuckDB, with <c>query_tag_map</c> seeded
/// directly. One test per behaviour in the issue's "Semantics to pin" list, plus the deadlock rule.
/// </summary>
public sealed class LiteQueryTagRollupTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerOne = 9101;
    private const int ServerTwo = 9102;
    private static readonly (int Id, string Name)[] Scope = [(ServerOne, "one"), (ServerTwo, "two")];

    private readonly LocalDataService _service;
    private readonly SharedDuckDbFixture _fixture;
    private long _nextId = 1_000_000;

    public LiteQueryTagRollupTests(SharedDuckDbFixture fixture)
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

    private static DateTime Ago(int minutes) => DateTime.UtcNow.AddMinutes(-minutes);

    private Task PlanAsync(int server, string hash, long cpuUs, long executions = 1) => ExecAsync(
        "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', 'db1', $4, $5, $6, $6, 10, 1)",
        _nextId++, Ago(30), server, hash, executions, cpuUs);

    private Task MapAsync(int server, string kind, string key, int tagId) => ExecAsync(
        "INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ($1, $2, $3, $4, 'hand_hash')", server, kind, key, tagId);

    private Task DeadlockAsync(int server, params string[] apps) => ExecAsync(
        "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, 'x', $2, $4, 'db1')",
        _nextId++, Ago(15), server,
        "<deadlock><process-list>" + string.Concat(apps.Select((a, i) => $"<process id=\"p{i}\" clientapp=\"{a}\" loginname=\"lg\" hostname=\"h\" currentdbname=\"db1\" />")) + "</process-list></deadlock>");

    private async Task<QueryTagRollupData> ReadAsync() =>
        await _service.GetQueryTagRollupDataAsync(Scope.Select(s => s.Id).ToList(), Ago(120), DateTime.UtcNow);

    private static Dictionary<string, object?> Plan(QueryTagRollupData data, IReadOnlyList<(int Id, string Name)> scope, bool all, int? tag = null) =>
        QueryTagRollup.Section(QueryTagRollupSection.PlanCache, data.Sections["plan_cache"], scope, all, data.Tags, tag, 25);

    private static double Fig(object? figures, string measure) => Convert.ToDouble(((Dictionary<string, object>)figures!)[measure], CultureInfo.InvariantCulture);

    private static Dictionary<string, object?> RowOf(Dictionary<string, object?> section, string tag) =>
        ((List<Dictionary<string, object?>>)section["tag_rows"]!).Single(r => (string)r["tag"]! == tag);

    [Fact]
    public async Task PlanCache_TwoKeysTaggedAndOneUntagged_GiveTheTagTheUntaggedRowAndTheirSum()
    {
        var a = await _service.CreateQueryTagAsync("A", null);
        await PlanAsync(ServerOne, "0x01", 4_000_000);
        await PlanAsync(ServerOne, "0x02", 2_000_000);
        await PlanAsync(ServerOne, "0x03", 1_000_000);
        await MapAsync(ServerOne, "hash", "0x01", a);
        await MapAsync(ServerOne, "hash", "0x02", a);

        var section = Plan(await ReadAsync(), [Scope[0]], all: false);

        Assert.Equal(6000, Fig(RowOf(section, "A")["own"], "cpu_ms"));
        Assert.Equal(1000, Fig(section["untagged"], "cpu_ms"));
        Assert.Equal(7000, Fig(section["total"], "cpu_ms"));
    }

    [Fact]
    public async Task AllServersView_ListsEachServerAndTotalsAcrossThem()
    {
        var a = await _service.CreateQueryTagAsync("A", null);
        await PlanAsync(ServerOne, "0x01", 4_000_000);
        await PlanAsync(ServerTwo, "0x01", 1_000_000);
        await MapAsync(ServerOne, "hash", "0x01", a);

        var section = Plan(await ReadAsync(), Scope, all: true);

        var servers = ((List<object>)section["servers"]!).Cast<Dictionary<string, object?>>().ToList();
        Assert.Equal(["one", "two"], servers.Select(s => (string)s["server"]!).ToArray());
        Assert.Equal(4000, Fig(RowOf(servers[0], "A")["own"], "cpu_ms"));
        /* A key tagged on one server is untagged on another. */
        Assert.Equal(1000, Fig(servers[1]["untagged"], "cpu_ms"));
        Assert.Equal(5000, Fig(section["fleet_total"], "cpu_ms"));
    }

    [Fact]
    public async Task ParentSubtreeFigure_IncludesTheChild_AndItsOwnFigureDoesNot()
    {
        var parent = await _service.CreateQueryTagAsync("Parent", null);
        var child = await _service.CreateQueryTagAsync("Child", parent);
        await PlanAsync(ServerOne, "0x01", 3_000_000);
        await PlanAsync(ServerOne, "0x02", 2_000_000);
        await MapAsync(ServerOne, "hash", "0x01", parent);
        await MapAsync(ServerOne, "hash", "0x02", child);

        var section = Plan(await ReadAsync(), [Scope[0]], all: false);

        Assert.Equal(3000, Fig(RowOf(section, "Parent")["own"], "cpu_ms"));
        Assert.Equal(5000, Fig(RowOf(section, "Parent")["subtree"], "cpu_ms"));
        Assert.Equal(2000, Fig(RowOf(section, "Child")["subtree"], "cpu_ms"));
        Assert.Equal(5000, Fig(section["total"], "cpu_ms"));
    }

    [Fact]
    public async Task Deadlock_CountsOnceForEachTagAmongItsProcesses()
    {
        var a = await _service.CreateQueryTagAsync("A", null);
        var b = await _service.CreateQueryTagAsync("B", null);
        await DeadlockAsync(ServerOne, "appA", "appB", "appC");
        await MapAsync(ServerOne, "caller", QueryTagRollup.CallerKey("appA", "lg", "h", "db1"), a);
        await MapAsync(ServerOne, "caller", QueryTagRollup.CallerKey("appB", "lg", "h", "db1"), b);

        var data = await ReadAsync();
        var section = QueryTagRollup.DeadlockSection(data.Deadlocks, [Scope[0]], false, data.Tags, null, 25, data.ScanTruncated);

        Assert.Equal(1, Fig(RowOf(section, "A")["own"], "deadlocks"));
        Assert.Equal(1, Fig(RowOf(section, "B")["own"], "deadlocks"));
        Assert.Equal(1, Fig(section["untagged"], "deadlocks"));
        Assert.Equal(1, Fig(section["total"], "deadlocks"));
        Assert.Equal(3, Fig(section["total"], "processes"));
        Assert.Equal(false, section["rows_add_up_to_total"]);
    }

    /// <summary>
    /// The shared fixture: seeded plan-cache rows (hash, cpu microseconds, executions) with the map, and the figures both
    /// stores must answer. Darling's roll-up suite should seed the same rows and expect the same figures.
    /// </summary>
    public static readonly (string Hash, long CpuUs, long Executions, string? Tag)[] ParityRows =
    [
        ("0x01", 4_000_000, 2, "A"), ("0x02", 2_000_000, 3, "A"), ("0x03", 1_000_000, 5, null),
    ];

    public static readonly (string Tag, double OwnCpuMs, double OwnExecutions)[] ParityExpected =
        [("A", 6000, 5), ("untagged", 1000, 5)];

    [Fact]
    public async Task SharedFixture_LiteAnswersTheFixturesFigures()
    {
        var a = await _service.CreateQueryTagAsync("A", null);
        foreach (var row in ParityRows)
        {
            await PlanAsync(ServerOne, row.Hash, row.CpuUs, row.Executions);
            if (row.Tag is not null)
            {
                await MapAsync(ServerOne, "hash", row.Hash, a);
            }
        }

        var section = Plan(await ReadAsync(), [Scope[0]], all: false);

        foreach (var (tag, cpu, executions) in ParityExpected)
        {
            var figures = tag == "untagged" ? section["untagged"] : RowOf(section, tag)["own"];
            Assert.Equal(cpu, Fig(figures, "cpu_ms"));
            Assert.Equal(executions, Fig(figures, "executions"));
        }
    }
}
