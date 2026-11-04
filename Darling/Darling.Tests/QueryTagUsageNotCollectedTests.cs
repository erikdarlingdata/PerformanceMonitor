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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7374: the Viewer's "Resource use by query tag" window listed a PostgreSQL server in all six SQL Server sections: "empty" in five and
/// "unavailable" (the Query Store gate's "no coverage row") in the sixth, which the top message named as a section not served. A PostgreSQL
/// server that the read found no rows for collects none of these sources, so its table reads not_collected with the engine reason, as
/// <c>get_resource_use_by_tag</c> answers it. The engine question is asked AFTER the read (both scopes), so a server whose registry row says
/// PostgreSQL but that holds SQL Server rows keeps them.
/// </summary>
public sealed class QueryTagUsageNotCollectedTests
{
    private static DarlingServer Sql(int id, string name) => new(id, name, name, true, 15);

    private static DarlingServer Postgres(int id, string name) => new(id, name, name, true, null, engineKind: "postgres");

    private static QueryTagRollupData Data(
        Dictionary<string, List<QueryTagRollupFact>>? sections = null, Dictionary<string, string>? unavailable = null,
        Dictionary<int, string>? refused = null, Dictionary<string, string>? notes = null) =>
        new([], sections ?? new Dictionary<string, List<QueryTagRollupFact>>(), [], false,
            unavailable ?? new Dictionary<string, string>(), refused ?? new Dictionary<int, string>(), notes ?? new Dictionary<string, string>());

    private static QueryTagRollupFact Fact(int serverId) => new(serverId, null, new double[QueryTagRollupSection.PlanCache.Measures.Count]);

    private static List<(int Id, string Name)> Scope(IEnumerable<DarlingServer> servers) => servers.Select(s => (Id: s.ServerId, Name: s.DisplayName)).ToList();

    [Fact]
    public void NotCollectedReasons_NamesAPostgresServerWithNoRows_AndSkipsASqlServer()
    {
        var reasons = QueryTagUsageView.NotCollectedReasons([Sql(1, "sql-one"), Postgres(2, "pg-one")], Data());

        Assert.Equal([2], reasons.Keys);
        Assert.Contains("pg-one", reasons[2], StringComparison.Ordinal);
    }

    [Fact]
    public void NotCollectedReasons_LeavesOutAPostgresServerThatHoldsRows_InAnySection()
    {
        var planCache = new Dictionary<string, List<QueryTagRollupFact>> { [QueryTagRollupSection.PlanCache.Name] = [Fact(2)] };
        var data = Data(planCache) with { Deadlocks = [new QueryTagDeadlockFact(3, new Dictionary<int, int>(), 1)] };

        var reasons = QueryTagUsageView.NotCollectedReasons([Postgres(2, "pg-rows"), Postgres(3, "pg-deadlock"), Postgres(4, "pg-none")], data);

        Assert.Equal([4], reasons.Keys);
    }

    [Fact]
    public void AllServersBuild_ReadsAPostgresServerNotCollectedInEverySection_AndNeverNamesItAsNotServed()
    {
        var servers = new[] { Sql(1, "sql-one"), Postgres(2, "pg-one") };
        var data = Data();

        var result = QueryTagUsageView.Build(
            data, Scope(servers), allServers: true, tagFilter: null, notCollected: QueryTagUsageView.NotCollectedReasons(servers, data));

        var postgres = result.Tables.Where(t => t.Title.EndsWith(" - pg-one", StringComparison.Ordinal)).ToList();
        Assert.Equal(6, postgres.Count);
        Assert.All(postgres, t =>
        {
            Assert.Equal("not_collected", t.Status);
            Assert.Contains("pg-one", t.Message!, StringComparison.Ordinal);
        });
        Assert.All(result.Tables.Where(t => t.Title.EndsWith(" - sql-one", StringComparison.Ordinal)), t => Assert.Equal("empty", t.Status));
        Assert.DoesNotContain("pg-one", result.Message ?? string.Empty, StringComparison.Ordinal);
    }

    /// <summary>A PostgreSQL-registered server that holds rows in the window keeps them: no table of it reads not_collected.</summary>
    [Fact]
    public void AllServersBuild_KeepsTheRowsOfAPostgresRegisteredServerThatHoldsSqlServerRows()
    {
        var servers = new[] { Sql(1, "sql-one"), Postgres(2, "pg-rows") };
        var data = Data(new Dictionary<string, List<QueryTagRollupFact>> { [QueryTagRollupSection.PlanCache.Name] = [Fact(2)] });

        var result = QueryTagUsageView.Build(
            data, Scope(servers), allServers: true, tagFilter: null, notCollected: QueryTagUsageView.NotCollectedReasons(servers, data));

        Assert.DoesNotContain(result.Tables, t => t.Status == "not_collected");
        Assert.Equal("ok", result.Tables.Single(t => t.Title.StartsWith("Plan cache", StringComparison.OrdinalIgnoreCase) && t.Title.EndsWith(" - pg-rows", StringComparison.Ordinal)).Status);
    }

    /// <summary>The gate refused every server read, so Query Store is "not readable here" with the refusal reasons. The PostgreSQL server's
    /// refusal ("no coverage row") says nothing true about it, so only the SQL Server's reason is given.</summary>
    [Fact]
    public void Build_LeavesAPostgresServersGateRefusalOutOfTheQueryStoreNotServedText()
    {
        var servers = new[] { Sql(1, "sql-one"), Postgres(2, "pg-one") };
        var refused = new Dictionary<int, string> { [1] = "sql refusal reason", [2] = "pg refusal reason" };
        var data = Data(
            unavailable: new Dictionary<string, string> { [QueryTagRollupSection.QueryStore.Name] = QueryTagUsageNotes.QueryStoreNotReadable(refused.Values) },
            refused: refused);

        var result = QueryTagUsageView.Build(
            data, Scope(servers), allServers: true, tagFilter: null, notCollected: QueryTagUsageView.NotCollectedReasons(servers, data));

        Assert.Contains("sql refusal reason", result.Message!, StringComparison.Ordinal);
        Assert.DoesNotContain("pg refusal reason", result.Message!, StringComparison.Ordinal);
    }

    /// <summary>A section note says how the read reached the window: it is copied onto each read server's table, not onto a server that reads not_collected.</summary>
    [Fact]
    public void Build_DoesNotCopyASectionNoteOntoAPostgresServersNotCollectedTable()
    {
        var servers = new[] { Sql(1, "sql-one"), Postgres(2, "pg-one") };
        var data = Data(notes: new Dictionary<string, string> { [QueryTagRollupSection.PlanCache.Name] = "hourly rollup note" });

        var result = QueryTagUsageView.Build(
            data, Scope(servers), allServers: true, tagFilter: null, notCollected: QueryTagUsageView.NotCollectedReasons(servers, data));

        var plan = result.Tables.Where(t => t.Section == QueryTagRollupSection.PlanCache.Name).ToList();
        Assert.Equal("hourly rollup note", plan.Single(t => t.Title.EndsWith(" - sql-one", StringComparison.Ordinal)).Note);
        Assert.Null(plan.Single(t => t.Title.EndsWith(" - pg-one", StringComparison.Ordinal)).Note);
    }
}
