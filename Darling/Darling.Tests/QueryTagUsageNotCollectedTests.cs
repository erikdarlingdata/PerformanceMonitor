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
/// #7374: the Viewer's "Resource use by query tag" window, set to all servers, listed a PostgreSQL server in all six SQL Server sections:
/// "empty" in five and "unavailable" (the Query Store gate's "no coverage row") in the sixth, which the top message named as a section not
/// served. A PostgreSQL server collects none of these sources, so its table reads not_collected with the engine reason, as
/// <c>get_resource_use_by_tag</c> answers it.
/// </summary>
public sealed class QueryTagUsageNotCollectedTests
{
    private static DarlingServer Sql(int id, string name) => new(id, name, name, true, 15);

    private static DarlingServer Postgres(int id, string name) => new(id, name, name, true, null, engineKind: "postgres");

    [Fact]
    public void NotCollectedReasons_NamesAPostgresServerWithTheEngineReason_AndSkipsASqlServer()
    {
        var reasons = QueryTagUsageView.NotCollectedReasons([Sql(1, "sql-one"), Postgres(2, "pg-one")]);

        Assert.Equal([2], reasons.Keys);
        Assert.Contains("pg-one", reasons[2], StringComparison.Ordinal);
    }

    [Fact]
    public void AllServersBuild_ReadsAPostgresServerNotCollectedInEverySection_AndNeverNamesItAsNotServed()
    {
        var servers = new[] { Sql(1, "sql-one"), Postgres(2, "pg-one") };
        var data = new QueryTagRollupData(
            [], new Dictionary<string, List<QueryTagRollupFact>>(), [], false,
            new Dictionary<string, string>(), new Dictionary<int, string>(), new Dictionary<string, string>());

        var result = QueryTagUsageView.Build(
            data, servers.Select(s => (Id: s.ServerId, Name: s.DisplayName)).ToList(), allServers: true, tagFilter: null,
            notCollected: QueryTagUsageView.NotCollectedReasons(servers));

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
}
