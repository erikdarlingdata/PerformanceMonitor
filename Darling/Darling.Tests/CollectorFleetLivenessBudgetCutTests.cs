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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9202: the fleet per-collector reply is held to the response budget in whole rows, applicable servers first. When the applicable
/// rows fit and the not-applicable list does not, the cut falls inside the not-applicable list: every applicable row stays, part of
/// that list is left out, and the counts and the note still cover every server.
/// </summary>
public sealed class CollectorFleetLivenessBudgetCutTests
{
    private const int SqlServerEdition = 2;

    [Fact]
    public void ACutInsideTheNotApplicableList_KeepsEveryApplicableRow_AndCountsEveryServer()
    {
        const int applicableServers = 20;
        const int gapServers = 150;
        var now = DateTime.UtcNow;
        var servers = new List<CollectorFleetLiveness.ServerFacts>();
        var runs = new List<CollectorFleetLiveness.RunFacts>();
        for (var i = 0; i < applicableServers; i++)
        {
            servers.Add(new(i, $"ok-{i:D3}", null, SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, 16));
            runs.Add(new(i, now.AddMinutes(-5), null, 10, 0, null, null, SuccessRuns: 10, LastRunUtc: now.AddMinutes(-5), LastNonSkipUtc: now.AddMinutes(-5), LastProductiveUtc: now.AddMinutes(-5)));
        }

        /* A server below the supported SQL Server floor is a version gap for this collector, so it is listed under not_applicable with a reason. */
        for (var i = 0; i < gapServers; i++)
        {
            servers.Add(new(applicableServers + i, $"old-{i:D3}", null, SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, 11));
        }

        var json = CollectorFleetLiveness.Build("query_snapshots", 24, now.AddHours(-24), now, servers, runs);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        Assert.False(string.IsNullOrEmpty(root.GetProperty("budget_note").GetString()));

        /* Every applicable row is kept, and the not-applicable list is cut part way: whole rows, not none and not all. */
        Assert.Equal(applicableServers, root.GetProperty("servers").GetArrayLength());
        var keptGaps = root.GetProperty("not_applicable").GetArrayLength();
        Assert.InRange(keptGaps, 1, gapServers - 1);
        Assert.Equal("old-000", root.GetProperty("not_applicable")[0].GetProperty("server_name").GetString());

        /* The counts at the top are over every server, whatever the page kept. */
        Assert.Equal(applicableServers, root.GetProperty("servers_with_a_run").GetInt32());
        Assert.Equal(gapServers, root.GetProperty("servers_not_applicable").GetInt32());
        Assert.Equal(0, root.GetProperty("servers_failing").GetInt32());
        Assert.Contains($"{applicableServers + keptGaps}", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        Assert.Contains($"{applicableServers + gapServers}", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AReplyThatFits_IsNotCut_AndCarriesNoBudgetNote()
    {
        var now = DateTime.UtcNow;
        var servers = new List<CollectorFleetLiveness.ServerFacts>
        {
            new(1, "ok-1", null, SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, 16),
            new(2, "old-1", null, SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, 11),
        };
        var runs = new List<CollectorFleetLiveness.RunFacts> { new(1, now.AddMinutes(-5), null, 3, 0, null, null, SuccessRuns: 3) };

        using var doc = JsonDocument.Parse(CollectorFleetLiveness.Build("query_snapshots", 24, now.AddHours(-24), now, servers, runs));

        Assert.False(doc.RootElement.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("budget_note").ValueKind);
        Assert.Equal(1, doc.RootElement.GetProperty("servers").GetArrayLength());
        Assert.Equal(1, doc.RootElement.GetProperty("not_applicable").GetArrayLength());
    }
}
