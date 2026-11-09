/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

// #1776 own-store: this class creates and drops its own scratch database.
public sealed class ScopedSweepBatchLiveTests
{
    [Fact]
    public async Task BatchIncludesPredecessorsAndFiltersEvidenceWithoutChangingTimelineOrder()
    {
        var baseConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnection), "Set DARLING_TEST_PG to run scoped sweep batch tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnection!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var connection = await postgres.OpenConnectionAsync(ct))
            await PgMigrations.MigrateAsync(connection, null, ct);

        var at = new DateTime(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);
        FleetSweepRun Run(long id, long? previous, bool alerts) =>
            new(id, at.AddMinutes(id - 1003), at.AddHours(-1), at.AddMinutes(id - 1003), previous,
                alerts, 3, 3, true, "{}", """{"private":"operations-server"}""");
        var predecessor = Run(1001, null, false);
        var current = Run(1002, 1001, false);
        var newest = Run(1003, 1002, true);
        foreach (var run in new[] { predecessor, current, newest })
        {
            var verdicts = new[]
            {
                new FleetSweepServerVerdict(41, "finance-server", "Healthy", null, null, """{"has_data":true}"""),
                new FleetSweepServerVerdict(42, "operations-server", "Critical", null, "operations-server", """{"has_data":true}"""),
                new FleetSweepServerVerdict(43, "finance-departed", "Healthy", null, null, """{"has_data":true}"""),
            };
            await FleetSweepStore.RecordSweepAsync(postgres, run,
                run.SweepId == predecessor.SweepId ? verdicts : verdicts.Take(2).ToArray(),
                run.AlertsEnabled ? Array.Empty<FleetSweepWouldHavePagedEntry>() : new[]
                {
                    new FleetSweepWouldHavePagedEntry(41, "resources", """{"server":"finance-server"}"""),
                    new FleetSweepWouldHavePagedEntry(42, "resources", """{"server":"operations-server"}"""),
                },
                Array.Empty<FleetSweepWatchItem>(), ct);
        }

        // Only the two current runs are requested. The predecessor is outside this timeline,
        // but its permitted departed server must still appear in the first run's comparison.
        var scoped = await DarlingWebEndpoints.ScopeSweepsAsync(postgres,
            new[] { newest, current }, new[] { 41, 43 }, ct);
        Assert.Equal(new long[] { 1003, 1002 }, scoped.Select(run => run.SweepId));
        Assert.All(scoped, run =>
        {
            Assert.Equal(1, run.ServersExpected);
            Assert.Equal(1, run.ServersReported);
            Assert.DoesNotContain("operations-server", run.ReportJson, StringComparison.Ordinal);
        });
        var report = JsonNode.Parse(scoped[1].ReportJson)!;
        Assert.Equal("finance-departed", report["changes"]!["departed_servers"]![0]!.GetValue<string>());
        Assert.Single(report["would_have_paged"]!.AsArray());
        Assert.Null(JsonNode.Parse(scoped[0].ReportJson)!["would_have_paged"]);

        var empty = await DarlingWebEndpoints.ScopeSweepsAsync(postgres,
            new[] { newest, current }, Array.Empty<int>(), ct);
        Assert.All(empty, run =>
        {
            Assert.Equal(0, run.ServersExpected);
            Assert.Equal(0, run.ServersReported);
            Assert.DoesNotContain("finance-server", run.ReportJson, StringComparison.Ordinal);
            Assert.DoesNotContain("operations-server", run.ReportJson, StringComparison.Ordinal);
        });
        Assert.Empty(await DarlingWebEndpoints.ScopeSweepsAsync(postgres,
            Array.Empty<FleetSweepRun>(), new[] { 41 }, ct));

        // Presentation scoping never rewrites the stored whole-fleet record.
        Assert.Equal(current, await FleetSweepStore.GetSweepAsync(postgres, current.SweepId, ct));
    }
}
