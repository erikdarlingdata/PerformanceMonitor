/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The optimization view's five section reads each hold a pooled connection while they run, and the read pool is bounded, so one
/// read never has more than two of them in flight at once: the two 24-hour raw scans (wait categories and expensive queries) run
/// together, and the idle, tempdb and memory-grant reads run one at a time around them. The count comes from the tool's own
/// test-only seam around each section read, not from timing: the seam counts a section in when its read starts and out when it
/// completes, and the peak is what the shape allows.
/// </summary>
/* #1776 own-store: the fact builds its own scratch database and shares rows with nothing. */
public sealed class FinOpsOptimizationSectionFanOutLiveTests
{
    private const string ServerName = "darling-finops-fanout-a";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task TheOptimizationRead_HasAtMostTwoSectionReadsInFlight_AndReadsEverySection()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live optimization fan-out test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
        }

        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        var inFlight = 0;
        var peak = 0;
        var sections = new ConcurrentQueue<string>();
        DarlingMcpFinOpsTools.TestOnlyOptimizationSectionRead = async (section, read) =>
        {
            sections.Enqueue(section);
            var now = Interlocked.Increment(ref inFlight);
            int seen;
            while ((seen = Volatile.Read(ref peak)) < now && Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            try
            {
                await read();
            }
            finally
            {
                Interlocked.Decrement(ref inFlight);
            }
        };
        string body;
        try
        {
            body = await DarlingMcpFinOpsTools.GetFinOps(ds, "optimization", ServerName, 24, 10, cancellationToken: ct);
        }
        finally
        {
            DarlingMcpFinOpsTools.TestOnlyOptimizationSectionRead = null;
        }

        /* Every section was read through the seam, and the answer is the view's (an empty store answers the bare empty status). */
        Assert.Equal(
            new[] { "expensive_queries", "idle_databases", "memory_grant_efficiency", "tempdb_pressure", "wait_categories" },
            sections.OrderBy(s => s, StringComparer.Ordinal).ToArray());
        using var doc = JsonDocument.Parse(body);
        Assert.True(doc.RootElement.TryGetProperty("status", out var status) || doc.RootElement.TryGetProperty("view", out status), body);

        Assert.InRange(peak, 1, 2);
        Assert.Equal(0, inFlight);
    }
}
