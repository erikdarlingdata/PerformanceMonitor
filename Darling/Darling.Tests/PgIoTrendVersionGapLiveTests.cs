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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: a scratch database made through ScratchPostgres. */

/// <summary>
/// #8918: <c>get_pg_io_trend</c> on a PostgreSQL 15 target says <c>not_collected</c> (pg_stat_io arrived in 16), as <c>get_pg_io_stats</c> does,
/// and not the idle-window <c>empty</c>. A PostgreSQL 16 target with no I/O keeps the idle-window wording. The Viewer's Overview collector row
/// and I/O note read the same version check.
/// </summary>
public sealed class PgIoTrendVersionGapLiveTests
{
    private const int Pg15 = -891801;
    private const int Pg16 = -891802;

    private static async Task RegisterAsync(NpgsqlConnection store, int id, int major, System.Threading.CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(store, ct,
            @"INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, postgres_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $4, $5, $5)",
            id, "io-gap-" + major, MonitoredEngineKind.Postgres, major, DarlingMcpTestData.Naive(DateTime.UtcNow));
    }

    [Fact]
    public async Task Pg15IsNotCollected_Pg16WithNoIoKeepsTheIdleWording()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await RegisterAsync(store, Pg15, 15, ct);
            await RegisterAsync(store, Pg16, 16, ct);

            using var old = JsonDocument.Parse(await DarlingMcpPgTrendTools.GetPgIoTrend(dataSource, "io-gap-15", null, null, 24, null, null, null, ct));
            Assert.Equal("not_collected", old.RootElement.GetProperty("status").GetString());
            var oldText = old.RootElement.GetRawText();
            Assert.Contains("PostgreSQL 15", oldText, StringComparison.Ordinal);
            Assert.DoesNotContain("idle", oldText, StringComparison.OrdinalIgnoreCase);

            using var named = JsonDocument.Parse(await DarlingMcpPgTrendTools.GetPgIoTrend(dataSource, "io-gap-15", "client backend", "normal", 24, null, null, null, ct));
            Assert.Equal("not_collected", named.RootElement.GetProperty("status").GetString());

            using var current = JsonDocument.Parse(await DarlingMcpPgTrendTools.GetPgIoTrend(dataSource, "io-gap-16", null, null, 24, null, null, null, ct));
            Assert.Equal("empty", current.RootElement.GetProperty("status").GetString());
            Assert.Contains("genuinely idle", current.RootElement.GetRawText(), StringComparison.Ordinal);

            /* The Viewer's Overview row for the collector: a gap on 15, a collector to chase on 16. */
            var collectors = ViewerPostgresTabs.PostgresCollectors().Where(c => c.Name == "pg_io_stats").ToList();
            var facts = new List<ViewerDataService.PostgresCollectorLogFacts>();
            var row15 = ViewerDataService.BuildPostgresCollectorHealth(
                new DarlingServer(Pg15, "io-gap-15", "io-gap-15", true, null, engineKind: MonitoredEngineKind.Postgres, postgresMajorVersion: 15), collectors, facts).Single();
            Assert.True(row15.IsPermanentGap);
            Assert.False(row15.IsFault);
            Assert.Contains("PostgreSQL 15", row15.Explanation, StringComparison.Ordinal);
            var row16 = ViewerDataService.BuildPostgresCollectorHealth(
                new DarlingServer(Pg16, "io-gap-16", "io-gap-16", true, null, engineKind: MonitoredEngineKind.Postgres, postgresMajorVersion: 16), collectors, facts).Single();
            Assert.False(row16.IsPermanentGap);
            Assert.True(row16.IsFault);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
