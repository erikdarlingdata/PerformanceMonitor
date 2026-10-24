/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres. */
/// <summary>
/// A composed panel's own connection runs its compiled statement inside a read-only transaction, on a pool whose login holds write
/// grants: a statement that tries to change something is refused by PostgreSQL (25006), and a read still returns its rows.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class ComposeReadOnlyTransactionLiveTests
{
    private const string PanelJson = "{\"panel\":{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}}";

    private static readonly ComposeRoute RawRoute = new(ComposeSourceTier.Raw, null);

    private static string BaseConnectionString
    {
        get
        {
            var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live compose read-only transaction tests.");
            return cs!;
        }
    }

    [Fact]
    public async Task ACompiledRead_StillReturnsItsRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString, ct);
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);

        var rows = await ComposePanelRunner.RunComposedQueryAsync(
            store, new ComposeCompiled("SELECT 7 AS n, 'x'::text AS label", new List<NpgsqlParameter>(), RawRoute), 30, ct);

        var row = Assert.Single(rows)!.AsObject();
        Assert.Equal(7, row["n"]!.GetValue<int>());
        Assert.Equal("x", row["label"]!.GetValue<string>());
    }

    [Fact]
    public async Task ACompiledStatementThatWrites_IsRefusedByPostgres_AndChangesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString, ct);
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var setup = store.CreateCommand("CREATE TABLE public.compose_readonly_probe (x int)"))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        var ex = await Assert.ThrowsAsync<PostgresException>(() => ComposePanelRunner.RunComposedQueryAsync(
            store, new ComposeCompiled("INSERT INTO public.compose_readonly_probe (x) VALUES (1) RETURNING x", new List<NpgsqlParameter>(), RawRoute), 30, ct));

        Assert.Equal("25006", ex.SqlState);
        await using var count = store.CreateCommand("SELECT count(*) FROM public.compose_readonly_probe");
        Assert.Equal(0L, (long)(await count.ExecuteScalarAsync(ct))!);
    }

    [Fact]
    public async Task AComposedPanel_StillRunsEndToEnd()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
        }

        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);

        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(
            store, (JsonObject)JsonNode.Parse(PanelJson)!, ct, null, DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds, remapClientTimeout: true);

        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");
        Assert.NotNull(outcome.Payload!["rows"]);
    }
}
