/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
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
/// #4605 review round 2: drives both timeout phases of a compose run against a real store. The pool phase
/// exhausts a one-connection pool so the open itself times out; the statement phase blocks the panel's table
/// behind an ACCESS EXCLUSIVE lock so the client deadline fires inside the statement.
/// </summary>
public sealed class ComposeTimeoutPhaseLiveTests
{
    private const string PanelJson = "{\"panel\":{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}}";

    private static string BaseConnectionString
    {
        get
        {
            var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live compose timeout-phase tests.");
            return cs!;
        }
    }

    /// <summary>Opens the connection that fills the one-connection pool. The pool's own <c>Timeout=1</c> is the bar the
    /// open UNDER TEST must face, but the first physical open of a data source also loads types and can take longer
    /// than a second on a loaded machine, which failed this fixture before the exhausted open ever ran. A timed-out
    /// open leaves the pool as it was, so only this filling open is retried, for up to a minute; the open inside
    /// <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/> keeps the one-second bar.</summary>
    private static async Task<NpgsqlConnection> OpenHeldConnectionAsync(NpgsqlDataSource store, CancellationToken ct)
    {
        var giveUp = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                return await store.OpenConnectionAsync(ct);
            }
            catch (NpgsqlException ex) when (ex.InnerException is TimeoutException && giveUp.Elapsed < TimeSpan.FromSeconds(60))
            {
                /* The physical open missed the pool's one-second bar; try again. */
            }
        }
    }

    [Fact]
    public async Task AnExhaustedPool_IsAStoreConnectionError_NotTheStatementTimeoutText()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString, ct);
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString + ";Pooling=true;Maximum Pool Size=1;Timeout=1");
        await using var held = await OpenHeldConnectionAsync(store, ct);

        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(
            store, (JsonObject)JsonNode.Parse(PanelJson)!, ct, null, DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds, remapClientTimeout: true);

        Assert.True(outcome.IsServerError, outcome.Error);
        Assert.StartsWith("Error running query: could not get a store connection in time: ", outcome.Error, StringComparison.Ordinal);
        Assert.Contains("connection pool has been exhausted", outcome.Error, StringComparison.Ordinal);
        Assert.NotEqual(DarlingWebEndpoints.StatementTimeoutText, outcome.Error);
        Assert.NotEqual("57014", outcome.AuthorSqlState);
    }

    [Fact]
    public async Task ABlockedStatement_BecomesTheStatementTimeoutText_WhenTheClientDeadlineFires()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            await using var update = new NpgsqlCommand("INSERT INTO config.config_service (id, updated_at, compose_statement_timeout_seconds) VALUES (1, now() AT TIME ZONE 'UTC', 5) ON CONFLICT (id) DO UPDATE SET compose_statement_timeout_seconds = EXCLUDED.compose_statement_timeout_seconds", setup);
            await update.ExecuteNonQueryAsync(ct);
        }

        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        Assert.Equal(5, await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(store, ct));
        await using var locker = new NpgsqlConnection(scratch.ConnectionString);
        await locker.OpenAsync(ct);
        await using var tx = await locker.BeginTransactionAsync(ct);
        try
        {
            await using (var lockCommand = new NpgsqlCommand("LOCK TABLE collect.query_stats IN ACCESS EXCLUSIVE MODE", locker, tx))
            {
                await lockCommand.ExecuteNonQueryAsync(ct);
            }

            var clock = Stopwatch.StartNew();
            var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(
                store, (JsonObject)JsonNode.Parse(PanelJson)!, ct, null, DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds, remapClientTimeout: true);
            clock.Stop();

            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(60), $"The 5 s compose timeout plus headroom should bound the measure query; it took {clock.Elapsed.TotalSeconds:F1} s.");
            Assert.Equal("57014", outcome.AuthorSqlState);
            Assert.Equal(DarlingWebEndpoints.StatementTimeoutText, outcome.Error);
        }
        finally
        {
            await tx.RollbackAsync(CancellationToken.None);
        }
    }
}
