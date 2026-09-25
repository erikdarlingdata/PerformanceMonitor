/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4214 part 2 / #4198: <c>get_store_host</c>'s own response-budget pin. Unlike the row-scaling #4198 tools
/// (get_query_store_regressions, get_blocking, ...), this tool's payload is a FIXED shape - platform/ram/
/// data_volume/store facts plus exactly one row per sizing-relevant setting (eight today) - so there is no
/// "plant N rows" scale knob; a single live call against the shared rig is the whole proof.
///
/// <para>Measured against the <c>not_managed</c> (bring-your-own) shape deliberately: every setting's
/// <c>source</c> text is the longest of the four verdicts' strings ("not-managed (bring-your-own store;
/// pg_settings.source = ...)"), so it is the more conservative of the two shapes for a byte count, not the
/// managed one a sized store would actually return.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpStoreHostBudgetLiveTests
{
    private readonly ITestOutputHelper _output;

    public DarlingMcpStoreHostBudgetLiveTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task GetStoreHost_BringYourOwnShape_StaysUnderResponseBudget()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the get_store_host budget test.");

        var ct = TestContext.Current.CancellationToken;
        await using (var connection = new NpgsqlConnection(cs))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(cs!);
        /* A fresh cache per test (round-1 review, Medium 2), not the production Shared singleton — this
           test's budget measurement must always hit the live gather, never a hit left warm by another test
           in the same process. */
        var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5));
        var json = await DarlingMcpStoreHostTools.GetStoreHost(postgres, new PostgresConfig { Managed = false }, cache);

        var bytes = Encoding.UTF8.GetByteCount(json);
        _output.WriteLine($"get_store_host (not_managed) call: {bytes:N0} bytes (budget {McpResponseBudget.DefaultBytes:N0}).");
        Assert.True(bytes < McpResponseBudget.DefaultBytes,
            $"get_store_host is {bytes:N0} bytes, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
    }
}
