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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4689: a composed Query Store panel that reads the interval table below raw's chunk floor binds the runner's
/// exact start (<see cref="ComposeRunContext.QueryStoreWideStart"/>), not the window start. Seeds through the
/// real write path, forces <c>filled_since</c> to S + 12 h so the seed's backdated table rows (S + 1 h) sit
/// outside the coverage claim, drops raw's older chunks, then runs the panel the way the endpoint does.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class ComposeQueryStoreWideStartLiveTests
{
    private const int ServerId = -4689002;
    private const string ServerName = "qsiw-compose-start";

    private static readonly DateTime S = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-6), DateTimeKind.Unspecified);

    private const string PanelJson =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    [Fact]
    public async Task ComposePanel_BindsTheExactWideStart_NotTheWindowStart()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4689 Compose live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, S, ct);
        await ExecAsync(connection,
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
            + $"VALUES ({ServerId}, '{ServerName}', '{ServerName}', TRUE, 16, now(), now()) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct);
        await ExecAsync(connection,
            $"UPDATE collect.query_store_interval_wide_coverage SET filled_since = TIMESTAMP '{S.AddHours(12):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);

        var end = (DateTime)(await ScalarAsync(connection,
            $"SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = {ServerId}", ct))!;
        await ExecAsync(connection,
            $"SELECT drop_chunks('collect.query_store_stats', older_than => TIMESTAMP '{S.AddDays(1):yyyy-MM-dd HH:mm:ss}')", ct);

        /* The runner's own eligibility step, through the product path. */
        var resolution = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(postgres, null, S, end, end, ct);
        Assert.True(resolution.Eligible);
        Assert.Equal(S.AddHours(12), resolution.WideStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.FilledSince, resolution.Bound);

        var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, ct);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(PanelJson)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        var served = await RunBucketsAsync(connection, plan!,
            new ComposeRunContext(null, S, end, ComposeRunContext.NoVariables, rollups, end, coverage, resolution.Eligible, resolution.WideStart), ct);
        Assert.NotEmpty(served);
        Assert.True(served.Min() >= S.AddHours(12), $"the panel must not include intervals below filled_since; earliest bucket {served.Min():o}");

        /* The window-start bind (what the panel did before the exact start was bound) does include the
           backdated table rows, so the bound above is what excludes them. */
        var unbound = await RunBucketsAsync(connection, plan!,
            new ComposeRunContext(null, S, end, ComposeRunContext.NoVariables, rollups, end, coverage, true), ct);
        Assert.True(unbound.Min() < S.AddHours(12), "expected the unbound read to include the backdated table rows");

        Assert.Equal(S.AddHours(12), resolution.WideStart);
        Assert.Equal(ServerName, resolution.SettingServer);
        var note = DarlingWebEndpoints.QueryStoreHistoryNote(resolution.WideStart!.Value, resolution.Bound, resolution.SettingServer);
        Assert.Contains("interval table began keeping complete history for " + ServerName, note, StringComparison.Ordinal);
    }

    private static async Task<List<DateTime>> RunBucketsAsync(NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        var buckets = new List<DateTime>();
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            buckets.Add(reader.GetDateTime(0));
        }

        return buckets;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
