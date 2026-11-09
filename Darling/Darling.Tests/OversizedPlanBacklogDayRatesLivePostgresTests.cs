/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8404: <c>get_oversized_plan_backlog</c> carries, beside each server's pending figure, the rows added, captured
/// and expired over the day before the read, and the sweep's own ceiling per server per day, so a reader can tell a
/// backlog that is draining from one that is not, and a drain at the ceiling from a drain that has stalled. The
/// three day counts are stamped against the read's own clock, so the fixture is seeded relative to it: one row of
/// each kind inside the day and one outside it.
/// </summary>
public sealed class OversizedPlanBacklogDayRatesLivePostgresTests
{
    private const int ServerId = 4101;
    private const string ServerName = "day-rates-server";

    /// <summary>
    /// Seven rows for one server. Stamps are parameters, offsets from the read's clock: <c>$1</c> two hours
    /// back, <c>$2</c> thirty minutes back, <c>$3</c> one hour back, <c>$4</c> two days back, <c>$5</c> three
    /// days back.
    /// </summary>
    private const string ServerSql = @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4101, 'day-rates-server', TRUE);";

    private const string FixtureSql = @"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at,
 plan_xml, captured_at, expired_at, last_attempt_at, attempt_count)
VALUES
    /* pending, first sighted two hours ago: added over the last day */
    (4101, 'query_stats', '0xADDED_TODAY', '0xSQL1', 0, 400, 'fixture_db', '0xHASH1',
     1000000, $1, $1, NULL, NULL, NULL, NULL, 0),
    /* pending, first sighted three days ago */
    (4101, 'query_stats', '0xADDED_EARLIER', '0xSQL2', 0, 400, 'fixture_db', '0xHASH2',
     900000, $5, $1, NULL, NULL, NULL, NULL, 0),
    /* captured an hour ago */
    (4101, 'query_stats', '0xCAPTURED_TODAY', '0xSQL3', 0, 400, 'fixture_db', '0xHASH3',
     800000, $5, $1, '<ShowPlanXML />', $3, NULL, $3, 1),
    /* captured two days ago */
    (4101, 'query_stats', '0xCAPTURED_EARLIER', '0xSQL4', 0, 400, 'fixture_db', '0xHASH4',
     750000, $5, $4, '<ShowPlanXML />', $4, NULL, $4, 1),
    /* expired thirty minutes ago */
    (4101, 'procedure_stats', '0xEXPIRED_TODAY', '0xSQL5', 0, 0, 'fixture_db', NULL,
     600000, $5, $1, NULL, NULL, $2, $2, 1),
    /* expired two days ago */
    (4101, 'procedure_stats', '0xEXPIRED_EARLIER', '0xSQL6', 0, 0, 'fixture_db', NULL,
     550000, $5, $4, NULL, NULL, $4, $4, 1),
    /* both stamps inside the day: captured, and counted as captured, never as expired */
    (4101, 'procedure_stats', '0xBOTH_TODAY', '0xSQL7', 0, 0, 'fixture_db', NULL,
     700000, $5, $1, NULL, $3, $2, $3, 2);";

    [Fact]
    public async Task TheRead_CountsTheDayBeforeIt_BesidePending_AndNamesTheSweepsCeiling()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8404 backlog day-rate test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);

        var seeded = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
            await using (var registry = new NpgsqlCommand(ServerSql, migrate))
            {
                await registry.ExecuteNonQueryAsync(ct);
            }
            await using var fixture = new NpgsqlCommand(FixtureSql, migrate);
            fixture.Parameters.AddWithValue(seeded.AddHours(-2));
            fixture.Parameters.AddWithValue(seeded.AddMinutes(-30));
            fixture.Parameters.AddWithValue(seeded.AddHours(-1));
            fixture.Parameters.AddWithValue(seeded.AddDays(-2));
            fixture.Parameters.AddWithValue(seeded.AddDays(-3));
            await fixture.ExecuteNonQueryAsync(ct);
        }

        /* The reads resolve `servers` through the search path, exactly as the MCP host's pool does. */
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var before = DateTime.UtcNow;
        var json = await DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(postgres, cancellationToken: ct);
        var after = DateTime.UtcNow;

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        /* The sweep's own ceiling, from its two constants: plans per server per pass times passes per day. */
        Assert.True(root.TryGetProperty("sweep_ceiling_rows_per_server_per_day", out var ceiling),
            "the payload carries no sweep_ceiling_rows_per_server_per_day: " + Head(json));
        Assert.Equal(
            OversizedPlanBacklogSweep.MaxPlansPerServerPerTick * (int)(TimeSpan.FromDays(1) / OversizedPlanBacklogSweep.SweepInterval),
            ceiling.GetInt32());

        /* The day the three counts cover starts one day before the read, stamped as UTC. */
        Assert.True(root.TryGetProperty("last_day_since", out var since), "the payload carries no last_day_since: " + Head(json));
        var sinceUtc = DateTime.Parse(since.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange(sinceUtc, before.AddDays(-1).AddSeconds(-1), after.AddDays(-1).AddSeconds(1));

        var server = Assert.Single(root.GetProperty("by_server").EnumerateArray());
        Assert.Equal(ServerId, server.GetProperty("server_id").GetInt32());
        Assert.Equal(ServerName, server.GetProperty("server_name").GetString());

        /* The partition is as before: seven rows, two pending, three captured (one of them also stamped expired), two expired. */
        Assert.Equal(7L, server.GetProperty("total_rows").GetInt64());
        Assert.Equal(2L, server.GetProperty("pending_rows").GetInt64());
        Assert.Equal(3L, server.GetProperty("captured_rows").GetInt64());
        Assert.Equal(2L, server.GetProperty("expired_rows").GetInt64());

        /* And the day before the read: one row first sighted, two captured, one expired. The rows stamped two and
           three days back are outside it, and the row carrying both stamps counts as captured, as the partition does. */
        Assert.True(server.TryGetProperty("rows_added_last_day", out var added), "by_server carries no rows_added_last_day: " + Head(json));
        Assert.Equal(1L, added.GetInt64());
        Assert.True(server.TryGetProperty("rows_captured_last_day", out var captured), "by_server carries no rows_captured_last_day: " + Head(json));
        Assert.Equal(2L, captured.GetInt64());
        Assert.True(server.TryGetProperty("rows_expired_last_day", out var expired), "by_server carries no rows_expired_last_day: " + Head(json));
        Assert.Equal(1L, expired.GetInt64());

        /* The note tells a reader how to use them. */
        var note = root.GetProperty("note").GetString() ?? "";
        Assert.Contains("rows_captured_last_day", note, StringComparison.Ordinal);
        Assert.Contains("sweep_ceiling_rows_per_server_per_day", note, StringComparison.Ordinal);
    }

    private static string Head(string json) => json.Length <= 300 ? json : json[..300];

    /// <summary>
    /// The scratch database's connection string with the store's own search path, so the reads resolve the
    /// bare <c>servers</c> name the way the MCP host's pool does.
    /// </summary>
    private static string ReadConnectionString(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString;
}
