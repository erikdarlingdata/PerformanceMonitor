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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5911 gated live load test (DARLING_TEST_PG) for <c>backtest_custom_alert_rule</c>: ten registered servers, seven days
/// of CPU samples each every five minutes, and a replay at the caps (ten servers, the 168-hour span, so 288 steps per
/// server and 2,880 panel queries). It asserts the query and time caps hold, and that the default span is 24 hours. The
/// measured time and query count are written to the test output (#5794 is the store-load context).
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertBacktestLiveTests
{
    private const int FirstServerId = 991911;
    private const int ServerCount = 10;

    private readonly ITestOutputHelper _output;

    public CustomAlertBacktestLiveTests(ITestOutputHelper output) => _output = output;

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the backtest live test.");
        return connectionString!;
    }

    [Fact]
    public async Task TheBacktest_AtItsCaps_HoldsTheQueryAndTimeCaps()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;
        await using var dataSource = NpgsqlDataSource.Create(dataSourceConnectionString);

        var prefix = "bt_" + Guid.NewGuid().ToString("N")[..12] + "_";
        var names = Enumerable.Range(0, ServerCount).Select(i => prefix + i.ToString("00", System.Globalization.CultureInfo.InvariantCulture)).ToArray();
        var end = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Utc);

        // Warning at >= 25; the seeded CPU sits at 40 for the first 20 minutes of every hour and at 10 otherwise.
        var definition =
            "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25}," +
            "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
            "\"scope\":{\"mode\":\"all\"}}";

        var bodySucceeded = false;
        try
        {
            for (var i = 0; i < ServerCount; i++)
            {
                await using (var insert = dataSource.CreateCommand("INSERT INTO servers (server_id, server_name, display_name) VALUES ($1, $2, $2)"))
                {
                    insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = FirstServerId + i });
                    insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = names[i] });
                    await insert.ExecuteNonQueryAsync(ct);
                }

                await using var seed = dataSource.CreateCommand(
                    "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, "
                    + "sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc) "
                    + "SELECT g, t, $1, $2, t, CASE WHEN extract(minute FROM t) < 20 THEN 40 ELSE 10 END, 0, t "
                    + "FROM generate_series(1, 2016) AS g, LATERAL (SELECT $3::timestamp - make_interval(mins => 5 * (g - 1)) AS t) AS s");
                seed.Parameters.Add(new NpgsqlParameter<int> { TypedValue = FirstServerId + i });
                seed.Parameters.Add(new NpgsqlParameter<string> { TypedValue = names[i] });
                seed.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
                await seed.ExecuteNonQueryAsync(ct);
            }

            var asOf = end.ToString("yyyy-MM-ddTHH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

            // At the caps: ten servers, 168 hours.
            var json = await DarlingMcpCustomAlertTools.BacktestCustomAlertRule(
                dataSource, definition: definition, servers: string.Join(",", names), hours: 168, as_of: asOf, cancellationToken: ct);
            var answer = (JsonObject)JsonNode.Parse(json)!;
            var queries = (int)answer["queries_run"]!;
            var elapsed = (long)answer["elapsed_ms"]!;
            _output.WriteLine($"backtest at the caps: status={answer["status"]}, queries_run={queries}, elapsed_ms={elapsed}");

            Assert.True((string?)answer["status"] is "ok" or "partial", json[..Math.Min(json.Length, 400)]);
            Assert.InRange(queries, 1, CustomAlertBacktest.MaxQueries);
            Assert.InRange(elapsed, 0, (long)CustomAlertBacktest.MaxElapsed.TotalMilliseconds + 5_000); // one in-flight query past the cap at most
            var servers = (JsonArray)answer["servers"]!;
            Assert.Equal(ServerCount, servers.Count);
            if ((string?)answer["status"] == "ok")
            {
                Assert.Equal(CustomAlertBacktest.MaxQueries, queries);
                Assert.All(servers, s => Assert.Equal(CustomAlertBacktest.StepsPerSpan, (int)s!["steps_run"]!));
                Assert.All(servers, s => Assert.Equal("ok", (string?)s!["status"]));
                Assert.All(servers, s => Assert.True((int)s!["fire_count"]! > 0, "the seeded hourly spikes fire"));
            }
            else
            {
                Assert.Equal("time", (string?)answer["stopped_at"]!["cap"]);
            }

            // The default span is 24 hours.
            var single = (JsonObject)JsonNode.Parse(await DarlingMcpCustomAlertTools.BacktestCustomAlertRule(
                dataSource, definition: definition, servers: names[0], as_of: asOf, cancellationToken: ct))!;
            Assert.Equal(24.0, (double)single["span_hours"]!);
            Assert.Equal("ok", (string?)single["status"]);
            Assert.Equal(300, (long)single["servers"]![0]!["step_seconds"]!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using (var setPath = new NpgsqlCommand("SET search_path TO collect, config, public", cleanup))
                {
                    await setPath.ExecuteNonQueryAsync(cleanupCt);
                }

                using (var samples = new NpgsqlCommand("DELETE FROM cpu_utilization_stats WHERE server_id BETWEEN $1 AND $2", cleanup))
                {
                    samples.Parameters.AddWithValue(FirstServerId);
                    samples.Parameters.AddWithValue(FirstServerId + ServerCount - 1);
                    await samples.ExecuteNonQueryAsync(cleanupCt);
                }

                using var server = new NpgsqlCommand("DELETE FROM servers WHERE server_id BETWEEN $1 AND $2", cleanup);
                server.Parameters.AddWithValue(FirstServerId);
                server.Parameters.AddWithValue(FirstServerId + ServerCount - 1);
                await server.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
