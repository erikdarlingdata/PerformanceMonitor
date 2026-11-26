/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: <c>get_analysis_facts</c> asked for a source whose families find nothing over an observed window answers as the
/// whole read answers when the server has other facts: no facts shown, never the no-facts envelope. The store holds only
/// waits, so the <c>jobs</c>, <c>sessions</c> and <c>bad_actor</c> reads, which skip the wait family, collect no fact.
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>).</para>
/// </summary>
public sealed class FactSourceNarrowingEnvelopeLiveTests
{
    private const int ServerId = -857_004;
    private const string ServerName = "FactSourceEnvelopeSrv";

    /// <summary>Waits every 10 minutes over the 200 minutes before the window end. Each statement reads $1 server id, $2 server name, $3 window end.</summary>
    private const string SeedSql = @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $3);

INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
SELECT 970000000 + w.n * 100 + s, $3::timestamp - make_interval(mins => 10 * s), $1, $2, w.wait_type, 400, w.wait_ms, w.wait_ms / 9
FROM (VALUES (1, 'CXPACKET', 3000000), (2, 'PAGEIOLATCH_SH', 1500000), (3, 'WRITELOG', 300000)) AS w (n, wait_type, wait_ms)
CROSS JOIN generate_series(0, 20) AS s;";

    private static bool IsUnavailable(JsonElement root) =>
        root.TryGetProperty("status", out var status) && status.GetString() == "unavailable";

    [Fact]
    public async Task SourceWithNoFactsOverAnObservedWindow_AnswersWithNoneShown()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fact source envelope test.");
        var ct = TestContext.Current.CancellationToken;

        /* The tool reads the product's own clock, so the window is anchored where both clocks have passed and asked for by as_of. */
        var windowEnd = LiveClock.AnchoredEndNotAfterNow();
        var asOf = windowEnd.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            /* One statement per command: PostgreSQL prepares one statement per command. */
            foreach (var statement in SeedSql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                await using var seed = new NpgsqlCommand(statement, connection);
                seed.Parameters.AddWithValue(ServerId);
                seed.Parameters.AddWithValue(ServerName);
                seed.Parameters.AddWithValue(windowEnd);
                await seed.ExecuteNonQueryAsync(ct);
            }
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var service = new DarlingAnalysisService(postgres) { MinimumDataHours = 0 };

        /* The whole read has wait facts, so the server has facts to show. */
        using (var whole = JsonDocument.Parse(await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: 4, as_of: asOf, cancellationToken: ct)))
        {
            Assert.False(IsUnavailable(whole.RootElement), whole.RootElement.GetRawText());
            Assert.Contains(whole.RootElement.GetProperty("facts").EnumerateArray(), f => f.GetProperty("source").GetString() == "waits");
        }

        foreach (var source in new[] { "jobs", "sessions", "bad_actor" })
        {
            var json = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: 4, source: source, as_of: asOf, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            Assert.False(IsUnavailable(root), $"{source}: {json}");
            Assert.Equal(0, root.GetProperty("shown").GetInt32());
            Assert.Empty(root.GetProperty("facts").EnumerateArray());
            Assert.Equal(source, root.GetProperty("filters").GetProperty("source").GetString());
            Assert.True(root.GetProperty("coverage").ValueKind == JsonValueKind.Object, $"{source}: {json}");
            Assert.False(root.TryGetProperty("collection_caveats", out _), $"{source}: {json}");
        }
    }
}
