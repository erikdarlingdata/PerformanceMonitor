/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_health_parser_significant_waits (#2484): the ninth member of the system_health parse-on-read
/// family, and the only one the WPF viewer could show that neither the browser nor an agent could reach.
///
/// <para>The assertions that carry the weight are the three empty ones. This category has the most
/// selective gate in the family - a real session, a non-BACKUP statement, 500 ms, a non-idle wait type -
/// so zero rows is routinely the honest answer, and "no significant waits" is exactly the sentence an
/// operator wants to hear. Told it by a server whose system_health has never been captured, they stop
/// looking. So the read distinguishes THREE nothings: events captured and none qualified (healthy), a
/// quiet window (widen it), and nothing ever captured (NOT an all-clear).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingSignificantWaitsReadTests
{
    private const int ServerId = -949554;
    private const string ServerName = "significant-waits-read";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    [Fact]
    public async Task ThreeKindsOfNothing_AndTheWaitsThemselves_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live significant-waits read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* -- never captured: NOT an all-clear, and it must refuse to read as one -- */
            var never = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, ServerName);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("NOT an all-clear", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /* -- captured, but outside the window: a quiet window, and the move is to widen it -- */
            await PlantWaitAsync(connection, ct, HoursAgo(48), LoadFixture("wait_info.xml"));

            var quiet = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, ServerName, hours_back: 1);
            var quietDoc = JsonDocument.Parse(quiet);
            Assert.Equal("empty", quietDoc.RootElement.GetProperty("status").GetString());
            var quietText = quietDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("widen", quietText, StringComparison.Ordinal);

            /* Same zero rows as the branch above, and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);
            Assert.NotEqual(neverText, quietText);

            /*
                -- captured IN the window but gated out: the healthy answer, and the one the other two
                must not be confused with. Same fixture, duration dropped under the 500 ms bar.
            */
            var tooShort = LoadFixture("wait_info.xml").Replace("<value>1500</value>", "<value>100</value>", StringComparison.Ordinal);
            Assert.DoesNotContain("<value>1500</value>", tooShort, StringComparison.Ordinal);
            await PlantWaitAsync(connection, ct, MinutesAgo(10), tooShort);

            var gated = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, ServerName, hours_back: 4);
            var gatedDoc = JsonDocument.Parse(gated);
            Assert.Equal("empty", gatedDoc.RootElement.GetProperty("status").GetString());
            var gatedText = gatedDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("none was significant", gatedText, StringComparison.Ordinal);
            Assert.Contains("500", gatedText, StringComparison.Ordinal);

            /* The three empty messages are three different sentences, not one sentence three times. */
            Assert.DoesNotContain("EVER", gatedText, StringComparison.Ordinal);
            Assert.DoesNotContain("widen", gatedText, StringComparison.Ordinal);

            /* -- a real significant wait: the payload, with the statement that paid for it -- */
            await PlantWaitAsync(connection, ct, MinutesAgo(9), LoadFixture("wait_info.xml"));

            var hit = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, ServerName, hours_back: 4);
            var root = JsonDocument.Parse(hit).RootElement;
            Assert.Equal(ServerName, root.GetProperty("server").GetString());

            /* The gated-out event is still in the window and still must not appear as a wait. */
            Assert.Equal(1, root.GetProperty("wait_count").GetInt32());

            var wait = root.GetProperty("waits")[0];
            Assert.Equal("PAGEIOLATCH_SH", wait.GetProperty("wait_type").GetString());
            Assert.Equal(1500, wait.GetProperty("duration_ms").GetInt64());
            Assert.Equal(12, wait.GetProperty("signal_duration_ms").GetInt64());
            Assert.Equal(57, wait.GetProperty("session_id").GetInt32());

            /*
                The SQL text is the half get_wait_stats can never give: the instance-wide totals name a
                wait type and never the statement that paid it.
            */
            Assert.Contains("dbo.big_table", wait.GetProperty("query_text").GetString()!, StringComparison.Ordinal);

            /* -- the cap REFUSES out of range rather than quietly rewriting it -- */
            var refused = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, ServerName, 4, 5000);
            Assert.Contains("exceeds maximum of 1000", refused, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task PlantWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime eventTimeUtc, string eventXml) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(eventTimeUtc), ServerId, ServerName,
            DarlingMcpTestData.Naive(eventTimeUtc), SystemHealthParser.WaitInfoEvent, eventXml);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM system_health_events WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
