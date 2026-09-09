/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3198, behaviourally, against a real Postgres store: the Default Trace's server-local
/// <c>event_time</c> has to come back — and be selected — in naive UTC on every server offset.
///
/// <para><b>What each arm discriminates, and why one alone would not.</b> The MCP read already skewed its
/// window BOUNDS into the server's local frame, so it selected exactly the right rows and only the returned
/// VALUE was wrong: <see cref="GetDefaultTraceEvents_ReturnsTheSameUtcInstants_AtEveryServerOffset"/> is
/// therefore an assertion about the timestamps in the response, and a row-count test would have passed on
/// the defect. The compose annotation overlay had the opposite shape — no de-skew at all — so
/// <see cref="DefaultTraceAnnotation_SelectsByUtc_AndAgreesWithAUtcSourceOnTheSameChart"/> discriminates on
/// SELECTION as well, with an event whose UTC instant is inside the window while its stored local value is
/// outside it.</para>
///
/// <para><b>Invariance is asserted alongside membership</b>, per #2992: a read that returns nothing is also
/// invariant across offsets, so "the three offsets agree" is not by itself a test. Each arm asserts the
/// exact instant set, not merely that the sets match.</para>
///
/// <para>The offsets are the production fleet's (UTC-4), a dev box (UTC), and a positive one (UTC+10) —
/// the direction that would suppress rather than mis-render, and the one no store anyone develops against
/// ever exercises.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DefaultTraceEventFrameLivePostgresTests
{
    private const string ServerName = "darling-deftrace-frame-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The collected offset is <c>DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())</c> — local minus UTC —
    /// so a trace event at UTC instant <c>t</c> is STORED as <c>t + offset</c>. Planting it that way is what
    /// makes this a test of the read rather than of the arithmetic in the fixture.</summary>
    private static DateTime StoredLocal(DateTime utc, int offsetMinutes) =>
        DateTime.SpecifyKind(utc.AddMinutes(offsetMinutes), DateTimeKind.Unspecified);

    [Theory]
    [InlineData(-240)] /* the production fleet */
    [InlineData(0)]    /* a dev box - the only offset the defect was ever right on */
    [InlineData(600)]  /* east of UTC: the suppression direction */
    public async Task GetDefaultTraceEvents_ReturnsTheSameUtcInstants_AtEveryServerOffset(int offsetMinutes)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the default-trace clock-frame test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            /* A fixed instant rather than "now": the window is asked for with an explicit as_of, so nothing
               here depends on the clock of the machine running the test. */
            var asOf = new DateTime(2026, 9, 9, 06, 00, 00, DateTimeKind.Utc);
            var instants = new[] { asOf.AddHours(-1), asOf.AddHours(-2), asOf.AddHours(-3) };

            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await PlantServerPropertiesAsync(connection, asOf, offsetMinutes, ct);

            foreach (var utc in instants)
            {
                await PlantTraceEventAsync(connection, StoredLocal(utc, offsetMinutes), "ErrorLog", 20, ct);
            }

            var json = await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(
                postgres, ServerName, hours_back: 4, limit: 100, as_of: asOf.ToString("o", CultureInfo.InvariantCulture));

            DarlingMcpTestData.AssertEnvelope(json, ServerName, "events");

            /* Membership, exactly: the three planted UTC instants, newest first. Not "the sets agree" - a
               read returning nothing agrees with itself at every offset. */
            Assert.Equal(
                instants.OrderByDescending(x => x).ToArray(),
                ReturnedEventTimes(json));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The mixed-frame arm. A Default Trace event and a system_health event recorded at the SAME UTC instant
    /// must land on the same x-position when both are overlaid on one panel — the failure #3198 calls out as
    /// worse than a uniform error, because no single correction recovers a chart carrying two frames. The
    /// window is chosen so the Default Trace event's STORED local value falls outside it while its UTC
    /// instant falls inside, which is what makes this an assertion about selection as well as rendering.
    /// </summary>
    [Fact]
    public async Task DefaultTraceAnnotation_SelectsByUtc_AndAgreesWithAUtcSourceOnTheSameChart()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the annotation clock-frame test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            const int OffsetMinutes = -240;
            var utc = new DateTime(2026, 9, 9, 04, 28, 06, DateTimeKind.Utc);

            /* 03:00-06:00 UTC brackets the event's UTC instant (04:28) and EXCLUDES its stored local value
               (00:28), so an un-de-skewed bound drops the row outright. */
            var windowStart = new DateTime(2026, 9, 9, 03, 00, 00, DateTimeKind.Utc);
            var windowEnd = new DateTime(2026, 9, 9, 06, 00, 00, DateTimeKind.Utc);

            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await PlantServerPropertiesAsync(connection, windowEnd, OffsetMinutes, ct);
            await PlantTraceEventAsync(connection, StoredLocal(utc, OffsetMinutes), "Object:Altered", null, ct);
            await PlantSystemHealthEventAsync(connection, utc, ct);

            var trace = await RunAnnotationAsync(connection, "default_trace_events", windowStart, windowEnd, ct);
            var systemHealth = await RunAnnotationAsync(connection, "system_health_events", windowStart, windowEnd, ct);

            /* Both sources found their one event... */
            Assert.Equal(new[] { DarlingMcpTestData.Naive(utc) }, systemHealth);
            Assert.Equal(new[] { DarlingMcpTestData.Naive(utc) }, trace);

            /* ...and, said as the property the chart actually depends on, at the same x-position. */
            Assert.Equal(systemHealth, trace);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ─────────────────────────── fixtures ─────────────────────────── */

    private static async Task PlantServerPropertiesAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, int offsetMinutes, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName, offsetMinutes);

    private static async Task PlantTraceEventAsync(
        NpgsqlConnection connection, DateTime storedLocalEventTime, string eventName, int? severity, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, severity)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), storedLocalEventTime, ServerId, ServerName, storedLocalEventTime,
            eventName, (object?)severity ?? DBNull.Value);

    private static async Task PlantSystemHealthEventAsync(NpgsqlConnection connection, DateTime eventTimeUtc, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(eventTimeUtc), ServerId, ServerName,
            DarlingMcpTestData.Naive(eventTimeUtc), SystemHealthParser.WaitInfoEvent, "<event name=\"wait_info\" />");

    /* ─────────────────────────── readers ─────────────────────────── */

    /// <summary>The <c>event_time</c> values the MCP tool returned, parsed back from its ISO-8601 output and
    /// ordered newest first the way the read emits them.</summary>
    private static DateTime[] ReturnedEventTimes(string json)
    {
        using var doc = JsonDocument.Parse(json);

        return doc.RootElement.GetProperty("events").EnumerateArray()
            .Select(e => DateTime.Parse(
                e.GetProperty("event_time").GetString()!,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal))
            .ToArray();
    }

    /// <summary>Compiles the panel's annotation query for one source through the real
    /// <see cref="ComposeCompiler"/> and runs it, returning the <c>ts</c> column.</summary>
    private static async Task<DateTime[]> RunAnnotationAsync(
        NpgsqlConnection connection, string sourceKey, DateTime startUtc, DateTime endUtc, CancellationToken ct)
    {
        var json = System.Text.Json.Nodes.JsonNode.Parse(
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\","
            + "\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"" + sourceKey + "\"]}")!;
        var (plan, error) = ComposeSpec.TryParsePanel((System.Text.Json.Nodes.JsonObject)json, Array.Empty<string>());
        Assert.True(error is null, error);

        var context = new ComposeRunContext(
            new[] { ServerName }, DarlingMcpTestData.Naive(startUtc), DarlingMcpTestData.Naive(endUtc),
            ComposeRunContext.NoVariables, RollupAvailability.All, DarlingMcpTestData.Naive(endUtc), RollupCoverage.Unknown);

        var compiled = Assert.Single(ComposeCompiler.CompileAnnotations(plan!, context)).Compiled;

        await using var command = new NpgsqlCommand(compiled.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        var stamps = new List<DateTime>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            stamps.Add(reader.GetDateTime(0));
        }

        return stamps.ToArray();
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct, bool keepServer = false)
    {
        var sql = $"DELETE FROM default_trace_events WHERE server_id = {ServerId};"
            + $" DELETE FROM system_health_events WHERE server_id = {ServerId};"
            + $" DELETE FROM server_properties WHERE server_id = {ServerId};";
        if (!keepServer)
        {
            sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        }

        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
