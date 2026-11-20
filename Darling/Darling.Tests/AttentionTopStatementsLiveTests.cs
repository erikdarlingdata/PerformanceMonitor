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
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts top-statements read (#9148, part D) on a real PostgreSQL store. The read used to carry every sampled statement's
/// text through a sorted <c>array_agg</c> to keep one text per fingerprint, which detoasts every wide text of the window. It now ranks the
/// fingerprints first and fetches the newest text for the five winners only. Two behaviours are held here: the answer is the old answer
/// (ties, NULL texts, the window edges), and the read touches far fewer buffers when the texts are wide.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class AttentionTopStatementsLiveTests
{
    private const int ServerId = -9148401;
    private const int OtherServerId = -9148402;
    private const int WideServerId = -9148403;

    /// <summary>The read as it was before #9148, kept here only as the reference the new read must agree with.</summary>
    private static string OldTopStatements(bool bounded) => $"""
SELECT
    statement_fingerprint,
    CAST(COUNT(*) AS BIGINT) AS samples,
    (array_agg(statement_text ORDER BY collection_time DESC, event_time DESC NULLS LAST, attention_event_id DESC) FILTER (WHERE statement_text IS NOT NULL))[1] AS sample_text
FROM attention_events
WHERE server_id = $1
AND   {AttentionWindowSql.SpanFloor(bounded)}{AttentionWindowSql.Ceiling(bounded)}
AND   event_kind = 'sample'
AND   statement_fingerprint IS NOT NULL
GROUP BY statement_fingerprint
ORDER BY COUNT(*) DESC, statement_fingerprint COLLATE "C"
LIMIT {AttentionWindowSql.TopCount}
""";

    [Fact]
    public async Task TheTopStatements_AreTheOldAnswer_TiesNullTextsAndWindowEdgesIncluded()
    {
        await RunAsync(async (connection, ct) =>
        {
            var floor = new DateTime(2026, 11, 1, 12, 0, 0, DateTimeKind.Unspecified);
            var ceiling = floor.AddMinutes(1);
            long id = 1000;

            /* fp-a: 3 samples, the newest has no text, so the text is the one before it. */
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(10), floor.AddSeconds(10), "fp-a", "A-old", ct);
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(20), floor.AddSeconds(20), "fp-a", "A-new", ct);
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(30), floor.AddSeconds(30), "fp-a", null, ct);

            /* fp-b: 3 samples, two share collection_time and event_time, so the higher id wins; one has no event_time and loses to a dated row. */
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(40), floor.AddSeconds(40), "fp-b", "B-low-id", ct);
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(40), floor.AddSeconds(40), "fp-b", "B-high-id", ct);
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(40), null, "fp-b", "B-no-event-time", ct);

            /* fp-c: only NULL texts, still listed, with no text. */
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(15), floor.AddSeconds(15), "fp-c", null, ct);
            await SampleAsync(connection, id++, ServerId, floor.AddSeconds(16), floor.AddSeconds(16), "fp-c", null, ct);

            /* fp-d to fp-g: two samples each, so the fifth place is a count tie that the fingerprint name breaks (fp-f and fp-g miss the list). */
            foreach (var fp in new[] { "fp-d", "fp-e", "fp-f", "fp-g" })
            {
                await SampleAsync(connection, id++, ServerId, floor.AddSeconds(5), floor.AddSeconds(5), fp, fp + "-1", ct);
                await SampleAsync(connection, id++, ServerId, floor.AddSeconds(6), floor.AddSeconds(6), fp, fp + "-2", ct);
            }

            /* Rows that must not count: no fingerprint, a count row, another server, a row on the exclusive floor, a row past the ceiling. */
            for (var i = 0; i < 10; i++)
            {
                await SampleAsync(connection, id++, ServerId, floor.AddSeconds(20 + i), floor.AddSeconds(20 + i), null, "no fingerprint", ct);
                await SampleAsync(connection, id++, ServerId, floor.AddSeconds(20 + i), floor.AddSeconds(20 + i), "fp-z", "Z", ct, kind: "count");
                await SampleAsync(connection, id++, OtherServerId, floor.AddSeconds(20 + i), floor.AddSeconds(20 + i), "fp-o", "O", ct);
            }

            for (var i = 0; i < 6; i++)
            {
                await SampleAsync(connection, id++, ServerId, floor, floor, "fp-floor", "on the floor", ct);
                await SampleAsync(connection, id++, ServerId, ceiling.AddSeconds(1), ceiling.AddSeconds(1), "fp-late", "after the ceiling", ct);
            }

            var expectedNames = new Dictionary<bool, string[]>
            {
                [true] = new[] { "fp-a", "fp-b", "fp-c", "fp-d", "fp-e" },
                [false] = new[] { "fp-floor", "fp-late", "fp-a", "fp-b", "fp-c" },
            };
            foreach (var bounded in new[] { true, false })
            {
                var expected = await ReadAsync(connection, OldTopStatements(bounded), floor, ceiling, bounded, ct);
                var actual = await ReadAsync(connection, AttentionWindowSql.TopStatements("attention_events", bounded), floor, ceiling, bounded, ct);

                Assert.Equal(AttentionWindowSql.TopCount, expected.Count);
                Assert.Equal(expected, actual);
                Assert.Equal(expectedNames[bounded], actual.Select(r => r.Fingerprint).ToArray());
            }

            var bounded5 = await ReadAsync(connection, AttentionWindowSql.TopStatements("attention_events", true), floor, ceiling, true, ct);
            Assert.Equal("A-new", bounded5.Single(r => r.Fingerprint == "fp-a").Text);
            Assert.Equal("B-high-id", bounded5.Single(r => r.Fingerprint == "fp-b").Text);
            Assert.Null(bounded5.Single(r => r.Fingerprint == "fp-c").Text);
        });
    }

    [Fact]
    public async Task TheTopStatements_ReadFarFewerBuffers_WhenTheTextsAreWide()
    {
        await RunAsync(async (connection, ct) =>
        {
            var floor = new DateTime(2026, 11, 1, 12, 0, 0, DateTimeKind.Unspecified);
            var ceiling = floor.AddHours(1);

            /* 3,000 samples over 30 fingerprints with a text of about 8 KB that does not compress, so each text lives in TOAST. */
            using (var seed = new NpgsqlCommand(
                @"INSERT INTO attention_events
                      (attention_event_id, collection_time, server_id, server_name, event_time, event_kind, attention_count, statement_fingerprint, statement_text)
                  SELECT 5000000 + g, $2 + (g * interval '1 second'), $1, 'attention-wide', $2 + (g * interval '1 second'), 'sample', 0, 'wide-' || (g % 30),
                         (SELECT string_agg(md5(g::text || '-' || j::text), '') FROM generate_series(1, 256) AS j)
                  FROM generate_series(1, 3000) AS g", connection))
            {
                seed.CommandTimeout = 120;
                seed.Parameters.AddWithValue(NpgsqlDbType.Integer, WideServerId);
                seed.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
                await seed.ExecuteNonQueryAsync(ct);
            }

            using (var analyze = new NpgsqlCommand("ANALYZE attention_events", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            var oldSql = OldTopStatements(true);
            var newSql = AttentionWindowSql.TopStatements("attention_events", true);

            /* Same rows first, then the buffers of each statement on a warm backend (a second run of each, so catalog reads are out of the count). */
            Assert.Equal(
                await ReadAsync(connection, oldSql, floor, ceiling, true, ct, WideServerId),
                await ReadAsync(connection, newSql, floor, ceiling, true, ct, WideServerId));
            var oldBuffers = await BuffersAsync(connection, oldSql, floor, ceiling, ct);
            var newBuffers = await BuffersAsync(connection, newSql, floor, ceiling, ct);

            Assert.True(oldBuffers > 3000, $"The fixture must be wide enough to show the cost: the old read touched only {oldBuffers} buffers.");
            Assert.True(newBuffers * 5 < oldBuffers, $"The read touched {newBuffers} buffers against {oldBuffers} before #9148.");
        });
    }

    private static async Task<List<(string Fingerprint, long Samples, string? Text)>> ReadAsync(
        NpgsqlConnection connection, string sql, DateTime floor, DateTime ceiling, bool bounded, CancellationToken ct, int serverId = ServerId)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        if (bounded)
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, ceiling);
        }

        var rows = new List<(string Fingerprint, long Samples, string? Text)>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return rows;
    }

    /// <summary>The shared buffers the statement's plan touched, hits plus reads, from the root node of a warm second run.</summary>
    private static async Task<long> BuffersAsync(NpgsqlConnection connection, string sql, DateTime floor, DateTime ceiling, CancellationToken ct)
    {
        long total = 0;
        for (var run = 0; run < 2; run++)
        {
            /* EXPLAIN takes no bind parameters, so the three are written into the text as literals. */
            var literal = sql
                .Replace("$1", WideServerId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
                .Replace("$2", $"'{floor:yyyy-MM-dd HH:mm:ss}'::timestamp", StringComparison.Ordinal)
                .Replace("$3", $"'{ceiling:yyyy-MM-dd HH:mm:ss}'::timestamp", StringComparison.Ordinal);
            using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + literal, connection) { CommandTimeout = 120 };
            var json = Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
            using var document = JsonDocument.Parse(json);
            var plan = document.RootElement[0].GetProperty("Plan");
            total = plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();
        }

        return total;
    }

    private static async Task SampleAsync(
        NpgsqlConnection connection, long id, int serverId, DateTime collectionTime, DateTime? eventTime, string? fingerprint, string? text, CancellationToken ct,
        string kind = "sample")
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events
                  (attention_event_id, collection_time, server_id, server_name, event_time, event_kind, attention_count, statement_fingerprint, statement_text)
              VALUES ($1, $2, $3, 'attention-9148', $4, $5, 0, $6, $7)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, collectionTime);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)eventTime ?? DBNull.Value });
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Client Timeouts top-statements test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand("DELETE FROM attention_events WHERE server_id IN ($1, $2, $3)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, OtherServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, WideServerId);
        await command.ExecuteNonQueryAsync(ct);
    }
}
