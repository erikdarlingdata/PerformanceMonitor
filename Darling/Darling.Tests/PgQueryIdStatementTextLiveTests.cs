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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7428: the sampled-waits and OS CPU reads return the captured statement text beside each query id.
/// The text store is keyed on (server_id, queryid), so the join must neither multiply a row nor invent a text:
/// one query id under two databases yields two rows with the same text, an id the hourly capture has not reached
/// yields null, and id 0 and a null id yield null.
/// </summary>
[Collection("live-postgres")]
public sealed class PgQueryIdStatementTextLiveTests
{
    private const int ServerId = -742801;
    private const string ServerName = "pg-query-id-text";
    private const long WithText = 1247446479259998619L;
    private const long NoText = 4185925123159566327L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheKernelAndSamplingReads_CarryTheCapturedText_WithoutMultiplyingRows()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live statement text test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var t0 = DateTime.UtcNow.AddMinutes(-30);
            var t1 = DateTime.UtcNow.AddMinutes(-10);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen) VALUES ($1, $2, $3, $4, $4)",
                ServerId, WithText, "SELECT * FROM orders WHERE id = $1", DarlingMcpTestData.Naive(t0));

            /* The same query id under two databases, plus an id with no captured text. Two collections each, so
               the window has an interval to difference. */
            foreach (var (at, user) in new[] { (t0, 10.0), (t1, 40.0) })
            {
                foreach (var (db, id) in new[] { ("app", WithText), ("reporting", WithText), ("app", NoText) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO pg_kernel_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, exec_user_time_ms, exec_system_time_ms, plan_cpu_time_ms, exec_read_bytes, exec_write_bytes, minor_faults, major_faults, stats_since) "
                        + "VALUES ($1, $2, $3, $4, $5, $6, $7, 1.0, 0, 8192, 0, 0, 0, $8)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, db, id, user, DarlingMcpTestData.Naive(t0.AddDays(-1)));
                }

                /* Samples: attributed with text, attributed without, "no statement" (0), and not reported (NULL). */
                var sample = at == t0 ? 100L : 300L;
                foreach (var (ev, id) in new (string, long?)[] { ("relation", WithText), ("tuple", NoText), ("transactionid", 0L), ("buffer_content", null) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) "
                        + "VALUES ($1, $2, $3, $4, 'Lock', $5, $6, $7, 10, 1, NULL)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, ev, id, sample);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var start = t0.AddMinutes(-5);
            var end = DateTime.UtcNow;

            var kernel = await DarlingPgKernelStatsReader.GetPgKernelStatsPageAsync(postgres, ServerId, start, end, 10, ct);
            Assert.Equal(3, kernel.Rows.Count);
            Assert.Equal(2, kernel.Rows.Count(r => r.QueryId == WithText && r.QueryText == "SELECT * FROM orders WHERE id = $1"));
            Assert.Null(Assert.Single(kernel.Rows, r => r.QueryId == NoText).QueryText);

            var waits = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(postgres, ServerId, start, end, 10, ct);
            Assert.Equal(4, waits.Rows.Count);
            Assert.Equal("SELECT * FROM orders WHERE id = $1", Assert.Single(waits.Rows, r => r.QueryId == WithText).QueryText);
            Assert.Null(Assert.Single(waits.Rows, r => r.QueryId == NoText).QueryText);
            Assert.Null(Assert.Single(waits.Rows, r => r.QueryId == 0).QueryText);
            Assert.Null(Assert.Single(waits.Rows, r => r.QueryId is null).QueryText);
            Assert.All(waits.Rows, r => Assert.Equal(200, r.SampleCount));

            /* A cap smaller than the window still ranks the same and still carries the text of the rows it keeps. */
            var capped = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(postgres, ServerId, start, end, 2, ct);
            Assert.Equal(2, capped.Rows.Count);
            Assert.Equal(800, capped.WindowTotalSamples);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public void TheToolRows_PreviewTheText_UnlessFullTextIsAsked_AndLeaveAMissingTextNull()
    {
        var longText = "SELECT " + new string('x', 600);
        var row = new DarlingPgKernelStatsReader.PgKernelStatRow("app", WithText, 10, 9, 1, 0, 0, 0, false, DateTime.UtcNow, longText);
        var bare = row with { QueryId = NoText, QueryText = null };
        var page = new DarlingPgKernelStatsReader.PgKernelStatsPage([row, bare], 20);

        var preview = JsonDocument.Parse(DarlingMcpPgKernelStatsTools.BuildKernelStatsJson("s", 24, page, 20)).RootElement.GetProperty("queries");
        Assert.StartsWith(longText[..DarlingMcpPgStatementTools.QueryTextPreviewLength], preview[0].GetProperty("query_text").GetString(), StringComparison.Ordinal);
        Assert.True(preview[0].GetProperty("query_text").GetString()!.Length < longText.Length);
        Assert.True(preview[0].GetProperty("query_text_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, preview[1].GetProperty("query_text").ValueKind);
        Assert.False(preview[1].GetProperty("query_text_truncated").GetBoolean());

        var full = JsonDocument.Parse(DarlingMcpPgKernelStatsTools.BuildKernelStatsJson("s", 24, page, 20, fullText: true)).RootElement.GetProperty("queries");
        Assert.Equal(longText, full[0].GetProperty("query_text").GetString());
        Assert.False(full[0].GetProperty("query_text_truncated").GetBoolean());
    }
}
