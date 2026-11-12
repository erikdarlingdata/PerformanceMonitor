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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7428: Captured Plans, the Vacuum tab's session states and Predicate Stats return the captured statement text
/// beside each query id, joined from the text store keyed on (server_id, queryid). The join must neither multiply a
/// row nor invent a text: one query id under two plan shapes or two predicates yields the same text on both rows, an
/// id the hourly capture has not reached yields null, and id 0 and a null id yield null.
/// </summary>
[Collection("live-postgres")]
public sealed class PgQueryIdStatementTextGridsLiveTests
{
    private const int ServerId = -742802;
    private const string ServerName = "pg-query-id-text-grids";
    private const long WithText = 1247446479259998619L;
    private const long NoText = 4185925123159566327L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ThePlansPredicatesAndSessionsReads_CarryTheCapturedText_WithoutChangingRowCounts()
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
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var longText = "SELECT " + new string('x', 600);
            var t0 = DateTime.UtcNow.AddMinutes(-30);
            var t1 = DateTime.UtcNow.AddMinutes(-10);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen) VALUES ($1, $2, $3, $4, $4)",
                ServerId, WithText, longText, DarlingMcpTestData.Naive(t0));

            /* Plans: one statement under two plan shapes, one with no captured text, id 0, and a NULL id. */
            foreach (var at in new[] { t0, t1 })
            {
                foreach (var (id, hash, duration) in new (long?, string, double)[] { (WithText, "shapeA", 900), (WithText, "shapeB", 800), (NoText, "shapeC", 700), (0L, "shapeD", 600), (null, "shapeE", 500) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO pg_plan_capture (collection_id, collection_time, server_id, server_name, query_id, plan_hash, duration_ms, node_count, top_node_type, plan_json) "
                        + "VALUES ($1, $2, $3, $4, $5, $6, $7, 3, 'Seq Scan', '{\"Plan\":{\"Node Type\":\"Seq Scan\"}}')",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, id, hash, duration);
                }
            }

            /* Predicates: the same statement on two columns, one without text, id 0, and a NULL id. */
            foreach (var (column, id) in new (string, long?)[] { ("a", WithText), ("b", WithText), ("c", NoText), ("d", 0L), ("e", null) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_predicate_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name, operator, query_id, sample_count, rows_evaluated, rows_filtered, worst_estimate_error_ratio, sample_rate) "
                    + "VALUES ($1, $2, $3, $4, 'app', 'public', 'orders', $5, '=', $6, 10, 1000, 900, 2.0, 0.01)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t1), ServerId, ServerName, column, id);
            }

            /* Sessions: four backends in one capture, the same four shapes. */
            var collectionId = CollectionIdGenerator.Next();
            var backend = 1L;
            foreach (var id in new long?[] { WithText, NoText, 0L, null })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_session_states (collection_id, collection_time, server_id, server_name, backend_id, pid, state, query_id, xact_duration_ms, state_duration_ms, query_duration_ms, backend_duration_ms, horizon_age, is_idle_in_transaction, is_horizon_holder, state_is_redacted, total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions) "
                    + "VALUES ($1, $2, $3, $4, $5, $6, 'idle in transaction', $7, 60000, 60000, 1000, 120000, 5, TRUE, TRUE, FALSE, 10, 2, 4, 4)",
                    collectionId, DarlingMcpTestData.Naive(t1), ServerId, ServerName, backend, (int)(100 + backend), id);
                backend++;
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var start = t0.AddMinutes(-5);
            var end = DateTime.UtcNow;

            var plans = await DarlingPgPlanCaptureReader.GetPgPlanCaptureAsync(postgres, ServerId, start, end, 20, ct);
            Assert.Equal(5, plans.Count);
            Assert.Equal(2, plans.Count(r => r.QueryId == WithText && r.QueryText == longText));
            Assert.Null(Assert.Single(plans, r => r.QueryId == NoText).QueryText);
            Assert.All(plans.Where(r => r.QueryId == 0), r => Assert.Null(r.QueryText));
            Assert.All(plans, r => Assert.Equal(2, r.Captures));

            /* A cap smaller than the window ranks the same and still carries the text of the rows it keeps. */
            var cappedPlans = await DarlingPgPlanCaptureReader.GetPgPlanCaptureAsync(postgres, ServerId, start, end, 2, ct);
            Assert.Equal(2, cappedPlans.Count);
            Assert.All(cappedPlans, r => Assert.Equal(longText, r.QueryText));

            var predicates = await DarlingPgPredicateStatsReader.GetPgPredicateStatsAsync(postgres, ServerId, start, end, 20, ct);
            Assert.Equal(5, predicates.Count);
            Assert.Equal(2, predicates.Count(r => r.QueryId == WithText && r.QueryText == longText));
            Assert.Null(Assert.Single(predicates, r => r.QueryId == NoText).QueryText);
            Assert.Equal(2, predicates.Count(r => r.QueryId == 0 && r.QueryText == null));
            Assert.Equal(3, (await DarlingPgPredicateStatsReader.GetPgPredicateStatsAsync(postgres, ServerId, start, end, 3, ct)).Count);

            var sessions = await DarlingPgSessionStatesReader.GetPgSessionStatesAsync(postgres, ServerId, start, end, 20, ct);
            Assert.Equal(4, sessions.Count);
            Assert.Equal(longText, Assert.Single(sessions, r => r.LastQueryId == WithText).LastQueryText);
            Assert.Null(Assert.Single(sessions, r => r.LastQueryId == NoText).LastQueryText);
            Assert.Null(Assert.Single(sessions, r => r.LastQueryId == 0).LastQueryText);
            Assert.Null(Assert.Single(sessions, r => r.LastQueryId is null).LastQueryText);
            Assert.Equal(2, (await DarlingPgSessionStatesReader.GetPgSessionStatesAsync(postgres, ServerId, start, end, 2, ct)).Count);

            /* The tools: a preview by default, the whole text with full_text, null where there is none. */
            var previewLength = DarlingMcpPgStatementTools.QueryTextPreviewLength;
            var withTextId = WithText.ToString(CultureInfo.InvariantCulture);
            foreach (var fullText in new[] { false, true })
            {
                var plansJson = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlans(postgres, ServerName, 24, 10, null, null, fullText, cancellationToken: ct)).RootElement.GetProperty("plans");
                var predicatesJson = JsonDocument.Parse(await DarlingMcpPgPredicateTools.GetPgPredicateStats(postgres, ServerName, 24, 25, null, fullText, cancellationToken: ct)).RootElement.GetProperty("predicates");
                var sessionsJson = JsonDocument.Parse(await DarlingMcpPgSessionStatesTools.GetPgSessionStates(postgres, ServerName, 24, 25, null, fullText, cancellationToken: ct)).RootElement.GetProperty("sessions");

                foreach (var (rows, idKey, textKey) in new[] { (plansJson, "queryid", "query_text"), (predicatesJson, "queryid", "query_text"), (sessionsJson, "last_query_id", "last_query_text") })
                {
                    var all = rows.EnumerateArray().ToList();
                    Assert.Equal(textKey == "last_query_text" ? 4 : 5, all.Count);

                    /* Only the rows of the statement that has text carry any; the others read null. */
                    var withText = all.Where(r => r.GetProperty(textKey).ValueKind == JsonValueKind.String).ToList();
                    Assert.Equal(textKey == "last_query_text" ? 1 : 2, withText.Count);
                    Assert.All(withText, r => Assert.Equal(withTextId, r.GetProperty(idKey).ToString()));
                    Assert.All(all.Except(withText), r => Assert.Equal(JsonValueKind.Null, r.GetProperty(textKey).ValueKind));

                    foreach (var row in withText)
                    {
                        var text = row.GetProperty(textKey).GetString()!;
                        var truncated = row.GetProperty(textKey + "_truncated").GetBoolean();
                        if (fullText)
                        {
                            Assert.Equal(longText, text);
                            Assert.False(truncated);
                        }
                        else
                        {
                            Assert.True(text.Length < longText.Length);
                            Assert.StartsWith(longText[..previewLength], text, StringComparison.Ordinal);
                            Assert.True(truncated);
                        }
                    }
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
