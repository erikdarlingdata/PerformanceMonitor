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
using System.Text;
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
/// #8568: six row reads answered over the shared 32,768-byte response budget at default arguments and none said
/// it had been cut for bytes: <c>get_alert_history</c> (50 rows with their whole <c>detail_text</c>),
/// <c>get_forced_plans</c> (200 forces), <c>get_top_queries_by_cpu</c> (20 rows with 2,000 characters of
/// <c>query_text</c>), <c>get_pg_session_states</c>, <c>get_pg_server_config</c> (100 settings) and
/// <c>get_store_log</c> (every retained row with its whole <c>sample_line</c>). Each test seeds a store whose
/// default answer is over the budget, calls the tool as a caller would, and holds the answer to the budget, to
/// whole rows, and to saying what it left out. The fleet cards are covered in
/// <see cref="McpFleetResponseBudgetLivePostgresTests"/> and the four trend reads in
/// <see cref="TrendPayloadBudgetLiveTests"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class McpResponseBudget8568LiveTests
{
    private const string ServerName = "mcp-budget-8568";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One scratch store per test, built with the product's own migrations, holding the one server.</summary>
    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #8568 response-budget tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(connection, postgres, ct);
    }

    /// <summary>The answer is data, within the budget; returns its root.</summary>
    private static JsonElement AssertWithinBudget(string tool, string json)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"{tool} answered an error: {json[..Math.Min(json.Length, 400)]}");
        var bytes = Encoding.UTF8.GetByteCount(json);
        Assert.True(bytes <= McpResponseBudget.DefaultBytes,
            $"default {tool} was {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    /// <summary>The byte cut is flagged under its own key, with a note that names the budget and both counts.</summary>
    private static void AssertSaysItWasCutForBytes(JsonElement root, int rowsRead, int rowsReturned)
    {
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        var note = root.GetProperty("budget_note").GetString();
        Assert.NotNull(note);
        Assert.Contains("32,768-byte response budget", note, StringComparison.Ordinal);
        Assert.Contains($"{rowsRead:#,0} ", note, StringComparison.Ordinal);
        Assert.Contains($"only {rowsReturned:#,0} fit", note, StringComparison.Ordinal);
        Assert.Contains($"{rowsRead - rowsReturned:#,0} were left out", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AlertHistory_Default_FiftyAlertsWithLongDetail_FitsTheBudget_PreviewsTheText_AndSaysWhatItLeftOut()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            const int detailLength = 3000;
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            for (var i = 0; i < 60; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 91.5, 80, TRUE, 'email', NULL, FALSE, FALSE, $5, NULL)",
                    DarlingMcpTestData.Naive(newest.AddMinutes(-i)), ServerId, ServerName, $"Collector Cost Digest {i:D2}",
                    DetailText(i, detailLength));
            }

            var root = AssertWithinBudget("get_alert_history", await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName));
            var alerts = root.GetProperty("alerts").EnumerateArray().ToList();

            /* The limit's page is 50 of the 60; fewer than 50 fit, newest first, each one whole. */
            Assert.InRange(alerts.Count, 1, 49);
            Assert.Equal(alerts.Count, root.GetProperty("alerts_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            AssertSaysItWasCutForBytes(root, 50, alerts.Count);
            for (var i = 0; i < alerts.Count; i++)
            {
                /* The fields that say which alert this is are whole; only the long text is a preview, and it says so. */
                Assert.Equal($"Collector Cost Digest {i:D2}", alerts[i].GetProperty("metric_name").GetString());
                Assert.Equal(ServerName, alerts[i].GetProperty("server_name").GetString());
                Assert.Equal(newest.AddMinutes(-i), ParseUtc(alerts[i].GetProperty("alert_time").GetString()!));
                Assert.True(alerts[i].GetProperty("detail_text_truncated").GetBoolean());
                Assert.Equal(detailLength, alerts[i].GetProperty("detail_text_length").GetInt32());
                var shown = alerts[i].GetProperty("detail_text").GetString()!;
                Assert.InRange(shown.Length, 1, 400);
                Assert.StartsWith(shown, DetailText(i, detailLength), StringComparison.Ordinal);
            }

            /* The span the page reports is the span of the rows it kept. */
            Assert.Equal(newest.AddMinutes(-(alerts.Count - 1)), ParseUtc(root.GetProperty("oldest_returned_alert_time").GetString()!));
        });
    }

    [Fact]
    public async Task ForcedPlans_Default_OverTwoHundredForces_FitsTheBudget_AndPagesToEveryForce()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            const int forces = 250;
            const string db = "BigDb";
            var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-1));
            await DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count, last_force_failure_reason)
VALUES ($1,$2,$3,$4,$5,NULL,NULL,NULL,NULL,NULL,NULL)", CollectionIdGenerator.Next(), at, ServerId, ServerName, db);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count, last_force_failure_reason)
SELECT $1 + g, $2, $3, $4, $5, g, g + 1000, '0x' || g::text, 'MANUAL', 0, NULL FROM generate_series(1, $6::int) AS g",
                CollectionIdGenerator.Next() + 1_000_000, at, ServerId, ServerName, db, forces);

            var root = AssertWithinBudget("get_forced_plans", await DarlingMcpForcedPlanTools.GetForcedPlans(postgres, ServerName, cancellationToken: ct));
            var listed = root.GetProperty("forced_plans").GetArrayLength();
            Assert.InRange(listed, 1, 199);
            Assert.Equal(listed, root.GetProperty("forces_returned").GetInt32());
            Assert.Equal(forces, root.GetProperty("total_forces").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.True(root.GetProperty("has_more").GetBoolean());
            Assert.Equal(listed, root.GetProperty("next_offset").GetInt32());
            AssertSaysItWasCutForBytes(root, 200, listed);

            /* Nothing is dropped: following next_offset reads every force exactly once, each page within the budget. */
            var seen = new List<long>();
            var page = root;
            for (var calls = 0; calls < 50; calls++)
            {
                seen.AddRange(page.GetProperty("forced_plans").EnumerateArray().Select(f => f.GetProperty("plan_id").GetInt64()));
                if (!page.GetProperty("has_more").GetBoolean())
                {
                    break;
                }

                page = AssertWithinBudget("get_forced_plans", await DarlingMcpForcedPlanTools.GetForcedPlans(
                    postgres, ServerName, offset: page.GetProperty("next_offset").GetInt32(), cancellationToken: ct));
            }

            Assert.Equal(Enumerable.Range(1, forces).Select(i => (long)i), seen.OrderBy(id => id));
        });
    }

    [Fact]
    public async Task TopQueriesByCpu_Default_TwentyLongStatements_FitsTheBudget_AndALongerPageSaysWhatItLeftOut()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            const int queries = 120;
            var at = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10));
            for (var i = 0; i < queries; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       query_text_digest, delta_execution_count, delta_worker_time,
                                       delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, "Sales",
                    QueryHash(i), "0xPLANHASH", $"0xSQLH{i:D6}", "0xPLANH", QueryText(i),
                    DBNull.Value, 1000L + (queries - i), (queries - i) * 1_000_000L, (queries - i) * 2_000_000L, 5000L, 1, 1);
            }

            /* The default page: 20 rows, all of them, because 400 characters of text each fit where 2,000 did not. */
            var root = AssertWithinBudget("get_top_queries_by_cpu", await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, cancellationToken: ct));
            var rows = root.GetProperty("queries").EnumerateArray().ToList();
            Assert.Equal(20, rows.Count);
            Assert.False(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
            for (var i = 0; i < rows.Count; i++)
            {
                /* Heaviest first, the hash whole, the text a marked preview of the stored statement. */
                Assert.Equal(QueryHash(i), rows[i].GetProperty("query_hash").GetString());
                Assert.True(rows[i].GetProperty("query_text_truncated").GetBoolean());
                var shown = rows[i].GetProperty("query_text").GetString()!;
                Assert.EndsWith("... (truncated)", shown, StringComparison.Ordinal);
                Assert.StartsWith(shown[..^"... (truncated)".Length], QueryText(i), StringComparison.Ordinal);
                Assert.InRange(shown.Length, 1, 400 + "... (truncated)".Length);
            }

            /* A caller who asks for more rows than the budget holds gets the heaviest that fit, and is told. */
            var wide = AssertWithinBudget("get_top_queries_by_cpu top=120", await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, top: queries, cancellationToken: ct));
            var kept = wide.GetProperty("queries").EnumerateArray().ToList();
            Assert.InRange(kept.Count, 21, queries - 1);
            Assert.True(wide.GetProperty("truncated").GetBoolean());
            var note = wide.GetProperty("budget_note").GetString();
            Assert.Contains("32,768-byte response budget", note, StringComparison.Ordinal);
            Assert.Contains($"{queries} queries were read; only {kept.Count} fit", note, StringComparison.Ordinal);
            Assert.Equal(Enumerable.Range(0, kept.Count).Select(QueryHash), kept.Select(r => r.GetProperty("query_hash").GetString()));
        });
    }

    [Fact]
    public async Task PgSessionStates_Default_TwentyFiveWideSessions_FitsTheBudget_AndSaysWhatItLeftOut()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            const int sessions = 60;
            var at = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10));
            var collectionId = CollectionIdGenerator.Next();
            for (var i = 0; i < sessions; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, backend_id, pid, database_name, username, application_name,
     client_addr, backend_type, state, wait_event_type, wait_event, command_tag, query_id,
     state_duration_ms, xact_duration_ms, query_duration_ms, backend_duration_ms, xmin_age, xid_age, horizon_age,
     is_idle_in_transaction, is_horizon_holder, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'client backend', 'idle in transaction', 'Client', 'ClientRead', 'UPDATE', $11,
        $12, $12, 1500, $13, $14, $14, $14, TRUE, TRUE, FALSE, 412, 37, 60, 60)",
                    collectionId, at, ServerId, ServerName, 10_000L + i, 20_000 + i,
                    $"tenant_orders_database_{i:D3}", $"svc_order_fulfilment_writer_{i:D3}",
                    $"order-fulfilment-worker/3.18.4 (pool=primary-writer; pod=fulfilment-7c9d8b6f5-{i:D5}; region=east; shard={i:D3})",
                    $"10.42.{i / 200}.{i % 200 + 1}", 880_000_000_000L + i,
                    /* The longest transaction first: the page's own order, so the offsets are known. */
                    (long)(sessions - i) * 3_600_000L, (long)(sessions - i) * 7_200_000L, 5_000_000L + sessions - i);
            }

            var root = AssertWithinBudget("get_pg_session_states", await DarlingMcpPgSessionStatesTools.GetPgSessionStates(postgres, ServerName, cancellationToken: ct));
            var rows = root.GetProperty("sessions").EnumerateArray().ToList();

            /* The limit's page is 25 of the 60; fewer than 25 fit, worst first, each one whole. */
            Assert.InRange(rows.Count, 1, 24);
            Assert.Equal(rows.Count, root.GetProperty("sessions_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(rows.Count, root.GetProperty("next_offset").GetInt32());
            AssertSaysItWasCutForBytes(root, 25, rows.Count);
            Assert.Equal(Enumerable.Range(0, rows.Count).Select(i => 20_000 + i), rows.Select(r => r.GetProperty("pid").GetInt32()));
            Assert.All(rows, r => Assert.StartsWith("order-fulfilment-worker/3.18.4 (pool=primary-writer; pod=fulfilment-7c9d8b6f5-", r.GetProperty("application_name").GetString(), StringComparison.Ordinal));

            /* The counts above the rows cover the rows the answer lists, not the ones it left out. */
            Assert.Equal(rows.Count, root.GetProperty("horizon_holder_count").GetInt32());
        });
    }

    [Fact]
    public async Task PgServerConfig_Default_OneHundredFiftySettings_FitsTheBudget_AndSaysWhatItLeftOut()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            const int settings = 150;
            var at = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10));
            var collectionId = CollectionIdGenerator.Next();
            for (var i = 0; i < settings; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source,
     boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, 'kB', 'Resource Usage / Memory', 'sighup', 'integer', 'configuration file',
        '4096', $6, '/var/lib/postgresql/data/postgresql.conf', $7, FALSE, $8)",
                    collectionId, at, ServerId, ServerName, SettingName(i), (65536 + i).ToString(System.Globalization.CultureInfo.InvariantCulture), 100 + i,
                    $"Sets the maximum memory to be used for workload class {i:D3} before the server switches to temporary disk files.");
            }

            var root = AssertWithinBudget("get_pg_server_config", await DarlingMcpPgServerStateTools.GetPgServerConfig(postgres, ServerName, cancellationToken: ct));
            var rows = root.GetProperty("settings").EnumerateArray().ToList();

            /* The limit's page is 100 of the 150; fewer than 100 fit, each one whole. */
            Assert.InRange(rows.Count, 1, 99);
            Assert.Equal(rows.Count, root.GetProperty("settings_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(rows.Count, root.GetProperty("next_offset").GetInt32());
            AssertSaysItWasCutForBytes(root, 100, rows.Count);

            /* The snapshot's own count is not the page's: it still says how many settings there are to page through. */
            Assert.Equal(settings, root.GetProperty("non_default_count").GetInt32());
            Assert.Equal(rows.Count, root.GetProperty("non_default_returned").GetInt32());
            Assert.All(rows, r =>
            {
                Assert.StartsWith("workload_class_memory_ceiling_for_tenant_group_", r.GetProperty("name").GetString(), StringComparison.Ordinal);
                Assert.StartsWith("Sets the maximum memory to be used for workload class ", r.GetProperty("description").GetString(), StringComparison.Ordinal);
                Assert.EndsWith(" before the server switches to temporary disk files.", r.GetProperty("description").GetString(), StringComparison.Ordinal);
            });
            Assert.Equal(rows.Count, rows.Select(r => r.GetProperty("name").GetString()).Distinct(StringComparer.Ordinal).Count());
        });
    }

    [Fact]
    public async Task StoreLog_Default_RetainedPlansThousandsOfCharactersLong_FitsTheBudget_PreviewsTheEvidence_AndKeepsTheCensusWhole()
    {
        await WithStoreAsync(async (_, postgres, ct) =>
        {
            const int plans = 40;
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            for (var i = 0; i < plans; i++)
            {
                await WriteCaptureAsync(postgres, newest.AddMinutes(-i), LoggedPlan(i), ct);
            }

            var root = AssertWithinBudget("get_store_log", await DarlingMcpStoreLogTools.GetStoreLog(postgres, cancellationToken: ct));
            var rows = root.GetProperty("retained_events").EnumerateArray().ToList();

            /* Every plan is its own retained row, and each row's evidence is a marked preview of a much longer entry. */
            Assert.InRange(rows.Count, 1, plans);
            Assert.Equal(rows.Count, root.GetProperty("retained_events_returned").GetInt32());
            Assert.All(rows, r =>
            {
                Assert.True(r.GetProperty("sample_line_truncated").GetBoolean());
                Assert.InRange(r.GetProperty("sample_line").GetString()!.Length, 1, 400);
                Assert.True(r.GetProperty("sample_line_length").GetInt32() > 3000, "the stored entry is thousands of characters long");
                Assert.False(string.IsNullOrEmpty(r.GetProperty("event_class").GetString()));
                Assert.False(string.IsNullOrEmpty(r.GetProperty("message_text").GetString()));
            });

            /* The census above the rows is never what gives way: every capture and every entry is still counted. */
            Assert.Equal(plans, root.GetProperty("capture_summary").GetProperty("captures").GetInt64());
            Assert.Equal(plans, root.GetProperty("classes").EnumerateArray().Sum(c => c.GetProperty("occurrences_window").GetInt64()));

            /* When rows were left out for bytes the answer says so, with both counts. */
            var cut = root.GetProperty("budget_truncated").GetBoolean();
            Assert.Equal(cut, root.GetProperty("budget_note").ValueKind == JsonValueKind.String);
            if (cut)
            {
                Assert.Contains("32,768-byte response budget", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
                Assert.Contains($"only {rows.Count:#,0} fit", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
            }
            else
            {
                Assert.Equal(plans, rows.Count);
            }
        });
    }

    private static DateTime ParseUtc(string value) =>
        DateTime.Parse(value, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal);

    /// <summary>A digest's long text, distinct per alert from its first characters on.</summary>
    private static string DetailText(int i, int length)
    {
        var text = new StringBuilder($"Digest {i:D2}: ");
        for (var line = 0; text.Length < length; line++)
        {
            text.Append(System.Globalization.CultureInfo.InvariantCulture, $"collector_{line:D3} ran 288 times, p95 {1200 + line} ms, peak {4800 + line} ms, 0 failures; ");
        }

        return text.ToString(0, length);
    }

    private static string QueryHash(int i) => $"0x{0x8568_0000_0000L + i:X16}";

    /// <summary>About 2,500 characters of statement, distinct per query from its first characters on.</summary>
    private static string QueryText(int i)
    {
        var text = new StringBuilder($"SELECT /* report {i:D3} */ o.order_id");
        for (var column = 0; text.Length < 2400; column++)
        {
            text.Append(System.Globalization.CultureInfo.InvariantCulture, $", o.measure_{column:D3} + l.adjustment_{column:D3} AS adjusted_measure_{column:D3}");
        }

        return text.Append(" FROM dbo.orders AS o JOIN dbo.order_lines AS l ON l.order_id = o.order_id WHERE o.region_id = @region").ToString();
    }

    private static string SettingName(int i) => $"workload_class_memory_ceiling_for_tenant_group_{i:D3}";

    /// <summary>One auto_explain entry as the store's own log holds it: a duration line and a JSON plan of several
    /// thousand characters. Each plan reads a different relation with different estimates, so no two share a row.</summary>
    private static string LoggedPlan(int i)
    {
        var relation = "ledger_" + new string((char)('a' + (i % 26)), 3) + new string((char)('a' + (i / 26)), 2);
        var plan = new StringBuilder();
        plan.Append("2026-09-22 14:03:02.551 UTC [5288] LOG:  duration: 5123.456 ms  plan:\n");
        plan.Append(System.Globalization.CultureInfo.InvariantCulture,
            $"\t{{\"Query Text\": \"SELECT account_id, sum(amount) FROM {relation} GROUP BY account_id\", \"Plan\": {{\"Node Type\": \"Append\", \"Startup Cost\": 0.00, \"Total Cost\": {250000 + i}.00, \"Plan Rows\": {9000000 + i}, \"Plan Width\": 40, \"Plans\": [");
        for (var node = 0; node < 36; node++)
        {
            plan.Append(System.Globalization.CultureInfo.InvariantCulture,
                $"{(node == 0 ? string.Empty : ", ")}{{\"Node Type\": \"Seq Scan\", \"Parent Relationship\": \"Member\", \"Relation Name\": \"{relation}_partition_{node:D2}\", \"Alias\": \"{relation}_{node:D2}\", \"Startup Cost\": 0.00, \"Total Cost\": {41000 + node}.00, \"Plan Rows\": {250000 + i + node}, \"Plan Width\": 12}}");
        }

        return plan.Append("]}}\n").ToString();
    }

    /// <summary>Writes one hourly capture the way the sweep does: the classifier's groups, then the capture row.</summary>
    private static async Task WriteCaptureAsync(NpgsqlDataSource postgres, DateTime captureUtc, string slab, CancellationToken ct)
    {
        var census = StoreLogClassifier.Classify(slab);
        var captureTime = DateTime.SpecifyKind(captureUtc, DateTimeKind.Unspecified);
        await using var connection = await postgres.OpenConnectionAsync(ct);

        await using (var events = new NpgsqlCommand(StoreLogSweep.EventInsertSql, connection))
        {
            events.Parameters.AddWithValue(captureTime);
            events.Parameters.AddWithValue(census.Groups.Select(g => g.EventClass).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.Severity).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.Occurrences).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.MessageText).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.SampleLine).ToArray());
            await events.ExecuteNonQueryAsync(ct);
        }

        await using var capture = new NpgsqlCommand(StoreLogSweep.CaptureInsertSql, connection);
        capture.Parameters.AddWithValue(captureTime);
        capture.Parameters.AddWithValue("postgresql-Tue.log");
        capture.Parameters.AddWithValue((long)slab.Length);
        capture.Parameters.AddWithValue(0L);
        capture.Parameters.AddWithValue(census.LinesRead);
        capture.Parameters.AddWithValue(census.EntriesRead);
        capture.Parameters.AddWithValue(false);
        capture.Parameters.AddWithValue(census.GroupsDropped);
        await capture.ExecuteNonQueryAsync(ct);
    }
}
