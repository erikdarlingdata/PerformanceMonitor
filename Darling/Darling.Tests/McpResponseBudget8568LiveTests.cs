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
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
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

            /* full_text: the text whole, on fewer rows, with the same accounting. */
            var whole = AssertWithinBudget("get_alert_history full_text", await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, full_text: true));
            var wholeRows = whole.GetProperty("alerts").EnumerateArray().ToList();
            Assert.InRange(wholeRows.Count, 1, alerts.Count - 1);
            AssertSaysItWasCutForBytes(whole, 50, wholeRows.Count);
            for (var i = 0; i < wholeRows.Count; i++)
            {
                Assert.False(wholeRows[i].GetProperty("detail_text_truncated").GetBoolean());
                Assert.Equal(DetailText(i, detailLength), wholeRows[i].GetProperty("detail_text").GetString());
            }
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

            /* full_text: 2,000 characters a row, so the 20-row page no longer fits and says so. */
            var longer = AssertWithinBudget("get_top_queries_by_cpu full_text", await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, full_text: true, cancellationToken: ct));
            var longerRows = longer.GetProperty("queries").EnumerateArray().ToList();
            Assert.InRange(longerRows.Count, 1, 19);
            Assert.True(longer.GetProperty("truncated").GetBoolean());
            Assert.Contains($"20 queries were read; only {longerRows.Count} fit", longer.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
            Assert.All(longerRows, r => Assert.InRange(r.GetProperty("query_text").GetString()!.Length, 1900, 2000 + "... (truncated)".Length));

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

            /* Nothing is dropped: following next_offset reads every session exactly once, each page within the budget. */
            var seen = new List<int>();
            var page = root;
            for (var calls = 0; calls < 50; calls++)
            {
                seen.AddRange(page.GetProperty("sessions").EnumerateArray().Select(r => r.GetProperty("pid").GetInt32()));
                if (!page.GetProperty("truncated").GetBoolean())
                {
                    Assert.Equal(JsonValueKind.Null, page.GetProperty("next_offset").ValueKind);
                    break;
                }

                page = AssertWithinBudget("get_pg_session_states", await DarlingMcpPgSessionStatesTools.GetPgSessionStates(
                    postgres, ServerName, offset: page.GetProperty("next_offset").GetInt32(), cancellationToken: ct));
            }

            Assert.Equal(Enumerable.Range(0, sessions).Select(i => 20_000 + i), seen);
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

            /* Nothing is dropped: following next_offset reads every setting exactly once, each page within the budget. */
            var seen = new List<string>();
            var page = root;
            for (var calls = 0; calls < 50; calls++)
            {
                seen.AddRange(page.GetProperty("settings").EnumerateArray().Select(r => r.GetProperty("name").GetString()!));
                if (!page.GetProperty("truncated").GetBoolean())
                {
                    Assert.Equal(JsonValueKind.Null, page.GetProperty("next_offset").ValueKind);
                    break;
                }

                page = AssertWithinBudget("get_pg_server_config", await DarlingMcpPgServerStateTools.GetPgServerConfig(
                    postgres, ServerName, offset: page.GetProperty("next_offset").GetInt32(), cancellationToken: ct));
            }

            Assert.Equal(Enumerable.Range(0, settings).Select(SettingName), seen.OrderBy(name => name, StringComparer.Ordinal));
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

            /* full_text: each row's evidence whole, so far fewer rows fit, and the answer says how many. */
            var whole = AssertWithinBudget("get_store_log full_text", await DarlingMcpStoreLogTools.GetStoreLog(postgres, full_text: true, cancellationToken: ct));
            var wholeRows = whole.GetProperty("retained_events").EnumerateArray().ToList();
            Assert.InRange(wholeRows.Count, 1, rows.Count - 1);
            Assert.True(whole.GetProperty("budget_truncated").GetBoolean());
            Assert.Contains($"{plans} retained messages were read; only {wholeRows.Count} fit", whole.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
            Assert.All(wholeRows, r =>
            {
                Assert.False(r.GetProperty("sample_line_truncated").GetBoolean());
                Assert.Equal(r.GetProperty("sample_line_length").GetInt32(), r.GetProperty("sample_line").GetString()!.Length);
            });
            Assert.Equal(plans, whole.GetProperty("capture_summary").GetProperty("captures").GetInt64());
        });
    }

    /// <summary>
    /// A reply cut for the budget keeps its newest rows, so the rows left out are older ones and the way to them is
    /// an earlier window end. The reply names the oldest <c>last_capture_at</c> it returned, its note points
    /// <c>as_of</c> there, and a reader who follows it reaches every message.
    /// </summary>
    [Fact]
    public async Task StoreLog_CutForTheBudget_PointsAsOfAtTheOldestRowReturned_AndFollowingItReadsEveryMessageLeftOut()
    {
        await WithStoreAsync(async (_, postgres, ct) =>
        {
            const int plans = 40;
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            for (var i = 0; i < plans; i++)
            {
                await WriteCaptureAsync(postgres, newest.AddMinutes(-i), LoggedPlan(i), ct);
            }

            /* Whole evidence lines: a page holds a handful of the 40 messages, one capture each. */
            var seen = new List<string>();
            string? asOf = null;
            for (var calls = 0; calls < plans; calls++)
            {
                var root = AssertWithinBudget("get_store_log full_text", await DarlingMcpStoreLogTools.GetStoreLog(postgres, as_of: asOf, full_text: true, cancellationToken: ct));
                var rows = root.GetProperty("retained_events").EnumerateArray().ToList();
                var fresh = rows.Select(r => r.GetProperty("message_text").GetString()!).Except(seen).ToList();
                Assert.True(fresh.Count > 0, $"the page read at as_of {asOf ?? "(now)"} held no message the pages before it had not.");
                seen.AddRange(fresh);
                if (!root.GetProperty("budget_truncated").GetBoolean())
                {
                    break;
                }

                Assert.True(root.TryGetProperty("oldest_returned_last_capture_at", out var oldest) && oldest.ValueKind == JsonValueKind.String,
                    "a reply cut for the budget names the oldest last_capture_at among the rows it returned.");
                Assert.Equal(rows.Min(r => ParseUtc(r.GetProperty("last_capture_at").GetString()!)), ParseUtc(oldest.GetString()!));
                var note = root.GetProperty("budget_note").GetString();
                Assert.Contains("as_of at oldest_returned_last_capture_at", note, StringComparison.Ordinal);
                asOf = oldest.GetString();
            }

            Assert.Equal(plans, seen.Count);
        });
    }

    /// <summary>
    /// Every message of one capture carries that capture's time, and a window includes its end. When the rows that
    /// fit are all from one capture, a window ending at that time returns them again, so the note names the ways that
    /// do move: first an <c>offset</c> past the rows returned, which reads the rest of that capture, then previews,
    /// which fit more of its messages, and a window that ends before it.
    /// </summary>
    [Fact]
    public async Task StoreLog_CutInsideOneCapture_SendsTheReaderToPreviewsAndToTheCapturesBeforeIt()
    {
        await WithStoreAsync(async (_, postgres, ct) =>
        {
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            var earlier = newest.AddHours(-1);
            await WriteCaptureAsync(postgres, newest, string.Concat(Enumerable.Range(0, 12).Select(LoggedPlan)), ct);
            await WriteCaptureAsync(postgres, earlier, string.Concat(Enumerable.Range(12, 2).Select(LoggedPlan)), ct);

            var root = AssertWithinBudget("get_store_log full_text", await DarlingMcpStoreLogTools.GetStoreLog(postgres, full_text: true, cancellationToken: ct));
            var held = root.GetProperty("retained_events").EnumerateArray().Select(r => r.GetProperty("message_text").GetString()!).ToList();
            Assert.True(root.GetProperty("budget_truncated").GetBoolean());
            Assert.InRange(held.Count, 1, 11);
            Assert.True(root.TryGetProperty("oldest_returned_last_capture_at", out var oldest) && oldest.ValueKind == JsonValueKind.String,
                "a reply cut for the budget names the oldest last_capture_at among the rows it returned.");
            Assert.Equal(newest, ParseUtc(oldest.GetString()!));

            /* A window ending at that capture is the same page. */
            var again = AssertWithinBudget("get_store_log at the capture", await DarlingMcpStoreLogTools.GetStoreLog(postgres, as_of: oldest.GetString(), full_text: true, cancellationToken: ct));
            Assert.Equal(held, again.GetProperty("retained_events").EnumerateArray().Select(r => r.GetProperty("message_text").GetString()!));

            var note = root.GetProperty("budget_note").GetString();
            Assert.DoesNotContain("as_of at oldest_returned_last_capture_at", note, StringComparison.Ordinal);
            Assert.Contains("full_text", note, StringComparison.Ordinal);
            Assert.Contains("as_of earlier than", note, StringComparison.Ordinal);

            /* The way on inside the capture comes first: an offset past the rows held reads messages the page left out. */
            Assert.Contains($"offset={held.Count}", note, StringComparison.Ordinal);
            Assert.True(note!.IndexOf("offset=", StringComparison.Ordinal) < note.IndexOf("full_text", StringComparison.Ordinal),
                $"the note names the offset before the previews: {note}");
            var onward = AssertWithinBudget("get_store_log at the offset", await ReadStoreLogAsync(
                postgres, held.Count, root.GetProperty("as_of").GetString(), fullText: true, ct));
            var onwardMessages = onward.GetProperty("retained_events").EnumerateArray().Select(r => r.GetProperty("message_text").GetString()!).ToList();
            Assert.NotEmpty(onwardMessages);
            Assert.All(onwardMessages, message => Assert.DoesNotContain(message, held));

            /* Previews: more of the capture's twelve messages fit, among them ones the whole lines left out. */
            var previews = AssertWithinBudget("get_store_log", await DarlingMcpStoreLogTools.GetStoreLog(postgres, cancellationToken: ct));
            var previewed = previews.GetProperty("retained_events").EnumerateArray().Select(r => r.GetProperty("message_text").GetString()!).ToList();
            Assert.Contains(previewed, message => !held.Contains(message));

            /* A window that ends before the capture reads the one before it. */
            var before = AssertWithinBudget("get_store_log before the capture", await DarlingMcpStoreLogTools.GetStoreLog(
                postgres, as_of: newest.AddSeconds(-1).ToString("o", System.Globalization.CultureInfo.InvariantCulture), full_text: true, cancellationToken: ct));
            var beforeRows = before.GetProperty("retained_events").EnumerateArray().ToList();
            Assert.Equal(2, beforeRows.Count);
            Assert.All(beforeRows, r =>
            {
                Assert.Equal(earlier, ParseUtc(r.GetProperty("last_capture_at").GetString()!));
                Assert.DoesNotContain(r.GetProperty("message_text").GetString()!, held);
            });
        });
    }

    /// <summary>
    /// One capture keeps up to twenty distinct messages a class, each message whole, so one capture's rows can be
    /// more than the budget holds, and no <c>as_of</c> divides a capture. <c>offset</c> does: a reader who passes the
    /// offset each note names, at the first answer's <c>as_of</c>, reads every message of the capture exactly once.
    /// The web viewer's read holds them all in one answer.
    /// </summary>
    [Fact]
    public async Task StoreLog_OneCaptureOverTheBudget_FollowingOffsetReadsEveryRetainedMessageExactlyOnce()
    {
        await WithStoreAsync(async (_, postgres, ct) =>
        {
            var slab = string.Concat(Enumerable.Range(0, 20).Select(LoggedPlan)) + string.Concat(Enumerable.Range(0, 20).Select(LongComplaint));
            var retained = StoreLogClassifier.Classify(slab).Groups.Count(g => g.MessageText is not null);
            Assert.Equal(40, retained);
            await WriteCaptureAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), slab, ct);

            var seen = new List<(string EventClass, string Message)>();
            var named = new List<(int Offset, string? Note)>();
            var offset = 0;
            string? asOf = null;
            var pages = 0;
            while (true)
            {
                Assert.True(pages < retained, $"{pages} pages did not reach the end of {retained} messages.");
                var root = AssertWithinBudget("get_store_log", await ReadStoreLogAsync(postgres, offset, asOf, fullText: false, ct));
                pages++;
                var rows = root.GetProperty("retained_events").EnumerateArray()
                    .Select(r => (EventClass: r.GetProperty("event_class").GetString()!, Message: r.GetProperty("message_text").GetString()!))
                    .ToList();
                Assert.Equal(rows.Count, root.GetProperty("retained_events_returned").GetInt32());
                var repeated = rows.Intersect(seen).Count();
                Assert.True(repeated == 0, $"the page read at offset {offset} repeated {repeated} of the messages the pages before it held.");
                seen.AddRange(rows);

                /* The census counts the whole capture on every page. */
                Assert.Equal(retained, root.GetProperty("classes").EnumerateArray().Sum(c => c.GetProperty("occurrences_window").GetInt64()));

                if (!root.GetProperty("budget_truncated").GetBoolean())
                {
                    break;
                }

                Assert.NotEmpty(rows);
                offset += rows.Count;
                named.Add((offset, root.GetProperty("budget_note").GetString()));
                asOf ??= root.GetProperty("as_of").GetString();
            }

            Assert.True(pages > 1, "the capture's messages are more than one answer holds.");
            Assert.Equal(retained, seen.Count);
            Assert.Equal(retained, seen.Distinct().Count());
            Assert.All(named, page => Assert.Contains($"offset={page.Offset}", page.Note, StringComparison.Ordinal));

            /* Rows that tie on capture and count are listed by class, so every read lists them the same way. */
            Assert.Equal(seen.Select(r => r.EventClass).OrderBy(c => c, StringComparer.Ordinal), seen.Select(r => r.EventClass));

            /* The web viewer's read is not held to the budget: one answer, every message. */
            var mirror = JsonDocument.Parse(await DarlingWebEndpoints.BuildReadDispatch()["get_store_log"](new DefaultHttpContext(), postgres, null!)).RootElement;
            Assert.False(mirror.GetProperty("budget_truncated").GetBoolean());
            Assert.Equal(
                seen.Select(r => r.Message).OrderBy(m => m, StringComparer.Ordinal),
                mirror.GetProperty("retained_events").EnumerateArray().Select(r => r.GetProperty("message_text").GetString()!).OrderBy(m => m, StringComparer.Ordinal));
        });
    }

    /// <summary>
    /// The alert page is newest first, so the alerts left out are older: <c>as_of</c> at the oldest one returned reads
    /// on to every alert, and fewer rows asked for is the same newest alerts again, which the note does not offer.
    /// </summary>
    [Fact]
    public async Task AlertHistory_CutForTheBudget_FollowingAsOfReadsEveryAlert_AndTheNoteDoesNotOfferFewerRows()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            const int seeded = 60;
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            for (var i = 0; i < seeded; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 91.5, 80, TRUE, 'email', NULL, FALSE, FALSE, $5, NULL)",
                    DarlingMcpTestData.Naive(newest.AddMinutes(-i)), ServerId, ServerName, $"Collector Cost Digest {i:D2}",
                    DetailText(i, 3000));
            }

            var first = AssertWithinBudget("get_alert_history", await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName));
            var held = first.GetProperty("alerts").EnumerateArray().Select(a => a.GetProperty("metric_name").GetString()!).ToList();
            Assert.True(first.GetProperty("budget_truncated").GetBoolean());

            /* Fewer rows asked for returns alerts the cut page already held, and none it left out. */
            var fewer = AssertWithinBudget("get_alert_history limit", await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, limit: held.Count - 1));
            Assert.All(fewer.GetProperty("alerts").EnumerateArray(), a => Assert.Contains(a.GetProperty("metric_name").GetString()!, held));
            var note = first.GetProperty("budget_note").GetString();
            Assert.Contains("as_of at oldest_returned_alert_time", note, StringComparison.Ordinal);
            Assert.DoesNotContain("limit", note, StringComparison.Ordinal);

            var seen = new List<string>(held);
            var asOf = first.GetProperty("oldest_returned_alert_time").GetString();
            for (var calls = 0; calls < seeded; calls++)
            {
                var page = AssertWithinBudget("get_alert_history as_of", await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, as_of: asOf));
                var fresh = page.GetProperty("alerts").EnumerateArray().Select(a => a.GetProperty("metric_name").GetString()!).Except(seen).ToList();
                Assert.True(fresh.Count > 0, $"the page read at as_of {asOf} held no alert the pages before it had not.");
                seen.AddRange(fresh);
                if (!page.GetProperty("truncated").GetBoolean())
                {
                    break;
                }

                asOf = page.GetProperty("oldest_returned_alert_time").GetString();
            }

            Assert.Equal(Enumerable.Range(0, seeded).Select(i => $"Collector Cost Digest {i:D2}"), seen);
        });
    }

    /// <summary>
    /// The query page is heaviest first, so the queries left out are lighter ones. Fewer rows asked for is the same
    /// heaviest queries again, which the note does not offer; one database's page reads queries the wide page left out.
    /// </summary>
    [Fact]
    public async Task TopQueriesByCpu_CutForTheBudget_TheNotesFilterReadsQueriesLeftOut_AndItDoesNotOfferFewerRows()
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
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, i % 2 == 0 ? "Sales" : "Billing",
                    QueryHash(i), "0xPLANHASH", $"0xSQLH{i:D6}", "0xPLANH", QueryText(i),
                    DBNull.Value, 1000L + (queries - i), (queries - i) * 1_000_000L, (queries - i) * 2_000_000L, 5000L, 1, 1);
            }

            var wide = AssertWithinBudget("get_top_queries_by_cpu top=120", await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, top: queries, cancellationToken: ct));
            var held = wide.GetProperty("queries").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()!).ToList();
            Assert.True(wide.GetProperty("truncated").GetBoolean());
            Assert.InRange(held.Count, 2, queries - 1);

            /* Fewer rows asked for returns queries the cut page already held, and none it left out. */
            var fewer = AssertWithinBudget("get_top_queries_by_cpu fewer", await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, top: held.Count - 1, cancellationToken: ct));
            Assert.All(fewer.GetProperty("queries").EnumerateArray(), r => Assert.Contains(r.GetProperty("query_hash").GetString()!, held));
            var note = wide.GetProperty("budget_note").GetString();
            Assert.DoesNotContain("row count", note, StringComparison.Ordinal);
            Assert.Contains("database_name", note, StringComparison.Ordinal);

            /* One database's page holds queries the wide page left out. */
            var billing = AssertWithinBudget("get_top_queries_by_cpu Billing", await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, top: queries, database_name: "Billing", cancellationToken: ct));
            Assert.Contains(billing.GetProperty("queries").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()!), hash => !held.Contains(hash));
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

    /// <summary>Calls get_store_log by its parameter names, so the call reads the same whether or not the tool takes
    /// <c>offset</c>: a tool without the parameter answers from its first row.</summary>
    private static Task<string> ReadStoreLogAsync(NpgsqlDataSource postgres, int offset, string? asOf, bool fullText, CancellationToken ct)
    {
        var method = typeof(DarlingMcpStoreLogTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(DarlingMcpStoreLogTools.GetStoreLog));
        var supplied = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["postgres"] = postgres,
            ["as_of"] = asOf,
            ["full_text"] = fullText,
            ["offset"] = offset,
            ["cancellationToken"] = ct,
        };
        var args = method.GetParameters()
            .Select(p => supplied.TryGetValue(p.Name!, out var value) ? value : p.HasDefaultValue ? p.DefaultValue : null)
            .ToArray();
        return McpReadToolBudgetLiveTests.InvokeAsync(method, args);
    }

    /// <summary>One ERROR no rule recognises, so it keeps its text: a message of about 1,900 characters, kept whole.
    /// Each names a different relation, in letters, so no two share a row.</summary>
    private static string LongComplaint(int i)
    {
        var relation = "journal_" + new string((char)('a' + i), 4);
        var line = new StringBuilder("2026-09-22 14:03:02.551 UTC [5288] ERROR:  reconciliation of ").Append(relation).Append(" stopped");
        for (var part = 0; line.Length < 1900; part++)
        {
            line.Append(", partition ").Append(relation).Append('_').Append((char)('a' + (part % 26)), 3).Append(" disagrees with its summary");
        }

        return line.Append('\n').ToString();
    }

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
