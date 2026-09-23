/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgPlanCaptureCollector"/>'s self-hosted <c>pg_read_file</c> route against a REAL target with
/// <c>auto_explain</c> actually writing to a REAL log file (#4008) — the SQL-side twin of
/// <see cref="PgPlanLogParserTests.AForgedHeaderInsideAStatementEcho_YieldsNoPlan"/> and its colon-prefixed
/// sibling in <c>Lite.Tests</c>, neither of which exercises the SQL <c>regexp_matches</c> this class fixed
/// alongside the C# one. A unit test proves the C# parser; only a live target proves the SQL did too, because
/// the two are separate spellings of the same pattern in separate languages and only one shipped correctly
/// before #4008 found neither anchored to a real log line.
///
/// <para><b>Why this needs its own target rather than the shared <c>DARLING_TEST_PG</c> store.</b>
/// <c>logging_collector</c> is postmaster-context (restart-only) and <c>log_line_prefix</c> has to carry
/// <c>%Q</c> for a query id to exist at all — settings the shared store must not carry, because every other
/// live test that opens it would then have its own session chatter landing in a log this test reads. Gated on
/// <c>DARLING_TEST_PG_AUTOEXPLAIN</c> instead: a connection string to a target (any Postgres 13+, self-hosted,
/// with filesystem access) started with <c>logging_collector = on</c> and
/// <c>log_line_prefix = '%m [%p] %Q '</c> in its <c>postgresql.conf</c> from before its first start. Nothing
/// else needs presetting — <c>auto_explain</c> loads per SESSION (<c>LOAD 'auto_explain'</c>) and every knob
/// this test needs is <c>SET</c>-able by a superuser without a restart, so the target does not need the
/// extension preloaded.</para>
/// </summary>
public sealed class PgPlanCaptureLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_AUTOEXPLAIN");

    /// <summary>
    /// A real query still captures — correct query id, duration and top node type, through the FIXED SQL —
    /// while a forged STATEMENT: echo sharing the same log tail, planted by the exact PoC #4008 measured live
    /// (a syntax error whose offending SQL contains a comment reading like a plan header, with the JSON that
    /// follows arriving as the statement's own tab-indented continuation), produces no plan at all: no row
    /// carries its telltale relation name, and no row carries the forged query id the attacker chose.
    /// </summary>
    [Fact]
    public async Task ARealPlanStillCaptures_AndAForgedStatementEcho_YieldsNone()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG_AUTOEXPLAIN to a Postgres connection string for a target started with " +
            "logging_collector = on and log_line_prefix = '%m [%p] %Q ' to run the live plan-capture test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        /* auto_explain loads per session and every knob here is SET-able by a superuser without touching
           shared_preload_libraries — see the type header for why the target only has to preset the two
           postmaster-context settings that cannot be reached this way. */
        await using (var setup = new NpgsqlCommand(
            "LOAD 'auto_explain';" +
            "SET auto_explain.log_min_duration = 0;" +
            "SET auto_explain.log_analyze = true;" +
            "SET auto_explain.log_format = 'json';" +
            "SET compute_query_id = on;" +
            "DROP TABLE IF EXISTS plan_capture_live_4008;" +
            "CREATE TABLE plan_capture_live_4008 (id int);" +
            "INSERT INTO plan_capture_live_4008 VALUES (1);",
            connection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        /* The real, distinctively-named query auto_explain must still capture. */
        await using (var real = new NpgsqlCommand("SELECT * FROM plan_capture_live_4008;", connection))
        await using (var reader = await real.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
            }
        }

        /* The issue's own PoC, reproduced live and measured against this exact target (#4008): a syntax
           error's STATEMENT: companion echoes the offending SQL back verbatim, including its embedded
           newline — which PostgreSQL renders as an ordinary tab-indented continuation, exactly what a real
           auto_explain block looks like on the wire. Query id 7 and the HUNTER2_LEAK marker are the
           forger's choice, not auto_explain's. */
        await using (var forged = new NpgsqlCommand(
            "SELECT 1 -- [1] 7 LOG:  duration: 1.0 ms  plan:\n"
            + "{\"Plan\": {\"Node Type\": \"Seq Scan\", \"Relation Name\": \"HUNTER2_LEAK\", \"Alias\": \"card 4111111111111111\"}};",
            connection))
        {
            try
            {
                await forged.ExecuteNonQueryAsync(ct);
                Assert.Fail("The forged statement was expected to be a syntax error and was not.");
            }
            catch (PostgresException ex) when (ex.SqlState == "42601" /* syntax_error */)
            {
                /* Expected: this is what puts the forged text into the STATEMENT: companion. */
            }
        }

        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-rig-as-target",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 18,
                PostgresVersionNum = 180000,
            },
        };
        var sql = PgPlanCaptureCollector.Instance.BuildQuery(context).Text;

        System.Collections.Generic.List<PgPlanCaptureCollector.Row> rows;
        await using (var read = new NpgsqlCommand(sql, connection))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            rows = await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, ct);
        }

        Assert.Contains(rows, r => r.TopNodeType == "Seq Scan" && r.PlanJson.Contains("plan_capture_live_4008", StringComparison.Ordinal));
        Assert.DoesNotContain(rows, r => r.PlanJson.Contains("HUNTER2_LEAK", StringComparison.Ordinal));
        Assert.DoesNotContain(rows, r => r.QueryId == 7);

        await using (var cleanup = new NpgsqlCommand("DROP TABLE IF EXISTS plan_capture_live_4008;", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
