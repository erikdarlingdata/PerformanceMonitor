/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store statement history snapshot (#5097) against a real server. Each fact mints its own scratch database, migrates
/// it, and stands in for the reader functions with plain SQL functions over planted tables, so the epoch and the counters
/// are exactly what the fact says and the shared cluster's own statement counters are never reset.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreStatementHistoryLiveTests
{
    private static readonly DateTime T0 = new(2026, 3, 1, 10, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Fakes = @"
CREATE TABLE public.fake_stats
(
    role_name text, queryid bigint, calls bigint, total_exec_ms double precision, max_exec_ms double precision,
    rows_returned bigint, shared_blks_hit bigint, shared_blks_read bigint, temp_blks_written bigint
);
CREATE TABLE public.fake_info (stats_reset timestamptz, dealloc bigint);
INSERT INTO public.fake_info VALUES (NULL, 0);
CREATE FUNCTION config.store_statement_stats()
RETURNS TABLE (role_name text, queryid bigint, calls bigint, total_exec_ms double precision, mean_exec_ms double precision,
               max_exec_ms double precision, total_plan_ms double precision, rows_returned bigint, shared_blks_hit bigint,
               shared_blks_read bigint, temp_blks_written bigint, query text)
LANGUAGE sql AS $$ SELECT f.role_name, f.queryid, f.calls, f.total_exec_ms, f.total_exec_ms / NULLIF(f.calls, 0), f.max_exec_ms,
                          0::double precision, f.rows_returned, f.shared_blks_hit, f.shared_blks_read, f.temp_blks_written, NULL::text
                   FROM public.fake_stats f $$;
CREATE FUNCTION config.store_statement_stats_info()
RETURNS TABLE (stats_reset timestamptz, dealloc bigint)
LANGUAGE sql AS $$ SELECT i.stats_reset, i.dealloc FROM public.fake_info i $$;";

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> StartAsync(CancellationToken ct, bool withFakes = true)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the statement history snapshot's live pins (each mints its own scratch database).");
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (withFakes)
        {
            await ExecAsync(connection, Fakes, ct);
        }

        return (scratch, connection);
    }

    private static async Task SetStats(NpgsqlConnection c, string rows, CancellationToken ct)
    {
        await ExecAsync(c, "TRUNCATE public.fake_stats", ct);
        if (rows.Length > 0)
        {
            await ExecAsync(c, "INSERT INTO public.fake_stats VALUES " + rows, ct);
        }
    }

    [Fact]
    public async Task TheFirstCapture_Rebaselines_WritesNoHistory_AndFillsTheBaseline()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = c;
        var ok = false;
        try
        {
            await SetStats(c, "('owner', 1, 10, 100, 20, 10, 5, 1, 0), ('owner', 2, 4, 40, 15, 4, 1, 0, 0)", ct);
            var result = await StoreStatementHistory.SnapshotAsync(c, T0, null, ct);

            Assert.Equal(StoreStatementHistory.OutcomeRebaselined, result.Outcome);
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_history", ct));
            Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM config.store_statement_baseline", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_captures WHERE outcome = 'rebaselined' AND interval_seconds IS NULL", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ASameEpochDelta_IsCurrentMinusBaseline_IdleStatementsWriteNothing_AndAServiceRestartChangesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = c;
        var ok = false;
        try
        {
            await SetStats(c, "('owner', 1, 10, 100, 20, 10, 5, 1, 0), ('owner', 2, 4, 40, 15, 4, 1, 0, 0)", ct);
            await StoreStatementHistory.SnapshotAsync(c, T0, null, ct);

            /* Statement 1 ran 5 more times for 50 ms; statement 2 was idle; statement 3 appeared. */
            await SetStats(c, "('owner', 1, 15, 150, 25, 15, 8, 1, 0), ('owner', 2, 4, 40, 15, 4, 1, 0, 0), ('owner', 3, 2, 8, 5, 2, 0, 0, 0)", ct);

            /* A new connection stands in for a restarted service: nothing but the store is carried over. */
            await using var second = new NpgsqlConnection(scratch.ConnectionString);
            await second.OpenAsync(ct);
            var result = await StoreStatementHistory.SnapshotAsync(second, T0.AddHours(1), null, ct);

            Assert.Equal(StoreStatementHistory.OutcomeOk, result.Outcome);
            Assert.Equal(2, result.StatementsSeen);
            Assert.Equal(2, result.StatementsKept);
            Assert.Equal(5L, await ScalarAsync(c, "SELECT delta_calls FROM collect.store_statement_history WHERE queryid = 1", ct));
            Assert.Equal(50d, await ScalarAsync(c, "SELECT delta_total_exec_ms FROM collect.store_statement_history WHERE queryid = 1", ct));
            Assert.Equal(3600, await ScalarAsync(c, "SELECT interval_seconds FROM collect.store_statement_history WHERE queryid = 1", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_history WHERE queryid = 2", ct));
            Assert.Equal(true, await ScalarAsync(c, "SELECT first_seen FROM collect.store_statement_history WHERE queryid = 3", ct));
            Assert.Equal(false, await ScalarAsync(c, "SELECT first_seen OR entry_restarted OR reset_in_interval FROM collect.store_statement_history WHERE queryid = 1", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AFallenCounter_CreditsTheCurrentValue_AsAnEntryRestart()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = c;
        var ok = false;
        try
        {
            await SetStats(c, "('owner', 1, 100, 1000, 20, 10, 5, 1, 0)", ct);
            await StoreStatementHistory.SnapshotAsync(c, T0, null, ct);
            await SetStats(c, "('owner', 1, 3, 30, 20, 3, 1, 0, 0)", ct);
            await StoreStatementHistory.SnapshotAsync(c, T0.AddHours(1), null, ct);

            Assert.Equal(3L, await ScalarAsync(c, "SELECT delta_calls FROM collect.store_statement_history WHERE queryid = 1", ct));
            Assert.Equal(true, await ScalarAsync(c, "SELECT entry_restarted FROM collect.store_statement_history WHERE queryid = 1", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AResetInsideTheInterval_CreditsTheCurrentValue_AndARebaselineWritesNoHistory()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = c;
        var ok = false;
        try
        {
            await ExecAsync(c, "UPDATE public.fake_info SET stats_reset = TIMESTAMPTZ '2026-02-20 00:00:00+00'", ct);
            await SetStats(c, "('owner', 1, 100, 1000, 20, 10, 5, 1, 0)", ct);
            await StoreStatementHistory.SnapshotAsync(c, T0, null, ct);

            /* The reset lands 20 minutes after the previous capture. */
            await ExecAsync(c, "UPDATE public.fake_info SET stats_reset = TIMESTAMPTZ '2026-03-01 10:20:00+00'", ct);
            await SetStats(c, "('owner', 1, 7, 70, 20, 7, 1, 0, 0)", ct);
            var inside = await StoreStatementHistory.SnapshotAsync(c, T0.AddHours(1), null, ct);
            Assert.Equal(StoreStatementHistory.OutcomeOk, inside.Outcome);
            Assert.Equal(7L, await ScalarAsync(c, "SELECT delta_calls FROM collect.store_statement_history", ct));
            Assert.Equal(true, await ScalarAsync(c, "SELECT reset_in_interval FROM collect.store_statement_history", ct));

            /* A stamp that moved to a time BEFORE the previous capture cannot be placed. */
            await ExecAsync(c, "UPDATE public.fake_info SET stats_reset = TIMESTAMPTZ '2026-01-01 00:00:00+00'", ct);
            await SetStats(c, "('owner', 1, 9, 90, 20, 9, 1, 0, 0)", ct);
            var rebase = await StoreStatementHistory.SnapshotAsync(c, T0.AddHours(2), null, ct);
            Assert.Equal(StoreStatementHistory.OutcomeRebaselined, rebase.Outcome);
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_history", ct));
            Assert.Equal(9L, await ScalarAsync(c, "SELECT calls FROM config.store_statement_baseline WHERE queryid = 1", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheCap_KeepsTheTopStatementsByTime_AndTheCaptureCountsWhatItSaw_AndHiddenStatements()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = c;
        var ok = false;
        try
        {
            await StoreStatementHistory.SnapshotAsync(c, T0, null, ct);
            await ExecAsync(c,
                "INSERT INTO public.fake_stats SELECT 'owner', g, 1, g, g, 1, 0, 0, 0 FROM generate_series(1, " + (StoreStatementHistory.TopStatements + 25) + ") g", ct);
            await ExecAsync(c, "INSERT INTO public.fake_stats VALUES ('other', NULL, 5, 5, 5, 5, 0, 0, 0)", ct);
            var result = await StoreStatementHistory.SnapshotAsync(c, T0.AddHours(1), null, ct);

            Assert.Equal(StoreStatementHistory.TopStatements + 25, result.StatementsSeen);
            Assert.Equal(StoreStatementHistory.TopStatements, result.StatementsKept);
            Assert.Equal(1, result.HiddenStatements);
            Assert.Equal((long)StoreStatementHistory.TopStatements, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_history", ct));
            Assert.Equal(26L, await ScalarAsync(c, "SELECT min(queryid) FROM collect.store_statement_history", ct));
            Assert.Equal(1, await ScalarAsync(c, "SELECT hidden_statements FROM collect.store_statement_captures ORDER BY capture_time DESC LIMIT 1", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Retention_DeletesOldHistoryAndCaptures_AndNeverTheBaseline()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = c;
        var ok = false;
        try
        {
            var old = T0.AddDays(-(StoreStatementHistory.RetentionDays + 5));
            await ExecAsync(c, $"INSERT INTO collect.store_statement_captures (capture_time, statements_seen, statements_kept, hidden_statements, outcome) VALUES ('{old:yyyy-MM-dd HH:mm:ss}', 0, 0, 0, 'ok')", ct);
            await ExecAsync(c, $"INSERT INTO collect.store_statement_history VALUES ('{old:yyyy-MM-dd HH:mm:ss}', 3600, 'owner', 9, 1, 1, 1, 0, 0, 0, 1, false, false, false)", ct);
            await ExecAsync(c, $"INSERT INTO config.store_statement_baseline VALUES ('owner', 77, 1, 1, 1, 0, 0, 0, '{old:yyyy-MM-dd HH:mm:ss}')", ct);
            await SetStats(c, "('owner', 77, 1, 1, 1, 1, 0, 0, 0)", ct);

            await StoreStatementHistory.SnapshotAsync(c, T0, null, ct);

            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_history", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_captures", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM config.store_statement_baseline WHERE queryid = 77", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task WhenTheReaderIsNotThere_ThePassRecordsAPreconditionCapture_AndDoesNotThrow()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await StartAsync(ct, withFakes: false);
        await using var _ = scratch;
        await using var __ = c;
        var ok = false;
        try
        {
            var result = await StoreStatementHistory.SnapshotAsync(c, T0, null, ct);

            Assert.Equal(StoreStatementHistory.OutcomePrecondition, result.Outcome);
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_captures WHERE outcome = 'precondition'", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.store_statement_history", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM config.store_statement_baseline", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
