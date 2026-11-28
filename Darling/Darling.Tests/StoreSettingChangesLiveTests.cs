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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. ALTER SYSTEM is server wide, so each test that runs it puts its settings
   back with ALTER SYSTEM RESET before it ends, and runs on the scratch server only. */
/// <summary>
/// #9427: the store's setting changes between starts, against a real server. A capture is compared with the last one, so each test takes a baseline first, changes a
/// setting with <c>ALTER SYSTEM</c> and <c>pg_reload_conf()</c>, and captures again.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class StoreSettingChangesLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source, NpgsqlConnection Capture)> StartAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the store setting changes live pins (each mints its own scratch database).");
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var capture = new NpgsqlConnection(scratch.ConnectionString);
        await capture.OpenAsync(ct);
        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString), capture);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>Reloads the configuration and waits until the capture connection has loaded it (a backend loads it between commands, a moment after the signal).</summary>
    private static async Task ReloadAsync(NpgsqlDataSource source, NpgsqlConnection capture, CancellationToken ct)
    {
        var before = (DateTime)(await ScalarAsync(capture, "SELECT pg_conf_load_time()::timestamp", ct))!;
        await ExecAsync(source, "SELECT pg_reload_conf()", ct);
        for (var attempt = 0; attempt < 100; attempt++)
        {
            if ((DateTime)(await ScalarAsync(capture, "SELECT pg_conf_load_time()::timestamp", ct))! > before)
            {
                return;
            }

            await Task.Delay(100, ct);
        }

        Assert.Fail("the capture connection never loaded the reloaded configuration");
    }

    private static async Task ResetAsync(NpgsqlDataSource source, NpgsqlConnection capture, string setting, CancellationToken ct)
    {
        await ExecAsync(source, "ALTER SYSTEM RESET " + setting, ct);
        await ReloadAsync(source, capture, ct);
    }

    private static async Task<JsonElement> ReadAsync(NpgsqlDataSource source, string? setting, CancellationToken ct, int daysBack = 7, int limit = 50)
    {
        var json = await DarlingMcpStoreSettingChangesTools.GetStoreSettingChanges(source, daysBack, setting, limit, ct);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Fact]
    public async Task TheFirstCapture_StoresABaselineAndNoChangeRow_AndAQuietCaptureAfterItWritesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source, capture) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        await using var ___ = capture;
        var ok = false;
        try
        {
            var before = await ReadAsync(source, null, ct);
            Assert.Equal("unavailable", before.GetProperty("status").GetString());

            var baseline = await StoreSettingChanges.CaptureAsync(capture, ct);
            Assert.True(baseline.IsBaseline);
            Assert.Equal(0, baseline.Changes);
            var settings = (long)(await ScalarAsync(capture, "SELECT count(*) FROM pg_settings", ct))!;
            Assert.Equal(settings, baseline.Stored);
            Assert.Equal(settings, (long)(await ScalarAsync(capture, "SELECT count(*) FROM collect.store_setting_state", ct))!);

            /* Many settings have a NULL source file and line: a comparison that is not NULL-safe would read each of them as a change on every capture. */
            var quiet = await StoreSettingChanges.CaptureAsync(capture, ct);
            Assert.False(quiet.IsBaseline);
            Assert.Equal(0, quiet.Changes);
            Assert.Equal(0, quiet.Stored);
            Assert.Equal(0, quiet.Dropped);
            Assert.Equal(0L, (long)(await ScalarAsync(capture, "SELECT count(*) FROM collect.store_setting_changes", ct))!);

            var empty = await ReadAsync(source, null, ct);
            Assert.Equal("empty", empty.GetProperty("status").GetString());
            Assert.Equal(settings, empty.GetProperty("tracked_settings").GetInt64());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AnAlterSystemAndReload_IsOneChangeRow_WithOldAndNewValue_TheSourceFile_AndTheReloadTime_AndAResetIsTheSecond()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source, capture) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        await using var ___ = capture;
        var ok = false;
        try
        {
            await StoreSettingChanges.CaptureAsync(capture, ct);
            var original = (string)(await ScalarAsync(capture, "SELECT setting FROM pg_settings WHERE name = 'work_mem'", ct))!;
            /* A row old enough to be past the retention, so the pass that writes the change also prunes it. */
            await ExecAsync(source,
                "INSERT INTO collect.store_setting_changes (seen_at, setting_name) VALUES (timezone('UTC', now()) - interval '401 days', 'planted_old_row')", ct);

            await ExecAsync(source, "ALTER SYSTEM SET work_mem = '77MB'", ct);
            try
            {
                await ReloadAsync(source, capture, ct);
                var changed = await StoreSettingChanges.CaptureAsync(capture, ct);
                Assert.Equal(1, changed.Changes);
                Assert.False(changed.IsBaseline);

                var read = await ReadAsync(source, null, ct);
                var rows = read.GetProperty("changes").EnumerateArray().ToArray();
                var row = Assert.Single(rows);
                Assert.Equal("work_mem", row.GetProperty("setting_name").GetString());
                Assert.Equal(original, row.GetProperty("old_value").GetString());
                Assert.Equal((77 * 1024).ToString(CultureInfo.InvariantCulture), row.GetProperty("new_value").GetString());
                Assert.Equal("kB", row.GetProperty("unit").GetString());
                Assert.Equal("configuration file", row.GetProperty("new_source").GetString());
                Assert.EndsWith("postgresql.auto.conf", row.GetProperty("new_sourcefile").GetString(), StringComparison.Ordinal);
                Assert.True(row.GetProperty("new_sourceline").GetInt32() > 0);
                Assert.True(DateTime.Parse(row.GetProperty("seen_at").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal) > DateTime.UtcNow.AddMinutes(-5));
                Assert.True((bool)(await ScalarAsync(capture,
                    "SELECT conf_load_time = timezone('UTC', pg_conf_load_time()) FROM collect.store_setting_changes WHERE setting_name = 'work_mem'", ct))!);
                Assert.Equal(0L, (long)(await ScalarAsync(capture, "SELECT count(*) FROM collect.store_setting_changes WHERE setting_name = 'planted_old_row'", ct))!);

                /* The name filter is in the query: another setting's name reads as no change, and the name matches in any case. */
                Assert.Equal("empty", (await ReadAsync(source, "maintenance_work_mem", ct)).GetProperty("status").GetString());
                Assert.Single((await ReadAsync(source, "WORK_MEM", ct)).GetProperty("changes").EnumerateArray().ToArray());
            }
            finally
            {
                await ResetAsync(source, capture, "work_mem", CancellationToken.None);
            }

            var reset = await StoreSettingChanges.CaptureAsync(capture, ct);
            Assert.Equal(1, reset.Changes);
            var both = (await ReadAsync(source, "work_mem", ct)).GetProperty("changes").EnumerateArray().ToArray();
            Assert.Equal(2, both.Length);
            Assert.Equal((77 * 1024).ToString(CultureInfo.InvariantCulture), both[0].GetProperty("old_value").GetString());
            Assert.Equal(original, both[0].GetProperty("new_value").GetString());
            Assert.Equal(original, both[1].GetProperty("old_value").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ASourceOnlyChange_IsARow_AndTheLimitIsAPageOfTheNewestRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source, capture) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        await using var ___ = capture;
        var ok = false;
        try
        {
            await StoreSettingChanges.CaptureAsync(capture, ct);

            /* The first reload a store sees after it starts can flip pending_restart on a setting the server's command line fixes whose file value
               differs (the TimescaleDB image tunes max_worker_processes and others into postgresql.conf, and the Linux test container and the compose
               store pass their own on the command line). That is a real row, and it arrives once per start, so a store that has not reloaded yet
               would put it on this test's first pass. A reload and a capture first settle it, and the reads below name lock_timeout. */
            await ReloadAsync(source, capture, ct);
            await StoreSettingChanges.CaptureAsync(capture, ct);
            var original = (string)(await ScalarAsync(capture, "SELECT setting FROM pg_settings WHERE name = 'lock_timeout'", ct))!;
            var originalSource = (string)(await ScalarAsync(capture, "SELECT source FROM pg_settings WHERE name = 'lock_timeout'", ct))!;

            /* A setting still at its built-in default, so writing it to the file moves its source from "default" to "configuration file". The
               TimescaleDB image tunes work_mem into postgresql.conf, where the source is already "configuration file" (only the file differs),
               so work_mem has no source to move on that store. */
            Assert.SkipWhen(originalSource != "default", $"lock_timeout comes from '{originalSource}' on this store, so writing it to the file moves no source.");

            /* The same value as the one in force, written to the file: the value does not move, only where it comes from. */
            await ExecAsync(source, $"ALTER SYSTEM SET lock_timeout = {original}", ct);
            try
            {
                await ReloadAsync(source, capture, ct);
                Assert.Equal(1, (await StoreSettingChanges.CaptureAsync(capture, ct)).Changes);
                var row = Assert.Single((await ReadAsync(source, "lock_timeout", ct)).GetProperty("changes").EnumerateArray().ToArray());
                Assert.Equal(original, row.GetProperty("old_value").GetString());
                Assert.Equal(original, row.GetProperty("new_value").GetString());
                Assert.Equal(originalSource, row.GetProperty("old_source").GetString());
                Assert.Equal("configuration file", row.GetProperty("new_source").GetString());
            }
            finally
            {
                await ResetAsync(source, capture, "lock_timeout", CancellationToken.None);
            }

            Assert.Equal(1, (await StoreSettingChanges.CaptureAsync(capture, ct)).Changes);
            var page = await ReadAsync(source, "lock_timeout", ct, limit: 1);
            Assert.True(page.GetProperty("truncated").GetBoolean());
            var newest = Assert.Single(page.GetProperty("changes").EnumerateArray().ToArray());
            Assert.Equal("configuration file", newest.GetProperty("old_source").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ASettingThatNeedsARestart_ShowsPendingRestartInItsRow()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source, capture) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        await using var ___ = capture;
        var ok = false;
        try
        {
            await StoreSettingChanges.CaptureAsync(capture, ct);
            var megabytes = (long)(await ScalarAsync(capture, "SELECT (setting::bigint * 8) / 1024 FROM pg_settings WHERE name = 'shared_buffers'", ct))!;

            await ExecAsync(source, $"ALTER SYSTEM SET shared_buffers = '{(megabytes + 16).ToString(CultureInfo.InvariantCulture)}MB'", ct);
            try
            {
                await ReloadAsync(source, capture, ct);
                Assert.Equal(1, (await StoreSettingChanges.CaptureAsync(capture, ct)).Changes);
                var row = Assert.Single((await ReadAsync(source, "shared_buffers", ct)).GetProperty("changes").EnumerateArray().ToArray());
                Assert.False(row.GetProperty("old_pending_restart").GetBoolean());
                Assert.True(row.GetProperty("new_pending_restart").GetBoolean());
            }
            finally
            {
                await ResetAsync(source, capture, "shared_buffers", CancellationToken.None);
            }

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Theory]
    [InlineData(0, 50)]
    [InlineData(StoreSettingChanges.RetentionDays + 1, 50)]
    [InlineData(7, 0)]
    [InlineData(7, 1001)]
    public async Task AnOutOfRangeWindowOrLimit_IsRefused_NotClamped(int daysBack, int limit)
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source, capture) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        await using var ___ = capture;
        var ok = false;
        try
        {
            var json = await DarlingMcpStoreSettingChangesTools.GetStoreSettingChanges(source, daysBack, null, limit, ct);
            using var doc = JsonDocument.Parse(json);
            Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
            Assert.False(doc.RootElement.TryGetProperty("changes", out var changesElement), changesElement.ToString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }
}
