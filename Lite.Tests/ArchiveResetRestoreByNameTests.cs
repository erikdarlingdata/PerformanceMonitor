using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4887 review round 1 (M1, m1, NIT): the reset's restore is by column NAME (an old store has the
/// ALTER-added watermark_time last), the cooldown seeds read through the archive, and the user-choice
/// tables analysis_muted / server_tags / server_tag_map survive. All run the REAL reset.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveResetRestoreByNameTests : IDisposable
{
    private static readonly DateTime T2 = new(2026, 5, 1, 10, 5, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T3 = new(2026, 5, 1, 10, 10, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Sent = new(2026, 5, 1, 9, 30, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;

    public ArchiveResetRestoreByNameTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        CollectionResetGate.ResetForTests();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort */ }
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private async Task SeedAsync(params string[] statements)
    {
        await _duckDb.InitializeAsync();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private Task ResetAsync() => new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();

    private async Task<long> CountAsync(string sql)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task PreV31ColumnOrder_WatermarksRestoredByName_ValuesUnchanged()
    {
        /* Exactly the v31 shape: watermark_time ALTER-added, so it is the LAST column. */
        await SeedAsync(
            "DROP TABLE config_edge_trigger_watermarks",
            @"CREATE TABLE config_edge_trigger_watermarks (server_id INTEGER NOT NULL, metric_name VARCHAR NOT NULL,
                watermark INTEGER NOT NULL, updated_at TIMESTAMP NOT NULL, PRIMARY KEY (server_id, metric_name))",
            "ALTER TABLE config_edge_trigger_watermarks ADD COLUMN IF NOT EXISTS watermark_time TIMESTAMP",
            $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Deadlocks', 3, NULL, {Ts(T3)})",
            $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Failed Agent Job', 0, {Ts(T2)}, {Ts(T3)})");
        await ResetAsync();

        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM config_edge_trigger_watermarks WHERE server_id = 1 AND metric_name = 'Deadlocks' AND watermark = 3 AND watermark_time IS NULL AND updated_at = {Ts(T3)}"));
        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM config_edge_trigger_watermarks WHERE server_id = 1 AND metric_name = 'Failed Agent Job' AND watermark = 0 AND watermark_time = {Ts(T2)} AND updated_at = {Ts(T3)}"));
    }

    [Fact]
    public async Task CooldownSeeds_AfterRealReset_ReturnThePreResetValue()
    {
        string Row(string metric, string type, string sent, string err) =>
            $"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error) VALUES ({Ts(Sent)}, 1, 'S', '{metric}', 1, 1, {sent}, '{type}', {err})";
        await SeedAsync(
            Row("Email Metric", "email", "true", "NULL"),
            Row("Webhook Metric", "webhook", "true", "NULL"),
            Row("Analysis Metric", "email", "true", "NULL"));
        await ResetAsync();

        var store = new DuckDbAlertHistoryStore(_duckDb);
        Assert.Equal(Sent, await store.GetLastEmailSentUtcAsync("1", "Email Metric"));
        Assert.Equal(Sent, await store.GetLastWebhookSentUtcAsync("1", "Webhook Metric"));
        Assert.Equal(Sent, await store.GetLastAlertTimeAsync("1", "Analysis Metric"));
        Assert.Equal(Sent, await store.GetLastDeliveredPageUtcAsync("1", "Analysis Metric"));
    }

    [Fact]
    public async Task UserChoiceTables_SurviveRealReset_Unchanged()
    {
        await SeedAsync(
            $"INSERT INTO analysis_muted (mute_id, server_id, database_name, story_path_hash, story_path, muted_date, reason) VALUES (7, 1, 'DbA', 'abc123', 'a > b', {Ts(T2)}, 'noise')",
            $"INSERT INTO server_tags (id, name, parent_id, sort_order, colour, created_at) VALUES (5, 'Prod', NULL, 2, '#ff0000', {Ts(T3)})",
            "INSERT INTO server_tag_map (server_id, tag_id) VALUES (1, 5)");
        await ResetAsync();

        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM analysis_muted WHERE mute_id = 7 AND server_id = 1 AND database_name = 'DbA' AND story_path_hash = 'abc123' AND story_path = 'a > b' AND muted_date = {Ts(T2)} AND reason = 'noise'"));
        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM server_tags WHERE id = 5 AND name = 'Prod' AND parent_id IS NULL AND sort_order = 2 AND colour = '#ff0000' AND created_at = {Ts(T3)}"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM server_tag_map WHERE server_id = 1 AND tag_id = 5"));
    }
}
