using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// E2 round-trip invariants for the Lite DuckDB stores: a recorded alert
/// produces a config_alert_log row with the same columns/values as the pre-E2
/// EmailAlertService.LogAlertAsync did (numeric resolution, notification_type,
/// send_error, UtcNow stamping), the cooldown-seed reads filter correctly, and
/// mute-rule CRUD round-trips through config_mute_rules unchanged.
/// </summary>
public class StoreRoundTripTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public StoreRoundTripTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    [Fact]
    public async Task RecordAlert_PersistsAllColumns_WithTextNumericFallback()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);
        var before = DateTime.UtcNow;

        /* No numerics supplied → store resolves the doubles from the display
           text through AlertValueParser's leading-numeric extraction (#1830). */
        await store.RecordAlertAsync(new AlertHistoryRecord(
            ServerId: "7", ServerName: "Srv", MetricName: "CPU",
            CurrentValueText: "92.5%", ThresholdValueText: "80%",
            NumericCurrentValue: null, NumericThresholdValue: null,
            AlertSent: true, NotificationType: "email", SendError: null,
            Muted: false, DetailText: "detail text", ContextJson: "{\"k\":1}"));

        var after = DateTime.UtcNow;
        var row = await ReadSingleAlertRowAsync();

        Assert.NotNull(row);
        Assert.InRange(row!.AlertTime, before.AddSeconds(-1), after.AddSeconds(1));
        Assert.Equal(7, row.ServerId);
        Assert.Equal("Srv", row.ServerName);
        Assert.Equal("CPU", row.MetricName);
        Assert.Equal(92.5, row.CurrentValue);
        Assert.Equal(80.0, row.ThresholdValue);
        Assert.True(row.AlertSent);
        Assert.Equal("email", row.NotificationType);
        Assert.Null(row.SendError);
        Assert.False(row.Muted);
        Assert.Equal("detail text", row.DetailText);
        Assert.Equal("{\"k\":1}", row.ContextJson);
    }

    [Fact]
    public async Task RecordAlert_DecoratedText_WithoutNumerics_StoresTheLeadingNumber_NotZero()
    {
        /* #1830 regression pin, at the exact seam that shipped broken: a High CPU record whose
           text carries a parenthesized label and NO numerics. The old TrimEnd('%') fallback
           failed on the ')' and stored 0 for every such row in the field. */
        var store = new DuckDbAlertHistoryStore(_duckDb);

        await store.RecordAlertAsync(new AlertHistoryRecord(
            ServerId: "7", ServerName: "Srv", MetricName: "High CPU",
            CurrentValueText: "87% (Total CPU)", ThresholdValueText: "80%",
            NumericCurrentValue: null, NumericThresholdValue: null,
            AlertSent: true, NotificationType: "toast", SendError: null,
            Muted: false, DetailText: null, ContextJson: null));

        var row = await ReadSingleAlertRowAsync();

        Assert.NotNull(row);
        Assert.Equal(87.0, row!.CurrentValue);
        Assert.Equal(80.0, row.ThresholdValue);
    }

    [Fact]
    public async Task RecordAlert_PrefersSuppliedNumerics_OverText()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);

        await store.RecordAlertAsync(new AlertHistoryRecord(
            ServerId: "1", ServerName: "Srv", MetricName: "Analysis",
            CurrentValueText: "not-a-number", ThresholdValueText: "also-bad",
            NumericCurrentValue: 1.8, NumericThresholdValue: 1.5,
            AlertSent: false, NotificationType: "tray", SendError: null,
            Muted: false, DetailText: null, ContextJson: null));

        var row = await ReadSingleAlertRowAsync();
        Assert.NotNull(row);
        Assert.Equal(1.8, row!.CurrentValue);
        Assert.Equal(1.5, row.ThresholdValue);
        Assert.Null(row.DetailText);
        Assert.Null(row.ContextJson);
    }

    [Fact]
    public async Task GetLastEmailSentUtc_FiltersToSuccessfulEmail_GetLastAlertTime_IsUnfiltered()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);

        /* A failed email and a webhook row must NOT seed the email cooldown,
           but they DO count for the unfiltered analysis cooldown. */
        await RecordAsync(store, "3", "CPU", "email", "smtp boom");   // failed email
        await RecordAsync(store, "3", "CPU", "webhook", null);         // webhook only
        await RecordAsync(store, "3", "CPU", "email", null);           // successful email (latest)

        var lastEmail = await store.GetLastEmailSentUtcAsync("3", "CPU");
        var lastAny = await store.GetLastAlertTimeAsync("3", "CPU");

        Assert.NotNull(lastEmail);
        Assert.NotNull(lastAny);
        /* The successful email is the last row written, so both reads land on it. */
        Assert.Equal(lastAny!.Value, lastEmail!.Value);

        /* A metric with only a failed email → no successful-email seed, but the
           unfiltered read still returns the row. */
        await RecordAsync(store, "3", "OnlyFailed", "email", "boom");
        Assert.Null(await store.GetLastEmailSentUtcAsync("3", "OnlyFailed"));
        Assert.NotNull(await store.GetLastAlertTimeAsync("3", "OnlyFailed"));

        /* Unknown metric → null both ways. */
        Assert.Null(await store.GetLastEmailSentUtcAsync("3", "Missing"));
        Assert.Null(await store.GetLastAlertTimeAsync("3", "Missing"));
    }

    [Fact]
    public async Task GetLastWebhookSentUtc_FiltersToWebhookRows_IncludingEmailWebhook()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);

        /* #1145: only rows whose notification_type implies a webhook delivered seed the webhook
           cooldown. email-only / tray rows must NOT; an 'email+webhook' row counts even though its
           send_error is the EMAIL failure (the webhook still delivered), because send_error tracks
           the email channel, not the webhook. */
        await RecordAsync(store, "5", "Blocking Detected", "email", null);          // email only
        await RecordAsync(store, "5", "Blocking Detected", "tray", null);           // tray only
        await RecordAsync(store, "5", "Blocking Detected", "webhook", null);        // webhook
        await RecordAsync(store, "5", "Blocking Detected", "email+webhook", "smtp boom"); // webhook sent, email failed (latest)

        var lastWebhook = await store.GetLastWebhookSentUtcAsync("5", "Blocking Detected");
        var lastAny = await store.GetLastAlertTimeAsync("5", "Blocking Detected");

        Assert.NotNull(lastWebhook);
        /* The email+webhook row is the last written, so it is both the unfiltered max and the
           webhook-filtered max. */
        Assert.Equal(lastAny!.Value, lastWebhook!.Value);

        /* A metric with only email/tray rows → no webhook seed. */
        await RecordAsync(store, "5", "EmailOnly", "email", null);
        await RecordAsync(store, "5", "EmailOnly", "tray", null);
        Assert.Null(await store.GetLastWebhookSentUtcAsync("5", "EmailOnly"));

        /* Unknown metric → null. */
        Assert.Null(await store.GetLastWebhookSentUtcAsync("5", "Missing"));
    }

    [Fact]
    public async Task GetLastSentUtc_WithDedupKey_FiltersToContextJsonFingerprint()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);

        /* #1154: two distinct deadlock incidents recorded as successful email rows (AAA earlier,
           BBB later) carrying real serialized #1140 context, plus a later null-context row that
           any dedupKey filter must exclude (it would NRE/over-match a naive scan). */
        await RecordWithContextAsync(store, "9", "Deadlocks Detected", "email", JsonWith("aaaa1111"));
        await Task.Delay(10, TestContext.Current.CancellationToken);
        await RecordWithContextAsync(store, "9", "Deadlocks Detected", "email", JsonWith("bbbb2222"));
        await Task.Delay(10, TestContext.Current.CancellationToken);
        await RecordAsync(store, "9", "Deadlocks Detected", "email", null); // null context, latest row

        var lastAaa = await store.GetLastEmailSentUtcAsync("9", "Deadlocks Detected", "aaaa1111");
        var lastBbb = await store.GetLastEmailSentUtcAsync("9", "Deadlocks Detected", "bbbb2222");
        var lastCcc = await store.GetLastEmailSentUtcAsync("9", "Deadlocks Detected", "cccc3333");
        var lastMetric = await store.GetLastEmailSentUtcAsync("9", "Deadlocks Detected"); // metric-level (null key)

        Assert.NotNull(lastAaa);
        Assert.NotNull(lastBbb);
        Assert.Null(lastCcc);                            // no such fingerprint
        Assert.True(lastAaa!.Value < lastBbb!.Value);    // the filter isolates per-fingerprint (AAA is earlier)
        Assert.NotNull(lastMetric);
        Assert.True(lastMetric!.Value >= lastBbb.Value); // metric-level still sees the later null-context row

        /* Webhook channel uses the identical filter — prove it too. */
        await RecordWithContextAsync(store, "9", "Blocking Detected", "webhook", JsonWith("dddd4444"));
        Assert.NotNull(await store.GetLastWebhookSentUtcAsync("9", "Blocking Detected", "dddd4444"));
        Assert.Null(await store.GetLastWebhookSentUtcAsync("9", "Blocking Detected", "eeee5555"));
    }

    /// <summary>
    /// #2716's Postgres Tier-0-predictor cooldown seed passes a RAW database/slot name as dedupKey —
    /// not a #1140 hash — so this filter has to be correct for arbitrary text, not just hex. Three
    /// failure modes, proven against the real store rather than the escaping logic in isolation:
    /// a quote/backslash breaks the naive string-concatenation match (the value Serialize() actually
    /// writes is JSON-escaped, the old hand-built search pattern was not); an underscore is a SQL LIKE
    /// wildcard, so a naive pattern for "orders_db" would also match "ordersXdb" for any X; and a
    /// non-ASCII character is escaped by the default JSON encoder into a \uXXXX sequence the old
    /// pattern never accounted for either.
    /// </summary>
    [Fact]
    public async Task GetLastAlertTime_WithDedupKey_HandlesRawNonHexSubjectsSafely()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);

        // Quote/backslash: a subject like a Windows-style path or a quoted identifier.
        await RecordWithContextAsync(store, "20", "Wraparound Risk", "tray", JsonWith("db\"with\\quote"));
        Assert.NotNull(await store.GetLastAlertTimeAsync("20", "Wraparound Risk", "db\"with\\quote"));

        // Underscore: must match only the exact subject, never a same-shaped different one.
        await RecordWithContextAsync(store, "21", "Wraparound Risk", "tray", JsonWith("orders_db"));
        Assert.NotNull(await store.GetLastAlertTimeAsync("21", "Wraparound Risk", "orders_db"));
        Assert.Null(await store.GetLastAlertTimeAsync("21", "Wraparound Risk", "ordersXdb"));

        // Non-ASCII: the default JSON encoder escapes this to a \uXXXX sequence before it is stored.
        await RecordWithContextAsync(store, "22", "Wraparound Risk", "tray", JsonWith("café"));
        Assert.NotNull(await store.GetLastAlertTimeAsync("22", "Wraparound Risk", "café"));

        // Percent: the other SQL LIKE wildcard, same shape as the underscore case.
        await RecordWithContextAsync(store, "23", "Wraparound Risk", "tray", JsonWith("100%done"));
        Assert.NotNull(await store.GetLastAlertTimeAsync("23", "Wraparound Risk", "100%done"));
        Assert.Null(await store.GetLastAlertTimeAsync("23", "Wraparound Risk", "100Xdone"));
    }

    [Fact]
    public async Task EdgeTriggerWatermark_SaveLoad_RoundTripsAndUpserts()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);

        /* #1145: empty table → empty load. */
        Assert.Empty(await store.LoadEdgeTriggerWatermarksAsync());

        await store.SaveEdgeTriggerWatermarkAsync(1, "Blocking Detected", 4);
        await store.SaveEdgeTriggerWatermarkAsync(1, "Deadlocks Detected", 2);
        await store.SaveEdgeTriggerWatermarkAsync(2, "Blocking Detected", 7);

        var loaded = await store.LoadEdgeTriggerWatermarksAsync();
        Assert.Equal(3, loaded.Count);
        Assert.Contains(loaded, r => r.ServerId == 1 && r.MetricName == "Blocking Detected" && r.Watermark == 4);
        Assert.Contains(loaded, r => r.ServerId == 1 && r.MetricName == "Deadlocks Detected" && r.Watermark == 2);
        Assert.Contains(loaded, r => r.ServerId == 2 && r.MetricName == "Blocking Detected" && r.Watermark == 7);

        /* Upsert on the (server_id, metric_name) primary key: same key overwrites, no dup row. */
        await store.SaveEdgeTriggerWatermarkAsync(1, "Blocking Detected", 9);
        loaded = await store.LoadEdgeTriggerWatermarksAsync();
        Assert.Equal(3, loaded.Count);
        Assert.Contains(loaded, r => r.ServerId == 1 && r.MetricName == "Blocking Detected" && r.Watermark == 9);

        /* A reset to 0 (the window drained) persists too — so a restart restores 0, not a stale count. */
        await store.SaveEdgeTriggerWatermarkAsync(1, "Blocking Detected", 0);
        loaded = await store.LoadEdgeTriggerWatermarksAsync();
        Assert.Equal(3, loaded.Count);
        Assert.Contains(loaded, r => r.ServerId == 1 && r.MetricName == "Blocking Detected" && r.Watermark == 0);
    }

    [Fact]
    public async Task FailedJobWatermark_SaveLoad_RoundTripsExactValue_AndUpserts()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);

        /* Empty table → empty load. */
        Assert.Empty(await store.LoadFailedJobWatermarksAsync());

        /* Server-local run times (Kind=Unspecified) must round-trip byte-for-byte: the watermark is
           compared directly against FailedJobInfo.RunDateTime, so any UTC coercion would corrupt it. */
        var server1 = new DateTime(2026, 6, 19, 14, 30, 15);
        var server2 = new DateTime(2026, 6, 19, 9, 5, 0);
        await store.SaveFailedJobWatermarkAsync(1, server1);
        await store.SaveFailedJobWatermarkAsync(2, server2);

        var loaded = await store.LoadFailedJobWatermarksAsync();
        Assert.Equal(2, loaded.Count);
        Assert.Equal(server1, loaded.Single(r => r.ServerId == 1).Watermark);
        Assert.Equal(server2, loaded.Single(r => r.ServerId == 2).Watermark);

        /* Upsert on the (server_id, metric_name) primary key: a newer failure overwrites, no dup row. */
        var server1Newer = new DateTime(2026, 6, 19, 16, 45, 0);
        await store.SaveFailedJobWatermarkAsync(1, server1Newer);
        loaded = await store.LoadFailedJobWatermarksAsync();
        Assert.Equal(2, loaded.Count);
        Assert.Equal(server1Newer, loaded.Single(r => r.ServerId == 1).Watermark);

        /* The time-based failed-job rows and the count-based blocking/deadlock rows share the table
           but never bleed into each other's load. */
        await store.SaveEdgeTriggerWatermarkAsync(1, "Blocking Detected", 5);
        Assert.Equal(2, (await store.LoadFailedJobWatermarksAsync()).Count);          // unchanged by the count row
        Assert.DoesNotContain(await store.LoadEdgeTriggerWatermarksAsync(),
            r => r.MetricName == "Failed Agent Job");                                 // failed-job row absent from count load
    }

    [Fact]
    public async Task MuteRuleStore_InsertUpdateSetEnabledDeleteExpire_RoundTrips()
    {
        var store = new DuckDbMuteRuleStore(_duckDb);

        var rule = new MuteRule
        {
            Id = "rule-1",
            Enabled = true,
            CreatedAtUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            ServerName = "Srv",
            MetricName = "CPU",
            Reason = "noisy"
        };

        await store.InsertAsync(rule);
        var loaded = await store.LoadAllAsync();
        Assert.Single(loaded);
        Assert.Equal("rule-1", loaded[0].Id);
        Assert.Equal("Srv", loaded[0].ServerName);
        Assert.Equal("CPU", loaded[0].MetricName);
        Assert.Equal("noisy", loaded[0].Reason);
        Assert.True(loaded[0].Enabled);

        rule.Reason = "still noisy";
        rule.MetricName = "Blocking";
        await store.UpdateAsync(rule);
        loaded = await store.LoadAllAsync();
        Assert.Single(loaded);
        Assert.Equal("still noisy", loaded[0].Reason);
        Assert.Equal("Blocking", loaded[0].MetricName);

        await store.SetEnabledAsync("rule-1", false);
        loaded = await store.LoadAllAsync();
        Assert.False(loaded[0].Enabled);

        await store.DeleteAsync("rule-1");
        Assert.Empty(await store.LoadAllAsync());

        /* DeleteExpired removes only the given ids. */
        var expired = new MuteRule
        {
            Id = "exp-1",
            Enabled = true,
            CreatedAtUtc = DateTime.UtcNow.AddDays(-2),
            ExpiresAtUtc = DateTime.UtcNow.AddDays(-1)
        };
        var live = new MuteRule { Id = "live-1", Enabled = true, CreatedAtUtc = DateTime.UtcNow };
        await store.InsertAsync(expired);
        await store.InsertAsync(live);
        await store.DeleteExpiredAsync(new List<string> { "exp-1" });
        loaded = await store.LoadAllAsync();
        Assert.Single(loaded);
        Assert.Equal("live-1", loaded[0].Id);
    }

    private static Task RecordAsync(IAlertHistoryStore store, string serverId, string metric, string type, string? error)
        => store.RecordAlertAsync(new AlertHistoryRecord(
            serverId, "Srv", metric, "90", "80", 90, 80,
            true, type, error, false, null, null));

    private static Task RecordWithContextAsync(IAlertHistoryStore store, string serverId, string metric, string type, string? contextJson)
        => store.RecordAlertAsync(new AlertHistoryRecord(
            serverId, "Srv", metric, "90", "80", 90, 80,
            true, type, null, false, null, contextJson));

    /// <summary>Real serialized #1140 context carrying a single incident with the given dedup key.</summary>
    private static string JsonWith(string dedupKey)
    {
        var ctx = new AlertContext
        {
            Incidents = new List<AlertIncident> { new(dedupKey, new[] { "db.dbo.T" }) }
        };
        return AlertContextSerializer.Serialize(ctx);
    }

    private sealed record AlertRow(
        DateTime AlertTime, int ServerId, string ServerName, string MetricName,
        double CurrentValue, double ThresholdValue, bool AlertSent,
        string NotificationType, string? SendError, bool Muted,
        string? DetailText, string? ContextJson);

    /// <summary>Reads the single config_alert_log row. DB I/O lives in a helper
    /// (not a [Fact] body) to match the existing test convention.</summary>
    private async Task<AlertRow?> ReadSingleAlertRowAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT alert_time, server_id, server_name, metric_name, current_value, threshold_value,
       alert_sent, notification_type, send_error, muted, detail_text, context_json
FROM config_alert_log";
        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new AlertRow(
            DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
            reader.GetInt32(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetDouble(4),
            reader.GetDouble(5),
            reader.GetBoolean(6),
            reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.GetBoolean(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : reader.GetString(11));
    }
}
