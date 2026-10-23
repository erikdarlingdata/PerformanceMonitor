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
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6334: known-pattern entries written through the store's functions are read back by <c>get_known_patterns</c> and carried on the
/// <c>get_alert_history</c> rows they match, and only on those. Also proves the writes move the <c>config_version</c> reload beacon,
/// the note limit and the time zone check refuse a write, and update and delete report whether the entry existed.
/// </summary>
public sealed class KnownPatternLiveTests
{
    private const int ServerId = 7_633_401;
    private const int OtherServerId = 7_633_402;
    private const string ServerName = "LcKnownSrv";
    private const string Metric = "Blocking";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task InsertAlertAsync(NpgsqlConnection connection, DateTime atUtc, string metric, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 45, 30, TRUE, 'email', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(atUtc), ServerId, ServerName, metric);

    private static async Task<long> BeaconAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT config_version FROM config.config_service WHERE id = 1", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task EntriesWrittenThroughTheStore_AreReadBackAndMatchOnlyTheirAlerts_AndMoveTheBeacon()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the known pattern live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await DarlingMcpTestData.ExecAsync(connection, ct, DarlingManagedRoles.BuildKnownPatternFunctionsSql("config"));
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, OtherServerId, "other-server", ct);
        /* The write functions refuse a server id the registry (config_monitored_servers) does not hold. */
        await DarlingMcpTestData.ExecAsync(connection, ct, $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerId}, 'kp-a', 'kp-a', TRUE), ({OtherServerId}, 'kp-b', 'kp-b', TRUE) ON CONFLICT DO NOTHING");

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new PgKnownPatternStore(dataSource);

        // A nightly Blocking entry for this server in UTC; a 02:00 New York entry for any metric; a fleet-wide note with no time; an
        // entry for another server; one for another metric; one that expired before the alerts fired.
        var beforeInsert = await BeaconAsync(connection, ct);
        var nightly = await store.InsertAsync(new KnownPatternInput(ServerId, Metric, new TimeOnly(2, 10), null, null, "nightly index job", null), "erik", ct);
        var afterInsert = await BeaconAsync(connection, ct);
        Assert.True(afterInsert > beforeInsert, "an entry written through the store function must bump config_version, or a cached copy stays stale");

        var newYork = await store.InsertAsync(new KnownPatternInput(null, null, new TimeOnly(2, 0), "America/New_York", 10, "new york batch", null), "erik", ct);
        var anyTime = await store.InsertAsync(new KnownPatternInput(null, "Blocking", null, null, null, "blocking is known on this fleet", null), null, ct);
        var otherServer = await store.InsertAsync(new KnownPatternInput(OtherServerId, Metric, new TimeOnly(2, 10), null, null, "another server", null), "erik", ct);
        var otherMetric = await store.InsertAsync(new KnownPatternInput(ServerId, "High CPU", new TimeOnly(2, 10), null, null, "another metric", null), "erik", ct);
        var expired = await store.InsertAsync(new KnownPatternInput(ServerId, Metric, new TimeOnly(2, 10), null, null, "lapsed",
            DateTime.UtcNow.Date.AddDays(-1).AddHours(1)), "erik", ct);

        // Alerts: 02:20 UTC yesterday (inside the nightly window); 14:10 UTC yesterday (outside it); and one at 02:05 New York time.
        var yesterday = DateTime.UtcNow.Date.AddDays(-1);
        var matching = yesterday.AddHours(2).AddMinutes(20);
        var outside = yesterday.AddHours(14).AddMinutes(10);
        var zone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        var localNight = DateTime.SpecifyKind(yesterday.AddDays(-1).AddHours(2).AddMinutes(5), DateTimeKind.Unspecified);
        var newYorkNight = TimeZoneInfo.ConvertTimeToUtc(localNight, zone);
        await InsertAlertAsync(connection, matching, Metric, ct);
        await InsertAlertAsync(connection, outside, Metric, ct);
        await InsertAlertAsync(connection, newYorkNight, "Deadlocks Detected", ct);

        var answer = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertHistory(dataSource, ServerName, 240, 50, cancellationToken: ct)).RootElement;
        var rows = answer.GetProperty("alerts").EnumerateArray().ToList();
        long[] Known(DateTime at, string metric) => rows.Single(a =>
                a.GetProperty("metric_name").GetString() == metric
                && DateTime.Parse(a.GetProperty("alert_time").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind) == at)
            .GetProperty("known_patterns").EnumerateArray().Select(k => k.GetProperty("id").GetInt64()).OrderBy(i => i).ToArray();

        Assert.Equal(new[] { nightly, anyTime }.OrderBy(i => i).ToArray(), Known(matching, Metric));
        Assert.Equal(new[] { anyTime }, Known(outside, Metric));
        Assert.Equal(new[] { newYork }, Known(newYorkNight, "Deadlocks Detected"));

        var matchRow = rows.First(a => a.GetProperty("known_patterns").EnumerateArray().Any(k => k.GetProperty("id").GetInt64() == nightly));
        var nightlyWire = matchRow.GetProperty("known_patterns").EnumerateArray().Single(k => k.GetProperty("id").GetInt64() == nightly);
        Assert.Equal("nightly index job", nightlyWire.GetProperty("note").GetString());
        Assert.Equal("erik", nightlyWire.GetProperty("created_by").GetString());

        var listed = JsonDocument.Parse(await DarlingMcpKnownPatternTools.GetKnownPatterns(dataSource, cancellationToken: ct)).RootElement;
        var listedIds = listed.GetProperty("known_patterns").EnumerateArray().Select(k => k.GetProperty("id").GetInt64()).ToHashSet();
        Assert.Equal(new[] { nightly, newYork, anyTime, otherServer, otherMetric }.ToHashSet(), listedIds); // the expired one is left out by default

        // Security review of #6334: at metadataOnly the operator's note is withheld in both reads the tools make (the host filter masks
        // created_by by key name, so this checks the note, which only the tool can mask).
        var metadataOnly = new McpDataDetailSetting(McpDataDetail.MetadataOnly);
        var withheldList = await DarlingMcpKnownPatternTools.GetKnownPatterns(dataSource, maskSetting: metadataOnly, cancellationToken: ct);
        Assert.DoesNotContain("nightly index job", withheldList, StringComparison.Ordinal);
        Assert.Contains(McpDataDetailMasker.WithheldText, withheldList, StringComparison.Ordinal);
        var withheldAlerts = await DarlingMcpAlertTools.GetAlertHistory(dataSource, ServerName, 240, 50, maskSetting: metadataOnly, cancellationToken: ct);
        Assert.DoesNotContain("nightly index job", withheldAlerts, StringComparison.Ordinal);
        Assert.DoesNotContain("blocking is known on this fleet", withheldAlerts, StringComparison.Ordinal);

        var forServer = JsonDocument.Parse(await DarlingMcpKnownPatternTools.GetKnownPatterns(dataSource, ServerName, include_expired: true, cancellationToken: ct)).RootElement;
        var forServerIds = forServer.GetProperty("known_patterns").EnumerateArray().Select(k => k.GetProperty("id").GetInt64()).ToHashSet();
        Assert.Equal(new[] { nightly, newYork, anyTime, otherMetric, expired }.ToHashSet(), forServerIds);

        // Refused writes and the update/delete results.
        var longNote = new string('x', KnownPatternMatcher.MaxNoteLength + 1);
        await Assert.ThrowsAsync<ArgumentException>(() => store.InsertAsync(new KnownPatternInput(null, null, null, null, null, longNote, null), "erik", ct));
        await Assert.ThrowsAsync<ArgumentException>(() => store.InsertAsync(new KnownPatternInput(null, null, new TimeOnly(1, 0), "Not/AZone", null, "bad zone", null), "erik", ct));
        Assert.True(await store.UpdateAsync(nightly, new KnownPatternInput(ServerId, Metric, new TimeOnly(2, 10), null, 5, "now narrower", null), "priya", ct));
        Assert.False(await store.UpdateAsync(long.MaxValue, new KnownPatternInput(null, null, null, null, null, "none", null), "priya", ct));
        var updated = (await store.LoadAllAsync(ct)).Single(e => e.Id == nightly);
        Assert.Equal("now narrower", updated.Note);
        Assert.Equal(5, updated.WindowMinutes);
        Assert.Equal("erik", updated.CreatedBy);
        Assert.Equal("priya", updated.UpdatedBy);
        Assert.NotNull(updated.UpdatedAtUtc);

        var beforeDelete = await BeaconAsync(connection, ct);
        Assert.True(await store.DeleteAsync(nightly, ct));
        Assert.False(await store.DeleteAsync(nightly, ct));
        Assert.True(await BeaconAsync(connection, ct) > beforeDelete, "a delete must bump config_version");
        Assert.DoesNotContain(await store.LoadAllAsync(ct), e => e.Id == nightly);

        // The column limits hold for a write that skips the function.
        var ex = await Assert.ThrowsAsync<PostgresException>(() => DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO config.config_known_patterns (note) VALUES ($1)", longNote));
        Assert.Equal(PostgresErrorCodes.CheckViolation, ex.SqlState);
    }
}
