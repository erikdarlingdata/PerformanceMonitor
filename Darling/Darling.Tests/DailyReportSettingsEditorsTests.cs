/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Daily Health Report settings editors (#7041): the Viewer's rules and its upsert round trip, and the web route's write, against a
/// real PostgreSQL store. Each live fact mints its own scratch database.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each fact mints its own scratch database through
   ScratchPostgres and never touches the shared store's tables. */
public sealed class DailyReportSettingsEditorsTests
{
    private const string ColumnsSql = "SELECT daily_report_enabled, daily_report_send_at::text, daily_report_time_zone, daily_report_title FROM config_alert_settings WHERE id = 1";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Theory]
    [InlineData("07:00", true)]
    [InlineData("7:05", true)]
    [InlineData("23:59", true)]
    [InlineData("24:00", false)]
    [InlineData("7", false)]
    [InlineData("", false)]
    [InlineData("noon", false)]
    public void SendAt_AcceptsATimeOfDay_AndRefusesAnythingElse(string text, bool valid)
    {
        Assert.Equal(valid, ViewerDailyReportRules.TryParseSendAt(text, out _));
    }

    [Fact]
    public void ZoneAndTitle_FollowTheServiceRules()
    {
        Assert.True(ViewerDailyReportRules.IsKnownZone(null));
        Assert.True(ViewerDailyReportRules.IsKnownZone("  "));
        Assert.True(ViewerDailyReportRules.IsKnownZone("UTC"));
        Assert.False(ViewerDailyReportRules.IsKnownZone("Mars/Olympus_Mons"));

        Assert.True(ViewerDailyReportRules.TryValidateTitle("Nightly fleet report", out _));
        Assert.False(ViewerDailyReportRules.TryValidateTitle(new string('x', ViewerDailyReportRules.MaxTitleLength + 1), out _));
        Assert.False(ViewerDailyReportRules.TryValidateTitle("two\nlines", out _));
        Assert.Equal(DBNull.Value, ViewerDailyReportRules.TextOrNull("   "));
        Assert.Equal("America/New_York", ViewerDailyReportRules.TextOrNull(" America/New_York "));
    }

    [Fact]
    public async Task TheViewerUpsert_WritesAndReadsTheFourSettings_AndABlankZoneAndTitleAreStoredAsNull()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Daily Health Report editor test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var row = await viewer.GetAlertSettingsAsync(ct);
        Assert.NotNull(row);
        Assert.False(row!.DailyReportEnabled);
        Assert.Equal("07:00", row.DailyReportSendAt);
        Assert.Null(row.DailyReportTimeZone);
        Assert.Null(row.DailyReportTitle);

        row.DailyReportEnabled = true;
        row.DailyReportSendAt = "06:30";
        row.DailyReportTimeZone = "America/New_York";
        row.DailyReportTitle = "Morning fleet report";
        await viewer.UpsertAlertSettingsAsync(row, ct);

        var saved = await viewer.GetAlertSettingsAsync(ct);
        Assert.True(saved!.DailyReportEnabled);
        Assert.Equal("06:30", saved.DailyReportSendAt);
        Assert.Equal("America/New_York", saved.DailyReportTimeZone);
        Assert.Equal("Morning fleet report", saved.DailyReportTitle);
        Assert.Equal((true, "06:30:00", "America/New_York", "Morning fleet report"), await ReadColumnsAsync(connection, ct));

        saved.DailyReportTimeZone = "  ";
        saved.DailyReportTitle = null;
        await viewer.UpsertAlertSettingsAsync(saved, ct);
        Assert.Equal((true, "06:30:00", (string?)null, (string?)null), await ReadColumnsAsync(connection, ct));
    }

    [Fact]
    public async Task TheWebRoute_SavesTheGroup_RefusesABadValueWithTheServicesSentence_AndWritesNothingOnARefusal()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Daily Health Report editor test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (status, body) = await DarlingWebAlertSettings.SaveDailyReportAsync(postgres,
            Body("{\"enabled\":true,\"send_at\":\"08:15\",\"time_zone\":\"UTC\",\"title\":\"Fleet health\"}"));
        Assert.Equal(200, status);
        Assert.Equal("updated", body["status"]!.GetValue<string>());
        Assert.Equal((true, "08:15:00", "UTC", "Fleet health"), await ReadColumnsAsync(connection, ct));

        /* A null zone and title clear the columns (the host's zone, the default title). */
        (status, _) = await DarlingWebAlertSettings.SaveDailyReportAsync(postgres, Body("{\"time_zone\":null,\"title\":null}"));
        Assert.Equal(200, status);
        Assert.Equal((true, "08:15:00", (string?)null, (string?)null), await ReadColumnsAsync(connection, ct));

        foreach (var (json, field) in new[]
        {
            ("{\"send_at\":\"25:99\"}", "daily_report.send_at"),
            ("{\"time_zone\":\"Mars/Olympus_Mons\"}", "daily_report.time_zone"),
            ("{\"title\":\"\"}", "daily_report.title"),
        })
        {
            (status, body) = await DarlingWebAlertSettings.SaveDailyReportAsync(postgres, Body(json));
            Assert.Equal(400, status);
            Assert.Contains(field, body["error"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal((true, "08:15:00", (string?)null, (string?)null), await ReadColumnsAsync(connection, ct));
        }

        (status, body) = await DarlingWebAlertSettings.SaveDailyReportAsync(postgres, Body("{\"alerts_enabled\":false}"));
        Assert.Equal(400, status);
        Assert.StartsWith("Unknown field", body["error"]!.GetValue<string>(), StringComparison.Ordinal);
        (status, _) = await DarlingWebAlertSettings.SaveDailyReportAsync(postgres, Body("{}"));
        Assert.Equal(400, status);
    }

    private static JsonObject Body(string json) => JsonNode.Parse(json)!.AsObject();

    private static async Task<(bool Enabled, string SendAt, string? Zone, string? Title)> ReadColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(ColumnsSql, connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "config_alert_settings has no singleton row");
        return (reader.GetBoolean(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    private static async Task SeedAsync(string connectionString, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(new DarlingConfig(), ct);
    }
}
