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
/// The editors for the three Database Protection overdue ages (#7611): the Viewer Settings window's rules and its save round trip, the
/// web Settings tab's boxes (the shipped admin.js run under Node), and the web route's write, against a real PostgreSQL store for the
/// live facts. Each live fact mints its own scratch database.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each fact mints its own scratch database through
   ScratchPostgres and never touches the shared store's tables. */
public sealed class DatabaseProtectionSettingsEditorsTests
{
    private const string ColumnsSql = "SELECT backup_overdue_threshold_hours, log_backup_overdue_threshold_minutes, checkdb_overdue_threshold_hours FROM config_alert_settings WHERE id = 1";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    // ---- the Viewer Settings window's rules

    [Theory]
    [InlineData(ViewerDatabaseProtectionRules.BackupHoursKey, "1", true)]
    [InlineData(ViewerDatabaseProtectionRules.BackupHoursKey, " 1440 ", true)]
    [InlineData(ViewerDatabaseProtectionRules.BackupHoursKey, "0", false)]
    [InlineData(ViewerDatabaseProtectionRules.BackupHoursKey, "1441", false)]
    [InlineData(ViewerDatabaseProtectionRules.LogBackupMinutesKey, "86400", true)]
    [InlineData(ViewerDatabaseProtectionRules.LogBackupMinutesKey, "86401", false)]
    [InlineData(ViewerDatabaseProtectionRules.LogBackupMinutesKey, "-5", false)]
    [InlineData(ViewerDatabaseProtectionRules.CheckDbHoursKey, "8760", true)]
    [InlineData(ViewerDatabaseProtectionRules.CheckDbHoursKey, "8761", false)]
    [InlineData(ViewerDatabaseProtectionRules.CheckDbHoursKey, "1.5", false)]
    [InlineData(ViewerDatabaseProtectionRules.CheckDbHoursKey, "", false)]
    [InlineData(ViewerDatabaseProtectionRules.CheckDbHoursKey, "two weeks", false)]
    public void TheWindowsBoxes_AcceptAWholeNumberInsideTheKeysBounds_AndRefuseAnythingElse(string wire, string text, bool valid)
    {
        Assert.Equal(valid, ViewerDatabaseProtectionRules.TryParse(text, wire, out var value));
        Assert.Equal(valid ? int.Parse(text.Trim(), System.Globalization.CultureInfo.InvariantCulture) : 0, value);
    }

    [Fact]
    public void TheRefusal_NamesTheUnitAndTheBounds()
    {
        Assert.Equal("Log backup overdue age must be a whole number of minutes between 1 and 86400.",
            ViewerDatabaseProtectionRules.Refusal("Log backup overdue age", ViewerDatabaseProtectionRules.LogBackupMinutesKey, "minutes"));
    }

    // ---- the web Settings tab

    private static JsonElement Tab(string param) => AdminServerEditBehaviourTests.Run("protectionAges:" + param);

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheWebTab_ShowsTheThreeAgesWithTheirUnits_AsRowsAndAsBoxesForAnEditor()
    {
        var page = Tab("editor");

        Assert.True(page.GetProperty("form").GetBoolean());
        Assert.Equal(["168", "60", "336"], Strings(page.GetProperty("before")));
        var labels = string.Join('\n', Strings(page.GetProperty("labels")));
        Assert.Contains("Full backup overdue when older than (hours, 1 to 1440):", labels, StringComparison.Ordinal);
        Assert.Contains("Log backup overdue when older than (minutes, 1 to 86400):", labels, StringComparison.Ordinal);
        Assert.Contains("CHECKDB overdue when older than (hours, 1 to 8760):", labels, StringComparison.Ordinal);
        var rows = string.Join('\n', Strings(page.GetProperty("rowTexts")));
        Assert.Contains("Backup overdue: threshold (hours)", rows, StringComparison.Ordinal);
        Assert.Contains("Log backup overdue: threshold (minutes)", rows, StringComparison.Ordinal);
        Assert.Contains("CHECKDB overdue: threshold (hours)", rows, StringComparison.Ordinal);
        Assert.Empty(page.GetProperty("writes").EnumerateArray());
    }

    [Fact]
    public void TheWebTab_ShowsTheAgesAsRowsOnly_ForAReadOnlySeat()
    {
        var page = Tab("readonly");

        Assert.False(page.GetProperty("form").GetBoolean());
        var rows = string.Join('\n', Strings(page.GetProperty("rowTexts")));
        Assert.Contains("Log backup overdue: threshold (minutes)", rows, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWebTab_SavesTheThreeAgesInOneWrite_ThroughTheDatabaseProtectionRoute()
    {
        var page = Tab("editor/48,1440,720");

        var writes = page.GetProperty("writes").EnumerateArray().ToArray();
        var write = Assert.Single(writes);
        Assert.Equal("/api/alert-settings/database-protection", write.GetProperty("url").GetString());
        var body = write.GetProperty("body");
        Assert.Equal(48, body.GetProperty("backup_overdue").GetProperty("threshold_hours").GetInt32());
        Assert.Equal(1440, body.GetProperty("log_backup_overdue").GetProperty("threshold_minutes").GetInt32());
        Assert.Equal(720, body.GetProperty("checkdb_overdue").GetProperty("threshold_hours").GetInt32());
    }

    [Theory]
    [InlineData("0,60,336", "Full backup overdue age must be a whole number of hours between 1 and 1440.")]
    [InlineData("168,86401,336", "Log backup overdue age must be a whole number of minutes between 1 and 86400.")]
    [InlineData("168,60,8761", "CHECKDB overdue age must be a whole number of hours between 1 and 8760.")]
    [InlineData("168,sixty,336", "Log backup overdue age must be a whole number of minutes between 1 and 86400.")]
    [InlineData("1.5,60,336", "Full backup overdue age must be a whole number of hours between 1 and 1440.")]
    public void TheWebTab_RefusesAnOutOfBoundsAge_WithItsUnit_AndSendsNothing(string typed, string sentence)
    {
        var page = Tab("editor/" + typed);

        Assert.Empty(page.GetProperty("writes").EnumerateArray());
        Assert.Equal([sentence], Strings(page.GetProperty("after").GetProperty("status")));
    }

    [Fact]
    public void TheWebTab_ShowsTheServicesOwnSentence_WhenTheServiceRefusesAWrite()
    {
        var page = Tab("editor/168,60,8760");

        Assert.Single(page.GetProperty("writes").EnumerateArray());
        Assert.Equal(["The service refused 8760."], Strings(page.GetProperty("after").GetProperty("status")));
    }

    // ---- the Viewer save and the web route, against a store

    private static async Task<(int, int, int)> ReadColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ColumnsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
    }

    [Fact]
    public async Task TheViewerUpsert_RoundTripsTheThreeAges_AndAnUnrelatedSaveKeepsThem()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Database Protection editor test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var row = await viewer.GetAlertSettingsAsync(ct) ?? new AlertSettingsRow();
        Assert.Equal((168, 60, 336), (row.BackupOverdueThresholdHours, row.LogBackupOverdueThresholdMinutes, row.CheckDbOverdueThresholdHours));

        row.BackupOverdueThresholdHours = 48;
        row.LogBackupOverdueThresholdMinutes = 1440;
        row.CheckDbOverdueThresholdHours = 720;
        await viewer.UpsertAlertSettingsAsync(row, ct);
        Assert.Equal((48, 1440, 720), await ReadColumnsAsync(connection, ct));

        var saved = await viewer.GetAlertSettingsAsync(ct);
        Assert.Equal((48, 1440, 720), (saved!.BackupOverdueThresholdHours, saved.LogBackupOverdueThresholdMinutes, saved.CheckDbOverdueThresholdHours));

        /* A save of something else carries the loaded ages back, so it never resets them. */
        saved.CooldownMinutes = 9;
        await viewer.UpsertAlertSettingsAsync(saved, ct);
        Assert.Equal((48, 1440, 720), await ReadColumnsAsync(connection, ct));
    }

    [Fact]
    public async Task TheWebRoute_SavesTheAges_RefusesAnOutOfBoundsOneWithTheServicesSentence_AndWritesNothingOnARefusal()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Database Protection editor test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (status, body) = await DarlingWebAlertSettings.SaveDatabaseProtectionAsync(postgres,
            Body("{\"backup_overdue\":{\"threshold_hours\":24},\"log_backup_overdue\":{\"threshold_minutes\":10080},\"checkdb_overdue\":{\"threshold_hours\":8760}}"));
        Assert.Equal(200, status);
        Assert.Equal("updated", body["status"]!.GetValue<string>());
        Assert.Equal((24, 10080, 8760), await ReadColumnsAsync(connection, ct));

        /* One age alone changes only that column. */
        (status, _) = await DarlingWebAlertSettings.SaveDatabaseProtectionAsync(postgres, Body("{\"log_backup_overdue\":{\"threshold_minutes\":30}}"));
        Assert.Equal(200, status);
        Assert.Equal((24, 30, 8760), await ReadColumnsAsync(connection, ct));

        foreach (var (json, field) in new[]
        {
            ("{\"backup_overdue\":{\"threshold_hours\":0}}", "backup_overdue.threshold_hours"),
            ("{\"backup_overdue\":{\"threshold_hours\":1441}}", "backup_overdue.threshold_hours"),
            ("{\"log_backup_overdue\":{\"threshold_minutes\":86401}}", "log_backup_overdue.threshold_minutes"),
            ("{\"checkdb_overdue\":{\"threshold_hours\":8761}}", "checkdb_overdue.threshold_hours"),
        })
        {
            (status, body) = await DarlingWebAlertSettings.SaveDatabaseProtectionAsync(postgres, Body(json));
            Assert.Equal(400, status);
            Assert.Contains(field, body["error"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal((24, 30, 8760), await ReadColumnsAsync(connection, ct));
        }

        /* The on/off boxes and any other field are not writable through this route. */
        foreach (var json in new[] { "{\"backup_overdue\":{\"enabled\":true}}", "{\"cpu\":{\"threshold_percent\":50}}", "{}", "{\"backup_overdue\":{}}" })
        {
            (status, _) = await DarlingWebAlertSettings.SaveDatabaseProtectionAsync(postgres, Body(json));
            Assert.Equal(400, status);
        }

        Assert.Equal((24, 30, 8760), await ReadColumnsAsync(connection, ct));
    }

    private static async Task SeedAsync(string connectionString, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(new DarlingConfig(), ct);
    }

    private static JsonObject Body(string json) => (JsonObject)JsonNode.Parse(json)!;
}
