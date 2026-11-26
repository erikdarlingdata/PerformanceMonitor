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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Settings window's three settings that darling.json only seeds (#9638): plan retention days, the composed reads' statement
/// timeout and the plan XML storage form. What a typed value is allowed to be, what the window shows for a stored value, and the
/// save and read through the viewer's data service on a real PostgreSQL store.
/// </summary>
public sealed class ServiceSeedSettingsTests
{
    [Theory]
    [InlineData("21", "60", "gzip", 21, 60, "gzip")]
    [InlineData("0", "5", "none", 0, 5, "none")]
    [InlineData(" 365 ", " 600 ", "gzip", 365, 600, "gzip")]
    [InlineData("7", "30", "none", 7, 30, "none")]
    [InlineData("21", "60", " GZIP ", 21, 60, "gzip")]
    public void TryRead_AcceptsTheAllowedValues(string retention, string timeout, string compression, int days, int seconds, string plan)
    {
        var refusal = ServiceSeedSettings.TryRead(retention, timeout, compression, out var gotDays, out var gotSeconds, out var gotPlan, out var badField);

        Assert.Null(refusal);
        Assert.Null(badField);
        Assert.Equal((days, seconds, plan), (gotDays, gotSeconds, gotPlan));
    }

    [Theory]
    [InlineData("6", "60", "gzip", ServiceSeedSettings.RetentionDaysField)]
    [InlineData("-1", "60", "gzip", ServiceSeedSettings.RetentionDaysField)]
    [InlineData("366", "60", "gzip", ServiceSeedSettings.RetentionDaysField)]
    [InlineData("abc", "60", "gzip", ServiceSeedSettings.RetentionDaysField)]
    [InlineData("", "60", "gzip", ServiceSeedSettings.RetentionDaysField)]
    [InlineData("21", "4", "gzip", ServiceSeedSettings.TimeoutSecondsField)]
    [InlineData("21", "0", "gzip", ServiceSeedSettings.TimeoutSecondsField)]
    [InlineData("21", "601", "gzip", ServiceSeedSettings.TimeoutSecondsField)]
    [InlineData("21", "1.5", "gzip", ServiceSeedSettings.TimeoutSecondsField)]
    [InlineData("21", "60", "zip", ServiceSeedSettings.CompressionField)]
    [InlineData("21", "60", null, ServiceSeedSettings.CompressionField)]
    public void TryRead_RefusesAValueTheServiceWouldNotUseAsWritten(string retention, string timeout, string? compression, string field)
    {
        var refusal = ServiceSeedSettings.TryRead(retention, timeout, compression, out _, out _, out _, out var badField);

        Assert.NotNull(refusal);
        Assert.Equal(field, badField);
    }

    [Theory]
    [InlineData(-5)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(ServiceFlagLimits.PlanContentRetentionDaysMin - 1)]
    [InlineData(ServiceFlagLimits.PlanContentRetentionDaysMin)]
    [InlineData(21)]
    [InlineData(ServiceFlagLimits.PlanContentRetentionDaysMax)]
    [InlineData(ServiceFlagLimits.PlanContentRetentionDaysMax + 1)]
    [InlineData(9000)]
    public void TheWindowShowsTheRetentionTheServiceUses(int stored)
    {
        var shown = ServiceSeedSettings.EffectiveRetentionDays(stored);
        Assert.Equal(ServiceFlagLimits.ClampPlanContentRetentionDays(stored), shown);
        Assert.Equal(StoreConfigProvider.ClampPlanContentRetentionDays(stored), shown);
        Assert.Null(ServiceFlagLimits.PlanContentRetentionDaysError(shown));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(ServiceFlagLimits.ComposeStatementTimeoutSecondsMin - 1)]
    [InlineData(ServiceFlagLimits.ComposeStatementTimeoutSecondsMin)]
    [InlineData(15)]
    [InlineData(ServiceFlagLimits.ComposeStatementTimeoutSecondsMax)]
    [InlineData(ServiceFlagLimits.ComposeStatementTimeoutSecondsMax + 1)]
    [InlineData(5000)]
    public void TheWindowShowsTheTimeoutTheServiceUses(int stored)
    {
        var shown = ServiceSeedSettings.EffectiveTimeoutSeconds(stored);
        Assert.Equal(ServiceFlagLimits.ClampComposeStatementTimeoutSeconds(stored), shown);
        Assert.Equal(StoreConfigProvider.ClampComposeStatementTimeoutSeconds(stored), shown);
        Assert.Null(ServiceFlagLimits.ComposeStatementTimeoutSecondsError(shown));
    }

    [Theory]
    [InlineData("none", "none")]
    [InlineData(" NONE ", "none")]
    [InlineData("gzip", "gzip")]
    [InlineData("zstd", "gzip")]
    [InlineData(null, "gzip")]
    public void TheWindowShowsTheStorageFormTheServiceUses(string? stored, string shown)
    {
        Assert.Equal(shown, ServiceSeedSettings.EffectiveCompression(stored));
        Assert.Null(ServiceFlagLimits.PlanXmlCompressionError(shown));
    }

    [Fact]
    public void ARowTheStoreHoldsOutOfRange_ShowsAsAValueTheWindowCanSaveUnchanged()
    {
        /* A hand-edited store must not trap the operator: saving an unrelated setting writes back what the window shows. */
        var row = new ServiceConfigRow { PlanContentRetentionDays = 2, ComposeStatementTimeoutSeconds = 3, PlanXmlCompression = "other" };

        var refusal = ServiceSeedSettings.TryRead(
            ServiceSeedSettings.EffectiveRetentionDays(row.PlanContentRetentionDays).ToString(System.Globalization.CultureInfo.CurrentCulture),
            ServiceSeedSettings.EffectiveTimeoutSeconds(row.ComposeStatementTimeoutSeconds).ToString(System.Globalization.CultureInfo.CurrentCulture),
            ServiceSeedSettings.EffectiveCompression(row.PlanXmlCompression),
            out var days, out var seconds, out var plan, out _);

        Assert.Null(refusal);
        Assert.Equal((7, 5, "gzip"), (days, seconds, plan));
    }
}

/// <summary>
/// The viewer's save and read of the three settings on a real PostgreSQL store (#9638). The row is the store's singleton, so its
/// values are put back. Gated on DARLING_TEST_PG.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class ServiceSeedSettingsLiveTests
{
    private const string ColumnsSql =
        "SELECT capture_plans, mcp_enabled, mcp_port, web_enabled, web_port, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n, plan_content_retention_days, compose_statement_timeout_seconds, plan_xml_compression FROM config_service WHERE id = 1";

    [Fact]
    public async Task TheViewerSavesAndReadsTheThreeSettings_AndMovesNoOtherColumn()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live service settings test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
        var original = await ReadColumnsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(connectionString!);

            /* Start from the shipped defaults, as a store seeded from an untouched darling.json holds them. */
            await ExecAsync(connection,
                "UPDATE config_service SET plan_content_retention_days = 21, compose_statement_timeout_seconds = 60, plan_xml_compression = 'gzip' WHERE id = 1", ct);
            var before = await viewer.GetServiceConfigAsync(ct);
            Assert.NotNull(before);
            Assert.Equal((21, 60, "gzip"), (before!.PlanContentRetentionDays, before.ComposeStatementTimeoutSeconds, before.PlanXmlCompression));

            var startColumns = await ReadColumnsAsync(connection, ct);

            /* Save the three at new values, carrying every other flag through unchanged, as the Settings window does. */
            await viewer.UpdateServiceFlagsAsync(
                before.CapturePlans, before.McpEnabled, before.McpPort, before.WebEnabled, before.WebPort, before.QueryStoreBackfillEnabled,
                before.QueryStoreTextBudgetMb, before.MaxConcurrentSweeps, before.QueryStoreFetchTopN,
                planContentRetentionDays: 45, composeStatementTimeoutSeconds: 120, planXmlCompression: "none", cancellationToken: ct);

            var after = await viewer.GetServiceConfigAsync(ct);
            Assert.NotNull(after);
            Assert.Equal((45, 120, "none"), (after!.PlanContentRetentionDays, after.ComposeStatementTimeoutSeconds, after.PlanXmlCompression));

            /* The nine other columns the save writes did not move. */
            var afterColumns = await ReadColumnsAsync(connection, ct);
            Assert.Equal(startColumns[..9], afterColumns[..9]);
            Assert.Equal(new object[] { 45, 120, "none" }, afterColumns[9..]);

            /* Retention off (0) is a value the store keeps. */
            await viewer.UpdateServiceFlagsAsync(
                after.CapturePlans, after.McpEnabled, after.McpPort, after.WebEnabled, after.WebPort, after.QueryStoreBackfillEnabled,
                after.QueryStoreTextBudgetMb, after.MaxConcurrentSweeps, after.QueryStoreFetchTopN,
                planContentRetentionDays: 0, composeStatementTimeoutSeconds: 60, planXmlCompression: "gzip", cancellationToken: ct);
            var off = await viewer.GetServiceConfigAsync(ct);
            Assert.Equal((0, 60, "gzip"), (off!.PlanContentRetentionDays, off.ComposeStatementTimeoutSeconds, off.PlanXmlCompression));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var command = new NpgsqlCommand(
                    "UPDATE config_service SET capture_plans = $1, mcp_enabled = $2, mcp_port = $3, web_enabled = $4, web_port = $5, query_store_backfill_enabled = $6, query_store_text_budget_mb = $7, max_concurrent_sweeps = $8, query_store_fetch_top_n = $9, plan_content_retention_days = $10, compose_statement_timeout_seconds = $11, plan_xml_compression = $12 WHERE id = 1",
                    cleanup);
                foreach (var value in original)
                {
                    command.Parameters.Add(new NpgsqlParameter { Value = value });
                }

                await command.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object[]> ReadColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ColumnsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "config_service has no singleton row");
        var values = new object[reader.FieldCount];
        reader.GetValues(values);
        return values;
    }
}
