/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7159: get_server_summary, get_daily_summary and get_daily_summary_range read only SQL Server tables, but a PostgreSQL target
/// still writes collection_log rows, so the reads never missed: they answered blocking_count 0 and deadlock_count 0 as if measured,
/// and banded every collected day Healthy. A PostgreSQL target now gets the not_collected envelope; a SQL Server target is unchanged.
/// </summary>
/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class DarlingMcpHealthToolsPostgresTargetTests
{
    private const int PgServerId = 71;
    private const string PgServerName = "pg-seven-one";
    private const int SqlServerId = 72;
    private const string SqlServerName = "sql-seven-two";

    [Theory]
    [InlineData(MonitoredEngineKind.Postgres, true)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, true)]
    [InlineData(MonitoredEngineKind.SqlServer, false)]
    [InlineData(null, false)]
    public void PostgresTargetSqlServerReadStatus_GatesOnlyPostgresKinds(string? engineKind, bool gated)
    {
        var envelope = DarlingEngineCapability.PostgresTargetSqlServerReadStatus("s", engineKind, "get_daily_summary");

        Assert.Equal(gated, envelope is not null);
        if (gated)
        {
            using var doc = JsonDocument.Parse(envelope!);
            Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
        }
    }

    [Fact]
    public async Task PostgresTarget_AnswersNotCollected_AndSqlServerTargetStillReadsItsRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the health tools PostgreSQL target test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(storeString);

        var now = DateTime.UtcNow;
        var today = now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        /* Both servers have a collection_log row today, which is all a PostgreSQL target's collectors need to defeat the no-data check. */
        foreach (var (id, name, kind) in new[] { (PgServerId, PgServerName, "postgres"), (SqlServerId, SqlServerName, "sqlserver") })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) VALUES ($1, $2, $2, true, $3, now(), now())",
                id, name, kind);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now), id, name, "pg_database_stats", "SUCCESS", 42, 100);
        }

        foreach (var json in new[]
        {
            await DarlingMcpHealthTools.GetServerSummary(postgres, PgServerName, cancellationToken: ct),
            await DarlingMcpHealthTools.GetDailySummary(postgres, PgServerName, today, cancellationToken: ct),
            await DarlingMcpHealthTools.GetDailySummaryRange(postgres, PgServerName, 3, cancellationToken: ct),
        })
        {
            using var doc = JsonDocument.Parse(json);
            Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
            Assert.Contains("get_pg_", json, StringComparison.Ordinal);
            Assert.DoesNotContain("blocking_count", json, StringComparison.Ordinal);
            Assert.DoesNotContain("health_band", json, StringComparison.Ordinal);
        }

        /* A SQL Server target is untouched: its collection_log-only day is still a collected, banded day. */
        var sqlDay = await DarlingMcpHealthTools.GetDailySummary(postgres, SqlServerName, today, cancellationToken: ct);
        using (var doc = JsonDocument.Parse(sqlDay))
        {
            Assert.False(doc.RootElement.TryGetProperty("status", out _), sqlDay);
            Assert.Equal("Healthy", doc.RootElement.GetProperty("health_band").GetString());
        }

        var sqlSummary = await DarlingMcpHealthTools.GetServerSummary(postgres, SqlServerName, cancellationToken: ct);
        Assert.Contains("blocking_count", sqlSummary, StringComparison.Ordinal);
    }
}
