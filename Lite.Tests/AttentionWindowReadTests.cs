/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Client Timeouts window read (#5640) on a real DuckDB file: successful runs from the collection log only, count rows
/// with their times, sample groups, the top lists, the other server and the out-of-lookback rows left out, and a quiet server
/// (runs but no rows). The twin of Darling's <c>AttentionWindowReadLiveTests</c>.
/// </summary>
public sealed class AttentionWindowReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 5640;
    private const int OtherServerId = 5641;
    private const int QuietServerId = 5642;
    private const int Lookback = 10;

    private readonly DuckDbInitializer _duckDb;

    public AttentionWindowReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task TheWindow_HoldsOnlyThisServersSuccessfulRunsAndRowsInsideTheLookback()
    {
        var now = DateTime.UtcNow;
        long id = 1;
        for (var i = 0; i < 6; i++)
        {
            await LogAsync(id++, ServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS");
        }

        await LogAsync(id++, ServerId, now.AddMinutes(-3.5), "SESSION_MISSING");
        await LogAsync(id++, ServerId, now.AddMinutes(-Lookback - 2), "SUCCESS");
        await LogAsync(id++, OtherServerId, now.AddSeconds(-30), "SUCCESS");

        await RowAsync(id++, ServerId, now.AddSeconds(-28), "count", 7, null, null, null, null);
        await RowAsync(id++, ServerId, now.AddMinutes(-2).AddSeconds(-28), "count", 5, null, null, null, null);
        await RowAsync(id++, ServerId, now.AddSeconds(-28), "sample", 0, "db1", "etl", "fp-a", "SELECT 1", "ws-1");
        await RowAsync(id++, ServerId, now.AddSeconds(-28), "sample", 0, "db1", "etl", "fp-a", "SELECT 1", "ws-1");
        await RowAsync(id++, ServerId, now.AddMinutes(-1).AddSeconds(-28), "sample", 0, "db2", "web", "fp-b", "SELECT 2", "app-1");
        await RowAsync(id++, ServerId, now.AddMinutes(-Lookback - 2), "count", 999, null, null, null, null);
        await RowAsync(id++, OtherServerId, now.AddSeconds(-28), "count", 888, null, null, null, null);

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(ServerId, Lookback);

        Assert.Equal(6, window.RunTimes.Count);
        Assert.True(window.RunTimes.SequenceEqual(window.RunTimes.OrderByDescending(t => t)), "newest first");
        Assert.Equal(12, window.Counts.Sum(c => c.Count));
        Assert.Equal(2, window.Counts.Count);
        Assert.Equal(3, window.SampleGroups.Sum(g => g.Samples));
        Assert.Contains(window.SampleGroups, g => g.DatabaseName == "db1" && g.ClientAppName == "etl" && g.Samples == 2);
        Assert.Equal(new[] { "fp-a", "fp-b" }, window.TopStatements.Select(t => t.Name).ToArray());
        Assert.Equal("SELECT 1", window.TopStatements[0].SampleText);
        Assert.Equal(new[] { "db1", "db2" }, window.TopDatabases.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "etl", "web" }, window.TopApplications.Select(t => t.Name).ToArray());
        Assert.Equal(2, window.TopApplications[0].Samples);
        Assert.Equal(new[] { "ws-1", "app-1" }, window.Hosts.Select(t => t.Name).ToArray());
        Assert.Equal(2, window.Hosts[0].Samples);
    }

    [Fact]
    public async Task TheTopLists_CoverTheSpansSamples_NotTheWholeLookback()
    {
        var now = DateTime.UtcNow;
        long id = 200;
        for (var i = 0; i < 6; i++)
        {
            await LogAsync(id++, ServerId + 100, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS");
        }

        /* The span is the newest three minutes (runs a minute apart, the boundary about three minutes back). The old samples sit
           four and a half minutes back, outside it (round 2 of #5714, L6). */
        await RowAsync(id++, ServerId + 100, now.AddSeconds(-28), "sample", 0, "dbNew", "appNew", "fp-new", "SELECT new");
        await RowAsync(id++, ServerId + 100, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old");
        await RowAsync(id++, ServerId + 100, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old");

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(ServerId + 100, Lookback);

        Assert.False(window.TopsFromLookback);
        Assert.Equal(new[] { "fp-new" }, window.TopStatements.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "dbNew" }, window.TopDatabases.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "appNew" }, window.TopApplications.Select(t => t.Name).ToArray());
    }

    [Fact]
    public async Task ASpanThatKeptNoSamples_FallsBackToTheLookbackLists_AndSaysSo()
    {
        var now = DateTime.UtcNow;
        long id = 300;
        for (var i = 0; i < 6; i++)
        {
            await LogAsync(id++, ServerId + 200, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS");
        }

        await RowAsync(id++, ServerId + 200, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old");

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(ServerId + 200, Lookback);

        Assert.True(window.TopsFromLookback);
        Assert.Equal(new[] { "fp-old" }, window.TopStatements.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "dbOld" }, window.TopDatabases.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "appOld" }, window.TopApplications.Select(t => t.Name).ToArray());
    }

    [Fact]
    public async Task ARowPastTheNewestRunPlusSkew_IsInNeitherTheCountNorTheCardsLists()
    {
        /* Final round of #5714 (L2): the next collector run has committed its rows but not yet its SUCCESS log row, so they sit past
           the newest logged run + RowSkewSeconds. The judged count (Take) never sees them; the card's lists must not either. */
        var now = DateTime.UtcNow;
        long id = 400;
        var server = ServerId + 300;
        for (var i = 0; i < 6; i++)
        {
            await LogAsync(id++, server, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS");
        }

        await RowAsync(id++, server, now.AddSeconds(-28), "count", 7, null, null, null, null);
        await RowAsync(id++, server, now.AddSeconds(-28), "sample", 0, "dbIn", "appIn", "fp-in", "SELECT in");
        await RowAsync(id++, server, now.AddSeconds(60), "count", 100, null, null, null, null);
        await RowAsync(id++, server, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late");
        await RowAsync(id++, server, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late");

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(server, Lookback);

        Assert.False(window.TopsFromLookback);
        Assert.Equal(new[] { "fp-in" }, window.TopStatements.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "dbIn" }, window.TopDatabases.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "appIn" }, window.TopApplications.Select(t => t.Name).ToArray());
        Assert.Equal(7, AttentionEvaluator.Read(window, now)!.TotalCount);
    }

    [Fact]
    public async Task ASpanWhoseOnlySamplesArePastTheCeiling_StillFallsBackToTheLookbackLists_WithoutThem()
    {
        var now = DateTime.UtcNow;
        long id = 500;
        var server = ServerId + 400;
        for (var i = 0; i < 6; i++)
        {
            await LogAsync(id++, server, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS");
        }

        await RowAsync(id++, server, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old");
        await RowAsync(id++, server, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late");

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(server, Lookback);

        Assert.True(window.TopsFromLookback);
        Assert.Equal(new[] { "fp-old" }, window.TopStatements.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "dbOld" }, window.TopDatabases.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "appOld" }, window.TopApplications.Select(t => t.Name).ToArray());
    }

    [Fact]
    public async Task AQuietServer_HasRuns_ButNoCountOrSampleRows()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 5; i++)
        {
            await LogAsync(100 + i, QuietServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS");
        }

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(QuietServerId, Lookback);

        Assert.Equal(5, window.RunTimes.Count);
        Assert.Empty(window.Counts);
        Assert.Empty(window.SampleGroups);
        Assert.Empty(window.TopStatements);
        Assert.NotNull(AttentionEvaluator.Read(window, now));
        Assert.Equal(0, AttentionEvaluator.Read(window, now)!.TotalCount);
    }

    private async Task LogAsync(long logId, int serverId, DateTime time, string status)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
VALUES ($1, $2, 'test-server', $3, $4, 5, $5)";
        command.Parameters.Add(new DuckDBParameter { Value = logId });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = AttentionWindowSql.CollectorName });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        command.Parameters.Add(new DuckDBParameter { Value = status });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task RowAsync(
        long id, int serverId, DateTime time, string kind, long count, string? db, string? app, string? fingerprint, string? text, string? host = null)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO attention_events
    (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, client_hostname)
VALUES ($1, $2, $3, 'test-server', $4, $5, $6, $7, $8, $9, $10)";
        command.Parameters.Add(new DuckDBParameter { Value = id });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = kind });
        command.Parameters.Add(new DuckDBParameter { Value = count });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)text ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)host ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
