/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8146: the ring-buffer position fragments and <see cref="RingBufferLossAccounting"/> against a real session. A server-scoped
/// session with a 10-event ring_buffer takes user events from <c>sp_trace_generateevent</c> (which needs sysadmin or ALTER
/// TRACE), and each read runs the fragments a collector batch composes, then <see cref="RingBufferLossAccounting.Apply"/>.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (optional <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL
/// auth), so it skips where the runner has no SQL Server; <c>DARLING_TEST_SQL_READONLY=1</c> skips it too, because it creates a
/// session. Each test creates and drops its own session.</para>
/// </summary>
public sealed class RingBufferLossLiveSqlTests
{
    private const string SessionName = "pm_test_8146";

    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live ring-buffer loss tests; DARLING_TEST_SQL_READONLY=1 skips them.";

    private static string? ConnectionString(string applicationName)
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host) || Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1")
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = applicationName,
            Pooling = false,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string DropSql() =>
        "IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'" + SessionName + "') DROP EVENT SESSION [" + SessionName + "] ON SERVER;";

    private static string CreateSql() =>
        DropSql()
        + " CREATE EVENT SESSION [" + SessionName + "] ON SERVER ADD EVENT sqlserver.user_event"
        + " ADD TARGET package0.ring_buffer (SET max_memory = 4096, max_events_limit = 10)"
        + " WITH (MAX_DISPATCH_LATENCY = 1 SECONDS, STARTUP_STATE = OFF);"
        + " ALTER EVENT SESSION [" + SessionName + "] ON SERVER STATE = START;";

    private static string RestartSql() =>
        "ALTER EVENT SESSION [" + SessionName + "] ON SERVER STATE = STOP; WAITFOR DELAY '00:00:02'; "
        + "ALTER EVENT SESSION [" + SessionName + "] ON SERVER STATE = START;";

    /// <summary>Fires <paramref name="count"/> user events, then waits past the session's 1 second dispatch latency so the target holds them.</summary>
    private static Task FireAsync(string connectionString, int count, CancellationToken ct) =>
        ExecuteAsync(
            connectionString,
            "DECLARE @i int = 0; WHILE @i < " + count.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + " BEGIN EXEC sp_trace_generateevent @eventid = 82, @userinfo = N'8146'; SET @i += 1; END; WAITFOR DELAY '00:00:02';",
            ct);

    /// <summary>One read the way a collector batch composes it: the target, the parse, truncation, the header, the session row and the position columns.</summary>
    private static async Task<(RingBufferObservation Observation, bool Truncated)> ReadAsync(string connectionString, CancellationToken ct)
    {
        var sql = RingBufferXml.DeclareSql + @"
SELECT
    @ring_session = xes.name,
    @ring_text = CONVERT(nvarchar(max), xet.target_data)
FROM sys.dm_xe_sessions AS xes
JOIN sys.dm_xe_session_targets AS xet
  ON xet.event_session_address = xes.address
WHERE xes.name = N'" + SessionName + @"'
AND   xet.target_name = N'ring_buffer';
" + RingBufferXml.ParseSql + @"
" + RingBufferXml.TruncationSql + @"
" + RingBufferXml.PositionDeclareSql + @"
SET @ring_position_session = N'" + SessionName + @"';
" + RingBufferXml.HeaderSql + @"
" + RingBufferXml.SessionPositionServerSql + @"
SELECT
    " + RingBufferXml.TruncationColumnsSql + @",
    " + RingBufferXml.PositionColumnsSql + ";";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await using DbDataReader reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        return (RingBufferObservation.Read(reader, 3), reader.GetBoolean(0));
    }

    private static async Task<CollectorContext> ReadAndApplyAsync(string connectionString, IReadOnlyDictionary<string, string> state, CancellationToken ct)
    {
        var (observation, truncated) = await ReadAsync(connectionString, ct);
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = state,
        };
        RingBufferLossAccounting.Apply(context, observation, truncated);
        context.LandStagedItemState();
        return context;
    }

    private static IReadOnlyDictionary<string, string> Staged(CollectorContext context) =>
        new Dictionary<string, string>(context.PendingState);

    [Fact]
    public async Task ThirtyThenFiftyNineEvents_ReportsFifteenEvicted()
    {
        var connectionString = ConnectionString("pm-8146-ring-loss-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await ExecuteAsync(connectionString!, CreateSql(), ct);

            await FireAsync(connectionString!, 30, ct);
            var first = await ReadAndApplyAsync(connectionString!, CollectorContext.NoState, ct);
            Assert.Empty(first.Measurements);
            Assert.Equal(30, RingBufferLoss.TryParseState(first.PendingState[RingBufferLossAccounting.StateKey])!.Value.Total);

            /* 4 more events since the first read: the 10-event buffer still holds all 4, so nothing was lost. */
            await FireAsync(connectionString!, 4, ct);
            var second = await ReadAndApplyAsync(connectionString!, Staged(first), ct);
            Assert.Empty(second.Measurements);

            /* 25 more since the second read: the buffer holds 10 of them, so 15 were evicted before this read saw them. */
            await FireAsync(connectionString!, 25, ct);
            var third = await ReadAndApplyAsync(connectionString!, Staged(second), ct);
            Assert.Equal(15, third.Measurements.Single(m => m.Label == "ring_buffer_evicted").Value);
            Assert.Equal(10, third.Measurements.Single(m => m.Label == "ring_buffer_events_limit").Value);
        }
        finally
        {
            await ExecuteAsync(connectionString!, DropSql(), CancellationToken.None);
        }
    }

    [Fact]
    public async Task StopStart_ReportsUnknown()
    {
        var connectionString = ConnectionString("pm-8146-ring-loss-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await ExecuteAsync(connectionString!, CreateSql(), ct);

            await FireAsync(connectionString!, 30, ct);
            var first = await ReadAndApplyAsync(connectionString!, CollectorContext.NoState, ct);

            await ExecuteAsync(connectionString!, RestartSql(), ct);
            await FireAsync(connectionString!, 3, ct);
            var second = await ReadAndApplyAsync(connectionString!, Staged(first), ct);

            Assert.Equal(1, second.Measurements.Single(m => m.Label == "ring_buffer_loss_unknown").Value);
            Assert.DoesNotContain(second.Measurements, m => m.Label == "ring_buffer_evicted");
        }
        finally
        {
            await ExecuteAsync(connectionString!, DropSql(), CancellationToken.None);
        }
    }
}
