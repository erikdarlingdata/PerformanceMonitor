/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8146: the recreate against a real SQL Server. An own-named long-query session is created at <c>max_events_limit = 1000</c>,
/// the runner's recreate runs with the setting at 5000, and the session reads back at 5000 and running.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (optional <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL
/// auth), so it skips where the runner has no SQL Server; <c>DARLING_TEST_SQL_READONLY=1</c> skips it too, because it creates a
/// session. It drops its session at the end.</para>
/// </summary>
public sealed class LongQuerySessionRecreateLiveSqlTests
{
    private const string InstallId = "8146b0b0";
    private static readonly string SessionName = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallId);

    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live long-query recreate test; DARLING_TEST_SQL_READONLY=1 skips it.";

    private static string? ConnectionString()
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
            ApplicationName = "pm-8146-lq-recreate-live-test",
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

    /// <summary>The session's ring-buffer <c>max_events_limit</c> and whether it runs, or nulls when it does not exist.</summary>
    private static async Task<(long? Limit, bool Running)> ReadSessionAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(@"
SELECT
    max_events_limit = CONVERT(bigint, sesf.value),
    is_running = CASE WHEN EXISTS (SELECT 1/0 FROM sys.dm_xe_sessions AS dxs WHERE dxs.name = ses.name) THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.server_event_session_fields AS sesf
  ON  sesf.event_session_id = ses.event_session_id
  AND sesf.name = N'max_events_limit'
WHERE ses.name = @session_name;", connection) { CommandTimeout = 60 };
        command.Parameters.Add(new SqlParameter("@session_name", System.Data.SqlDbType.NVarChar, 128) { Value = SessionName });
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return (null, false);
        }

        return (reader.IsDBNull(0) ? null : reader.GetInt64(0), reader.GetInt32(1) == 1);
    }

    private static string DropSql() =>
        "IF EXISTS (SELECT 1/0 FROM sys.server_event_sessions WHERE name = N'" + SessionName + "') DROP EVENT SESSION [" + SessionName + "] ON SERVER;";

    [Fact]
    public async Task AnOwnSessionAt1000_IsRecreatedAt5000_AndRuns()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        await ExecuteAsync(connectionString!, DropSql(), ct);
        await ExecuteAsync(
            connectionString!,
            LongQueryCompletionsCollector.BuildCreateSessionSql(SessionName, databaseScoped: false, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds, 1000)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(SessionName, databaseScoped: false),
            ct);

        try
        {
            Assert.Equal((1000L, true), await ReadSessionAsync(connectionString!, ct));

            using var store = Npgsql.NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");
            var logger = new CapturingTestLogger();
            var runner = new DarlingCollectorRunner(store, new CollectorDeltaCalculator(), logger: logger, installId: () => InstallId, xeRingBufferMaxEvents: 5000);
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "lqrecreate-live", Host = "127.0.0.1" },
                ConnectionString = connectionString!,
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 16 },
                StorageName = "lqrecreate-live",
                ServerId = -498147,
            };

            await DarlingXeSessions.RecreateLongQuerySessionsBelowSettingAsync(
                server, runner, new[] { new LongQuerySessionRecreateRequest(null, 1000) }, logger, ct);

            Assert.True((5000L, true) == await ReadSessionAsync(connectionString!, ct), logger.Joined);
            Assert.Contains(
                string.Format(CultureInfo.InvariantCulture, "Recreated long-query session {0} to apply max_events_limit 5000 (was 1000)", SessionName),
                logger.Joined,
                StringComparison.Ordinal);
        }
        finally
        {
            await ExecuteAsync(connectionString!, DropSql(), CancellationToken.None);
        }
    }
}
