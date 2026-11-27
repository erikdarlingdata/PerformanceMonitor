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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6868: on an Azure SQL Managed Instance <c>sys.dm_xe_session_targets.execution_count</c> reads 0 on every target even
/// with events in the ring buffer, so the #4200 gate (skip the cast and shred when the count equals the stored prior)
/// compared 0 with 0 on every run after the first and never collected a deadlock or a blocked process report. A fresh
/// session on a boxed server reads 0 as well, so it stands in for the Managed Instance: the real batch, with a stored
/// prior of 0 and a current count of 0, must shred (<c>shred_gated</c> = 0).
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. Each test CREATEs and DROPs
/// its own server-scoped session in the own-name form and never touches the shared sessions a live install collects from.
/// <c>DARLING_TEST_SQL_READONLY=1</c> skips the class: it creates a session, and a server that holds real monitoring data
/// must not get one.</para>
///
/// <para>#9518: the blocked-process batch reads <c>blocked process threshold (s)</c> first and reads nothing of its session when
/// the setting is 0 in use. No test here changes that server-wide setting. Each blocked-process test reads it and runs on the
/// side it covers: the gate test where the threshold is set, the threshold-off test where it is 0.</para>
/// </summary>
public sealed class XeShredGateLiveSqlTests
{
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

    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live XE shred gate tests; DARLING_TEST_SQL_READONLY=1 skips them.";

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static CollectorContext MakeContext(string sessionName, IReadOnlyDictionary<string, string> state)
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = state,
            AlwaysOnSessionName = sessionName,
            Watermark = DateTime.UtcNow.AddMinutes(-10),
        };

    /// <summary>The shred_gated measurement one real run of the collector's batch and its own <c>ReadAsync</c> produced.</summary>
    private static async Task<long> RunAndReadShredGatedAsync<TRow>(
        string connectionString, CollectorDefinitionBase<TRow> collector, string measurementLabel, string sessionName, string? priorCount, CancellationToken ct)
    {
        var state = priorCount is null
            ? CollectorContext.NoState
            : new Dictionary<string, string> { [XeShredGate.KeyFor(null)] = priorCount };
        var context = MakeContext(sessionName, state);
        var query = collector.BuildQuery(context);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using DbDataReader reader = await command.ExecuteReaderAsync(ct);
        await collector.ReadAsync(reader, context, ct);

        return context.Measurements.Single(m => m.Label == measurementLabel).Value;
    }

    /// <summary>#9518: the server's <c>blocked process threshold (s)</c> in use, which decides the path the blocked-process batch takes.</summary>
    private static async Task<int> BlockedProcessThresholdAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            "SELECT CONVERT(integer, c.value_in_use) FROM sys.configurations AS c WHERE c.name = N'" + BlockedProcessThresholdOffNote.SettingName + "';",
            connection) { CommandTimeout = 60 };
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static string CreateSql(string sessionName, string eventName)
        => "CREATE EVENT SESSION [" + sessionName + "] ON SERVER ADD EVENT " + eventName
           + " ADD TARGET package0.ring_buffer (SET max_memory = 4096)"
           + " WITH (MAX_DISPATCH_LATENCY = 5 SECONDS, STARTUP_STATE = OFF);"
           + " ALTER EVENT SESSION [" + sessionName + "] ON SERVER STATE = START;";

    [Fact]
    public async Task BlockedProcessBatch_CountZeroAgainstPriorZero_Shreds()
    {
        var connectionString = ConnectionString("pm-6868-bpr-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        /* #9518: with the threshold at 0 the batch never reaches the gate. BlockedProcessBatch_ThresholdZero_ReadsNothingOfTheSession covers that server. */
        Assert.SkipWhen(
            await BlockedProcessThresholdAsync(connectionString!, ct) == 0,
            "This server's 'blocked process threshold (s)' is 0 in use, so the blocked-process batch does not reach the gate.");
        var sessionName = AlwaysOnXeSessions.OwnNameFor(
            LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], AlwaysOnXeSessionKind.BlockedProcess);

        var created = false;
        try
        {
            await ExecuteAsync(connectionString!, CreateSql(sessionName, "sqlserver.blocked_process_report"), ct);
            created = true;

            Assert.Equal(0, await RunAndReadShredGatedAsync(connectionString!, BlockedProcessReportCollector.Instance,
                BlockedProcessReportCollector.ShredGatedMeasurement, sessionName, "0", ct));
            Assert.Equal(0, await RunAndReadShredGatedAsync(connectionString!, BlockedProcessReportCollector.Instance,
                BlockedProcessReportCollector.ShredGatedMeasurement, sessionName, null, ct));
        }
        finally
        {
            if (created)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + sessionName + "] ON SERVER;", CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// #9518: on a server whose threshold is 0 the real batch, pointed at a session that exists and is running, reads the setting
    /// and nothing of the session. The session's counter reads 0 there, which a batch that read it would stage and gate on, so
    /// no staged state and no gate verdict is the proof it was not read. The run carries the off count and the batch still hands
    /// the host its probe-failure result set.
    /// </summary>
    [Fact]
    public async Task BlockedProcessBatch_ThresholdZero_ReadsNothingOfTheSession()
    {
        var connectionString = ConnectionString("pm-9518-bpr-off-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var threshold = await BlockedProcessThresholdAsync(connectionString!, ct);
        Assert.SkipWhen(
            threshold != 0,
            "Runs only where 'blocked process threshold (s)' is 0 in use. This server's is " + threshold.ToString(CultureInfo.InvariantCulture) + ".");
        var sessionName = AlwaysOnXeSessions.OwnNameFor(
            LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], AlwaysOnXeSessionKind.BlockedProcess);

        var created = false;
        try
        {
            await ExecuteAsync(connectionString!, CreateSql(sessionName, "sqlserver.blocked_process_report"), ct);
            created = true;

            foreach (var capturePlanXml in new[] { false, true })
            {
                var context = new CollectorContext
                {
                    ServerId = 1,
                    ServerName = "live-sql",
                    CollectionTime = DateTime.UtcNow,
                    Deltas = null!,
                    State = CollectorContext.NoState,
                    AlwaysOnSessionName = sessionName,
                    Watermark = DateTime.UtcNow.AddMinutes(-10),
                    CapturePlanXml = capturePlanXml,
                };
                var query = BlockedProcessReportCollector.Instance.BuildQuery(context);

                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(ct);
                await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
                await using DbDataReader reader = await command.ExecuteReaderAsync(ct);

                /* The empty payload has the payload's own shape. */
                Assert.Equal(capturePlanXml ? 7 : 5, reader.FieldCount);

                var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, ct);

                Assert.Empty(rows);
                Assert.Empty(context.StagedItemState);
                Assert.DoesNotContain(context.Measurements, m => m.Label == BlockedProcessReportCollector.ShredGatedMeasurement);
                Assert.Equal(1, context.Measurements.Single(m => m.Label == BlockedProcessReportCollector.ThresholdOffMeasurement).Value);
                Assert.True(BlockedProcessThresholdOffNote.TryParse(CollectorMeasurementNote.Render(context.Measurements)));

                /* The host reads the probe-failure set next: present, and empty. */
                Assert.True(await reader.NextResultAsync(ct));
                Assert.False(await reader.ReadAsync(ct));
                Assert.False(await reader.NextResultAsync(ct));
            }
        }
        finally
        {
            if (created)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + sessionName + "] ON SERVER;", CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task DeadlocksBatch_CountZeroAgainstPriorZero_Shreds()
    {
        var connectionString = ConnectionString("pm-6868-deadlocks-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var sessionName = AlwaysOnXeSessions.OwnNameFor(
            LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], AlwaysOnXeSessionKind.Deadlock);

        var created = false;
        try
        {
            await ExecuteAsync(connectionString!, CreateSql(sessionName, "sqlserver.xml_deadlock_report"), ct);
            created = true;

            Assert.Equal(0, await RunAndReadShredGatedAsync(connectionString!, DeadlocksCollector.Instance,
                DeadlocksCollector.ShredGatedMeasurement, sessionName, "0", ct));
            Assert.Equal(0, await RunAndReadShredGatedAsync(connectionString!, DeadlocksCollector.Instance,
                DeadlocksCollector.ShredGatedMeasurement, sessionName, null, ct));
        }
        finally
        {
            if (created)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + sessionName + "] ON SERVER;", CancellationToken.None);
            }
        }
    }
}
