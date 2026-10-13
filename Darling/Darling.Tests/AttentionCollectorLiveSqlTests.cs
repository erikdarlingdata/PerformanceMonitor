/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5754: the Client Timeouts collector's REAL batch (<see cref="AttentionCollector.BuildQuery"/>) run against a live SQL
/// Server. #5714's tests pinned the batch text and fed <see cref="AttentionCollector.ReadAsync"/> hand-made rows, so nothing
/// ever ran the <c>sum(@count)</c> read on a real event_counter, and it failed on every run with "Error converting data type
/// nvarchar to bigint" (an XQuery sum returns xs:double, whose text, 0E0, is not a bigint).
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth, as the live collection E2E reads them), so it skips where the runner has no
/// SQL Server. Two modes:</para>
/// <list type="bullet">
/// <item><description>default: the test CREATEs and DROPs its own server-scoped session, named with the capture's own-name
/// form (<c>PerformanceMonitor_Darling_{8 hex}_Attention</c>), so it never touches the shared
/// <c>PerformanceMonitor_Attention</c> session a live install collects from. It reads at count 0, makes a real attention
/// (a 5-second WAITFOR under a 1-second client command timeout), then reads again.</description></item>
/// <item><description><c>DARLING_TEST_SQL_READONLY=1</c>: for a server that holds real monitoring data. It creates, alters and
/// drops nothing: it runs the collector's read-only batch against the existing shared session, and skips when there is none.</description></item>
/// </list>
/// </summary>
public sealed class AttentionCollectorLiveSqlTests
{
    private static readonly DateTime Now = DateTime.UtcNow;

    private static string? ConnectionString(string applicationName)
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
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
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static bool ReadOnlyMode => Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1";

    private static CollectorContext MakeContext(string sessionName, IReadOnlyDictionary<string, string>? state = null)
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = state ?? CollectorContext.NoState,
            AlwaysOnSessionName = sessionName,
            Watermark = Now.AddMinutes(-10),
        };

    /// <summary>Runs the collector's real batch and its real <c>ReadAsync</c>, the way the host does.</summary>
    private static async Task<(List<AttentionCollector.Row> Rows, CollectorContext Context)> RunCollectorAsync(
        string connectionString, string sessionName, IReadOnlyDictionary<string, string>? state, CancellationToken ct)
    {
        var context = MakeContext(sessionName, state);
        var query = AttentionCollector.Instance.BuildQuery(context);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        /* The shipped command builder, so the text and the parameter bindings are the host's own, not a copy of them. */
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);

        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, ct);
        return (rows, context);
    }

    private static long StagedCount(CollectorContext context) =>
        long.Parse(context.StagedItemState[AttentionCollector.CountKeyFor(null)], CultureInfo.InvariantCulture);

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One real attention: a 5-second WAITFOR under a 1-second client command timeout, so the client cancels.</summary>
    private static async Task GenerateAttentionAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand("WAITFOR DELAY '00:00:05';", connection) { CommandTimeout = 1 };
        var ex = await Assert.ThrowsAnyAsync<SqlException>(() => command.ExecuteNonQueryAsync(ct));
        Assert.Contains("Execution Timeout Expired", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OwnSession_CollectorBatchRunsAtCountZero_ThenCountsARealClientTimeout()
    {
        const string appName = "pm-5754-attention-live-test";
        var connectionString = ConnectionString(appName);
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live attention collector test.");
        Assert.SkipWhen(ReadOnlyMode, "DARLING_TEST_SQL_READONLY=1: this server holds real monitoring data, so the read-only test runs instead.");

        var ct = TestContext.Current.CancellationToken;
        var sessionName = AlwaysOnXeSessions.OwnNameFor(
            LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], AlwaysOnXeSessionKind.Attention);
        Assert.True(AlwaysOnXeSessions.IsOwnName(sessionName, AlwaysOnXeSessionKind.Attention));

        /* The ensure's own DDL, renamed to the test session, and not started with the server. The test session is the only one touched. */
        var create = AlwaysOnXeSessions.BuildAttentionServerCreateSql()
            .Replace("[" + AttentionCollector.XeSessionName + "]", "[" + sessionName + "]", StringComparison.Ordinal)
            .Replace("STARTUP_STATE = ON", "STARTUP_STATE = OFF", StringComparison.Ordinal);
        Assert.DoesNotContain("[" + AttentionCollector.XeSessionName + "]", create, StringComparison.Ordinal);
        Assert.Contains("[" + sessionName + "]", create, StringComparison.Ordinal);

        var created = false;
        try
        {
            await ExecuteAsync(connectionString!, create, ct);
            created = true;

            /* Count 0: the counter target lists the event at zero from the start, which is the case that failed on every run. */
            var (quietRows, quietContext) = await RunCollectorAsync(connectionString!, sessionName, null, ct);
            /* Filtered by this test's application name: the session is server-scoped, so another client's attention must not red this read. */
            Assert.DoesNotContain(quietRows, r => r.EventKind == AttentionCollector.KindSample && r.ClientAppName == appName);
            Assert.Equal(0, StagedCount(quietContext));
            Assert.True(quietContext.StagedItemState.ContainsKey(XeShredGate.KeyFor(null)), "the ring buffer's execution_count is staged");

            await GenerateAttentionAsync(connectionString!, ct);

            /* The counter counts at once; the ring buffer gets the event up to MAX_DISPATCH_LATENCY (5 s) later. */
            var prior = new Dictionary<string, string> { [AttentionCollector.CountKeyFor(null)] = "0" };
            List<AttentionCollector.Row> rows = new();
            CollectorContext? context = null;
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (true)
            {
                (rows, context) = await RunCollectorAsync(connectionString!, sessionName, prior, ct);
                if (rows.Exists(r => r.EventKind == AttentionCollector.KindSample) || DateTime.UtcNow > deadline)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }

            Assert.True(StagedCount(context!) >= 1, "the event_counter counted the client timeout");

            var count = Assert.Single(rows, r => r.EventKind == AttentionCollector.KindCount);
            Assert.True(count.AttentionCount >= 1);
            Assert.Equal(StagedCount(context!), count.AttentionCount);

            var sample = Assert.Single(rows, r => r.EventKind == AttentionCollector.KindSample && r.ClientAppName == appName);
            Assert.NotNull(sample.EventTime);
            Assert.Equal("master", sample.DatabaseName);
            Assert.NotNull(sample.SessionId);
            Assert.True(sample.SessionId > 0);
            Assert.NotNull(sample.RequestId);
            Assert.NotNull(sample.InterruptDurationUs);
            Assert.True(sample.InterruptDurationUs > 0);
            Assert.False(string.IsNullOrEmpty(sample.ClientHostName));
            Assert.Contains("WAITFOR DELAY", sample.StatementText ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (created)
            {
                /* Drops the test session by its own name, and only that one. */
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + sessionName + "] ON SERVER;", CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task ExistingSharedSession_ReadOnlyBatchRunsGreen()
    {
        var connectionString = ConnectionString("pm-5754-attention-live-readonly");
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL to run the live attention collector test.");
        Assert.SkipUnless(ReadOnlyMode, "Runs with DARLING_TEST_SQL_READONLY=1 only: it reads the shared session of a server that holds real monitoring data.");

        var ct = TestContext.Current.CancellationToken;
        await using (var probe = new SqlConnection(connectionString))
        {
            await probe.OpenAsync(ct);
            await using var exists = new SqlCommand("SELECT COUNT(*) FROM sys.dm_xe_sessions WHERE name = @name;", probe);
            exists.Parameters.AddWithValue("@name", AttentionCollector.XeSessionName);
            Assert.SkipWhen(Convert.ToInt32(await exists.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) == 0,
                "No running " + AttentionCollector.XeSessionName + " session on this server.");
        }

        /* Read only: the batch selects from the XE DMVs and writes nothing. It must run, and the counter must read as a number. */
        var (_, context) = await RunCollectorAsync(connectionString!, AttentionCollector.XeSessionName, null, ct);
        Assert.True(StagedCount(context) >= 0);
    }

    [Theory]
    [InlineData("<CounterTarget truncated=\"0\"><Packages><Package name=\"sqlserver\"><Event name=\"attention\" count=\"0\"/></Package></Packages></CounterTarget>", 0L)]
    [InlineData("<CounterTarget truncated=\"0\"><Packages><Package name=\"sqlserver\"><Event name=\"attention\" count=\"12\"/></Package></Packages></CounterTarget>", 12L)]
    [InlineData("<CounterTarget truncated=\"0\"><Packages><Package name=\"sqlserver\"><Event name=\"attention\" count=\"4294967303\"/></Package></Packages></CounterTarget>", 4294967303L)]
    [InlineData("<CounterTarget truncated=\"0\"><Packages><Package name=\"sqlserver\"><Event name=\"attention\" count=\"999999999999999\"/></Package></Packages></CounterTarget>", 999999999999999L)]
    [InlineData("<CounterTarget truncated=\"0\"><Packages><Package name=\"sqlserver\"><Event name=\"other\" count=\"99\"/><Event name=\"attention\" count=\"3\"/></Package><Package name=\"p2\"><Event name=\"attention\" count=\"4\"/></Package></Packages></CounterTarget>", 7L)]
    public async Task CounterRead_IsExact_ForZero_AboveTwoToThe31_AndTheRealEventCounterShape(string counterXml, long expected)
    {
        var connectionString = ConnectionString("pm-5754-attention-live-counter");
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL to run the live counter read test.");

        var ct = TestContext.Current.CancellationToken;

        /* A variable and a SELECT: reads nothing from any server object, so it is safe on a server that holds real data. */
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            "DECLARE @target_data xml = CONVERT(xml, @xml); SELECT " + AttentionCollector.EventCountExpression + ";", connection);
        command.Parameters.Add(new SqlParameter("@xml", SqlDbType.NVarChar, -1) { Value = counterXml });

        Assert.Equal(expected, await command.ExecuteScalarAsync(ct));
    }

    /* The shipped TEXT, independent of EventCountExpression (BuildText substitutes that constant itself, so comparing against it passes by
       construction): the event-count read is an XQuery sum read as 'float', wrapped in CONVERT(bigint, ...). */
    private static readonly Regex EventCountRead = new(
        @"@event_count\s*=\s*CONVERT\(\s*bigint\s*,\s*@target_data\.value\(\s*'sum\([^']*\)'\s*,\s*'float'\s*\)\s*\)\s*;",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    [Theory]
    [InlineData(false, "sys.dm_xe_session_targets")]
    [InlineData(true, "sys.dm_xe_database_session_targets")]
    public void BatchText_ReadsTheCountAsFloat_WrappedInConvertBigint_OnBothDmvForms(bool isAzureSqlDb, string targetsDmv)
    {
        var text = AttentionCollector.BuildText(AttentionCollector.XeSessionName, isAzureSqlDb);
        Assert.DoesNotContain("{3}", text, StringComparison.Ordinal);
        Assert.Contains(targetsDmv, text, StringComparison.Ordinal);
        Assert.Single(EventCountRead.Matches(text));
        Assert.DoesNotMatch(new Regex(@"value\s*\(\s*'sum\([^']*'\s*,\s*'(big)?int", RegexOptions.IgnoreCase), text);
    }
}
