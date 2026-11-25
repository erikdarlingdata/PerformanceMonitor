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
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
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

    private static async Task<List<AttentionCollector.Row>> CollectUntilAsync(
        string connectionString, string sessionName, Func<List<AttentionCollector.Row>, bool> done, CancellationToken ct)
    {
        var prior = new Dictionary<string, string> { [AttentionCollector.CountKeyFor(null)] = "0" };
        var rows = new List<AttentionCollector.Row>();
        var deadline = DateTime.UtcNow.AddSeconds(40);
        while (true)
        {
            (rows, _) = await RunCollectorAsync(connectionString, sessionName, prior, ct);
            if (done(rows) || DateTime.UtcNow > deadline)
            {
                return rows;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
    }

    private static async Task RunCallAsync(string connectionString, string sql, int timeoutSeconds, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = timeoutSeconds };
        try
        {
            await command.ExecuteNonQueryAsync(ct);
        }
        catch (SqlException)
        {
            /* The call is meant to fail: a timeout or a divide by zero. */
        }
    }

    /// <summary>
    /// #9507: the session keeps a call the client cancelled while it ran, plus a batch-aborting error after 0.9 s or more (which has no
    /// attention partner, so every read leaves it out). A 2 s command timeout against a 5 s WAITFOR
    /// leaves one captured call of about 2 s (result Abort on SQL Server 2016 to 2022, Error on 2025), which sits at the attention's time
    /// on the same session. A fast failed batch and a reader disposed early after a call that finished leave none.
    /// </summary>
    [Fact]
    public async Task OwnSession_ACommandTimeoutLeavesOneCapturedCall_AFastFailedBatchAndAnEarlyDisposedReaderLeaveNone()
    {
        const string timeoutApp = "pm-9507-timeout";
        const string fastFailApp = "pm-9507-fastfail";
        const string earlyApp = "pm-9507-early";
        var connectionString = ConnectionString(timeoutApp);
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live attention collector test.");
        Assert.SkipWhen(ReadOnlyMode, "DARLING_TEST_SQL_READONLY=1: this server holds real monitoring data.");

        var ct = TestContext.Current.CancellationToken;
        var sessionName = AlwaysOnXeSessions.OwnNameFor(
            LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], AlwaysOnXeSessionKind.Attention);
        var create = AlwaysOnXeSessions.BuildAttentionServerCreateSql()
            .Replace("[" + AttentionCollector.XeSessionName + "]", "[" + sessionName + "]", StringComparison.Ordinal)
            .Replace("STARTUP_STATE = ON", "STARTUP_STATE = OFF", StringComparison.Ordinal);
        Assert.DoesNotContain("[" + AttentionCollector.XeSessionName + "]", create, StringComparison.Ordinal);

        var created = false;
        try
        {
            await ExecuteAsync(connectionString!, create, ct);
            created = true;

            string Conn(string app) => new SqlConnectionStringBuilder(connectionString!) { ApplicationName = app }.ConnectionString;
            await RunCallAsync(Conn(fastFailApp), "SELECT 1/0;", 2, ct);
            await using (var connection = new SqlConnection(Conn(earlyApp)))
            {
                await connection.OpenAsync(ct);
                await using var command = new SqlCommand(
                    "SELECT TOP (200000) a.object_id, a.name FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;", connection) { CommandTimeout = 30 };
                await using var early = await command.ExecuteReaderAsync(ct);
                Assert.True(await early.ReadAsync(ct));
            }

            await RunCallAsync(Conn(timeoutApp), "WAITFOR DELAY '00:00:05';", 2, ct);

            var rows = await CollectUntilAsync(
                connectionString!, sessionName,
                r => r.Exists(x => x.EventKind == AttentionCollector.KindAborted && x.ClientAppName == timeoutApp)
                    && r.Exists(x => x.EventKind == AttentionCollector.KindSample && x.ClientAppName == timeoutApp), ct);

            var captured = Assert.Single(rows, r => r.EventKind == AttentionCollector.KindAborted && r.ClientAppName == timeoutApp);
            Assert.InRange(captured.InterruptDurationUs ?? 0, 1_500_000, 4_000_000);
            Assert.Contains("WAITFOR DELAY", captured.StatementText ?? string.Empty, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("master", captured.DatabaseName);

            /* The match the judge makes: same session, and the call's time inside the attention's window. */
            var attention = Assert.Single(rows, r => r.EventKind == AttentionCollector.KindSample && r.ClientAppName == timeoutApp);
            Assert.Equal(attention.SessionId, captured.SessionId);
            var gapUs = (captured.EventTime!.Value - attention.EventTime!.Value).Ticks / 10;
            Assert.InRange(gapUs, -AttentionWindowSql.CapturedCallMatchBeforeUs, AttentionWindowSql.CapturedCallMatchAfterUs);

            Assert.DoesNotContain(rows, r => r.EventKind == AttentionCollector.KindAborted && (r.ClientAppName == fastFailApp || r.ClientAppName == earlyApp));
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
    /// #9538: a call the application cancelled a few milliseconds into its run (a data layer cancelling before it closes a reader) is not
    /// kept: the session's Abort arm has the same 0.9 s floor as its Error arm, so the cancel takes no ring buffer slot from the attention
    /// samples. Its attention event is still sampled, and a command timeout of 2 s still leaves its captured call.
    /// </summary>
    [Fact]
    public async Task OwnSession_ACallCancelledUnderTheFloorLeavesNoCapturedCall_ButItsAttentionIsSampled()
    {
        const string cancelApp = "pm-9538-cancel";
        const string timeoutApp = "pm-9538-timeout";
        var connectionString = ConnectionString(cancelApp);
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live attention collector test.");
        Assert.SkipWhen(ReadOnlyMode, "DARLING_TEST_SQL_READONLY=1: this server holds real monitoring data.");

        var ct = TestContext.Current.CancellationToken;
        var sessionName = AlwaysOnXeSessions.OwnNameFor(
            LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], AlwaysOnXeSessionKind.Attention);
        var create = AlwaysOnXeSessions.BuildAttentionServerCreateSql()
            .Replace("[" + AttentionCollector.XeSessionName + "]", "[" + sessionName + "]", StringComparison.Ordinal)
            .Replace("STARTUP_STATE = ON", "STARTUP_STATE = OFF", StringComparison.Ordinal);
        Assert.DoesNotContain("[" + AttentionCollector.XeSessionName + "]", create, StringComparison.Ordinal);

        var created = false;
        try
        {
            await ExecuteAsync(connectionString!, create, ct);
            created = true;

            string Conn(string app) => new SqlConnectionStringBuilder(connectionString!) { ApplicationName = app }.ConnectionString;
            await using (var connection = new SqlConnection(Conn(cancelApp)))
            {
                await connection.OpenAsync(ct);
                await using var command = new SqlCommand("WAITFOR DELAY '00:00:05';", connection) { CommandTimeout = 30 };
                using var cancelAfter = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cancelAfter.CancelAfter(TimeSpan.FromMilliseconds(100));
                try
                {
                    await command.ExecuteNonQueryAsync(cancelAfter.Token);
                }
                catch (Exception ex) when (ex is SqlException or OperationCanceledException)
                {
                    /* The call is meant to be cancelled. */
                }
            }

            await RunCallAsync(Conn(timeoutApp), "WAITFOR DELAY '00:00:05';", 2, ct);

            var rows = await CollectUntilAsync(
                connectionString!, sessionName,
                r => r.Exists(x => x.EventKind == AttentionCollector.KindAborted && x.ClientAppName == timeoutApp)
                    && r.Exists(x => x.EventKind == AttentionCollector.KindSample && x.ClientAppName == timeoutApp)
                    && r.Exists(x => x.EventKind == AttentionCollector.KindSample && x.ClientAppName == cancelApp), ct);

            Assert.Contains(rows, r => r.EventKind == AttentionCollector.KindSample && r.ClientAppName == cancelApp);
            Assert.DoesNotContain(rows, r => r.EventKind == AttentionCollector.KindAborted && r.ClientAppName == cancelApp);
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
    /// #9538: a session an earlier build made, with the 0.9 s floor on the Error arm only, reads as another definition, so the host drops it
    /// and creates it again with the floor on both arms. The probe is the shipped one, pointed at two scratch sessions of this test's own.
    /// </summary>
    [Fact]
    public async Task ServerProbe_ReadsAFloorOnTheErrorArmOnlyAsAMismatch_AndTheCurrentDefinitionAsOurs()
    {
        var connectionString = ConnectionString("pm-9538-probe-live");
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL to run the live attention definition test.");
        Assert.SkipWhen(ReadOnlyMode, "DARLING_TEST_SQL_READONLY=1: this server holds real monitoring data.");

        var ct = TestContext.Current.CancellationToken;
        var oldName = "PerformanceMonitor_Darling_" + Guid.NewGuid().ToString("N")[..8] + "_Attention";
        var newName = "PerformanceMonitor_Darling_" + Guid.NewGuid().ToString("N")[..8] + "_Attention";

        const string actions = "ACTION (sqlserver.client_app_name, sqlserver.client_hostname, sqlserver.database_name, sqlserver.session_id, sqlserver.sql_text)";
        var oldCreate = "CREATE EVENT SESSION [" + oldName + "] ON SERVER "
            + "ADD EVENT sqlserver.attention (" + actions + "), "
            + "ADD EVENT sqlserver.rpc_completed (SET collect_statement = (0) " + actions + " WHERE ([result] = 2 OR ([result] = 1 AND [duration] >= 900000))), "
            + "ADD EVENT sqlserver.sql_batch_completed (SET collect_batch_text = (0) " + actions + " WHERE ([result] = 2 OR ([result] = 1 AND [duration] >= 900000))) "
            + "ADD TARGET package0.event_counter, ADD TARGET package0.ring_buffer (SET max_memory = 1024) WITH (MEMORY_PARTITION_MODE = NONE, STARTUP_STATE = OFF);";
        var newCreate = AlwaysOnXeSessions.BuildAttentionServerCreateSql()
            .Replace("[" + AttentionCollector.XeSessionName + "]", "[" + newName + "]", StringComparison.Ordinal)
            .Replace("STARTUP_STATE = ON", "STARTUP_STATE = OFF", StringComparison.Ordinal);
        /* Not started: the probe answers 0 (stopped) for a matching definition and 2 for a mismatch. */
        newCreate = newCreate.Substring(0, newCreate.IndexOf("ALTER EVENT SESSION", StringComparison.Ordinal));

        async Task<int?> ProbeAsync(string name)
        {
            await using var connection = new SqlConnection(connectionString!);
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerProbeSql("pm-9538-test"), connection);
            command.Parameters.AddWithValue("@session_name", name);
            var value = await command.ExecuteScalarAsync(ct);
            return value is null or DBNull ? null : Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        var madeOld = false;
        var madeNew = false;
        try
        {
            await ExecuteAsync(connectionString!, oldCreate, ct);
            madeOld = true;
            await ExecuteAsync(connectionString!, newCreate, ct);
            madeNew = true;

            Assert.Equal(AlwaysOnXeSessions.AttentionDefinitionMismatch, await ProbeAsync(oldName));
            Assert.Equal(0, await ProbeAsync(newName));
        }
        finally
        {
            if (madeOld)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + oldName + "] ON SERVER;", CancellationToken.None);
            }

            if (madeNew)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + newName + "] ON SERVER;", CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// #9507: the session keeps the call's parameter values out of the store. rpc_completed's own <c>statement</c> field carries them
    /// (<c>exec dbo.proc @name=N'...'</c>), and sensitive-statement withholding does not strip values, so the session switches that field
    /// and <c>batch_text</c> off and the row's text comes from the <c>sql_text</c> action, which declares parameters and holds no value.
    /// Three timed-out calls: a parameterised statement (sp_executesql), a stored procedure that has an object name, and a plain batch.
    /// </summary>
    [Fact]
    public async Task OwnSession_ATimedOutRpcStoresTheSqlTextAndNeverTheParameterValue()
    {
        const string secret = "jane.doe@pm9509-secret.example";
        const string statementApp = "pm-9509-statement";
        const string procApp = "pm-9509-proc";
        const string batchApp = "pm-9509-batch";
        const string procName = "pm9509_slow_proc";
        var connectionString = ConnectionString(statementApp);
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live attention collector test.");
        Assert.SkipWhen(ReadOnlyMode, "DARLING_TEST_SQL_READONLY=1: this server holds real monitoring data.");

        var ct = TestContext.Current.CancellationToken;
        var sessionName = AlwaysOnXeSessions.OwnNameFor(
            LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], AlwaysOnXeSessionKind.Attention);
        var create = AlwaysOnXeSessions.BuildAttentionServerCreateSql()
            .Replace("[" + AttentionCollector.XeSessionName + "]", "[" + sessionName + "]", StringComparison.Ordinal)
            .Replace("STARTUP_STATE = ON", "STARTUP_STATE = OFF", StringComparison.Ordinal);
        Assert.DoesNotContain("[" + AttentionCollector.XeSessionName + "]", create, StringComparison.Ordinal);

        string Conn(string app, string database) =>
            new SqlConnectionStringBuilder(connectionString!) { ApplicationName = app, InitialCatalog = database }.ConnectionString;

        async Task TimeOutAsync(string app, string database, string text, CommandType type, string? parameterName)
        {
            await using var connection = new SqlConnection(Conn(app, database));
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand(text, connection) { CommandTimeout = 2, CommandType = type };
            if (parameterName is not null)
            {
                command.Parameters.Add(new SqlParameter(parameterName, SqlDbType.NVarChar, 100) { Value = secret });
            }

            await Assert.ThrowsAnyAsync<SqlException>(() => command.ExecuteNonQueryAsync(ct));
        }

        var created = false;
        var procCreated = false;
        try
        {
            await ExecuteAsync(connectionString!, create, ct);
            created = true;
            await ExecuteAsync(
                Conn("pm-9509-setup", "tempdb"),
                "CREATE PROCEDURE dbo." + procName + " @name nvarchar(100) AS BEGIN WAITFOR DELAY '00:00:05'; SELECT @name; END;", ct);
            procCreated = true;

            await TimeOutAsync(statementApp, "master", "WAITFOR DELAY '00:00:05'; SELECT @p;", CommandType.Text, "@p");
            await TimeOutAsync(procApp, "tempdb", procName, CommandType.StoredProcedure, "@name");
            await TimeOutAsync(batchApp, "master", "WAITFOR DELAY '00:00:05'; SELECT 1;", CommandType.Text, null);

            var rows = await CollectUntilAsync(
                connectionString!, sessionName,
                r => new[] { statementApp, procApp, batchApp }.All(
                    app => r.Exists(x => x.EventKind == AttentionCollector.KindAborted && x.ClientAppName == app)), ct);

            var aborted = rows.Where(r => r.EventKind == AttentionCollector.KindAborted).ToList();
            Assert.DoesNotContain(aborted, r => (r.StatementText ?? string.Empty).Contains(secret, StringComparison.Ordinal));

            var statement = Assert.Single(aborted, r => r.ClientAppName == statementApp);
            Assert.Contains("WAITFOR DELAY", statement.StatementText ?? string.Empty, StringComparison.OrdinalIgnoreCase);

            var proc = Assert.Single(aborted, r => r.ClientAppName == procApp);
            Assert.Contains(procName, proc.StatementText ?? string.Empty, StringComparison.OrdinalIgnoreCase);

            var batch = Assert.Single(aborted, r => r.ClientAppName == batchApp);
            Assert.Contains("WAITFOR DELAY", batch.StatementText ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (procCreated)
            {
                await ExecuteAsync(Conn("pm-9509-setup", "tempdb"), "DROP PROCEDURE dbo." + procName + ";", CancellationToken.None);
            }

            if (created)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + sessionName + "] ON SERVER;", CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// #9507: the drop-and-recreate that upgrades a box replaces an older attention-only session once and leaves the new definition alone.
    /// The probe is the shipped one, pointed at two scratch sessions of this test's own.
    /// </summary>
    [Fact]
    public async Task ServerProbe_ReadsAnAttentionOnlySessionAsAMismatch_AndTheCurrentDefinitionAsOurs()
    {
        var connectionString = ConnectionString("pm-9507-probe-live");
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL to run the live attention definition test.");
        Assert.SkipWhen(ReadOnlyMode, "DARLING_TEST_SQL_READONLY=1: this server holds real monitoring data.");

        var ct = TestContext.Current.CancellationToken;
        var oldName = "PerformanceMonitor_Darling_" + Guid.NewGuid().ToString("N")[..8] + "_Attention";
        var newName = "PerformanceMonitor_Darling_" + Guid.NewGuid().ToString("N")[..8] + "_Attention";

        var oldCreate = "CREATE EVENT SESSION [" + oldName + "] ON SERVER "
            + "ADD EVENT sqlserver.attention (ACTION (sqlserver.client_app_name, sqlserver.client_hostname, sqlserver.database_name, sqlserver.session_id, sqlserver.sql_text)) "
            + "ADD TARGET package0.event_counter, ADD TARGET package0.ring_buffer (SET max_memory = 1024) WITH (MEMORY_PARTITION_MODE = NONE, STARTUP_STATE = OFF);";
        var newCreate = AlwaysOnXeSessions.BuildAttentionServerCreateSql()
            .Replace("[" + AttentionCollector.XeSessionName + "]", "[" + newName + "]", StringComparison.Ordinal)
            .Replace("STARTUP_STATE = ON", "STARTUP_STATE = OFF", StringComparison.Ordinal);
        /* Not started: the probe answers 0 (stopped) for a matching definition and 2 for a mismatch. */
        newCreate = newCreate.Substring(0, newCreate.IndexOf("ALTER EVENT SESSION", StringComparison.Ordinal));

        async Task<int?> ProbeAsync(string name)
        {
            await using var connection = new SqlConnection(connectionString!);
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerProbeSql("pm-9507-test"), connection);
            command.Parameters.AddWithValue("@session_name", name);
            var value = await command.ExecuteScalarAsync(ct);
            return value is null or DBNull ? null : Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        var madeOld = false;
        var madeNew = false;
        try
        {
            await ExecuteAsync(connectionString!, oldCreate, ct);
            madeOld = true;
            await ExecuteAsync(connectionString!, newCreate, ct);
            madeNew = true;

            Assert.Equal(AlwaysOnXeSessions.AttentionDefinitionMismatch, await ProbeAsync(oldName));
            Assert.Equal(0, await ProbeAsync(newName));
        }
        finally
        {
            if (madeOld)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + oldName + "] ON SERVER;", CancellationToken.None);
            }

            if (madeNew)
            {
                await ExecuteAsync(connectionString!, "DROP EVENT SESSION [" + newName + "] ON SERVER;", CancellationToken.None);
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
