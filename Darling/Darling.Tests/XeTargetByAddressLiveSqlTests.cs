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
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9517: every Extended Events target read finds the session once (<see cref="RingBufferXml.ResolveAddressSql"/>) and then reads
/// its targets by that address (<see cref="RingBufferXml.TargetFromSql"/>), instead of joining the targets DMV to the sessions DMV
/// by name. These tests run the two fragments and the collectors' real batches against a live SQL Server and check what the
/// swap must not change: the read returns the NAMED session's targets and not another session's, and a session that is not
/// running, or that went away between the two steps, reads as no row and raises nothing.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. Each test CREATEs and DROPs
/// its own server-scoped sessions in the own-name form and never touches the shared sessions a live install collects from.
/// <c>DARLING_TEST_SQL_READONLY=1</c> skips the class: it creates sessions, and a server that holds real monitoring data must
/// not get any. The database-scoped pair (Azure SQL Database) is not reachable from a test server: its fragment differs only in
/// the two DMV names.</para>
/// </summary>
public sealed class XeTargetByAddressLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live by-address target read tests; DARLING_TEST_SQL_READONLY=1 skips them.";

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

    private static string DropIfExistsSql(string sessionName)
        => "IF EXISTS (SELECT 1/0 FROM sys.server_event_sessions WHERE name = N'" + sessionName + "') DROP EVENT SESSION [" + sessionName + "] ON SERVER;";

    /// <summary>
    /// A session that counts one event, only for connections that name <paramref name="application"/>, with the two targets the
    /// collectors read. The event_counter target lists an event once it has fired, so firing it from one application tells the two
    /// test sessions' targets apart.
    /// </summary>
    private static string CreateCountingSql(string sessionName, string eventName, string application)
        => "CREATE EVENT SESSION [" + sessionName + "] ON SERVER ADD EVENT " + eventName
           + " (WHERE ([sqlserver].[client_app_name] = N'" + application + "'))"
           + " ADD TARGET package0.event_counter, ADD TARGET package0.ring_buffer (SET max_memory = 1024)"
           + " WITH (MAX_DISPATCH_LATENCY = 5 SECONDS, STARTUP_STATE = OFF);"
           + " ALTER EVENT SESSION [" + sessionName + "] ON SERVER STATE = START;";

    private const string ApplicationA = "pm-9517-application-a";
    private const string ApplicationB = "pm-9517-application-b";
    private const string EventA = "sql_batch_completed";
    private const string EventB = "sql_statement_completed";

    private static Task CreateSessionAAsync(string connectionString, string sessionName, CancellationToken ct)
        => ExecuteAsync(connectionString, CreateCountingSql(sessionName, "sqlserver." + EventA, ApplicationA), ct);

    private static Task CreateSessionBAsync(string connectionString, string sessionName, CancellationToken ct)
        => ExecuteAsync(connectionString, CreateCountingSql(sessionName, "sqlserver." + EventB, ApplicationB), ct);

    /// <summary>Runs one trivial batch under <paramref name="application"/>, which fires that application's session event once.</summary>
    private static Task FireAsync(string? baseConnectionString, string application, CancellationToken ct)
    {
        var builder = new SqlConnectionStringBuilder(baseConnectionString) { ApplicationName = application };
        return ExecuteAsync(builder.ConnectionString, "SELECT 1;", ct);
    }

    private sealed record TargetRead(
        byte[]? Address, string? Name, long? ExecutionCount, string? CounterData, string? RingData);

    /// <summary>
    /// Runs the two fragments the way a collector batch does: resolve once, then read the ring_buffer counter, the event_counter
    /// target and the ring_buffer data by that one address. <paramref name="betweenSql"/> runs in the same batch, after the address
    /// is resolved and before any target is read, so it is exactly the gap a session could go away in.
    /// </summary>
    private static async Task<TargetRead> ReadByAddressAsync(string connectionString, string sessionName, string betweenSql, CancellationToken ct)
    {
        var batch = RingBufferXml.ResolveAddressSql(false, sessionName) + "\n"
            + betweenSql + "\n"
            + "DECLARE @count bigint, @counter nvarchar(max), @ring nvarchar(max);\n"
            + "SELECT @count = xet.execution_count\n" + RingBufferXml.TargetFromSql(false, "ring_buffer") + "\nOPTION(RECOMPILE);\n"
            + "SELECT @counter = CONVERT(nvarchar(max), xet.target_data)\n" + RingBufferXml.TargetFromSql(false, "event_counter") + "\nOPTION(RECOMPILE);\n"
            + "SELECT @ring = CONVERT(nvarchar(max), xet.target_data)\n" + RingBufferXml.TargetFromSql(false, "ring_buffer") + "\nOPTION(RECOMPILE);\n"
            + "SELECT @xe_address, @xe_name, @count, @counter, @ring;";

        return await RunAsync(connectionString, batch, ct);
    }

    /// <summary>The same three reads through the name join the collectors replaced, as the reference the by-address read must equal.</summary>
    private static async Task<TargetRead> ReadByJoinAsync(string connectionString, string sessionName, CancellationToken ct)
    {
        var batch =
            "DECLARE @xe_address varbinary(8), @xe_name sysname, @count bigint, @counter nvarchar(max), @ring nvarchar(max);\n"
            + "SELECT @xe_address = xes.address, @xe_name = xes.name FROM sys.dm_xe_sessions AS xes WHERE xes.name = N'" + sessionName + "';\n"
            + "SELECT @count = xet.execution_count FROM sys.dm_xe_session_targets AS xet JOIN sys.dm_xe_sessions AS xes ON xes.address = xet.event_session_address WHERE xes.name = N'" + sessionName + "' AND xet.target_name = N'ring_buffer' OPTION(RECOMPILE);\n"
            + "SELECT @counter = CONVERT(nvarchar(max), xet.target_data) FROM sys.dm_xe_session_targets AS xet JOIN sys.dm_xe_sessions AS xes ON xes.address = xet.event_session_address WHERE xes.name = N'" + sessionName + "' AND xet.target_name = N'event_counter' OPTION(RECOMPILE);\n"
            + "SELECT @ring = CONVERT(nvarchar(max), xet.target_data) FROM sys.dm_xe_session_targets AS xet JOIN sys.dm_xe_sessions AS xes ON xes.address = xet.event_session_address WHERE xes.name = N'" + sessionName + "' AND xet.target_name = N'ring_buffer' OPTION(RECOMPILE);\n"
            + "SELECT @xe_address, @xe_name, @count, @counter, @ring;";

        return await RunAsync(connectionString, batch, ct);
    }

    private static async Task<TargetRead> RunAsync(string connectionString, string batch, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(batch, connection) { CommandTimeout = 60 };
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return new TargetRead(
            reader.IsDBNull(0) ? null : reader.GetFieldValue<byte[]>(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    private static string OwnName(AlwaysOnXeSessionKind kind)
        => AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8], kind);

    [Fact]
    public async Task TwoRunningSessions_EachByAddressReadReturnsItsOwnTargets_EqualToTheNameJoin()
    {
        var connectionString = ConnectionString("pm-9517-by-address-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var sessionA = OwnName(AlwaysOnXeSessionKind.BlockedProcess);
        var sessionB = OwnName(AlwaysOnXeSessionKind.Deadlock);

        try
        {
            await CreateSessionAAsync(connectionString!, sessionA, ct);
            await CreateSessionBAsync(connectionString!, sessionB, ct);

            var sessions = new[] { (Name: sessionA, OwnEvent: EventA, OtherEvent: EventB), (Name: sessionB, OwnEvent: EventB, OtherEvent: EventA) };

            /* Nothing has fired yet, so every field of both targets is stable and the two forms must agree on all of them. */
            foreach (var session in sessions)
            {
                var byAddress = await ReadByAddressAsync(connectionString!, session.Name, string.Empty, ct);
                var byJoin = await ReadByJoinAsync(connectionString!, session.Name, ct);

                Assert.NotNull(byAddress.Address);
                Assert.Equal(session.Name, byAddress.Name);
                Assert.NotNull(byAddress.ExecutionCount);
                Assert.NotNull(byAddress.RingData);
                Assert.Equal(byJoin, byAddress, TargetReadComparer.Instance);
            }

            await FireAsync(connectionString, ApplicationA, ct);
            await FireAsync(connectionString, ApplicationB, ct);

            /* The event_counter target counts as the event fires, so it tells the sessions' targets apart: each address reads its own
               session's counter and never the other's. The ring buffer is dispatched on a latency, so only what it cannot hold is asserted. */
            foreach (var session in sessions)
            {
                var byAddress = await ReadByAddressAsync(connectionString!, session.Name, string.Empty, ct);
                var byJoin = await ReadByJoinAsync(connectionString!, session.Name, ct);

                Assert.NotNull(byAddress.CounterData);
                Assert.Contains(session.OwnEvent, byAddress.CounterData, StringComparison.Ordinal);
                Assert.DoesNotContain(session.OtherEvent, byAddress.CounterData, StringComparison.Ordinal);
                Assert.NotNull(byAddress.RingData);
                Assert.DoesNotContain(session.OtherEvent, byAddress.RingData, StringComparison.Ordinal);
                Assert.Equal(byJoin.Address, byAddress.Address);
                Assert.Equal(byJoin.CounterData, byAddress.CounterData);
            }
        }
        finally
        {
            await ExecuteAsync(connectionString!, DropIfExistsSql(sessionA) + DropIfExistsSql(sessionB), CancellationToken.None);
        }
    }

    /// <summary>Field-by-field equality, so the two byte[] addresses compare by content.</summary>
    private sealed class TargetReadComparer : IEqualityComparer<TargetRead>
    {
        public static readonly TargetReadComparer Instance = new();

        public bool Equals(TargetRead? x, TargetRead? y)
            => x is not null && y is not null
               && ((x.Address is null && y.Address is null) || (x.Address is not null && y.Address is not null && x.Address.SequenceEqual(y.Address)))
               && x.Name == y.Name
               && x.ExecutionCount == y.ExecutionCount
               && x.CounterData == y.CounterData
               && x.RingData == y.RingData;

        public int GetHashCode(TargetRead obj) => obj.Name?.GetHashCode() ?? 0;
    }

    [Fact]
    public async Task ASessionThatIsNotRunning_ResolvesToNothingAndEveryTargetReadsNull()
    {
        var connectionString = ConnectionString("pm-9517-missing-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        var read = await ReadByAddressAsync(connectionString!, OwnName(AlwaysOnXeSessionKind.Deadlock), string.Empty, ct);

        Assert.Null(read.Address);
        Assert.Null(read.Name);
        Assert.Null(read.ExecutionCount);
        Assert.Null(read.CounterData);
        Assert.Null(read.RingData);
    }

    [Fact]
    public async Task ASessionDroppedBetweenTheTwoSteps_ReadsNoRowAndRaisesNothing()
    {
        var connectionString = ConnectionString("pm-9517-dropped-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var sessionName = OwnName(AlwaysOnXeSessionKind.BlockedProcess);

        try
        {
            await CreateSessionAAsync(connectionString!, sessionName, ct);
            await FireAsync(connectionString, ApplicationA, ct);

            var read = await ReadByAddressAsync(connectionString!, sessionName, "DROP EVENT SESSION [" + sessionName + "] ON SERVER;", ct);

            Assert.NotNull(read.Address);
            Assert.Equal(sessionName, read.Name);
            Assert.Null(read.ExecutionCount);
            Assert.Null(read.CounterData);
            Assert.Null(read.RingData);
        }
        finally
        {
            await ExecuteAsync(connectionString!, DropIfExistsSql(sessionName), CancellationToken.None);
        }
    }

    [Fact]
    public async Task ASessionDroppedAndCreatedAgainBetweenTheTwoSteps_RaisesNothingAndNeverReadsAnotherSessionsTargets()
    {
        var connectionString = ConnectionString("pm-9517-recreated-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var sessionName = OwnName(AlwaysOnXeSessionKind.BlockedProcess);
        var otherName = OwnName(AlwaysOnXeSessionKind.Deadlock);

        try
        {
            await CreateSessionAAsync(connectionString!, sessionName, ct);
            await CreateSessionBAsync(connectionString!, otherName, ct);
            await FireAsync(connectionString, ApplicationA, ct);
            await FireAsync(connectionString, ApplicationB, ct);

            var read = await ReadByAddressAsync(
                connectionString!,
                sessionName,
                "DROP EVENT SESSION [" + sessionName + "] ON SERVER;\n" + CreateCountingSql(sessionName, "sqlserver." + EventA, ApplicationA),
                ct);

            /* The engine may hand the new session the old address, so the read finds either no row or the recreated session's own
               targets, which have counted nothing yet. It never raises, and it never returns the other running session's counter
               (which does hold its fired event). */
            Assert.NotNull(read.Address);
            Assert.DoesNotContain(EventB, read.CounterData ?? string.Empty, StringComparison.Ordinal);
            Assert.DoesNotContain(EventA, read.CounterData ?? string.Empty, StringComparison.Ordinal);
            Assert.DoesNotContain(EventB, read.RingData ?? string.Empty, StringComparison.Ordinal);

            var other = await ReadByAddressAsync(connectionString!, otherName, string.Empty, ct);
            Assert.Contains(EventB, other.CounterData, StringComparison.Ordinal);
        }
        finally
        {
            await ExecuteAsync(connectionString!, DropIfExistsSql(sessionName) + DropIfExistsSql(otherName), CancellationToken.None);
        }
    }

    private static CollectorContext MakeContext(string sessionName)
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = CollectorContext.NoState,
            AlwaysOnSessionName = sessionName,
            LongQuerySessionName = sessionName,
            Watermark = DateTime.UtcNow.AddMinutes(-10),
        };

    private static async Task<(List<TRow> Rows, CollectorContext Context)> RunCollectorAsync<TRow>(
        string connectionString, CollectorDefinitionBase<TRow> collector, string sessionName, CancellationToken ct)
    {
        var context = MakeContext(sessionName);
        var query = collector.BuildQuery(context);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using DbDataReader reader = await command.ExecuteReaderAsync(ct);
        var rows = await collector.ReadAsync(reader, context, ct);
        return (rows, context);
    }

    [Fact]
    public async Task TheDeadlockBlockedProcessAndLongQueryBatches_AgainstASessionThatIsNotRunning_ReadZeroRowsAndRaiseNothing()
    {
        var connectionString = ConnectionString("pm-9517-batches-missing-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        var (deadlocks, deadlocksContext) = await RunCollectorAsync(
            connectionString!, DeadlocksCollector.Instance, OwnName(AlwaysOnXeSessionKind.Deadlock), ct);
        Assert.Empty(deadlocks);
        /* A counter that read NULL shreds (the gate treats it as "unknown"), so the run is never reported as gated. */
        Assert.Equal(0, deadlocksContext.Measurements.Single(m => m.Label == DeadlocksCollector.ShredGatedMeasurement).Value);

        var (blocked, blockedContext) = await RunCollectorAsync(
            connectionString!, BlockedProcessReportCollector.Instance, OwnName(AlwaysOnXeSessionKind.BlockedProcess), ct);
        Assert.Empty(blocked);
        Assert.Equal(0, blockedContext.Measurements.Single(m => m.Label == BlockedProcessReportCollector.ShredGatedMeasurement).Value);

        var longQueryName = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, Guid.NewGuid().ToString("N")[..8]);
        var (longQuery, _) = await RunCollectorAsync(connectionString!, LongQueryCompletionsCollector.Instance, longQueryName, ct);
        Assert.Empty(longQuery);
    }

    [Fact]
    public async Task TheAttentionBatch_AgainstASessionThatIsNotRunning_StillRaisesTheSessionMissingError()
    {
        var connectionString = ConnectionString("pm-9517-attention-missing-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var sessionName = OwnName(AlwaysOnXeSessionKind.Attention);

        var ex = await Assert.ThrowsAsync<SqlException>(
            () => RunCollectorAsync(connectionString!, AttentionCollector.Instance, sessionName, ct));

        Assert.Equal(50000, ex.Number);
        Assert.Contains("XE session", ex.Message, StringComparison.Ordinal);
        Assert.Contains("is not running", ex.Message, StringComparison.Ordinal);
    }
}
