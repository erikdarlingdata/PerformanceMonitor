/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643: the schema-changes session against a real SQL Server. Gated on <c>DARLING_TEST_SQL</c> (host; optional
/// <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c>); <c>DARLING_TEST_SQL_READONLY=1</c> skips the class because it
/// creates a session and a database. Each test uses its own install id, so it never touches another install's session.
/// </summary>
public sealed class SchemaChangesXeSessionLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live schema session tests; DARLING_TEST_SQL_READONLY=1 skips them.";

    private static string? ConnectionString(string database = "master")
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
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "Darling.Tests schema session",
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

    private static string NewName() => SchemaChangesXeSession.NameFor(Guid.NewGuid().ToString("N")[..8]);

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Ensure_CreatesAndStarts_ThenProbeShowsRunningWithCreateTime()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var name = NewName();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        try
        {
            var state = await DarlingSchemaXeSession.EnsureAsync(connection, name, databaseScoped: false, logger: null, CancellationToken.None);

            Assert.True(state.Running);
            Assert.NotNull(state.CreateTimeUtc);
            Assert.Null(state.FaultMessage);
            Assert.False(state.FaultIsPermission);

            var again = await DarlingSchemaXeSession.EnsureAsync(connection, name, databaseScoped: false, logger: null, CancellationToken.None);
            Assert.True(again.Running);
            Assert.Equal(state.CreateTimeUtc, again.CreateTimeUtc);
        }
        finally
        {
            await DarlingSchemaXeSession.DropAsync(connection, name, databaseScoped: false, CancellationToken.None);
        }
    }

    [Fact]
    public async Task Ensure_AfterStop_StartsAndCreateTimeChanges()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var name = NewName();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        try
        {
            var first = await DarlingSchemaXeSession.EnsureAsync(connection, name, databaseScoped: false, logger: null, CancellationToken.None);
            await ExecuteAsync(connectionString!, $"ALTER EVENT SESSION [{name}] ON SERVER STATE = STOP;");
            await Task.Delay(1100);

            var second = await DarlingSchemaXeSession.EnsureAsync(connection, name, databaseScoped: false, logger: null, CancellationToken.None);

            Assert.True(second.Running);
            Assert.True(second.CreateTimeUtc > first.CreateTimeUtc, "a restart must move the session's create time, which is how the pass detects a gap");
        }
        finally
        {
            await DarlingSchemaXeSession.DropAsync(connection, name, databaseScoped: false, CancellationToken.None);
        }
    }

    [Fact]
    public async Task Drop_RemovesIt()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var name = NewName();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await DarlingSchemaXeSession.EnsureAsync(connection, name, databaseScoped: false, logger: null, CancellationToken.None);

        await DarlingSchemaXeSession.DropAsync(connection, name, databaseScoped: false, CancellationToken.None);
        await DarlingSchemaXeSession.DropAsync(connection, name, databaseScoped: false, CancellationToken.None);

        await using var probe = new SqlCommand("SELECT COUNT(*) FROM sys.server_event_sessions WHERE name = @n;", connection);
        probe.Parameters.AddWithValue("@n", name);
        Assert.Equal(0, (int)(await probe.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Session_RecordsCommittedTableDdl_AndIgnoresRollbackAndAutoStats()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var name = NewName();
        var database = "SchemaXeTest_" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connectionString!, $"CREATE DATABASE [{database}];");
            await DarlingSchemaXeSession.EnsureAsync(connection, name, databaseScoped: false, logger: null, CancellationToken.None);

            var inDatabase = ConnectionString(database)!;
            await ExecuteAsync(inDatabase, "CREATE TABLE dbo.Orders (Id int NOT NULL, Total int NULL);");
            await ExecuteAsync(inDatabase, "BEGIN TRAN; ALTER TABLE dbo.Orders ADD Note int NULL; ROLLBACK;");
            await ExecuteAsync(inDatabase, "SELECT COUNT(*) FROM dbo.Orders WHERE Total = 5;");

            /* MAX_DISPATCH_LATENCY is 5 seconds: the ring buffer fills only after the dispatch. */
            var events = new List<(string Event, int Phase, string Object)>();
            for (var attempt = 0; attempt < 20 && events.Count == 0; attempt++)
            {
                await Task.Delay(1000);
                events = await ReadEventsAsync(connection, name, database);
            }

            Assert.Contains(events, e => e.Event == "object_created" && e.Phase == 1 && e.Object == "Orders");
            Assert.DoesNotContain(events, e => e.Event == "object_altered");
            Assert.DoesNotContain(events, e => e.Object.StartsWith("_WA_Sys", StringComparison.Ordinal));
            Assert.All(events, e => Assert.Equal(1, e.Phase));
        }
        finally
        {
            await DarlingSchemaXeSession.DropAsync(connection, name, databaseScoped: false, CancellationToken.None);
            await ExecuteAsync(connectionString!, $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END;");
        }
    }

    private static async Task<List<(string Event, int Phase, string Object)>> ReadEventsAsync(SqlConnection connection, string name, string database)
    {
        await using var command = new SqlCommand(@"
SELECT
    event_name = e.value('@name', 'nvarchar(128)'),
    ddl_phase = e.value('(data[@name=""ddl_phase""]/value)[1]', 'int'),
    object_name = e.value('(data[@name=""object_name""]/value)[1]', 'nvarchar(256)')
FROM
(
    SELECT
        x = CONVERT(xml, t.target_data)
    FROM sys.dm_xe_sessions AS s
    JOIN sys.dm_xe_session_targets AS t
      ON t.event_session_address = s.address
    WHERE s.name = @n
      AND t.target_name = N'ring_buffer'
) AS r
CROSS APPLY r.x.nodes('/RingBufferTarget/event') AS n(e)
WHERE e.value('(action[@name=""database_name""]/value)[1]', 'nvarchar(128)') = @d;", connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue("@n", name);
        command.Parameters.AddWithValue("@d", database);
        var events = new List<(string, int, string)>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            events.Add((reader.GetString(0), reader.GetInt32(1), reader.IsDBNull(2) ? string.Empty : reader.GetString(2)));
        }

        return events;
    }
}
