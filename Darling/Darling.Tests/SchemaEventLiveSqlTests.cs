/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643: the event read and the server probe against a real SQL Server. Gated on <c>DARLING_TEST_SQL</c> (host; optional
/// <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c>); <c>DARLING_TEST_SQL_READONLY=1</c> skips the class because it
/// creates a session and a database. Each test uses its own install id and database, so it never touches another's.
/// </summary>
public sealed class SchemaEventLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live schema event tests; DARLING_TEST_SQL_READONLY=1 skips them.";

    private const string App = "Darling.Tests schema events";

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
            ApplicationName = App,
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

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<SchemaEventBatch> ReadEventsAsync(SqlConnection connection, string session, DateTime cutoff)
    {
        await using var command = new SqlCommand(SchemaEventQuery.Build(session, databaseScoped: false), connection) { CommandTimeout = 60 };
        command.Parameters.Add(new SqlParameter("@cutoff_time", System.Data.SqlDbType.DateTime2) { Value = cutoff });
        await using var reader = await command.ExecuteReaderAsync();
        return await SchemaEventReader.ReadAsync(reader, CancellationToken.None);
    }

    /* MAX_DISPATCH_LATENCY is five seconds, so an event reaches the ring buffer up to five seconds after it commits. */
    private static async Task<SchemaEventBatch> ReadWhenDispatchedAsync(SqlConnection connection, string session, DateTime cutoff, int expected)
    {
        var batch = await ReadEventsAsync(connection, session, cutoff);
        for (var attempt = 0; attempt < 20 && batch.Events.Count < expected; attempt++)
        {
            await Task.Delay(1000);
            batch = await ReadEventsAsync(connection, session, cutoff);
        }

        return batch;
    }

    [Fact]
    public async Task EventQuery_ReadsCommittedDdlAfterCutoff_WithSequenceAndAttribution()
    {
        var master = ConnectionString();
        Assert.SkipWhen(master is null, SkipMessage);
        var name = SchemaChangesXeSession.NameFor(Guid.NewGuid().ToString("N")[..8]);
        var database = "SchemaEvents_" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new SqlConnection(master);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(master!, $"CREATE DATABASE [{database}];");
            var state = await DarlingSchemaXeSession.EnsureAsync(connection, name, databaseScoped: false, logger: null, CancellationToken.None);
            Assert.True(state.Running, state.FaultMessage);

            var before = DateTime.UtcNow.AddSeconds(-5);
            await ExecuteAsync(ConnectionString(database)!, @"
CREATE TABLE dbo.Orders (OrderId int NOT NULL CONSTRAINT PK_Orders PRIMARY KEY, Customer nvarchar(50) NULL);
EXEC(N'CREATE INDEX IX_Orders_Customer ON dbo.Orders (Customer);');
EXEC(N'CREATE VIEW dbo.OrderCustomers AS SELECT Customer FROM dbo.Orders;');
EXEC(N'CREATE TRIGGER dbo.TR_Orders ON dbo.Orders AFTER INSERT AS SELECT 1;');
ALTER TABLE dbo.Orders ADD Total decimal(10, 2) NULL;
EXEC sp_rename N'dbo.Orders.Total', N'Amount', N'COLUMN';
DROP VIEW dbo.OrderCustomers;
EXEC(N'CREATE SCHEMA Staging;');
CREATE TABLE dbo.Moved (MovedId int NOT NULL);
ALTER SCHEMA Staging TRANSFER dbo.Moved;");

            var batch = await ReadWhenDispatchedAsync(connection, name, before, expected: 11);
            var events = batch.Events.Where(e => e.DatabaseName == database).ToList();
            var described = string.Join(" | ", batch.Events.Select(e =>
                $"{e.Sequence}:{e.EventName}:{e.DatabaseName}:type={e.ObjectType}:obj={e.ObjectId}:idx={e.IndexId}:name={e.ObjectName}:rel={e.RelatedObjectId}"));

            AssertSameSession(state.CreateTimeUtc, batch.SessionCreateTimeUtc);
            Assert.False(batch.XmlTruncated);
            Assert.True(batch.TotalEventsProcessed >= events.Count, described);

            var tableId = Convert.ToInt32(await ScalarAsync(ConnectionString(database)!, "SELECT OBJECT_ID(N'dbo.Orders');"));
            Assert.Contains(events, e => e.EventName == "object_created" && e.ObjectType == 8277 && e.ObjectId == tableId && e.ObjectName == "Orders");
            Assert.Contains(events, e => e.EventName == "object_created" && e.ObjectType == 22601 && e.ObjectId == tableId && e.IndexId == 2);
            Assert.Contains(events, e => e.EventName == "object_created" && e.ObjectType == 8278 && e.ObjectName == "OrderCustomers");
            Assert.Contains(events, e => e.EventName == "object_deleted" && e.ObjectType == 8278 && e.ObjectName == "OrderCustomers");
            Assert.Contains(events, e => e.EventName == "object_altered" && e.ObjectType == 8277 && e.ObjectId == tableId);
            Assert.Contains(batch.Events, e => e.EventName == "module_end" && e.ObjectName == "sp_rename");
            Assert.Contains(events, e => e.EventName == "object_altered" && e.ObjectType == 17235);

            /* A trigger event names the trigger itself (type 21076) and carries no parent table: related_object_id is 0 on it. */
            var triggerId = Convert.ToInt32(await ScalarAsync(ConnectionString(database)!, "SELECT OBJECT_ID(N'dbo.TR_Orders');"));
            Assert.Contains(events, e => e.EventName == "object_created" && e.ObjectType == 21076 && e.ObjectId == triggerId && e.RelatedObjectId is null);

            Assert.All(events, e => Assert.Equal(App, e.App));
            Assert.All(events, e => Assert.False(string.IsNullOrEmpty(e.Login)));
            Assert.All(events, e => Assert.False(string.IsNullOrEmpty(e.Host)));
            Assert.All(events, e => Assert.True(e.TimeUtc > before, $"{e.TimeUtc:o} is not after {before:o}: the event time must be UTC"));

            /* The design left open whether event_sequence runs without gaps across the events that pass the predicates. */
            var sequences = batch.Events.Select(e => e.Sequence).ToList();
            Assert.Equal(sequences.Order().ToList(), sequences);
            for (var i = 1; i < sequences.Count; i++)
            {
                Assert.True(sequences[i] == sequences[i - 1] + 1, $"event_sequence is not consecutive: {described}");
            }

            /* The total counts what the buffer took, so with nothing aged out it equals the events read. */
            Assert.Equal(batch.Events.Count, batch.TotalEventsProcessed);

            var after = await ReadEventsAsync(connection, name, DateTime.UtcNow.AddMinutes(5));
            Assert.Empty(after.Events);
            AssertSameSession(state.CreateTimeUtc, after.SessionCreateTimeUtc);
            Assert.Equal(batch.TotalEventsProcessed, after.TotalEventsProcessed);
        }
        finally
        {
            await DarlingSchemaXeSession.DropAsync(connection, name, databaseScoped: false, CancellationToken.None);
            await ExecuteAsync(master!, $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END;");
        }
    }

    [Fact]
    public async Task EventQuery_SessionNotPresent_ReturnsAHeaderWithNoCreateTimeAndNoEvents()
    {
        var master = ConnectionString();
        Assert.SkipWhen(master is null, SkipMessage);
        await using var connection = new SqlConnection(master);
        await connection.OpenAsync();

        var batch = await ReadEventsAsync(connection, SchemaChangesXeSession.NameFor(Guid.NewGuid().ToString("N")[..8]), DateTime.UtcNow.AddMinutes(-1));

        Assert.Null(batch.SessionCreateTimeUtc);
        Assert.Empty(batch.Events);
        Assert.False(batch.XmlTruncated);
    }

    [Fact]
    public async Task ServerProbe_ListsUserDatabasesWithStateAndFork()
    {
        var master = ConnectionString();
        Assert.SkipWhen(master is null, SkipMessage);
        var database = "SchemaProbe_" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new SqlConnection(master);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(master!, $"CREATE DATABASE [{database}];");

            await using var command = new SqlCommand(SchemaServerProbe.BuildSql(), connection) { CommandTimeout = 60 };
            await using var reader = await command.ExecuteReaderAsync();
            var probes = await SchemaServerProbe.ReadAsync(reader, CancellationToken.None);

            var probe = Assert.Single(probes, p => p.Name == database);
            Assert.All(probes, p => Assert.True(p.DatabaseId > 4));
            Assert.Equal("ONLINE", probe.StateDesc);
            Assert.False(probe.IsReadOnly);
            Assert.False(probe.IsInStandby);
            Assert.Null(probe.SourceDatabaseId);
            Assert.True(probe.HasAccess);
            Assert.NotNull(probe.RecoveryForkGuid);
            Assert.Null(probe.AgGroupId);
            Assert.Null(probe.IsPrimaryReplica);
        }
        finally
        {
            await ExecuteAsync(master!, $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END;");
        }
    }

    /* The server reports one session's create time a few milliseconds apart on different reads, so the planner compares within a second. */
    private static void AssertSameSession(DateTime? expected, DateTime? actual)
    {
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        Assert.True((expected.Value - actual.Value).Duration() < TimeSpan.FromSeconds(1), $"{expected:o} and {actual:o} are not the same session");
    }

    private static async Task<object?> ScalarAsync(string connectionString, string sql)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        return await command.ExecuteScalarAsync();
    }
}
