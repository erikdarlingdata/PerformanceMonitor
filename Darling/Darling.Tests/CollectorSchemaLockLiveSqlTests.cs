/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9526: index_object_stats, object_inventory and identity_capacity read one database at a time, and a DDL holding a schema
/// modification lock (Sch-M) on one user object used to stall them until the DDL committed or the command timeout fired (300 s and
/// 120 s). Each one's real per-database statement runs here against a throwaway database while a second connection holds Sch-M on a
/// table, a view and a procedure in an open transaction. Then the DDL commits and a normal run collects the rows.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL
/// auth, as the other live SQL classes read them), so it skips where the runner has no SQL Server. It creates its own database, named
/// <c>pm_9526_{8 hex}</c>, touches nothing else, and drops it in a finally.</para>
/// </summary>
[Collection("timing")]
public sealed class CollectorSchemaLockLiveSqlTests
{
    /// <summary>The longest a collector may take to give up on a blocked database. The lock timeout is 5 s.</summary>
    private static readonly TimeSpan YieldBound = TimeSpan.FromSeconds(10);

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

    private static async Task ExecuteAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> MajorVersionAsync(SqlConnection connection, CancellationToken ct)
    {
        await using var command = new SqlCommand("SELECT CONVERT(integer, SERVERPROPERTY(N'ProductMajorVersion'));", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>What one collector's per-database statement did: its rows, the fault the host would classify it as (null when it
    /// read), and how long it took.</summary>
    private sealed record Outcome<TRow>(List<TRow> Rows, CollectorTargetFault? Fault, TimeSpan Elapsed, string? Message);

    /// <summary>Runs the collector's real per-database statement and its real read, the way the on-prem host does. The 20 s command
    /// timeout stands in for the collector's own, so a statement that waits for the DDL shows up as a command timeout.</summary>
    private static async Task<Outcome<TRow>> RunAsync<TRow>(
        CollectorDefinitionBase<TRow> definition, string connectionString, string database, int majorVersion, CancellationToken ct)
        where TRow : class
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { SqlMajorVersion = majorVersion },
        };

        var watch = Stopwatch.StartNew();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        var rows = new List<TRow>();
        try
        {
            await using var command = SqlServerTargetProvider.Instance.CreateCommand(
                definition.BuildPerItemQuery(database, context), connection, 20);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await definition.ReadItemAsync(database, reader, rows, context, ct);
            return new Outcome<TRow>(rows, null, watch.Elapsed, null);
        }
        catch (SqlException ex)
        {
            var fault = SqlServerTargetProvider.Instance.Classify(ex, CollectorCatalog.YieldsOnLockTimeout(definition.Name));
            return new Outcome<TRow>(new List<TRow>(), fault, watch.Elapsed, ex.Message);
        }
    }

    private static async Task DropDatabaseAsync(string connectionString, string database)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await ExecuteAsync(
            connection,
            $@"IF DB_ID(N'{database}') IS NOT NULL
BEGIN
    ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{database}];
END;",
            CancellationToken.None);
    }

    [Fact]
    public async Task ADdlHoldingSchemaLocks_MakesTheThreeCollectorsYieldOrReadWithinSeconds_ThenACommittedDdlCollectsNormally()
    {
        var connectionString = ConnectionString("pm-9526-lock-live-test");
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live schema-lock collector test.");

        var ct = TestContext.Current.CancellationToken;
        var database = "pm_9526_" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        SqlConnection? blocker = null;
        try
        {
            await using var admin = new SqlConnection(connectionString);
            await admin.OpenAsync(ct);
            var major = await MajorVersionAsync(admin, ct);
            await ExecuteAsync(admin, $"CREATE DATABASE [{database}];", ct);

            /* t1 is the table the DDL holds, v1 and p1 the view and procedure it holds, t2 a table near the end of its identity range. */
            foreach (var statement in new[]
            {
                "CREATE TABLE dbo.t1 (id integer IDENTITY(1,1) NOT NULL PRIMARY KEY, v integer NULL);",
                "INSERT dbo.t1 (v) VALUES (1), (2), (3);",
                "CREATE VIEW dbo.v1 AS SELECT id, v FROM dbo.t1;",
                "CREATE PROCEDURE dbo.p1 AS SELECT 1;",
                "CREATE TABLE dbo.t2 (id tinyint IDENTITY(250,1) NOT NULL PRIMARY KEY);",
                "INSERT dbo.t2 DEFAULT VALUES;",
            })
            {
                await ExecuteAsync(admin, $"USE [{database}]; EXECUTE (N'{statement.Replace("'", "''", StringComparison.Ordinal)}');", ct);
            }

            /* The deployment's DDL: Sch-M on the table, the view and the procedure, held in an open transaction. */
            blocker = new SqlConnection(connectionString!);
            await blocker.OpenAsync(ct);
            await ExecuteAsync(blocker, $"USE [{database}]; BEGIN TRANSACTION;", ct);
            await ExecuteAsync(blocker, "ALTER TABLE dbo.t1 ADD c integer NULL;", ct);
            await ExecuteAsync(blocker, "EXECUTE (N'ALTER VIEW dbo.v1 AS SELECT id, v FROM dbo.t1');", ct);
            await ExecuteAsync(blocker, "EXECUTE (N'ALTER PROCEDURE dbo.p1 AS SELECT 2');", ct);

            var blockedIndexStats = await RunAsync(IndexObjectStatsCollector.Instance, connectionString!, database, major, ct);
            var blockedInventory = await RunAsync(ObjectInventoryCollector.Instance, connectionString!, database, major, ct);
            var blockedIdentity = await RunAsync(IdentityCapacityCollector.Instance, connectionString!, database, major, ct);

            /* index_object_stats and identity_capacity read something that waits behind Sch-M: they give up at the 5 s lock timeout,
               and the host classifies that as a yield, never an error. */
            Assert.True(
                blockedIndexStats.Fault == CollectorTargetFault.LockTimeoutYield && blockedIndexStats.Elapsed < YieldBound,
                $"index_object_stats: fault {blockedIndexStats.Fault}, {blockedIndexStats.Elapsed.TotalSeconds:F1}s, {blockedIndexStats.Message}");
            Assert.True(
                blockedIdentity.Fault == CollectorTargetFault.LockTimeoutYield && blockedIdentity.Elapsed < YieldBound,
                $"identity_capacity: fault {blockedIdentity.Fault}, {blockedIdentity.Elapsed.TotalSeconds:F1}s, {blockedIdentity.Message}");

            /* object_inventory now reads catalog columns, which do not wait behind Sch-M: it answers at once with its rows and no error. */
            Assert.True(
                blockedInventory.Fault is null && blockedInventory.Elapsed < YieldBound,
                $"object_inventory: fault {blockedInventory.Fault}, {blockedInventory.Elapsed.TotalSeconds:F1}s, {blockedInventory.Message}");
            Assert.Contains(blockedInventory.Rows, r => r.ObjectName == "p1");
            Assert.Contains(blockedInventory.Rows, r => r.ObjectName == "v1");

            /* The DDL commits: the same three statements now collect normally. */
            await ExecuteAsync(blocker, "COMMIT TRANSACTION;", ct);

            var indexStats = await RunAsync(IndexObjectStatsCollector.Instance, connectionString!, database, major, ct);
            var inventory = await RunAsync(ObjectInventoryCollector.Instance, connectionString!, database, major, ct);
            var identity = await RunAsync(IdentityCapacityCollector.Instance, connectionString!, database, major, ct);

            Assert.True(indexStats.Fault is null, $"index_object_stats after the commit: {indexStats.Fault} {indexStats.Message}");
            Assert.Contains(indexStats.Rows, r => r.TableName == "t1");
            Assert.Contains(indexStats.Rows, r => r.TableName == "t2");
            Assert.All(indexStats.Rows.Where(r => r.TableName is "t1" or "t2"), r => Assert.False(r.IsMemoryOptimized));

            Assert.True(inventory.Fault is null, $"object_inventory after the commit: {inventory.Fault} {inventory.Message}");
            Assert.Contains(inventory.Rows, r => r.ObjectName == "p1" && !r.IsNativelyCompiled);
            Assert.Contains(inventory.Rows, r => r.ObjectName == "v1");

            Assert.True(identity.Fault is null, $"identity_capacity after the commit: {identity.Fault} {identity.Message}");
            Assert.Contains(identity.Rows, r => r.TableName == "t2");

            bodySucceeded = true;
        }
        finally
        {
            if (blocker is not null)
            {
                await blocker.DisposeAsync();
            }

            try
            {
                await DropDatabaseAsync(connectionString!, database);
            }
            catch (SqlException) when (!bodySucceeded)
            {
                /* The body already failed: keep its failure as the one that reports. */
            }
        }
    }

    [Fact]
    public async Task AMemoryOptimizedTable_StillReadsAsMemoryOptimized_AndANativelyCompiledProcedureAsNativelyCompiled()
    {
        var connectionString = ConnectionString("pm-9526-memory-optimized-live-test");
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live memory-optimized collector test.");

        var ct = TestContext.Current.CancellationToken;
        var database = "pm_9526_" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        try
        {
            await using var admin = new SqlConnection(connectionString);
            await admin.OpenAsync(ct);
            var major = await MajorVersionAsync(admin, ct);
            await ExecuteAsync(admin, $"CREATE DATABASE [{database}];", ct);

            var created = true;
            try
            {
                await ExecuteAsync(admin, $"ALTER DATABASE [{database}] ADD FILEGROUP mo CONTAINS MEMORY_OPTIMIZED_DATA;", ct);
                await ExecuteAsync(
                    admin,
                    $@"DECLARE @path nvarchar(512) = CONVERT(nvarchar(512), SERVERPROPERTY(N'InstanceDefaultDataPath')) + N'{database}_mo';
DECLARE @sql nvarchar(max) = N'ALTER DATABASE [{database}] ADD FILE (NAME = N''{database}_mo'', FILENAME = N''' + @path + N''') TO FILEGROUP mo;';
EXECUTE (@sql);",
                    ct);
                foreach (var statement in new[]
                {
                    "CREATE TABLE dbo.m1 (id integer NOT NULL PRIMARY KEY NONCLUSTERED) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
                    "CREATE TABLE dbo.d1 (id integer NOT NULL PRIMARY KEY);",
                    "CREATE PROCEDURE dbo.n1 WITH NATIVE_COMPILATION, SCHEMABINDING AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english') SELECT id FROM dbo.m1; END;",
                    "CREATE PROCEDURE dbo.p1 AS SELECT 1;",
                })
                {
                    await ExecuteAsync(admin, $"USE [{database}]; EXECUTE (N'{statement.Replace("'", "''", StringComparison.Ordinal)}');", ct);
                }
            }
            catch (SqlException)
            {
                created = false;
            }

            Assert.SkipUnless(created, "This SQL Server cannot create a memory-optimized filegroup, table and natively compiled procedure here.");

            var indexStats = await RunAsync(IndexObjectStatsCollector.Instance, connectionString!, database, major, ct);
            var inventory = await RunAsync(ObjectInventoryCollector.Instance, connectionString!, database, major, ct);

            Assert.True(indexStats.Fault is null, $"index_object_stats: {indexStats.Fault} {indexStats.Message}");
            Assert.All(indexStats.Rows.Where(r => r.TableName == "m1"), r => Assert.True(r.IsMemoryOptimized));
            Assert.Contains(indexStats.Rows, r => r.TableName == "m1");
            Assert.All(indexStats.Rows.Where(r => r.TableName == "d1"), r => Assert.False(r.IsMemoryOptimized));
            Assert.Contains(indexStats.Rows, r => r.TableName == "d1");

            Assert.True(inventory.Fault is null, $"object_inventory: {inventory.Fault} {inventory.Message}");
            Assert.Contains(inventory.Rows, r => r.ObjectName == "n1" && r.IsNativelyCompiled);
            Assert.Contains(inventory.Rows, r => r.ObjectName == "p1" && !r.IsNativelyCompiled);

            bodySucceeded = true;
        }
        finally
        {
            try
            {
                await DropDatabaseAsync(connectionString!, database);
            }
            catch (SqlException) when (!bodySucceeded)
            {
                /* The body already failed: keep its failure as the one that reports. */
            }
        }
    }
}
