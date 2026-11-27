/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9813: the Query Store enumeration probes, run against a REAL SQL Server, for a database whose name holds a character
/// outside that database's code page. The probes build an inner statement that returns the database name as a literal. A
/// literal without the N prefix is converted to the code page, so U+200C came back as '?', and the next step ran against a
/// database that does not exist (error 911) on every cycle. Each probe must hand back the name unchanged.
/// The scratch database is created on a Latin1 collation so U+200C is outside its code page whatever the server default is.
/// Needs <c>DARLING_TEST_SQL</c> with <c>DARLING_TEST_SQL_USER</c> / <c>DARLING_TEST_SQL_PASSWORD</c> for a login that may
/// create a database; skips without.
/// </summary>
[Collection("remediation-live-sql")]
public sealed class QueryStoreUnicodeDatabaseNameLiveSqlTests
{
    private static SqlConnectionStringBuilder? Connection(string database)
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        if (string.IsNullOrEmpty(host) || string.IsNullOrEmpty(user))
        {
            return null;
        }

        return new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = database,
            UserID = user,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            Pooling = false,
        };
    }

    private static async Task ExecuteAsync(string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Connection(database)!.ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static CollectorContext Context(bool replicaConfigured) => new()
    {
        ServerId = 1,
        ServerName = "live-sql",
        CollectionTime = DateTime.UtcNow,
        Deltas = null!,
        Target = new CollectorTargetInfo { SqlMajorVersion = 15 },
        QueryStoreReplicaConfigured = replicaConfigured,
    };

    private static async Task<T> RunAsync<T>(CollectorQuery query, Func<System.Data.Common.DbDataReader, Task<T>> read, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Connection("master")!.ConnectionString);
        await connection.OpenAsync(ct);
        /* The shipped command builder, so the text and the parameter bindings are the host's own. */
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 120);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await read(reader);
    }

    [Fact]
    public async Task DatabaseNameOutsideItsCodePage_ComesBackUnchanged_FromEveryQueryStoreEnumerationProbe()
    {
        Assert.SkipWhen(Connection("master") is null, "Set DARLING_TEST_SQL, DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD (a login that can create a database) to run the Query Store Unicode database name test.");

        var ct = TestContext.Current.CancellationToken;
        // U+200C is a zero-width non-joiner: not in code page 1252, so a non-Unicode literal turns it into '?'.
        var database = "zw‌db_9813_" + Guid.NewGuid().ToString("N")[..12];
        await ExecuteAsync("master", $"CREATE DATABASE [{database}] COLLATE Latin1_General_100_CI_AS; ALTER DATABASE [{database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, DATA_FLUSH_INTERVAL_SECONDS = 60, INTERVAL_LENGTH_MINUTES = 1);", ct);
        try
        {
            // Primary enumeration with a replica set: the item list and the facts read both carry the name.
            var primary = await RunAsync(
                QueryStoreCollector.Instance.BuildEnumerationQuery(Context(true))!,
                async reader =>
                {
                    var enumeration = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, ct);
                    var facts = await QueryStoreCollector.ReadPrimaryFactsAsync(reader, ct);
                    return (enumeration, facts);
                },
                ct);
            Assert.Contains(database, primary.enumeration.Items);
            Assert.DoesNotContain(primary.enumeration.ProbeFailures, f => f.Item == database);
            Assert.Equal(60, primary.facts.Single(f => f.Name == database).FlushIntervalSeconds);

            // Replica enumeration: the same name in the state list.
            var replica = await RunAsync(
                QueryStoreCollector.Instance.BuildReplicaEnumerationQuery(Context(true))!,
                reader => QueryStoreCollector.ReadReplicaEnumerationAsync(reader, ct),
                ct);
            var state = replica.Databases.Single(d => d.Name == database);
            Assert.Equal(2, state.ActualState);
            Assert.Equal(60, state.FlushIntervalSeconds);
        }
        finally
        {
            await ExecuteAsync("master", $"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}];", CancellationToken.None);
        }
    }
}
