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
/// #6254: the three statements the replica routing generates, run against a REAL SQL Server (the instance is not in an
/// availability group), each through the matching reader. The statements nest <c>sp_executesql</c> strings and double
/// their quotes, so only an engine can show that they compile, return the shape the reader expects and carry the right
/// facts: a scratch database with Query Store ON and a 60 s flush must come back with that flush from the primary's
/// third result set and from the replica enumeration, and must be absent from the redo read (it is in no group). With the
/// setting off the primary enumeration must stay the two result sets it always was. Needs <c>DARLING_TEST_SQL</c> with
/// <c>DARLING_TEST_SQL_USER</c> / <c>DARLING_TEST_SQL_PASSWORD</c> for a login that may create a database; skips without.
/// </summary>
[Collection("remediation-live-sql")]
public sealed class QueryStoreReplicaReadsLiveSqlTests
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
    public async Task ScratchDatabase_FlowsThroughThePrimaryFacts_TheReplicaEnumeration_AndIsAbsentFromTheRedoRead()
    {
        Assert.SkipWhen(Connection("master") is null, "Set DARLING_TEST_SQL, DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD (a login that can create a database) to run the Query Store replica read test.");

        var ct = TestContext.Current.CancellationToken;
        var database = "pm_6254_" + Guid.NewGuid().ToString("N")[..12];
        await ExecuteAsync("master", $"CREATE DATABASE [{database}]; ALTER DATABASE [{database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, DATA_FLUSH_INTERVAL_SECONDS = 60, INTERVAL_LENGTH_MINUTES = 1);", ct);
        try
        {
            // Primary enumeration with a replica set: the item list, no probe failure, and the third result set.
            var withFacts = await RunAsync(
                QueryStoreCollector.Instance.BuildEnumerationQuery(Context(true))!,
                async reader =>
                {
                    var enumeration = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, ct);
                    var facts = await QueryStoreCollector.ReadPrimaryFactsAsync(reader, ct);
                    return (enumeration, facts);
                },
                ct);
            Assert.Contains(database, withFacts.enumeration.Items);
            Assert.DoesNotContain(withFacts.enumeration.ProbeFailures, f => f.Item == database);
            Assert.Equal(60, withFacts.facts.Single(f => f.Name == database).FlushIntervalSeconds);

            // The same enumeration with the setting empty is today's two result sets: the item list is the same, no facts.
            var withoutFacts = await RunAsync(
                QueryStoreCollector.Instance.BuildEnumerationQuery(Context(false))!,
                async reader =>
                {
                    var enumeration = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, ct);
                    var facts = await QueryStoreCollector.ReadPrimaryFactsAsync(reader, ct);
                    return (enumeration, facts);
                },
                ct);
            Assert.Contains(database, withoutFacts.enumeration.Items);
            Assert.Empty(withoutFacts.facts);

            // Replica enumeration: this instance is read-write, so it reports the database as such, with its flush.
            var replica = await RunAsync(
                QueryStoreCollector.Instance.BuildReplicaEnumerationQuery(Context(true))!,
                reader => QueryStoreCollector.ReadReplicaEnumerationAsync(reader, ct),
                ct);
            var state = replica.Databases.Single(d => d.Name == database);
            Assert.Equal(2, state.ActualState);
            Assert.Equal(60, state.FlushIntervalSeconds);
            Assert.DoesNotContain(replica.ProbeFailures, f => f.Item == database);

            // Redo read: a database in no availability group has no row, so the routing leaves it out of read_source.
            var redo = await RunAsync(
                QueryStoreCollector.BuildRedoStateQuery(),
                reader => QueryStoreCollector.ReadRedoStatesAsync(reader, ct),
                ct);
            Assert.DoesNotContain(database, redo.Keys);
        }
        finally
        {
            await ExecuteAsync("master", $"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}];", CancellationToken.None);
        }
    }
}
