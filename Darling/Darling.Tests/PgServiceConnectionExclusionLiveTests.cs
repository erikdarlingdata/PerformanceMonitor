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
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7428: the lock and session-state collectors leave out the service's own connections by <c>application_name</c>, the way
/// the wait sampler does. Each test holds the same thing on two connections, one named like the service and one named like an
/// application, runs the collector's real query on a third, and checks that only the application's is reported. The query
/// text and <c>ReadAsync</c> are the collector's own, so a query that leaves the exclusion out reports both and fails here.
/// Nothing is stored: the collector reads the server it runs on, so no store schema is involved.
/// </summary>
[Collection("live-postgres")]
public sealed class PgServiceConnectionExclusionLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the service-connection exclusion tests.";

    private static CollectorContext Context(int major) => new()
    {
        ServerId = -7428,
        ServerName = "pm7428",
        CollectionTime = new DateTime(2031, 6, 10, 12, 0, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major, PostgresVersionNum = major * 10000,
        },
    };

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, string applicationName, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { ApplicationName = applicationName, Pooling = false };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> ServerMajorAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT current_setting('server_version_num')::int / 10000", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<List<TRow>> ReadAsync<TRow>(CollectorDefinitionBase<TRow> definition, CollectorContext context, NpgsqlConnection source, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(definition.BuildQuery(context).Text, source);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await definition.ReadAsync(reader, context, ct);
    }

    [Theory]
    [InlineData(PgWaitSamplingCollector.ServiceApplicationName)]
    [InlineData(PgWaitSamplingCollector.ServiceRemediationApplicationName)]
    public async Task LockStats_LeavesOutALockHeldUnderAServiceName_AndKeepsTheSameLockUnderAnotherName(string serviceName)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..12];
        var serviceTable = "pm7428_svc_" + suffix;
        var appTable = "pm7428_app_" + suffix;

        /* A temp table is private to the connection that made it and goes with the connection, so there is nothing to drop. The
           lock is held by the open transaction, and the table name tells the two holders apart in the collector's rows. */
        await using var service = await OpenAsync(connectionString!, serviceName, ct);
        await using var application = await OpenAsync(connectionString!, "pm7428-application", ct);
        await using var collector = await OpenAsync(connectionString!, "pm7428-collector", ct);
        await ExecAsync(service, $"CREATE TEMP TABLE {serviceTable} (id int)", ct);
        await ExecAsync(service, $"BEGIN; LOCK TABLE {serviceTable} IN ROW EXCLUSIVE MODE", ct);
        await ExecAsync(application, $"CREATE TEMP TABLE {appTable} (id int)", ct);
        await ExecAsync(application, $"BEGIN; LOCK TABLE {appTable} IN ROW EXCLUSIVE MODE", ct);

        var rows = await ReadAsync(PgLockStatsCollector.Instance, Context(await ServerMajorAsync(collector, ct)), collector, ct);

        Assert.Contains(rows, row => row.RelationName == appTable);
        Assert.DoesNotContain(rows, row => row.RelationName == serviceTable);
    }

    [Theory]
    [InlineData(PgWaitSamplingCollector.ServiceApplicationName)]
    [InlineData(PgWaitSamplingCollector.ServiceRemediationApplicationName)]
    public async Task SessionStates_LeavesOutATransactionHeldUnderAServiceName_AndKeepsTheSameOneUnderAnotherName(string serviceName)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;

        /* The service's connection opens its transaction FIRST, so it is the older xid and, without the exclusion, the session
           the collector force-includes as the horizon holder. With it, the application's younger transaction is the holder. */
        await using var service = await OpenAsync(connectionString!, serviceName, ct);
        await using var application = await OpenAsync(connectionString!, "pm7428-application", ct);
        await using var collector = await OpenAsync(connectionString!, "pm7428-collector", ct);
        await ExecAsync(service, "BEGIN; SELECT pg_catalog.txid_current()", ct);
        await ExecAsync(application, "BEGIN; SELECT pg_catalog.txid_current()", ct);

        var rows = await ReadAsync(PgSessionStatesCollector.Instance, Context(await ServerMajorAsync(collector, ct)), collector, ct);

        Assert.DoesNotContain(rows, row => row.ApplicationName == serviceName);
        Assert.Contains(rows, row => row.ApplicationName == "pm7428-application");
    }
}
