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
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7381: the session-state and xmin-horizon collectors leave out every connection the service itself holds
/// (the monitoring connections and the remediation ones), not only their own backend. The wait sampler runs
/// its batch on a second connection inside a transaction, and was reported as a Critical horizon holder.
/// Run on a real server: each service-named connection opens a transaction that holds a transaction id, then
/// the collectors' own query text runs and must report the ordinary application connection and neither of the
/// service ones.
/// </summary>
[Collection("live-postgres")]
public sealed class PgOwnConnectionExclusionLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the own-connection exclusion tests.";

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, string applicationName, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            ApplicationName = applicationName,
            Pooling = false,
            SearchPath = "pg_catalog",
        };
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

    private static async Task<int> HoldTransactionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var begin = new NpgsqlCommand("BEGIN", connection))
        {
            await begin.ExecuteNonQueryAsync(ct);
        }

        await using (var xid = new NpgsqlCommand("SELECT pg_catalog.txid_current()", connection))
        {
            await xid.ExecuteScalarAsync(ct);
        }

        await using var pid = new NpgsqlCommand("SELECT pg_catalog.pg_backend_pid()", connection);
        return Convert.ToInt32(await pid.ExecuteScalarAsync(ct));
    }

    private static CollectorContext Context() => new()
    {
        ServerId = -1,
        ServerName = "pm7381",
        CollectionTime = new DateTime(2031, 6, 10, 12, 0, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 16, PostgresVersionNum = 160000,
        },
    };

    [Fact]
    public async Task SessionStates_AndXminHorizon_LeaveOutTheServicesOwnConnections()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;

        /* The service connections open first, so each holds the oldest transaction id: without the
           exclusion the xmin horizon read names one of them, which is the defect. */
        await using var monitoring = await OpenAsync(connectionString!, PgWaitSamplingCollector.ServiceApplicationName, ct);
        await using var remediation = await OpenAsync(connectionString!, PgWaitSamplingCollector.ServiceRemediationApplicationName, ct);
        await using var application = await OpenAsync(connectionString!, "pm7381-app", ct);
        await using var source = await OpenAsync(connectionString!, "pm7381-collector", ct);

        var monitoringPid = await HoldTransactionAsync(monitoring, ct);
        var remediationPid = await HoldTransactionAsync(remediation, ct);

        /* Burn some transaction ids so the held ones have a real age (the xmin read ignores age 0). */
        for (var i = 0; i < 3; i++)
        {
            await using var burn = new NpgsqlCommand("SELECT pg_catalog.txid_current()", source);
            await burn.ExecuteScalarAsync(ct);
        }

        var applicationPid = await HoldTransactionAsync(application, ct);
        for (var i = 0; i < 3; i++)
        {
            await using var burn = new NpgsqlCommand("SELECT pg_catalog.txid_current()", source);
            await burn.ExecuteScalarAsync(ct);
        }

        var context = Context();

        var states = PgSessionStatesCollector.Instance;
        await using (var command = new NpgsqlCommand(states.BuildQuery(context).Text, source))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            var rows = await states.ReadAsync(reader, context, ct);
            Assert.Contains(rows, r => r.Pid == applicationPid);
            Assert.DoesNotContain(rows, r => r.Pid == monitoringPid);
            Assert.DoesNotContain(rows, r => r.Pid == remediationPid);
        }

        var horizon = PgXminHorizonCollector.Instance;
        await using (var command = new NpgsqlCommand(horizon.BuildQuery(context).Text, source))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            var rows = await horizon.ReadAsync(reader, context, ct);
            var session = rows.Single(r => r.Source == "session");
            Assert.Equal(applicationPid.ToString(System.Globalization.CultureInfo.InvariantCulture), session.Holder);
        }
    }
}
