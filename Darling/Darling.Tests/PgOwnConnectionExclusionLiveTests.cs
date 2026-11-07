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
    public Task SessionStates_AndXminHorizon_LeaveOutTheServicesOwnConnections() => RunAsync(anotherSessionHoldsAnOlderTransaction: false);

    /* #8203: pg_stat_activity is cluster-wide, so a transaction another session holds anywhere on the server (a test in its
       own database, an autovacuum worker) can be older than this test's application connection for the whole read. The
       collector then rightly names that holder, and the service's own connections, older still, must stay out of it. */
    [Fact]
    public Task SessionStates_AndXminHorizon_LeaveOutTheServicesOwnConnections_WhenAnotherSessionHoldsAnOlderTransaction() =>
        RunAsync(anotherSessionHoldsAnOlderTransaction: true);

    private static async Task RunAsync(bool anotherSessionHoldsAnOlderTransaction)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;

        /* The older transaction another session on the cluster can hold: opened before the service connections, so it is
           the oldest holder of all, and the collector names it over the application connection. */
        await using var elsewhere = anotherSessionHoldsAnOlderTransaction ? await OpenAsync(connectionString!, "pm7381-elsewhere", ct) : null;
        if (elsewhere is not null)
        {
            await HoldTransactionAsync(elsewhere, ct);
        }

        /* The service connections open before the application one, so each holds an older transaction id than it: without
           the exclusion the xmin horizon read names one of them, which is the defect. */
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
        /* The collector reports a session only when it holds the oldest horizon, or has sat idle in a transaction for the floor
           (10 s) or held one open for the floor (30 s). On a shared cluster another class's connection can hold an older xmin
           for a moment, and then this fresh session is under both floors and rightly absent (CI leg 13 read one row, a DROP
           from another class, with ReportableSessions = 1: not the row cap). So read again until this session is reported, which
           it is once it becomes the oldest holder or crosses the 10 s idle floor. The service's two connections must be absent
           on every read. */
        var found = false;
        for (var attempt = 0; attempt < 60 && !found; attempt++)
        {
            if (attempt > 0)
            {
                await Task.Delay(500, ct);
            }

            await using var command = new NpgsqlCommand(states.BuildQuery(context).Text, source);
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = await states.ReadAsync(reader, context, ct);
            Assert.DoesNotContain(rows, r => r.Pid == monitoringPid);
            Assert.DoesNotContain(rows, r => r.Pid == remediationPid);
            found = rows.Any(r => r.Pid == applicationPid);
        }

        Assert.True(found, "the test's own idle-in-transaction session was never reported in 30 s");

        /* The cluster is shared with classes outside this collection, and one of their connections or an autovacuum worker
           can hold an older xmin for a moment, so read again until the oldest holder is this test's own. The service's two
           connections are older than it and must never be named, on any read. */
        var horizon = PgXminHorizonCollector.Instance;
        var invariant = System.Globalization.CultureInfo.InvariantCulture;
        var applicationHolder = applicationPid.ToString(invariant);
        string? holder = null;
        for (var attempt = 0; attempt < 60 && holder != applicationHolder; attempt++)
        {
            if (attempt > 0)
            {
                await Task.Delay(500, ct);
            }

            await using var command = new NpgsqlCommand(horizon.BuildQuery(context).Text, source);
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = await horizon.ReadAsync(reader, context, ct);
            holder = rows.Single(r => r.Source == "session").Holder;
            Assert.NotEqual(monitoringPid.ToString(invariant), holder);
            Assert.NotEqual(remediationPid.ToString(invariant), holder);
        }

        Assert.Equal(applicationHolder, holder);
    }
}
