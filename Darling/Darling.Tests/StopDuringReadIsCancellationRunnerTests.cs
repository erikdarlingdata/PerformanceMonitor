/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A service stop while a server-scoped read is in flight reaches the runner as the PROVIDER's exception, not an
/// OperationCanceledException: SqlClient reports a cancelled command as a SqlException. Runs the host's real
/// <see cref="DarlingCollectorRunner.RunAsync"/> on a fake target whose read cancels the run's own token and then throws
/// that SqlException. The runner must surface a cancellation, which is what RunOneAsync's cancellation arm claims; a
/// SqlException would reach its ERROR arm instead. The store is never reached, so no database is needed.
/// </summary>
public sealed class StopDuringReadIsCancellationRunnerTests
{
    private const int ServerId = -490078;

    [Fact]
    public async Task AStopDuringTheServerScopedRead_SurfacesACancellation_NotTheProvidersException()
    {
        using var stop = new CancellationTokenSource();
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Timeout=1;Username=x;Database=x");
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator())
        {
            TargetProviderOverrideForTests = _ => new FakeTargetProvider(stop),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "t", Host = "h" },
            ConnectionString = "Server=onprem;Database=master",
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
            StorageName = "h",
            ServerId = ServerId,
        };

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            runner.RunAsync(QueryStatsCollector.Instance, server, stop.Token));

        /* The provider's exception travels as the cause, so the cancellation carries the provider's message. */
        Assert.IsType<SqlException>(thrown.InnerException);
    }

    /// <summary>
    /// The stop arm rolls the delta baselines back, not just the status. query_stats' read advances every candidate's baseline,
    /// then the deferred text fetch (a second query on the same connection) is where the stop lands, so the rows exist and are
    /// never stored. The next run's delta must be taken against the baseline from before the stopped run: 60 over both
    /// intervals, not the 30 of the last one alone.
    /// </summary>
    [Fact]
    public async Task AStopAfterTheServerScopedRead_RollsTheDeltaBaselinesBack()
    {
        var deltas = new CollectorDeltaCalculator();
        var before = DateTime.UtcNow.AddMinutes(-10);
        await QueryStatsAbandonedRunBaselineTests.ReadAsync(deltas, before, (1, 100));

        using var stop = new CancellationTokenSource();
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Timeout=1;Username=x;Database=x");
        var runner = new DarlingCollectorRunner(postgres, deltas)
        {
            TargetProviderOverrideForTests = _ => new FakeTargetProvider(
                stop, firstRead: () => QueryStatsAbandonedRunBaselineTests.Table(DateTime.UtcNow, (1, 130)).CreateDataReader()),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "t", Host = "h" },
            ConnectionString = "Server=onprem;Database=master",
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
            StorageName = "h",
            ServerId = 7, // the id the helper's context reads under
            CapturePlans = false, // the helper's 44-column shape carries no plan column
        };

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            runner.RunAsync(QueryStatsCollector.Instance, server, stop.Token));
        Assert.IsType<SqlException>(thrown.InnerException);

        /* The stopped run advanced the baseline to 130; the arm put it back to the 100 of the last stored run. */
        Assert.Equal(100, deltas.PeekBaselines(7, "query_stats_exec").Values.Single());
        var next = (await QueryStatsAbandonedRunBaselineTests.ReadAsync(deltas, DateTime.UtcNow.AddMinutes(1), (1, 160))).Single().Precomputed!;
        Assert.Equal(60, next.Exec);
    }

    /// <summary>
    /// The fake target. Every read cancels the stop token and throws the SqlException SqlClient raises for a cancelled command,
    /// except the first when <paramref name="firstRead"/> is given: that one answers with the main query's rows, so the stop
    /// lands in the next query on the connection.
    /// </summary>
    internal sealed class FakeTargetProvider(CancellationTokenSource stop, Func<DbDataReader>? firstRead = null) : ITargetProvider
    {
        private int _reads;

        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;
        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);
        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand(query.Text, (FakeConnection)connection, Read);

        private DbDataReader Read()
        {
            if (firstRead is not null && Interlocked.Increment(ref _reads) == 1)
            {
                return firstRead();
            }

            stop.Cancel();
            throw LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(0, errorClass: 11, message: "Operation cancelled by user.");
        }

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;
        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;
        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, System.Collections.Generic.IReadOnlyList<string>? excludedDatabases, System.Collections.Generic.IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _cs; set => _cs = value ?? string.Empty; }
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        public override Task OpenAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    /// <summary>The read: answers through the provider, which stops the service mid-command and raises SqlClient's SqlException for the cancelled command.</summary>
    private sealed class FakeCommand(string text, FakeConnection owner, Func<DbDataReader> read) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = text;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get => owner; set { } }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => null;
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult(read());
    }
}
