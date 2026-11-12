/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7419: runs the host's REAL <see cref="DarlingCollectorRunner.RunAsync"/> for query_stats on a fake target whose text
/// fetch never answers, with a short item budget. The budget fires in the text fetch, after the read advanced every
/// baseline, and the abandoned run must leave the baselines where the last stored run put them. The store is never reached
/// (the abandon arm returns first), so no database is needed.
/// </summary>
public sealed class QueryStatsAbandonedRunRunnerTests
{
    private const int ServerId = -490077;
    private static readonly DateTime T0 = DateTime.UtcNow.AddMinutes(-5);

    [Fact]
    public async Task AnItemBudgetThatFiresInTheTextFetch_LeavesTheBaselinesWhereTheLastStoredRunPutThem()
    {
        var ct = TestContext.Current.CancellationToken;
        var deltas = new CollectorDeltaCalculator();
        const string key = "0x03AB:0:-1:0x06AB";

        /* The baselines the last stored run left: 100 executions at T0. */
        foreach (var group in new[]
        {
            "query_stats_exec", "query_stats_worker", "query_stats_elapsed", "query_stats_reads",
            "query_stats_writes", "query_stats_phys_reads", "query_stats_rows", "query_stats_spills",
        })
        {
            deltas.CalculateDeltaWithSeriesAge(ServerId, group, key, 100, null, out _, collectionTime: T0, maxGapSeconds: 3600);
        }

        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Timeout=1;Username=x;Database=x");
        var runner = new DarlingCollectorRunner(postgres, deltas)
        {
            ItemBudgetOverrideForTests = TimeSpan.FromMilliseconds(500),
            TargetProviderOverrideForTests = _ => new FakeTargetProvider(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "t", Host = "h" },
            ConnectionString = "Server=onprem;Database=master",
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
            StorageName = "h",
            ServerId = ServerId,
        };

        var result = await runner.RunAsync(QueryStatsCollector.Instance, server, ct);

        Assert.True(result.Abandoned, "the item budget fired in the text fetch");
        Assert.Equal(100, deltas.PeekBaselines(ServerId, "query_stats_exec")[key]);
    }

    private sealed class FakeTargetProvider : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;
        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);
        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand(query.Text, (FakeConnection)connection);
        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;
        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;
        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
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

    private sealed class FakeCommand(string text, FakeConnection owner) : DbCommand
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

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            if (CommandText.Contains("sys.dm_exec_sql_text(k.plan_handle)", StringComparison.Ordinal))
            {
                /* The text fetch: the target never answers. */
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            return MainQuery(DateTime.UtcNow);
        }

        /// <summary>One statement at 160 executions, in the 44-column shape of the candidate query.</summary>
        private static DbDataReader MainQuery(DateTime at)
        {
            var table = new DataTable();
            var stringColumns = new HashSet<int> { 0, 1, 2, 36, 37, 38, 42 };
            for (var i = 0; i < 44; i++)
            {
                var type = stringColumns.Contains(i) ? typeof(string)
                    : i is 3 or 4 ? typeof(DateTime)
                    : i is 40 or 41 or 43 ? typeof(int)
                    : typeof(long);
                table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), type);
            }

            var values = new object?[44];
            for (var i = 0; i < 44; i++)
            {
                values[i] = table.Columns[i].DataType == typeof(long) ? 0L : DBNull.Value;
            }

            values[0] = "app";
            values[1] = "0x01";
            values[3] = at.AddHours(-1);
            values[4] = at;
            values[5] = 160L;
            values[6] = 1600L;
            values[7] = 3200L;
            values[36] = "0x03AB";
            values[37] = "0x06AB";
            values[40] = 0;
            values[41] = -1;
            values[43] = 3600;
            table.Rows.Add(values);
            return table.CreateDataReader();
        }
    }
}
