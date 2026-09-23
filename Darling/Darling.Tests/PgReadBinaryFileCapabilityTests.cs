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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgReadBinaryFileCapability"/>'s cache discipline (#4046 part 1c): a target's grant is checked
/// at most once per <see cref="PgReadBinaryFileCapability.CacheTtl"/>, so three log-tail collectors on the
/// same cycle do not each pay a round trip to learn the same answer.
/// </summary>
[Collection("pg-read-binary-file-statics")]
public sealed class PgReadBinaryFileCapabilityTests : IDisposable
{
    /// <summary>Every test starts and ends clean — this is process-wide static state shared with every
    /// other test in the process, including the collector definitions' own binary-route tests.</summary>
    public PgReadBinaryFileCapabilityTests()
    {
        PgReadBinaryFileCapability.Reset();
        PgReadBinaryFileCapability.CacheTtl = TimeSpan.FromHours(1);
    }

    public void Dispose()
    {
        PgReadBinaryFileCapability.Reset();
        PgReadBinaryFileCapability.CacheTtl = TimeSpan.FromHours(1);
    }

    /// <summary>
    /// A fake ADO.NET connection whose only load-bearing behavior is counting how many times a command
    /// against it was executed, and returning a caller-supplied scalar — everything else is the minimum
    /// override surface <see cref="DbConnection"/>/<see cref="DbCommand"/> require and none of it is read.
    /// </summary>
    internal sealed class FakeScalarConnection : DbConnection
    {
        public int ExecuteCount;
        public object? Scalar;

        [AllowNull]
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => new FakeScalarCommand(this);

        private sealed class FakeScalarCommand : DbCommand
        {
            private readonly FakeScalarConnection _owner;
            public FakeScalarCommand(FakeScalarConnection owner) => _owner = owner;

            [AllowNull]
            public override string CommandText { get; set; } = string.Empty;
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection? DbConnection { get; set; }
            protected override DbParameterCollection DbParameterCollection { get; } = new FakeParameterCollection();
            protected override DbTransaction? DbTransaction { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override void Cancel() { }
            public override int ExecuteNonQuery() => throw new NotSupportedException();
            public override object? ExecuteScalar()
            {
                _owner.ExecuteCount++;
                return _owner.Scalar;
            }
            public override void Prepare() { }
            protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
        }

        private sealed class FakeParameterCollection : DbParameterCollection
        {
            public override int Add(object value) => throw new NotSupportedException();
            public override void AddRange(Array values) { }
            public override void Clear() { }
            public override bool Contains(object value) => false;
            public override bool Contains(string value) => false;
            public override void CopyTo(Array array, int index) { }
            public override int Count => 0;
            public override System.Collections.IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
            protected override DbParameter GetParameter(int index) => throw new NotSupportedException();
            protected override DbParameter GetParameter(string parameterName) => throw new NotSupportedException();
            public override int IndexOf(object value) => -1;
            public override int IndexOf(string parameterName) => -1;
            public override void Insert(int index, object value) { }
            public override bool IsFixedSize => false;
            public override bool IsReadOnly => false;
            public override bool IsSynchronized => false;
            public override void Remove(object value) { }
            public override void RemoveAt(int index) { }
            public override void RemoveAt(string parameterName) { }
            protected override void SetParameter(int index, DbParameter value) { }
            protected override void SetParameter(string parameterName, DbParameter value) { }
            public override object SyncRoot => new();
        }
    }

    [Fact]
    public async Task ACacheHitInsideTheTtlMakesNoSecondRoundTrip()
    {
        var connection = new FakeScalarConnection { Scalar = true };

        var first = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);
        var second = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.True(first);
        Assert.True(second);
        Assert.Equal(1, connection.ExecuteCount);
    }

    [Fact]
    public async Task ARepobeHappensAfterExpiry()
    {
        PgReadBinaryFileCapability.CacheTtl = TimeSpan.FromMilliseconds(20);
        var connection = new FakeScalarConnection { Scalar = false };

        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);
        Assert.Equal(1, connection.ExecuteCount);

        await Task.Delay(60);

        connection.Scalar = true;
        var second = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.True(second);
        Assert.Equal(2, connection.ExecuteCount);
    }

    [Fact]
    public async Task PerTargetKeysAreIndependent()
    {
        var connection = new FakeScalarConnection { Scalar = true };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        connection.Scalar = false;
        var forB = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-b", CancellationToken.None);

        Assert.False(forB);
        Assert.Equal(2, connection.ExecuteCount);

        /* target-a's cached true must be untouched by target-b's probe. */
        connection.Scalar = false; // if a round trip happened for target-a, this would flip it to false
        var forAAgain = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);
        Assert.True(forAAgain);
        Assert.Equal(2, connection.ExecuteCount);
    }

    [Fact]
    public async Task ResetClearsEveryCachedVerdict()
    {
        var connection = new FakeScalarConnection { Scalar = true };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        PgReadBinaryFileCapability.Reset();

        connection.Scalar = false;
        var afterReset = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.False(afterReset);
        Assert.Equal(2, connection.ExecuteCount);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(42)]
    [InlineData("true")]
    public async Task ADbNullOrNonBoolScalarGivesFalse(object? scalar)
    {
        var connection = new FakeScalarConnection { Scalar = scalar is null ? DBNull.Value : scalar };

        var granted = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.False(granted);
    }

    [Fact]
    public void TryGetCachedVerdict_FindsNothingForAnUncheckedTarget()
    {
        Assert.False(PgReadBinaryFileCapability.TryGetCachedVerdict("never-checked", out var granted));
        Assert.False(granted);
    }

    [Fact]
    public async Task TryGetCachedVerdict_ReadsTheProbedValueWithoutARoundTrip()
    {
        var connection = new FakeScalarConnection { Scalar = true };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.True(PgReadBinaryFileCapability.TryGetCachedVerdict("target-a", out var granted));
        Assert.True(granted);
        Assert.Equal(1, connection.ExecuteCount);
    }

    /// <summary>
    /// The runner's gate (#4046 part 1c): a PostgreSQL log-tail collector takes the probe's answer; any other
    /// collector, and any SQL Server target, stays on the text route with no round trip.
    /// </summary>
    [Theory]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_log_events", true, 1)]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_deadlocks", true, 1)]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_plan_capture", true, 1)]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_database_stats", false, 0)]
    [InlineData(CollectorTargetEngine.SqlServer, "pg_log_events", false, 0)]
    public async Task TheRunnerGateProbesOnlyAPostgresLogTailCollector(
        CollectorTargetEngine engine, string collectorName, bool expectedGranted, int expectedProbes)
    {
        var connection = new FakeScalarConnection { Scalar = true };
        var context = new CollectorContext
        {
            ServerId = 1, ServerName = "gate-target", CollectionTime = new DateTime(2026, 9, 23, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
        };

        await PerformanceMonitor.Darling.Service.DarlingCollectorRunner.ResolvePgReadBinaryFileGrantAsync(
            context, engine, collectorName, connection, "gate-target", CancellationToken.None);

        Assert.Equal(expectedGranted, context.PgReadBinaryFileGranted);
        Assert.Equal(expectedProbes, connection.ExecuteCount);
    }
}
