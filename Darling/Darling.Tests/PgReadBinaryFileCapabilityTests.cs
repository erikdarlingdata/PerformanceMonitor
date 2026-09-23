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
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgReadBinaryFileCapability"/>'s cache discipline (#4046 part 1c): a target's grant is checked
/// at most once per <see cref="PgReadBinaryFileCapability.CacheTtl"/>, so three log-tail collectors on the
/// same cycle do not each pay a round trip to learn the same answer.
/// </summary>
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
    /// The real-server case (#4046 part 1c): a dedicated role with no grant at all reads false, and after
    /// granting <c>pg_read_binary_file</c> and resetting the cache (a real cycle would simply wait out the
    /// TTL) the same probe reads true. Same DARLING_TEST_PG gate and dedicated-role-create-and-drop shape as
    /// the suite's other <c>*_AgainstDevPostgres</c> live tests.
    /// </summary>
    [Fact]
    public async Task IsGrantedAsync_AgainstDevPostgres()
    {
        var connectionStringRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrWhiteSpace(connectionStringRoot))
        {
            return;
        }

        const string role = "pm_test_pgreadbinaryfile_role";

        await using var adminConnection = new NpgsqlConnection(connectionStringRoot);
        await adminConnection.OpenAsync();

        await using (var drop = adminConnection.CreateCommand())
        {
            drop.CommandText = $"DROP ROLE IF EXISTS {role}";
            await drop.ExecuteNonQueryAsync();
        }

        await using (var create = adminConnection.CreateCommand())
        {
            create.CommandText = $"CREATE ROLE {role} LOGIN PASSWORD 'pm_test_pgreadbinaryfile' NOSUPERUSER";
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionStringRoot)
            {
                Username = role,
                Password = "pm_test_pgreadbinaryfile",
            };

            await using (var roleConnection = new NpgsqlConnection(builder.ConnectionString))
            {
                await roleConnection.OpenAsync();

                var ungranted = await PgReadBinaryFileCapability.IsGrantedAsync(
                    roleConnection, "dev-postgres-role-probe", CancellationToken.None);
                Assert.False(ungranted);
            }

            await using (var grant = adminConnection.CreateCommand())
            {
                grant.CommandText =
                    $"GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint) TO {role}";
                await grant.ExecuteNonQueryAsync();
            }

            PgReadBinaryFileCapability.Reset();

            await using (var roleConnection = new NpgsqlConnection(builder.ConnectionString))
            {
                await roleConnection.OpenAsync();

                var granted = await PgReadBinaryFileCapability.IsGrantedAsync(
                    roleConnection, "dev-postgres-role-probe", CancellationToken.None);
                Assert.True(granted);
            }
        }
        finally
        {
            await using var drop = adminConnection.CreateCommand();
            drop.CommandText = $"DROP ROLE IF EXISTS {role}";
            await drop.ExecuteNonQueryAsync();
        }
    }
}
