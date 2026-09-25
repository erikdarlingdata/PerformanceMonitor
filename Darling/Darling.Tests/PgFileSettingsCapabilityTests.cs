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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgFileSettingsCapability"/>'s cache discipline and the #4251 round-1 review fixes: H1(a)'s
/// Windows gate (both on the probe's own regex and on the cached fact it carries alongside the grant
/// verdict), and M1's stale-verdict forgetting on a revoked grant.
/// </summary>
[Collection("pg-file-settings-statics")]
public sealed class PgFileSettingsCapabilityTests : IDisposable
{
    public PgFileSettingsCapabilityTests()
    {
        PgFileSettingsCapability.Reset();
        PgFileSettingsCapability.CacheTtl = TimeSpan.FromHours(1);
    }

    public void Dispose()
    {
        PgFileSettingsCapability.Reset();
        PgFileSettingsCapability.CacheTtl = TimeSpan.FromHours(1);
    }

    /// <summary>A runtime keyed as <paramref name="storageName"/>, the same shape
    /// <c>PgReadBinaryFileCapabilityTests.Runtime</c> builds for its own sibling capability.</summary>
    internal static ServerRuntime Runtime(
        string storageName,
        CollectorTargetEngine engine = CollectorTargetEngine.PostgreSql) => new()
    {
        Config = new MonitoredServer { Name = storageName, Host = "h" },
        ConnectionString = "Host=h",
        Target = new CollectorTargetInfo { Engine = engine },
        StorageName = storageName,
        ServerId = 1,
    };

    private static PostgresException Pg(string sqlState) => new("boom", "ERROR", "ERROR", sqlState);

    /// <summary>
    /// A fake ADO.NET connection whose only load-bearing behavior is counting how many times a command
    /// against it was executed and returning a caller-supplied scalar — the same minimum override surface
    /// <c>PgReadBinaryFileCapabilityTests.FakeScalarConnection</c> uses for the sibling capability, copied
    /// here rather than shared across files because it is `internal` to that test class.
    /// </summary>
    private sealed class FakeScalarConnection : DbConnection
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

    /// <summary>
    /// #4251 round-1 review, H1(a): the regex embedded in <see cref="PgFileSettingsCapability.ProbeSql"/>,
    /// pulled out of the constant and run the way PostgreSQL's <c>~*</c> runs it (case-insensitive), against
    /// two real Linux <c>version()</c> strings, a pre-17 Windows one, and this rig's own live 18.6 Windows
    /// string — so a change to the pattern that stops matching real Windows builds, or starts matching real
    /// Linux ones, fails here rather than only being noticed on whichever platform CI happens to run on.
    /// </summary>
    [Theory]
    [InlineData("PostgreSQL 16.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 12.2.0, 64-bit", false)]
    [InlineData("PostgreSQL 16.4 on aarch64-unknown-linux-gnu, compiled by aarch64-unknown-linux-gnu-gcc (GCC) 9.5.0, 64-bit", false)]
    [InlineData("PostgreSQL 15.8, compiled by Visual C++ build 1940, 64-bit", true)]
    [InlineData("PostgreSQL 18.6 on x86_64-windows, compiled by msvc-19.44.35228, 64-bit", true)] // this rig's own version() (2026-09-25)
    public void TheWindowsRegexClassifiesKnownVersionStrings(string versionText, bool expectWindows)
    {
        var match = Regex.Match(PgFileSettingsCapability.ProbeSql, @"~\*\s*'([^']*)'");
        Assert.True(match.Success, "ProbeSql must carry a ~* regex literal for the Windows fact");

        var isWindows = Regex.IsMatch(versionText, match.Groups[1].Value, RegexOptions.IgnoreCase);

        Assert.Equal(expectWindows, isWindows);
    }

    /// <summary>
    /// The probe carries both facts in one scalar, "readable:isWindows" (#4251 round-1 review, H1(a)) — one
    /// round trip either way, mirroring the shape <c>PgReadBinaryFileCapability</c> uses for its own
    /// encoding-plus-verdict scalar.
    /// </summary>
    [Theory]
    [InlineData("true:true", true, true)]
    [InlineData("false:true", false, true)]
    [InlineData("true:false", true, false)]
    [InlineData("false:false", false, false)]
    public async Task IsReadableAsync_CachesBothFactsFromOneScalar(string scalar, bool expectReadable, bool expectWindows)
    {
        var connection = new FakeScalarConnection { Scalar = scalar };

        var readable = await PgFileSettingsCapability.IsReadableAsync(connection, "target-a", CancellationToken.None);

        Assert.Equal(expectReadable, readable);
        Assert.Equal(1, connection.ExecuteCount);

        Assert.True(PgFileSettingsCapability.TryGetCachedVerdict("target-a", out var cachedReadable, out var cachedWindows));
        Assert.Equal(expectReadable, cachedReadable);
        Assert.Equal(expectWindows, cachedWindows);
    }

    [Fact]
    public async Task ACacheHitInsideTheTtlMakesNoSecondRoundTrip()
    {
        var connection = new FakeScalarConnection { Scalar = "true:true" };

        var first = await PgFileSettingsCapability.IsReadableAsync(connection, "target-a", CancellationToken.None);
        var second = await PgFileSettingsCapability.IsReadableAsync(connection, "target-a", CancellationToken.None);

        Assert.True(first);
        Assert.True(second);
        Assert.Equal(1, connection.ExecuteCount);
    }

    [Fact]
    public void TryGetCachedVerdict_FindsNothingForAnUncheckedTarget()
    {
        Assert.False(PgFileSettingsCapability.TryGetCachedVerdict("never-checked", out var readable, out var isWindows));
        Assert.False(readable);
        Assert.False(isWindows);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(42)]
    [InlineData("true")] // missing the ":isWindows" half the real probe always sends
    [InlineData("notabool:notabool")]
    public async Task AMalformedOrMissingScalarGivesFalseForBoth(object? scalar)
    {
        var connection = new FakeScalarConnection { Scalar = scalar is null ? DBNull.Value : scalar };

        var readable = await PgFileSettingsCapability.IsReadableAsync(connection, "target-a", CancellationToken.None);

        Assert.False(readable);
        Assert.True(PgFileSettingsCapability.TryGetCachedVerdict("target-a", out var cachedReadable, out var cachedWindows));
        Assert.False(cachedReadable);
        Assert.False(cachedWindows);
    }

    /// <summary>
    /// #4251 round-1 review, M1: the three cases <c>ForgetStaleFileSettingsVerdict</c> must tell apart — the
    /// same shape <c>AStaleVerdictIsForgottenOnlyOnTheFaultsThatMakeItStale</c> pins for the read-binary-file
    /// sibling.
    /// </summary>
    [Fact]
    public async Task ForgetStaleFileSettingsVerdict_DropsOnlyAStaleReadableVerdictOnPgServerConfigs42501()
    {
        /* Case 1: a cached "readable" verdict plus 42501 on pg_server_config drops the verdict. */
        await PgFileSettingsCapability.IsReadableAsync(
            new FakeScalarConnection { Scalar = "true:true" }, "revoked-target", CancellationToken.None);
        Assert.True(PgFileSettingsCapability.TryGetCachedVerdict("revoked-target", out _, out _));

        DarlingCollectorRunner.ForgetStaleFileSettingsVerdict("pg_server_config", Pg("42501"), Runtime("revoked-target"));

        Assert.False(PgFileSettingsCapability.TryGetCachedVerdict("revoked-target", out _, out _));

        /* Case 2: 42501 on another collector leaves it. */
        await PgFileSettingsCapability.IsReadableAsync(
            new FakeScalarConnection { Scalar = "true:true" }, "other-collector-target", CancellationToken.None);

        DarlingCollectorRunner.ForgetStaleFileSettingsVerdict("pg_database_stats", Pg("42501"), Runtime("other-collector-target"));

        Assert.True(PgFileSettingsCapability.TryGetCachedVerdict("other-collector-target", out var stillReadable, out _));
        Assert.True(stillReadable);

        /* Case 3: a cached "unreadable" verdict is left alone (nothing to drop — re-probing early would only
           find the same answer, and ShouldLogUnreadable's one-time latch must not be revisited). */
        await PgFileSettingsCapability.IsReadableAsync(
            new FakeScalarConnection { Scalar = "false:true" }, "already-unreadable-target", CancellationToken.None);

        DarlingCollectorRunner.ForgetStaleFileSettingsVerdict("pg_server_config", Pg("42501"), Runtime("already-unreadable-target"));

        Assert.True(PgFileSettingsCapability.TryGetCachedVerdict("already-unreadable-target", out var stillUnreadable, out _));
        Assert.False(stillUnreadable);
    }

    /// <summary>A non-PostgresException, a wrong SQLSTATE, and a proven store write all leave the verdict
    /// alone too — the same defensiveness <c>ForgetStaleReadBinaryFileVerdict</c> has, since this general
    /// handler is also the OutOfMemoryException landing pad.</summary>
    [Fact]
    public async Task ForgetStaleFileSettingsVerdict_IgnoresNonMatchingFaults()
    {
        await PgFileSettingsCapability.IsReadableAsync(
            new FakeScalarConnection { Scalar = "true:true" }, "target-x", CancellationToken.None);

        DarlingCollectorRunner.ForgetStaleFileSettingsVerdict("pg_server_config", new InvalidOperationException("not a server fault"), Runtime("target-x"));
        Assert.True(PgFileSettingsCapability.TryGetCachedVerdict("target-x", out var afterNonPg, out _));
        Assert.True(afterNonPg);

        DarlingCollectorRunner.ForgetStaleFileSettingsVerdict("pg_server_config", Pg("42P01"), Runtime("target-x"));
        Assert.True(PgFileSettingsCapability.TryGetCachedVerdict("target-x", out var afterWrongSqlState, out _));
        Assert.True(afterWrongSqlState);
    }

    /// <summary>
    /// #4251 round-1 review, H1(a): the collector-runner gate. On a non-Windows target,
    /// <see cref="CollectorContext.PgFileSettingsReadable"/> stays false even when the grants ARE present,
    /// so <c>PgServerConfigCollector.BuildQuery</c> always keeps the plain query — the grants fix nothing
    /// there — and no caveat log line fires. On a Windows target the existing readable/unreadable behavior
    /// is unchanged.
    /// </summary>
    [Theory]
    [InlineData("true:true", true)]   // Windows, readable -> enhanced query
    [InlineData("false:true", false)] // Windows, unreadable -> plain query, logs once
    [InlineData("true:false", false)] // non-Windows, "readable" -> plain query anyway, no log
    [InlineData("false:false", false)] // non-Windows, unreadable -> plain query, no log
    public async Task ResolvePgFileSettingsReadableAsync_GatesOnWindows(string scalar, bool expectEnhancedQuery)
    {
        var connection = new FakeScalarConnection { Scalar = scalar };
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = new DateTime(2026, 9, 25, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
        };

        await DarlingCollectorRunner.ResolvePgFileSettingsReadableAsync(
            context, "pg_server_config", connection, Runtime("gate-target"), logger: null, CancellationToken.None);

        Assert.Equal(expectEnhancedQuery, context.PgFileSettingsReadable);
    }

    /// <summary>Every other collector, and every non-PostgreSQL target, is untouched — pg_file_settings only
    /// matters to pg_server_config.</summary>
    [Fact]
    public async Task ResolvePgFileSettingsReadableAsync_OnlyAppliesToPgServerConfigOnPostgres()
    {
        var connection = new FakeScalarConnection { Scalar = "true:true" };
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = new DateTime(2026, 9, 25, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
        };

        await DarlingCollectorRunner.ResolvePgFileSettingsReadableAsync(
            context, "pg_database_stats", connection, Runtime("other-target"), logger: null, CancellationToken.None);
        Assert.False(context.PgFileSettingsReadable);
        Assert.Equal(0, connection.ExecuteCount);

        await DarlingCollectorRunner.ResolvePgFileSettingsReadableAsync(
            context, "pg_server_config", connection, Runtime("sql-target", CollectorTargetEngine.SqlServer), logger: null, CancellationToken.None);
        Assert.False(context.PgFileSettingsReadable);
        Assert.Equal(0, connection.ExecuteCount);
    }
}
