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
using PerformanceMonitor.Darling.Service;
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
        /* #4053 part b1: PgLogFormatCapability's cache is process-wide static state too, keyed the same way
           (target-key), and TheRunnerGateProbesOnlyAPostgresLogTailCollector's theory rows all probe
           "gate-target" — without this reset, a later row's csvlog probe hits the cache an earlier row in
           the same run already filled and makes one fewer round trip than expected. */
        PgLogFormatCapability.Reset();
    }

    public void Dispose()
    {
        PgReadBinaryFileCapability.Reset();
        PgReadBinaryFileCapability.CacheTtl = TimeSpan.FromHours(1);
        PgLogFormatCapability.Reset();
    }

    /// <summary>A runtime keyed as <paramref name="storageName"/>. The cache helpers take the runtime, not a
    /// key (#4051 round-2 review), so a test builds one.</summary>
    internal static ServerRuntime Runtime(
        string storageName,
        CollectorTargetEngine engine = CollectorTargetEngine.PostgreSql,
        string? connectedDatabase = null) => new()
    {
        Config = new MonitoredServer { Name = storageName, Host = "h" },
        ConnectionString = "Host=h",
        Target = new CollectorTargetInfo { Engine = engine },
        StorageName = storageName,
        ServerId = 1,
        ConnectedDatabase = connectedDatabase,
    };

    private static PostgresException Pg(string sqlState) => new("boom", "ERROR", "ERROR", sqlState);

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
        var connection = new FakeScalarConnection { Scalar = "UTF8:true" };

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
        var connection = new FakeScalarConnection { Scalar = "UTF8:false" };

        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);
        Assert.Equal(1, connection.ExecuteCount);

        await Task.Delay(60);

        connection.Scalar = "UTF8:true";
        var second = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.True(second);
        Assert.Equal(2, connection.ExecuteCount);
    }

    [Fact]
    public async Task PerTargetKeysAreIndependent()
    {
        var connection = new FakeScalarConnection { Scalar = "UTF8:true" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        connection.Scalar = "UTF8:false";
        var forB = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-b", CancellationToken.None);

        Assert.False(forB);
        Assert.Equal(2, connection.ExecuteCount);

        /* target-a's cached true must be untouched by target-b's probe. */
        connection.Scalar = "UTF8:false"; // if a round trip happened for target-a, this would flip it to false
        var forAAgain = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);
        Assert.True(forAAgain);
        Assert.Equal(2, connection.ExecuteCount);
    }

    [Fact]
    public async Task ResetClearsEveryCachedVerdict()
    {
        var connection = new FakeScalarConnection { Scalar = "UTF8:true" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        PgReadBinaryFileCapability.Reset();

        connection.Scalar = "UTF8:false";
        var afterReset = await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.False(afterReset);
        Assert.Equal(2, connection.ExecuteCount);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(42)]
    [InlineData("true")] // missing the 'encoding:' prefix the real probe always sends
    [InlineData("UTF8:notabool")]
    public async Task ADbNullOrMalformedScalarGivesFalse(object? scalar)
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
        var connection = new FakeScalarConnection { Scalar = "UTF8:true" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.True(PgReadBinaryFileCapability.TryGetCachedVerdict("target-a", out var granted));
        Assert.True(granted);
        Assert.Equal(1, connection.ExecuteCount);
    }

    /// <summary>#4062: the cached encoding rides alongside the verdict, mapped from server_encoding.</summary>
    [Fact]
    public async Task TryGetCachedEncoding_ReadsTheMappedEncodingWithoutARoundTrip()
    {
        var connection = new FakeScalarConnection { Scalar = "WIN1252:true" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.True(PgReadBinaryFileCapability.TryGetCachedEncoding("target-a", out var encoding));
        Assert.Same(PgServerEncoding.TryGet("WIN1252", out var expected) ? expected : null, encoding is not null ? encoding : null);
        Assert.NotNull(encoding);
    }

    /// <summary>#4062: an unmapped encoding caches no Encoding at all, even though the privilege is granted.</summary>
    [Fact]
    public async Task TryGetCachedEncoding_FindsNothingForAnUnmappedEncoding()
    {
        var connection = new FakeScalarConnection { Scalar = "EUC_TW:true" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);

        Assert.False(PgReadBinaryFileCapability.TryGetCachedEncoding("target-a", out _));
    }

    /// <summary>
    /// The runner's gate (#4046 part 1c): a PostgreSQL log-tail collector takes the probe's answer; any other
    /// collector, and any SQL Server target, stays on the text route with no round trip. A log-tail collector in
    /// <c>PgLogFormatCapability.RoutedCollectors</c> makes a second probe (#4053: whether csvlog is configured) —
    /// pg_log_events since part a1b and pg_deadlocks since part b1 (pg_plan_capture joins with part b2).
    /// </summary>
    [Theory]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_log_events", true, 2)]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_deadlocks", true, 2)]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_plan_capture", true, 1)]
    [InlineData(CollectorTargetEngine.PostgreSql, "pg_database_stats", false, 0)]
    [InlineData(CollectorTargetEngine.SqlServer, "pg_log_events", false, 0)]
    public async Task TheRunnerGateProbesOnlyAPostgresLogTailCollector(
        CollectorTargetEngine engine, string collectorName, bool expectedGranted, int expectedProbes)
    {
        var connection = new FakeScalarConnection { Scalar = "UTF8:true" };
        var context = new CollectorContext
        {
            ServerId = 1, ServerName = "gate-target", CollectionTime = new DateTime(2026, 9, 23, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
        };

        await DarlingCollectorRunner.ResolvePgReadBinaryFileGrantAsync(
            context, collectorName, connection, Runtime("gate-target", engine), CancellationToken.None);

        Assert.Equal(expectedGranted, context.PgReadBinaryFileGranted);
        Assert.Equal(expectedProbes, connection.ExecuteCount);
    }

    /// <summary>
    /// #4062: the privilege is GRANTED, but the database's server_encoding is one <see cref="PgServerEncoding"/>
    /// does not map (EUC_TW here, standing in for the unmapped list). The target stays on the text route, the
    /// advisory reads it as nothing to advise, and the verdict is cached like any other, so it costs no extra
    /// round trip. The cache also reports it as an encoding the byte route does not serve.
    /// </summary>
    [Fact]
    public async Task AGrantedButUnmappedEncodingStaysOnTheTextRouteAndIsNotAdvised()
    {
        var connection = new FakeScalarConnection { Scalar = "EUC_TW:true" };

        Assert.False(await PgReadBinaryFileCapability.IsGrantedAsync(connection, "euctw-target", CancellationToken.None));
        Assert.False(await PgReadBinaryFileCapability.IsGrantedAsync(connection, "euctw-target", CancellationToken.None));
        Assert.Equal(1, connection.ExecuteCount);
        Assert.False(PgReadBinaryFileCapability.TryGetCachedVerdict("euctw-target", out _));
        Assert.True(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding("euctw-target"));
    }

    /// <summary>An ungranted target on an unmapped encoding reads as ungranted, not as unsupported-encoding:
    /// the grant matters first.</summary>
    [Fact]
    public async Task AnUngrantedTargetOnAnUnmappedEncodingReadsAsUngranted()
    {
        var connection = new FakeScalarConnection { Scalar = "EUC_TW:false" };

        Assert.False(await PgReadBinaryFileCapability.IsGrantedAsync(connection, "euctw-ungranted", CancellationToken.None));
        Assert.False(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding("euctw-ungranted"));
        Assert.True(PgReadBinaryFileCapability.TryGetCachedVerdict("euctw-ungranted", out var granted));
        Assert.False(granted);
    }

    /// <summary>Only granted-but-unmapped reads as an encoding the byte route does not serve. A granted-and-mapped
    /// target, an ungranted one, and one that was never checked all read false.</summary>
    [Fact]
    public async Task OnlyAGrantedAndUnmappedVerdictReadsAsAnUnsupportedEncoding()
    {
        await PgReadBinaryFileCapability.IsGrantedAsync(new FakeScalarConnection { Scalar = "UTF8:true" }, "granted", default);
        await PgReadBinaryFileCapability.IsGrantedAsync(new FakeScalarConnection { Scalar = "UTF8:false" }, "ungranted", default);

        Assert.False(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding("granted"));
        Assert.False(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding("ungranted"));
        Assert.False(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding("never-checked"));
    }

    /// <summary>
    /// #4062: the probe has no CASE gate — it always reports the server_encoding plus the raw privilege check.
    /// C# decides whether the mapped encoding serves the binary route.
    /// </summary>
    [Fact]
    public void TheProbeAnswersUnconditionallyAndLeavesTheEncodingDecisionToCSharp()
    {
        Assert.DoesNotContain("CASE", PgReadBinaryFileCapability.ProbeSql, StringComparison.Ordinal);
        Assert.Contains(
            "current_setting('server_encoding')", PgReadBinaryFileCapability.ProbeSql, StringComparison.Ordinal);
        Assert.Contains(
            "'pg_catalog.pg_read_binary_file(text, bigint, bigint)'", PgReadBinaryFileCapability.ProbeSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InvalidateDropsOneTargetsVerdictOnly()
    {
        var connection = new FakeScalarConnection { Scalar = "UTF8:true" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", CancellationToken.None);
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-b", CancellationToken.None);

        PgReadBinaryFileCapability.Invalidate("target-a");

        Assert.False(PgReadBinaryFileCapability.TryGetCachedVerdict("target-a", out _));
        Assert.True(PgReadBinaryFileCapability.TryGetCachedVerdict("target-b", out _));
    }

    /// <summary>
    /// #4051 review L1: which faults drop a target's cached verdict. A 22021 on a log-tail collector always does,
    /// so a grant made in answer takes effect on the next cycle. A 42501 does only while the verdict says granted,
    /// which means the grant was revoked. A 42501 on the text route, any other SQLSTATE, and any other collector
    /// leave it alone.
    /// </summary>
    [Theory]
    [InlineData("pg_log_events", "22021", false, true)]
    [InlineData("pg_deadlocks", "22021", true, true)]
    [InlineData("pg_plan_capture", "42501", true, true)]
    [InlineData("pg_log_events", "42501", false, false)]
    [InlineData("pg_log_events", "58P01", true, false)]
    [InlineData("pg_log_events", null, true, false)]
    [InlineData("pg_database_stats", "22021", true, false)]
    public async Task AStaleVerdictIsForgottenOnlyOnTheFaultsThatMakeItStale(
        string collectorName, string? sqlState, bool cachedGranted, bool expectForgotten)
    {
        var connection = new FakeScalarConnection { Scalar = $"UTF8:{(cachedGranted ? "true" : "false")}" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "stale-target", CancellationToken.None);

        Exception fault = sqlState is null ? new InvalidOperationException("not a server fault") : Pg(sqlState);
        DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict(collectorName, fault, Runtime("stale-target"));

        Assert.Equal(!expectForgotten, PgReadBinaryFileCapability.TryGetCachedVerdict("stale-target", out _));
    }

    /// <summary>
    /// #4051 round-2 review: two 22021s that leave the cached verdict alone. One is proven to come from a write
    /// to the STORE, so it says nothing about the target's log. The other is on a database whose encoding the
    /// byte route does not serve, where a re-check would only find the same NULL.
    /// </summary>
    [Fact]
    public async Task A22021FromTheStoreOrOnAnUnservedEncodingKeepsTheVerdict()
    {
        await PgReadBinaryFileCapability.IsGrantedAsync(new FakeScalarConnection { Scalar = "UTF8:false" }, "store-target", default);
        var storeWrite = Pg("22021");
        CollectorFaultCopyPhase.Stamp(storeWrite, StoreCopyPhase.Data);

        DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict("pg_log_events", storeWrite, Runtime("store-target"));

        Assert.True(PgReadBinaryFileCapability.TryGetCachedVerdict("store-target", out _));

        await PgReadBinaryFileCapability.IsGrantedAsync(new FakeScalarConnection { Scalar = "EUC_TW:true" }, "euc-target", default);

        DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict("pg_log_events", Pg("22021"), Runtime("euc-target"));

        Assert.True(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding("euc-target"));
    }

    /// <summary>
    /// #4051 round-2 review: the general handler's one call for a log-tail fault. It builds the sentence before
    /// it drops the verdict, because the sentence depends on that verdict.
    /// <list type="bullet">
    /// <item>On a UTF8 or SQL_ASCII database, a planted byte gets the grant sentence, and the cached verdict is
    /// dropped, so a grant made in answer takes effect on the next cycle.</item>
    /// <item>On an encoding the byte route does not serve, the sentence says the grant does not help, with no
    /// issue reference in the served text, and the verdict stays.</item>
    /// <item>Any other fault, or any other collector, gets null and changes nothing.</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task TheGeneralHandlersLogTailCallPicksTheSentenceThenDropsOnlyAStaleVerdict()
    {
        var planted = new PostgresException("invalid byte sequence for encoding UTF8: 0xff", "ERROR", "ERROR", "22021");

        await PgReadBinaryFileCapability.IsGrantedAsync(new FakeScalarConnection { Scalar = "UTF8:false" }, "utf8-target", default);
        var grant = DarlingWorker.LogTailGeneralFault(planted, "pg_deadlocks", Runtime("utf8-target", connectedDatabase: "appdb"));

        Assert.NotNull(grant);
        Assert.Contains("EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint)", grant, StringComparison.Ordinal);
        Assert.Contains("database 'appdb'", grant, StringComparison.Ordinal);
        Assert.False(PgReadBinaryFileCapability.TryGetCachedVerdict("utf8-target", out _));

        await PgReadBinaryFileCapability.IsGrantedAsync(new FakeScalarConnection { Scalar = "EUC_TW:true" }, "euc-target", default);
        var noRemedy = DarlingWorker.LogTailGeneralFault(planted, "pg_deadlocks", Runtime("euc-target"));

        Assert.NotNull(noRemedy);
        Assert.Contains("does not help", noRemedy, StringComparison.Ordinal);
        Assert.DoesNotContain("#4062", noRemedy, StringComparison.Ordinal);
        Assert.DoesNotContain("Grant EXECUTE", noRemedy, StringComparison.Ordinal);
        Assert.True(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding("euc-target"));

        await PgReadBinaryFileCapability.IsGrantedAsync(new FakeScalarConnection { Scalar = "UTF8:true" }, "other-target", default);

        Assert.Null(DarlingWorker.LogTailGeneralFault(new InvalidOperationException("not a server fault"), "pg_deadlocks", Runtime("other-target")));
        Assert.Null(DarlingWorker.LogTailGeneralFault(planted, "pg_database_stats", Runtime("other-target")));
        Assert.True(PgReadBinaryFileCapability.TryGetCachedVerdict("other-target", out _));
    }
}
