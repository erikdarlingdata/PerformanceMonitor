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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #6254: Test Connection on a server that names a Query Store replica also opens the replica connection, built the way collection
/// builds it, and says so apart from the primary. Before this a test opened only the primary, so a replica that was down or refused
/// the login still read "connected" while collection quietly read the primary. A replica that will not open is a warning on every
/// surface (the service's test_connect reply, the --test-connection line, the Viewer's dialogs through the reply, add_servers and
/// edit_server), never a failed test or a refused save. The replica's connection string is checked against the one builder.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class QueryStoreReplicaConnectionTestTests : IDisposable
{
    private const string Replica = "replica-01.example.test,14331";
    private const string DriverText = "Distinctive-Replica-Text: login failed for user 'svc_probe' from 10.9.8.7";
    private const string Password = "Synth-Replica-Pw-1";

    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public QueryStoreReplicaConnectionTestTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private static MonitoredServer Server(string? replica) => new()
    {
        Name = "qs-test",
        Host = "primary.example.test",
        Auth = "windows",
        QueryStoreReplica = replica,
    };

    private static ConnectionProbeResult Probe(string? replica, string? replicaError) =>
        new(true, 16, 3, "Enterprise", false, false, false, true, null, QueryStoreReplica: replica, QueryStoreReplicaError: replicaError);

    /* ---------------- the probe opens the replica connection collection would open ---------------- */

    [Fact]
    public async Task TheReplicaProbe_OpensTheConnectionTheCollectionBuilderGives_ReadOnlyIntentOnTheReplicaHost()
    {
        var server = Server(Replica);
        var opened = new List<string>();

        var (host, error) = await DarlingServerConnector.ProbeQueryStoreReplicaAsync(
            server, false, null, CancellationToken.None, connectionString => new RecordingConnection(connectionString, opened, null));

        Assert.Equal(Replica, host);
        Assert.Null(error);
        var text = Assert.Single(opened);
        Assert.Equal(MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(server, false), text);
        var builder = new SqlConnectionStringBuilder(text);
        Assert.EndsWith(Replica, builder.DataSource, StringComparison.Ordinal); /* the builder pins TCP with a tcp: prefix (#6254) */
        Assert.Equal(ApplicationIntent.ReadOnly, builder.ApplicationIntent);
    }

    [Fact]
    public async Task AReplicaThatCannotBeOpened_IsReportedWithItsError_AndDoesNotThrow()
    {
        var (host, error) = await DarlingServerConnector.ProbeQueryStoreReplicaAsync(
            Server(Replica), false, null, CancellationToken.None,
            connectionString => new RecordingConnection(connectionString, [], new InvalidOperationException(DriverText)));

        Assert.Equal(Replica, host);
        Assert.Equal(DriverText, error);
    }

    [Fact]
    public async Task AnUnreachableHost_ThroughTheRealSqlConnection_IsAnErrorNotAnException()
    {
        /* The default seam opens a SqlConnection, as collection does. A closed local port refuses at once. */
        var server = Server("127.0.0.1,1");

        var (host, error) = await DarlingServerConnector.ProbeQueryStoreReplicaAsync(server, false, null, CancellationToken.None);

        Assert.Equal("127.0.0.1,1", host);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public async Task NoReplica_APostgresTarget_AndAnAzureSqlDatabase_OpenNothing()
    {
        var opened = new List<string>();
        Func<string, DbConnection> create = s => new RecordingConnection(s, opened, null);

        Assert.Equal((null, null), await DarlingServerConnector.ProbeQueryStoreReplicaAsync(Server(null), false, null, CancellationToken.None, create));
        Assert.Equal((null, null), await DarlingServerConnector.ProbeQueryStoreReplicaAsync(Server("   "), false, null, CancellationToken.None, create));
        Assert.Equal((null, null), await DarlingServerConnector.ProbeQueryStoreReplicaAsync(Server(Replica), true, null, CancellationToken.None, create));
        var postgres = Server(Replica);
        postgres.Engine = "postgres";
        Assert.Equal((null, null), await DarlingServerConnector.ProbeQueryStoreReplicaAsync(postgres, false, null, CancellationToken.None, create));
        Assert.Empty(opened);
    }

    /* ---------------- #6459: a replica set on an Azure SQL Database is said to be unused, at save time ---------------- */

    [Fact]
    public void AReplicaOnAnAzureSqlDatabase_IsReportedAsIgnored_AndNoOtherTargetIs()
    {
        Assert.Equal(Replica, DarlingServerConnector.IgnoredQueryStoreReplica(Server("  " + Replica + " "), true));
        Assert.Null(DarlingServerConnector.IgnoredQueryStoreReplica(Server(null), true));
        Assert.Null(DarlingServerConnector.IgnoredQueryStoreReplica(Server("   "), true));
        Assert.Null(DarlingServerConnector.IgnoredQueryStoreReplica(Server(Replica), false));

        var postgres = Server(Replica);
        postgres.Engine = "postgres";
        Assert.Null(DarlingServerConnector.IgnoredQueryStoreReplica(postgres, true));
    }

    private static ConnectionProbeResult AzureProbe(string? replica) =>
        new(true, 12, 5, "Azure SQL Database", true, false, false, false, null,
            QueryStoreReplica: replica, QueryStoreReplicaIgnoredOnAzureSqlDb: replica is not null);

    [Fact]
    public void TheReplicaNote_OnAnAzureSqlDatabase_SaysTheSettingHasNoEffect_OnEverySurface()
    {
        var probe = AzureProbe(Replica);

        var note = DarlingServerConnector.DescribeQueryStoreReplica(probe);
        Assert.NotNull(note);
        Assert.Contains(Replica, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database has no readable-replica Query Store read", note, StringComparison.Ordinal);
        Assert.Contains("no effect", note, StringComparison.Ordinal);
        Assert.DoesNotContain("connected", note, StringComparison.Ordinal);

        /* The MCP reply (edit_server's query_store_replica_test, add_servers' detail) carries it too, with no driver text to redact. */
        Assert.Equal(note, Core.QueryStoreReplicaReply(Server(Replica), probe, null, false));

        /* The --test-connection line is a [WARN], and a server with no replica set says nothing. */
        var line = DarlingCliCommands.FormatProbeLine("azure", probe);
        Assert.Contains("  [WARN] azure: Primary: connected. Query Store replica " + Replica + ": not used.", line, StringComparison.Ordinal);
        Assert.DoesNotContain("replica", DarlingCliCommands.FormatProbeLine("azure", AzureProbe(null)), StringComparison.OrdinalIgnoreCase);
        Assert.Null(DarlingServerConnector.DescribeQueryStoreReplica(AzureProbe(null)));
    }

    [Fact]
    public async Task ACancelledProbe_Propagates_NotReportedAsAReplicaError()
    {
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingServerConnector.ProbeQueryStoreReplicaAsync(
            Server(Replica), false, null, cancelled.Token,
            connectionString => new RecordingConnection(connectionString, [], new OperationCanceledException(cancelled.Token))));
    }

    /* ---------------- the CLI line ---------------- */

    [Fact]
    public void TheCliLine_NamesTheReplicaApart_AFailingReplicaIsAWarningNotAFailure_AndNoReplicaChangesNothing()
    {
        var none = DarlingCliCommands.FormatProbeLine("alpha", Probe(null, null));
        Assert.StartsWith("  [PASS] alpha:", none, StringComparison.Ordinal);
        Assert.DoesNotContain("replica", none, StringComparison.OrdinalIgnoreCase);

        var ok = DarlingCliCommands.FormatProbeLine("alpha", Probe(Replica, null));
        Assert.StartsWith("  [PASS] alpha:", ok, StringComparison.Ordinal);
        Assert.Contains("Primary: connected. Query Store replica " + Replica + ": connected.", ok, StringComparison.Ordinal);
        Assert.DoesNotContain("[WARN]", ok, StringComparison.Ordinal);

        var warned = DarlingCliCommands.FormatProbeLine("alpha", Probe(Replica, DriverText));
        Assert.StartsWith("  [PASS] alpha:", warned, StringComparison.Ordinal);
        Assert.DoesNotContain("[FAIL]", warned, StringComparison.Ordinal);
        Assert.Contains("[WARN] alpha: Primary: connected. Query Store replica " + Replica + ": could not connect (" + DriverText + ")", warned, StringComparison.Ordinal);
        Assert.Contains("reads Query Store from the primary", warned, StringComparison.Ordinal);
    }

    /* ---------------- the test_connect reply the Viewer reads ---------------- */

    [Fact]
    public void TheTestConnectReply_CarriesTheReplicaResult_StaysConnectedWhenTheReplicaFails_AndTheViewerReadsTheNote()
    {
        var (status, json) = DarlingCommandExecutor.MapProbeResult(Probe(Replica, DriverText));
        Assert.Equal("connected", status);
        using var failed = JsonDocument.Parse(json);
        Assert.Equal(Replica, failed.RootElement.GetProperty("queryStoreReplica").GetString());
        Assert.Equal(DriverText, failed.RootElement.GetProperty("queryStoreReplicaError").GetString());
        var failedNote = ViewerDataService.ProbeReplicaNote(failed.RootElement);
        Assert.NotNull(failedNote);
        Assert.Contains("Query Store replica " + Replica + ": could not connect (" + DriverText + ")", failedNote, StringComparison.Ordinal);

        var (_, okJson) = DarlingCommandExecutor.MapProbeResult(Probe(Replica, null));
        using var ok = JsonDocument.Parse(okJson);
        Assert.Equal("Query Store replica " + Replica + ": connected.", ViewerDataService.ProbeReplicaNote(ok.RootElement));

        var (_, noneJson) = DarlingCommandExecutor.MapProbeResult(Probe(null, null));
        using var none = JsonDocument.Parse(noneJson);
        Assert.Null(ViewerDataService.ProbeReplicaNote(none.RootElement));
        /* A reply from a service that predates the field says nothing about a replica. */
        Assert.Null(ViewerDataService.ProbeReplicaNote(JsonDocument.Parse("""{"success":true,"majorVersion":16}""").RootElement));
    }

    /* ---------------- add_servers and edit_server ---------------- */

    private static readonly Core.ServerProbe ReplicaDown = (_, _) => Task.FromResult(Probe(Replica, DriverText + " password=" + Password));

    private sealed class SavingDefinitions : Core.IServerDefinitions
    {
        public Task<List<Core.RegisteredTarget>> LoadTargetsAsync(CancellationToken cancellationToken) => Task.FromResult(new List<Core.RegisteredTarget>());

        public Task<int?> ScopedAgentRouteAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<int?>(null);

        public Task<List<string>> LoadStorageKeysAsync(CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<string?> ReadStorageKeyAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<string?>(null);

        public Task<int> InsertAsync(Core.ParsedServerEntry entry, string? encryptedPassword, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken) =>
            Task.FromResult(1);
    }

    private static string AddBody() => JsonSerializer.Serialize(new[]
    {
        new { host = "sql-test-01,1444", auth = "SQL", username = "monitor", password = Password },
    });

    private static JsonNode AddedResult(string answer) => JsonNode.Parse(answer)!["results"]![0]!;

    [Fact]
    public async Task McpAdd_AReplicaThatWillNotOpen_IsANoteOnAnAddedServer_WithoutTheDriverText_AndTheTextIsLogged()
    {
        var log = new CapturingLogger();

        var answer = await Core.AddServersAsync(
            new SavingDefinitions(), AddBody(), ReplicaDown, CancellationToken.None, ring: TestKeyRings.Healthy, logger: log);

        var result = AddedResult(answer);
        Assert.Equal("added", result["status"]!.GetValue<string>());
        var detail = result["detail"]!.GetValue<string>();
        Assert.Contains("Query Store replica " + Replica + ": could not connect.", detail, StringComparison.Ordinal);
        Assert.Contains("reads Query Store from the primary", detail, StringComparison.Ordinal);
        Assert.DoesNotContain("Distinctive-Replica-Text", detail, StringComparison.Ordinal);
        Assert.DoesNotContain("svc_probe", detail, StringComparison.Ordinal);
        Assert.DoesNotContain(Password, answer, StringComparison.Ordinal);
        Assert.Contains(log.Lines, l => l.Contains("Distinctive-Replica-Text", StringComparison.Ordinal));
        Assert.DoesNotContain(Password, string.Join("\n", log.Lines), StringComparison.Ordinal);
    }

    [Fact]
    public async Task McpAdd_AReachableReplica_IsNamedConnected_AndAServerWithNoReplicaSaysNothingAboutOne()
    {
        var withReplica = await Core.AddServersAsync(
            new SavingDefinitions(), AddBody(), (_, _) => Task.FromResult(Probe(Replica, null)), CancellationToken.None, ring: TestKeyRings.Healthy);
        Assert.Contains("Query Store replica " + Replica + ": connected.", AddedResult(withReplica)["detail"]!.GetValue<string>(), StringComparison.Ordinal);

        var without = await Core.AddServersAsync(
            new SavingDefinitions(), AddBody(), (_, _) => Task.FromResult(Probe(null, null)), CancellationToken.None, ring: TestKeyRings.Healthy);
        Assert.DoesNotContain("replica", AddedResult(without)["detail"]!.GetValue<string>(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TheAddServerVerb_ShowsTheDriverTextOfAReplicaThatWillNotOpen_WithThePasswordRemoved()
    {
        var answer = await Core.AddServersAsync(
            new SavingDefinitions(), AddBody(), ReplicaDown, CancellationToken.None, ring: TestKeyRings.Healthy, revealConnectError: true);

        var detail = AddedResult(answer)["detail"]!.GetValue<string>();
        Assert.Contains("Distinctive-Replica-Text", detail, StringComparison.Ordinal);
        Assert.DoesNotContain(Password, detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task McpEdit_AReplicaThatWillNotOpen_IsANoteOnASavedEdit_NotARefusal()
    {
        var store = new OneRowStore();
        var log = new CapturingLogger();
        var body = "{\"host\":\"sql-02.example.test\",\"password\":\"" + Password + "\"}";

        var answer = await Core.EditServerCoreAsync(store, 41, body, ReplicaDown, TestKeyRings.Healthy, log, CancellationToken.None);

        var reply = JsonNode.Parse(answer)!;
        Assert.Equal("updated", reply["status"]!.GetValue<string>());
        Assert.True(reply["tested"]!.GetValue<bool>());
        var note = reply["query_store_replica_test"]!.GetValue<string>();
        Assert.Contains("Query Store replica " + Replica + ": could not connect.", note, StringComparison.Ordinal);
        Assert.DoesNotContain("Distinctive-Replica-Text", answer, StringComparison.Ordinal);
        Assert.DoesNotContain(Password, answer, StringComparison.Ordinal);
        Assert.Equal(1, store.Writes);
        Assert.Contains(log.Lines, l => l.Contains("Distinctive-Replica-Text", StringComparison.Ordinal));
    }

    [Fact]
    public async Task McpEdit_AServerWithNoReplica_CarriesNoReplicaNote()
    {
        var store = new OneRowStore();
        var body = "{\"host\":\"sql-02.example.test\",\"password\":\"" + Password + "\"}";

        var answer = await Core.EditServerCoreAsync(
            store, 41, body, (_, _) => Task.FromResult(Probe(null, null)), TestKeyRings.Healthy, null, CancellationToken.None);

        var reply = JsonNode.Parse(answer)!;
        Assert.Equal("updated", reply["status"]!.GetValue<string>());
        Assert.Null(reply["query_store_replica_test"]);
    }

    /* ---------------- stubs ---------------- */

    private static readonly DateTime Stamp = new(2026, 10, 5, 12, 0, 0, 123, DateTimeKind.Unspecified);

    private sealed class OneRowStore : Core.IServerEditStore
    {
        public int Writes { get; private set; }

        public Core.ServerEditRow Row { get; } = new(
            ServerId: 41, Name: "alpha-01", Host: "alpha-01.example.test", Port: 0, Database: null, ReadOnlyIntent: false, Engine: "sqlserver",
            Auth: "sql", Username: "monitor", EncryptMode: "Mandatory", TrustServerCertificate: false,
            MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: Stamp.AddTicks(7));

        public Task<Core.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<Core.ServerEditRow?>(Row);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<Core.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Core.EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken)
        {
            Writes++;
            return Task.FromResult(new Core.ServerEditWrite(Core.ServerEditWriteKind.Written, Stamp.AddSeconds(1)));
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }

    /// <summary>A connection that records the string it was given and either opens or throws; nothing else is used by the probe.</summary>
    private sealed class RecordingConnection(string connectionString, List<string> opened, Exception? failure) : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get; set; } = connectionString;

        public override string Database => "";

        public override string DataSource => "";

        public override string ServerVersion => "16.0";

        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) { }

        public override void Close() => _state = ConnectionState.Closed;

        public override void Open()
        {
            opened.Add(ConnectionString);
            if (failure is not null)
            {
                throw failure;
            }

            _state = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }
}
