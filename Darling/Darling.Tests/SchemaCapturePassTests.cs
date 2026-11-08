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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643: the <c>schema_objects</c> pass of one server (<see cref="DarlingSchemaCapture"/>) on a live store, with the SQL Server side
/// replaced by a script: what the session reports, which databases exist and what each catalog holds. Each test mints its own store.
/// The live SQL Server end to end is <c>SchemaCaptureEndToEndLiveTests</c>.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class SchemaCapturePassTests
{
    private const string InstallIdValue = "0a1b2c3d";
    private const string Sales = "SalesDb";
    private const string Reports = "ReportsDb";
    private static readonly DateTime s_created = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime s_t0 = new(2026, 11, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly Guid s_group = new("11111111-2222-3333-4444-555555555555");

    /// <summary>The SQL Server side: the session, the databases, their catalogs and the events the ring buffer holds.</summary>
    private sealed class FakeSource : ISchemaCaptureSource
    {
        public SchemaSessionState Session { get; set; } = new(true, s_created, null, false);
        public Dictionary<string, SchemaSessionState> AzureSessions { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<SchemaDatabaseProbe> Probes { get; } = new();
        public Dictionary<string, List<SchemaObjectDefinition>> Catalog { get; } = new(StringComparer.Ordinal);
        public HashSet<string> OverCap { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> NoViewDefinition { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, Exception> Faults { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<SchemaEvent> Ring { get; } = new();
        public List<(string Database, IReadOnlyCollection<int>? Ids)> Reads { get; } = new();
        public List<DateTime> Cutoffs { get; } = new();
        public Exception? EventsFault { get; set; }
        public CancellationTokenSource? StopOnCatalogRead { get; set; }

        public void AddEvent(string name, string database, int? objectId, int? type = 8277, int? related = null)
        {
            var sequence = Ring.Count + 1;
            Ring.Add(new SchemaEvent(name, sequence, s_t0.AddMinutes(sequence), database, objectId, type, null, null, "app_login", "App", "host", related));
        }

        public Task<ISchemaCaptureConnection> OpenAsync(ServerRuntime server, string? azureDatabase, CancellationToken cancellationToken)
        {
            return Task.FromResult<ISchemaCaptureConnection>(new Connection(this, azureDatabase));
        }

        private sealed class Connection : ISchemaCaptureConnection
        {
            private readonly FakeSource _source;
            private readonly string? _azure;

            public Connection(FakeSource source, string? azure)
            {
                _source = source;
                _azure = azure;
            }

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            private SchemaSessionState Session => _azure is not null && _source.AzureSessions.TryGetValue(_azure, out var s) ? s : _source.Session;

            public Task<SchemaSessionState> EnsureSessionAsync(string sessionName, CancellationToken cancellationToken) => Task.FromResult(Session);

            public Task<IReadOnlyList<SchemaDatabaseProbe>> ReadProbesAsync(CancellationToken cancellationToken) =>
                Task.FromResult<IReadOnlyList<SchemaDatabaseProbe>>(
                    _azure is null ? _source.Probes.ToList() : _source.Probes.Where(p => string.Equals(p.Name, _azure, StringComparison.OrdinalIgnoreCase)).ToList());

            public Task<SchemaEventBatch> ReadEventsAsync(string sessionName, DateTime cutoffUtc, CancellationToken cancellationToken)
            {
                _source.Cutoffs.Add(cutoffUtc);
                if (_source.EventsFault is { } eventsFault)
                {
                    throw eventsFault;
                }

                var events = _source.Ring
                    .Where(e => e.TimeUtc > cutoffUtc && (_azure is null || string.Equals(e.DatabaseName, _azure, StringComparison.OrdinalIgnoreCase)))
                    .ToList();
                var total = _source.Ring.Count(e => _azure is null || string.Equals(e.DatabaseName, _azure, StringComparison.OrdinalIgnoreCase));
                return Task.FromResult(new SchemaEventBatch(Session.CreateTimeUtc, total, false, events));
            }

            public Task<SchemaCatalogSnapshot> ReadCatalogAsync(string database, IReadOnlyCollection<int>? objectIds, CancellationToken cancellationToken)
            {
                _source.Reads.Add((database, objectIds));
                if (_source.StopOnCatalogRead is { } stop)
                {
                    stop.Cancel();
                    stop.Token.ThrowIfCancellationRequested();
                }

                if (_source.Faults.TryGetValue(database, out var fault))
                {
                    throw fault;
                }

                var objects = _source.Catalog.TryGetValue(database, out var all) ? all : new List<SchemaObjectDefinition>();
                if (objectIds is not null)
                {
                    objects = objects.Where(o => objectIds.Contains(o.ObjectId)).ToList();
                }

                return Task.FromResult(new SchemaCatalogSnapshot(!_source.NoViewDefinition.Contains(database), _source.OverCap.Contains(database), objects));
            }
        }
    }

    private sealed class Rig
    {
        public Rig(NpgsqlDataSource store, NpgsqlConnection connection)
        {
            Connection = connection;
            Source = new FakeSource();
            Scope = new List<string>();
            Runner = new DarlingCollectorRunner(
                store,
                new CollectorDeltaCalculator(),
                databaseScope: (_, _) => Scope,
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => InstallIdValue);
            Runner.SchemaSource = Source;
        }

        public NpgsqlConnection Connection { get; }
        public FakeSource Source { get; }
        public DarlingCollectorRunner Runner { get; }
        public List<string> Scope { get; set; }

        public ServerRuntime Server(string host = "schemapass-sql", bool azure = false, params string[] excluded)
        {
            var config = new MonitoredServer { Name = host, Host = host, ExcludedDatabases = excluded.ToList() };
            return new ServerRuntime
            {
                Config = config,
                ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
                Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
                StorageName = config.StorageName,
                ServerId = config.ServerId,
            };
        }

        public Task<CollectorRunResult> PassAsync(ServerRuntime server, CancellationToken ct) => DarlingSchemaCapture.RunAsync(Runner, server, ct);

        public async Task<CollectorRunResult> RunNowAsync(ServerRuntime server, CancellationToken ct)
        {
            using (DarlingSnapshotScope.Enter())
            {
                return await DarlingSchemaCapture.RunAsync(Runner, server, ct);
            }
        }

        public async Task<string> ScalarAsync(string sql, CancellationToken ct) => await ActionProposalStoreLiveTests.ScalarAsync(Connection, sql, ct);

    }

    private static async Task WithRigAsync(Func<Rig, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the schema pass tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await body(new Rig(store, connection), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static SchemaDatabaseProbe Probe(
        string name, int id, string state = "ONLINE", bool readOnly = false, Guid? group = null, bool? primary = null, bool access = true) =>
        new(name, id, state, readOnly, false, null, access, null, group, group is null ? null : "AgOne", primary);

    private static SchemaObjectDefinition Table(int id, string name, params string[] columns) =>
        new(
            id, "dbo", name, 'U', s_t0,
            new SchemaTableProperties(null, null, false, null, null, null, "TABLE"), null,
            columns.Select((n, i) => new SchemaColumn(i + 1, n, "int", 4, 10, 0, false, null, null, null, null, null, null, null, null, null)).ToList(),
            new List<SchemaIndex> { new(1, "PK_Main", "CLUSTERED", true, true, false, new[] { columns[0] }, Array.Empty<string>(), null, "NONE", false) },
            new List<SchemaConstraint> { new('P', "PK_Main", null, Array.Empty<string>(), null, null, Array.Empty<string>(), null, null, false, false, false) },
            new List<SchemaTrigger>());

    private static void Seed(FakeSource source)
    {
        source.Probes.Add(Probe(Sales, 5));
        source.Probes.Add(Probe(Reports, 6));
        source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total"), Table(101, "Customers", "Id")];
        source.Catalog[Reports] = [Table(200, "Monthly", "Id")];
    }

    private static Task<string> Versions(Rig rig, string where, CancellationToken ct) =>
        rig.ScalarAsync("SELECT count(*) FROM collect.schema_objects WHERE " + where, ct);

    [Fact]
    public async Task Pass_FirstRun_BaselinesEveryInScopeWritableDatabase()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe("ReportServer", 7));
            rig.Source.Probes.Add(Probe("FrozenDb", 8, readOnly: true));
            rig.Source.Probes.Add(Probe("MirrorDb", 9, group: s_group, primary: false));
            rig.Source.Catalog["ReportServer"] = [Table(300, "Skipped", "Id")];
            rig.Source.Catalog["FrozenDb"] = [Table(400, "Frozen", "Id")];
            rig.Source.Catalog["MirrorDb"] = [Table(500, "Mirrored", "Id")];
            var server = rig.Server();

            var result = await rig.PassAsync(server, ct);

            Assert.Equal(new[] { Reports, Sales }, rig.Source.Reads.Select(r => r.Database).Order().ToArray());
            Assert.All(rig.Source.Reads, r => Assert.Null(r.Ids));
            Assert.Equal("3", await Versions(rig, "change_kind = 'baseline' AND detected_by = 'baseline' AND valid_to IS NULL", ct));
            Assert.Equal("0", await Versions(rig, "database_name NOT IN ('SalesDb', 'ReportsDb')", ct));
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE pass_kind = 'baseline' AND complete AND versions_written > 0", ct));
            Assert.Equal("3", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_state WHERE server_id = " + server.ServerId + " AND last_seen_in_scope_at IS NOT NULL", ct));
            Assert.Equal(2, result.Rows);
            Assert.Contains("FrozenDb", result.HostNote);
            Assert.Contains("read-only", result.HostNote);
            Assert.Contains("MirrorDb", result.HostNote);
            Assert.DoesNotContain("ReportServer", result.HostNote);
        });
    }

    [Fact]
    public async Task Pass_NoEvents_QueriesNoDatabase()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            rig.Source.Reads.Clear();

            var result = await rig.PassAsync(server, ct);

            Assert.Empty(rig.Source.Reads);
            Assert.Equal(0, result.Rows);
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes", ct));
            Assert.Null(result.HostNote);
        });
    }

    [Fact]
    public async Task Pass_IncompleteRead_RecordsNoDrops()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);

            /* Both databases now report one object fewer, but over the cap in one and without VIEW DEFINITION in the other. */
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total")];
            rig.Source.Catalog[Reports] = [];
            rig.Source.OverCap.Add(Sales);
            rig.Source.NoViewDefinition.Add(Reports);
            var result = await rig.RunNowAsync(server, ct);

            Assert.Equal("0", await Versions(rig, "change_kind = 'dropped'", ct));
            Assert.Equal("3", await Versions(rig, "valid_to IS NULL", ct));
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE pass_kind = 'reconcile' AND NOT complete AND drops_written = 0 AND note IS NOT NULL", ct));
            Assert.Contains("only the first were read", result.HostNote);
            Assert.Contains("VIEW DEFINITION", result.HostNote);

            /* The same two reads, complete, now record the drops. */
            rig.Source.OverCap.Clear();
            rig.Source.NoViewDefinition.Clear();
            await rig.RunNowAsync(server, ct);
            Assert.Equal("2", await Versions(rig, "change_kind = 'dropped'", ct));
        });
    }

    [Fact]
    public async Task Pass_SessionDenied_BaselinesAndNotesTheGrant()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Session = new SchemaSessionState(false, null, "Permission denied (error 262).", true);
            var server = rig.Server();

            var first = await rig.PassAsync(server, ct);

            Assert.Equal("3", await Versions(rig, "change_kind = 'baseline' AND valid_to IS NULL", ct));
            Assert.Contains("ALTER ANY EVENT SESSION", first.HostNote);
            Assert.DoesNotContain("ALTER ANY DATABASE EVENT SESSION", first.HostNote);
            Assert.Contains("run_collectors_now", first.HostNote);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_state WHERE session_create_time IS NOT NULL", ct));

            /* Nothing is read while the session is down, and the note stays. Run now still reads. */
            rig.Source.Reads.Clear();
            var second = await rig.PassAsync(server, ct);
            Assert.Empty(rig.Source.Reads);
            Assert.Contains("ALTER ANY EVENT SESSION", second.HostNote);
            rig.Source.Catalog[Reports] = [Table(200, "Monthly", "Id", "Extra")];
            await rig.RunNowAsync(server, ct);
            Assert.Equal("1", await Versions(rig, "change_kind = 'altered' AND detected_by = 'run_now'", ct));
        });
    }

    [Fact]
    public async Task Pass_SecondaryThenPrimary_ContinuesTheAgHistoryWithOneReconcile()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            var serverA = rig.Server("schemapass-a");
            var serverB = rig.Server("schemapass-b");
            Assert.NotEqual(serverA.ServerId, serverB.ServerId);
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total"), Table(101, "Customers", "Id")];

            /* A holds the primary and baselines the group's history. */
            rig.Source.Probes.Add(Probe(Sales, 5, group: s_group, primary: true));
            await rig.PassAsync(serverA, ct);
            Assert.Equal("2", await Versions(rig, $"ag_group_id = '{s_group}' AND valid_to IS NULL AND detected_on_server_id = {serverA.ServerId}", ct));

            /* B is the secondary: nothing is read from it. */
            rig.Source.Probes.Clear();
            rig.Source.Probes.Add(Probe(Sales, 5, group: s_group, primary: false));
            rig.Source.Reads.Clear();
            var secondary = await rig.PassAsync(serverB, ct);
            Assert.Empty(rig.Source.Reads);
            Assert.Contains("secondary replica", secondary.HostNote);

            /* B becomes the primary, with one table altered meanwhile: one full read, the history continues, one version is written. */
            rig.Source.Probes.Clear();
            rig.Source.Probes.Add(Probe(Sales, 5, group: s_group, primary: true));
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total", "Extra"), Table(101, "Customers", "Id")];
            await rig.PassAsync(serverB, ct);

            Assert.Single(rig.Source.Reads);
            Assert.Null(rig.Source.Reads[0].Ids);
            Assert.Equal("1", await Versions(rig, $"ag_group_id = '{s_group}' AND change_kind = 'altered' AND detected_on_server_id = {serverB.ServerId}", ct));
            Assert.Equal("2", await Versions(rig, $"ag_group_id = '{s_group}' AND valid_to IS NULL", ct));
            Assert.Equal("0", await Versions(rig, "server_id IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task Pass_RunNow_ReadsEveryDatabaseWithDetectedByRunNow()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            rig.Source.Reads.Clear();
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total", "Extra"), Table(101, "Customers", "Id")];

            await rig.RunNowAsync(server, ct);

            Assert.Equal(new[] { Reports, Sales }, rig.Source.Reads.Select(r => r.Database).Order().ToArray());
            Assert.All(rig.Source.Reads, r => Assert.Null(r.Ids));
            Assert.Equal("1", await Versions(rig, "change_kind = 'altered' AND detected_by = 'run_now' AND database_name = 'SalesDb'", ct));
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE pass_kind = 'reconcile' AND reason = 'run_now'", ct));
        });
    }

    [Fact]
    public async Task Pass_DisabledCollector_DropsTheSession()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            var drops = new List<(string Database, string Session)>();
            rig.Runner.SchemaSessionDropForTests = (_, database, session, _) =>
            {
                drops.Add((database, session));
                return Task.CompletedTask;
            };
            Task<LongQueryTraceInstanceGuard> NoGuard(CancellationToken _) => Task.FromResult(LongQueryTraceInstanceGuard.NoKeepers);
            Task Disabled() => DarlingSchemaCapture.DropWhenDisabledAsync(
                rig.Runner, server, Array.Empty<MonitoredServer>(), _ => false, NoGuard, NullLogger.Instance, ct);

            /* A server that never ran the collector is not touched, and is not asked about again. */
            await Disabled();
            Assert.Empty(drops);
            Assert.True(rig.Runner.SchemaSessionSettled(server.ServerId));

            /* After a pass the store shows a session; the disabled sweep drops it once and the store forgets it. */
            await rig.PassAsync(server, ct);
            Assert.False(rig.Runner.SchemaSessionSettled(server.ServerId));
            Assert.True(await SchemaCaptureStateStore.HasSessionAsync(rig.Connection, server.ServerId, ct));
            await Disabled();
            await Disabled();

            var dropped = Assert.Single(drops);
            Assert.Equal(string.Empty, dropped.Database);
            Assert.Equal(SchemaChangesXeSession.NameFor(InstallIdValue), dropped.Session);
            Assert.False(await SchemaCaptureStateStore.HasSessionAsync(rig.Connection, server.ServerId, ct));

            /* Switched on again, the next pass reconciles each database for what no session recorded meanwhile. */
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Equal(2, rig.Source.Reads.Count);
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE reason = 'session_started'", ct));
        });
    }

    [Fact]
    public async Task Pass_SessionGrantedLater_ReconcilesAndRecordsTheAlterMadeMeanwhile()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Session = new SchemaSessionState(false, null, "Permission denied (error 262).", true);
            var server = rig.Server();
            await rig.PassAsync(server, ct);

            /* A table changes while no session runs; then the grant is fixed and the session starts. */
            rig.Source.Catalog[Reports] = [Table(200, "Monthly", "Id", "Extra")];
            rig.Source.Session = new SchemaSessionState(true, s_created, null, false);
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);

            Assert.Equal(2, rig.Source.Reads.Count);
            Assert.Equal("1", await Versions(rig, "change_kind = 'altered' AND detected_by = 'reconcile' AND database_name = 'ReportsDb'", ct));
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE reason = 'session_started'", ct));

            /* Once, not on every pass. */
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Empty(rig.Source.Reads);
        });
    }

    /// <summary>
    /// The pass makes the session and then fails before it stores the session's position: the event read fails, or the service stops
    /// during the baseline. The collector is then disabled, and the sweep still drops the session.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Pass_SessionMadeThenThePassFails_TheDisabledSweepStillDropsIt(bool eventReadFails)
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            var drops = 0;
            rig.Runner.SchemaSessionDropForTests = (_, _, _, _) =>
            {
                drops++;
                return Task.CompletedTask;
            };
            Task<LongQueryTraceInstanceGuard> NoGuard(CancellationToken _) => Task.FromResult(LongQueryTraceInstanceGuard.NoKeepers);

            if (eventReadFails)
            {
                rig.Source.EventsFault = new InvalidOperationException("The ring buffer could not be read.");
                await rig.PassAsync(server, ct);
            }
            else
            {
                using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
                rig.Source.StopOnCatalogRead = stop;
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rig.PassAsync(server, stop.Token));
            }

            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_state WHERE session_create_time IS NOT NULL", ct));
            await DarlingSchemaCapture.DropWhenDisabledAsync(rig.Runner, server, Array.Empty<MonitoredServer>(), _ => false, NoGuard, NullLogger.Instance, ct);

            Assert.Equal(1, drops);
            Assert.False(await SchemaCaptureStateStore.HasSessionAsync(rig.Connection, server.ServerId, ct));
        });
    }

    /// <summary>
    /// The collector was on for this one server only, and the server is removed after a pass. The removal transaction deletes the server's
    /// own schedule overrides, so its collector reads as off by the time the reload looks; the session is dropped all the same.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Pass_ServerRemovedAfterAPass_DropsTheSession_ThoughItsOverridesAreGone(bool azure)
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server(azure: azure);
            if (azure)
            {
                rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Sales });
                rig.Runner.LongQueryTraceListOverrideForTests = (_, _, _, _) => Task.FromResult(new List<string> { "master", Sales });
            }

            var drops = new List<string>();
            rig.Runner.SchemaSessionDropForTests = (_, database, _, _) =>
            {
                drops.Add(database);
                return Task.CompletedTask;
            };
            await rig.PassAsync(server, ct);

            /* What the reload does: the server is gone from the registry, its trace was never on, and no override turns the collector on. */
            var states = new List<DarlingWorker.ServerLoopState> { new() { Config = server.Config, Runtime = server, LongQueryTraceApplied = false } };
            Func<int, bool> overridesGone = _ => false;
            var recorded = await DarlingRemovedServerSessions.RecordedSchemaSessionsAsync(
                rig.Runner, states, Array.Empty<MonitoredServer>(), NullLogger.Instance, ct);
            var removed = DarlingRemovedServerSessions.Capture(states, Array.Empty<MonitoredServer>(), rig.Runner, recorded);
            foreach (var each in removed)
            {
                await DarlingRemovedServerSessions.DropAsync(
                    each, rig.Runner, Array.Empty<MonitoredServer>(), overridesGone, _ => Task.FromResult(LongQueryTraceInstanceGuard.NoKeepers),
                    NullLogger.Instance, ct, overridesGone, _ => Task.FromResult(LongQueryTraceInstanceGuard.NoKeepers));
            }

            Assert.Equal(azure ? [Sales] : [string.Empty], drops);

            /* A server the store shows no session for is not captured for it. */
            var other = rig.Server(host: "schemapass-other");
            var otherStates = new List<DarlingWorker.ServerLoopState> { new() { Config = other.Config, Runtime = other, LongQueryTraceApplied = false } };
            var none = await DarlingRemovedServerSessions.RecordedSchemaSessionsAsync(
                rig.Runner, otherStates, Array.Empty<MonitoredServer>(), NullLogger.Instance, ct);
            Assert.Empty(DarlingRemovedServerSessions.Capture(otherStates, Array.Empty<MonitoredServer>(), rig.Runner, none));
        });
    }

    [Fact]
    public async Task Pass_DisabledCollector_FailedDropIsRetriedOnTheNextSweep()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            var attempts = 0;
            rig.Runner.SchemaSessionDropForTests = (_, _, _, _) =>
            {
                attempts++;
                return attempts == 1 ? throw new InvalidOperationException("The drop was refused.") : Task.CompletedTask;
            };
            Task<LongQueryTraceInstanceGuard> NoGuard(CancellationToken _) => Task.FromResult(LongQueryTraceInstanceGuard.NoKeepers);
            await rig.PassAsync(server, ct);

            await DarlingSchemaCapture.DropWhenDisabledAsync(rig.Runner, server, Array.Empty<MonitoredServer>(), _ => false, NoGuard, NullLogger.Instance, ct);
            Assert.True(await SchemaCaptureStateStore.HasSessionAsync(rig.Connection, server.ServerId, ct));
            await DarlingSchemaCapture.DropWhenDisabledAsync(rig.Runner, server, Array.Empty<MonitoredServer>(), _ => false, NoGuard, NullLogger.Instance, ct);

            Assert.Equal(2, attempts);
            Assert.False(await SchemaCaptureStateStore.HasSessionAsync(rig.Connection, server.ServerId, ct));
        });
    }

    [Fact]
    public async Task Pass_DatabaseOutOfScope_IsNeitherReadNorDropped()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            rig.Source.Reads.Clear();

            /* ReportsDb leaves the collector's scope and loses its table meanwhile: a run-now pass reads only SalesDb. */
            rig.Scope = new List<string> { Sales };
            rig.Source.Catalog[Reports] = [];
            await rig.RunNowAsync(server, ct);
            Assert.Equal(new[] { Sales }, rig.Source.Reads.Select(r => r.Database).ToArray());
            Assert.Equal("0", await Versions(rig, "change_kind = 'dropped'", ct));
            Assert.Equal("1", await Versions(rig, "database_name = 'ReportsDb' AND valid_to IS NULL", ct));

            /* An excluded database is out of scope the same way. */
            rig.Scope = new List<string>();
            rig.Source.Reads.Clear();
            await rig.RunNowAsync(rig.Server("schemapass-sql", false, Reports), ct);
            Assert.Equal(new[] { Sales }, rig.Source.Reads.Select(r => r.Database).ToArray());
            Assert.Equal("0", await Versions(rig, "change_kind = 'dropped'", ct));

            /* Back in scope it is reconciled, because the pass that left it stamped nothing for it, and the drop is recorded then. */
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Equal(new[] { Reports }, rig.Source.Reads.Select(r => r.Database).ToArray());
            Assert.Equal("1", await Versions(rig, "change_kind = 'dropped' AND database_name = 'ReportsDb'", ct));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE reason = 'returned_to_scope'", ct));
        });
    }

    [Fact]
    public async Task Pass_QuietDatabase_IsNotReconciledOnTheNextPass()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            rig.Source.Reads.Clear();

            /* One database changes, the other is quiet and is read not at all. */
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total", "Extra"), Table(101, "Customers", "Id")];
            rig.Source.AddEvent("object_altered", Sales, 100);
            await rig.PassAsync(server, ct);
            var targeted = Assert.Single(rig.Source.Reads);
            Assert.Equal(Sales, targeted.Database);
            Assert.Equal(new[] { 100 }, targeted.Ids!.ToArray());
            Assert.Equal("1", await Versions(rig, "change_kind = 'altered' AND detected_by = 'xe_event'", ct));

            /* The pass after it has nothing to read: the quiet database was stamped as seen, so it does not look like one that left scope.
               Its event query starts a second before the last event it stored, and the first one read everything. */
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Empty(rig.Source.Reads);
            Assert.Equal(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc), rig.Source.Cutoffs[0]);
            Assert.Equal(s_t0.AddMinutes(1).AddSeconds(-1), rig.Source.Cutoffs[^1]);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE reason = 'returned_to_scope'", ct));
        });
    }

    [Fact]
    public async Task Pass_DatabaseReadFails_IsReconciledOnTheNextPass()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            var stateBefore = await rig.ScalarAsync($"SELECT last_seen_in_scope_at FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name = 'SalesDb'", ct);

            /* Both databases have an event; the read of SalesDb fails, the one of ReportsDb works. */
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total", "Extra"), Table(101, "Customers", "Id")];
            rig.Source.Catalog[Reports] = [Table(200, "Monthly", "Id", "Extra")];
            rig.Source.AddEvent("object_altered", Sales, 100);
            rig.Source.AddEvent("object_altered", Reports, 200);
            rig.Source.Faults[Sales] = new InvalidOperationException("The read was refused.");
            var failed = await rig.PassAsync(server, ct);

            Assert.Contains("SalesDb", failed.HostNote);
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = 'SalesDb' AND NOT complete AND note LIKE 'The read did not finish%'", ct));
            Assert.Equal(stateBefore, await rig.ScalarAsync($"SELECT last_seen_in_scope_at FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name = 'SalesDb'", ct));
            /* The server's event position moved on, which is why the database needs the reconcile. */
            Assert.Equal("2", await rig.ScalarAsync($"SELECT last_event_sequence FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name = ''", ct));

            /* The next pass finds the session down (a probe fault while it still runs): the owed full read needs no session, so it runs. */
            var running = rig.Source.Session;
            rig.Source.Session = new SchemaSessionState(false, null, "The probe timed out.", false);
            rig.Source.Faults.Clear();
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);

            var read = Assert.Single(rig.Source.Reads);
            Assert.Equal(Sales, read.Database);
            Assert.Null(read.Ids);
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = 'SalesDb' AND reason = 'returned_to_scope' AND complete", ct));
            Assert.Equal("1", await Versions(rig, "database_name = 'SalesDb' AND change_kind = 'altered'", ct));

            /* The session is seen again, the same one: nothing is owed. */
            rig.Source.Session = running;
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Empty(rig.Source.Reads);
        });
    }

    [Fact]
    public async Task Pass_DatabaseJoinsAnAvailabilityGroup_StartsTheGroupHistoryWithABaseline()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);

            /* SalesDb joins a group, as its primary. */
            rig.Source.Probes[0] = Probe(Sales, 5, group: s_group, primary: true);
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);

            var read = Assert.Single(rig.Source.Reads);
            Assert.Null(read.Ids);
            Assert.Equal("2", await Versions(rig, $"ag_group_id = '{s_group}' AND change_kind = 'baseline' AND valid_to IS NULL", ct));
            Assert.Equal("0", await Versions(rig, "change_kind = 'created'", ct));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = 'SalesDb' AND pass_kind = 'baseline' AND reason = 'history_key_changed'", ct));
            Assert.Equal(s_group.ToString(), await rig.ScalarAsync($"SELECT history_ag_group_id::text FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name = 'SalesDb'", ct));

            /* The server history's current versions are closed at the pass, children too, so they age out with the retention window. */
            Assert.Equal("0", await Versions(rig, $"server_id = {server.ServerId} AND database_name = 'SalesDb' AND valid_to IS NULL", ct));
            Assert.Equal("2", await Versions(rig, $"server_id = {server.ServerId} AND database_name = 'SalesDb' AND valid_to = (SELECT max(valid_from) FROM collect.schema_objects WHERE ag_group_id = '{s_group}')", ct));
            Assert.Equal("0", await rig.ScalarAsync($"SELECT count(*) FROM collect.schema_columns c JOIN collect.schema_objects o USING (version_id) WHERE o.server_id = {server.ServerId} AND o.database_name = 'SalesDb' AND c.is_current", ct));

            /* Once: the pass after it has nothing to read. */
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Empty(rig.Source.Reads);
        });
    }

    [Fact]
    public async Task Pass_DatabaseLeavesItsAvailabilityGroup_StartsTheServerHistoryWithABaseline()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            rig.Source.Probes.Add(Probe(Sales, 5, group: s_group, primary: true));
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total"), Table(101, "Customers", "Id")];
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            Assert.Equal("2", await Versions(rig, $"ag_group_id = '{s_group}' AND valid_to IS NULL", ct));

            /* The group is removed and the former primary's copy stays online as a standalone database. */
            rig.Source.Probes[0] = Probe(Sales, 5);
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);

            Assert.Null(Assert.Single(rig.Source.Reads).Ids);
            Assert.Equal("2", await Versions(rig, $"server_id = {server.ServerId} AND change_kind = 'baseline' AND valid_to IS NULL", ct));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = 'SalesDb' AND pass_kind = 'baseline' AND reason = 'history_key_changed'", ct));
            Assert.Equal(server.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), await rig.ScalarAsync($"SELECT history_server_id::text FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name = 'SalesDb'", ct));
            Assert.Equal("0", await Versions(rig, $"ag_group_id = '{s_group}' AND valid_to IS NULL", ct));
        });
    }

    /// <summary>
    /// A database that leaves the group on this server while another server of the group still writes the group's history: the group's
    /// current versions are that server's live history, so this server leaves them open.
    /// </summary>
    [Fact]
    public async Task Pass_DatabaseLeavesTheGroupHereWhileAnotherServerKeepsIt_LeavesTheGroupHistoryOpen()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            rig.Source.Probes.Add(Probe(Sales, 5, group: s_group, primary: true));
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total"), Table(101, "Customers", "Id")];
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            await rig.ScalarAsync($@"INSERT INTO collect.schema_capture_state (server_id, database_name, history_ag_group_id, last_seen_in_scope_at)
VALUES ({server.ServerId + 1}, 'SalesDb', '{s_group}', now()) RETURNING 1", ct);

            rig.Source.Probes[0] = Probe(Sales, 5);
            await rig.PassAsync(server, ct);

            Assert.Equal("2", await Versions(rig, $"ag_group_id = '{s_group}' AND valid_to IS NULL", ct));
        });
    }

    [Fact]
    public async Task Pass_DatabasesWhoseNamesDifferOnlyInCase_EachKeepTheirOwnState()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            /* A server with a case-sensitive collation can hold both. */
            rig.Source.Probes.Add(Probe(Sales, 5));
            rig.Source.Probes.Add(Probe("salesdb", 6));
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id")];
            rig.Source.Catalog["salesdb"] = [Table(100, "Orders", "Id", "Total")];
            var server = rig.Server();
            await rig.PassAsync(server, ct);

            rig.Source.Catalog["salesdb"] = [Table(100, "Orders", "Id", "Total", "Extra")];
            rig.Source.AddEvent("object_altered", "salesdb", 100);
            rig.Source.Reads.Clear();
            var result = await rig.PassAsync(server, ct);

            Assert.Equal("salesdb", Assert.Single(rig.Source.Reads).Database);
            Assert.Equal("1", await Versions(rig, "database_name = 'salesdb' AND change_kind = 'altered'", ct));
            Assert.Equal("0", await Versions(rig, "database_name = 'SalesDb' AND change_kind = 'altered'", ct));
            Assert.Equal("2", await rig.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name <> ''", ct));
            Assert.Null(result.HostNote);
        });
    }

    [Fact]
    public async Task Pass_PassBudgetSpent_LeavesTheRestForTheNextPass()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            rig.Runner.SchemaBudgetsForTests = (TimeSpan.Zero, TimeSpan.FromSeconds(120));

            var first = await rig.PassAsync(server, ct);

            /* At least one database always runs (the first in probe order); the other is left, with no state, and said so. */
            Assert.Equal(new[] { Sales }, rig.Source.Reads.Select(r => r.Database).ToArray());
            Assert.Contains("pass budget", first.HostNote);
            Assert.Equal("1", await rig.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name <> ''", ct));

            rig.Runner.SchemaBudgetsForTests = null;
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Equal(new[] { Reports }, rig.Source.Reads.Select(r => r.Database).ToArray());
            Assert.Equal("3", await Versions(rig, "change_kind = 'baseline' AND valid_to IS NULL", ct));
        });
    }

    /// <summary>
    /// A database whose read fails every pass does not hold its place at the front: the database the last pass attempted longest ago goes
    /// first, a never-seen one before any, so the one behind it is read though the pass budget allows only one database.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Pass_ADatabaseThatFailsEveryPass_DoesNotStarveTheOnesAfterIt(bool azure)
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Faults[Sales] = new InvalidOperationException("Lock request time out period exceeded.");
            if (azure)
            {
                rig.Source.AzureSessions[Sales] = new SchemaSessionState(true, s_created, null, false);
                rig.Source.AzureSessions[Reports] = new SchemaSessionState(true, s_created, null, false);
                rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", Reports, Sales });
            }

            var server = azure ? rig.Server("schemafail.database.windows.net", azure: true) : rig.Server();
            rig.Runner.SchemaBudgetsForTests = (TimeSpan.Zero, TimeSpan.FromSeconds(120));

            /* The first pass starts with the first database (SalesDb: the probe order on premises; on Azure the list puts ReportsDb first,
               so the failing one is made to go first by reading ReportsDb once beforehand). */
            if (azure)
            {
                rig.Source.Faults[Reports] = new InvalidOperationException("Not yet.");
                await rig.PassAsync(server, ct);
                rig.Source.Faults.Remove(Reports);
                rig.Source.Reads.Clear();
            }

            await rig.PassAsync(server, ct);
            Assert.Equal(new[] { Sales }, rig.Source.Reads.Select(r => r.Database).ToArray());

            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Equal(new[] { Reports }, rig.Source.Reads.Select(r => r.Database).ToArray());
            Assert.Equal("1", await Versions(rig, "change_kind = 'baseline' AND database_name = 'ReportsDb'", ct));
        });
    }

    [Fact]
    public async Task Pass_ManyTargetedObjects_AreReadInFull()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.PassAsync(server, ct);
            rig.Source.Reads.Clear();

            for (var i = 0; i < SchemaCatalogQueries.MaxTargetedIds + 1; i++)
            {
                rig.Source.AddEvent("object_altered", Sales, 1000 + i);
            }

            await rig.PassAsync(server, ct);

            var read = Assert.Single(rig.Source.Reads);
            Assert.Equal(Sales, read.Database);
            Assert.Null(read.Ids);
        });
    }

    [Fact]
    public async Task Pass_Azure_EachDatabaseKeepsItsOwnSessionPosition()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var salesCreated = s_created;
            var reportsCreated = s_created.AddHours(1);
            rig.Source.AzureSessions[Sales] = new SchemaSessionState(true, salesCreated, null, false);
            rig.Source.AzureSessions[Reports] = new SchemaSessionState(true, reportsCreated, null, false);
            rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", Sales, Reports });
            var server = rig.Server("schemapass.database.windows.net", azure: true);

            await rig.PassAsync(server, ct);

            Assert.Equal("3", await Versions(rig, "change_kind = 'baseline' AND valid_to IS NULL", ct));
            Assert.Equal("2", await rig.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name <> '' AND session_create_time IS NOT NULL", ct));
            Assert.Equal("1", await rig.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_state WHERE server_id = {server.ServerId} AND database_name = '' AND last_seen_in_scope_at IS NOT NULL", ct));

            /* An event in one database reads that object only; the other database's own session shows nothing and it is not read. */
            rig.Source.Reads.Clear();
            rig.Source.Catalog[Sales] = [Table(100, "Orders", "Id", "Total", "Extra"), Table(101, "Customers", "Id")];
            rig.Source.AddEvent("object_altered", Sales, 100);
            await rig.PassAsync(server, ct);

            var read = Assert.Single(rig.Source.Reads);
            Assert.Equal(Sales, read.Database);
            Assert.Equal(new[] { 100 }, read.Ids!.ToArray());
            Assert.Equal("1", await Versions(rig, "change_kind = 'altered' AND detected_by = 'xe_event'", ct));

            /* Nothing more to read, and a database that was quiet is not reconciled for it. */
            rig.Source.Reads.Clear();
            await rig.PassAsync(server, ct);
            Assert.Empty(rig.Source.Reads);

            /* A session fault in one database names the Azure grant. */
            rig.Source.AzureSessions[Reports] = new SchemaSessionState(false, null, "Permission denied (error 262).", true);
            var faulted = await rig.PassAsync(server, ct);
            Assert.Contains("ALTER ANY DATABASE EVENT SESSION", faulted.HostNote);
            Assert.Contains(Reports, faulted.HostNote);
        });
    }
}
