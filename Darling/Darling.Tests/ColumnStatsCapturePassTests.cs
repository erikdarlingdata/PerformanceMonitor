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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8798: the <c>column_stats</c> run of one server (<see cref="DarlingColumnStatsCapture"/>) on a live store, with the SQL Server side
/// replaced by a script: which databases exist, which grant SELECT, which statistics each holds and what reading one returns. Each test
/// mints its own store. The live SQL Server end to end is <c>ColumnStatsEndToEndLiveTests</c>.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class ColumnStatsCapturePassTests
{
    private const string Sales = "SalesDb";
    private const string Reports = "ReportsDb";
    private static readonly DateTime s_t0 = new(2026, 11, 1, 10, 0, 0, 123, DateTimeKind.Unspecified);
    private static readonly Guid s_fork1 = new("aaaaaaaa-0000-0000-0000-000000000001");
    private static readonly Guid s_fork2 = new("aaaaaaaa-0000-0000-0000-000000000002");
    private static readonly Guid s_group = new("11111111-2222-3333-4444-555555555555");

    /// <summary>The SQL Server side: the databases, their grants, their statistics and what reading one returns.</summary>
    private sealed class FakeSource : IColumnStatsSource
    {
        public List<SchemaDatabaseProbe> Probes { get; } = new();
        public HashSet<string> NoSelect { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, List<StatCandidate>> Candidates { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, SkipCounts> Skipped { get; } = new(StringComparer.Ordinal);
        public Dictionary<(int ObjectId, int StatsId), (int Number, string Message)> Errors { get; } = new();
        public List<(string Database, List<(int ObjectId, int StatsId)> Stats, bool Shape)> Reads { get; } = new();
        public Version Version { get; set; } = new(16, 0, 4135);

        /* The STAT_HEADER shape the server really has; a batch built for the other fails its first header with error 213. Null: any. */
        public bool? HeaderShape { get; set; }
        public int Opens { get; private set; }

        public IEnumerable<(int ObjectId, int StatsId)> ReadIds(string database) =>
            Reads.Where(r => r.Database == database).SelectMany(r => r.Stats);

        public Task<IColumnStatsConnection> OpenAsync(ServerRuntime server, string? azureDatabase, CancellationToken cancellationToken)
        {
            Opens++;
            return Task.FromResult<IColumnStatsConnection>(new Connection(this));
        }

        private sealed class Connection : IColumnStatsConnection
        {
            private readonly FakeSource _source;

            public Connection(FakeSource source) => _source = source;

            public Version ServerVersion => _source.Version;

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            public Task<IReadOnlyList<SchemaDatabaseProbe>> ReadProbesAsync(CancellationToken cancellationToken) =>
                Task.FromResult<IReadOnlyList<SchemaDatabaseProbe>>(_source.Probes.ToList());

            public Task<bool> HasSelectAsync(string database, CancellationToken cancellationToken) =>
                Task.FromResult(!_source.NoSelect.Contains(database));

            public Task<(IReadOnlyList<StatCandidate> Candidates, SkipCounts Skipped)> ReadCandidatesAsync(string database, CancellationToken cancellationToken) =>
                Task.FromResult<(IReadOnlyList<StatCandidate>, SkipCounts)>((
                    _source.Candidates.TryGetValue(database, out var list) ? list.ToList() : new List<StatCandidate>(),
                    _source.Skipped.TryGetValue(database, out var skipped) ? skipped : SkipCounts.None));

            public Task<(IReadOnlyList<ColumnStatsReadRow> Rows, IReadOnlyList<StatError> Errors)> ReadStatisticsAsync(
                string database, IReadOnlyList<(int ObjectId, int StatsId)> stats, bool headerHasPersistedSamplePercent, CancellationToken cancellationToken)
            {
                _source.Reads.Add((database, stats.ToList(), headerHasPersistedSamplePercent));
                if (_source.HeaderShape is { } shape && shape != headerHasPersistedSamplePercent)
                {
                    return Task.FromResult<(IReadOnlyList<ColumnStatsReadRow>, IReadOnlyList<StatError>)>((
                        new List<ColumnStatsReadRow>(),
                        new List<StatError> { new(stats[0].ObjectId, stats[0].StatsId, 213, "Column name or number of supplied values does not match table definition.") }));
                }

                var byId = _source.Candidates[database].ToDictionary(c => (c.ObjectId, c.StatsId));
                var rows = new List<ColumnStatsReadRow>();
                var errors = new List<StatError>();
                foreach (var id in stats)
                {
                    if (_source.Errors.TryGetValue(id, out var error))
                    {
                        errors.Add(new StatError(id.ObjectId, id.StatsId, error.Number, error.Message));
                        continue;
                    }

                    var c = byId[id];
                    var header = new StatHeader(c.TableRows, c.TableRows, headerHasPersistedSamplePercent ? 100f : null);
                    var densities = Enumerable.Range(1, c.StatColumns.Count).Select(i => 1.0 / (10 * i)).ToList();
                    rows.Add(new ColumnStatsReadRow(id.ObjectId, id.StatsId, header, densities));
                }

                return Task.FromResult<(IReadOnlyList<ColumnStatsReadRow>, IReadOnlyList<StatError>)>((rows, errors));
            }
        }
    }

    private sealed class Rig
    {
        public Rig(NpgsqlDataSource store, NpgsqlConnection connection)
        {
            Connection = connection;
            Source = new FakeSource();
            Runner = new DarlingCollectorRunner(
                store,
                new CollectorDeltaCalculator(),
                databaseScope: (name, _) => name == ColumnStatsCollector.Instance.Name ? ColumnStatsScope : SchemaScope,
                separatelyMonitoredDatabases: _ => new List<string>(),
                collectorEnabled: (name, _) => name != SchemaObjectsCollector.Instance.Name || SchemaObjectsOn);
            Runner.ColumnStatsSource = Source;
        }

        public NpgsqlConnection Connection { get; }
        public FakeSource Source { get; }
        public DarlingCollectorRunner Runner { get; }
        public List<string> SchemaScope { get; set; } = new();
        public List<string> ColumnStatsScope { get; set; } = new();
        public bool SchemaObjectsOn { get; set; } = true;

        public ServerRuntime Server(string host = "colstats-sql", params string[] excluded)
        {
            var config = new MonitoredServer { Name = host, Host = host, ExcludedDatabases = excluded.ToList() };
            return new ServerRuntime
            {
                Config = config,
                ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
                Target = new CollectorTargetInfo(),
                StorageName = config.StorageName,
                ServerId = config.ServerId,
            };
        }

        public Task<CollectorRunResult> RunAsync(ServerRuntime server, CancellationToken ct) => DarlingColumnStatsCapture.RunAsync(Runner, server, ct);

        public async Task<CollectorRunResult> RunNowAsync(ServerRuntime server, CancellationToken ct)
        {
            using (DarlingSnapshotScope.Enter())
            {
                return await DarlingColumnStatsCapture.RunAsync(Runner, server, ct);
            }
        }

        public async Task<string> ScalarAsync(string sql, CancellationToken ct) => await ActionProposalStoreLiveTests.ScalarAsync(Connection, sql, ct);

        /* The newest run row of one database, as "kind|checked|read|deleted|locked|over_cap|errors|refused|complete". */
        public Task<string> LastRunAsync(string database, CancellationToken ct) => ScalarAsync(
            "SELECT kind || '|' || checked || '|' || read || '|' || deleted || '|' || skipped_locked || '|' || skipped_over_cap || '|' || errors || '|' "
            + "|| coalesce(refused, '-') || '|' || complete::text FROM collect.column_stats_runs WHERE database_name = '" + database + "' ORDER BY collection_time DESC, collection_id DESC LIMIT 1", ct);

        public Task<string> StoredStatsAsync(string database, CancellationToken ct) =>
            ScalarAsync("SELECT count(DISTINCT (object_id, stats_id)) FROM collect.column_stats WHERE database_name = '" + database + "'", ct);

        public async Task TraceFlagAsync(ServerRuntime server, int flag, CancellationToken ct)
        {
            await using var command = new NpgsqlCommand(@"
INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES (1, $1, $2, $3, $4, true, true, false)", Connection);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(server.ServerId);
            command.Parameters.AddWithValue(server.StorageName);
            command.Parameters.AddWithValue(flag);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task WithRigAsync(Func<Rig, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the column_stats run tests.");
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
        string name, int id, bool readOnly = false, Guid? group = null, bool? primary = null, Guid? fork = null) =>
        new(name, id, "ONLINE", readOnly, false, null, true, fork ?? s_fork1, group, group is null ? null : "AgOne", primary);

    private static StatCandidate Stat(int objectId, int statsId, DateTime date, long tableRows = 1000, params string[] columns) =>
        new(objectId, "dbo", "T" + objectId, statsId, "st_" + objectId + "_" + statsId, statsId == 1 ? "index" : "auto", date, date.AddHours(5),
            columns.Length == 0 ? new[] { "Id" } : columns, Array.Empty<string>(), tableRows);

    private static void Seed(FakeSource source)
    {
        source.Probes.Add(Probe(Sales, 5));
        source.Candidates[Sales] =
        [
            Stat(100, 1, s_t0, 5000, "Id"),
            Stat(100, 2, s_t0, 5000, "TeamId", "Status"),
            Stat(200, 1, s_t0, 10, "Id"),
        ];
    }

    [Fact]
    public async Task SchemaObjectsDisabled_ReadsNothing_AndSaysSo()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.SchemaObjectsOn = false;

            var result = await rig.RunAsync(rig.Server(), ct);

            Assert.Equal(0, rig.Source.Opens);
            Assert.Equal(0, result.Rows);
            Assert.Contains("schema_objects", result.HostNote);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_runs", ct));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats", ct));
        });
    }

    [Fact]
    public async Task OnlyDatabasesBothScopesCover_AreRead()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            foreach (var (name, id) in new[] { ("A", 5), ("B", 6), ("C", 7), ("D", 8) })
            {
                rig.Source.Probes.Add(Probe(name, id));
                rig.Source.Candidates[name] = [Stat(id * 100, 1, s_t0)];
            }

            rig.SchemaScope = ["A", "B", "D"];
            rig.ColumnStatsScope = ["B", "C", "D"];

            await rig.RunAsync(rig.Server(excluded: "D"), ct);

            Assert.Equal(["B"], rig.Source.Reads.Select(r => r.Database).Distinct().ToArray());
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_runs", ct));
        });
    }

    [Fact]
    public async Task SecondaryAndReadOnly_AreSkippedByReason()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe("FrozenDb", 8, readOnly: true));
            rig.Source.Probes.Add(Probe("MirrorDb", 9, group: s_group, primary: false));
            rig.Source.Candidates["FrozenDb"] = [Stat(800, 1, s_t0)];
            rig.Source.Candidates["MirrorDb"] = [Stat(900, 1, s_t0)];

            var result = await rig.RunAsync(rig.Server(), ct);

            Assert.Equal([Sales], rig.Source.Reads.Select(r => r.Database).Distinct().ToArray());
            Assert.Contains("FrozenDb", result.HostNote);
            Assert.Contains("read-only", result.HostNote);
            Assert.Contains("MirrorDb", result.HostNote);
            Assert.Contains("secondary replica", result.HostNote);
        });
    }

    [Fact]
    public async Task FirstRun_ReadsEveryCandidate()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();

            var result = await rig.RunAsync(server, ct);

            Assert.Equal(3, rig.Source.ReadIds(Sales).Count());
            Assert.Equal("3", await rig.StoredStatsAsync(Sales, ct));
            /* One row per prefix: 1 + 2 + 1. The stored date is STATS_DATE to the millisecond as the server gave it (compared next run), and
               beside it the same instant in UTC (shown); the fake server runs five hours behind UTC. */
            Assert.Equal("4", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats WHERE last_updated = '2026-11-01 10:00:00.123' AND last_updated_utc = '2026-11-01 15:00:00.123'", ct));
            Assert.Equal("baseline|3|3|0|0|0|0|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.Equal("1", await rig.ScalarAsync(
                "SELECT count(*) FROM collect.column_stats_state WHERE server_id = " + server.ServerId
                + " AND last_full_read_at IS NOT NULL AND last_seen_in_scope_at IS NOT NULL AND recovery_fork_guid = '" + s_fork1 + "'", ct));
            Assert.Equal(1, result.Rows);
        });
    }

    [Fact]
    public async Task SecondRun_ReadsOnlyChangedAndNew_DeletesGone()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.RunAsync(server, ct);
            rig.Source.Reads.Clear();

            /* (100, 2) updated, (200, 1) dropped, (300, 1) new; (100, 1) unchanged. */
            rig.Source.Candidates[Sales] =
            [
                Stat(100, 1, s_t0, 5000, "Id"),
                Stat(100, 2, s_t0.AddHours(1), 5000, "TeamId", "Status"),
                Stat(300, 1, s_t0, 20, "Id"),
            ];

            await rig.RunAsync(server, ct);

            Assert.Equal(new[] { (100, 2), (300, 1) }, rig.Source.ReadIds(Sales).Order().ToArray());
            Assert.Equal("incremental|3|2|1|0|0|0|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats WHERE object_id = 200", ct));
            Assert.Equal("3", await rig.StoredStatsAsync(Sales, ct));

            /* Nothing changed: nothing read. */
            rig.Source.Reads.Clear();
            await rig.RunAsync(server, ct);
            Assert.Empty(rig.Source.Reads);
            Assert.Equal("incremental|3|0|0|0|0|0|-|true", await rig.LastRunAsync(Sales, ct));
        });
    }

    [Fact]
    public async Task AgFailover_NewPrimaryReplacesTheOldPrimarysRows_AndAnOrphanGoes()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            var oldPrimary = rig.Server("colstats-node-a");
            var newPrimary = rig.Server("colstats-node-b");
            Assert.NotEqual(oldPrimary.ServerId, newPrimary.ServerId);

            /* The old primary reads a large-sample statistic on Id and one only it has. */
            rig.Source.Probes.Add(Probe(Sales, 5, group: s_group, primary: true));
            rig.Source.Candidates[Sales] = [Stat(100, 1, s_t0, 50_000, "Id"), Stat(100, 2, s_t0, 50_000, "Status")];
            await rig.RunAsync(oldPrimary, ct);
            Assert.Equal("2", await rig.StoredStatsAsync(Sales, ct));

            /* Failover: the new primary's Id statistic is newer and sampled smaller, and (100, 2) was dropped after the failover. */
            rig.Source.Candidates[Sales] = [Stat(100, 1, s_t0.AddHours(2), 300, "Id")];
            rig.Source.Reads.Clear();
            await rig.RunAsync(newPrimary, ct);

            Assert.Equal("baseline|1|1|0|0|0|0|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.Equal("1", await rig.StoredStatsAsync(Sales, ct));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats WHERE stats_id = 2", ct));
            Assert.Equal("300|" + newPrimary.ServerId, await rig.ScalarAsync("SELECT string_agg(DISTINCT rows_sampled || '|' || server_id, ',') FROM collect.column_stats", ct));
            Assert.Equal("ag:" + s_group.ToString("D"), await rig.ScalarAsync("SELECT string_agg(DISTINCT history_key, ',') FROM collect.column_stats", ct));

            /* Failing back, the old primary compares with the group's rows the new primary wrote: only the statistic made since is read. */
            rig.Source.Candidates[Sales] = [Stat(100, 1, s_t0.AddHours(2), 300, "Id"), Stat(100, 4, s_t0.AddHours(3), 300, "Total")];
            rig.Source.Reads.Clear();
            await rig.RunAsync(oldPrimary, ct);

            Assert.Equal(new[] { (100, 4) }, rig.Source.ReadIds(Sales).ToArray());
            Assert.Equal("2", await rig.StoredStatsAsync(Sales, ct));
        });
    }

    [Fact]
    public async Task RecoveryForkChange_ReadsEverything()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.RunAsync(server, ct);
            rig.Source.Reads.Clear();

            rig.Source.Probes[0] = Probe(Sales, 5, fork: s_fork2);
            await rig.RunAsync(server, ct);

            Assert.Equal(3, rig.Source.ReadIds(Sales).Count());
            Assert.Equal("baseline|3|3|0|0|0|0|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_state WHERE recovery_fork_guid = '" + s_fork2 + "'", ct));
        });
    }

    [Fact]
    public async Task RunNow_ReadsEverything()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.RunAsync(server, ct);
            rig.Source.Reads.Clear();

            await rig.RunNowAsync(server, ct);

            Assert.Equal(3, rig.Source.ReadIds(Sales).Count());
            Assert.Equal("run_now|3|3|0|0|0|0|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.Equal("3", await rig.StoredStatsAsync(Sales, ct));
        });
    }

    [Fact]
    public async Task MissingGrant_OutsideOwnList_IsSkippedNotPrecondition()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe(Reports, 6));
            rig.Source.Candidates[Reports] = [Stat(600, 1, s_t0)];
            rig.Source.NoSelect.Add(Reports);

            var result = await rig.RunAsync(rig.Server(), ct);

            Assert.Equal([Sales], rig.Source.Reads.Select(r => r.Database).Distinct().ToArray());
            Assert.Equal("baseline|0|0|0|0|0|0|no_select|false", await rig.LastRunAsync(Reports, ct));
            Assert.Contains(Reports, result.HostNote);
            Assert.Contains("SELECT", result.HostNote);
            Assert.DoesNotContain("precondition", result.HostNote, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public async Task MissingGrant_InOwnList_IsPrecondition()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe(Reports, 6));
            rig.Source.Candidates[Reports] = [Stat(600, 1, s_t0)];
            rig.Source.NoSelect.Add(Reports);
            rig.ColumnStatsScope = [Sales, Reports];

            var result = await rig.RunAsync(rig.Server(), ct);

            Assert.Equal("3", await rig.StoredStatsAsync(Sales, ct));
            Assert.Contains("precondition", result.HostNote, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(Reports, result.HostNote);
            Assert.Contains("GRANT SELECT", result.HostNote);
            Assert.DoesNotContain("9485", result.HostNote);
        });
    }

    [Fact]
    public async Task NoDatabaseHasGrant_IsPrecondition()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe(Reports, 6));
            rig.Source.NoSelect.Add(Sales);
            rig.Source.NoSelect.Add(Reports);

            var result = await rig.RunAsync(rig.Server(), ct);

            Assert.Empty(rig.Source.Reads);
            Assert.Equal(2, result.Rows);
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_runs WHERE refused = 'no_select'", ct));
            Assert.Contains("precondition", result.HostNote, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("GRANT SELECT", result.HostNote);
        });
    }

    [Fact]
    public async Task TraceFlag9485_PreconditionSaysGrantIsNotEnough()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.TraceFlagAsync(server, 9485, ct);
            foreach (var c in rig.Source.Candidates[Sales])
            {
                rig.Source.Errors[(c.ObjectId, c.StatsId)] = (2557, "User 'monitor' does not have permission to run DBCC SHOW_STATISTICS for object 'T'.");
            }

            var result = await rig.RunAsync(server, ct);

            Assert.Equal("baseline|3|0|0|0|0|3|tf9485|true", await rig.LastRunAsync(Sales, ct));
            Assert.Contains("precondition", result.HostNote, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("9485", result.HostNote);
            Assert.Contains("does not satisfy DBCC SHOW_STATISTICS", result.HostNote);
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_runs WHERE sample_error LIKE '%DBCC SHOW_STATISTICS%'", ct));
        });
    }

    [Fact]
    public async Task LockTimeout_CountsLocked_AndRetriesNextRun()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            rig.Source.Errors[(100, 2)] = (1222, "Lock request time out period exceeded.");

            await rig.RunAsync(server, ct);

            Assert.Equal("baseline|3|2|0|1|0|0|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats WHERE object_id = 100 AND stats_id = 2", ct));

            rig.Source.Errors.Clear();
            rig.Source.Reads.Clear();
            await rig.RunAsync(server, ct);

            Assert.Equal(new[] { (100, 2) }, rig.Source.ReadIds(Sales).ToArray());
            Assert.Equal("3", await rig.StoredStatsAsync(Sales, ct));
        });
    }

    [Fact]
    public async Task BudgetSpent_ContinuesWhereItStopped()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            rig.Source.Probes.Add(Probe(Sales, 5));
            rig.Source.Candidates[Sales] = Enumerable.Range(1, 450).Select(i => Stat(1000 + i, 1, s_t0, 100_000 - i)).ToList();
            var server = rig.Server();
            rig.Runner.SchemaBudgetsForTests = (TimeSpan.FromHours(1), TimeSpan.FromTicks(1));

            var first = await rig.RunAsync(server, ct);

            /* One batch of 200, the largest tables, stored before the budget stopped the database. */
            Assert.Single(rig.Source.Reads);
            Assert.Equal(Enumerable.Range(1, 200).Select(i => (1000 + i, 1)).ToArray(), rig.Source.ReadIds(Sales).ToArray());
            Assert.Equal("200", await rig.StoredStatsAsync(Sales, ct));
            Assert.Equal("baseline|450|200|0|0|0|0|-|false", await rig.LastRunAsync(Sales, ct));
            Assert.Contains("budget", first.HostNote);

            rig.Runner.SchemaBudgetsForTests = null;
            rig.Source.Reads.Clear();
            await rig.RunAsync(server, ct);

            Assert.Equal(Enumerable.Range(201, 250).Select(i => (1000 + i, 1)).ToArray(), rig.Source.ReadIds(Sales).ToArray());
            Assert.Equal("450", await rig.StoredStatsAsync(Sales, ct));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_state WHERE last_full_read_at IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task OverCap_ReadsLargestTablesFirst_CountsTheRest()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            rig.Source.Probes.Add(Probe(Sales, 5));
            rig.Source.Candidates[Sales] = [Stat(10, 1, s_t0, 10), Stat(30, 1, s_t0, 3000), Stat(20, 1, s_t0, 200)];
            rig.Source.Skipped[Sales] = new SkipCounts(1, 2, 3, 4, 7);

            await rig.RunAsync(rig.Server(), ct);

            Assert.Equal(new[] { (30, 1), (20, 1), (10, 1) }, rig.Source.ReadIds(Sales).ToArray());
            Assert.Equal("baseline|3|3|0|0|7|0|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.Equal("1|2|3|4", await rig.ScalarAsync(
                "SELECT skipped_filtered || '|' || skipped_memory_optimized || '|' || skipped_xml_spatial_columnstore || '|' || skipped_never_updated FROM collect.column_stats_runs", ct));
        });
    }

    [Fact]
    public async Task HeaderShapeMismatch_RetriesOnceWithTheOtherShape()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Version = new Version(12, 0, 2000);
            rig.Source.HeaderShape = true;

            await rig.RunAsync(rig.Server(), ct);

            Assert.Equal(new[] { false, true }, rig.Source.Reads.Select(r => r.Shape).ToArray());
            Assert.Equal("3", await rig.StoredStatsAsync(Sales, ct));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats WHERE persisted_sample_percent = 100 AND object_id = 200", ct));
        });
    }
}
