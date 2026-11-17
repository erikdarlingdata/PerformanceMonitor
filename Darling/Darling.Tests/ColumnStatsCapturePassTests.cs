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

        /* The ids of every candidate, which the read returns only over the cap. */
        public Dictionary<string, List<(int ObjectId, int StatsId)>> AllIds { get; } = new(StringComparer.Ordinal);

        /* When set, the candidate read of every database throws with this message. */
        public string? CandidatesFailure { get; set; }
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

            public Task<(IReadOnlyList<StatCandidate> Candidates, SkipCounts Skipped, IReadOnlyList<(int ObjectId, int StatsId)> AllIds)> ReadCandidatesAsync(
                string database, CancellationToken cancellationToken) =>
                _source.CandidatesFailure is { } failure
                    ? throw new InvalidOperationException(failure)
                    : Task.FromResult<(IReadOnlyList<StatCandidate>, SkipCounts, IReadOnlyList<(int, int)>)>((
                        _source.Candidates.TryGetValue(database, out var list) ? list.ToList() : new List<StatCandidate>(),
                        _source.Skipped.TryGetValue(database, out var skipped) ? skipped : SkipCounts.None,
                        _source.AllIds.TryGetValue(database, out var all) ? all.ToList() : new List<(int, int)>()));

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
            Store = store;
            Connection = connection;
            Source = new FakeSource();
            Runner = new DarlingCollectorRunner(
                store,
                new CollectorDeltaCalculator(),
                databaseScope: (name, _) => name == ColumnStatsCollector.Instance.Name ? ColumnStatsScope : SchemaScope,
                separatelyMonitoredDatabases: _ => new List<string>(),
                collectorEnabled: (name, _) => name == SchemaObjectsCollector.Instance.Name ? SchemaObjectsOn : name != ColumnStatsCollector.Instance.Name || ColumnStatsOn);
            Runner.ColumnStatsSource = Source;
        }

        public NpgsqlDataSource Store { get; }
        public NpgsqlConnection Connection { get; }
        public FakeSource Source { get; }
        public DarlingCollectorRunner Runner { get; }
        public List<string> SchemaScope { get; set; } = new();
        public List<string> ColumnStatsScope { get; set; } = new();
        public bool SchemaObjectsOn { get; set; } = true;
        public bool ColumnStatsOn { get; set; } = true;

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

        /* An Azure SQL Database logical server, whose database list the test sets. */
        public ServerRuntime AzureServer(List<string> listed)
        {
            var server = Server("colstats-azure");
            Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(listed);
            return new ServerRuntime
            {
                Config = server.Config,
                ConnectionString = server.ConnectionString,
                Target = new CollectorTargetInfo { IsAzureSqlDb = true },
                StorageName = server.StorageName,
                ServerId = server.ServerId,
            };
        }

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
            Stat(100, 2, s_t0, 5000, "CustomerId", "Status"),
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
    public async Task ColumnStatsDisabled_NamedRun_ReadsNothing()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.ColumnStatsOn = false;

            /* run_collectors_now naming column_stats runs it even when it is off; the collector itself must refuse. */
            var result = await rig.RunNowAsync(rig.Server(), ct);

            Assert.Equal(0, rig.Source.Opens);
            Assert.Equal(0, result.Rows);
            Assert.Equal("column_stats is off for this server", result.HostNote);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_runs", ct));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_state", ct));
        });
    }

    [Fact]
    public async Task SecondaryNeverPrimary_IsStampedWithoutAStateRow_SoTheGroupsRowsSurviveThePrimarysRemoval()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            var primary = rig.Server("colstats-node-a");
            var secondary = rig.Server("colstats-node-b");

            /* The primary reads the group's database; the secondary has never been primary and has no state row for it. */
            rig.Source.Probes.Add(Probe("MirrorDb", 9, group: s_group, primary: true));
            rig.Source.Candidates["MirrorDb"] = [Stat(900, 1, s_t0)];
            await rig.RunAsync(primary, ct);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_state WHERE server_id = " + secondary.ServerId, ct));

            rig.Source.Probes[0] = Probe("MirrorDb", 9, group: s_group, primary: false);
            await rig.RunAsync(secondary, ct);

            Assert.Equal("ag:" + s_group.ToString("D"), await rig.ScalarAsync(
                "SELECT history_key FROM collect.column_stats_state WHERE server_id = " + secondary.ServerId
                + " AND database_name = 'MirrorDb' AND last_seen_in_scope_at IS NOT NULL AND last_full_read_at IS NULL", ct));

            /* The primary is removed: its state row goes, and the secondary's stamp keeps the group's rows through the sweep. */
            await rig.ScalarAsync("WITH d AS (DELETE FROM collect.column_stats_state WHERE server_id = " + primary.ServerId + " RETURNING 1) SELECT count(*) FROM d", ct);
            await DarlingRetention.PurgeAsync(rig.Store, timescaleAvailable: false, null, ct);

            Assert.Equal("1", await rig.StoredStatsAsync("MirrorDb", ct));
        });
    }

    [Fact]
    public async Task DroppedAndOutOfScopeDatabases_TheRunDeletesTheirRowsAndState()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            foreach (var (name, id) in new[] { (Reports, 6), ("ArchiveDb", 7) })
            {
                rig.Source.Probes.Add(Probe(name, id));
                rig.Source.Candidates[name] = [Stat(id * 100, 1, s_t0)];
            }

            var server = rig.Server();
            await rig.RunAsync(server, ct);
            Assert.Equal("3", await rig.ScalarAsync("SELECT count(DISTINCT database_name) FROM collect.column_stats", ct));

            /* ReportsDb is dropped, ArchiveDb leaves column_stats' scope. */
            rig.Source.Probes.RemoveAll(p => p.Name == Reports);
            rig.ColumnStatsScope = [Sales];
            await rig.RunAsync(server, ct);

            Assert.Equal(Sales, await rig.ScalarAsync("SELECT string_agg(DISTINCT database_name, ',') FROM collect.column_stats", ct));
            Assert.Equal(Sales, await rig.ScalarAsync("SELECT string_agg(database_name, ',') FROM collect.column_stats_state", ct));
        });
    }

    [Fact]
    public async Task GroupDatabaseGoneFromOneServer_RowsStayWhileAnotherServerStillSeesIt()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            var primary = rig.Server("colstats-node-a");
            var secondary = rig.Server("colstats-node-b");
            rig.Source.Probes.Add(Probe("MirrorDb", 9, group: s_group, primary: true));
            rig.Source.Candidates["MirrorDb"] = [Stat(900, 1, s_t0)];
            await rig.RunAsync(primary, ct);
            rig.Source.Probes[0] = Probe("MirrorDb", 9, group: s_group, primary: false);
            await rig.RunAsync(secondary, ct);

            /* The database leaves the first server: its state goes, the group's rows stay for the second. */
            rig.Source.Probes.Clear();
            await rig.RunAsync(primary, ct);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_state WHERE server_id = " + primary.ServerId, ct));
            Assert.Equal("1", await rig.StoredStatsAsync("MirrorDb", ct));

            /* Gone from the second server too: nothing points at the rows any more. */
            await rig.RunAsync(secondary, ct);
            Assert.Equal("0", await rig.StoredStatsAsync("MirrorDb", ct));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_state", ct));
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
    public async Task Renames_KeepStatsDate_AndAreReadAgain()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.RunAsync(server, ct);
            rig.Source.Reads.Clear();

            /* sp_rename of a column and of a table, ALTER SCHEMA ... TRANSFER: STATS_DATE stays, the names do not. */
            rig.Source.Candidates[Sales] =
            [
                Stat(100, 1, s_t0, 5000, "Id"),
                Stat(100, 2, s_t0, 5000, "CustomerKey", "Status"),
                Stat(200, 1, s_t0, 10, "Id") with { ObjectName = "T200Renamed", SchemaName = "archive" },
            ];

            await rig.RunAsync(server, ct);

            Assert.Equal(new[] { (100, 2), (200, 1) }, rig.Source.ReadIds(Sales).Order().ToArray());
            Assert.Equal("{CustomerKey,Status}", await rig.ScalarAsync("SELECT columns::text FROM collect.column_stats WHERE object_id = 100 AND stats_id = 2 AND prefix_length = 2", ct));
            Assert.Equal("archive.T200Renamed", await rig.ScalarAsync("SELECT DISTINCT schema_name || '.' || object_name FROM collect.column_stats WHERE object_id = 200", ct));

            /* Read once: the next run sees the new names stored. */
            rig.Source.Reads.Clear();
            await rig.RunAsync(server, ct);
            Assert.Empty(rig.Source.Reads);
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
                Stat(100, 2, s_t0.AddHours(1), 5000, "CustomerId", "Status"),
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
    public async Task TraceFlag9485_OneDeniedStatistic_AmongReadOnes_IsAnErrorNotARefusal()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.TraceFlagAsync(server, 9485, ct);
            rig.Source.Errors[(100, 2)] = (230, "The SELECT permission was denied on the column 'Status' of the object 'T100'.");

            var result = await rig.RunAsync(server, ct);

            Assert.Equal("baseline|3|2|0|0|0|1|-|true", await rig.LastRunAsync(Sales, ct));
            Assert.DoesNotContain("precondition", result.HostNote ?? string.Empty, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("could not be read", result.HostNote);

            /* Next run: only the statistic that failed is read again, it fails again, and nothing stored was refused: still an error. */
            await rig.RunAsync(server, ct);
            Assert.Equal("incremental|3|0|0|0|0|1|-|true", await rig.LastRunAsync(Sales, ct));
        });
    }

    [Fact]
    public async Task TraceFlag9485_StoredStatisticNowRefused_IsARefusal()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            var server = rig.Server();
            await rig.TraceFlagAsync(server, 9485, ct);
            await rig.RunAsync(server, ct);

            /* The grant still works for the catalog, but a statistic read before is now refused and nothing else was read. */
            rig.Source.Candidates[Sales] = [Stat(100, 1, s_t0.AddHours(1), 5000, "Id"), Stat(100, 2, s_t0, 5000, "CustomerId", "Status"), Stat(200, 1, s_t0, 10, "Id")];
            rig.Source.Errors[(100, 1)] = (2557, "User 'monitor' does not have permission to run DBCC SHOW_STATISTICS for object 'T100'.");

            var result = await rig.RunAsync(server, ct);

            Assert.Equal("incremental|3|0|0|0|0|1|tf9485|true", await rig.LastRunAsync(Sales, ct));
            Assert.Contains("precondition", result.HostNote, StringComparison.OrdinalIgnoreCase);
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
    public async Task OverCap_AStatisticGoneFromTheFullIdList_IsDeleted()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            rig.Source.Probes.Add(Probe(Sales, 5));
            rig.Source.Candidates[Sales] = [Stat(30, 1, s_t0, 3000), Stat(20, 1, s_t0, 200), Stat(10, 1, s_t0, 10)];
            var server = rig.Server();
            await rig.RunAsync(server, ct);
            Assert.Equal("3", await rig.StoredStatsAsync(Sales, ct));

            /* The database is now over the cap: the read list holds the largest table only, (10, 1) was dropped, and (20, 1) is still
               there beyond the cap. */
            rig.Source.Candidates[Sales] = [Stat(30, 1, s_t0, 3000)];
            rig.Source.Skipped[Sales] = new SkipCounts(0, 0, 0, 0, 1);
            rig.Source.AllIds[Sales] = [(30, 1), (20, 1)];
            await rig.RunAsync(server, ct);

            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats WHERE object_id = 10", ct));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats WHERE object_id = 20", ct));
            Assert.Equal("incremental|1|0|1|0|1|0|-|true", await rig.LastRunAsync(Sales, ct));
        });
    }

    [Fact]
    public async Task ServerMessages_AreCutBeforeTheyAreStored()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Errors[(100, 2)] = (2601, new string('x', 20_000));
            var server = rig.Server();
            await rig.RunAsync(server, ct);

            var sample = int.Parse(await rig.ScalarAsync("SELECT max(length(sample_error)) FROM collect.column_stats_runs", ct), System.Globalization.CultureInfo.InvariantCulture);
            Assert.InRange(sample, DarlingColumnStatsCapture.MaxStoredMessageChars, DarlingColumnStatsCapture.MaxStoredMessageChars + 200);

            rig.Source.CandidatesFailure = new string('y', 20_000);
            await rig.RunAsync(server, ct);
            var note = int.Parse(await rig.ScalarAsync("SELECT length(note) FROM collect.column_stats_runs WHERE note LIKE 'The read failed%'", ct), System.Globalization.CultureInfo.InvariantCulture);
            Assert.InRange(note, DarlingColumnStatsCapture.MaxStoredMessageChars, DarlingColumnStatsCapture.MaxStoredMessageChars + 100);
        });
    }

    [Fact]
    public async Task NoSelect_WritesOneRunRowUntilTheAnswerChanges()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe(Reports, 6));
            rig.Source.Candidates[Reports] = [Stat(600, 1, s_t0)];
            rig.Source.NoSelect.Add(Reports);
            var server = rig.Server();

            for (var run = 0; run < 3; run++)
            {
                var result = await rig.RunAsync(server, ct);
                Assert.Contains(Reports, result.HostNote);
            }

            /* One refusal row for ReportsDb, and it is still the newest, so the status still reads the refusal; SalesDb has a row per run. */
            Assert.Equal("1", await rig.ScalarAsync($"SELECT count(*) FROM collect.column_stats_runs WHERE database_name = '{Reports}'", ct));
            Assert.Equal("3", await rig.ScalarAsync($"SELECT count(*) FROM collect.column_stats_runs WHERE database_name = '{Sales}'", ct));
            Assert.EndsWith("|no_select|false", await rig.LastRunAsync(Reports, ct), StringComparison.Ordinal);

            /* The grant arrives: the answer changed, a row says so. Then it is revoked: a new refusal row. */
            rig.Source.NoSelect.Clear();
            await rig.RunAsync(server, ct);
            Assert.EndsWith("|-|true", await rig.LastRunAsync(Reports, ct), StringComparison.Ordinal);
            rig.Source.NoSelect.Add(Reports);
            await rig.RunAsync(server, ct);
            await rig.RunAsync(server, ct);
            Assert.Equal("2", await rig.ScalarAsync($"SELECT count(*) FROM collect.column_stats_runs WHERE database_name = '{Reports}' AND refused = 'no_select'", ct));
            Assert.EndsWith("|no_select|false", await rig.LastRunAsync(Reports, ct), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task GroupDatabase_RefusedThenFailoverThenFailBack_TheRefusalIsWrittenAgain()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            var nodeA = rig.Server("colstats-node-a");
            var nodeB = rig.Server("colstats-node-b");
            rig.Source.Candidates["MirrorDb"] = [Stat(900, 1, s_t0)];

            /* A is primary and has no SELECT there: one refusal row. */
            rig.Source.Probes.Add(Probe("MirrorDb", 9, group: s_group, primary: true));
            rig.Source.NoSelect.Add("MirrorDb");
            await rig.RunAsync(nodeA, ct);

            /* Failover to B, which has the grant: B reads and its row is the newest of the history. */
            rig.Source.NoSelect.Clear();
            await rig.RunAsync(nodeB, ct);
            Assert.EndsWith("|-|true", await rig.LastRunAsync("MirrorDb", ct), StringComparison.Ordinal);

            /* Fail back to A, still without the grant: A's own newest row says no_select, but the history's newest is B's granted row,
               which is what the status reads, so A must write its refusal again. */
            rig.Source.NoSelect.Add("MirrorDb");
            await rig.RunAsync(nodeA, ct);

            Assert.EndsWith("|no_select|false", await rig.LastRunAsync("MirrorDb", ct), StringComparison.Ordinal);
            Assert.Equal("2", await rig.ScalarAsync(
                "SELECT count(*) FROM collect.column_stats_runs WHERE refused = 'no_select' AND server_id = " + nodeA.ServerId, ct));
        });
    }

    [Fact]
    public async Task Azure_ADatabaseMissingFromThePartialList_IsKept_OneOutOfScope_IsForgotten()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe(Reports, 6));
            rig.Source.Candidates[Reports] = [Stat(600, 1, s_t0)];
            var listed = new List<string> { "master", Sales, Reports };
            var server = rig.AzureServer(listed);
            await rig.RunAsync(server, ct);
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(DISTINCT database_name) FROM collect.column_stats", ct));

            /* The list comes back without ReportsDb (a fallback or a paused database): not known to be gone, so it stays. */
            listed.Remove(Reports);
            await rig.RunAsync(server, ct);
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(DISTINCT database_name) FROM collect.column_stats", ct));

            /* column_stats' scope now names SalesDb only: leaving the scope is configuration, so ReportsDb is forgotten. */
            rig.ColumnStatsScope = [Sales];
            await rig.RunAsync(server, ct);
            Assert.Equal(Sales, await rig.ScalarAsync("SELECT string_agg(DISTINCT database_name, ',') FROM collect.column_stats", ct));
            Assert.Equal(Sales, await rig.ScalarAsync("SELECT string_agg(database_name, ',') FROM collect.column_stats_state", ct));
        });
    }

    [Fact]
    public async Task NoDatabaseHasGrant_RepeatedRuns_TheNewestServerRunStillShowsEveryRefusal()
    {
        await WithRigAsync(async (rig, ct) =>
        {
            Seed(rig.Source);
            rig.Source.Probes.Add(Probe(Reports, 6));
            rig.Source.NoSelect.Add(Sales);
            rig.Source.NoSelect.Add(Reports);
            var server = rig.Server();

            await rig.RunAsync(server, ct);
            var second = await rig.RunAsync(server, ct);

            /* No new run row, but both databases still count in the run's rows: collection health must not read the second run as a
               collector that produced and then stopped. */
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM collect.column_stats_runs", ct));
            Assert.Equal(2, second.Rows);
            Assert.Contains("precondition", second.HostNote, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("2|2", await rig.ScalarAsync(
                "SELECT count(*) || '|' || count(*) FILTER (WHERE refused = 'no_select') FROM collect.column_stats_runs WHERE server_id = " + server.ServerId
                + " AND collection_time = (SELECT max(collection_time) FROM collect.column_stats_runs WHERE server_id = " + server.ServerId + ")", ct));
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
