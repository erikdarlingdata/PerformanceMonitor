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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7681: the shape registry (<see cref="PgOwnStatement"/>) is process memory, so a restart empties it, and the first hourly text
/// fetch rides the first statement-statistics run, before a daily collector has sent its statement. Two guards, both driven
/// through the real fetch (<see cref="DarlingWorker.ReadPgStatementTextAsync"/>) and the real upsert:
/// the service registers the statements it can know at startup, and the store keeps a pair an earlier fetch learned while its
/// entry is still among the unmarked ones, so no restart un-learns an entry. Also: the fetch never moves a sensitive statement's
/// raw text as a candidate.
/// </summary>
/* #1776 own-store: a scratch target database of its own, the shared store's rows cleaned by server id, and a cluster-wide role. The
   collection serializes every class that empties or fills the process-wide shape registry. */
[Collection("pg-cluster-roles")]
public sealed class PgOwnStatementRestartLiveTests
{
    private const string SensitiveRole = "pm7697_sens";
    private const string AppRole = "pm7697_app";

    /// <summary>#7842: the name the planted pair's statement carries, and the literal in it that the filter's pattern names once the
    /// text is not normalized (a quoted name ending in that word).</summary>
    private const string SiblingProbeName = "pm7842_sibling_probe";

    private const string SiblingLiteral = "pm7842_sibling_password";

    /// <summary>The column-statistics collector's statement: a daily collector, the example in the issue.</summary>
    private static readonly string CollectorText = PgColumnStatsCollector.QueryText;

    [Fact]
    public async Task AfterARestart_TheStartupRegistrationLearnsADailyCollectorsEntry_BeforeThatCollectorHasRun()
    {
        await using var fixture = await Fixture.CreateAsync();
        /* The old build created the entry with the unmarked text; the service then restarts (an empty registry), registers what it
           knows at startup, and fetches before the daily collector has sent a statement. */
        await fixture.RunUnmarkedAsync(CollectorText);
        PgOwnStatement.ResetRegistryForTests();
        PgServiceStatements.Register();

        var (queryId, userId, databaseId) = await fixture.FindEntryAsync();
        var fetch = await DarlingWorker.ReadPgStatementTextAsync(fixture.Runtime, fixture.Ct);
        var own = fetch.OwnEntries[fetch.QueryIds.IndexOf(queryId)];

        Assert.Contains((userId, databaseId), Pairs(own));
        fixture.Succeeded();
    }

    [Fact]
    public async Task AfterARestart_AFetchBeforeTheCollectorHasSentItsText_KeepsAPairAnEarlierFetchLearned()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.RunUnmarkedAsync(CollectorText);
        /* Before the restart: the collector sent its (marked) statement, so the fetch learns the old entry, and the store holds it. */
        await fixture.RunMarkedAsync(CollectorText);
        var (queryId, userId, databaseId) = await fixture.FindEntryAsync();
        var first = await DarlingWorker.ReadPgStatementTextAsync(fixture.Runtime, fixture.Ct);
        Assert.Contains((userId, databaseId), Pairs(first.OwnEntries[first.QueryIds.IndexOf(queryId)]));
        await fixture.UpsertAsync(first);
        Assert.Contains((userId, databaseId), Pairs(await fixture.StoredOwnEntriesAsync(queryId)));

        /* The restart: an empty registry, and no collector has sent its text yet, so this fetch cannot match the entry by shape. */
        PgOwnStatement.ResetRegistryForTests();
        var second = await DarlingWorker.ReadPgStatementTextAsync(fixture.Runtime, fixture.Ct);
        Assert.DoesNotContain((userId, databaseId), PairsOrEmpty(second.OwnEntries[second.QueryIds.IndexOf(queryId)]));
        await fixture.UpsertAsync(second);

        /* The learned pair is still there: past windows keep filtering the service's own statement. */
        Assert.Contains((userId, databaseId), Pairs(await fixture.StoredOwnEntriesAsync(queryId)));
        fixture.Succeeded();
    }

    [Fact]
    public async Task APairWhoseEntryIsGone_DropsOutWhenItsQueryIdComesBackFromAnotherLogin()
    {
        await using var fixture = await Fixture.CreateAsync();

        await fixture.RunUnmarkedAsync(CollectorText);
        await fixture.RunMarkedAsync(CollectorText);
        var (queryId, userId, databaseId) = await fixture.FindEntryAsync();
        var first = await DarlingWorker.ReadPgStatementTextAsync(fixture.Runtime, fixture.Ct);
        await fixture.UpsertAsync(first);
        Assert.Contains((userId, databaseId), Pairs(await fixture.StoredOwnEntriesAsync(queryId)));

        /* A reset takes the service's entry out; the application then runs the same statement under its own login, so the queryid
           comes back with an entry that is not the service's and no unmarked entry of the service's login. */
        await fixture.ExecOnTargetAsync(
            "DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '" + AppRole + "') THEN CREATE ROLE " + AppRole + " NOLOGIN; END IF; END $do$");
        await fixture.ExecOnTargetAsync("GRANT " + AppRole + " TO CURRENT_USER");
        /* This database's entries only: the view is cluster-wide and other tests keep theirs. */
        await fixture.ExecOnTargetAsync("SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0)");
        PgOwnStatement.ResetRegistryForTests();
        await fixture.ExecOnTargetAsync("SET ROLE " + AppRole);
        await fixture.RunUnmarkedAsync(CollectorText);
        await fixture.ExecOnTargetAsync("RESET ROLE");
        var second = await DarlingWorker.ReadPgStatementTextAsync(fixture.Runtime, fixture.Ct);
        Assert.Contains(queryId, second.QueryIds);
        await fixture.UpsertAsync(second);

        Assert.DoesNotContain((userId, databaseId), PairsOrEmpty(await fixture.StoredOwnEntriesAsync(queryId)));
        fixture.Succeeded();
    }

    [Fact]
    public async Task ASensitiveStatement_IsNotACandidate_AndItsRawTextDoesNotCrossTheWire()
    {
        await using var fixture = await Fixture.CreateAsync();
        /* #7842: pg_stat_statements is cluster-wide, so the fetch returns every statement the other classes ran on this cluster, and
           the withheld rows among them are not this test's. One queryid can have entries whose texts differ: an entry created without
           a parse of its own keeps the statement's raw text, while its sibling in another database holds the normalized text. The
           picked (costliest) entry's text then names the filter's pattern and the row is withheld, and the sibling, which does not
           name it, is a candidate. Plant such a pair first so every run meets that row, and then judge only this test's statements. */
        var siblingQueryIds = await fixture.PlantWithheldRowWithACleanSiblingAsync();
        await fixture.ExecOnTargetAsync(
            "DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '" + SensitiveRole + "') THEN CREATE ROLE " + SensitiveRole + " NOLOGIN; END IF; END $do$");
        /* Unmarked, under the service's own login: both are candidates by every test but the sensitive one. */
        await fixture.ExecOnTargetAsync("ALTER ROLE " + SensitiveRole + " PASSWORD 'pm7697-secret-value'");
        await fixture.RunUnmarkedAsync("SELECT count(*) AS pm7697_ordinary FROM pg_catalog.pg_class WHERE relpages > 7697");

        List<RawRow> rows;
        try
        {
            rows = await fixture.FetchRawAsync();
        }
        finally
        {
            await fixture.ForgetAsync(siblingQueryIds);
        }

        /* The planted row: withheld, and it carries its clean sibling, never the raw text of the entry that was withheld. */
        var sibling = Assert.Single(rows, r => siblingQueryIds.Contains(r.QueryId));
        Assert.Equal(PgSensitiveStatementFilter.PlaceholderText, sibling.QueryText);
        Assert.NotNull(sibling.Candidates);
        Assert.Contains(SiblingProbeName, sibling.Candidates, StringComparison.Ordinal);
        Assert.DoesNotContain(SiblingLiteral, sibling.Candidates, StringComparison.Ordinal);

        /* This test's own statement, found by its text in this database: withheld, and not a candidate. */
        var ownQueryIds = await fixture.QueryIdsContainingAsync("ALTER ROLE " + SensitiveRole);
        Assert.NotEmpty(ownQueryIds);
        var sensitive = rows.Where(r => ownQueryIds.Contains(r.QueryId)).ToList();
        Assert.NotEmpty(sensitive);
        Assert.All(sensitive, r => Assert.Equal(PgSensitiveStatementFilter.PlaceholderText, r.QueryText));
        Assert.All(sensitive, r => Assert.Null(r.Candidates));
        Assert.DoesNotContain(rows, r => r.Candidates is not null && r.Candidates.Contains("ALTER ROLE", StringComparison.OrdinalIgnoreCase));
        /* An ordinary unmarked entry of the login still is one. */
        Assert.Contains(rows, r => r.Candidates is not null && r.Candidates.Contains("pm7697_ordinary", StringComparison.Ordinal));
        fixture.Succeeded();
    }

    private static (long User, long Database)[] Pairs(string? json)
    {
        Assert.NotNull(json);
        using var document = JsonDocument.Parse(json);
        return document.RootElement.EnumerateArray()
            .Select(e => (e.GetProperty("u").GetInt64(), e.GetProperty("d").GetInt64()))
            .ToArray();
    }

    private static (long User, long Database)[] PairsOrEmpty(string? json) =>
        json is null ? Array.Empty<(long, long)>() : Pairs(json);

    private sealed record RawRow(long QueryId, string QueryText, string? Own, string? Candidates);

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly string _cs;
        private readonly ScratchPostgres _scratch;
        private readonly NpgsqlConnection _store;
        private readonly NpgsqlConnection _target;
        private bool _succeeded;

        private Fixture(string cs, ScratchPostgres scratch, NpgsqlConnection store, NpgsqlConnection target, ServerRuntime runtime, CancellationToken ct)
        {
            _cs = cs;
            _scratch = scratch;
            _store = store;
            _target = target;
            Runtime = runtime;
            Ct = ct;
        }

        public ServerRuntime Runtime { get; }

        public CancellationToken Ct { get; }

        public void Succeeded() => _succeeded = true;

        public static async Task<Fixture> CreateAsync()
        {
            var cs = PgOwnStatementLiveTests.ConnectionString;
            Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
            var ct = TestContext.Current.CancellationToken;

            var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
            var store = new NpgsqlConnection(cs);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await PgOwnStatementLiveTests.DeleteRowsAsync(store, ct);
            await DarlingMcpTestData.RegisterServerAsync(store, PgOwnStatementLiveTests.ServerId, PgOwnStatementLiveTests.ServerName, ct);
            var target = new NpgsqlConnection(scratch.ConnectionString);
            await target.OpenAsync(ct);
            try
            {
                await DarlingMcpTestData.ExecAsync(target, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
                await using var probe = new NpgsqlCommand("SELECT count(*) FROM pg_stat_statements", target);
                await probe.ExecuteScalarAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
            {
                await target.DisposeAsync();
                await store.DisposeAsync();
                await scratch.DisposeAsync();
                Assert.Skip("pg_stat_statements is not available on this server: " + ex.Message);
            }

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString);
            var config = new MonitoredServer
            {
                Name = "pm7697-target",
                Host = builder.Host ?? "127.0.0.1",
                Port = builder.Port,
                Database = builder.Database,
                Engine = "postgres",
                Auth = "sql",
                Username = builder.Username,
                EncryptMode = "Optional",
            };
            var runtime = new ServerRuntime
            {
                Config = config,
                ConnectionString = MonitoredServerConnection.BuildConnectionString(config, builder.Password ?? "unused"),
                Target = new CollectorTargetInfo
                {
                    Engine = CollectorTargetEngine.PostgreSql,
                    PostgresMajorVersion = target.PostgreSqlVersion.Major,
                    PostgresVersionNum = target.PostgreSqlVersion.Major * 10000,
                },
                StorageName = "pm7697-target",
                ServerId = PgOwnStatementLiveTests.ServerId,
            };
            return new Fixture(cs!, scratch, store, target, runtime, ct);
        }

        public Task ExecOnTargetAsync(string sql) => DarlingMcpTestData.ExecAsync(_target, Ct, sql);

        public Task RunUnmarkedAsync(string sql) => DarlingMcpTestData.ExecAsync(_target, Ct, sql);

        public async Task RunMarkedAsync(string sql)
        {
            await using var marked = PostgresTargetProvider.Instance.CreateCommand(new CollectorQuery(sql), _target, 60);
            await marked.ExecuteNonQueryAsync(Ct);
        }

        private static string SiblingStatement(string seconds) =>
            "SELECT pg_sleep(" + seconds + "), '" + SiblingLiteral + "' AS " + SiblingProbeName;

        /// <summary>
        /// #7842: one queryid with two entries of the service's login. In the target database the statement was prepared, the database's
        /// entries were reset, and the statement was then executed, so its entry (the costlier one: five sleeps) is created with the
        /// statement's raw text, which names the filter's pattern. The same statement run once in the store's database leaves the
        /// normalized text, which does not. Returns the queryid the target database's entry has.
        /// </summary>
        public async Task<List<long>> PlantWithheldRowWithACleanSiblingAsync()
        {
            await ExecOnTargetAsync("PREPARE pm7842_p AS " + SiblingStatement("0.05"));
            await ExecOnTargetAsync("SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0)");
            for (var i = 0; i < 5; i++)
            {
                await ExecOnTargetAsync("EXECUTE pm7842_p");
            }

            await DarlingMcpTestData.ExecAsync(_store, Ct, SiblingStatement("0.001"));
            var queryIds = await QueryIdsContainingAsync(SiblingProbeName);
            Assert.NotEmpty(queryIds);
            return queryIds;
        }

        /// <summary>Takes the planted entries out of the cluster-wide view, in every database, so no other class's fetch meets them.</summary>
        public async Task ForgetAsync(List<long> queryIds)
        {
            foreach (var queryId in queryIds)
            {
                await DarlingMcpTestData.ExecAsync(_target, Ct, "SELECT pg_stat_statements_reset(0, 0, $1)", queryId);
            }
        }

        /// <summary>The queryids of this database's entries whose text contains <paramref name="fragment"/>: the test's own statements,
        /// apart from the cluster's other statements the view also holds.</summary>
        public async Task<List<long>> QueryIdsContainingAsync(string fragment)
        {
            await using var command = new NpgsqlCommand(
                @"SELECT DISTINCT s.queryid FROM pg_stat_statements AS s
WHERE position($1 in s.query) > 0 AND s.queryid IS NOT NULL
AND s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())",
                _target);
            command.Parameters.AddWithValue(fragment);
            var queryIds = new List<long>();
            await using var reader = await command.ExecuteReaderAsync(Ct);
            while (await reader.ReadAsync(Ct))
            {
                queryIds.Add(reader.GetInt64(0));
            }

            return queryIds;
        }

        /// <summary>The entry the collector's text created under the service's login.</summary>
        public async Task<(long QueryId, long UserId, long DatabaseId)> FindEntryAsync()
        {
            await using var command = new NpgsqlCommand(
                @"SELECT s.queryid, s.userid::bigint, s.dbid::bigint FROM pg_stat_statements AS s
WHERE s.query LIKE '%cardinality(s.most\_common\_vals)%' AND s.query NOT LIKE '%pg\_stat\_statements%'
AND s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND s.userid = (SELECT oid FROM pg_roles WHERE rolname = current_user)",
                _target);
            await using var reader = await command.ExecuteReaderAsync(Ct);
            Assert.True(await reader.ReadAsync(Ct), "the collector's statement left no entry");
            return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
        }

        public async Task UpsertAsync((List<long> QueryIds, List<string> Texts, List<string?> OwnEntries, List<string?> Candidates) fetch)
        {
            await using var upsert = new NpgsqlCommand(PgStatementText.UpsertSql, _store);
            var stamp = new DateTime[fetch.QueryIds.Count];
            Array.Fill(stamp, PgStatementText.Naive(DateTime.UtcNow));
            upsert.Parameters.AddWithValue(Enumerable.Repeat(PgOwnStatementLiveTests.ServerId, fetch.QueryIds.Count).ToArray());
            upsert.Parameters.AddWithValue(fetch.QueryIds.ToArray());
            upsert.Parameters.AddWithValue(fetch.Texts.ToArray());
            upsert.Parameters.AddWithValue(stamp);
            upsert.Parameters.AddWithValue(fetch.OwnEntries.ToArray());
            upsert.Parameters.AddWithValue(fetch.Candidates.ToArray());
            await upsert.ExecuteNonQueryAsync(Ct);
        }

        public async Task<string?> StoredOwnEntriesAsync(long queryId)
        {
            await using var command = new NpgsqlCommand(
                "SELECT own_entries::text FROM pg_statement_text WHERE server_id = $1 AND queryid = $2", _store);
            command.Parameters.AddWithValue(PgOwnStatementLiveTests.ServerId);
            command.Parameters.AddWithValue(queryId);
            return await command.ExecuteScalarAsync(Ct) as string;
        }

        /// <summary>The fetch's own rows, with the raw candidate column, as the target sends them.</summary>
        public async Task<List<RawRow>> FetchRawAsync()
        {
            var schemas = new CollectorContext
            {
                ServerId = Runtime.ServerId,
                ServerName = Runtime.Config.DisplayName,
                CollectionTime = DateTime.UtcNow,
                Deltas = new CollectorDeltaCalculator(),
                Target = Runtime.Target,
            };
            await using var connection = PostgresTargetProvider.CreatePinnedConnection(Runtime.ConnectionString);
            await connection.OpenAsync(Ct);
            await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(schemas, PgStatementStatsCollector.Instance, connection, logger: null, Ct);
            await using var command = new NpgsqlCommand(
                PgStatementText.FetchSqlFor(Runtime.Target.IsAurora, Runtime.Target.PostgresMajorVersion, schemas), connection);
            command.Parameters.AddWithValue(10_000);
            var rows = new List<RawRow>();
            await using var reader = await command.ExecuteReaderAsync(Ct);
            while (await reader.ReadAsync(Ct))
            {
                rows.Add(new RawRow(
                    reader.GetInt64(0), reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3)));
            }

            return rows;
        }

        public async ValueTask DisposeAsync()
        {
            await _target.DisposeAsync();
            await LiveStoreCleanup.RunAsync(_cs, _succeeded, async (cleanup, cleanupCt) =>
                await PgOwnStatementLiveTests.DeleteRowsAsync(cleanup, cleanupCt));
            await _store.DisposeAsync();
            await _scratch.DisposeAsync();
            /* Roles are cluster-wide, so the scratch database going does not take it. */
            await using var admin = new NpgsqlConnection(_cs);
            await admin.OpenAsync(Ct);
            await DarlingMcpTestData.ExecAsync(admin, Ct, "DROP ROLE IF EXISTS " + SensitiveRole);
            await DarlingMcpTestData.ExecAsync(admin, Ct, "DROP ROLE IF EXISTS " + AppRole);
        }
    }
}

/// <summary>
/// #7681: the statements registered at startup cover every PostgreSQL collector in the catalog (the list the runner dispatches
/// from) and the probes, so a collector added later is covered without anyone editing the seeding.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PgServiceStatementsTests
{
    [Fact]
    public void Startup_RegistersTheStatementOfEveryPostgresCollectorInTheCatalog()
    {
        PgOwnStatement.ResetRegistryForTests();
        PgServiceStatements.Register();

        var postgres = CollectorCatalog.All.Where(c => c.TargetEngine == CollectorTargetEngine.PostgreSql).ToList();
        Assert.NotEmpty(postgres);
        var unbuildable = new List<string>();
        var needsTarget = new List<string>();
        var texts = PgServiceStatements.CollectorTexts(unbuildable, needsTarget);

        /* Each collector either builds its statement for the default context or needs a target, and every statement built is
           registered. */
        Assert.Empty(unbuildable);
        Assert.Equal(postgres.Count, texts.Count + needsTarget.Count);
        foreach (var text in texts)
        {
            Assert.True(PgOwnStatement.IsRegistered(text), "not registered at startup: " + text[..Math.Min(80, text.Length)]);
        }

        foreach (var probe in PgServiceStatements.ProbeTexts)
        {
            Assert.True(PgOwnStatement.IsRegistered(probe), "not registered at startup: " + probe[..Math.Min(80, probe.Length)]);
        }
    }

    [Fact]
    public void AnEmptyRegistry_KnowsNoStatement_UntilStartupRegistersThem()
    {
        PgOwnStatement.ResetRegistryForTests();
        var text = PgColumnStatsCollector.QueryText;
        Assert.False(PgOwnStatement.IsServiceStatementText(text));

        PgServiceStatements.Register();

        Assert.True(PgOwnStatement.IsServiceStatementText(text));
    }
}
