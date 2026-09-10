/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the V44 <c>collector_state</c> table (#1962) — the per-server state a collector needs that is NOT
/// derivable from its rows, so it cannot be a MAX() over the target table the way the <c>event_time</c> /
/// <c>instance_id</c> watermarks are. default_trace_events stores the trace FILE it read and compares it
/// next cycle to decide whether it can read only the current rollover file (the measured 5.0x saving) or
/// must re-read the whole set because the trace rolled.
///
/// <para>The cross-store pin is the point of this file. Lite and Darling run the SAME collector definition
/// against two different stores, so the definition's state contract only holds if BOTH stores keep the same
/// key AND both runners actually load and persist it. A drift on either side would not fail a build or a
/// query — the affected SKU would simply read nothing back, bind NULL forever, and pay the fallback on
/// every cycle on every server, which looks exactly like "the fix did not help" rather than like a bug.
/// Lite's DDL and both runners' wiring live in projects this one cannot all reference, so they are pinned
/// at source, the idiom this suite already uses for cross-artifact contracts.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorStateContractTests
{
    private const string RepoRootNotFound = "repo root not found -- the source pin cannot run";

    [Fact]
    public void V44_CreatesCollectorState_InCollect_KeyedPerServerCollectorAndKey()
    {
        var v44 = PgMigrations.Scripts.Single(m => m.Version == 44);

        Assert.Equal("collector-state", v44.Name);

        /* Schema-qualified collect.*: service-written state the operator never mutates, so it belongs with
           analysis_state (V19) and not in the config control plane. The migrate session's search_path would
           resolve a bare name to collect anyway — qualifying makes the intent explicit, per V17/V18/V19. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.collector_state (", v44.Sql, StringComparison.Ordinal);

        /* The key is what scopes state per server AND per collector: servers roll their traces
           independently, so a coarser key would let one server's rollover put another on the wrong arm. */
        Assert.Contains("PRIMARY KEY (server_id, collector_name, state_key)", v44.Sql, StringComparison.Ordinal);

        Assert.Contains("server_id integer NOT NULL", v44.Sql, StringComparison.Ordinal);
        Assert.Contains("collector_name text NOT NULL", v44.Sql, StringComparison.Ordinal);
        Assert.Contains("state_key text NOT NULL", v44.Sql, StringComparison.Ordinal);
        Assert.Contains("state_value text NOT NULL", v44.Sql, StringComparison.Ordinal);
        Assert.Contains("updated_at timestamp NOT NULL", v44.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectorState_IsNotAHypertable_AndCarriesNoPassthroughView()
    {
        /* A keyed registry, not time-series growth: TimescaleDB would reject the PRIMARY KEY or force it
           onto the partition column. The hypertable set is catalog-driven and this table is not a
           collector, so exclusion is structural — this pins that it stays that way. */
        Assert.DoesNotContain("collector_state", string.Join(",", TimescaleSupport.HypertableTables), StringComparison.Ordinal);

        /* Nothing outside the collector runner reads it, so it gets no v_* passthrough. */
        Assert.DoesNotContain("v_collector_state", string.Join(",", PgSchemaGenerator.AllPassthroughViews), StringComparison.Ordinal);
    }

    [Fact]
    public void BothStoresDeclareTheSameStateContract()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, RepoRootNotFound);

        var liteSchema = File.ReadAllText(Path.Combine(root!, "Lite", "Database", "Schema.cs"));

        Assert.Contains("CREATE TABLE IF NOT EXISTS collector_state (", liteSchema, StringComparison.Ordinal);
        Assert.Contains("PRIMARY KEY (server_id, collector_name, state_key)", liteSchema, StringComparison.Ordinal);

        /* Same five columns in both stores, so the shared definition's key means the same thing on either.
           Types differ by store dialect (VARCHAR/TIMESTAMP vs text/timestamp) and are pinned per store
           above; what must not drift is the column set and the key. */
        foreach (var column in new[] { "server_id", "collector_name", "state_key", "state_value", "updated_at" })
        {
            Assert.Contains(column, liteSchema, StringComparison.Ordinal);
        }

        /* Lite creates it unconditionally on every startup (GetAllTableStatements is CREATE IF NOT EXISTS),
           which is what makes an upgraded Lite store get the table with no migration; Darling needs the
           versioned migration because its store is created once. Both paths must exist or one SKU silently
           has no state. */
        Assert.Contains("yield return CreateCollectorStateTable;", liteSchema, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCollectorsDeclaringStateArePinned()
    {
        /* Both hosts load and persist state ONLY for collectors that declare keys, so a declaring
           collector is a two-host concern rather than a definition-local one. Pinned on the catalog
           surface both hosts iterate.

           ONE of them now: default_trace_events' last-seen trace FILE (#1962). pg_index_bloat's
           per-database rotation cursor (#3153) was the other and #3234 retired it — the statistics
           estimate covers every index in one statement, so there is no position to resume from. It did
           not need host CODE, and neither does the survivor: the wiring below is generic. Enumerated in
           the same file that pins that wiring, so a collector newly declaring state lands here first. */
        Assert.Equal(
            new[] { "default_trace_events" },
            CollectorCatalog.All
                .Where(c => c.StateKeys.Count > 0)
                .Select(c => c.Name)
                .OrderBy(n => n, StringComparer.Ordinal)
                .ToArray());
    }

    /// <summary>
    /// Both hosts load state by <c>(server_id, collector_name)</c> and NOT by the declared key, which is
    /// what lets a definition whose keys are discovered per cycle declare a PREFIX and still get them all.
    ///
    /// <para><b>Why this needs its own pin.</b> <c>pg_index_bloat</c> runs per DATABASE, so its rotation
    /// cursors are keyed <c>rotate: || database_name</c> and the database set is not knowable at
    /// declaration time — it declares the prefix, and the load returns every key stored under the
    /// collector's name. Filtering the load down to the declared keys looks like an obvious tightening and
    /// would return NOTHING for that collector: no cursor would ever load, rotation would stop, and the
    /// collector would go straight back to re-measuring its largest index forever while every other row
    /// claimed to be deferred. Nothing would fail, nothing would log, and #3153 would be reopened by a
    /// change that looked like hygiene.</para>
    ///
    /// <para>Read out of both hosts' source, like <see cref="BothRunnersLoadAndPersistTheState"/> and for
    /// the same reason: the seam is invisible to every behavioural test in this repo.</para>
    /// </summary>
    [Fact]
    public void TheStateLoadIsByCollectorName_NotByDeclaredKey()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, RepoRootNotFound);

        var hosts = new[]
        {
            Path.Combine(root!, "Lite", "Services", "RemoteCollectorService.cs"),
            Path.Combine(root!, "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"),
        };

        foreach (var host in hosts)
        {
            var source = File.ReadAllText(host);
            var name = Path.GetFileName(host);

            var load = Regex.Match(
                source,
                @"SELECT state_key, state_value FROM collector_state WHERE (?<predicate>[^""]*)");

            Assert.True(load.Success, $"{name} no longer reads collector_state with a recognisable SELECT");

            var predicate = load.Groups["predicate"].Value;

            Assert.Contains("server_id = $1", predicate, StringComparison.Ordinal);
            Assert.Contains("collector_name = $2", predicate, StringComparison.Ordinal);
            Assert.DoesNotContain(
                "state_key",
                predicate,
                StringComparison.Ordinal);
        }

        /* No definition declares a PREFIX today. pg_index_bloat was the one that did, and #3234 removed
           its rotation cursor: the statistics estimate covers every index in one statement, so there is no
           position to resume from. Pinned as statelessness rather than deleted, because re-introducing a
           cursor here silently re-opens the prune question this whole class exists to force. */
        Assert.Empty(PgIndexBloatCollector.Instance.StateKeys);
    }

    [Fact]
    public void BothRunnersLoadAndPersistTheState()
    {
        /* This wiring is INVISIBLE to every behavioural test in this repo. Drop the save call and every
           definition test, every schema test and every round-trip test still passes — the collector keeps
           collecting, it just never records a path, so it binds NULL forever and re-reads the whole
           rollover set on every cycle on every server. That is the exact cost #1962 exists to remove, and
           it would come back silently. The runners need a live SQL Server and a live store to exercise, so
           the wiring is pinned at source in BOTH hosts, together, because a fix applied to one host and not
           the other is the drift this product keeps paying for. */
        var root = FindRepoRoot();
        Assert.True(root is not null, RepoRootNotFound);

        var hosts = new[]
        {
            Path.Combine(root!, "Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"),
            Path.Combine(root!, "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"),
        };

        foreach (var host in hosts)
        {
            var source = File.ReadAllText(host);
            var name = Path.GetFileName(host);

            /* Loaded only for the collectors that declare keys — the other 37 must not pay a query. */
            Assert.True(
                source.Contains("definition.StateKeys.Count == 0", StringComparison.Ordinal),
                $"{name} must gate the state read on the definition's declared keys");
            Assert.True(
                source.Contains("GetCollectorStateAsync(", StringComparison.Ordinal),
                $"{name} must load collector state before building the query");

            /* Handed to the definition, with the shared empty for the no-keys case. */
            Assert.True(
                source.Contains("State = collectorState ?? CollectorContext.NoState", StringComparison.Ordinal),
                $"{name} must pass the loaded state to the definition");

            /* Persisted after the cycle, from what the definition observed. */
            Assert.True(
                source.Contains("context.PendingState.Count > 0", StringComparison.Ordinal),
                $"{name} must persist only when the definition recorded something");
            Assert.True(
                source.Contains("SaveCollectorStateAsync(", StringComparison.Ordinal),
                $"{name} must persist the observed state after the cycle");
        }
    }

    /// <summary>
    /// EVERY <c>*KeyPrefix</c> in the collectors assembly has a prune verdict — in one of the two
    /// per-database registries or in one of their not-keyed-by-database lists.
    ///
    /// <para><b>Why this exists as well as the query_store guard.</b>
    /// <c>QueryStoreStatePruneTests.EveryPerDatabaseKeyPrefixIsInOneSharedList</c> discovers its population
    /// by NAME PATTERN — static classes called <c>QueryStore*State</c> — which is the right census for that
    /// family and blind to everything else. <c>pg_index_bloat</c>'s <c>rotate:</c> prefix — retired in
    /// #3234, and named here as the history that justifies this census rather than as a live member — was
    /// declared on a COLLECTOR, so it matched no pattern, appeared in no list, and shipped with no verdict:
    /// the cursors accumulated for the life of the server, and because <c>BuildQuery</c> splices all of
    /// them into one statement reused per database, the end state was PostgreSQL's 65,535-parameter limit
    /// rather than untidiness. This census is over the ASSEMBLY, so the next prefix declared anywhere
    /// arrives already covered.</para>
    ///
    /// <para><b>Membership in the right registry is checked, not just membership.</b> The two prunes have
    /// different live-database sources — <c>database_states</c> (a <c>sys.databases</c> snapshot, empty for
    /// a PostgreSQL <c>server_id</c>) versus the sweep's own enumeration — so a PostgreSQL prefix in the
    /// query_store list would be pruned by a statement that can never see its databases, and would delete
    /// nothing forever while looking like a fix.</para>
    /// </summary>
    [Fact]
    public void EveryDeclaredStateKeyPrefixHasAPruneVerdict()
    {
        var declared = typeof(PgIndexBloatCollector).Assembly.GetTypes()
            .SelectMany(type => type.GetFields(BindingFlags.Public | BindingFlags.Static))
            .Where(field => field.IsLiteral && field.FieldType == typeof(string)
                && field.Name.EndsWith("KeyPrefix", StringComparison.Ordinal))
            .Select(field => (Declarer: field.DeclaringType!.Name, Prefix: (string)field.GetRawConstantValue()!))
            .ToArray();

        /* The census has to have found something, and specifically the one the pattern-based guard misses:
           a discovery that silently returned nothing would make every assertion below vacuous. */
        Assert.NotEmpty(declared);

        /* The positive control. It used to be pg_index_bloat's rotation cursor, which was the only prefix
           declared on a COLLECTOR rather than on a *State class and therefore the one the pattern-based
           guard missed; #3234 removed it. Re-anchored to a prefix that still exists so the census cannot
           silently return nothing and make every assertion below vacuous -- and note the collector-declared
           case now has no member, so this census currently guards an empty class of prefix and is here for
           the next one. */
        Assert.Contains(
            (nameof(QueryStoreOpenIntervalState), QueryStoreOpenIntervalState.WatermarkKeyPrefix),
            declared);

        var queryStorePruned = QueryStorePerDatabaseState.PrunableKeys.Select(p => p.Prefix).ToArray();
        var pgPruned = PgPerDatabaseCollectorState.PrunableKeys.Select(p => p.Prefix).ToArray();

        foreach (var (declarer, prefix) in declared)
        {
            Assert.True(
                queryStorePruned.Contains(prefix, StringComparer.Ordinal)
                    || pgPruned.Contains(prefix, StringComparer.Ordinal)
                    || QueryStorePerDatabaseState.NotKeyedByDatabase.Contains(prefix, StringComparer.Ordinal)
                    || PgPerDatabaseCollectorState.NotKeyedByDatabase.Contains(prefix, StringComparer.Ordinal),
                $"{declarer} declares the state key prefix '{prefix}' and no registry has a verdict on it, "
                + "so nothing retires it when its database goes away. Decide which it is:\n"
                + "  - keyed by DATABASE NAME on a PostgreSQL collector: add it to "
                + "PgPerDatabaseCollectorState.PrunableKeys with its owning collector_name.\n"
                + "  - keyed by DATABASE NAME on query_store: add it to "
                + "QueryStorePerDatabaseState.PrunableKeys.\n"
                + "  - keyed by anything ELSE: add it to the matching NotKeyedByDatabase list. Do NOT put "
                + "it in a PrunableKeys to silence this - both prunes rebuild a key as prefix + "
                + "databaseName, so a key not shaped that way matches no database and is DELETED every "
                + "cycle.\n"
                + "  - and check the SOURCE: the query_store prune anti-joins database_states, which is a "
                + "sys.databases snapshot and holds no rows for a PostgreSQL server_id, so a PostgreSQL "
                + "prefix there deletes nothing forever.");
        }

        /* The PostgreSQL registry is EMPTY since #3234, and asserted empty rather than left unmentioned:
           its only member was pg_index_bloat's rotation cursor. The rule for the next member is the part
           worth keeping, because it is invisible until it is wrong -- owner and prefix travel together and
           the owner is the name the WRITER used, so a collector declaring StateKeys has both hosts persist
           under definition.Name and the prune must delete under that same name. Read it off the definition
           rather than retyping it; a literal that drifted would delete nothing and report success. */
        Assert.Empty(PgPerDatabaseCollectorState.PrunableKeys);
    }

    /// <summary>
    /// The PostgreSQL prune is WIRED, and wired after the per-database loop with the list that loop
    /// enumerated.
    ///
    /// <para>Invisible to everything else: delete the call and every prune assertion still passes, because
    /// they drive the prune directly — the cursors would simply accumulate in production, which is the
    /// shape this whole item was. Source-pinned for the same reason
    /// <see cref="BothRunnersLoadAndPersistTheState"/> is.</para>
    ///
    /// <para><b>Darling only, and that is the decision rather than an omission.</b> The query_store prune
    /// is pinned in both hosts because both SKUs run query_store. Lite registers no PostgreSQL collector at
    /// all — its <c>RunsPerDatabase</c> loop enumerates Azure databases — so a PostgreSQL prune there would
    /// be unreachable code on a SKU that cannot hold the rows. The assertion below records that by
    /// checking Lite does NOT carry it, so if Lite ever gains PostgreSQL targets this fails and the
    /// decision gets remade instead of inherited.</para>
    /// </summary>
    [Fact]
    public void TheDarlingHostPrunesPostgresPerDatabaseState_AndLiteDeliberatelyDoesNot()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, RepoRootNotFound);

        var darling = File.ReadAllText(Path.Combine(
            root!, "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

        Assert.Contains("PrunePgPerDatabaseStateAsync(", darling, StringComparison.Ordinal);

        /* Called with the list the sweep enumerated, not a re-read: the enumeration is what makes it
           authoritative, and a second read could disagree with what the loop actually swept. */
        Assert.Contains(
            "await PrunePgPerDatabaseStateAsync(server.ServerId, definition.Name, databases, cancellationToken);",
            darling,
            StringComparison.Ordinal);

        /* Gated on the REGISTRY, so a second per-database PostgreSQL collector needs no host edit — and so
           the prune cannot run for the 40 collectors that never wrote the prefix. */
        Assert.Contains("PgPerDatabaseCollectorState.PrunableKeys", darling, StringComparison.Ordinal);

        /* BOTH empty-list guards, pinned at source because they are deliberately redundant and redundancy
           is invisible to a behavioural test: with the statement's `array_length($4, 1) > 0` in place,
           deleting the caller's check changes no observable outcome, so nothing would have failed. That is
           the redundancy working — and it is also how one of the two layers gets removed as dead code by
           someone who measured that removing it broke nothing. The property they jointly protect is that an
           empty enumeration, which is how a login that cannot read pg_database presents, never deletes a
           cursor; AnEmptyEnumerationRemovesNothing exercises the statement arm directly. */
        Assert.Contains("enumeratedDatabases.Count == 0", darling, StringComparison.Ordinal);
        Assert.Contains("array_length($4, 1) > 0", darling, StringComparison.Ordinal);

        var lite = File.ReadAllText(Path.Combine(
            root!, "Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"));

        Assert.DoesNotContain("PrunePgPerDatabaseStateAsync", lite, StringComparison.Ordinal);

        /* The premise that makes Lite's exclusion correct, asserted rather than assumed. */
        Assert.DoesNotContain(
            CollectorCatalog.All.Where(c => c.TargetEngine == CollectorTargetEngine.PostgreSql),
            definition => File.ReadAllText(Path.Combine(root!, "Lite", "Services", "RemoteCollectorService.cs"))
                .Contains($"{definition.GetType().Name}.Instance", StringComparison.Ordinal));
    }

    /* ---------------- gated: live store round-trip ---------------- */

    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -196200;

    /// <summary>
    /// The live round-trip through the REAL runner against a REAL store, which is the only thing that can
    /// see this seam. Everything else in this file reads source or migration text.
    ///
    /// <para><b>Why the source pins were not enough, concretely.</b> The first version of this change bound
    /// <c>DateTime.UtcNow</c> (Kind=Utc) for <c>updated_at</c>, and <c>collector_state.updated_at</c> is
    /// <c>timestamp</c> WITHOUT time zone — Npgsql THROWS on that combination. The throw landed in
    /// <see cref="DarlingCollectorRunner.SaveCollectorStateAsync"/>'s own best-effort catch, so nothing
    /// failed, nothing logged above debug, and every source pin still passed while Darling persisted
    /// NOTHING and paid the expensive fallback on every cycle forever. Lite was unaffected (DuckDB accepts
    /// Kind=Utc), so this is exactly the single-SKU drift the rest of this file exists to prevent and could
    /// not detect.</para>
    ///
    /// <para>That is why this test asserts the value is READABLE rather than that the save did not throw:
    /// the save is best-effort by design and CANNOT throw at the caller. A write that silently does nothing
    /// and a write that works are distinguishable only by reading it back.</para>
    /// </summary>
    [Fact]
    public async Task CollectorState_SavesAndReloads_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-state test.");

        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            /* Idempotent — brings an older store to current (so V44 exists), no-ops on a current one. */
            await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

            await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        var bodySucceeded = false;
        try
        {
            var first = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [DefaultTraceEventsCollector.LastTraceFilePathStateKey] = @"S:\MSSQL\Log\log_766.trc",
            };

            await runner.SaveCollectorStateAsync(
                TestServerId, DefaultTraceEventsCollector.Instance.Name, first, TestContext.Current.CancellationToken);

            var loaded = await runner.GetCollectorStateAsync(
                TestServerId, DefaultTraceEventsCollector.Instance.Name, TestContext.Current.CancellationToken);

            Assert.Equal(
                @"S:\MSSQL\Log\log_766.trc",
                Assert.Contains(DefaultTraceEventsCollector.LastTraceFilePathStateKey, loaded));

            /* The path is rewritten on EVERY rollover, so the upsert's conflict target has to be right or
               the collector starts failing at the first rollover — the exact moment the state must hold. */
            var second = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [DefaultTraceEventsCollector.LastTraceFilePathStateKey] = @"S:\MSSQL\Log\log_767.trc",
            };

            await runner.SaveCollectorStateAsync(
                TestServerId, DefaultTraceEventsCollector.Instance.Name, second, TestContext.Current.CancellationToken);

            var reloaded = await runner.GetCollectorStateAsync(
                TestServerId, DefaultTraceEventsCollector.Instance.Name, TestContext.Current.CancellationToken);

            Assert.Equal(
                @"S:\MSSQL\Log\log_767.trc",
                Assert.Contains(DefaultTraceEventsCollector.LastTraceFilePathStateKey, reloaded));

            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            /* Exactly one row survived the second save, and updated_at was really written — a Kind=Utc bind
               never gets this far, and a broken conflict target would leave two rows. */
            /* Schema-qualified: this is the TEST's own housekeeping, and it must not depend on session
               search_path. The runner's own SQL stays bare (`collector_state`) and resolves through the
               store's `collect, config, public` path exactly as it does in production — that resolution is
               part of what this test exercises; only the assertions around it are pinned down. On a store
               migrated within this same run, a pooled physical connection can still carry the session path
               it was established with, which is a rig artifact rather than anything about the product. */
            using var check = new NpgsqlCommand(
                "SELECT COUNT(*), MAX(updated_at) FROM collect.collector_state WHERE server_id = $1", connection);
            check.Parameters.AddWithValue(TestServerId);
            using var reader = await check.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.False(reader.IsDBNull(1), "updated_at must be stored, not left null");

            /* NAIVE UTC — the store-wide convention every other timestamp column follows, and the assertion
               that actually has teeth here. Checking Kind alone would be vacuous: a `timestamp` column
               ALWAYS reads back Unspecified regardless of what was written. What can really go wrong is the
               VALUE: bind a Kind=Utc DateTime and Npgsql infers `timestamptz`, PostgreSQL casts it into the
               `timestamp` column, and the cast renders it in the SERVER's zone — so updated_at silently
               lands offset by the server's UTC offset (four hours on this rig, America/New_York) while
               every other timestamp in the store is UTC. A generous window keeps this about the offset
               rather than about clock skew or test duration. */
            var storedUpdatedAt = reader.GetDateTime(1);
            Assert.Equal(DateTimeKind.Unspecified, storedUpdatedAt.Kind);

            var driftFromUtc = (DateTime.UtcNow - storedUpdatedAt).Duration();
            Assert.True(
                driftFromUtc < TimeSpan.FromMinutes(5),
                $"updated_at must be naive UTC; stored {storedUpdatedAt:o} is {driftFromUtc} from UtcNow "
                + "(a whole-hour gap means it was written as timestamptz and rendered in the server's zone)");

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: teardown goes through LiveStoreCleanup, never a hand-rolled finally. A cleanup that
               throws on its own connection would REPLACE the body's real failure with a connection error,
               which is how the #1794 flake hid its actual cause for so long. */
            await LiveStoreCleanup.RunAsync(
                connectionString!,
                bodySucceeded,
                (connection, cancellationToken) => DeleteTestRowsAsync(connection, cancellationToken));
        }
    }

    /// <summary>
    /// Removes this test's rows so the shared live store is left exactly as found — the
    /// <see cref="LivePostgresStoreFixture"/> cleanup check fails the collection otherwise (#1873).
    /// </summary>
    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken cancellationToken)
    {
        using var delete = new NpgsqlCommand("DELETE FROM collect.collector_state WHERE server_id = $1", connection);
        delete.Parameters.AddWithValue(TestServerId);
        await delete.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Walks up from the test output directory to the repo root — the directory holding
    /// <c>PerformanceMonitor.sln</c>. Same walk-up idiom as <c>DocCommentHygieneTests.FindRepoRoot</c>.
    /// </summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
