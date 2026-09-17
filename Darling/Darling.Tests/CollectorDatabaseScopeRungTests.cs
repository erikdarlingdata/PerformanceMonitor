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
using System.Linq;
using System.Reflection;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V125 / #3477: the optional per-collector database scope on <c>config_collector_schedules</c> — an
/// ALLOW-LIST beside <c>enabled</c> and the cadence columns, so an expensive per-database collector
/// can be limited to a representative sample instead of turned off for the whole server. The measured
/// case: a 72-database instance paying a 43-minute <c>index_object_stats</c> pass whose slowest
/// database carried 4.97% of it — fan-out WIDTH, which only a narrower fan-out can cheapen.
///
/// <para>The semantics under pin, in one place: NULL/empty effective scope = every database the
/// server enumerates (no behavior change for anyone who does not set it); non-empty = ONLY those
/// databases; the fleet-wide row (<c>server_id IS NULL</c>) sets a default a server row overrides,
/// with the array-specific reading that a NULL column falls through and an explicit EMPTY array is
/// "no scope at this level" and stops the fall-through; and <c>excludedDatabases</c> still WINS,
/// because the composed predicate is scoped-in AND NOT excluded. New databases stay OUT of a
/// non-empty scope until named — the allow-list-not-deny-list argument the issue was won on.</para>
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="FleetSweepCadenceKnobRungTests"/> (V124) when this rung landed, the same handoff that
/// file received from <see cref="FleetSweepStateRungTests"/> (V123) — a fully-migrated store must map
/// to EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually
/// current.</para>
/// </summary>
public sealed class CollectorDatabaseScopeRungTests
{
    private const int RungVersion = 125;
    private const int PreviousVersion = 124;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 100;

    private const string ScopeColumn = "databases";

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "collector-database-scope",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds ONE nullable column, schema-qualified, and takes no other decision: no default
    /// (NULL is this sparse table's own "not overridden here"), no NOT NULL, no CHECK (there is no
    /// numeric bound to hold — the resolver sanitizes names, and an unmatchable name simply matches
    /// nothing on the engine), no GRANT (the table carries table-level grants with no column carve),
    /// no reload beacon of its own (V17's statement-level <c>trg_bump_collector_schedules</c> already
    /// bumps on any write here), and no data movement — so the #2894 data-moving rung census and the
    /// migration lock-wait floor are untouched by this rung.
    /// </summary>
    [Fact]
    public void TheRungAddsOneNullableColumn_SchemaQualified_WithNoDefaultCheckGrantBeaconOrDataMovement()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema (and the wrong ACL). */
        Assert.Equal(1, CountOf(rung, "ALTER TABLE config.config_collector_schedules"));
        Assert.DoesNotContain("ALTER TABLE config_collector_schedules", rung, StringComparison.Ordinal);

        /* IF NOT EXISTS so re-running the ladder over a store that already has it is a no-op rather
           than a 42701 that aborts the whole migration. text[], matching how the row's sibling
           name-list (the registry's excluded_databases) is stored — one storage shape for the two
           instruments that must agree on what a name is. */
        Assert.Equal(1, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));
        Assert.Contains($"ADD COLUMN IF NOT EXISTS {ScopeColumn} text[];", rung, StringComparison.Ordinal);

        Assert.DoesNotContain("NOT NULL", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("DEFAULT", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("CHECK", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("config_bump_version", rung, StringComparison.Ordinal);

        /* No data movement: a nullable no-default ADD COLUMN is metadata-only, which is what keeps
           this rung out of MigrationDataMovingRungCensusPins' register. */
        foreach (var shape in new[] { "UPDATE ", "DELETE ", "INSERT ", "CREATE INDEX" })
        {
            Assert.DoesNotContain(shape, rung, StringComparison.Ordinal);
        }
    }

    /* ---- the probe (three sites, top arm) ------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map treats it as the TOP arm.
    ///
    /// <para>The probe asks the question, the caller reads the answer, the map has the parameter — three
    /// sites, and a sentinel present at only some of them shifts every LATER ordinal onto the wrong column.
    /// Miss all three and a fully-migrated store probes one rung short, so the connect-time gate refuses a
    /// store that is in fact current — permanently, because no later upgrade changes the answer.</para>
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains($"column_name = '{ScopeColumn}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
        Assert.Contains("table_name = 'config_collector_schedules'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasCollectorScheduleDatabases", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The top rung's sentinel IS the last argument. */
        Assert.Equal(ProbeOrdinal, arity - 1);

        /* Every sentinel true = a fully-migrated store, which must map to exactly this version. Built by
           reflection so the arity tracks the signature. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* This rung's own arm answers for a store that stopped here. Expressed as "false above" rather than
           as one named ordinal, so a rung landing on top of this one does not quietly turn this case into a
           test of that rung. */
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        /* One rung behind: the same store WITHOUT this rung's sentinel reports the previous rung. */
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* And in the source, the arm sits ABOVE V124's — newest-first is the whole contract of that method —
           and returns this build's version rather than a literal that could drift from it. This is the
           textual half of the top-arm claim, inherited from FleetSweepCadenceKnobRungTests the way that
           file inherited it from FleetSweepStateRungTests. */
        var v125 = viewer.IndexOf("if (hasCollectorScheduleDatabases)", StringComparison.Ordinal);
        var v124 = viewer.IndexOf("if (hasFleetSweepCadenceKnobs)", StringComparison.Ordinal);
        Assert.True(v125 >= 0, "the viewer has no V125 sentinel arm — a fully-migrated store would map to 124");
        Assert.True(v124 >= 0, "the V124 arm is gone, so this pin is comparing against nothing");
        Assert.True(v125 < v124, "the V125 arm sits below V124's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[v125..], StringComparison.Ordinal);
    }

    /* ---- every schedule-row surface handles the column ------------------------------------------------ */

    /// <summary>
    /// EVERY surface that reads or replaces whole schedule rows names the scope column — and the ONE
    /// surface that must NOT is pinned to its abstinence. The read/replace surfaces drive ordinals and
    /// parameter positions, so a column added to one and not the others re-maps reads and writes at
    /// once; the toggle upsert is the opposite case: it upserts (server_id, collector_name, enabled)
    /// ONLY, so an enable/disable command can never null out a scope an operator configured — naming
    /// the column there would be the clobber, not the fix.
    /// </summary>
    [Fact]
    public void EveryScheduleRowSurfaceNamesTheScopeColumn_AndTheToggleUpsertDeliberatelyDoesNot()
    {
        /* The service's reload read: selected AND read as a string[], preserving NULL — a selected-but-
           unread column would silently resolve every scope to "unscoped" on the next reload. */
        var service = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");
        Assert.Contains(
            "SELECT server_id, collector_name, frequency_minutes, retention_days, enabled, databases FROM config_collector_schedules",
            service, StringComparison.Ordinal);
        Assert.Contains("reader.GetFieldValue<string[]>(5)", service, StringComparison.Ordinal);

        /* The viewer's editor select mirrors the service's column order. */
        Assert.Contains(
            "enabled, databases FROM config_collector_schedules",
            ViewerDataService.CollectorSchedulesSelectSql, StringComparison.Ordinal);

        /* Both viewer upserts write the column through the conflict arm — or Save silently drops
           whatever scope the store held (the editor's Save is a delete-and-reinsert of the whole
           scope, so "not named" is not "left alone" there; it is "destroyed"). */
        foreach (var sql in new[]
        {
            ViewerDataService.CollectorScheduleFleetUpsertSql,
            ViewerDataService.CollectorScheduleServerUpsertSql,
        })
        {
            Assert.Contains(ScopeColumn, sql, StringComparison.Ordinal);
            Assert.Contains($"{ScopeColumn} = EXCLUDED.{ScopeColumn}", sql, StringComparison.Ordinal);
        }

        /* The command executor's enable/disable upsert names exactly its three columns and updates
           exactly enabled — twice, one per scope arm. It is the one writer whose NOT naming the scope
           is the correctness property: a toggle's INSERT leaves the column NULL and its conflict arm
           leaves the stored value standing. */
        var executor = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCommandExecutor.cs");
        Assert.Equal(2, CountOf(executor, "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES"));
        Assert.Equal(2, CountOf(executor, "DO UPDATE SET enabled = EXCLUDED.enabled"));
    }

    /* ---- the layering (pure) --------------------------------------------------------------------------- */

    /// <summary>
    /// The layering matrix, at the resolver the runner actually calls: no rows = unscoped; a fleet row
    /// sets the default; a per-server row overrides it; a NULL per-server column falls through to the
    /// fleet value (the row's standard per-column layering); and an explicit EMPTY per-server array
    /// STOPS the fall-through — the opt-back-out a deny-list cannot express and the reason NULL and
    /// empty are distinct readings everywhere on this path.
    /// </summary>
    [Fact]
    public void ResolveDatabaseScope_LayersPerServerOverFleet_WithNullFallingThroughAndEmptyStopping()
    {
        /* No rows at all: unscoped — the shipped state of every install. */
        Assert.Empty(StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, Array.Empty<ScheduleOverride>()));

        var fleetOnly = new[]
        {
            new ScheduleOverride(null, "index_object_stats", null, null, true, new[] { "RefDb" }),
        };
        Assert.Equal(new[] { "RefDb" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, fleetOnly));

        /* A per-server list wins over the fleet's. */
        var serverWins = new[]
        {
            new ScheduleOverride(null, "index_object_stats", null, null, true, new[] { "RefDb" }),
            new ScheduleOverride(123, "index_object_stats", null, null, true, new[] { "MyDb", "OtherDb" }),
        };
        Assert.Equal(new[] { "MyDb", "OtherDb" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, serverWins));

        /* A NULL per-server column falls through to the fleet default — the per-column layering every
           other column on this row already has. */
        var serverNull = new[]
        {
            new ScheduleOverride(null, "index_object_stats", null, null, true, new[] { "RefDb" }),
            new ScheduleOverride(123, "index_object_stats", 720, null, true, Databases: null),
        };
        Assert.Equal(new[] { "RefDb" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, serverNull));

        /* An explicit EMPTY per-server array stops the fall-through: this server collects everything
           even though the fleet default scopes. Without this distinction the only way out of a fleet
           scope would be naming every database the server has — the deny-list failure, one layer up. */
        var serverEmpty = new[]
        {
            new ScheduleOverride(null, "index_object_stats", null, null, true, new[] { "RefDb" }),
            new ScheduleOverride(123, "index_object_stats", null, null, true, Array.Empty<string>()),
        };
        Assert.Empty(StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, serverEmpty));

        /* Another server's row does not leak, and the collector-name match is case-insensitive — both
           exactly as ResolveSchedule reads the same rows, because two resolvers over one table that
           disagree about row identity would scope one collector by another's row. */
        var foreign = new[]
        {
            new ScheduleOverride(999, "index_object_stats", null, null, true, new[] { "NotMine" }),
        };
        Assert.Empty(StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, foreign));

        var cased = new[]
        {
            new ScheduleOverride(123, "INDEX_OBJECT_STATS", null, null, true, new[] { "RefDb" }),
        };
        Assert.Equal(new[] { "RefDb" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, cased));

        /* Hand-edit noise degrades the ValidRetention way — toward collecting, never toward silently
           collecting nothing: blank entries drop, and a list that sanitizes to nothing is unscoped. */
        var noisy = new[]
        {
            new ScheduleOverride(123, "index_object_stats", null, null, true, new[] { "  RefDb  ", "", "   " }),
        };
        Assert.Equal(new[] { "RefDb" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, noisy));

        var allBlank = new[]
        {
            new ScheduleOverride(123, "index_object_stats", null, null, true, new[] { "", "  " }),
        };
        Assert.Empty(StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, allBlank));
    }

    /* ---- the predicate: matching parity, and the exclusion still wins ---------------------------------- */

    /// <summary>
    /// The scope filter is the exclusion filter's STRUCTURAL mirror: same parameter type, same
    /// per-name parameterization, plain engine-evaluated equality — so a scoped-in name matches
    /// exactly the way an excluded name does on that engine (SQL Server folds case under its default
    /// collations, PostgreSQL compares byte for byte), and no client-side comparer gets an opinion.
    /// The parity is the point: the two instruments must agree on what a name IS, or one spelling
    /// could be scoped in and not excludable.
    /// </summary>
    [Fact]
    public void TheScopeFilter_MirrorsTheExclusionFiltersParameterShape_SoTheEngineJudgesBothLists()
    {
        var (scopeClause, scopeParameters) = DatabaseScopeFilter.Build(new[] { "A", "B" }, "d.name");
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(new[] { "A", "B" }, "d.name");

        Assert.Equal("AND d.name IN (@scope_db_0, @scope_db_1)", scopeClause);
        Assert.Equal("AND d.name NOT IN (@excl_db_0, @excl_db_1)", exclusionClause);

        Assert.Equal(exclusionParameters.Count, scopeParameters.Count);
        for (var i = 0; i < scopeParameters.Count; i++)
        {
            Assert.Equal(exclusionParameters[i].Type, scopeParameters[i].Type);
            Assert.Equal(exclusionParameters[i].Value, scopeParameters[i].Value);
        }

        /* Empty in = nothing out: the unscoped query is byte-identical to the pre-#3477 form. */
        Assert.Equal(string.Empty, DatabaseScopeFilter.Build(Array.Empty<string>(), "d.name").Clause);
        Assert.Equal(string.Empty, DatabaseScopeFilter.Build(null, "d.name").Clause);
    }

    /// <summary>
    /// The composed predicate IS the intersection: scoped-in AND NOT excluded, so a database named in
    /// both lists stays OUT — <c>excludedDatabases</c> keeps its veto as the coarse instrument, which
    /// is the composition the issue was accepted on ("composes with, rather than replaces").
    /// </summary>
    [Fact]
    public void TheComposedPredicate_IsScopedInMinusExcluded_SoTheExclusionStillWins()
    {
        var context = Context(scope: new[] { "KeepMe", "AlsoExcluded" }, excluded: new[] { "AlsoExcluded" });

        var (clause, parameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");

        Assert.Equal("AND d.name IN (@scope_db_0, @scope_db_1) AND d.name NOT IN (@excl_db_0)", clause);
        Assert.Equal(new object?[] { "KeepMe", "AlsoExcluded", "AlsoExcluded" }, parameters.Select(p => p.Value).ToArray());

        /* Each list alone degrades to exactly that list's clause; neither leaves a dangling AND. */
        var scopeOnly = Context(scope: new[] { "KeepMe" });
        Assert.Equal("AND d.name IN (@scope_db_0)", DatabaseScopeFilter.BuildEnumerationPredicate(scopeOnly, "d.name").Clause);

        var exclusionOnly = Context(excluded: new[] { "Nope" });
        Assert.Equal("AND d.name NOT IN (@excl_db_0)", DatabaseScopeFilter.BuildEnumerationPredicate(exclusionOnly, "d.name").Clause);

        var neither = Context();
        Assert.Equal(string.Empty, DatabaseScopeFilter.BuildEnumerationPredicate(neither, "d.name").Clause);
    }

    /* ---- the seam reaches every fan-out enumeration ----------------------------------------------------- */

    /// <summary>
    /// The census, taken from the CATALOG rather than a hand-list: every definition that enumerates on
    /// ANY target (the five get_collection_health names today — query_store, plan_correction,
    /// query_store_health, index_object_stats, database_scoped_config) splices the scope into its
    /// enumeration when the context carries one, and none of them does when it does not. Walking
    /// CollectorCatalog.All is what makes a SIXTH enumerating collector that skips the seam fail here,
    /// in its author's own run, instead of shipping a collector the scope silently does not govern.
    /// </summary>
    [Fact]
    public void EveryEnumeratingCollector_SplicesTheScope_AndCountsMatchTheHealthCensus()
    {
        var scoped = new[] { "ScopeA", "ScopeB" };
        var excluded = new[] { "Nope" };
        var enumerators = new HashSet<string>(StringComparer.Ordinal);

        foreach (var target in new[]
        {
            new CollectorTargetInfo(),                       /* on-prem SQL Server */
            new CollectorTargetInfo { IsAzureSqlDb = true }, /* Azure SQL DB */
        })
        {
            foreach (var definition in CollectorCatalog.All)
            {
                var method = definition.GetType().GetMethod("BuildEnumerationQuery");
                if (method is null)
                {
                    continue; /* Not a definition shape that can enumerate. */
                }

                var scopedContext = Context(target, scoped, excluded);
                if (method.Invoke(definition, new object[] { scopedContext }) is not CollectorQuery scopedPlan)
                {
                    continue; /* Does not enumerate on this target. */
                }

                enumerators.Add(definition.GetType().Name);

                /* The scope rides the enumeration as parameters, before the exclusion, never as text. */
                Assert.Contains("@scope_db_0", scopedPlan.Text, StringComparison.Ordinal);
                Assert.Contains("@scope_db_1", scopedPlan.Text, StringComparison.Ordinal);
                Assert.True(
                    scopedPlan.Text.IndexOf("@scope_db_0", StringComparison.Ordinal)
                        < scopedPlan.Text.IndexOf("@excl_db_0", StringComparison.Ordinal),
                    $"{definition.GetType().Name}: the scope clause must precede the exclusion so the composed predicate reads scoped-in minus excluded");
                Assert.DoesNotContain("ScopeA", scopedPlan.Text, StringComparison.Ordinal);
                Assert.Contains(scopedPlan.Parameters, p => Equals(p.Value, "ScopeA"));
                Assert.Contains(scopedPlan.Parameters, p => Equals(p.Value, "ScopeB"));

                /* And an unscoped context builds the pre-#3477 enumeration — no clause, no parameters,
                   no behavior change for anyone who does not set the knob. */
                var unscopedContext = Context(target, excluded: excluded);
                var unscopedPlan = (CollectorQuery)method.Invoke(definition, new object[] { unscopedContext })!;
                Assert.DoesNotContain("@scope_db_", unscopedPlan.Text, StringComparison.Ordinal);
            }
        }

        /* The census count IS the get_collection_health description's census: five enumerate. A sixth
           name appearing here is the signal to extend that description, not loosen this pin. */
        Assert.Equal(
            new[]
            {
                nameof(DatabaseScopedConfigCollector),
                nameof(IndexObjectStatsCollector),
                nameof(PlanCorrectionCollector),
                nameof(QueryStoreCollector),
                nameof(QueryStoreHealthCollector),
            },
            enumerators.OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The OTHER fan-out family — the per-database CONNECTION loop (Azure SQL DB's eight, PostgreSQL's
    /// per-database collectors) — takes the scope inside the SAME engine-evaluated list plan the
    /// exclusion rides, per provider, so both instruments are judged by the engine's collation reality
    /// on that path too. And the maintenance-database screen survives a scope that names it: naming
    /// <c>rdsadmin</c> cannot un-screen it, because the screen is not an instrument operators compose.
    /// </summary>
    [Fact]
    public void BothProviders_SpliceTheScopeIntoTheDatabaseListPlan_AndTheMaintenanceScreenStandsAbove()
    {
        var scope = new[] { "KeepMe" };
        var excluded = new[] { "Nope" };

        var (_, sqlQuery) = SqlServerTargetProvider.Instance.BuildDatabaseListPlan("Server=sql1", excluded, scope);
        Assert.Contains("name IN (@scope_db_0)", sqlQuery.Text, StringComparison.Ordinal);
        Assert.Contains("name NOT IN (@excl_db_0)", sqlQuery.Text, StringComparison.Ordinal);
        Assert.Equal(2, sqlQuery.Parameters.Count);
        Assert.DoesNotContain("KeepMe", sqlQuery.Text, StringComparison.Ordinal);

        var (_, pgQuery) = PostgresTargetProvider.Instance.BuildDatabaseListPlan("Host=aurora", excluded, new[] { "KeepMe", PostgresTargetProvider.ManagedMaintenanceDatabase });
        Assert.Contains("datname IN (@scope_db_0, @scope_db_1)", pgQuery.Text, StringComparison.Ordinal);
        Assert.Contains("datname NOT IN (@excl_db_0)", pgQuery.Text, StringComparison.Ordinal);
        Assert.Contains($"datname <> '{PostgresTargetProvider.ManagedMaintenanceDatabase}'", pgQuery.Text, StringComparison.Ordinal);

        /* A null scope builds the pre-#3477 plan on both engines. */
        var (_, unscopedSql) = SqlServerTargetProvider.Instance.BuildDatabaseListPlan("Server=sql1", excluded, null);
        var (_, unscopedPg) = PostgresTargetProvider.Instance.BuildDatabaseListPlan("Host=aurora", excluded, null);
        Assert.DoesNotContain("@scope_db_", unscopedSql.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@scope_db_", unscopedPg.Text, StringComparison.Ordinal);

        /* The scoped plan still maps through each provider's own command factory — the same parameter
           mapping every collector query takes, so a scope parameter cannot be mapped two ways. */
        using var pgConnection = new NpgsqlConnection("Host=nowhere");
        using var pgCommand = PostgresTargetProvider.Instance.CreateCommand(pgQuery, pgConnection, 60);
        Assert.Equal(3, pgCommand.Parameters.Count);
    }

    /// <summary>
    /// The runner feeds ONE resolution per run to all three consumers — the dispatch probe, the
    /// per-database loop's list plan, and the enumeration context — and the stated boundaries hold:
    /// the worker's delegate reads the SAME live overrides the cadence gate reads; the Azure XE
    /// session provisioner deliberately passes NO scope (session inventory is server-shaped — three
    /// collectors with three possible scopes share those sessions); and Lite's definition runner
    /// never names the scope at all, so every Lite query stays byte-identical to the unscoped form.
    /// </summary>
    [Fact]
    public void TheRunnerResolvesOnceAndFeedsAllThreeConsumers_AndTheStatedBoundariesHold()
    {
        var runner = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        Assert.Equal(1, CountOf(runner, "_databaseScope(definition.Name, server.ServerId)"));
        Assert.Contains("GetPostgresDatabaseListAsync(server, databaseScope, cancellationToken)", runner, StringComparison.Ordinal);
        Assert.Contains("GetAzureDatabaseListAsync(server, databaseScope, cancellationToken)", runner, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(runner, "DatabaseScope = databaseScope,"));

        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains(
            "databaseScope: (collectorName, serverId) => StoreConfigProvider.ResolveDatabaseScope(collectorName, serverId, _scheduleOverrides)",
            worker, StringComparison.Ordinal);

        var xe = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");
        Assert.Equal(2, CountOf(xe, "GetAzureDatabaseListAsync(server, databaseScope: null"));

        var lite = RepoFile.ReadRepoFile("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs");
        Assert.DoesNotContain("DatabaseScope", lite, StringComparison.Ordinal);
    }

    /* ---- the viewer round trip -------------------------------------------------------------------------- */

    /// <summary>
    /// The editor's overlay carries the scope through its whole round trip, with the null/empty
    /// distinction landing exactly where the resolver reads it: a fleet row with a blank box writes
    /// NULL (nothing to opt out of at fleet level — NULL is "not overridden"), a CUSTOM server row
    /// with a blank box writes the EXPLICIT empty array (WYSIWYG: the server collects exactly the
    /// shown scope, so a fleet scope cannot bleed through a grid that shows none), and a scope alone
    /// is enough to earn a sparse fleet row — the #2064/#2061 skipped-row failure, one column over,
    /// is what that last arm exists to prevent.
    /// </summary>
    [Fact]
    public void TheOverlay_RoundTripsTheScope_WithNullAtFleet_EmptyAtCustomServer_AndAScopeAloneEarnsARow()
    {
        /* Parse mirrors the Settings window's excluded-databases discipline: split on comma, trim,
           drop blanks — one entry format for the two instruments. */
        Assert.Equal(new[] { "a", "b" }, CollectorScheduleOverlay.ParseDatabases(" a , ,b "));
        Assert.Empty(CollectorScheduleOverlay.ParseDatabases(null));
        Assert.Empty(CollectorScheduleOverlay.ParseDatabases("  ,  "));

        /* A fleet row's scope shows through in the effective grid; a server row's NULL falls through;
           a server row's EMPTY array overrides the fleet scope back to blank. */
        var overrides = new List<CollectorScheduleRow>
        {
            new(null, "index_object_stats", null, null, true, new[] { "RefDb" }),
            new(7, "index_object_stats", null, null, true, Array.Empty<string>()),
        };
        var fleetView = CollectorScheduleOverlay.BuildEffectiveSchedule(overrides, null);
        Assert.Equal("RefDb", fleetView.Single(i => i.Name == "index_object_stats").DatabasesText);

        var serverView = CollectorScheduleOverlay.BuildEffectiveSchedule(overrides, 7);
        Assert.Equal("", serverView.Single(i => i.Name == "index_object_stats").DatabasesText);

        /* Fleet save: a collector at DEFAULT cadence with a scope still writes its row, carrying the
           parsed list; a blank box writes NULL, not an empty array. */
        var edited = CollectorSchedulePresets.BuildDefaultSchedule();
        edited.Single(i => i.Name == "index_object_stats").DatabasesText = "RefDb, OtherDb";
        var fleetRows = CollectorScheduleOverlay.ToFleetOverrideRows(edited);
        var fleetRow = Assert.Single(fleetRows);
        Assert.Equal("index_object_stats", fleetRow.CollectorName);
        Assert.Equal(new[] { "RefDb", "OtherDb" }, fleetRow.Databases);

        edited.Single(i => i.Name == "index_object_stats").DatabasesText = "";
        Assert.Empty(CollectorScheduleOverlay.ToFleetOverrideRows(edited));

        /* Server save is a full WYSIWYG snapshot: blank writes the explicit empty array. */
        var serverRows = CollectorScheduleOverlay.ToServerOverrideRows(edited, 7);
        Assert.All(serverRows, r => Assert.NotNull(r.Databases));
        Assert.All(serverRows, r => Assert.Empty(r.Databases!));

        edited.Single(i => i.Name == "index_object_stats").DatabasesText = "RefDb";
        var scopedServerRows = CollectorScheduleOverlay.ToServerOverrideRows(edited, 7);
        Assert.Equal(new[] { "RefDb" }, scopedServerRows.Single(r => r.CollectorName == "index_object_stats").Databases);
    }

    /* ---- live (DARLING_TEST_PG): the column round-trips with NULL and empty distinct ------------------- */

    /// <summary>
    /// Against a real store: the ladder lands the column, the viewer's own upsert SQL round-trips a
    /// list, an explicit empty array and NULL as three DIFFERENT stored readings through the service's
    /// own select, and a scope write bumps the V17 reload beacon — the live half of the "a store write
    /// is honored on the next sweep" claim. Rolled back, so the shared dev store is never mutated.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task TheScopeColumn_RoundTripsLive_WithNullAndEmptyDistinct_AndBumpsTheBeacon()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the scope round-trip test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var tx = await connection.BeginTransactionAsync(ct);

        await using (var seed = new NpgsqlCommand(
            "INSERT INTO config.config_service (id, updated_at) VALUES (1, now() AT TIME ZONE 'UTC') ON CONFLICT (id) DO NOTHING",
            connection, tx))
        {
            await seed.ExecuteNonQueryAsync(ct);
        }

        long Beacon()
        {
            using var read = new NpgsqlCommand("SELECT config_version FROM config.config_service WHERE id = 1", connection, tx);
            return Convert.ToInt64(read.ExecuteScalar(), CultureInfo.InvariantCulture);
        }

        var before = Beacon();

        /* The viewer's own upsert SQL, exercised verbatim: a fleet row carrying a list, and a server
           row carrying the EXPLICIT empty array. Schema-qualified via search_path defaulting to the
           connection role's — the viewer runs these against config through its own search path, so
           set it the way the constants expect a bare table name to resolve. */
        await using (var path = new NpgsqlCommand("SET LOCAL search_path = config, public", connection, tx))
        {
            await path.ExecuteNonQueryAsync(ct);
        }

        await using (var fleet = new NpgsqlCommand(ViewerDataService.CollectorScheduleFleetUpsertSql, connection, tx))
        {
            fleet.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "index_object_stats" });
            fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            fleet.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = true });
            fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = new[] { "RefDb" } });
            await fleet.ExecuteNonQueryAsync(ct);
        }

        await using (var server = new NpgsqlCommand(ViewerDataService.CollectorScheduleServerUpsertSql, connection, tx))
        {
            server.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 424242 });
            server.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "index_object_stats" });
            server.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            server.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            server.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = true });
            server.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = Array.Empty<string>() });
            await server.ExecuteNonQueryAsync(ct);
        }

        Assert.True(Beacon() > before, "a scope write must bump config_version — the live-reload claim");

        /* Read back through the SERVICE's column order and prove all three readings are distinct. */
        var overrides = new List<ScheduleOverride>();
        await using (var read = new NpgsqlCommand(
            "SELECT server_id, collector_name, frequency_minutes, retention_days, enabled, databases " +
            "FROM config.config_collector_schedules WHERE collector_name = 'index_object_stats' AND (server_id = 424242 OR server_id IS NULL)",
            connection, tx))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                overrides.Add(new ScheduleOverride(
                    reader.IsDBNull(0) ? null : reader.GetInt32(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    reader.GetBoolean(4),
                    reader.IsDBNull(5) ? null : reader.GetFieldValue<string[]>(5)));
            }
        }

        var fleetRow = overrides.Single(o => o.ServerId is null);
        var serverRow = overrides.Single(o => o.ServerId == 424242);
        Assert.Equal(new[] { "RefDb" }, fleetRow.Databases);
        Assert.NotNull(serverRow.Databases);
        Assert.Empty(serverRow.Databases!);

        /* And the resolver reads them the documented way: the explicit empty stops the fall-through
           on the written server, while every OTHER server inherits the fleet scope. */
        Assert.Empty(StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 424242, overrides));
        Assert.Equal(new[] { "RefDb" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 777, overrides));

        await tx.RollbackAsync(ct);
    }

    /// <summary>A minimal context with the required members satisfied — the DeadlocksPlanSpliceTests
    /// idiom. Everything the scope predicate reads is what the caller passes.</summary>
    private static CollectorContext Context(
        CollectorTargetInfo? target = null,
        IReadOnlyList<string>? scope = null,
        IReadOnlyList<string>? excluded = null) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = DateTime.UtcNow,
        Deltas = null!,
        Target = target ?? new CollectorTargetInfo(),
        DatabaseScope = scope ?? Array.Empty<string>(),
        ExcludedDatabases = excluded ?? Array.Empty<string>(),
    };

    /// <summary>Non-overlapping occurrences of <paramref name="needle"/>.</summary>
    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var at = haystack.IndexOf(needle, StringComparison.Ordinal);
        while (at >= 0)
        {
            count++;
            at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }
}
