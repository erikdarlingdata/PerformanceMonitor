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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V123 / #3466 (lane 1 of 4): the fleet sweep's state store — the sweep-run tables, the
/// would-have-paged ledger, and the watch-item worklist with its hysteresis state machine.
///
/// <para>The missing primitive the spec names is sweep-over-sweep STATE: the external sweep that
/// produced the requirements diffed against its own previous report file, and the product implements
/// that as data. This rung is the store half only — the engine (lane 2), the web feed (lane 3) and
/// delivery/MCP (lane 4) land as their own PRs against these shapes.</para>
///
/// <para>The "I am the top rung" claims have moved ON to <see cref="FleetSweepCadenceKnobRungTests"/>
/// (V124, this feature's own lane-2 rung), the same handoff this file received from
/// <see cref="PgAlertCountKnobRungTests"/> (V122) and that file received from
/// <see cref="OversizedPlanBacklogPins"/> (V121). What stays here is everything true of this rung
/// wherever it sits in the ladder; what left is every claim that was really about being NEWEST.
/// Keeping a copy of those would assert this rung is still the top, which is how the NEXT rung's
/// build goes red — the note every demoted file in this line leaves for its successor.</para>
///
/// <para>The live half — the DDL parsing, the writer's single transaction, the upsert's birth-record
/// preservation, and the probe's arity against a real store — runs in
/// <c>FleetSweepStoreLivePostgresTests</c>, split out per the LivePostgresCollectionHygieneTests
/// shape so these pure pins never pay for a store connection.</para>
/// </summary>
public sealed class FleetSweepStateRungTests
{
    private const int RungVersion = 123;
    private const int PreviousVersion = 122;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V124
    /// appended its own — so this is a position within the signature rather than its end.</summary>
    private const int ProbeOrdinal = 98;

    private static PgMigrations.Migration V123 =>
        PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("fleet-sweep-state", V123.Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which
           stopped being true when V124 landed. The invariant that outlives the handoff is that the
           LADDER's top and the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung IS the store's own DDL — the V38/V121 idiom, asserted as composition so drift between
    /// the shape a store gets and the shape the statements address is unexpressible, not merely
    /// unpinned — and it takes no decision beyond the tables.
    /// </summary>
    [Fact]
    public void TheRungIsTheStoresOwnDdl_SchemaQualified_WithNoAclOrDataDecision()
    {
        Assert.Contains(FleetSweepStore.CreateTablesSql, V123.Sql, StringComparison.Ordinal);

        /* All four tables, schema-qualified, CREATE TABLE IF NOT EXISTS so replaying the ladder over
           a store that already has them is a no-op rather than a 42P07 that aborts the migration. */
        foreach (var table in new[]
        {
            FleetSweepStore.RunsTableName,
            FleetSweepStore.VerdictsTableName,
            FleetSweepStore.WouldHavePagedTableName,
            FleetSweepStore.WatchItemsTableName,
        })
        {
            Assert.StartsWith("collect.", table, StringComparison.Ordinal);
            Assert.Contains($"CREATE TABLE IF NOT EXISTS {table}", V123.Sql, StringComparison.Ordinal);
        }

        Assert.Equal(4, CountOf(V123.Sql, "CREATE TABLE IF NOT EXISTS"));
        Assert.Equal(4, CountOf(V123.Sql, "CREATE TABLE IF NOT EXISTS collect."));

        /* No GRANT: collect carries blanket SELECT for admin/viewer/mcp plus the owner-scoped
           default privileges, both re-asserted every managed start, so the web feed (viewer role,
           lane 3) and get_sweep_reports (mcp role, lane 4) read these tables with no new statement
           and the engine writes as the owner. A grant appearing here would mean the carve had
           changed and this rung had quietly taken on an ACL decision. */
        Assert.DoesNotContain("GRANT", V123.Sql, StringComparison.Ordinal);

        /* And no data movement: CREATE TABLE only, so the data-moving rung census and the migration
           lock-wait floor are untouched by this rung. */
        foreach (var shape in new[] { "UPDATE ", "DELETE ", "INSERT ", "ALTER TABLE" })
        {
            Assert.DoesNotContain(shape, V123.Sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The quiet-is-not-clean contract, held by the SCHEMA: both instrument-liveness columns are NOT
    /// NULL, so a sweep that did not prove its instruments cannot be written at all. The evidence-day
    /// control is the reason this is a constraint and not a convention — alert-pass counters frozen
    /// beside a delivering path is how a real gating defect was caught (#3464), and an empty ledger
    /// with a dead reader must be unrepresentable as a green sweep.
    /// </summary>
    [Fact]
    public void TheInstrumentLivenessPair_IsNotNullable_SoAQuietSweepMustProveItsInstruments()
    {
        Assert.Contains("instruments_alive boolean NOT NULL", FleetSweepStore.CreateTablesSql, StringComparison.Ordinal);
        Assert.Contains("instrument_liveness_json text NOT NULL", FleetSweepStore.CreateTablesSql, StringComparison.Ordinal);

        /* The mute header and the document are NOT NULL for the same reason: the spec requires the
           header to state the mute on every sweep, and a sweep without its document is not a sweep. */
        Assert.Contains("alerts_enabled boolean NOT NULL", FleetSweepStore.CreateTablesSql, StringComparison.Ordinal);
        Assert.Contains("report_json text NOT NULL", FleetSweepStore.CreateTablesSql, StringComparison.Ordinal);

        /* And a ledger entry without evidence is an accusation, not a record — the spec's acceptance
           is a would-have-paged condition "with its evidence". */
        Assert.Contains("evidence_json text NOT NULL", FleetSweepStore.CreateTablesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// None of the four tables is a collector table, so nothing catalog-driven reaches them:
    /// TimescaleSupport's hypertable conversion and DarlingRetention's catalog purge both enumerate
    /// the collector catalog — the store_metrics and oversized_plan_backlog precedents. The run table
    /// IS a time series, but at the default hourly cadence it is 24 rows a day, and the children key
    /// on sweep_id — a hypertable's unique constraint must include the partitioning column, the same
    /// shape rejection the backlog records. Derived from the shipped constants, not literals of them.
    /// </summary>
    [Fact]
    public void NoneOfTheFourTables_IsACollectorTable_SoNothingCatalogDrivenReachesThem()
    {
        foreach (var table in new[]
        {
            FleetSweepStore.RunsTableName,
            FleetSweepStore.VerdictsTableName,
            FleetSweepStore.WouldHavePagedTableName,
            FleetSweepStore.WatchItemsTableName,
        })
        {
            var bare = table.Split('.')[^1];
            Assert.DoesNotContain(bare, CollectorCatalog.All.Select(c => c.TargetTable));
            Assert.DoesNotContain(bare, TimescaleSupport.HypertableTables.Select(c => c.TargetTable));
        }
    }

    /* ---- the statements agree with the DDL ------------------------------------------------------------ */

    /// <summary>
    /// Every column every INSERT names exists in the shipped DDL — the DDL and the DML live in one
    /// file so they cannot drift, and this checks that they in fact agree, column by column, rather
    /// than resting on their proximity.
    /// </summary>
    [Fact]
    public void EveryColumnTheInsertsName_ExistsInTheShippedDdl()
    {
        foreach (var (sql, expectedCount) in new[]
        {
            (FleetSweepStore.InsertRunSql, 11),
            (FleetSweepStore.InsertVerdictSql, 7),
            (FleetSweepStore.InsertWouldHavePagedSql, 4),
            (FleetSweepStore.UpsertWatchItemSql, 13),
        })
        {
            var columns = InsertColumnList(sql);
            Assert.Equal(expectedCount, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains("    " + column + " ", FleetSweepStore.CreateTablesSql, StringComparison.Ordinal);
            }

            /* And the VALUES list binds exactly as many parameters as the column list names, with a
               dense $1..$N — a skipped ordinal is how a shifted bind passes a parse and re-maps every
               later column. */
            for (var i = 1; i <= expectedCount; i++)
            {
                Assert.Contains("$" + i.ToString(CultureInfo.InvariantCulture), sql, StringComparison.Ordinal);
            }

            Assert.DoesNotContain(
                "$" + (expectedCount + 1).ToString(CultureInfo.InvariantCulture), sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A sweep document is immutable: the run and its two child tables are INSERT-only, with no
    /// conflict arm to quietly turn a duplicate sweep id into an overwrite — a collision must be a
    /// 23505 the writer's transaction rolls back, never a rewrite of a published document.
    /// </summary>
    [Fact]
    public void TheRunAndItsChildren_AreInsertOnly()
    {
        foreach (var sql in new[]
        {
            FleetSweepStore.InsertRunSql,
            FleetSweepStore.InsertVerdictSql,
            FleetSweepStore.InsertWouldHavePagedSql,
        })
        {
            Assert.DoesNotContain("ON CONFLICT", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("UPDATE", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The watch upsert's conflict arm takes the engine's image wholesale EXCEPT the two deliberate
    /// carve-outs: the birth record is never touched, and standing evidence survives a miss.
    /// </summary>
    [Fact]
    public void TheWatchUpsert_NeverTouchesTheBirthRecord_AndKeepsEvidenceAcrossAMiss()
    {
        var upsert = FleetSweepStore.UpsertWatchItemSql;

        /* Addressed at the named constraint, so a second unique index arriving later cannot silently
           become the conflict target. */
        Assert.Contains("ON CONFLICT ON CONSTRAINT pk_fleet_sweep_watch_items", upsert, StringComparison.Ordinal);

        var onConflict = upsert.Split("DO UPDATE SET", StringSplitOptions.None)[1];

        /* first_seen_* is the item's birth record — the backlog's captured_at rule. An upsert that
           rewrote it would make every long-carried item look newborn on each sweep, and first-seen is
           half of what "carried (with trend)" renders. */
        Assert.DoesNotContain("first_seen_sweep_id", onConflict, StringComparison.Ordinal);
        Assert.DoesNotContain("first_seen_at", onConflict, StringComparison.Ordinal);

        /* A MISS sweep has no fresh evidence; overwriting the evidence that opened an item with a
           miss's nothing would leave a carried item citing no instrument at exactly the moment an
           operator asks why it is still open. */
        Assert.Contains("evidence_json = COALESCE(EXCLUDED.evidence_json, w.evidence_json)", onConflict, StringComparison.Ordinal);

        /* Everything else is the engine's, wholesale — it read the row and ran the state machine, so
           a COALESCE here would let a stale stamp outvote the transition it just computed. */
        foreach (var column in new[]
        {
            "state = EXCLUDED.state",
            "consecutive_hits = EXCLUDED.consecutive_hits",
            "consecutive_misses = EXCLUDED.consecutive_misses",
            "last_seen_sweep_id = EXCLUDED.last_seen_sweep_id",
            "opened_sweep_id = EXCLUDED.opened_sweep_id",
            "closed_sweep_id = EXCLUDED.closed_sweep_id",
            "last_seen_at = EXCLUDED.last_seen_at",
        })
        {
            Assert.Contains(column, onConflict, StringComparison.Ordinal);
        }
    }

    /* ---- the reads ------------------------------------------------------------------------------------ */

    /// <summary>
    /// The span read carries BOTH bounds as the caller's parameters — the #2506 lesson: a read with a
    /// start and no end cannot be anchored, and "now" belongs to the caller's clock. And no statement
    /// in the store says <c>now()</c> at all, the product-wide naive-UTC discipline.
    /// </summary>
    [Fact]
    public void TheSpanRead_CarriesBothBounds_AndNothingReadsTheStoresClock()
    {
        Assert.Contains("swept_at >= $1", FleetSweepStore.GetRunsBySpanSql, StringComparison.Ordinal);
        Assert.Contains("swept_at <= $2", FleetSweepStore.GetRunsBySpanSql, StringComparison.Ordinal);

        foreach (var sql in new[]
        {
            FleetSweepStore.InsertRunSql,
            FleetSweepStore.InsertVerdictSql,
            FleetSweepStore.InsertWouldHavePagedSql,
            FleetSweepStore.UpsertWatchItemSql,
            FleetSweepStore.GetLatestRunSql,
            FleetSweepStore.GetRunsBySpanSql,
            FleetSweepStore.GetVerdictsSql,
            FleetSweepStore.GetWouldHavePagedSql,
            FleetSweepStore.GetWatchItemsByStateSql,
            FleetSweepStore.GetActiveWatchItemsSql,
        })
        {
            Assert.DoesNotContain("now()", sql, StringComparison.OrdinalIgnoreCase);
        }

        /* Latest = the sweep INSTANT, id as tiebreak — the ORDER BY states the semantic rather than
           trusting that generator-issued ids stay time-ordered. */
        Assert.Contains("ORDER BY swept_at DESC, sweep_id DESC", FleetSweepStore.GetLatestRunSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", FleetSweepStore.GetLatestRunSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The engine's evaluation read excludes exactly the closed state, spliced from the state
    /// machine's own constant — a pending item's entry count and an open item's exit count both
    /// advance on sweeps that do NOT sight them, so "active" must mean everything the machine can
    /// still move.
    /// </summary>
    [Fact]
    public void TheActiveRead_ExcludesExactlyClosed_DerivedFromTheConstant()
    {
        Assert.Contains(
            "state <> '" + FleetSweepWatchStateMachine.Closed + "'",
            FleetSweepStore.GetActiveWatchItemsSql,
            StringComparison.Ordinal);

        /* And the by-state read is parameterized, never interpolated — state names are constants
           today and a knob is the kind of thing that changes that. */
        Assert.Contains("WHERE state = $1", FleetSweepStore.GetWatchItemsByStateSql, StringComparison.Ordinal);
    }

    /* ---- the probe (three sites) ---------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and its arm still answers.
    ///
    /// <para>The probe asks the question, the caller reads the answer, the map has the parameter — three
    /// sites, and a sentinel present at only some of them shifts every LATER ordinal onto the wrong column.
    /// The top-arm claims (last argument, textual newest-first ordering, return-literal) moved to
    /// <see cref="FleetSweepCadenceKnobRungTests"/> with the V124 handoff.</para>
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains("table_name = 'fleet_sweep_runs'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasFleetSweepState", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The ordinal has to be a position that exists, and one that is no longer the last: `arity - 1`
           asserted this rung is the NEWEST sentinel, which stopped being true the moment V124 appended
           its own. Strictly-less is the form every other non-top rung's test uses. */
        Assert.True(ProbeOrdinal < arity - 1);

        /* Every sentinel true = a fully-migrated store, which must map to exactly the ladder's top. Stated
           against StorageVersion rather than this rung's number, so it survives every later rung. Built by
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

        /* This rung's arm still sits ABOVE V122's — newest-first is the method's whole contract, and the
           relative ordering of the two standing arms is this rung's to keep even after the top-arm claims
           moved to the V124 file. */
        var v123 = viewer.IndexOf("if (hasFleetSweepState)", StringComparison.Ordinal);
        var v122 = viewer.IndexOf("if (hasPgAlertCountKnobs)", StringComparison.Ordinal);
        Assert.True(v123 >= 0, "the viewer has no V123 sentinel arm — a store that stopped here would map to 122");
        Assert.True(v122 >= 0, "the V122 arm is gone, so this pin is comparing against nothing");
        Assert.True(v123 < v122, "the V123 arm sits below V122's, so a store that stopped here maps one rung low");
    }

    /* ---- the state machine: hysteresis boundaries, exact ---------------------------------------------- */

    /// <summary>
    /// The two bars are the spec's own figures, and both are ≥ 2 — the whole hysteresis contract is
    /// that no single sample flips state, in either direction, so a bar at 1 would re-create exactly
    /// the single-sample gates the evidence day documented as noise.
    /// </summary>
    [Fact]
    public void TheBars_AreTheSpecsFigures_AndNeitherAdmitsASingleSample()
    {
        Assert.Equal(2, FleetSweepWatchStateMachine.EntryConsecutiveSweeps);
        Assert.Equal(2, FleetSweepWatchStateMachine.ExitConsecutiveSweeps);
    }

    [Fact]
    public void TheFourStates_AreDistinctSpellings()
    {
        var states = new[]
        {
            FleetSweepWatchStateMachine.Pending,
            FleetSweepWatchStateMachine.Open,
            FleetSweepWatchStateMachine.Carried,
            FleetSweepWatchStateMachine.Closed,
        };

        Assert.Equal(states.Length, states.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>One elevated read is a blip until a second sweep confirms it — the entry half of the
    /// single-samples-cannot-flip-state contract, at its exact boundary.</summary>
    [Fact]
    public void AFirstSighting_IsPending_AndTheSecondConsecutiveHit_OpensAtExactlyTheBar()
    {
        var first = FleetSweepWatchStateMachine.FirstSighting();
        Assert.Equal(FleetSweepWatchStateMachine.Pending, first.State);
        Assert.Equal(1, first.ConsecutiveHits);
        Assert.False(first.JustOpened);

        var second = FleetSweepWatchStateMachine.Advance(
            first.State, conditionHeld: true, first.ConsecutiveHits, first.ConsecutiveMisses);
        Assert.Equal(FleetSweepWatchStateMachine.Open, second.State);
        Assert.Equal(2, second.ConsecutiveHits);
        Assert.True(second.JustOpened);
        Assert.False(second.JustClosed);
    }

    /// <summary>
    /// An interrupted run never opens — a miss resets the entry count to zero, so alternating
    /// hit/miss reads hold an item pending FOREVER rather than accumulating toward the bar.
    /// Simulated across 48 sweeps rather than asserted on one, because "does not creep open" is a
    /// claim about the SEQUENCE, and the count of decisions is a literal so a loop that stopped
    /// running cannot pass vacuously.
    /// </summary>
    [Fact]
    public void AnAlternatingRun_NeverOpens_HoweverLongItAlternates()
    {
        var decision = FleetSweepWatchStateMachine.FirstSighting();
        var decisions = 0;

        for (var sweep = 0; sweep < 48; sweep++)
        {
            decision = FleetSweepWatchStateMachine.Advance(
                decision.State, conditionHeld: sweep % 2 != 0,
                decision.ConsecutiveHits, decision.ConsecutiveMisses);
            decisions++;

            Assert.Equal(FleetSweepWatchStateMachine.Pending, decision.State);
            Assert.False(decision.JustOpened);
        }

        Assert.Equal(48, decisions);
    }

    /// <summary>One quiet sweep carries; the second consecutive quiet sweep closes — the spec's
    /// "closed after two quiet hours" at the default hourly cadence, at its exact boundary.</summary>
    [Fact]
    public void OneQuietSweep_Carries_AndTheSecondConsecutiveQuietSweep_Closes()
    {
        var open = FleetSweepWatchStateMachine.Advance(
            FleetSweepWatchStateMachine.Pending, conditionHeld: true, consecutiveHits: 1, consecutiveMisses: 0);
        Assert.True(open.JustOpened);

        var firstQuiet = FleetSweepWatchStateMachine.Advance(
            open.State, conditionHeld: false, open.ConsecutiveHits, open.ConsecutiveMisses);
        Assert.Equal(FleetSweepWatchStateMachine.Carried, firstQuiet.State);
        Assert.Equal(1, firstQuiet.ConsecutiveMisses);
        Assert.False(firstQuiet.JustClosed);

        var secondQuiet = FleetSweepWatchStateMachine.Advance(
            firstQuiet.State, conditionHeld: false, firstQuiet.ConsecutiveHits, firstQuiet.ConsecutiveMisses);
        Assert.Equal(FleetSweepWatchStateMachine.Closed, secondQuiet.State);
        Assert.Equal(2, secondQuiet.ConsecutiveMisses);
        Assert.True(secondQuiet.JustClosed);
    }

    /// <summary>
    /// A recovery resets the exit count: miss-hit-miss never closes, however often it repeats — the
    /// exit half of the alternation pin, and the reading that keeps a flapping episode CARRIED (its
    /// standing counters are the trend) instead of oscillating open/closed on each flap.
    /// </summary>
    [Fact]
    public void AMissHitMissSequence_NeverCloses_BecauseARecoveryResetsTheExitCount()
    {
        var decision = FleetSweepWatchStateMachine.Advance(
            FleetSweepWatchStateMachine.Pending, conditionHeld: true, consecutiveHits: 1, consecutiveMisses: 0);
        Assert.Equal(FleetSweepWatchStateMachine.Open, decision.State);

        var decisions = 0;
        for (var sweep = 0; sweep < 48; sweep++)
        {
            decision = FleetSweepWatchStateMachine.Advance(
                decision.State, conditionHeld: sweep % 2 != 0,
                decision.ConsecutiveHits, decision.ConsecutiveMisses);
            decisions++;

            Assert.Equal(FleetSweepWatchStateMachine.Carried, decision.State);
            Assert.False(decision.JustClosed);
            Assert.InRange(decision.ConsecutiveMisses, 0, 1);
        }

        Assert.Equal(48, decisions);
    }

    /// <summary>An open item that keeps holding is CARRIED with its hit count as the trend, and a
    /// recovery clears the standing misses so lapse-and-recover cannot creep toward the exit bar.</summary>
    [Fact]
    public void AHitOnAnOpenItem_Carries_AndClearsStandingMisses()
    {
        var carried = FleetSweepWatchStateMachine.Advance(
            FleetSweepWatchStateMachine.Open, conditionHeld: true, consecutiveHits: 2, consecutiveMisses: 0);
        Assert.Equal(FleetSweepWatchStateMachine.Carried, carried.State);
        Assert.Equal(3, carried.ConsecutiveHits);
        Assert.False(carried.JustOpened);

        var recovered = FleetSweepWatchStateMachine.Advance(
            FleetSweepWatchStateMachine.Carried, conditionHeld: true, consecutiveHits: 0, consecutiveMisses: 1);
        Assert.Equal(FleetSweepWatchStateMachine.Carried, recovered.State);
        Assert.Equal(0, recovered.ConsecutiveMisses);
    }

    /// <summary>
    /// A hit after a confirmed close starts a NEW episode's count — pending at one hit, never
    /// straight back to open — because evidence separated by a confirmed close is evidence of a new
    /// episode, and resuming the old count would let two blips a week apart open an item no two
    /// consecutive sweeps ever confirmed. A miss on a closed item changes nothing.
    /// </summary>
    [Fact]
    public void AHitAfterClose_StartsAFreshEpisode_AndClosedStaysClosedOnMisses()
    {
        var reSighted = FleetSweepWatchStateMachine.Advance(
            FleetSweepWatchStateMachine.Closed, conditionHeld: true, consecutiveHits: 5, consecutiveMisses: 2);
        Assert.Equal(FleetSweepWatchStateMachine.Pending, reSighted.State);
        Assert.Equal(1, reSighted.ConsecutiveHits);
        Assert.False(reSighted.JustOpened);

        var stillClosed = FleetSweepWatchStateMachine.Advance(
            FleetSweepWatchStateMachine.Closed, conditionHeld: false, consecutiveHits: 0, consecutiveMisses: 7);
        Assert.Equal(FleetSweepWatchStateMachine.Closed, stillClosed.State);
        Assert.False(stillClosed.JustClosed);
    }

    /* ---- helpers -------------------------------------------------------------------------------------- */

    /// <summary>The column list of an INSERT statement — the names between the target's opening
    /// parenthesis and the VALUES keyword, whatever the line endings.</summary>
    private static string[] InsertColumnList(string sql)
    {
        var open = sql.IndexOf('(', sql.IndexOf("INSERT INTO", StringComparison.Ordinal));
        var values = sql.IndexOf("VALUES", StringComparison.Ordinal);
        var close = sql.LastIndexOf(')', values);

        return sql[(open + 1)..close]
            .Split(',')
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToArray();
    }

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
