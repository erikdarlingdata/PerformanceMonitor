/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V117 / #3282: the built-in alert catalog's persistence-gate state (config.alert_persistence_state).
/// This carries the "I am the top rung" claims that moved off <see cref="CustomAlertCoreMigrationTests"/>
/// (V116) when this rung landed — a fully-migrated store must map to EXACTLY this version, or the viewer's
/// connect-time gate refuses a store that is actually current.
/// </summary>
public sealed class BuiltinAlertPersistenceRungTests
{
    private const int RungVersion = 117;
    private const int PreviousVersion = 116;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 92;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "builtin-alert-persistence",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    [Fact]
    public void TheRungCreatesTheStateTable_SchemaQualified_WithNoBumpTrigger()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema (and the wrong ACL). */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.alert_persistence_state", rung, StringComparison.Ordinal);

        /* The subject is (server_id, metric_name) — the key the engine's other state already uses, and
           the reason this could not be a row in custom_alert_state (keyed on a rule that does not exist
           for a built-in alert). */
        Assert.Contains("PRIMARY KEY (server_id, metric_name)", rung, StringComparison.Ordinal);

        /* The gate's three state fields plus its observation identity. last_observed_sample_at is the one
           that makes the counters mean SAMPLES rather than sweeps, so it is asserted explicitly: without
           it "three consecutive breaches" is satisfied inside ninety seconds by one re-read row. */
        foreach (var column in new[]
                 {
                     "consecutive_breaches", "consecutive_clears", "firing", "last_observed_sample_at",
                 })
        {
            Assert.Contains(column, rung, StringComparison.Ordinal);
        }

        /* Naive UTC, the store convention — a timestamptz here would be zone-shifted against every other
           timestamp it is compared with. */
        Assert.Contains("timestamp", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("timestamptz", rung, StringComparison.Ordinal);

        /* No reload beacon: this is evaluator state written on every new sample, and a bump would force a
           fleet-wide ReloadFromStoreAsync about once a minute per server. */
        Assert.DoesNotContain("config_bump_version", rung, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            "table_name = 'alert_persistence_state'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasBuiltinAlertPersistence", viewer, StringComparison.Ordinal);

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

        /* One rung behind: every sentinel EXCEPT this one reports 116 (the previous top rung). */
        var behind = Enumerable.Repeat((object)true, arity).ToArray();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }

    [Fact]
    public void TheStoreReadAndWriteNameTheRungsColumns()
    {
        /* The rung is inert unless the store that reads and writes it names the same columns. Asserted
           against the shipped source rather than a retyped copy, for the reason the PostgreSQL work
           learned the hard way: a proven query and a working feature are different claims, and every
           blocking defect in that slice was a call site rather than the SQL.

           Not a live-store test, deliberately: Darling PostgreSQL tests cover the migration applying, and
           what can go wrong HERE is a column renamed on one side of the seam, which is a text fact. */
        var store = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "PgAlertStateStore.cs");

        Assert.Contains("config.alert_persistence_state", store, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, metric_name) DO UPDATE SET", store, StringComparison.Ordinal);

        foreach (var column in new[]
                 {
                     "consecutive_breaches", "consecutive_clears", "firing", "last_observed_sample_at",
                 })
        {
            Assert.Contains(column, store, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The PostgreSQL High CPU evaluator is wired to the SAME gate, with the SAME constants, and reads a
    /// BATCH of samples rather than the latest one.
    ///
    /// <para>The batch is the load-bearing half and it is engine-specific. On SQL Server the 30-second
    /// sweep is faster than the ~60-second sample, so "latest row, counted once when its instant moves"
    /// loses nothing. Performance Insights is sampled at 60 seconds and <c>pg_cpu_utilization</c> is a
    /// five-minute collector, so five points land together: counting only the newest would discard four in
    /// five and make three consecutive breaches take three BATCHES, about fifteen minutes. That is a
    /// saturation event reported long after it mattered, and it is exactly the failure the issue's own
    /// brief warned a too-high N would cause — reached here by an invisible route rather than by choosing
    /// a number.</para>
    ///
    /// <para>Source assertions rather than a driven test because this evaluator lives inside
    /// <c>DarlingWorker</c> behind an AWS client and a live store; the gate's arithmetic is covered by
    /// <c>AlertPersistenceGateTests</c> and its SQL Server wiring by <c>AlertEngineTests</c>, so what is
    /// left to go wrong here is a call site, which is a text fact. Sliced to this method so an assertion
    /// cannot be satisfied by the SQL Server CPU alert's neighbour text.</para>
    /// </summary>
    [Fact]
    public void ThePostgresCpuAlertUsesTheSharedGate_OverABatchOfSamples()
    {
        var body = EvaluatePgCpuBody();

        Assert.Contains("AlertPersistenceGate.Evaluate", body, StringComparison.Ordinal);
        Assert.Contains("AlertEngine.CpuBreachSamples", body, StringComparison.Ordinal);
        Assert.Contains("AlertEngine.CpuClearSamples", body, StringComparison.Ordinal);

        /* No second set of numbers: the whole point of #3282 is one mechanism, so a local constant or a
           bare literal here would be the drift the issue predicted. */
        Assert.DoesNotContain("breachSamples: 3", body, StringComparison.Ordinal);
        Assert.DoesNotContain("clearSamples: 2", body, StringComparison.Ordinal);

        /* The batch read, and NOT the single-latest read it replaced. */
        Assert.Contains("GetSamplesSinceAsync", body, StringComparison.Ordinal);
        Assert.DoesNotContain("GetLatestAsync", body, StringComparison.Ordinal);

        /* It persists through the same seam under the same subject key, so a restart does not re-announce
           an open incident and the two engines' rows are one shape. */
        Assert.Contains("stateStore.LoadAlertPersistenceAsync", body, StringComparison.Ordinal);
        Assert.Contains("stateStore.SaveAlertPersistenceAsync", body, StringComparison.Ordinal);
        Assert.Contains("AlertEngine.CpuPersistenceMetric", body, StringComparison.Ordinal);

        /* The active-flag it replaced is gone from the whole file, not just from this method — a leftover
           would be a second source of truth for "is an incident open". */
        var worker = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.DoesNotContain("_activePgCpuAlert", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// A restart must not re-announce an already-open incident on EITHER engine. The persisted Firing bit
    /// stops a second rising edge, but the cooldown dictionaries are in-memory on both sides, so the
    /// seeding has to stamp the clock too — and doing that on only one of the two engines is the
    /// shared-seam half-fix this whole issue is about, in miniature.
    /// </summary>
    [Fact]
    public void ARestartStampsTheCooldownOnBothEngines()
    {
        var pg = EvaluatePgCpuBody();
        Assert.Contains("if (seeded.Value.State.Firing)", pg, StringComparison.Ordinal);
        Assert.Contains("_lastPgCpuAlert[key] = now;", pg, StringComparison.Ordinal);

        var engine = RepoFile.ReadRepoFileLf("PerformanceMonitor.Alerting", "AlertEngine.cs");
        Assert.Contains("if (cpuPersistence.Value.State.Firing)", engine, StringComparison.Ordinal);
        Assert.Contains("_lastCpuAlert[key] = _utcNow();", engine, StringComparison.Ordinal);

        /* And neither engine bypasses the cooldown for the rising edge, so one threshold means one thing
           across them — the parity #2719 chose the shared metric names for. */
        Assert.DoesNotContain("cooldownElapsed = fired", pg, StringComparison.Ordinal);
    }

    /// <summary>
    /// An incident that OPENS AND CLOSES inside one pass delivers nothing. Reachable only on the
    /// PostgreSQL side and only because of how the metric arrives: Performance Insights is sampled every
    /// 60 seconds while <c>pg_cpu_utilization</c> is a five-minute collector, so one batch can hold a whole
    /// three-minute excursion that had already ended before the data was handed over.
    ///
    /// <para>This is the defect the batch read introduced and it had to be closed rather than accepted:
    /// without it the falling edge is delivered on its own, which is a recovery notice for a message
    /// nobody received. Delivering both instead would be the unactionable pair the issue was filed about,
    /// for an excursion nobody could have looked at. The same call <c>CustomAlertEvaluator</c> makes on the
    /// same gate — it resolves only an incident that was actually delivered.</para>
    /// </summary>
    [Fact]
    public void AnIncidentThatOpensAndClosesInOnePass_DeliversNothing()
    {
        var body = EvaluatePgCpuBody();

        Assert.Contains("if (fired && resolved)", body, StringComparison.Ordinal);

        /* The guard has to sit BEFORE both delivery arms, or it guards nothing. */
        var guard = body.IndexOf("if (fired && resolved)", StringComparison.Ordinal);
        var fire = body.IndexOf("if (record.State.Firing && breaching)", StringComparison.Ordinal);
        var resolve = body.IndexOf("else if (resolved)", StringComparison.Ordinal);
        Assert.True(guard >= 0 && fire > guard, "the same-pass guard must precede the fire arm");
        Assert.True(resolve > guard, "the same-pass guard must precede the resolve arm");

        /* And it must not skip the state write, or the streak it just completed would be replayed. */
        var save = body.IndexOf("stateStore.SaveAlertPersistenceAsync", StringComparison.Ordinal);
        Assert.True(save >= 0 && save < guard, "the state save must happen before the same-pass early return");
    }

    /// <summary>
    /// A missing capacity reading must FREEZE the gate rather than clear it. #3281 established that the
    /// alert never FIRES on an absent capacity sample (a fallback to percent-of-allocated silently arms the
    /// threshold against the wrong denominator); the other half is that it must not RESOLVE on one either,
    /// because that announces a recovery nobody measured. The pre-#3282 code treated absent capacity as
    /// "not exceeded", which is the resolve arm.
    /// </summary>
    [Fact]
    public void AnAbsentCapacityReadingIsNotAClear()
    {
        var body = EvaluatePgCpuBody();

        Assert.Contains("if (!capacityPercent.HasValue)", body, StringComparison.Ordinal);

        /* `continue` is the freeze: the sample is neither counted nor marked observed, so the streak holds
           and a later reading picks up where the last real one left off. */
        var absent = body.IndexOf("if (!capacityPercent.HasValue)", StringComparison.Ordinal);
        var next = body.IndexOf("AlertPersistenceGate.Evaluate", absent, StringComparison.Ordinal);
        Assert.True(next > absent, "the no-capacity arm must sit before the gate call it skips");
        Assert.Contains("continue;", body[absent..next], StringComparison.Ordinal);
    }

    /// <summary>The <c>EvaluatePgCpuAsync</c> body, sliced from its own declaration to the next member at
    /// the same indent — sliced rather than searched whole-file because the SQL Server CPU alert's text
    /// lives in sibling files and a pin that reads a neighbour is not a pin. The integrity assertions are
    /// the same shape <c>PgCpuCapacityHeadroomTests</c> uses on the same method.</summary>
    private static string EvaluatePgCpuBody()
    {
        var source = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        const string start = "private async Task EvaluatePgCpuAsync(";
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, "EvaluatePgCpuAsync was not found, so this pin would read nothing");

        var to = source.IndexOf("\n    private ", from + start.Length, StringComparison.Ordinal);
        Assert.True(to > from, "the method's end was not found, so this pin would read the rest of the file");

        var body = source[from..to];

        /* The slice has to be a method, not a fragment: an off-by-one on either bound silently shrinks it
           and every Assert.DoesNotContain above starts passing for the wrong reason. */
        Assert.Contains("await DarlingPgCpuUtilizationReader.GetSamplesSinceAsync", body, StringComparison.Ordinal);
        Assert.True(body.Length > 1500, $"the sliced body is only {body.Length} chars, which cannot be this method");

        return body;
    }

    /// <summary>
    /// The README's alert catalog states the sample count, so the number is pinned to the constant rather
    /// than left as a prose copy of it. A doc that says "3 samples" while the code says something else is
    /// a stale count with a numeral welded on, and nothing else in the build would notice — the whole
    /// point of the catalog row is that it is what a user reads before tuning the threshold.
    /// </summary>
    [Fact]
    public void TheReadmeAlertCatalogStatesTheSampleCountItActuallyUses()
    {
        var readme = RepoFile.ReadRepoFileLf("README.md");

        var row = readme
            .Split('\n')
            .Single(l => l.StartsWith("| **High CPU**", StringComparison.Ordinal));

        Assert.Contains($"held for {AlertEngine.CpuBreachSamples} samples", row, StringComparison.Ordinal);
        Assert.Contains($"{AlertEngine.CpuBreachSamples} consecutive collected samples", row, StringComparison.Ordinal);
        Assert.Contains($"{AlertEngine.CpuClearSamples} consecutive samples below it", row, StringComparison.Ordinal);

        /* NUMERALS, not spelled-out words, and this is the reason rather than a style preference: the row
           first read "three consecutive collected samples", which this pin could not match against the
           constant. A doc count that cannot be compared to the thing it describes is a count nobody can
           check. */

        /* And it must not still claim the pre-#3282 rule, which is the sentence a reader would act on. */
        Assert.DoesNotContain("Fires when total CPU (SQL + other) exceeds the threshold |", row, StringComparison.Ordinal);
    }

    /// <summary>
    /// The batch read is bounded and floors its lower bound at the freshness window, so a subject with no
    /// memory (or a collector that was away) considers only readings recent enough to describe "right now"
    /// — the same bound the single-reading path applies, so the gate and the card cannot disagree about
    /// what counts as current.
    /// </summary>
    [Fact]
    public void TheBatchReadIsBoundedAndSharesTheFreshnessWindow()
    {
        Assert.Contains("sample_time > $2", DarlingPgCpuUtilizationReader.SamplesSinceSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", DarlingPgCpuUtilizationReader.SamplesSinceSql, StringComparison.Ordinal);

        /* OLDEST FIRST. Descending order would feed the gate backwards, which produces a plausible count
           from an impossible sequence: a recovery followed by a breach would read as a breach followed by
           a recovery, and the streak would be built out of samples in the wrong direction. */
        Assert.Contains("ORDER BY sample_time\n", DarlingPgCpuUtilizationReader.SamplesSinceSql.Replace("\r\n", "\n"), StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY sample_time DESC", DarlingPgCpuUtilizationReader.SamplesSinceSql, StringComparison.Ordinal);

        /* A null capacity is stored as NULL rather than 0, and the gate must never see a fabricated
           reading — so the read filters on cpu_percent only, like its sibling, and leaves the capacity
           nullable for the evaluator's own named no-capacity state. */
        Assert.Contains("cpu_percent IS NOT NULL", DarlingPgCpuUtilizationReader.SamplesSinceSql, StringComparison.Ordinal);
        Assert.DoesNotContain("acu_utilization_percent IS NOT NULL", DarlingPgCpuUtilizationReader.SamplesSinceSql, StringComparison.Ordinal);

        /* Bounded above by a constant that cannot change the outcome — the gate's counters saturate at
           their thresholds — so this only stops one pass reading an unbounded list. */
        Assert.True(DarlingPgCpuUtilizationReader.GateBatchLimit >= AlertEngine.CpuBreachSamples + AlertEngine.CpuClearSamples);
        Assert.True(DarlingPgCpuUtilizationReader.GateBatchLimit <= 128);
    }
}
