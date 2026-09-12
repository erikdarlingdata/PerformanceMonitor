/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V118 / #3282: the built-in alert catalog's persistence-gate state (config.alert_persistence_state).
/// This carries the "I am the top rung" claims that moved off <see cref="MuteRuleReloadBeaconTests"/>
/// (V117) when this rung landed — a fully-migrated store must map to EXACTLY this version, or the viewer's
/// connect-time gate refuses a store that is actually current.
/// </summary>
public sealed class BuiltinAlertPersistenceRungTests
{
    private const int RungVersion = 118;
    private const int PreviousVersion = 117;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 93;

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

        /* TWO READS, TWO ROLES, and the roles are what is pinned rather than which names appear.
           The first spelling of this pin asserted GetLatestAsync was ABSENT, which encoded "the batch
           replaced it" — and that was the wrong property. The gate counts new samples, so it reads the
           batch. The standing-condition REMINDER asks whether the condition is still there, which is a
           different question and has to be answerable on a sweep that brought no new sample: the sweep is
           30 seconds and the collector is five minutes, so most sweeps bring none. Asserting a name's
           absence made a parity break look correct — the reminder's cadence silently capped at the
           collector interval instead of the configured cooldown (review catch). */
        Assert.Contains("GetSamplesSinceAsync", body, StringComparison.Ordinal);

        /* The gate's observation is the batch's sample, never the fallback reading. */
        var gate = body.IndexOf("AlertPersistenceGate.Evaluate", StringComparison.Ordinal);
        var fallback = body.IndexOf("GetLatestAsync", StringComparison.Ordinal);
        Assert.True(fallback > gate, "the latest-reading fallback must sit AFTER the gate loop, so it cannot advance the gate");

        /* And it is reached only when the batch brought nothing AND an incident is open — it exists for
           the reminder, not as a second way to observe. */
        Assert.Contains("if (!lastCapacityPercent.HasValue && record.State.Firing)", body, StringComparison.Ordinal);

        /* breaching is computed AFTER the fallback, or the fallback informs nothing. */
        var breaching = body.IndexOf("bool breaching = lastCapacityPercent.HasValue", StringComparison.Ordinal);
        Assert.True(breaching > fallback, "breaching must be computed after the fallback that populates it");

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
    /// AT MOST ONE EDGE PER PASS, which is the invariant that makes a whole defect class unreachable
    /// rather than guarded.
    ///
    /// <para>The batch read introduced it. Accumulating a batch's edges into flags and deciding at the end
    /// compresses a SEQUENCE into a summary, and that lost an edge twice: first a fire and a resolve for
    /// the same incident flattening into "both happened", then — the review catch — a pre-existing
    /// incident's resolve being overwritten by a later, unrelated fire in the same batch, so nobody heard
    /// that the incident they had open was over. Confirmed reachable at seven samples
    /// (CpuClearSamples + CpuBreachSamples + CpuClearSamples), well inside the batch limit and the
    /// freshness window, i.e. one restart or one collector gap.</para>
    ///
    /// <para>Stopping at the first edge makes both instances impossible, and makes this pass the same
    /// shape as AlertEngine's SQL Server twin: one observation, one edge, the fire and resolve arms
    /// mutually exclusive by construction. So what is pinned is the <c>break</c> and the absence of the
    /// accumulators — a reintroduced flag is the return of the category.</para>
    /// </summary>
    [Fact]
    public void ThePassStopsAtTheFirstEdge_SoNoEdgeSequenceIsEverFlattened()
    {
        var body = EvaluatePgCpuBody();

        /* The edge is taken and the loop stops. */
        Assert.Contains("if (evaluation.Outcome != PersistenceOutcome.None)", body, StringComparison.Ordinal);
        var edge = body.IndexOf("if (evaluation.Outcome != PersistenceOutcome.None)", StringComparison.Ordinal);
        var close = body.IndexOf("\n            }", edge, StringComparison.Ordinal);
        Assert.True(close > edge, "the edge arm's end was not found, so this pin would read nothing");
        Assert.Contains("break;", body[edge..close], StringComparison.Ordinal);

        /* And the accumulators are GONE. Either one coming back is the category returning, because both
           lost edges were produced by exactly this shape. */
        Assert.DoesNotContain("bool fired = false", body, StringComparison.Ordinal);
        Assert.DoesNotContain("bool resolved = false", body, StringComparison.Ordinal);
        Assert.DoesNotContain("fired && resolved", body, StringComparison.Ordinal);

        /* The two arms read the ONE edge, so they cannot both run. */
        Assert.Contains("else if (outcome == PersistenceOutcome.Resolve)", body, StringComparison.Ordinal);

        /* One more route to the same loss, closed by the same break: an edge taken but not persisted would
           be replayed or skipped depending on which side of the save it fell. The save is after the loop
           and before both arms, so the state at the edge is committed whatever the arms do. */
        var save = body.IndexOf("stateStore.SaveAlertPersistenceAsync", StringComparison.Ordinal);
        var fire = body.IndexOf("if (record.State.Firing && breaching)", StringComparison.Ordinal);
        Assert.True(save > edge, "the state save must come after the loop that takes the edge");
        Assert.True(fire > save, "the state save must come before the delivery arms");
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
    /// Every comment that cites this rung's NUMBER cites the right one.
    ///
    /// <para>A rung number in prose is a frozen claim, and this branch froze the wrong one: #3315 landed
    /// V117 first, this rung renumbered to V118, and five comments across both SKUs kept saying V117 —
    /// three of which review found and two of which it did not. The repo leans on exactly these citations
    /// to check Lite/Darling parity, so a wrong one sends the next person to the wrong rung.</para>
    ///
    /// <para><b>The number is derived from <see cref="StorageVersion.SchemaVersion"/></b> rather than
    /// compared against a literal, so a future renumber reds this instead of leaving silent copies.</para>
    ///
    /// <para><b>And each phrase carries its own SUBJECT</b>, which is what makes a citation attributable
    /// without parsing comments at all. Two earlier spellings of this pin are the reason it is shaped this
    /// way. Scoped to the file, it flagged nine correct citations — those files cite V32, V40, V44, V50,
    /// V60, V61, V80 and V81 for their own tables, all right. Scoped to comment BLOCKS instead, it needed a
    /// hand-rolled line-prefix comment filter, which <c>CommentFilterAdoptionTests</c> correctly refuses
    /// without a stated bound — and the bound I measured for it came back unreliable on its own terms. A
    /// phrase that names both the table and the rung needs neither: it cannot match another rung's citation
    /// because it does not describe another rung's subject.</para>
    /// </summary>
    [Fact]
    public void EveryCommentCitingThisRungsNumber_CitesTheRealOne()
    {
        var rung = StorageVersion.SchemaVersion;

        /* Each entry names a file and the citation in it, with {0} where the rung goes. The subject words
           are part of the phrase deliberately — see the remarks. A reword reds this, which is correct: the
           prose and the pin are one claim, so the pin has to be edited with it. */
        var citations = new (string Path, string Phrase)[]
        {
            (Path.Combine("Lite", "Database", "Schema.cs"),
                "config.alert_persistence_state (PgMigrations V{0})"),
            (Path.Combine("Lite", "Database", "DuckDbInitializer.cs"),
                "persistence-gate state, porting Darling's V{0}"),
            (Path.Combine("Lite", "Services", "DuckDbAlertHistoryStore.cs"),
                "the Lite twin of Darling's V{0} table"),
            (Path.Combine("Lite", "Services", "LiteAlertStateStore.cs"),
                "Darling's V{0} table, so the shared engine's CPU gate"),
            (Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "PgAlertStateStore.cs"),
                "persistence-gate record from the V{0}"),
        };

        foreach (var (relative, phrase) in citations)
        {
            var source = RepoFile.ReadRepoFileLf(relative);

            /* EXACTLY once. Absent means the prose was reworded and this pin went stale with it; twice
               means a copy was made that the next renumber would miss. */
            var expected = string.Format(CultureInfo.InvariantCulture, phrase, rung);
            Assert.Equal(1, CountOf(source, expected));

            /* And the same phrase carrying ANY other rung number must not appear — the renumber case. The
               regex is built from the phrase itself, so it cannot drift away from the string above. */
            var pattern = Regex.Escape(phrase).Replace(@"V\{0}", @"V(\d+)", StringComparison.Ordinal);
            var found = Regex.Matches(source, pattern, RegexOptions.CultureInvariant)
                .Select(m => m.Groups[1].Value)
                .ToArray();

            /* The regex has to MATCH, or the assertion below is over an empty set and passes for the wrong
               reason — the failure mode of building a pattern out of an escaped literal. */
            Assert.NotEmpty(found);
            Assert.All(found, n => Assert.Equal(rung.ToString(CultureInfo.InvariantCulture), n));
        }
    }

    /// <summary>Non-overlapping occurrences of <paramref name="needle"/> — <c>IndexOf</c> in a loop, because
    /// there is no overload that counts and a <c>Split</c> would allocate the whole file per call.</summary>
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
