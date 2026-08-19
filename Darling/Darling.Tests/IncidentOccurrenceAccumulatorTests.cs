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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Coverage for <see cref="IncidentOccurrenceAccumulator"/> and the V61 store surface it needs (#2216).
///
/// <para>The property under test is the one the feature exists for: the number an alert reports must be
/// recoverable by a consumer that only sees THROTTLED deliveries. That makes the interesting cases the ones
/// where the underlying gauge is uninformative — it held steady while events arrived and aged out, it fell
/// and rose back to the same level, or the process died and came back — because those are exactly where a
/// naive "remember the last count" implementation reports a number that is wrong in the confident
/// direction.</para>
/// </summary>
public sealed class IncidentOccurrenceAccumulatorTests
{
    private static readonly DateTime T0 = new(2026, 8, 12, 14, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Window = TimeSpan.FromHours(1);

    private const string KeyA = "aaaa1111";
    private const string KeyB = "bbbb2222";

    private static AlertIncident Incident(string dedupKey, int windowCount) =>
        new(dedupKey, new[] { "dbo.Users" }, windowCount);

    private static IncidentOccurrenceAccumulator.Result Accumulate(
        IReadOnlyList<AlertIncident> incidents,
        IReadOnlyDictionary<string, IncidentOccurrenceState>? persisted,
        DateTime now) =>
        IncidentOccurrenceAccumulator.Accumulate(incidents, persisted, now, Window);

    [Fact]
    public void FirstContact_CountsTheWholeWindow_AndStampsTheIncidentStart()
    {
        /* With no persisted state the accumulator cannot know which of the events already in the window it
           would have counted before, so the total starts at the window count — the same first-read behavior
           the gauge has always had. The stamp is what lets a consumer see that this is a NEW incident. */
        var result = Accumulate(new[] { Incident(KeyA, 3) }, persisted: null, T0);

        Assert.Equal(3L, result.Incidents[0].TotalOccurrences);
        Assert.Equal(T0, result.Incidents[0].IncidentStartedUtc);
        Assert.True(result.Changed);

        var state = result.States[KeyA];
        Assert.Equal(3L, state.TotalOccurrences);
        Assert.Equal(3, state.ObservedWindowCount);
        Assert.Equal(T0, state.IncidentStartedUtc);
        Assert.Equal(T0, state.LastObservedUtc);
    }

    [Fact]
    public void OccurrencesArrivingDuringTheCooldown_AreCountedOnTheNextDelivery()
    {
        /* THE REPORTED CASE. Two deadlocks are delivered, three more happen while the per-fingerprint
           cooldown suppresses delivery, and the next delivery's gauge reads 5. The total must be 5 — not 2
           (only what was delivered) and not 7 (the two deliveries added together). */
        var first = Accumulate(new[] { Incident(KeyA, 2) }, persisted: null, T0);
        var second = Accumulate(new[] { Incident(KeyA, 5) }, first.States, T0.AddMinutes(5));

        Assert.Equal(2L, first.Incidents[0].TotalOccurrences);
        Assert.Equal(5L, second.Incidents[0].TotalOccurrences);

        /* Same incident throughout — the start time did not move, which is how the consumer knows the 5 is
           a continuation of the 2 rather than a fresh incident that happens to read 5. */
        Assert.Equal(T0, second.Incidents[0].IncidentStartedUtc);
    }

    [Fact]
    public void AgingOut_ThenRisingBackToTheSameLevel_CountsTheNewEventsOnce()
    {
        /* The case a single watermark cannot express. The gauge goes 5 -> 3 (two aged out) -> 5 (two new
           arrived). A mark that did not decay would see 5 then 5 and count nothing; a mark that decayed but
           re-counted from zero would count 5 again. The answer is 7: five, then two more. */
        var a = Accumulate(new[] { Incident(KeyA, 5) }, persisted: null, T0);
        var b = Accumulate(new[] { Incident(KeyA, 3) }, a.States, T0.AddMinutes(10));
        var c = Accumulate(new[] { Incident(KeyA, 5) }, b.States, T0.AddMinutes(20));

        Assert.Equal(5L, a.Incidents[0].TotalOccurrences);
        Assert.Equal(5L, b.Incidents[0].TotalOccurrences);   /* aging out is not an occurrence */
        Assert.Equal(7L, c.Incidents[0].TotalOccurrences);
        Assert.Equal(5, c.States[KeyA].ObservedWindowCount);
    }

    [Fact]
    public void TheTotal_NeverDecreases_AcrossAFallingGauge()
    {
        /* Monotonicity is the whole contract: a consumer that SETs a gauge from this field must never see it
           walk backwards while one incident is in flight. */
        var states = (IReadOnlyDictionary<string, IncidentOccurrenceState>?)null;
        long previous = 0;
        var now = T0;

        foreach (var windowCount in new[] { 4, 6, 2, 1, 5, 3, 9 })
        {
            var result = Accumulate(new[] { Incident(KeyA, windowCount) }, states, now);
            var total = result.Incidents[0].TotalOccurrences!.Value;

            Assert.True(total >= previous,
                $"total went backwards: {previous} -> {total} at window count {windowCount}");
            previous = total;
            states = result.States;
            now = now.AddMinutes(5);
        }

        /* 4, then +2 (6), then nothing while it falls to 2 and 1, then +4 (5), nothing at 3, then +6 (9). */
        Assert.Equal(16L, previous);
    }

    [Fact]
    public void ObservingOnlyAtDeliveryTime_Undercounts_WhichIsWhyTheEngineObservesEverySweep()
    {
        /* PR #2221's review found this: with observation only at delivery time, an event the window RETIRES
           during a cooldown cancels an arrival in the gauge and the arrival becomes invisible.

           Delivery A sees 10. Before delivery B, three age out (window 7) and four arrive (window 11). Seen
           only at the two deliveries, the rise from 10 to 11 counts ONE — the correct answer is four. Seen on
           the sweeps in between, both movements are observed and the total is exact.

           This is the pin on the reason the engine calls Accumulate on every sweep rather than inside the
           Fire branch. The pure function was always capable of the right answer; the first cut of the wiring
           did not call it often enough to get it. */
        var deliveryA = Accumulate(new[] { Incident(KeyA, 10) }, persisted: null, T0);

        var deliveryOnly = Accumulate(new[] { Incident(KeyA, 11) }, deliveryA.States, T0.AddMinutes(5));
        Assert.Equal(11L, deliveryOnly.Incidents[0].TotalOccurrences);

        var sweep1 = Accumulate(new[] { Incident(KeyA, 7) }, deliveryA.States, T0.AddMinutes(2));
        var sweep2 = Accumulate(new[] { Incident(KeyA, 11) }, sweep1.States, T0.AddMinutes(4));
        Assert.Equal(14L, sweep2.Incidents[0].TotalOccurrences);

        /* One incident throughout, either way. */
        Assert.Equal(T0, sweep2.Incidents[0].IncidentStartedUtc);
    }

    [Fact]
    public void ObservingEverySweep_DoesNotMeanWritingEverySweep()
    {
        /* Per-sweep observation would otherwise put a store round trip on every metric of every server on
           every sweep. A flat gauge is not a write; a moved total is; and the heartbeat fires at half the
           horizon so a live incident whose gauge never moves cannot sit long enough to judge ITSELF stale. */
        var seeded = Accumulate(new[] { Incident(KeyA, 4) }, persisted: null, T0);
        Assert.True(seeded.Changed);

        Assert.False(Accumulate(new[] { Incident(KeyA, 4) }, seeded.States, T0.AddMinutes(1)).Changed);
        Assert.True(Accumulate(new[] { Incident(KeyA, 6) }, seeded.States, T0.AddMinutes(1)).Changed);

        /* Half of the one-hour horizon. */
        Assert.True(Accumulate(new[] { Incident(KeyA, 4) }, seeded.States, T0.AddMinutes(31)).Changed);

        /* A fingerprint leaving the window is a write even when the survivor held steady — the replace-the-set
           contract has to record the removal. */
        var two = Accumulate(new[] { Incident(KeyA, 4), Incident(KeyB, 2) }, persisted: null, T0);
        Assert.True(Accumulate(new[] { Incident(KeyA, 4) }, two.States, T0.AddMinutes(1)).Changed);
    }

    [Fact]
    public void EveryFingerprintKeepsState_NotJustTheOnesTheAlertRenders()
    {
        /* The other half of PR #2221's review: the blocking context renders at most 10 incidents, and when
           the accumulation rode inside that capped list, an 11th concurrent fingerprint had its row dropped
           by the replace-the-set write and restarted from scratch the next time it surfaced. The engine now
           accumulates over the UNCAPPED grouping, so the render budget cannot evict occurrence state. */
        var many = new List<AlertIncident>();
        for (int n = 0; n < 14; n++)
        {
            many.Add(Incident($"fp{n:00}", 1));
        }

        var first = Accumulate(many, persisted: null, T0);
        Assert.Equal(14, first.States.Count);

        var second = Accumulate(many, first.States, T0.AddMinutes(1));
        Assert.Equal(T0, second.Incidents[13].IncidentStartedUtc);
        Assert.Equal(1L, second.Incidents[13].TotalOccurrences);
    }

    [Fact]
    public void DistinctFingerprints_KeepSeparateTotals()
    {
        /* Why the key is the fingerprint and not (server, metric): a deadlock on one object set and a
           deadlock on another are different incidents, and pooling them would report each one's total as
           the sum of both. */
        var first = Accumulate(new[] { Incident(KeyA, 2), Incident(KeyB, 1) }, persisted: null, T0);
        var second = Accumulate(new[] { Incident(KeyA, 2), Incident(KeyB, 4) }, first.States, T0.AddMinutes(5));

        Assert.Equal(2L, second.Incidents[0].TotalOccurrences);
        Assert.Equal(4L, second.Incidents[1].TotalOccurrences);
    }

    [Fact]
    public void AFingerprintRepeatedWithinOneObservation_IsNotCountedTwice()
    {
        /* The groupers collapse by fingerprint, so this should not arrive — but if it does, accumulating the
           second appearance against the state the first one just wrote (rather than against the stale
           persisted mark) is what keeps the total from doubling. */
        var result = Accumulate(new[] { Incident(KeyA, 3), Incident(KeyA, 3) }, persisted: null, T0);

        Assert.Equal(3L, result.States[KeyA].TotalOccurrences);
        Assert.All(result.Incidents, i => Assert.Equal(3L, i.TotalOccurrences));
    }

    [Fact]
    public void AnEmptyObservation_ClearsTheSet_ButOnlyWritesWhenThereWasState()
    {
        /* The falling edge. An empty result with Changed=true is the delete; with no persisted state there is
           nothing to write, and saying so lets the caller skip the store entirely. */
        var seeded = Accumulate(new[] { Incident(KeyA, 2) }, persisted: null, T0);

        var cleared = Accumulate(Array.Empty<AlertIncident>(), seeded.States, T0.AddMinutes(5));
        Assert.Empty(cleared.States);
        Assert.True(cleared.Changed);

        var quiet = IncidentOccurrenceAccumulator.Accumulate(null, null, T0, Window);
        Assert.Empty(quiet.States);
        Assert.False(quiet.Changed);
    }

    [Fact]
    public void AStaleRow_IsTreatedAsAbsent_SoARecurrenceIsANewIncident()
    {
        /* The crash case, and the reason LastObservedUtc is stored at all. A row stranded by a host that
           died mid-incident must not be trusted weeks later: its high observed-mark would decay to the new
           window count, the recurrence would read as nothing new, and the alert would report a stale total
           under a stale start time — an undercount delivered with a confident timestamp. */
        var stranded = new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal)
        {
            [KeyA] = new(TotalOccurrences: 40, ObservedWindowCount: 40, IncidentStartedUtc: T0, LastObservedUtc: T0),
        };

        var recurrence = Accumulate(new[] { Incident(KeyA, 2) }, stranded, T0.AddHours(3));

        Assert.Equal(2L, recurrence.Incidents[0].TotalOccurrences);
        Assert.Equal(T0.AddHours(3), recurrence.Incidents[0].IncidentStartedUtc);
    }

    [Fact]
    public void ARowInsideTheWindow_IsStillTrusted()
    {
        /* The boundary the horizon is chosen for: inside the read window a persisted row is describing the
           same events the gauge is still counting, so it must be trusted or every delivery inside an
           incident would restart the total. */
        var recent = new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal)
        {
            [KeyA] = new(TotalOccurrences: 6, ObservedWindowCount: 6, IncidentStartedUtc: T0, LastObservedUtc: T0.AddMinutes(30)),
        };

        var next = Accumulate(new[] { Incident(KeyA, 8) }, recent, T0.AddMinutes(70));

        Assert.Equal(8L, next.Incidents[0].TotalOccurrences);
        Assert.Equal(T0, next.Incidents[0].IncidentStartedUtc);
    }

    [Fact]
    public void ARowStampedInTheFuture_IsTrustedRatherThanJudgedStale()
    {
        /* A clock that stepped backwards (NTP correction, host migration) must not reset a live incident's
           total and start time. Only genuine age discards a row. */
        var future = new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal)
        {
            [KeyA] = new(TotalOccurrences: 5, ObservedWindowCount: 5, IncidentStartedUtc: T0, LastObservedUtc: T0.AddHours(2)),
        };

        var next = Accumulate(new[] { Incident(KeyA, 7) }, future, T0);

        Assert.Equal(7L, next.Incidents[0].TotalOccurrences);
        Assert.Equal(T0, next.Incidents[0].IncidentStartedUtc);
    }

    [Fact]
    public void AnIncidentWithNoFingerprint_PassesThroughWithNoTotal()
    {
        /* A blank dedup key cannot be keyed, and inventing one would pool unrelated incidents under a single
           total. It travels unchanged, with TotalOccurrences null — "no total available" rather than 0. */
        var blank = new AlertIncident(string.Empty, new[] { "dbo.Users" }, 4);

        var result = Accumulate(new[] { blank }, persisted: null, T0);

        Assert.Null(result.Incidents[0].TotalOccurrences);
        Assert.Null(result.Incidents[0].IncidentStartedUtc);
        Assert.Empty(result.States);
        Assert.False(result.Changed);
    }

    [Fact]
    public void ADisabledHorizon_TrustsEveryRow()
    {
        /* TimeSpan.Zero turns the staleness rule off — for tests that want the unguarded behavior, not for
           hosts. Pinned so the escape hatch cannot rot into "any non-positive value means one hour". */
        var ancient = new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal)
        {
            [KeyA] = new(TotalOccurrences: 11, ObservedWindowCount: 3, IncidentStartedUtc: T0, LastObservedUtc: T0),
        };

        var result = IncidentOccurrenceAccumulator.Accumulate(
            new[] { Incident(KeyA, 5) }, ancient, T0.AddDays(30), TimeSpan.Zero);

        Assert.Equal(13L, result.Incidents[0].TotalOccurrences);
        Assert.Equal(T0, result.Incidents[0].IncidentStartedUtc);
    }

    [Fact]
    public void TheHorizonMatchesTheEnginesReadWindow()
    {
        /* The horizon is only the right answer because it equals the window the gauge is computed over. If
           someone widens the read window, this is the pin that says the horizon has to move with it. */
        Assert.Equal(1, AlertEngine.RollingCountWindowHours);
    }

    /* ---------------- what a consumer actually reads ---------------- */

    [Fact]
    public void TheRenderer_EmitsTheTotalAndTheIncidentStart_AsTheirOwnFacts()
    {
        /* Downstream automation keys on fact NAMES, so the total is a new fact rather than a redefinition of
           "Occurrences" — that one still means the window gauge, and changing its meaning under consumers
           who already read it would be the silent kind of break. */
        var incident = new AlertIncident(
            KeyA, new[] { "dbo.Users" }, OccurrenceCount: 3, WaitRange: null, DetailFields: null,
            TotalOccurrences: 12, IncidentStartedUtc: T0);

        var item = AlertIncidentRenderer.BuildItem(incident, "Deadlock", includeDetailFields: false);

        Assert.Equal("3", item.Fields.Single(f => f.Label == "Occurrences").Value);
        Assert.Equal("12", item.Fields.Single(f => f.Label == "Total Occurrences").Value);
        Assert.Equal("2026-08-12 14:00:00Z", item.Fields.Single(f => f.Label == "Incident Since").Value);
    }

    [Fact]
    public void TheRenderer_EmitsTheTotalEvenWhenItEqualsTheWindowCount()
    {
        /* An incident's FIRST delivery has total == window. The field must still appear, or a consumer
           polling for it sees it wink in and out depending on how far along the incident is. */
        var incident = new AlertIncident(
            KeyA, new[] { "dbo.Users" }, OccurrenceCount: 1, WaitRange: null, DetailFields: null,
            TotalOccurrences: 1, IncidentStartedUtc: T0);

        var item = AlertIncidentRenderer.BuildItem(incident, "Deadlock", includeDetailFields: false);

        Assert.Contains(item.Fields, f => f.Label == "Total Occurrences" && f.Value == "1");
        /* "Occurrences" keeps its > 1 condition — unchanged behavior for the gauge. */
        Assert.DoesNotContain(item.Fields, f => f.Label == "Occurrences");
    }

    [Fact]
    public void AnIncidentWithNoTotal_RendersNeitherNewFact()
    {
        var incident = new AlertIncident(KeyA, new[] { "dbo.Users" }, OccurrenceCount: 4);

        var item = AlertIncidentRenderer.BuildItem(incident, "Deadlock", includeDetailFields: false);

        Assert.DoesNotContain(item.Fields, f => f.Label == "Total Occurrences");
        Assert.DoesNotContain(item.Fields, f => f.Label == "Incident Since");
    }

    [Fact]
    public void TheContextJson_RoundTripsBothNewMembers()
    {
        /* The alert-history row has to carry them: the in-app dialog rehydrates the context, and a total that
           survived delivery but not persistence would read as "no total" the moment anyone looked at it
           later. */
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, new[]
        {
            new AlertIncident(KeyA, new[] { "dbo.Users" }, OccurrenceCount: 3, WaitRange: null,
                DetailFields: null, TotalOccurrences: 12, IncidentStartedUtc: T0),
        });

        var json = AlertContextSerializer.Serialize(context);
        Assert.True(AlertContextSerializer.TryDeserialize(json, out var rehydrated));

        var incident = Assert.Single(rehydrated!.Incidents!);
        Assert.Equal(12L, incident.TotalOccurrences);
        Assert.Equal(T0, incident.IncidentStartedUtc);
    }

    /* ---------------- the V61 store surface ---------------- */

    [Fact]
    public void V61_MigrationIdentity_AndStorageVersionTracksTheNewestRung()
    {
        var v61 = PgMigrations.Scripts.Single(m => m.Version == 61);

        Assert.Equal("incident-occurrence-counters", v61.Name);
        /* Invariant form, no literal to go stale: the build's schema version IS the newest
           registered rung (the recurring in-flight-branch failure; converted on the V62 merge). */
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);

        /* config.-qualified per the V17 rule — an unqualified CREATE resolves into collect (first on the
           migrate session's search_path), which is the wrong schema and the wrong ACL. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.incident_occurrences (", v61.Sql, StringComparison.Ordinal);

        /* Keyed per FINGERPRINT, not per metric — the distinction the feature rests on. */
        Assert.Contains("PRIMARY KEY (server_id, metric_name, dedup_key)", v61.Sql, StringComparison.Ordinal);

        /* bigint: a long-running incident's total is not bounded by anything the gauge is bounded by. */
        Assert.Contains("total_occurrences bigint NOT NULL", v61.Sql, StringComparison.Ordinal);

        /* The staleness stamp — without it a crash-stranded row silently undercounts the next incident. */
        Assert.Contains("last_observed_at timestamp NOT NULL", v61.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerSchemaGate_KnowsV61_SoAFullyMigratedStoreIsNotRefused()
    {
        /* The trap a StorageVersion bump sets: a probe that cannot SEE the newest migration maps every
           healthy store below RequiredStoreSchemaVersion and the connect-time gate refuses it permanently. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
        Assert.Contains("table_name = 'incident_occurrences'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCounterTable_IsNotFoldedIntoTheWatermarkTable()
    {
        /* Two independent reasons, one pin. The key is wrong (watermarks are per (server, metric), these are
           per fingerprint), and Lite writes the watermark row with INSERT OR REPLACE over a PARTIAL column
           list — so a counter column living there would be reset to its default on every fired alert, which
           is precisely when it is read. If someone later "simplifies" these into one table, this fails. */
        var v61 = PgMigrations.Scripts.Single(m => m.Version == 61);

        Assert.DoesNotContain("config_edge_trigger_watermarks", v61.Sql, StringComparison.Ordinal);
    }
}
