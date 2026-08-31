/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V107 (#2138 phase 1) — the force-plan bot's journal and the per-server opt-in column: the rung's
/// registration and DDL pins run everywhere; the store round-trip (journal → history windows →
/// pending-review linkage) runs gated on <c>DARLING_TEST_PG</c>. The history reads matter most: the
/// cooldowns and the failure memory are properties of these WINDOWED queries, so this is where
/// "give-up state heals by the window sliding" is proven against a real store rather than assumed.
/// </summary>
[Collection("live-postgres")]
public sealed class PlanForceActionStoreTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -213821;

    private readonly LivePostgresStoreFixture _fixture;

    public PlanForceActionStoreTests(LivePostgresStoreFixture fixture) => _fixture = fixture;

    /* ---------------- the rung (ungated) ---------------- */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("plan-force-actions", PgMigrations.Scripts.Single(s => s.Version == 107).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    [Fact]
    public void TheRungCreatesTheJournal_AndTheOptInColumnDefaultsClosed()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 107).Sql;

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.plan_force_actions", sql, StringComparison.Ordinal);
        /* Identity PK so review rows can reference their force row; GENERATED ALWAYS so INSERTs need
           no sequence USAGE grant (the V64 reasoning). */
        Assert.Contains("action_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", sql, StringComparison.Ordinal);
        Assert.Contains("related_action_id bigint", sql, StringComparison.Ordinal);

        /* Write-gate 2 must default CLOSED on every existing and future row — the whole point of the
           two-gate contract is that no migration, seed, or restore can open it by omission. */
        Assert.Contains("ALTER TABLE config.config_monitored_servers", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS plan_force_bot_enabled boolean NOT NULL DEFAULT FALSE", sql, StringComparison.Ordinal);
    }

    /* ---------------- the store round-trip (gated) ---------------- */

    [Fact]
    public async Task JournalRows_RoundTrip_AndDriveTheWindowedHistoryReads()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live journal round-trip.");
        Assert.True(_fixture.Established, "The live-postgres fixture did not establish the store.");

        await using var postgres = NpgsqlDataSource.Create(_fixture.ConnectionString!);
        var store = new PgPlanForceActionStore(postgres);
        var ct = TestContext.Current.CancellationToken;
        var now = DateTime.UtcNow;

        /* Pre-clean through the same #1902 helper the teardown uses (bodySucceeded: true so a failure
           here surfaces as its own error): a prior ABORTED run on a long-lived dev store may have left
           rows under this server id, and count-shaped assertions below would misread them as bugs. */
        await LiveStoreCleanup.RunAsync(_fixture.ConnectionString!, bodySucceeded: true,
            (cleanup, cleanupCt) => DeleteRowsAsync(cleanup, cleanupCt));

        var bodySucceeded = false;
        try
        {
            /* 1. A shadow-mode decision round-trips with every field intact. */
            var wouldForceId = await store.JournalAsync(Record(now.AddHours(-1),
                action: PgPlanForceActionStore.ActionWouldForce,
                decision: PgPlanForceActionStore.ActionWouldForce,
                reasons: "dry_run,server_not_opted_in",
                outcome: PgPlanForceActionStore.OutcomeLogged), ct);
            Assert.True(wouldForceId > 0);

            var recent = await store.GetRecentActionsAsync(TestServerId, now.AddDays(-1), 50, ct);
            var row = Assert.Single(recent);
            Assert.Equal(PgPlanForceActionStore.ActionWouldForce, row.Action);
            Assert.Equal("dry_run,server_not_opted_in", row.Reasons);
            Assert.Equal(12.5, row.RegressionFactor);
            Assert.Equal("orders", row.DatabaseName);
            Assert.Equal(42, row.QueryId);
            Assert.True(row.ParameterSensitivityCoFired == false);
            Assert.Equal(DateTimeKind.Utc, row.ActionTimeUtc.Kind);

            /* 2. The per-query cooldown window sees it; the server budget counts it. */
            var history = await store.GetQueryHistoryAsync(
                TestServerId, "orders", 42, ForcePlanBotSettings.Default, now, ct);
            Assert.NotNull(history.LastJournaledForQueryUtc);
            Assert.Equal(1, history.ServerActionsLast24h);
            Assert.Equal(0, history.RecentFailedForces);

            /* A different query on the same server: no per-query memory, same server budget. */
            var other = await store.GetQueryHistoryAsync(
                TestServerId, "orders", 43, ForcePlanBotSettings.Default, now, ct);
            Assert.Null(other.LastJournaledForQueryUtc);
            Assert.Equal(1, other.ServerActionsLast24h);

            /* 3. Failure memory is windowed, not latched: two review unforces inside the window
               count; the same rows aged past the cooldown horizon count zero — eligibility returns
               by the window sliding, with no reset write anywhere (#2677). */
            await store.JournalAsync(Record(now.AddHours(-2),
                action: PgPlanForceActionStore.ActionUnforce, decision: "not_net_benefit",
                reasons: "not_net_benefit", outcome: PgPlanForceActionStore.OutcomeSucceeded), ct);
            await store.JournalAsync(Record(now.AddHours(-3),
                action: PgPlanForceActionStore.ActionUnforce, decision: "force_failing",
                reasons: "force_failing", outcome: PgPlanForceActionStore.OutcomeSucceeded), ct);

            var withFailures = await store.GetQueryHistoryAsync(
                TestServerId, "orders", 42, ForcePlanBotSettings.Default, now, ct);
            Assert.Equal(2, withFailures.RecentFailedForces);

            var tinyWindow = ForcePlanBotSettings.Default with { FailedForceCooldownHours = 1 };
            var slid = await store.GetQueryHistoryAsync(
                TestServerId, "orders", 42, tinyWindow.Normalize(), now, ct);
            Assert.Equal(0, slid.RecentFailedForces);

            /* 4. Pending-review linkage: a succeeded live force is owed a review until a related
               review/unforce row closes it. */
            var forceId = await store.JournalAsync(Record(now.AddMinutes(-90),
                action: PgPlanForceActionStore.ActionForce, decision: PgPlanForceActionStore.ActionForce,
                reasons: "", outcome: PgPlanForceActionStore.OutcomeSucceeded,
                mode: PgPlanForceActionStore.ModeLive), ct);

            var pending = await store.GetPendingReviewsAsync(TestServerId, now, ct);
            var pendingRow = Assert.Single(pending);
            Assert.Equal(forceId, pendingRow.ActionId);

            await store.JournalAsync(Record(now,
                action: PgPlanForceActionStore.ActionReview, decision: "net_benefit_confirmed",
                reasons: "net_benefit_confirmed", outcome: PgPlanForceActionStore.OutcomeLogged,
                mode: PgPlanForceActionStore.ModeLive, relatedActionId: forceId), ct);

            Assert.Empty(await store.GetPendingReviewsAsync(TestServerId, now, ct));

            /* 5. A LIVE force writes an intent row and a completion row (both action='force', the
               completion pointing back via related_action_id). The daily budget must count that as
               ONE action — the double-count would spend the blast-radius cap at twice the rate for
               exactly the actions it exists to bound. Before this pair: the would_force (1) and the
               step-4 force (2); the pair adds its INTENT only (3). */
            var intentId = await store.JournalAsync(Record(now.AddMinutes(-30),
                action: PgPlanForceActionStore.ActionForce, decision: PgPlanForceActionStore.ActionForce,
                reasons: "", outcome: PgPlanForceActionStore.OutcomeAttempting,
                mode: PgPlanForceActionStore.ModeLive), ct);
            await store.JournalAsync(Record(now.AddMinutes(-30),
                action: PgPlanForceActionStore.ActionForce, decision: PgPlanForceActionStore.ActionForce,
                reasons: "", outcome: PgPlanForceActionStore.OutcomeSucceeded,
                mode: PgPlanForceActionStore.ModeLive, relatedActionId: intentId), ct);

            var afterPair = await store.GetQueryHistoryAsync(
                TestServerId, "orders", 42, ForcePlanBotSettings.Default, now, ct);
            Assert.Equal(3, afterPair.ServerActionsLast24h);

            /* 6. The fleet-wide audit read (null server filter) — exercises the DBNull parameter
               path a server-scoped read never touches. */
            var fleetWide = await store.GetRecentActionsAsync(null, now.AddDays(-1), 100, ct);
            Assert.True(fleetWide.Count >= 5);

            /* 7. Orphaned intents (#2731 round 3): an 'attempting' row whose completion journal
               write never landed surfaces for review once past the grace window — a force the
               journal lost track of must not escape review — while a FRESH intent (mid-force) and an
               intent with a completion row do not. */
            var freshIntent = await store.JournalAsync(Record(now.AddMinutes(-2),
                action: PgPlanForceActionStore.ActionForce, decision: PgPlanForceActionStore.ActionForce,
                reasons: "", outcome: PgPlanForceActionStore.OutcomeAttempting,
                mode: PgPlanForceActionStore.ModeLive), ct);
            var orphanIntent = await store.JournalAsync(Record(now.AddMinutes(-30),
                action: PgPlanForceActionStore.ActionForce, decision: PgPlanForceActionStore.ActionForce,
                reasons: "", outcome: PgPlanForceActionStore.OutcomeAttempting,
                mode: PgPlanForceActionStore.ModeLive), ct);

            var withOrphan = await store.GetPendingReviewsAsync(TestServerId, now, ct);
            Assert.DoesNotContain(withOrphan, r => r.ActionId == freshIntent);
            Assert.DoesNotContain(withOrphan, r => r.ActionId == intentId); /* has a completion row */
            Assert.Contains(withOrphan, r => r.ActionId == orphanIntent);

            /* A review row closes the orphan exactly like a completed force. */
            await store.JournalAsync(Record(now,
                action: PgPlanForceActionStore.ActionReview, decision: "no_longer_forced",
                reasons: "no_longer_forced", outcome: PgPlanForceActionStore.OutcomeLogged,
                mode: PgPlanForceActionStore.ModeLive, relatedActionId: orphanIntent), ct);
            Assert.DoesNotContain(
                await store.GetPendingReviewsAsync(TestServerId, now, ct),
                r => r.ActionId == orphanIntent);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(_fixture.ConnectionString!, bodySucceeded,
                (cleanup, cleanupCt) => DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static PlanForceActionRecord Record(
        DateTime timeUtc,
        string action,
        string decision,
        string reasons,
        string outcome,
        string mode = PgPlanForceActionStore.ModeDryRun,
        long? relatedActionId = null) => new(
            ActionId: 0,
            ActionTimeUtc: timeUtc,
            ServerId: TestServerId,
            ServerName: "plan-force-store-e2e",
            DatabaseName: "orders",
            QueryId: 42,
            PlanId: 7,
            Action: action,
            Mode: mode,
            Decision: decision,
            Reasons: reasons,
            RegressionFactor: 12.5,
            LatestCpuPerExecUs: 50000,
            BestCpuPerExecUs: 4000,
            ReplicaRole: null,
            ParameterSensitivityCoFired: false,
            Outcome: outcome,
            Detail: null,
            RelatedActionId: relatedActionId);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "DELETE FROM collect.plan_force_actions WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }
}
