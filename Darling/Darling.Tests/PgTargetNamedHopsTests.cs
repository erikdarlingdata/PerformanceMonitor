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
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691 exit check §WRONG 2 — <c>PG_IDLE_IN_TRANSACTION</c> at 1.2 NAMES its holder (application, role,
/// database) but was consumed mid-chain under <c>PG_CONNECTION_SATURATION</c> at 1.25, and the root's card never
/// said who: <c>grep billing</c> over the <c>analyze_server</c> payload found nothing. The engine now records the
/// identity-bearing hops it consumed (<see cref="AnalysisStory.NamedHops"/>) and the composer writes one
/// "Behind it:" sentence per hop onto the root's frozen investigation — which both MCP tools render verbatim from
/// StoryText, so the holder reaches the payload without the payload files changing. These pins hold the
/// PostgreSQL side: the roster, each family's seam, and the exit shape through the real scorer, graph and composer.
/// </summary>
public class PgTargetNamedHopsTests
{
    /* ── the roster and the seams ── */

    [Fact]
    public void ThePostgresRoster_IsExplicit_AndTheseAreItsMembers()
    {
        Assert.Equal(
        [
            PgTargetFactKeys.IdleInTransaction, PgTargetFactKeys.BlockingChain, PgTargetFactKeys.LongRunningQuery,
            PgTargetFactKeys.LockWaitEvents, PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.XminHold,
            PgTargetFactKeys.BloatTrend, PgTargetFactKeys.IndexBloatTrend,
            PgTargetFactKeys.ReplicationLag, PgTargetFactKeys.SlotRetention, PgTargetFactKeys.SlotXmin,
        ], FactIdentity.PostgresKeys);
        Assert.All(FactIdentity.PostgresKeys, k => Assert.True(PgTargetFactKeys.IsPgKey(k)));
        Assert.True(FactIdentity.IsIdentityBearingKey(PgTargetFactKeys.BadActorKey(7001)));
        /* The alias an edge names is never a fact's key and never an identity; a wait fact restates its event in
           ObjectName and names no one; the saturation root names the pool, not a who. */
        Assert.False(FactIdentity.IsIdentityBearingKey(PgTargetFactKeys.BadActorFamily));
        Assert.False(FactIdentity.IsIdentityBearingKey(PgTargetFactKeys.WaitKey("Lock", "relation")));
        Assert.False(FactIdentity.IsIdentityBearingKey(PgTargetFactKeys.ConnectionSaturation));
    }

    [Fact]
    public void Describe_ReadsEachPostgresIdentityOffTheSeamItsCollectorPartialFills()
    {
        /* Lanes 3/14, 15, 17: "application as role" in ObjectName, the database beside it. */
        Assert.Equal("`billing-worker as billing` on `appdb`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.IdleInTransaction, ObjectName = "billing-worker as billing", DatabaseName = "appdb" }));
        Assert.Equal("`etl as loader`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.LongRunningQuery, ObjectName = "etl as loader" }));
        Assert.Equal("lead blocker `(no application_name) as app` on `appdb`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.BlockingChain, ObjectName = "(no application_name) as app", DatabaseName = "appdb" }));
        Assert.Equal("relation `public.orders`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.LockWaitEvents, ObjectName = "public.orders" }));
        /* Lane 7: the queryid is the name; pg_statement_stats carries the OID, not the database (DatabaseName null). */
        Assert.Equal("statement 7001",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.BadActorKey(7001) }));
        /* Lanes 4, 13: table or index, in its database. */
        Assert.Equal("table `public.hot` in `appdb`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.AutovacuumBacklog, ObjectName = "public.hot", DatabaseName = "appdb" }));
        Assert.Equal("table `public.hot` in `appdb`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.BloatTrend, ObjectName = "public.hot", DatabaseName = "appdb" }));
        Assert.Equal("index `public.hot.hot_pkey` in `appdb`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.IndexBloatTrend, ObjectName = "public.hot.hot_pkey", DatabaseName = "appdb" }));
        Assert.Equal("horizon holder `session:12345`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.XminHold, ObjectName = "session:12345" }));
        /* Lane 12: the standby's application_name, the slot. */
        Assert.Equal("standby `replica-2`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.ReplicationLag, ObjectName = "replica-2" }));
        Assert.Equal("slot `cdc_main` in `appdb`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.SlotRetention, ObjectName = "cdc_main", DatabaseName = "appdb" }));
        Assert.Equal("slot `cdc_main`",
            FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.SlotXmin, ObjectName = "cdc_main" }));

        /* An empty seam is no identity: a lock rollup with no top relation, a replication fact with no name. */
        Assert.Null(FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.LockWaitEvents }));
        Assert.Null(FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.ReplicationLag, ObjectName = " " }));
        Assert.Null(FactIdentity.Describe(new Fact { Key = PgTargetFactKeys.WaitKey("Lock", "relation"), ObjectName = "Lock:relation" }));
    }

    /* ── the exit shape ── */

    /// <summary>Lane 14's saturation fact at the share bar: 30 of 90 parked (share 0.333 ≥ 0.25), 90 of 97 usable → 1.25.</summary>
    private static Fact Saturation(double peak = 90, double idleInTransaction = 30, double maxConnections = 100, double reserved = 3)
    {
        var usable = maxConnections - reserved;
        return new Fact
        {
            Source = PgTargetSources.SessionsSource,
            Key = PgTargetFactKeys.ConnectionSaturation,
            Value = peak / usable,
            ServerId = 1,
            Metadata =
            {
                ["saturation_ratio"] = peak / usable,
                ["peak_total_sessions"] = peak,
                ["peak_active_sessions"] = 40,
                ["peak_idle_in_transaction_sessions"] = idleInTransaction,
                ["peak_other_sessions"] = Math.Max(0, peak - 40 - idleInTransaction),
                ["peak_idle_in_transaction_share"] = idleInTransaction / peak,
                ["peak_age_s"] = 7_200,
                ["latest_total_sessions"] = 60,
                ["latest_active_sessions"] = 20,
                ["latest_idle_in_transaction_sessions"] = 5,
                ["latest_age_s"] = 0,
                ["max_connections"] = maxConnections,
                ["superuser_reserved_connections"] = reserved,
                ["usable_connections"] = usable,
                ["max_connections_pending_restart"] = 0,
                ["config_snapshot_age_s"] = 1_800,
                ["captures_with_rows"] = 48,
                ["rows_redacted_share"] = 0,
            },
        };
    }

    /// <summary>Lane 14's holder: `billing-worker` as `billing` in `appdb`, 15 min, pinning the horizon, seen in six captures → 1.2.</summary>
    private static Fact Parked(string application = "billing-worker", string user = "billing", string database = "appdb") => new()
    {
        Source = PgTargetSources.SessionsSource,
        Key = PgTargetFactKeys.IdleInTransaction,
        Value = 900,
        ServerId = 1,
        DatabaseName = database,
        ObjectName = $"{application} as {user}",
        Metadata =
        {
            [PgTargetScorer.IdleInTransactionDurationMsKey] = 900_000,
            [PgTargetScorer.IdleInTransactionHolderHorizonAgeKey] = 5_000_000,
            ["holder_is_horizon_holder"] = 1,
            ["holder_captures_seen"] = 6,
            [PgTargetScorer.IdleInTransactionRecurringCapturesKey] = 6,
            ["holder_identities"] = 1,
            ["holder_identities_pinning_horizon"] = 1,
            ["captures_with_holders"] = 6,
            ["captures_with_rows"] = 48,
            ["holder_rows"] = 6,
            ["peak_concurrent_holders"] = 1,
            ["peak_age_s"] = 1_800,
            ["holder_last_seen_age_s"] = 300,
            ["floor_ms"] = PgTargetScorer.IdleInTransactionWarningMs,
            ["rows_redacted_share"] = 0,
        },
    };

    /// <summary>
    /// The exit check's fixture through the real pipeline order (score → BuildStories → PopulateStoryText): the
    /// saturation root (1.25) consumes the idle holder (1.2) as its one hop, and the ROOT card's investigation now
    /// names `billing-worker` — the grep that found nothing finds the root card. Headline and remediation are the
    /// saturation composer's own; the sentence is identity first (with the database, which the idle headline does
    /// not carry) and the idle composer's own headline after it.
    /// </summary>
    [Fact]
    public void SaturationRootConsumingTheIdleHolder_NamesTheHolderOnTheSaturationCard()
    {
        var facts = new List<Fact> { Saturation(), Parked() };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(1.25, facts[0].Severity, precision: 9);
        Assert.Equal(1.2, facts[1].Severity, precision: 9);

        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.IdleInTransaction }, story.Path);
        var hop = Assert.Single(story.NamedHops);
        Assert.Equal(new NamedHop(PgTargetFactKeys.IdleInTransaction, "`billing-worker as billing` on `appdb`", 1.2), hop);

        var byKey = facts.ToFactLookup();
        var rootAlone = FactAdvice.Compose(PgTargetFactKeys.ConnectionSaturation, byKey)!;
        Assert.DoesNotContain("billing", rootAlone.Investigation, StringComparison.OrdinalIgnoreCase);

        FactAdvice.PopulateStoryText([story], facts);
        var advice = FactAdvice.TryReadStoryText(story.StoryText)!;
        Assert.Equal(rootAlone.Headline, advice.Headline);
        Assert.Equal(rootAlone.Remediation, advice.Remediation);
        Assert.StartsWith(rootAlone.Investigation, advice.Investigation, StringComparison.Ordinal);

        var sentence = advice.Investigation.Substring(rootAlone.Investigation.Length);
        var idleHeadline = FactAdvice.Compose(PgTargetFactKeys.IdleInTransaction, byKey)!.Headline;
        Assert.Equal(
            $" {FactIdentity.SentenceMarker} `{PgTargetFactKeys.IdleInTransaction}` (severity 1.20) — `billing-worker as billing` on `appdb`: {idleHeadline}.",
            sentence);
        Assert.Contains("billing-worker", sentence, StringComparison.Ordinal);
        Assert.Contains("appdb", sentence, StringComparison.Ordinal);
        Assert.Equal(1, advice.Investigation.Split(FactIdentity.SentenceMarker).Length - 1);
        /* Lane 14's headline names the holder itself, so the name appears twice in the one sentence — the label and
           the finding; FactIdentity.Sentence says why that is accepted over a dedupe that would drop the database. */
        Assert.Contains("billing-worker as billing", idleHeadline, StringComparison.Ordinal);
    }

    /// <summary>
    /// Byte-identity for the story this does not concern: saturation alone (lane 5's one-card mesh) freezes
    /// exactly what the saturation composer returns — no marker, no NamedHops. And when the idle fact ROOTS (under
    /// the share bar the two split), its own card names the holder as it did before and gains no sentence.
    /// </summary>
    [Fact]
    public void SaturationAlone_AndAnIdleRoot_AreByteIdenticalToTheirComposedBlocks()
    {
        var alone = new List<Fact> { Saturation() };
        new FactScorer().ScoreAll(alone);
        var solo = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(alone));
        Assert.Empty(solo.NamedHops);
        FactAdvice.PopulateStoryText([solo], alone);
        Assert.Equal(FactAdvice.SerializeForStoryText(FactAdvice.Compose(PgTargetFactKeys.ConnectionSaturation, alone.ToFactLookup())), solo.StoryText);

        var split = new List<Fact> { Saturation(idleInTransaction: 22), Parked() };
        new FactScorer().ScoreAll(split);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(split);
        Assert.Equal(2, stories.Count);
        Assert.All(stories, s => Assert.Empty(s.NamedHops));
        FactAdvice.PopulateStoryText(stories, split);
        var idleRoot = Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.IdleInTransaction);
        Assert.Equal(FactAdvice.SerializeForStoryText(FactAdvice.Compose(PgTargetFactKeys.IdleInTransaction, split.ToFactLookup())), idleRoot.StoryText);
        Assert.Contains("billing-worker", idleRoot.StoryText, StringComparison.Ordinal);
        Assert.DoesNotContain(FactIdentity.SentenceMarker, idleRoot.StoryText, StringComparison.Ordinal);
    }
}
