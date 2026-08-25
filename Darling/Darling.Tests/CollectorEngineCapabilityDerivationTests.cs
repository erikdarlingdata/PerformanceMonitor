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
using System.Reflection;
using System.Reflection.Emit;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2518: proof that <see cref="CollectorEngineCapability.IsCollectedOnEngineEdition(ICollectorSchemaInfo, int)"/>
/// FOLLOWS a gate that moves, rather than merely agreeing with the gates as they ship today.
///
/// <para><b>Why the existing tests are not this.</b> Every assertion in
/// <c>CollectorEngineCapabilityTests</c> is a statement about the shipped collectors — system_health is a
/// gap on Azure SQL Database, job_history is not, box editions have none. All of them are true, none of them
/// can distinguish a derivation from a hard-coded set of gaps that happens to match today's gates, and a
/// hard-coded set is exactly the failure the derivation exists to prevent. Nothing in the suite moves a gate
/// and watches the answer move with it, so nothing in the suite would notice if the answer stopped being
/// derived at all.</para>
///
/// <para><b>Why a synthetic collector rather than a real one.</b> The obvious demonstration is to point at a
/// collector whose gate changed — and #2512/#2516 supplied one, <c>TempDbStatsCollector</c>, while #2511 was
/// still open. Pinning the derivation to the one gate that is in flight is pinning the wrong thing: the pin
/// then fails whenever that lane lands, says "update the pin", and gets updated rather than read. A gate this
/// test owns moves on demand, moves in both directions, and drags no real collector into the assertion. The
/// collectors as shipped stay the subject of the OTHER file, which is where a claim about them belongs.</para>
///
/// <para><b>Both directions, on ONE object.</b> The gate is mutated between calls on the same instance, so
/// the two answers differ with literally nothing else changed — not the collector name, not the object
/// identity, not the edition. A pair of differently-constructed fakes would leave "it keyed off something
/// about the instance" open; this does not.</para>
/// </summary>
public sealed class CollectorEngineCapabilityMovingGateTests
{
    private const int Enterprise = 3;
    private const int AzureSqlDb = CollectorEngineCapability.AzureSqlDatabaseEngineEdition;
    private const int AzureMi = CollectorEngineCapability.AzureManagedInstanceEngineEdition;

    /// <summary>
    /// Every <c>SERVERPROPERTY('EngineEdition')</c> value Microsoft documents, as a contiguous range rather
    /// than a curated list — this is the DOMAIN of the question, so covering it densely costs nothing and a
    /// value that gets defined later is already included.
    /// </summary>
    private static readonly int[] AllEngineEditions = Enumerable.Range(1, 12).ToArray();

    /// <summary>
    /// A collector definition whose gate the test owns and can move between calls. Everything else is the
    /// bare minimum <see cref="ICollectorSchemaInfo"/> demands — none of it participates in the capability
    /// question, and giving it plausible-looking values would only invite the reader to wonder whether it
    /// does.
    /// <para>The name is deliberately not a catalog name. That is load-bearing:
    /// <see cref="TheByDefinitionOverload_AsksTheGate_WhereTheByNameOverloadCanOnlyAskTheCatalog"/> uses the
    /// same object through both overloads and gets different answers, which is what shows the definition
    /// overload is evaluating the gate rather than resolving a name.</para>
    /// </summary>
    private sealed class SyntheticCollector : ICollectorSchemaInfo
    {
        /// <summary>The gate under test. Settable, not <c>init</c>, so one instance can be moved.</summary>
        public required Func<CollectorTargetInfo, bool> Gate { get; set; }

        /// <summary>The engine half of the dispatch gate, and the discriminator the KIND axis derives from
        /// (#2530). Settable for the same reason <see cref="Gate"/> is: the engine-kind assertions move it
        /// between calls on ONE instance, so the two answers differ with nothing else changed.</summary>
        public CollectorTargetEngine TargetEngine { get; set; } = CollectorTargetEngine.SqlServer;

        public string Name => "synthetic_gate_2518";

        public string TargetTable => "synthetic_gate_2518";

        public bool IncludesCollectionId => true;

        public string PrefixIdColumnName => "collection_id";

        public string PrefixTimeColumnName => "collection_time";

        public IReadOnlyList<CollectorColumn> PayloadColumns => Array.Empty<CollectorColumn>();

        public bool YieldsOnLockTimeout => false;

        public IReadOnlyList<string> StateKeys => Array.Empty<string>();

        public bool AppliesTo(CollectorTargetInfo target) => Gate(target);
    }

    /// <summary>
    /// The assertion the issue was filed for. One collector, one object, three gates: closed against Azure
    /// SQL Database, open everywhere, then closed against everything BUT Azure SQL Database. The capability
    /// answer tracks all three.
    ///
    /// <para>The third position matters as much as the first two. A "derivation" that had quietly become
    /// "Azure SQL Database is where the gaps are" would pass the first two moves and fail here, because here
    /// the gap is on Enterprise and Managed Instance and Azure SQL Database is the edition that collects.</para>
    /// </summary>
    [Fact]
    public void TheAnswerMoves_WhenTheGateMoves_InBothDirections()
    {
        var collector = new SyntheticCollector { Gate = target => !target.IsAzureSqlDb };

        /* Gate closed against Azure SQL Database: a permanent gap there, and nowhere else. */
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureSqlDb));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, Enterprise));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureMi));

        /* The gate OPENS — the #2516 direction, the one a hand-kept list fails silently in, because a list
           goes on claiming a gap that the gate stopped producing and nothing says so. */
        collector.Gate = _ => true;
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureSqlDb));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, Enterprise));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureMi));

        /* And closes again the OTHER way round, so the answer cannot be "Azure SQL Database is special". */
        collector.Gate = target => target.IsAzureSqlDb;
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureSqlDb));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, Enterprise));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureMi));
    }

    /// <summary>
    /// The by-definition overload asks the GATE; the by-name overload asks the CATALOG and then the gate it
    /// finds. Handed a definition the catalog has never heard of, they must therefore disagree — the
    /// definition form reports the gap its gate produces, the name form makes no claim at all because there
    /// is no gate behind that name to derive one from.
    ///
    /// <para>This is the assertion that stops the new overload being a rename. If it delegated back to the
    /// name, or resolved the definition through the catalog, both calls below would return <c>true</c> and
    /// the moving-gate test above would be exercising nothing.</para>
    /// </summary>
    [Fact]
    public void TheByDefinitionOverload_AsksTheGate_WhereTheByNameOverloadCanOnlyAskTheCatalog()
    {
        var collector = new SyntheticCollector { Gate = _ => false };

        Assert.Null(CollectorCatalog.Find(collector.Name));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureSqlDb));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector.Name, AzureSqlDb));
    }

    /// <summary>
    /// For every collector the catalog DOES know, the two overloads are the same function. The by-name form
    /// is the one both MCP trees call, so a divergence would mean the tests below prove a property of code
    /// nothing in production reaches.
    /// </summary>
    [Fact]
    public void TheTwoOverloads_AgreeOnEveryShippedCollectorAtEveryEdition()
    {
        Assert.NotEmpty(CollectorCatalog.All);

        foreach (var definition in CollectorCatalog.All)
        {
            foreach (var edition in AllEngineEditions)
            {
                Assert.Equal(
                    CollectorEngineCapability.IsCollectedOnEngineEdition(definition, edition),
                    CollectorEngineCapability.IsCollectedOnEngineEdition(definition.Name, edition));
            }
        }
    }

    /// <summary>
    /// A gate that closes on a FIXABLE fact is not an engine gap, demonstrated on gates this test controls
    /// rather than on <c>job_history</c>'s. Same claim as <c>AFixableGate_IsNotReportedAsAnEngineGap</c>,
    /// but it stays true when the Agent collectors' gates change, and it covers the gate shapes no shipped
    /// collector happens to have today.
    ///
    /// <para>The last case is the one worth reading: a gate that reads BOTH a fixable fact and the edition
    /// is still an engine gap on that edition, because no combination of the fixable facts rescues it. That
    /// is the distinction the whole helper exists to draw, and neither half of it alone would show it.</para>
    /// </summary>
    [Fact]
    public void AGateOnAFixableFact_IsNotAnEngineGap_ButTheSameGateAndAnEditionStillIs()
    {
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            new SyntheticCollector { Gate = target => target.HasMsdbAccess }, Enterprise));

        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            new SyntheticCollector { Gate = target => !target.IsAwsRds }, Enterprise));

        /* Both fixable facts at once: the sweep has to carry the COMBINATION, not just each value somewhere. */
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            new SyntheticCollector { Gate = target => target.HasMsdbAccess && target.IsAwsRds }, Enterprise));

        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            new SyntheticCollector { Gate = target => !target.HasMsdbAccess && !target.IsAwsRds }, Enterprise));

        /* Fixable AND edition-bound: permanent on Azure SQL Database, fixable everywhere else. */
        var mixed = new SyntheticCollector { Gate = target => target.HasMsdbAccess && !target.IsAzureSqlDb };
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition(mixed, AzureSqlDb));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(mixed, Enterprise));
    }

    /// <summary>
    /// A version gate is answered across the real majors, not by whichever single representative value the
    /// sweep happened to pick. Every major the sweep carries must be reachable ON ITS OWN, or a gate written
    /// as a RANGE — supported on 15 and 16, dropped on 17 — would be reported as a permanent engine gap on
    /// hardware it runs on perfectly well.
    /// <para>The floor and ceiling cases bracket it: a gate that needs the newest engine, and one that only
    /// tolerates the oldest, are both answered "collected".</para>
    /// </summary>
    [Fact]
    public void AVersionGate_IsAnsweredAcrossTheRealMajors_NotByOneRepresentativeValue()
    {
        foreach (var major in new[] { 11, 12, 13, 14, 15, 16, 17 })
        {
            var only = major;
            Assert.True(
                CollectorEngineCapability.IsCollectedOnEngineEdition(
                    new SyntheticCollector { Gate = target => target.SqlMajorVersion == only }, Enterprise),
                $"a gate that passes only on SQL major {only} was reported as a permanent engine gap, so the " +
                "sweep does not carry that major — every real major has to be reachable on its own");
        }

        /* A range, the shape the sweep's own doc comment says it exists for. */
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            new SyntheticCollector { Gate = target => target.SqlMajorVersion is 15 or 16 }, Enterprise));

        /* A floor above every real major, and the "unknown, assume newest" value. */
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            new SyntheticCollector { Gate = target => target.SqlMajorVersion >= 99 }, Enterprise));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            new SyntheticCollector { Gate = target => target.SqlMajorVersion == 0 }, Enterprise));
    }

    /// <summary>
    /// The non-vacuity floor, both ways and on every edition: a gate shut everywhere is a gap everywhere, a
    /// gate open everywhere is a gap nowhere. If either of these ever stopped holding, every other assertion
    /// in this file would be passing for a reason unrelated to the gate.
    /// <para>Unknown (0) is excluded from the loop and asserted separately, because it is the one edition
    /// where a shut gate must STILL make no claim — "we have not probed this server" is not "this will never
    /// work", and that branch sits above the sweep where no gate can reach it.</para>
    /// </summary>
    [Fact]
    public void AGateShutEverywhere_IsAGapOnEveryEdition_AndAnOpenGateOnNone()
    {
        var shut = new SyntheticCollector { Gate = _ => false };
        var open = new SyntheticCollector { Gate = _ => true };

        foreach (var edition in AllEngineEditions)
        {
            Assert.False(
                CollectorEngineCapability.IsCollectedOnEngineEdition(shut, edition),
                $"a gate that refuses every target was not reported as a gap on engine edition {edition}");
            Assert.True(
                CollectorEngineCapability.IsCollectedOnEngineEdition(open, edition),
                $"a gate that accepts every target was reported as a gap on engine edition {edition}");
        }

        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(
            shut, CollectorEngineCapability.UnknownEngineEdition));
    }

    /// <summary>
    /// A definition written in another engine's dialect is never a gap on a SQL Server edition, however its
    /// own gate is set — the question does not apply to it.
    /// <para>The <c>Gate = _ =&gt; true</c> case is the one that isolates the short-circuit. The sweep asks
    /// <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>, whose engine half
    /// rejects every SQL Server shape, so WITHOUT the short-circuit an always-open PostgreSQL collector comes
    /// back as a permanent gap on all nine editions — a confident claim about an engine it was never meant to
    /// run on. With <c>_ =&gt; false</c> the two paths agree by accident and prove nothing.</para>
    /// </summary>
    [Fact]
    public void AForeignEngineDefinition_IsNeverAGap_HoweverItsOwnGateIsSet()
    {
        foreach (var gate in new Func<CollectorTargetInfo, bool>[] { _ => true, _ => false })
        {
            var postgres = new SyntheticCollector
            {
                TargetEngine = CollectorTargetEngine.PostgreSql,
                Gate = gate,
            };

            foreach (var edition in AllEngineEditions)
            {
                Assert.True(
                    CollectorEngineCapability.IsCollectedOnEngineEdition(postgres, edition),
                    $"a PostgreSQL definition was reported as a permanent gap on engine edition {edition}");
            }
        }
    }

    /* ───────── the engine-KIND axis (#2530) ───────── */

    /// <summary>
    /// The kind axis is DERIVED too, and this is what says so: one object, one engine kind, the
    /// definition's own <see cref="ICollectorSchemaInfo.TargetEngine"/> moved between calls, and the answer
    /// moves with it. Nothing else changes — not the name, not the object identity, not the token.
    ///
    /// <para>Without this the kind axis could have been "the eight pg_ collectors are the PostgreSQL ones",
    /// which is true today, would pass every assertion anyone would write about the shipped catalog, and
    /// would go stale in the direction that keeps passing — exactly the failure #2518 exists to prevent
    /// one axis over.</para>
    /// </summary>
    [Fact]
    public void TheKindAnswerMoves_WhenTheDefinitionsEngineMoves_InBothDirections()
    {
        var collector = new SyntheticCollector { Gate = _ => true, TargetEngine = CollectorTargetEngine.SqlServer };

        /* A SQL Server dialect asked about a PostgreSQL target: a permanent gap, on both PG tokens. */
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.Postgres));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.AuroraPostgres));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.SqlServer));

        /* The SAME object, one field moved. Every answer inverts. */
        collector.TargetEngine = CollectorTargetEngine.PostgreSql;
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.Postgres));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.AuroraPostgres));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.SqlServer));

        /* And back, so the answer cannot be "PostgreSQL is the special one". */
        collector.TargetEngine = CollectorTargetEngine.SqlServer;
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.Postgres));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.SqlServer));
    }

    /// <summary>
    /// A gate SHUT everywhere is a kind gap on both PostgreSQL tokens; a gate on a fact the sweep varies is
    /// a gap on neither. This is the assertion #2532 replaced: until then the kind axis asked only the
    /// ENGINE half of the dispatch gate, and a definition's own <c>AppliesTo</c> could not make it claim
    /// anything at all.
    ///
    /// <para><b>Why that narrowing was right then and wrong now.</b> The discipline it protected — never
    /// report a FIXABLE gate as permanent — is unchanged and is the whole subject of this file. What it
    /// lacked was a sweep: with no way to separate "excluded on every target of this kind" from "excluded on
    /// the one target shape somebody happened to construct", the only safe answer was to decline. Now
    /// <see cref="CollectorEngineCapability.TargetsWithEngineKind"/> varies the PostgreSQL version floors and
    /// the recovery state and fixes only <see cref="CollectorTargetInfo.IsAurora"/>, so the fixable gates
    /// answer TRUE on their own merits rather than by the axis refusing to look — which is what the second
    /// half of this test is for, and what
    /// <c>EveryFactAPostgresGateReads_IsVariedBySweepOrFixedByKind</c> keeps true as gates are added.</para>
    /// </summary>
    [Fact]
    public void AShutAppliesToGate_IsAnEngineKindGap_ButAFixableOneIsNot()
    {
        var postgres = new SyntheticCollector { Gate = _ => false, TargetEngine = CollectorTargetEngine.PostgreSql };

        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.Postgres));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.AuroraPostgres));

        /* ...and the engine half still speaks, so a PostgreSQL definition is a gap on SQL Server for the
           dialect reason rather than for this one. */
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.SqlServer));

        /* The half that keeps the narrowing's point: a gate reading a fact an operator can move is not a
           permanent gap on EITHER token. An upgrade crosses the floor; a writer connection leaves recovery.
           A sweep that stopped varying either of these would report both of them as "never will". */
        foreach (var fixableGate in new Func<CollectorTargetInfo, bool>[]
                 {
                     target => target.PostgresMajorVersion >= 16,
                     target => target.PostgresVersionNum >= 170005,
                     target => !target.IsInRecovery,
                     target => target.IsInRecovery,
                 })
        {
            postgres.Gate = fixableGate;
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.Postgres));
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.AuroraPostgres));
        }

        /* And the one fact the KIND fixes: Aurora-ness. Nothing an operator does turns a stock PostgreSQL
           server into an Aurora one, so this is the only PostgreSQL gate shape that produces a permanent
           gap — and it produces it on exactly one of the two tokens, in each direction. */
        postgres.Gate = target => target.IsAurora;
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.Postgres));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.AuroraPostgres));

        postgres.Gate = target => !target.IsAurora;
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.Postgres));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(postgres, MonitoredEngineKind.AuroraPostgres));
    }

    /// <summary>
    /// The SQL Server side of the same question, and the reason the two axes do not answer each other's:
    /// a collector gated off on Azure SQL Database is NOT a kind gap on <c>sqlserver</c>, because it runs on
    /// every other SQL Server there is. The edition axis is what says so, with the edition named.
    ///
    /// <para>This is what <see cref="CollectorEngineCapability.TargetsWithEngineKind"/> varying the two Azure
    /// flags buys, and it is the property that would break silently if the SQL Server arm of that sweep ever
    /// fixed them the way <see cref="CollectorEngineCapability.TargetsWithEngineEdition"/> does: every
    /// Azure-gated collector would become a permanent gap on every SQL Server, and the message would stop
    /// naming the edition that is actually responsible.</para>
    /// </summary>
    [Fact]
    public void AnAzureOnlyGate_IsNotAKindGapOnSqlServer_ButIsStillAnEditionGap()
    {
        var collector = new SyntheticCollector
        {
            Gate = target => !target.IsAzureSqlDb,
            TargetEngine = CollectorTargetEngine.SqlServer,
        };

        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.SqlServer));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, AzureSqlDb));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(collector, Enterprise));

        /* A gate shut on every SQL Server shape IS a kind gap, so the assertion above is not vacuous. */
        collector.Gate = _ => false;
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(collector, MonitoredEngineKind.SqlServer));
    }

    /// <summary>
    /// An absent or unrecognised token makes NO claim, whatever the definition's engine. This is the
    /// guarantee #2530 was told to keep: the distinction being added is "known to be PostgreSQL", never
    /// "not known to be SQL Server". A store one rung behind, and a server that has not connected since the
    /// rung landed, both land here — and both must keep the miss vocabulary they had.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("something-a-newer-build-writes")]
    public void AnUnknownEngineKind_MakesNoClaim_ForEitherDialect(string? engineKind)
    {
        foreach (var engine in new[] { CollectorTargetEngine.SqlServer, CollectorTargetEngine.PostgreSql })
        {
            var collector = new SyntheticCollector { Gate = _ => true, TargetEngine = engine };
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(collector, engineKind));
        }
    }

    /// <summary>
    /// The by-name and by-definition kind overloads are the same function for every collector the catalog
    /// knows, and disagree exactly where they must: on a definition the catalog has never heard of, the
    /// name form has no gate to ask and makes no claim.
    /// </summary>
    [Fact]
    public void TheTwoKindOverloads_Agree_ExceptWhereTheNameCannotBeResolved()
    {
        Assert.NotEmpty(CollectorCatalog.All);

        foreach (var definition in CollectorCatalog.All)
        {
            foreach (var kind in MonitoredEngineKind.All)
            {
                Assert.Equal(
                    CollectorEngineCapability.IsCollectedOnEngineKind(definition, kind),
                    CollectorEngineCapability.IsCollectedOnEngineKind(definition.Name, kind));
            }
        }

        var unknown = new SyntheticCollector { Gate = _ => true, TargetEngine = CollectorTargetEngine.SqlServer };
        Assert.Null(CollectorCatalog.Find(unknown.Name));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(unknown, MonitoredEngineKind.Postgres));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(unknown.Name, MonitoredEngineKind.Postgres));
    }

    /// <summary>
    /// The shipped catalog, as the axis sees it: every SQL Server collector is a gap on a PostgreSQL
    /// target and none on a SQL Server one, and the PostgreSQL collectors are the mirror image.
    /// Both halves are counted from the catalog rather than listed, so the numbers track it.
    ///
    /// <para>This is the vacuity check for everything above: if the derivation quietly answered TRUE for
    /// everything, every "no claim" assertion in this file would still pass and this would not.</para>
    /// </summary>
    [Fact]
    public void TheShippedCatalogSplitsCleanlyOnTheKindAxis()
    {
        var sqlServer = CollectorCatalog.All.Where(c => c.TargetEngine == CollectorTargetEngine.SqlServer).ToArray();
        var postgres = CollectorCatalog.All.Where(c => c.TargetEngine == CollectorTargetEngine.PostgreSql).ToArray();

        Assert.True(sqlServer.Length >= 30, $"only {sqlServer.Length} SQL Server definitions — the catalog walk is broken");
        Assert.True(postgres.Length >= 8, $"only {postgres.Length} PostgreSQL definitions — the catalog walk is broken");

        foreach (var pgToken in new[] { MonitoredEngineKind.Postgres, MonitoredEngineKind.AuroraPostgres })
        {
            Assert.All(sqlServer, c => Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(c, pgToken)));
        }

        Assert.All(sqlServer, c => Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(c, MonitoredEngineKind.SqlServer)));
        Assert.All(postgres, c => Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(c, MonitoredEngineKind.SqlServer)));

        /* Aurora is a strict superset of the surfaces the PostgreSQL collectors read, so every one of them
           applies there. This is the half that would break if a new collector were written against something
           Aurora removes rather than adds. */
        Assert.All(postgres, c => Assert.True(
            CollectorEngineCapability.IsCollectedOnEngineKind(c, MonitoredEngineKind.AuroraPostgres),
            $"{c.Name} is reported as a permanent gap on Aurora PostgreSQL"));

        /* Stock PostgreSQL is where the Aurora-only surfaces become a real gap (#2532), so the two tokens
           genuinely differ — counted from the catalog rather than listed, and asserted as a PROPER subset so
           neither "everything is a gap" nor "nothing is" passes. */
        var stockGaps = postgres
            .Where(c => !CollectorEngineCapability.IsCollectedOnEngineKind(c, MonitoredEngineKind.Postgres))
            .Select(c => c.Name)
            .ToArray();

        Assert.NotEmpty(stockGaps);
        Assert.True(stockGaps.Length < postgres.Length,
            "every PostgreSQL collector is reported as a permanent gap on stock PostgreSQL, which would mean " +
            "the sweep stopped producing shapes rather than that the gates changed: " + string.Join(", ", stockGaps));
    }

    /// <summary>
    /// The measurement #2532 was filed on, named: <c>aurora_stat_system_waits()</c> is an Aurora built-in
    /// that core PostgreSQL has no equivalent of in any version, so <c>pg_wait_stats</c> can never run on a
    /// stock PostgreSQL target — and runs perfectly well on the Aurora one, which is the whole fleet today.
    ///
    /// <para>The three collectors beside it are the control. <c>pg_io_stats</c> carries a PG16 floor and
    /// <c>pg_autovacuum_stats</c> a writer-only gate; both are FIXABLE, both keep the <c>unavailable</c>
    /// vocabulary that sends an operator to look, and a sweep that stopped varying either fact would report
    /// them here as "never will". <c>pg_blocking</c> has no gate at all.</para>
    /// </summary>
    [Fact]
    public void AuroraOnlySurfaces_ArePermanentGapsOnStockPostgres_AndTheFixableGatesAreNot()
    {
        foreach (var auroraOnly in new[] { "pg_wait_stats", "pg_statement_stats" })
        {
            Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind(auroraOnly, MonitoredEngineKind.Postgres), auroraOnly);
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(auroraOnly, MonitoredEngineKind.AuroraPostgres), auroraOnly);
        }

        foreach (var fixable in new[] { "pg_io_stats", "pg_autovacuum_stats", "pg_blocking" })
        {
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(fixable, MonitoredEngineKind.Postgres), fixable);
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind(fixable, MonitoredEngineKind.AuroraPostgres), fixable);
        }
    }

    /// <summary>
    /// The two shapes of kind gap say different things, because they ARE different things: a foreign dialect
    /// is stopped by the dispatch gate's engine half before the collector's own gate is consulted, while
    /// <c>pg_wait_stats</c> on stock PostgreSQL is that collector's own <c>AppliesTo</c>. One sentence for
    /// both would tell a PostgreSQL operator that a PostgreSQL collector is not written for PostgreSQL,
    /// which is the sort of wrong that costs a reader their trust in the rest of the message.
    /// </summary>
    [Fact]
    public void TheKindMessage_NamesTheRealReason_DialectOrTheCollectorsOwnGate()
    {
        var dialect = CollectorEngineCapability.NotCollectedMessage(
            "aurora-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.AuroraPostgres, "wait_stats");

        Assert.NotNull(dialect);
        Assert.Contains("aurora-01 runs Aurora PostgreSQL.", dialect, StringComparison.Ordinal);
        Assert.Contains("is written against SQL Server", dialect, StringComparison.Ordinal);
        Assert.Contains("the dispatch gate's engine half never sends it at another engine", dialect, StringComparison.Ordinal);

        var ownGate = CollectorEngineCapability.NotCollectedMessage(
            "pg-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.Postgres, "pg_wait_stats");

        Assert.NotNull(ownGate);
        Assert.Contains("pg-01 runs PostgreSQL.", ownGate, StringComparison.Ordinal);
        Assert.Contains("its own AppliesTo gate excludes it", ownGate, StringComparison.Ordinal);
        Assert.Contains("the aurora_stat_system_waits() cumulative wait counters", ownGate, StringComparison.Ordinal);
        Assert.Contains("and never will.", ownGate, StringComparison.Ordinal);

        /* The dialect sentence must NOT appear on the own-gate message: it is the specific falsehood this
           split exists to prevent. */
        Assert.DoesNotContain("is written against PostgreSQL", ownGate, StringComparison.Ordinal);
        Assert.DoesNotContain("dispatch gate's engine half", ownGate, StringComparison.Ordinal);
        Assert.DoesNotContain("check that collection is running", ownGate, StringComparison.OrdinalIgnoreCase);

        /* No EngineEdition claim about a PostgreSQL server, on either shape. */
        Assert.DoesNotContain("EngineEdition", dialect, StringComparison.Ordinal);
        Assert.DoesNotContain("EngineEdition", ownGate, StringComparison.Ordinal);

        /* And the same read on the engine that DOES have the surface says nothing at all, so the read keeps
           its own miss vocabulary. */
        Assert.Null(CollectorEngineCapability.NotCollectedMessage(
            "aurora-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.AuroraPostgres, "pg_wait_stats"));
    }

    /// <summary>
    /// The two axes compose in the right ORDER inside the message, which is the one thing neither axis can
    /// assert on its own. A PostgreSQL target's engine edition is 0 — "no claim" — so asking edition
    /// first would return null for every PostgreSQL target and the read would fall back to
    /// <c>unavailable</c>: the wrong-cause message #2530 was filed about, still there after the fix.
    /// </summary>
    [Fact]
    public void TheKindAxisIsAskedFirst_SoAPostgresTargetsZeroEditionCannotSilenceIt()
    {
        /* Edition 0 AND a known PostgreSQL kind — exactly what the store holds for an Aurora target. */
        var message = CollectorEngineCapability.NotCollectedMessage(
            "aurora-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.AuroraPostgres, "system_health_events");

        Assert.NotNull(message);
        Assert.Contains("Aurora PostgreSQL", message, StringComparison.Ordinal);
        Assert.Contains("system_health_events", message, StringComparison.Ordinal);
        Assert.Contains("is written against SQL Server", message, StringComparison.Ordinal);
        Assert.Contains("and never will.", message, StringComparison.Ordinal);

        /* The edition axis must not have spoken: its sentence names an EngineEdition number, and
           "EngineEdition 0" would be a claim about a property this server does not have. */
        Assert.DoesNotContain("EngineEdition", message, StringComparison.Ordinal);
        Assert.DoesNotContain("check that collection is running", message, StringComparison.OrdinalIgnoreCase);

        /* Same server, same edition, kind absent — the pre-rung row — keeps its old silence. */
        Assert.Null(CollectorEngineCapability.NotCollectedMessage(
            "aurora-01", CollectorEngineCapability.UnknownEngineEdition, engineKind: null, "system_health_events"));

        /* And a PostgreSQL read asked about a KNOWN SQL Server target is the mirror image, which is what
           shows the axis is about engines rather than about PostgreSQL being second-class. */
        var mirrored = CollectorEngineCapability.NotCollectedMessage(
            "box-01", 3, MonitoredEngineKind.SqlServer, "pg_wait_stats");
        Assert.NotNull(mirrored);
        Assert.Contains("box-01 runs SQL Server.", mirrored, StringComparison.Ordinal);
        Assert.Contains("is written against PostgreSQL", mirrored, StringComparison.Ordinal);

        /* A SQL Server read about a SQL Server target whose collector runs everywhere: untouched. */
        Assert.Null(CollectorEngineCapability.NotCollectedMessage(
            "box-01", 3, MonitoredEngineKind.SqlServer, "wait_stats"));
    }
}

/// <summary>
/// #2518, the converse of the sweep-dimension pin: nothing asserted that a target fact the gates READ is a
/// fact the sweep VARIES.
///
/// <para><b>The hole.</b> <c>TheSweep_VariesEveryDimensionTheGatesRead</c> walks a hand-written list of
/// dimensions and checks the sweep spans each. Add a property to <see cref="CollectorTargetInfo"/>, write a
/// gate on it, and that test still passes — the new fact sits at its CLR default across all 36 swept shapes,
/// every shape fails the new gate, and the derivation reports a permanent engine gap on every edition for a
/// collector that runs fine. Nothing is red. The message is confident, specific and wrong, which is the exact
/// defect #2511 was filed to remove.</para>
///
/// <para><b>Why this is not a list under another name.</b> Both halves are read out of the shipped artifacts.
/// The facts the gates read come from decoding the IL of every SQL Server definition's <c>AppliesTo</c> and
/// recording which <see cref="CollectorTargetInfo"/> getters it calls — so a gate cannot be written without
/// this seeing it, and a gate that stops reading a fact stops being evidence for it. The facts the sweep
/// varies come from running <see cref="CollectorEngineCapability.TargetsWithEngineEdition"/> and observing
/// the values it produces — so extending the sweep is what satisfies the check, and there is no list here to
/// add a name to instead. Neither half can be brought into line by editing this file.</para>
///
/// <para><b>What it does not catch, stated rather than assumed.</b> A field ADDED to
/// <see cref="CollectorTargetInfo"/> that no SQL Server gate reads is not flagged, because it is not yet a
/// defect: the derivation only consults the gates, so an unread fact cannot make it over-claim.
/// <c>PostgresVersionNum</c> is the shipped proof that this state exists and is fine — it is populated by the
/// connector, read by no definition at all, and harmless. The guard fires at the moment the harm becomes
/// possible, which is the moment a gate reads the fact.</para>
/// </summary>
public sealed class CollectorEngineCapabilitySweepDimensionTests
{
    /// <summary>The same contiguous edition domain the moving-gate tests use.</summary>
    private static readonly int[] AllEngineEditions = Enumerable.Range(1, 12).ToArray();

    /// <summary>Every public instance property of <see cref="CollectorTargetInfo"/>, from the type itself —
    /// so a new one is in scope the moment it compiles.</summary>
    private static readonly IReadOnlyList<PropertyInfo> TargetFacts =
        typeof(CollectorTargetInfo)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(property => property.Name, StringComparer.Ordinal)
            .ToArray();

    /// <summary>Getter name → the property it reads, for mapping a decoded <c>callvirt</c> back to a fact.</summary>
    private static readonly IReadOnlyDictionary<string, PropertyInfo> FactByGetterName =
        TargetFacts
            .Where(property => property.GetGetMethod() is not null)
            .ToDictionary(property => property.GetGetMethod()!.Name, StringComparer.Ordinal);

    /// <summary>Fact name → the property, for going back from a decoded read to its sweep role.</summary>
    private static readonly IReadOnlyDictionary<string, PropertyInfo> FactByName =
        TargetFacts.ToDictionary(property => property.Name, StringComparer.Ordinal);

    /// <summary>
    /// The two PostgreSQL tokens the store can record, which are the KIND axis's domain the way
    /// <see cref="AllEngineEditions"/> is the edition axis's (#2532). Taken from the vocabulary rather than
    /// typed out, so a third PostgreSQL flavour is in scope the moment it is added — and filtered by dialect
    /// rather than by name, so it stays right if the tokens are ever renamed.
    /// </summary>
    private static readonly string[] PostgresKinds = MonitoredEngineKind.All
        .Where(kind => MonitoredEngineKind.EngineOf(kind) == CollectorTargetEngine.PostgreSql)
        .ToArray();

    /// <summary>
    /// How a sweep treats a fact. Derived by RUNNING the sweep and looking at what it produces, never
    /// declared — a declared role would be the hand-maintained list this test exists to replace.
    /// </summary>
    private enum SweepRole
    {
        /// <summary>The sweep produces more than one value for it within a single axis value.</summary>
        VariedBySweep,

        /// <summary>Constant within one axis value, but different between them — the axis decides it.</summary>
        FixedByAxis,

        /// <summary>The engine discriminator itself. Fixed by the question rather than swept.</summary>
        EngineDiscriminator,

        /// <summary>The same value in every shape of every axis value — a gate reading it can only ever see
        /// one answer, so a gate reading it is where over-claiming begins.</summary>
        ConstantEverywhere,
    }

    /// <summary>
    /// The role a fact plays across one axis, given the shapes that axis produces for each of its values.
    /// Parameterised over the shape families rather than hard-wired to an axis, because the two axes ask the
    /// identical question of different sweeps and a second copy of this arithmetic is exactly the kind of
    /// near-duplicate that drifts (#2532).
    /// </summary>
    private static SweepRole RoleOf(PropertyInfo fact, IReadOnlyList<CollectorTargetInfo[]> shapesPerAxisValue)
    {
        /* The engine is the precondition of the whole question, not a dimension of it: "does any target of
           this engine edition / engine kind run this collector" fixes the engine by construction. Recognised
           by TYPE rather than by name, so it stays recognised if the property is ever renamed. */
        if (fact.PropertyType == typeof(CollectorTargetEngine))
        {
            return SweepRole.EngineDiscriminator;
        }

        var perValue = shapesPerAxisValue
            .Select(shapes => shapes.Select(fact.GetValue).Distinct().ToArray())
            .ToArray();

        if (perValue.Any(values => values.Length > 1))
        {
            return SweepRole.VariedBySweep;
        }

        return perValue.Select(values => values[0]).Distinct().Count() > 1
            ? SweepRole.FixedByAxis
            : SweepRole.ConstantEverywhere;
    }

    /// <summary>The shapes the EDITION sweep produces, one array per engine edition.</summary>
    private static CollectorTargetInfo[][] EditionShapes() => AllEngineEditions
        .Select(edition => CollectorEngineCapability.TargetsWithEngineEdition(edition).ToArray())
        .ToArray();

    /// <summary>The shapes the KIND sweep produces, one array per PostgreSQL token.</summary>
    private static CollectorTargetInfo[][] PostgresKindShapes() => PostgresKinds
        .Select(kind => CollectorEngineCapability.TargetsWithEngineKind(kind).ToArray())
        .ToArray();

    private static SweepRole EditionRoleOf(PropertyInfo fact) => RoleOf(fact, EditionShapes());

    private static SweepRole PostgresKindRoleOf(PropertyInfo fact) => RoleOf(fact, PostgresKindShapes());

    /// <summary>fact name → the SQL Server collectors whose <c>AppliesTo</c> reads it, decoded from IL.</summary>
    private static SortedDictionary<string, SortedSet<string>> FactsReadBySqlServerGates() =>
        FactsReadByGatesOf(CollectorTargetEngine.SqlServer);

    /// <summary>The same, for the PostgreSQL definitions — the whole difference between the two guards, as
    /// #2532 said it would be.</summary>
    private static SortedDictionary<string, SortedSet<string>> FactsReadByPostgresGates() =>
        FactsReadByGatesOf(CollectorTargetEngine.PostgreSql);

    private static SortedDictionary<string, SortedSet<string>> FactsReadByGatesOf(CollectorTargetEngine engine)
    {
        var byFact = new SortedDictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        foreach (var definition in CollectorCatalog.All.Where(c => c.TargetEngine == engine))
        {
            foreach (var fact in FactsReadByGateOf(definition))
            {
                if (!byFact.TryGetValue(fact, out var collectors))
                {
                    byFact[fact] = collectors = new SortedSet<string>(StringComparer.Ordinal);
                }

                collectors.Add(definition.Name);
            }
        }

        return byFact;
    }

    /// <summary>The <see cref="CollectorTargetInfo"/> facts one definition's gate reads, following calls into
    /// other collector-assembly code so a gate refactored behind a helper cannot hide what it reads.</summary>
    private static SortedSet<string> FactsReadByGateOf(ICollectorSchemaInfo definition)
    {
        var gate = definition.GetType().GetMethod(
            nameof(ICollectorSchemaInfo.AppliesTo),
            BindingFlags.Public | BindingFlags.Instance,
            binder: null,
            types: new[] { typeof(CollectorTargetInfo) },
            modifiers: null);

        Assert.True(gate is not null, $"{definition.Name}: no AppliesTo(CollectorTargetInfo) to decode");

        var facts = new SortedSet<string>(StringComparer.Ordinal);
        CollectFactReads(gate!, new HashSet<MethodBase>(), facts);
        return facts;
    }

    private static void CollectFactReads(MethodBase method, HashSet<MethodBase> visited, SortedSet<string> facts)
    {
        if (!visited.Add(method))
        {
            return;
        }

        foreach (var callee in CalleesOf(method))
        {
            if (callee.DeclaringType == typeof(CollectorTargetInfo))
            {
                if (FactByGetterName.TryGetValue(callee.Name, out var fact))
                {
                    facts.Add(fact.Name);
                }

                continue;
            }

            /* Follow only into code this repo ships. Walking the framework would never terminate usefully,
               and a gate cannot read a target fact through a BCL call anyway — the fact only exists here. */
            if (callee.DeclaringType?.Assembly == typeof(CollectorTargetInfo).Assembly)
            {
                CollectFactReads(callee, visited, facts);
            }
        }
    }

    /// <summary>
    /// Every opcode the runtime defines, keyed by its encoded value — taken from
    /// <see cref="OpCodes"/> by reflection rather than typed out, so the operand-size table below cannot
    /// drift from the instruction set it is decoding.
    /// </summary>
    private static readonly IReadOnlyDictionary<short, OpCode> Opcodes =
        typeof(OpCodes)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field.FieldType == typeof(OpCode))
            .Select(field => (OpCode)field.GetValue(null)!)
            .GroupBy(opcode => opcode.Value)
            .ToDictionary(group => group.Key, group => group.First());

    /// <summary>
    /// The methods a method calls, by walking its IL stream instruction by instruction.
    ///
    /// <para><b>Decoded properly, not scanned for byte patterns.</b> Searching the stream for a
    /// <c>call</c>/<c>callvirt</c> byte would match those values inside other instructions' operands, and a
    /// four-byte slice of an operand can resolve to a perfectly valid, entirely unrelated method token — a
    /// guard that reports facts nobody reads, in a file whose whole subject is guards that stop guarding.
    /// Advancing by each opcode's real operand size is the only way to know a token is a token.</para>
    /// </summary>
    private static IReadOnlyList<MethodBase> CalleesOf(MethodBase method)
    {
        var callees = new List<MethodBase>();
        var il = method.GetMethodBody()?.GetILAsByteArray();
        if (il is null)
        {
            return callees;
        }

        var typeArguments = method.DeclaringType?.GetGenericArguments();
        var methodArguments = method.IsGenericMethod ? method.GetGenericArguments() : null;

        var position = 0;
        while (position < il.Length)
        {
            var value = il[position] == 0xFE && position + 1 < il.Length
                ? unchecked((short)(0xFE00 | il[position + 1]))
                : (short)il[position];

            Assert.True(
                Opcodes.TryGetValue(value, out var opcode),
                $"{method.DeclaringType?.Name}.{method.Name}: undecodable opcode 0x{value:X4} at IL offset " +
                $"{position}. The stream is being mis-walked from here on, and a mis-walked stream reads " +
                "operand bytes as call tokens — stop rather than report facts that were never read.");

            position += opcode.Size;
            var operandSize = OperandSize(opcode, il, position);

            if (opcode.OperandType is OperandType.InlineMethod or OperandType.InlineTok)
            {
                var token = BitConverter.ToInt32(il, position);
                try
                {
                    if (method.Module.ResolveMethod(token, typeArguments, methodArguments) is { } callee)
                    {
                        callees.Add(callee);
                    }
                }
                catch (ArgumentException)
                {
                    /* InlineTok also carries field and type handles; those are not calls. */
                }
            }

            position += operandSize;
        }

        return callees;
    }

    private static int OperandSize(OpCode opcode, byte[] il, int operandStart) => opcode.OperandType switch
    {
        OperandType.InlineNone => 0,
        OperandType.ShortInlineBrTarget or OperandType.ShortInlineI or OperandType.ShortInlineVar => 1,
        OperandType.InlineVar => 2,
        OperandType.InlineBrTarget or OperandType.InlineField or OperandType.InlineI
            or OperandType.InlineMethod or OperandType.InlineSig or OperandType.InlineString
            or OperandType.InlineTok or OperandType.InlineType or OperandType.ShortInlineR => 4,
        OperandType.InlineI8 or OperandType.InlineR => 8,
        OperandType.InlineSwitch => 4 + (4 * BitConverter.ToInt32(il, operandStart)),
        _ => throw new NotSupportedException($"unhandled operand type {opcode.OperandType} for {opcode.Name}"),
    };

    /// <summary>
    /// THE assertion. Every <see cref="CollectorTargetInfo"/> fact a SQL Server gate reads is a fact the
    /// sweep either varies or fixes by edition — so no gate can be answered by a single defaulted value.
    ///
    /// <para>A gate written on a fact the sweep leaves at its default fails EVERY swept shape, and the
    /// derivation then reports a permanent, unfixable engine gap for a collector that runs. This is the only
    /// check in the suite that fires on that, and it fires at the moment the gate is written rather than the
    /// day someone notices a read has gone quiet.</para>
    /// </summary>
    [Fact]
    public void EveryFactASqlServerGateReads_IsVariedBySweepOrFixedByEdition()
    {
        var read = FactsReadBySqlServerGates();

        var overClaiming = read
            .Where(entry => EditionRoleOf(FactByName[entry.Key]) == SweepRole.ConstantEverywhere)
            .Select(entry => $"  {entry.Key} — read by {string.Join(", ", entry.Value)}")
            .ToArray();

        Assert.True(
            overClaiming.Length == 0,
            "A SQL Server collector's AppliesTo gate reads a CollectorTargetInfo fact that " +
            "CollectorEngineCapability.TargetsWithEngineEdition never varies. Every swept target therefore " +
            "carries that fact's default, every one of them fails the gate, and the capability derivation " +
            "reports a PERMANENT engine gap on every edition for a collector that runs perfectly well — the " +
            "confident-and-wrong message #2511 exists to delete.\n\n" +
            "Fix the SWEEP, not this test: add the fact to TargetsWithEngineEdition (varying it, or deriving " +
            "it from the engine edition the way the two Azure flags are). There is no list here to add a name " +
            "to.\n\n" +
            string.Join("\n", overClaiming));
    }

    /// <summary>
    /// The scan is only worth anything if it reads the real gates, so pin what it finds against what the
    /// source plainly says — including a gate that reads NOTHING, which is what shows the decoder
    /// discriminates rather than reporting every fact for every collector.
    ///
    /// <para>A source-parsing guard that matches nothing passes for free and is worse than no guard at all:
    /// it converts an open question into false confidence. That has happened in this repo more than once, so
    /// the floor is asserted here rather than assumed.</para>
    /// </summary>
    [Fact]
    public void TheGateScan_ReadsTheGatesTheSourceActuallyCarries()
    {
        var sqlServerDefinitions = CollectorCatalog.All
            .Where(c => c.TargetEngine == CollectorTargetEngine.SqlServer)
            .ToArray();

        Assert.True(
            sqlServerDefinitions.Length >= 30,
            $"only {sqlServerDefinitions.Length} SQL Server definitions found — the catalog walk is broken, not the catalog");

        var read = FactsReadBySqlServerGates();

        /* The facts today's gates are written on. Not an expected-set pin — a floor, so a decoder that
           silently returned nothing cannot pass. */
        Assert.Contains(nameof(CollectorTargetInfo.IsAzureSqlDb), read.Keys);
        Assert.Contains(nameof(CollectorTargetInfo.IsAzureManagedInstance), read.Keys);
        Assert.Contains(nameof(CollectorTargetInfo.IsAwsRds), read.Keys);
        Assert.Contains(nameof(CollectorTargetInfo.SqlMajorVersion), read.Keys);

        var byName = CollectorCatalog.All.ToDictionary(c => c.Name, StringComparer.Ordinal);

        /* The collector the whole feature was filed on: one fact, exactly the one its source names. */
        Assert.Equal(
            new[] { nameof(CollectorTargetInfo.IsAzureSqlDb) },
            FactsReadByGateOf(byName["system_health_events"]).ToArray());

        /* Two facts in one gate, so the decoder is not stopping at the first call it finds. This read three
           until #2559 removed HasMsdbAccess from it — msdb access is a grant rather than an engine
           capability, so the collector attempts and fails into PERMISSIONS instead of gating off a probe
           cached for the connection's life. */
        Assert.Equal(
            new[]
            {
                nameof(CollectorTargetInfo.IsAwsRds),
                nameof(CollectorTargetInfo.IsAzureSqlDb),
            },
            FactsReadByGateOf(byName["running_jobs"]).ToArray());

        /* And a gate that reads nothing at all. Without this, "reports every fact for every collector" would
           satisfy every assertion above. */
        Assert.Empty(FactsReadByGateOf(byName["wait_stats"]));
    }

    /// <summary>
    /// The sweep is the FULL cross product of the dimensions it varies, not a sample of them.
    ///
    /// <para>The claim the derivation makes is "there is no target of this engine edition, under any
    /// COMBINATION of the other facts, for which this collector runs". A sweep that varied each dimension but
    /// only along one axis at a time would satisfy
    /// <c>TheSweep_VariesEveryDimensionTheGatesRead</c> while missing combinations entirely — and a
    /// conjunctive gate (<c>running_jobs</c> and <c>agent_status</c> are both conjunctions of three facts)
    /// would be reported as a permanent gap because the one shape that passes it was never generated.</para>
    ///
    /// <para>Both sides are measured from the sweep's own output: the dimensions are the facts it varies, the
    /// expected shape count is the product of the distinct values it produced for each. Nothing here says how
    /// many dimensions there should be, so adding one needs no edit — only keeping it exhaustive does.</para>
    /// </summary>
    [Fact]
    public void TheSweep_IsTheFullCrossProductOfTheDimensionsItVaries()
    {
        foreach (var edition in AllEngineEditions)
        {
            var shapes = CollectorEngineCapability.TargetsWithEngineEdition(edition).ToArray();
            var varied = TargetFacts.Where(fact => EditionRoleOf(fact) == SweepRole.VariedBySweep).ToArray();

            Assert.NotEmpty(varied);

            var expected = varied.Aggregate(
                1,
                (total, fact) => total * shapes.Select(fact.GetValue).Distinct().Count());

            Assert.Equal(expected, shapes.Length);

            /* Same size AND no repeats means every combination appears exactly once. Size alone would be
               satisfied by a sweep that emitted one shape twice and skipped another. */
            var fingerprints = shapes
                .Select(shape => string.Join("|", varied.Select(fact => fact.GetValue(shape))))
                .ToArray();

            Assert.Equal(shapes.Length, fingerprints.Distinct(StringComparer.Ordinal).Count());
        }
    }

    /// <summary>
    /// Every fact on <see cref="CollectorTargetInfo"/> lands in exactly one derived role, and the roles are
    /// non-degenerate: something is varied, something is fixed by edition, and there is exactly one engine
    /// discriminator.
    ///
    /// <para>This is the whole-type view the per-gate check above cannot give. If
    /// <see cref="CollectorEngineCapability.TargetsWithEngineEdition"/> ever collapsed — one shape per
    /// edition, or the Azure flags stopped following the edition — every fact would slide into
    /// <see cref="SweepRole.ConstantEverywhere"/> and the derivation would answer every question from a
    /// single target. The gap set would still look plausible; it would just have stopped being derived from
    /// anything.</para>
    /// </summary>
    [Fact]
    public void EveryTargetFact_LandsInExactlyOneDerivedRole()
    {
        var roles = TargetFacts.ToDictionary(fact => fact.Name, EditionRoleOf, StringComparer.Ordinal);

        Assert.NotEmpty(roles);
        Assert.Single(roles, role => role.Value == SweepRole.EngineDiscriminator);
        Assert.Contains(roles, role => role.Value == SweepRole.VariedBySweep);
        Assert.Contains(roles, role => role.Value == SweepRole.FixedByAxis);

        /* The two Azure flags are the edition-fixed pair, and they follow the edition in OPPOSITE directions
           — asserted here because "fixed by edition" is otherwise satisfied by a flag that is fixed at the
           wrong value. */
        var azureShapes = CollectorEngineCapability.TargetsWithEngineEdition(
            CollectorEngineCapability.AzureSqlDatabaseEngineEdition).ToArray();
        var miShapes = CollectorEngineCapability.TargetsWithEngineEdition(
            CollectorEngineCapability.AzureManagedInstanceEngineEdition).ToArray();

        Assert.All(azureShapes, shape => Assert.True(shape.IsAzureSqlDb && !shape.IsAzureManagedInstance));
        Assert.All(miShapes, shape => Assert.True(shape.IsAzureManagedInstance && !shape.IsAzureSqlDb));
    }

    /* ───────── the same guard, one engine over (#2532) ───────── */

    /// <summary>
    /// THE assertion, PostgreSQL side. Every <see cref="CollectorTargetInfo"/> fact a PostgreSQL gate reads
    /// is a fact <see cref="CollectorEngineCapability.TargetsWithEngineKind"/> either varies or fixes by the
    /// kind — so no PostgreSQL gate can be answered by a single defaulted value.
    ///
    /// <para><b>Why this had to land before the axis did.</b> #2530 deliberately shipped only the ENGINE half
    /// of the dispatch gate on this axis, because a sweep without this guard over-claims silently: a fact the
    /// sweep leaves at its CLR default fails every shape, and the derivation announces a permanent,
    /// unfixable engine gap for a collector that runs perfectly well — on the very engine whose support this
    /// work exists to make credible. #2532 made that the explicit prerequisite, and this is it.</para>
    ///
    /// <para><b>Nothing here is a list.</b> The facts come out of the gates' IL (the same decoder the edition
    /// half uses, filtered to the PostgreSQL definitions) and the roles come out of running the sweep. Fix
    /// the SWEEP, never this test — there is no name to add here instead.</para>
    /// </summary>
    [Fact]
    public void EveryFactAPostgresGateReads_IsVariedBySweepOrFixedByKind()
    {
        var read = FactsReadByPostgresGates();

        var overClaiming = read
            .Where(entry => PostgresKindRoleOf(FactByName[entry.Key]) == SweepRole.ConstantEverywhere)
            .Select(entry => $"  {entry.Key} — read by {string.Join(", ", entry.Value)}")
            .ToArray();

        Assert.True(
            overClaiming.Length == 0,
            "A PostgreSQL collector's AppliesTo gate reads a CollectorTargetInfo fact that " +
            "CollectorEngineCapability.TargetsWithEngineKind never varies. Every swept target therefore " +
            "carries that fact's default, every one of them fails the gate, and the capability derivation " +
            "reports a PERMANENT engine gap on both PostgreSQL tokens for a collector that runs perfectly " +
            "well — the confident-and-wrong message #2511 exists to delete, one engine over.\n\n" +
            "Fix the SWEEP, not this test: add the fact to TargetsWithEngineKind (varying it, or deriving it " +
            "from the engine kind the way IsAurora is). There is no list here to add a name to.\n\n" +
            string.Join("\n", overClaiming));
    }

    /// <summary>
    /// The PostgreSQL half of the scan reads the gates the source actually carries — the same non-vacuity
    /// floor the SQL Server half has, and for the same reason: a source-decoding guard that matches nothing
    /// passes for free and converts an open question into false confidence.
    /// </summary>
    [Fact]
    public void ThePostgresGateScan_ReadsTheGatesTheSourceActuallyCarries()
    {
        var definitions = CollectorCatalog.All
            .Where(c => c.TargetEngine == CollectorTargetEngine.PostgreSql)
            .ToArray();

        Assert.True(
            definitions.Length >= 8,
            $"only {definitions.Length} PostgreSQL definitions found — the catalog walk is broken, not the catalog");

        var read = FactsReadByPostgresGates();

        /* The three facts today's PostgreSQL gates are written on. A floor, not an expected-set pin. */
        Assert.Contains(nameof(CollectorTargetInfo.IsAurora), read.Keys);
        Assert.Contains(nameof(CollectorTargetInfo.IsInRecovery), read.Keys);
        Assert.Contains(nameof(CollectorTargetInfo.PostgresMajorVersion), read.Keys);

        var byName = CollectorCatalog.All.ToDictionary(c => c.Name, StringComparer.Ordinal);

        /* The collector #2532 was filed on: one fact, exactly the one its source names. */
        Assert.Equal(
            new[] { nameof(CollectorTargetInfo.IsAurora) },
            FactsReadByGateOf(byName["pg_wait_stats"]).ToArray());

        /* A version floor and a recovery gate, so the decoder is discriminating rather than reporting
           IsAurora for everything. */
        Assert.Equal(
            new[] { nameof(CollectorTargetInfo.PostgresMajorVersion) },
            FactsReadByGateOf(byName["pg_io_stats"]).ToArray());

        Assert.Equal(
            new[] { nameof(CollectorTargetInfo.IsInRecovery) },
            FactsReadByGateOf(byName["pg_autovacuum_stats"]).ToArray());

        /* And a gate that reads nothing at all — without this, "reports every fact for every collector"
           would satisfy every assertion above. */
        Assert.Empty(FactsReadByGateOf(byName["pg_blocking"]));

        /* No SQL Server fact leaks into the PostgreSQL scan. The two halves differ only by the filter, so a
           filter that stopped filtering would look exactly like a guard that was working. */
        Assert.DoesNotContain(nameof(CollectorTargetInfo.IsAzureSqlDb), read.Keys);
        Assert.DoesNotContain(nameof(CollectorTargetInfo.SqlMajorVersion), read.Keys);

        /* HasMsdbAccess was a third assertion here and has been REMOVED rather than left passing. Since
           #2559 no SQL Server gate reads it either, so the line could no longer fail for the reason it was
           written — a filter that stopped filtering would still not have surfaced it. A pin that cannot go
           red is worse than no pin, because it reads as coverage. The two facts left are both genuinely
           SQL-Server-only and still discriminate. */
    }

    /// <summary>
    /// The kind sweep is the FULL cross product of the dimensions it varies, for the reason the edition
    /// sweep is: the claim being made is "no target of this kind, under any COMBINATION of the other facts",
    /// and a sweep that moved one axis at a time would report a conjunctive gate as a permanent gap because
    /// the one shape that satisfies it was never generated.
    /// </summary>
    [Fact]
    public void TheKindSweep_IsTheFullCrossProductOfTheDimensionsItVaries()
    {
        Assert.NotEmpty(PostgresKinds);

        foreach (var kind in PostgresKinds)
        {
            var shapes = CollectorEngineCapability.TargetsWithEngineKind(kind).ToArray();
            var varied = TargetFacts.Where(fact => PostgresKindRoleOf(fact) == SweepRole.VariedBySweep).ToArray();

            Assert.NotEmpty(varied);
            Assert.All(shapes, shape => Assert.Equal(CollectorTargetEngine.PostgreSql, shape.Engine));

            var expected = varied.Aggregate(
                1,
                (total, fact) => total * shapes.Select(fact.GetValue).Distinct().Count());

            Assert.Equal(expected, shapes.Length);

            var fingerprints = shapes
                .Select(shape => string.Join("|", varied.Select(fact => fact.GetValue(shape))))
                .ToArray();

            Assert.Equal(shapes.Length, fingerprints.Distinct(StringComparer.Ordinal).Count());
        }
    }

    /// <summary>
    /// Every fact lands in exactly one derived role on the KIND axis too, and the roles are non-degenerate:
    /// <see cref="CollectorTargetInfo.IsAurora"/> is the one the kind fixes — in OPPOSITE directions for the
    /// two tokens, so "fixed by the kind" is not satisfied by a flag stuck at the wrong value — and the
    /// version and recovery facts are the ones it varies.
    ///
    /// <para>A kind sweep that collapsed to one shape per token would slide every fact into
    /// <see cref="SweepRole.ConstantEverywhere"/> and answer every question from a single target, with a gap
    /// set that still looked plausible. This is the whole-type view that says so.</para>
    /// </summary>
    [Fact]
    public void EveryTargetFact_LandsInExactlyOneDerivedKindRole()
    {
        var roles = TargetFacts.ToDictionary(fact => fact.Name, PostgresKindRoleOf, StringComparer.Ordinal);

        Assert.NotEmpty(roles);
        Assert.Single(roles, role => role.Value == SweepRole.EngineDiscriminator);
        Assert.Contains(roles, role => role.Value == SweepRole.VariedBySweep);

        Assert.Equal(SweepRole.FixedByAxis, roles[nameof(CollectorTargetInfo.IsAurora)]);
        Assert.Equal(SweepRole.VariedBySweep, roles[nameof(CollectorTargetInfo.IsInRecovery)]);
        Assert.Equal(SweepRole.VariedBySweep, roles[nameof(CollectorTargetInfo.PostgresMajorVersion)]);
        Assert.Equal(SweepRole.VariedBySweep, roles[nameof(CollectorTargetInfo.PostgresVersionNum)]);

        var stock = CollectorEngineCapability.TargetsWithEngineKind(MonitoredEngineKind.Postgres).ToArray();
        var aurora = CollectorEngineCapability.TargetsWithEngineKind(MonitoredEngineKind.AuroraPostgres).ToArray();

        Assert.NotEmpty(stock);
        Assert.NotEmpty(aurora);
        Assert.All(stock, shape => Assert.False(shape.IsAurora));
        Assert.All(aurora, shape => Assert.True(shape.IsAurora));
    }

    /// <summary>
    /// A kind this build does not recognise produces NO shapes, and the capability answer treats that as
    /// silence rather than as "no shape runs it".
    ///
    /// <para>This is the one place an empty sweep would be catastrophic instead of merely wrong: read as a
    /// gap set, an empty sweep makes every collector a permanent gap on every unknown token — which is
    /// exactly the row a store written by a NEWER service leaves behind, and exactly the guarantee #2530
    /// was told to keep.</para>
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("something-a-newer-build-writes")]
    public void AnUnrecognisedKind_SweepsNothing_AndStillClaimsNothing(string? engineKind)
    {
        Assert.Empty(CollectorEngineCapability.TargetsWithEngineKind(engineKind));

        Assert.All(
            CollectorCatalog.All,
            definition => Assert.True(
                CollectorEngineCapability.IsCollectedOnEngineKind(definition, engineKind),
                $"{definition.Name} was claimed as a permanent gap on an unrecognised engine kind"));
    }
}
