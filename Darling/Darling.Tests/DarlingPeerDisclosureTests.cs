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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the declared-peer disclosure (#2339, tier 1): the <c>peers</c> config block, the MCP-instructions
/// coverage section, <c>list_servers</c>' <c>peer_fleets</c> block, and the server-resolution miss message.
///
/// <para><b>The invariant that matters most is the negative one.</b> With nothing declared — every existing
/// deployment, and the shipped sample — the PROSE surfaces must be byte-for-byte what they were, because this
/// feature is disclosure bolted onto paths that ~90 MCP tools and the whole web read dispatch already go
/// through. Three of the four are: the MCP instructions (<see cref="DarlingMcpInstructions.Build"/> returns
/// the same reference), the server-resolution miss (pinned by exact string equality), and
/// <c>list_servers</c>' empty-registry sentence. Each gets a paired test: what it says with peers, and that
/// it says nothing extra without them.</para>
///
/// <para><b>The one deliberate exception is <c>list_servers</c>' JSON envelope</b>, which gains
/// <c>this_store_covers</c> / <c>peer_fleets</c> / <c>peer_note</c> on EVERY response, declared or not — so a
/// client doing an exact-shape comparison on that tool sees three new keys on upgrade even if it never
/// touches the <c>peers</c> config. That is the point rather than an oversight: an empty <c>peer_fleets</c>
/// means either "this is the only store" or "nobody declared the siblings", and an agent can only be told
/// the difference is unknowable if the note is there when the list is empty. A conditional block would say
/// nothing in exactly the case that produces the wrong conclusion. Called out here, and in the CHANGELOG,
/// rather than folded into the unchanged claim (raised in review on #2339).</para>
///
/// <para>All of it is pure over an explicit <see cref="DarlingPeerDirectory.Snapshot"/> except the one test
/// that exercises the ambient publish, which restores <see cref="DarlingPeerDirectory.Reset"/> in a finally.
/// A concurrently-running test in another collection could observe a published snapshot for that instant;
/// the only effect would be APPENDED text on a resolution miss, which no assertion in the suite is sensitive
/// to (they all use StartsWith/Contains) — which is itself the reason the disclosure is additive.</para>
/// </summary>
public sealed class DarlingPeerDisclosureTests
{
    private const string Use1Covers = "the 42 us-east-1 SQL Server primaries";

    private static DarlingPeerDirectory.Snapshot TwoPeers() =>
        DarlingPeerDirectory.FromConfig(new PeersConfig
        {
            ThisStoreCovers = Use1Covers,
            Stores =
            {
                new PeerStoreConfig
                {
                    Name = "prod-sql-use2-monitor-01",
                    Covers = "the readable replicas of those same 42 primaries, from us-east-2",
                    Matches = { "use2" },
                },
                new PeerStoreConfig
                {
                    Name = "prod-sql-pg-monitor-01",
                    Covers = "the Aurora PostgreSQL clusters",
                    /* No matches: a peer that declares none is still disclosed, just never singled out. */
                },
            },
        });

    private static DarlingServerResolver.RegisteredServer Registered(string storageName, string? displayName = null) =>
        new(storageName.GetHashCode(StringComparison.Ordinal), storageName, displayName ?? storageName);

    /* ───────────────────────── the config block ───────────────────────── */

    [Fact]
    public void PeersBlock_ParsesFromDarlingJson()
    {
        var config = DarlingConfig.Parse("""
            {
              "postgres": { "connectionString": "Host=localhost;Database=darling" },
              "servers": [ { "host": "SQL2022" } ],
              "peers": {
                "thisStoreCovers": "the 42 us-east-1 SQL Server primaries",
                "stores": [
                  { "name": "box2", "covers": "their readable replicas", "matches": ["use2", "-ro"] }
                ]
              }
            }
            """);

        Assert.Equal(Use1Covers, config.Peers.ThisStoreCovers);
        var peer = Assert.Single(config.Peers.Stores);
        Assert.Equal("box2", peer.Name);
        Assert.Equal("their readable replicas", peer.Covers);
        Assert.Equal(new[] { "use2", "-ro" }, peer.Matches);
        Assert.DoesNotContain(config.Validate(), p => p.Contains("peer", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void PeersBlock_IsOptional_AndAbsenceDeclaresNothing()
    {
        /* No "peers" key at all — the shape every existing darling.json has. */
        var config = DarlingConfig.Parse("""
            {
              "postgres": { "connectionString": "Host=localhost;Database=darling" },
              "servers": [ { "host": "SQL2022" } ]
            }
            """);

        Assert.Empty(config.Peers.Stores);
        Assert.True(DarlingPeerDirectory.FromConfig(config.Peers).IsEmpty);
        Assert.True(DarlingPeerDirectory.FromConfig(null).IsEmpty);
    }

    [Fact]
    public void SampleConfig_ShipsThePeersBlockDeclaringNothing()
    {
        /* The sample documents the block but must not declare fictional peers: a shipped declaration would
           be a lie told to every agent that connects to a fresh install. */
        var samplePath = System.IO.Path.Combine(AppContext.BaseDirectory, "darling.sample.json");
        var config = DarlingConfig.Parse(System.IO.File.ReadAllText(samplePath));

        Assert.NotNull(config.Peers);
        Assert.Empty(config.Peers.Stores);
        Assert.True(DarlingPeerDirectory.FromConfig(config.Peers).IsEmpty);
        Assert.Empty(PeersConfig.Validate(config.Peers));
    }

    [Fact]
    public void Validate_RequiresAPeerName()
    {
        /* A peer with only a description tells an agent "some other store has it" with nothing to point a
           human at — no better off than the bare not-found this whole feature replaces. */
        var problems = PeersConfig.Validate(new PeersConfig
        {
            Stores = { new PeerStoreConfig { Covers = "the replicas" } },
        });

        Assert.Contains(problems, p => p.Contains("name is required", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_AllowsAPeerWithNoCoversSentence()
    {
        /* Half a disclosure still names an endpoint, so a name-only peer is legal (and renders as the name). */
        Assert.Empty(PeersConfig.Validate(new PeersConfig
        {
            Stores = { new PeerStoreConfig { Name = "box2" } },
        }));
    }

    [Theory]
    [InlineData("Host=box2;Password=hunter2")]
    [InlineData("see its connectionString")]
    [InlineData("Server=x;Integrated Security=true")]
    public void Validate_RefusesCredentialShapedPeerText(string covers)
    {
        /* Everything in this block is sent verbatim to every connected MCP client, so failing OPEN here would
           broadcast the secret — the one place where refusing to start is the proportionate response. */
        var inCovers = PeersConfig.Validate(new PeersConfig
        {
            Stores = { new PeerStoreConfig { Name = "box2", Covers = covers } },
        });
        Assert.Contains(inCovers, p => p.Contains("DISCLOSURE ONLY", StringComparison.Ordinal));

        /* The same guard covers this store's own sentence and the match patterns — every disclosed string. */
        Assert.Contains(
            PeersConfig.Validate(new PeersConfig { ThisStoreCovers = covers }),
            p => p.Contains("DISCLOSURE ONLY", StringComparison.Ordinal));

        Assert.Contains(
            PeersConfig.Validate(new PeersConfig
            {
                Stores = { new PeerStoreConfig { Name = "box2", Matches = { covers } } },
            }),
            p => p.Contains("DISCLOSURE ONLY", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ChecksThisStoreCovers_EvenWhenStoresIsExplicitJsonNull()
    {
        /* Review finding on #2339. System.Text.Json assigns null OVER the property initializer for an
           explicit "stores": null (an omitted key leaves the default empty list), and the thisStoreCovers
           guard used to sit after the per-peer loop behind an early `Stores is null` return — so this exact
           config validated clean and then broadcast the credential through the instructions,
           list_servers' this_store_covers, and every resolution miss. */
        var config = DarlingConfig.Parse("""
            {
              "postgres": { "connectionString": "Host=localhost;Database=darling" },
              "servers": [ { "host": "SQL2022" } ],
              "peers": { "thisStoreCovers": "internal db, see Password=hunter2", "stores": null }
            }
            """);

        Assert.Null(config.Peers.Stores);
        Assert.Contains(
            PeersConfig.Validate(config.Peers),
            p => p.Contains("DISCLOSURE ONLY", StringComparison.Ordinal));
        Assert.Contains(config.Validate(), p => p.Contains("DISCLOSURE ONLY", StringComparison.Ordinal));

        /* And nothing reaches the ambient snapshot, so no surface can disclose it. */
        try
        {
            var published = DarlingPeerDirectory.Publish(config.Peers);
            Assert.True(published.Refused);
            Assert.True(published.Snapshot.IsEmpty);
            Assert.True(DarlingPeerDirectory.Current.IsEmpty);
        }
        finally
        {
            DarlingPeerDirectory.Reset();
        }
    }

    [Fact]
    public void Publish_RefusesTheWholeBlockOnAnyValidationProblem()
    {
        /* Review finding on #2339: the credential guard lived only in DarlingConfig.Validate, which the
           WORKER runs — but the worker's abort is a return from its own hosted service, not a process exit,
           and the MCP host loads its own config and deliberately never calls Validate. So the one path that
           actually broadcasts peer text was the one path the guard never covered. Validating inside Publish
           makes it structural: the ambient snapshot can only be written through here. */
        try
        {
            var leak = DarlingPeerDirectory.Publish(new PeersConfig
            {
                ThisStoreCovers = Use1Covers,
                Stores =
                {
                    new PeerStoreConfig { Name = "good", Covers = "the replicas", Matches = { "use2" } },
                    new PeerStoreConfig { Name = "bad", Covers = "Host=x;Password=hunter2" },
                },
            });

            Assert.True(leak.Refused);
            Assert.Contains(leak.RefusedProblems, p => p.Contains("DISCLOSURE ONLY", StringComparison.Ordinal));

            /* The WHOLE block, not the valid subset: a peers block that failed validation is one the
               operator has not finished, and half a disclosure would state coverage that may be wrong while
               the log says the config is broken. */
            Assert.True(leak.Snapshot.IsEmpty);
            Assert.True(DarlingPeerDirectory.Current.IsEmpty);

            /* A nameless peer is not a secret, but it is still an unfinished block — same refusal. */
            Assert.True(DarlingPeerDirectory
                .Publish(new PeersConfig { Stores = { new PeerStoreConfig { Covers = "the replicas" } } })
                .Refused);
            Assert.True(DarlingPeerDirectory.Current.IsEmpty);

            /* A valid block publishes, and reports no problems. */
            var ok = DarlingPeerDirectory.Publish(new PeersConfig
            {
                ThisStoreCovers = Use1Covers,
                Stores = { new PeerStoreConfig { Name = "box2", Covers = "the replicas" } },
            });

            Assert.False(ok.Refused);
            Assert.Empty(ok.RefusedProblems);
            Assert.Same(ok.Snapshot, DarlingPeerDirectory.Current);
        }
        finally
        {
            DarlingPeerDirectory.Reset();
        }
    }

    [Fact]
    public void Validate_PeerProblemsSurfaceThroughTheWholeConfigValidate()
    {
        /* Reported even on a config with no servers — the peers check runs BEFORE that early return. */
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig { ConnectionString = "Host=localhost;Database=darling" },
            Peers = new PeersConfig { Stores = { new PeerStoreConfig { Covers = "the replicas" } } },
        };

        var problems = config.Validate();
        Assert.Contains(problems, p => p.Contains("name is required", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.Contains("servers must contain at least one entry", StringComparison.Ordinal));
    }

    /* ───────────────────────── normalization + matching ───────────────────────── */

    [Fact]
    public void FromConfig_TrimsAndDropsEmptyEntriesAndBlankPatterns()
    {
        var snapshot = DarlingPeerDirectory.FromConfig(new PeersConfig
        {
            ThisStoreCovers = "  the primaries  ",
            Stores =
            {
                new PeerStoreConfig { Name = "  box2  ", Covers = "  the replicas  ", Matches = { " use2 ", "", "   " } },
                new PeerStoreConfig(),   /* an empty object in the array is a typo, not a peer */
            },
        });

        Assert.Equal("the primaries", snapshot.ThisStoreCovers);
        var peer = Assert.Single(snapshot.Peers);
        Assert.Equal("box2", peer.Name);
        Assert.Equal("the replicas", peer.Covers);

        /* A blank pattern is a substring of EVERY name, so keeping one would make this peer claim the whole
           fleet — the one normalization step that is a correctness fix rather than tidiness. */
        Assert.Equal(new[] { "use2" }, peer.Matches);
        Assert.False(peer.CoversServerName("anything-at-all"));
        Assert.True(peer.CoversServerName("prod-sql-USE2-alpha-01"));
    }

    [Fact]
    public void CoversServerName_IsCaseInsensitiveSubstring_AndNeverTrueWithoutPatterns()
    {
        var snapshot = TwoPeers();
        var use2 = snapshot.Peers[0];
        var postgres = snapshot.Peers[1];

        Assert.True(use2.CoversServerName("prod-sql-use2-beta-01"));
        Assert.True(use2.CoversServerName("PROD-SQL-USE2-GAMMA-01"));
        Assert.False(use2.CoversServerName("prod-sql-use1-beta-01"));
        Assert.False(use2.CoversServerName(null));
        Assert.False(use2.CoversServerName("   "));

        /* No declared patterns means "cannot tell", which must never render as "yes". */
        Assert.False(postgres.CoversServerName("anything"));

        Assert.Equal(new[] { "prod-sql-use2-monitor-01" },
            snapshot.PeersCovering("prod-sql-use2-beta-01").Select(p => p.Name));
        Assert.Empty(snapshot.PeersCovering("prod-sql-use1-beta-01"));
    }

    /* ───────────────────────── the MCP instructions ───────────────────────── */

    [Fact]
    public void Instructions_AreUnchangedWithNothingDeclared()
    {
        Assert.Equal("", DarlingPeerDirectory.InstructionsSection(DarlingPeerDirectory.Snapshot.Empty));
        Assert.Same(DarlingMcpInstructions.Text, DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty));
    }

    [Fact]
    public void Instructions_DiscloseThisStoreAndItsPeers_AboveTheToolCensus()
    {
        var text = DarlingMcpInstructions.Build(TwoPeers());

        Assert.Contains(Use1Covers, text, StringComparison.Ordinal);
        Assert.Contains("prod-sql-use2-monitor-01 — the readable replicas", text, StringComparison.Ordinal);
        Assert.Contains("prod-sql-pg-monitor-01 — the Aurora PostgreSQL clusters", text, StringComparison.Ordinal);

        /* The point of the section is that a peer is NAMED, never contacted — say so where the agent will
           read it, or it will try to route a query at the sibling. */
        Assert.Contains("NO cross-store connectivity", text, StringComparison.Ordinal);

        /* Placement is load-bearing: an agent must learn WHICH store it is talking to before it reads the
           tool census and starts planning. */
        var readOnly = text.IndexOf("## CRITICAL: Read-Only Access", StringComparison.Ordinal);
        var coverage = text.IndexOf("## Fleet Coverage", StringComparison.Ordinal);
        var census = text.IndexOf("This server exposes", StringComparison.Ordinal);
        Assert.True(readOnly >= 0 && coverage > readOnly && census > coverage,
            $"the coverage section must sit between the read-only preamble and the tool census (read-only {readOnly}, coverage {coverage}, census {census})");

        /* Inserting a section must not drop any of the body — the census sentence a cross-app test parses
           (Lite.Tests/CrossAppMcpToolInventoryPinTests) lives in it, as does every tool table. */
        Assert.Contains("are unique to Darling", text, StringComparison.Ordinal);
        Assert.EndsWith("mute a finding pattern the operator has accepted", text.TrimEnd(), StringComparison.Ordinal);
    }

    [Fact]
    public void Instructions_DiscloseCoverageEvenWithNoPeersDeclared()
    {
        /* "This store covers X" is worth saying on its own; the no-connectivity paragraph is not, because
           there is nothing to warn about connecting to. */
        var text = DarlingPeerDirectory.InstructionsSection(
            DarlingPeerDirectory.FromConfig(new PeersConfig { ThisStoreCovers = Use1Covers }));

        Assert.Contains(Use1Covers, text, StringComparison.Ordinal);
        Assert.DoesNotContain("NO cross-store connectivity", text, StringComparison.Ordinal);
    }

    /* ───────────────────────── list_servers ───────────────────────── */

    private static JsonElement RenderedServerList(DarlingPeerDirectory.Snapshot peers)
    {
        var rows = new List<DarlingDataReader.ServerListRow>
        {
            new(1, "prod-sql-use1-beta-01", "omega", 16, new DateTime(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc)),
        };

        return JsonDocument
            .Parse(DarlingMcpDataTools.RenderServerList(rows, new DateTime(2026, 8, 19, 12, 0, 30, DateTimeKind.Utc), peers))
            .RootElement;
    }

    [Fact]
    public void ListServers_CarriesThePeerFleetsSummary()
    {
        var root = RenderedServerList(TwoPeers());

        Assert.Equal(1, root.GetProperty("server_count").GetInt32());
        Assert.Equal(Use1Covers, root.GetProperty("this_store_covers").GetString());

        var fleets = root.GetProperty("peer_fleets").EnumerateArray().ToList();
        Assert.Equal(2, fleets.Count);
        Assert.Equal("prod-sql-use2-monitor-01", fleets[0].GetProperty("name").GetString());
        Assert.Equal("the readable replicas of those same 42 primaries, from us-east-2", fleets[0].GetProperty("covers").GetString());
        Assert.Equal(new[] { "use2" }, fleets[0].GetProperty("matches").EnumerateArray().Select(m => m.GetString()).ToArray());
        Assert.Empty(fleets[1].GetProperty("matches").EnumerateArray());

        Assert.Contains("cannot read a peer's data", root.GetProperty("peer_note").GetString(), StringComparison.Ordinal);

        /* The existing payload is untouched — the disclosure is additive here too. */
        var server = Assert.Single(root.GetProperty("servers").EnumerateArray());
        Assert.Equal("prod-sql-use1-beta-01", server.GetProperty("server_name").GetString());
        Assert.Equal("omega", server.GetProperty("display_name").GetString());
    }

    [Fact]
    public void ListServers_EmptyPeerFleets_SaysWhatItDoesNotProve()
    {
        /* An empty peer list has two very different meanings — this is the only store, or the operator never
           declared the siblings — and the service cannot tell them apart. Letting the empty array read as
           "you are looking at the whole fleet" is exactly the mistake #2339 was filed about. */
        var root = RenderedServerList(DarlingPeerDirectory.Snapshot.Empty);

        Assert.Empty(root.GetProperty("peer_fleets").EnumerateArray());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("this_store_covers").ValueKind);

        var note = root.GetProperty("peer_note").GetString();
        Assert.Contains("No peer stores are declared", note, StringComparison.Ordinal);
        Assert.Contains("not proof that nobody monitors it", note, StringComparison.Ordinal);
    }

    [Fact]
    public void EmptyRegistry_StillDisclosesThePeers()
    {
        /* list_servers answers an empty registry with prose, not the JSON envelope — so that path carries the
           disclosure explicitly, or it becomes the one place the declaration silently vanishes. And it is the
           worst one to lose: an empty registry is a fresh or just-restarted box, where "no servers here" with
           no mention of the siblings is the strongest version of the wrong conclusion. */
        var disclosure = DarlingPeerDirectory.EmptyRegistryDisclosure(TwoPeers());

        Assert.Contains("one of SEVERAL monitoring this fleet", disclosure, StringComparison.Ordinal);
        Assert.Contains("prod-sql-use2-monitor-01", disclosure, StringComparison.Ordinal);
        Assert.Contains("prod-sql-pg-monitor-01", disclosure, StringComparison.Ordinal);
        Assert.Contains($"This store covers: {Use1Covers}.", disclosure, StringComparison.Ordinal);

        /* Unchanged with nothing declared, like every other surface. */
        Assert.Equal("", DarlingPeerDirectory.EmptyRegistryDisclosure(DarlingPeerDirectory.Snapshot.Empty));
    }

    /* ───────────────────────── the resolution miss ───────────────────────── */

    private const string MissWithoutPeers =
        "Could not resolve server. Available servers:\nprod-sql-use1-beta-01";

    [Fact]
    public void ResolutionMiss_IsByteForByteUnchangedWithNothingDeclared()
    {
        var (resolved, error) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("prod-sql-use1-beta-01") },
            "prod-sql-use2-beta-01",
            DarlingPeerDirectory.Snapshot.Empty);

        Assert.Equal(default, resolved);
        Assert.Equal(MissWithoutPeers, error);
    }

    [Fact]
    public void ResolutionMiss_NamesThePeerWhoseDeclaredCoverageMatches()
    {
        var (resolved, error) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("prod-sql-use1-beta-01") },
            "prod-sql-use2-beta-01",
            TwoPeers());

        Assert.Equal(default, resolved);
        Assert.NotNull(error);

        /* The prefix and the local listing survive: 'Could not resolve server.' is what callers key off, and
           the local list is still the right answer to the commonest miss (a typo). */
        Assert.StartsWith(MissWithoutPeers, error, StringComparison.Ordinal);

        Assert.Contains("'prod-sql-use2-beta-01' is not monitored HERE", error, StringComparison.Ordinal);
        Assert.Contains("matches the declared coverage of peer store prod-sql-use2-monitor-01", error, StringComparison.Ordinal);
        Assert.Contains("That is a SEPARATE Darling store", error, StringComparison.Ordinal);
        Assert.Contains("this server cannot read it", error, StringComparison.Ordinal);
        Assert.Contains($"This store covers: {Use1Covers}.", error, StringComparison.Ordinal);

        /* The peer that declared no patterns must not be blamed for a name it never claimed. */
        Assert.DoesNotContain("prod-sql-pg-monitor-01", error, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolutionMiss_WithNoMatchingPeer_ListsThemWithoutClaimingUnmonitored()
    {
        var (_, error) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("prod-sql-use1-beta-01") },
            "some-other-box",
            TwoPeers());

        Assert.NotNull(error);
        Assert.Contains("'some-other-box' is not monitored HERE", error, StringComparison.Ordinal);
        Assert.Contains("matches no declared peer store's coverage either", error, StringComparison.Ordinal);

        /* Both peers are still disclosed: the declarations are prose plus optional patterns, not a live
           registry, so "no pattern matched" is not evidence the server is unmonitored. */
        Assert.Contains("prod-sql-use2-monitor-01", error, StringComparison.Ordinal);
        Assert.Contains("prod-sql-pg-monitor-01", error, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolutionMiss_WithTwoPeersClaimingTheName_AgreesInNumber()
    {
        /* Two peers can legitimately both claim a name through overlapping `matches`, so the follow-on
           sentence must not say "That is a SEPARATE store" about a list of two. */
        var overlapping = DarlingPeerDirectory.FromConfig(new PeersConfig
        {
            Stores =
            {
                new PeerStoreConfig { Name = "box2", Covers = "the replicas", Matches = { "use2" } },
                new PeerStoreConfig { Name = "box3", Covers = "the archive replicas", Matches = { "prod-sql" } },
            },
        });

        var (_, error) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("prod-sql-use1-beta-01") }, "prod-sql-use2-beta-01", overlapping);

        Assert.NotNull(error);
        Assert.Contains("these peer stores: box2 — the replicas; box3 — the archive replicas", error, StringComparison.Ordinal);
        Assert.Contains("Those are SEPARATE Darling stores", error, StringComparison.Ordinal);
        Assert.DoesNotContain("That is a SEPARATE Darling store", error, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolutionMiss_WithNoNameGiven_DisclosesPeersWithoutAccusingOne()
    {
        /* The blank-name miss (several servers, no server_name passed) has no name to match, so the
           disclosure must list rather than accuse. */
        var (_, error) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("prod-sql-use1-beta-01"), Registered("prod-sql-use1-alpha-01") },
            "  ",
            TwoPeers());

        Assert.NotNull(error);
        Assert.StartsWith("Could not resolve server.", error, StringComparison.Ordinal);
        Assert.Contains("That server is not monitored HERE", error, StringComparison.Ordinal);
        Assert.DoesNotContain("matches the declared coverage", error, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolutionMiss_ReadsTheAmbientDeclaration_ThroughTheTwoArgOverload()
    {
        /* The ~90 tool methods all call the two-arg form, so the ambient publish is the seam that actually
           delivers this to an MCP client. Reset in a finally: process-wide state must not leak between tests. */
        try
        {
            var published = DarlingPeerDirectory.Publish(new PeersConfig
            {
                ThisStoreCovers = Use1Covers,
                Stores = { new PeerStoreConfig { Name = "box2", Covers = "the replicas", Matches = { "use2" } } },
            });

            Assert.False(published.Refused);
            Assert.False(published.Snapshot.IsEmpty);
            Assert.Same(published.Snapshot, DarlingPeerDirectory.Current);

            var (_, error) = DarlingServerResolver.ResolveOrError(
                new[] { Registered("prod-sql-use1-beta-01") },
                "prod-sql-use2-beta-01");

            Assert.Contains("peer store box2 — the replicas", error, StringComparison.Ordinal);
        }
        finally
        {
            DarlingPeerDirectory.Reset();
        }

        Assert.True(DarlingPeerDirectory.Current.IsEmpty);

        var (_, afterReset) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("prod-sql-use1-beta-01") },
            "prod-sql-use2-beta-01");

        Assert.Equal(MissWithoutPeers, afterReset);
    }
}
