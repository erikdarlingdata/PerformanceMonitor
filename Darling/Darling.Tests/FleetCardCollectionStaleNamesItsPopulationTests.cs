/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3098: the fleet card's amber collection flag is named for the population it has. It is
/// <see cref="ServerFreshness.Stale"/> — how old the newest collection is, measured against the clock — and
/// the card publishes it as <c>collection_stale</c>.
///
/// <para><b>Why a NAME is worth a test.</b> Two agents read this one field four different ways in a single
/// day, from the payload plus the response around it, and a store query falsified every reading: cards
/// flagging it with all 39 collectors <c>HEALTHY</c> and <c>failed_collector_count: 0</c>; the flag set by
/// errors 15 to 26 hours outside the card's own published window; the flag clear while a collector row
/// carried <c>errors: 2</c>; and the flag clear on all 43 cards while four <c>ERROR</c> rows sat INSIDE the
/// window those cards stated. Every one of those was a reasonable reading of a name, and the value was
/// correct the whole time. A field whose name misdescribes a correctly-computed value is worse than an
/// unnamed one, because the name answers "what is this the value of" and nothing in the payload disagrees —
/// so what these pins hold is the agreement between the name, the computation, and the prose the same code
/// emits.</para>
///
/// <para><b>Two flags, two populations, and they are supposed to disagree.</b> Lite's card carries a flag of
/// the same shape fed from <c>ErroringCollectors &gt; 0</c>; this one is fed from a clock. A server can
/// collect on time with a collector failing, and can go quiet with every collector's last run a success, so
/// the axes part company in normal operation. <see cref="LitesErrorFlag_StillNamesAnErrorCount"/> is what
/// keeps that a decision rather than an oversight the tree-wide scan below quietly stepped around.</para>
///
/// <para><b>Scope.</b> Everything here sits on the classifier and on the serialized card — the surface
/// #3098 was read off — and none of it touches WPF, so it runs on any host rather than only on the Windows
/// leg of CI. The one link a behavioural test cannot reach is the reader's own assembly of a card, inside a
/// private method behind nine store reads; that link is held by source text in
/// <see cref="TheMcpCard_DerivesTheFlagFromFreshnessAndNothingElse"/>, against the same four lines the
/// arithmetic below exercises directly.</para>
/// </summary>
public sealed class FleetCardCollectionStaleNamesItsPopulationTests
{
    private static readonly DateTime Now = new(2026, 9, 6, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// The flag is the <see cref="ServerFreshness.Stale"/> band and no other, over every band there is. This
    /// is the assertion the name makes, stated so that wiring the flag to anything else — an error count, a
    /// window, a collection-log read — fails here rather than in a reader's head.
    /// </summary>
    [Fact]
    public void TheFlag_IsExactlyTheStaleFreshnessBand()
    {
        var bands = Enum.GetValues<ServerFreshness>();

        /* Enumerating the enum rather than listing four cases: a fifth band arriving with the flag set would
           otherwise be a new population under an unchanged name, which is this issue again. */
        Assert.Equal(4, bands.Length);

        foreach (var band in bands)
        {
            Assert.Equal(
                band == ServerFreshness.Stale,
                ServerCollectionStatusRules.FlagsFor(band).CollectionStale);
        }
    }

    /// <summary>
    /// The flag moves with the clock and with nothing else. Every boundary is read off
    /// <see cref="ServerHealthThresholds"/> rather than restated as 2 and 15, so this cannot pass while the
    /// card has grown thresholds of its own (#1562).
    /// </summary>
    [Fact]
    public void TheFlag_TracksHowOldTheNewestCollectionIs()
    {
        var stale = ServerHealthThresholds.StaleThreshold;
        var offline = ServerHealthThresholds.OfflineThreshold;

        Assert.False(Stale(TimeSpan.Zero));
        Assert.False(Stale(stale));                                     // at the threshold, still fresh
        Assert.True(Stale(stale + TimeSpan.FromSeconds(1)));            // past it, stale
        Assert.True(Stale(offline));                                    // at the far edge, still stale
        Assert.False(Stale(offline + TimeSpan.FromSeconds(1)));         // past it, offline — a different state
        Assert.False(Stale(null));                                      // never collected — a bootstrap state

        static bool Stale(TimeSpan? sinceLastCollection) =>
            ServerCollectionStatusRules.FlagsFor(
                ServerHealthClassifier.ClassifyFreshness(
                    sinceLastCollection.HasValue ? Now - sinceLastCollection.Value : null, Now))
                .CollectionStale;
    }

    /// <summary>
    /// The four combinations of (collection age, collectors failing), each with the value both axes report.
    /// Two of the four are the readings #3098 falsified — a flagged card with every collector healthy, and a
    /// clear card with a collector failing — and here they are the expected answers rather than a puzzle.
    /// </summary>
    [Fact]
    public void TheFlag_IsIndependentOfTheCollectorErrorCount()
    {
        var late = ServerHealthThresholds.StaleThreshold + TimeSpan.FromMinutes(1);
        var current = TimeSpan.FromSeconds(5);

        /* Collecting on time, nothing failing: neither axis has anything to say. */
        var calm = Card(current, failing: 0);
        Assert.False(calm.CollectionStale);
        Assert.Equal(HealthSeverity.Healthy, calm.CollectorSeverity);
        Assert.Equal(FleetHealthBand.Healthy, calm.Band);

        /* The issue's first observation: flagged, with every collector healthy and none failing. Staleness is
           not failure, so this is correct and the card names both axes for what they measure. */
        var quiet = Card(late, failing: 0);
        Assert.True(quiet.CollectionStale);
        Assert.Equal(0, quiet.FailedCollectorCount);
        Assert.Equal(HealthSeverity.Healthy, quiet.CollectorSeverity);
        Assert.Equal(FleetHealthBand.Warning, quiet.Band);

        /* The issue's sharpest observation, from the PostgreSQL-target store: band Warning, one collector
           failing, and the collection flag CLEAR. The band is right and comes from the failing count; the
           two fields answer different questions and neither is wrong. */
        var failingButCurrent = Card(current, failing: 1);
        Assert.False(failingButCurrent.CollectionStale);
        Assert.Equal(1, failingButCurrent.FailedCollectorCount);
        Assert.Equal(HealthSeverity.Warning, failingButCurrent.CollectorSeverity);
        Assert.Equal(FleetHealthBand.Warning, failingButCurrent.Band);

        /* Both at once, each still reported on its own axis. */
        var both = Card(late, failing: 3);
        Assert.True(both.CollectionStale);
        Assert.Equal(HealthSeverity.Warning, both.CollectorSeverity);

        /* Neither axis is a proxy for the other: the flag disagrees with "any collector failing" on half of
           these, which is what makes one name for both readings unusable. */
        var cards = new[] { calm, quiet, failingButCurrent, both };
        Assert.Equal(2, cards.Count(c => c.CollectionStale != (c.FailedCollectorCount > 0)));
    }

    /// <summary>
    /// The prose the card emits beside the flag says the same thing the flag is named for, and the failing
    /// count gets its own clause. These are the two strings a human reads off a Warning card, and a card that
    /// reported staleness as a collector error is how the name earned its reading.
    /// </summary>
    [Fact]
    public void TheReasonString_AgreesWithTheFlagsName()
    {
        var quiet = Card(ServerHealthThresholds.StaleThreshold + TimeSpan.FromMinutes(1), failing: 0);
        Assert.Equal("collection stale", DarlingFleetReader.BuildReason(quiet));

        var failingButCurrent = Card(TimeSpan.FromSeconds(5), failing: 1);
        var reason = DarlingFleetReader.BuildReason(failingButCurrent);
        Assert.Equal("1 collector failing", reason);
        Assert.DoesNotContain("stale", reason, StringComparison.Ordinal);

        /* A card carrying both reports both, in their own clauses — the axes are additive in the prose
           exactly as they are in the payload. */
        var both = Card(ServerHealthThresholds.StaleThreshold + TimeSpan.FromMinutes(1), failing: 2);
        Assert.Equal("2 collectors failing, collection stale", DarlingFleetReader.BuildReason(both));
    }

    /// <summary>
    /// The reader's whole derivation, in the four lines that are all of it: classify the newest collection's
    /// age, explode that band into the shared flags, take the flag, put it on the card. There is no error
    /// count in the chain, no <c>collection_log</c> read, and no use of the roll-up's published window — which
    /// is why the card's <c>window_start</c> / <c>window_end</c> never bounded it.
    /// </summary>
    [Fact]
    public void TheMcpCard_DerivesTheFlagFromFreshnessAndNothingElse()
    {
        var reader = ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs");

        Assert.Contains(
            "var freshness = ServerHealthClassifier.ClassifyFreshness(lastCollection, now);\n"
          + "        var flags = ServerCollectionStatusRules.FlagsFor(freshness);",
            reader, StringComparison.Ordinal);
        Assert.Contains("var collectionStale = flags.CollectionStale;", reader, StringComparison.Ordinal);
        Assert.Contains("CollectionStale = collectionStale,", reader, StringComparison.Ordinal);

        /* And the field is published under the name that derivation earns. */
        Assert.Contains(
            "[JsonPropertyName(\"collection_stale\")] public bool CollectionStale { get; init; }",
            reader, StringComparison.Ordinal);
    }

    /// <summary>
    /// The serialized card names the flag for its population and carries, in the same payload, everything a
    /// reader needs to recompute it: <c>last_collection</c> for the age and <c>status</c> for the band that
    /// age produced. That is #3098's acceptance criterion — the field's population is determinable from the
    /// response alone — and it is why no companion timestamp was added: the card already published one.
    /// </summary>
    [Fact]
    public void TheSerializedCard_LetsAReaderRecomputeTheFlag()
    {
        var card = new FleetServerCard
        {
            ServerId = 7,
            DisplayName = "sql-01",
            ServerName = "sql-01",
            Band = FleetHealthBand.Warning,
            Status = ServerCollectionStatus.Stale.Word(),
            IsOnline = true,
            CollectionStale = true,
            LastCollectionTime = new DateTime(2026, 9, 6, 11, 50, 0, DateTimeKind.Unspecified),
            FailedCollectorCount = 0,
            HealthyCollectorCount = 39,
            CollectorSeverity = HealthSeverity.Healthy,
        };

        var json = JsonSerializer.Serialize(card, DarlingFleetReader.JsonOptions);

        JsonAssert.Contains("\"collection_stale\": true", json);
        JsonAssert.Contains("\"last_collection\": \"2026-09-06T11:50:00\"", json);
        JsonAssert.Contains("\"status\": \"Warning\"", json);

        /* The error axis is still published, and still separately — the flag was renamed, not merged into
           these. A card can hold this exact combination and be right about all four values. */
        JsonAssert.Contains("\"failed_collector_count\": 0", json);
        JsonAssert.Contains("\"healthy_collector_count\": 39", json);
        JsonAssert.Contains("\"collector_severity\": \"Healthy\"", json);

        /* No field on this card claims an error population it does not have. */
        Assert.DoesNotContain(JsonSpelling, json, StringComparison.Ordinal);
    }

    /// <summary>
    /// The errors spelling is absent from every Darling surface — source, the web dashboard's scripts, the MCP
    /// instructions, <c>Darling/README.md</c>, <c>llms.txt</c>, the root README and <c>docs/</c>. A rename
    /// that survives in one document is the same defect with a smaller audience, and a documented name is the
    /// one a reader trusts hardest.
    ///
    /// <para><b>Every file, not an extension allow-list.</b> The walk reads BYTES over everything under the
    /// scanned roots, so a reference in a file type nobody thought of is found rather than skipped. Both
    /// needles are assembled at run time from fragments, because this file is inside the scanned tree and a
    /// scan that matches its own needle reports the search itself.</para>
    ///
    /// <para><c>CHANGELOG.md</c> is out of scope by construction — it is a record of what changed, so it
    /// names the old field on purpose, and it is not a surface anyone reads a live payload against. The same
    /// reasoning covers <c>docs/changelog/</c>, which is that same record: the prose for released versions
    /// moved there when the single file outgrew what GitHub will render, and 3.7.0's entry for this very
    /// rename names both spellings in order to describe it. Scoped to that one directory rather than to
    /// markdown under <c>docs/</c>, so a real document cannot fall out of the sweep by being markdown.</para>
    /// </summary>
    [Fact]
    public void TheErrorsSpelling_IsGoneFromEveryDarlingSurface()
    {
        var scanned = ScannedFiles().ToList();

        /* The walk is looking at something. A root that moved would otherwise make every assertion below
           vacuously true, which is the failure mode a coverage check has and a pin must not. */
        Assert.True(scanned.Count > 200, $"the walk found only {scanned.Count} files — a root has moved");

        var carriesNewName = scanned
            .Where(f => Holds(f.Value, "collection" + "_stale"))
            .Select(f => f.Key)
            .ToList();

        Assert.Contains(carriesNewName, p => p.EndsWith(
            Path.Combine("Mcp", "DarlingFleetReader.cs"), StringComparison.Ordinal));

        var offenders = scanned
            .Where(f => Holds(f.Value, JsonSpelling) || Holds(f.Value, ClrSpelling))
            .Select(f => f.Key)
            .ToList();

        Assert.True(offenders.Count == 0,
            "these Darling surfaces still name the flag for a population it does not have: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// Lite's flag keeps the errors name, because in Lite the name is accurate: it is fed from the count of
    /// collectors that are actually erroring, on a card whose status word comes from a live connection check
    /// rather than from a clock. This is the must-pass half of the scan above — without it, "the errors
    /// spelling is gone from the Darling trees" is equally satisfied by a scan whose roots exclude everything.
    /// </summary>
    [Fact]
    public void LitesErrorFlag_StillNamesAnErrorCount()
    {
        var declaration = ReadRepoFileLf("Lite", "Services", "LocalDataService.Overview.cs");
        Assert.Contains("public bool " + ClrSpelling + " { get; set; }", declaration, StringComparison.Ordinal);

        var window = ReadRepoFileLf("Lite", "MainWindow.xaml.cs");
        Assert.Contains(
            "." + ClrSpelling + " = _collectorService.GetHealthSummary(server).ErroringCollectors > 0;",
            window, StringComparison.Ordinal);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────

    /* Assembled from fragments so the literals never appear whole in a file the scan reads. */
    private static readonly string JsonSpelling = string.Concat("has", "_collector", "_errors");
    private static readonly string ClrSpelling = string.Concat("Has", "Collector", "Errors");

    /// <summary>A fleet card at a given collection age with a given number of collectors failing, assembled
    /// the way the reader assembles one: the flag comes out of a CLOCK through
    /// <see cref="ServerCollectionStatusRules.FlagsFor"/> rather than out of an assignment, so the age really
    /// is the input. Every other metric is calm, leaving the two collection axes as the only variables.</summary>
    private static FleetServerCard Card(TimeSpan sinceLastCollection, int failing)
    {
        var lastCollection = Now - sinceLastCollection;
        var flags = ServerCollectionStatusRules.FlagsFor(
            ServerHealthClassifier.ClassifyFreshness(lastCollection, Now));

        var metrics = new ServerHealthMetrics { CpuPercentForAlert = 4, FailedCollectorCount = failing };
        var overall = ServerHealthClassifier.OverallMetricSeverity(metrics);

        return new FleetServerCard
        {
            ServerId = 1,
            DisplayName = "sql-01",
            ServerName = "sql-01",
            IsOnline = flags.IsOnline,
            AwaitingFirstCollection = flags.AwaitingFirstCollection,
            CollectionStale = flags.CollectionStale,
            LastCollectionTime = lastCollection,
            Status = ServerCollectionStatusRules
                .Classify(flags.IsOnline, flags.CollectionStale, flags.AwaitingFirstCollection).Word(),
            FailedCollectorCount = failing,
            CollectorSeverity = ServerHealthClassifier.CollectorSeverity(failing),
            OverallMetricSeverity = overall,
            Band = ServerHealthClassifier.ClassifyBand(
                flags.IsOnline, flags.AwaitingFirstCollection, flags.CollectionStale, overall),
        };
    }

    /// <summary>Every file under the surfaces a consumer of this field can read, as bytes.</summary>
    private static IEnumerable<KeyValuePair<string, byte[]>> ScannedFiles()
    {
        var roots = new[]
        {
            Path.Combine(Root, "PerformanceMonitor.Common"),
            Path.Combine(Root, "Darling"),
            Path.Combine(Root, "docs"),
        };

        /* The archived changelog prose, which is CHANGELOG.md's own text under docs/ and carries the old
           name for the same reason CHANGELOG.md does. One exact directory, not a markdown-wide carve-out. */
        var archivedChangelog =
            $"{Path.DirectorySeparatorChar}docs{Path.DirectorySeparatorChar}changelog{Path.DirectorySeparatorChar}";

        var excluded = 0;
        foreach (var root in roots)
        {
            Assert.True(Directory.Exists(root), $"{root} is gone — this scan is walking nothing");

            foreach (var file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
            {
                if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                if (file.Contains(archivedChangelog, StringComparison.Ordinal))
                {
                    excluded++;
                    continue;
                }

                yield return new KeyValuePair<string, byte[]>(file, File.ReadAllBytes(file));
            }
        }

        /* The carve-out is live and is only the changelog archives. Zero would mean the directory moved and
           the exclusion had quietly become dead text; a number far above the release count would mean it had
           grown to cover something else. */
        Assert.InRange(excluded, 1, 100);

        foreach (var name in new[] { "llms.txt", "README.md" })
        {
            var path = Path.Combine(Root, name);
            Assert.True(File.Exists(path), $"{path} is gone — this scan is walking nothing");
            yield return new KeyValuePair<string, byte[]>(path, File.ReadAllBytes(path));
        }
    }

    /// <summary>Whether an ASCII needle occurs in a file's raw bytes. Bytes rather than decoded text so no
    /// encoding, BOM or invalid sequence can hide a match, and so a binary file cannot throw the walk.</summary>
    private static bool Holds(byte[] haystack, string needle)
    {
        var pattern = Encoding.ASCII.GetBytes(needle);

        for (var i = 0; i + pattern.Length <= haystack.Length; i++)
        {
            var hit = true;

            for (var j = 0; j < pattern.Length; j++)
            {
                if (haystack[i + j] != pattern[j])
                {
                    hit = false;
                    break;
                }
            }

            if (hit)
            {
                return true;
            }
        }

        return false;
    }
}
