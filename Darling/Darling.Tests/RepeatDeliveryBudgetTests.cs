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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3430: <see cref="IncidentCooldown"/> bounds posts per #1140 fingerprint, and
/// <see cref="AlertFingerprint"/> hashes the server name into every fingerprint — so one fault on N servers
/// is N fingerprints, each delivering once per window, and the aggregate is the fingerprint count times the
/// per-fingerprint allowance. Measured on a 43-server store: 15 servers carrying one metric produced 16
/// posts in 15.9 minutes, 60.2 posts/hour, with the throttle working exactly as designed.
/// <see cref="RepeatDeliveryBudget"/> bounds REPEATS per metric across the fleet instead.
///
/// <para><b>Every fixture here drives N servers for N greater than one, and that is not decoration.</b> At
/// N = 1 a per-fingerprint throttle and a per-metric one are the same mechanism, so a one-server fixture
/// cannot tell them apart and would pass against unmodified <c>dev</c>. The rate pins run fifteen servers
/// and four windows, which is the shape of the measurement.</para>
///
/// <para><b>Both halves are asserted, because either alone is satisfied by something wrong.</b> "The post
/// count is bounded" is satisfied by a fix that drops fourteen of fifteen incidents — which is #1154's bug
/// wearing this issue's clothes — and "every incident is named" is satisfied by not aggregating at all. So
/// every rate pin also asserts that each of the N incidents is named in what went out, and the wire pins
/// below run ONE fixture three ways (Summary, Per-event, and all-first-notice) where the only difference is
/// the input that is supposed to drive the difference.</para>
///
/// <para>The clock: <see cref="IncidentCooldown"/> reads <see cref="DateTime.UtcNow"/> itself, but the
/// budget is handed the cooldown's evaluation instant, so a pin that constructs an
/// <see cref="IncidentCooldown.Decision"/> directly has full control of time and can walk four fifteen-minute
/// windows without sleeping. The wire pins cannot, which is why they seed prior deliveries through the
/// history store — the same device #3313's pins use to put a key inside its window.</para>
/// </summary>
public sealed class RepeatDeliveryBudgetTests
{
    private const string Metric = "Failed Agent Job";
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(15);
    private static readonly DateTime T0 = new(2026, 9, 14, 21, 48, 33, DateTimeKind.Utc);

    /// <summary>
    /// How long ago the wire fixtures' seeded deliveries went out. Well past
    /// <c>DarlingConfig.Smtp.EmailCooldownMinutes</c>' default fifteen, because a repeat only reaches the
    /// budget once the per-fingerprint cooldown has already cleared it — a seed INSIDE the window suppresses
    /// the delivery at the cooldown and the aggregate is never consulted, which reads as the budget folding
    /// and is not.
    /// <para>Three days is the measured case rather than an arbitrary large number: alerting was re-enabled
    /// on the 43-server store after a three-day blanket mute, so every fingerprint's last delivery was days
    /// old and every one of them was a repeat.</para>
    /// </summary>
    private static readonly TimeSpan StaleDelivery = TimeSpan.FromDays(3);

    /* Fifteen servers, the fan-out width of the measurement. Invented labels; SRV-A..SRV-O rather than
       anything a fleet would issue. */
    private static readonly string[] Fleet =
        Enumerable.Range(0, 15).Select(n => "SRV-" + (char)('A' + n)).ToArray();

    /* ─────────────── the rate, and what it costs ─────────────── */

    /// <summary>
    /// <b>The pin that matters.</b> Fifteen servers repeat one metric through four fifteen-minute windows.
    /// Unaggregated that is 15 x 4 = 60 posts, which is the measured 60.2/hour; the budget makes it four,
    /// one per window, and by the end of the second window every one of the fifteen has been named in
    /// something that went out.
    /// <para>The named-set assertion is over the union of the carrier's own server and its roster's
    /// entries, which is exactly "what an operator saw". A fix that posted four cards naming four servers
    /// would satisfy the count and fail this.</para>
    /// </summary>
    [Fact]
    public void FifteenServersRepeatingOneMetric_PostFourTimesAnHour_AndEveryServerIsStillNamed()
    {
        var budget = new RepeatDeliveryBudget();
        var posts = 0;
        var named = new HashSet<string>(StringComparer.Ordinal);

        for (int window = 0; window < 4; window++)
        {
            var at = T0 + TimeSpan.FromTicks(Window.Ticks * window);
            foreach (var server in Fleet)
            {
                var decision = budget.Evaluate(
                    Metric, server, Repeat(at, Fingerprint(server)), Window,
                    aggregateRepeats: true, incidents: Incidents(server));

                if (!decision.ShouldSend)
                {
                    continue;
                }

                posts++;
                named.Add(server);
                foreach (var entry in RosterEntries(decision))
                {
                    named.Add(entry.Split(' ')[0]);
                }

                budget.Commit(decision);
            }
        }

        /* The fixture's own width, asserted rather than assumed — and this is the assertion that makes the
           two below mean anything. At N = 1 a per-fingerprint throttle and a per-metric one deliver the
           same four posts over four windows, so a fleet narrowed to one server turns the whole pin vacuous
           while leaving it green. Measured: narrowing Fleet to a single entry passes every other assertion
           in this method. */
        Assert.Equal(15, Fleet.Length);
        Assert.Equal(60, Fleet.Length * 4);

        /* Four windows, one post each — and NOT the 60 the same fixture costs per fingerprint. */
        Assert.Equal(4, posts);

        /* Nothing became unannounced: all fifteen appear in what went out. */
        Assert.Equal(Fleet.OrderBy(s => s, StringComparer.Ordinal), named.OrderBy(s => s, StringComparer.Ordinal));
    }

    /// <summary>
    /// The same fixture with aggregation declined: fifteen servers, fifteen posts per window. This is the
    /// control for the pin above — without it, "four posts" is satisfied by a budget that folds
    /// unconditionally, and the assertion would still be green if the first-notice exemption and the mode
    /// gate were both removed.
    /// </summary>
    [Fact]
    public void WithAggregationDeclined_EveryServerStillPosts_WhichIsTodaysRate()
    {
        var budget = new RepeatDeliveryBudget();
        var posts = 0;

        foreach (var server in Fleet)
        {
            var decision = budget.Evaluate(
                Metric, server, Repeat(T0, Fingerprint(server)), Window,
                aggregateRepeats: false, incidents: Incidents(server));

            Assert.True(decision.ShouldSend);
            Assert.Null(decision.Roster);
            posts++;
        }

        Assert.Equal(Fleet.Length, posts);
    }

    /// <summary>
    /// A first notice is never folded, no matter how spent the metric's window is. This is the line #1154
    /// drew: it re-keyed the cooldown onto the fingerprint precisely because distinct concurrent incidents
    /// were being silently dropped, and an aggregate that could swallow a never-delivered incident would
    /// reintroduce that. The first notice also CARRIES the roster, so the fold it did not participate in is
    /// still reported.
    /// </summary>
    [Fact]
    public void AFirstNotice_PostsEvenWithTheWindowSpent_AndCarriesTheRoster()
    {
        var budget = new RepeatDeliveryBudget();

        var carrier = budget.Evaluate(
            Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, aggregateRepeats: true);
        Assert.True(carrier.ShouldSend);
        budget.Commit(carrier);

        var folded = budget.Evaluate(
            Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window,
            aggregateRepeats: true, incidents: Incidents("SRV-B"));
        Assert.False(folded.ShouldSend);

        var firstNotice = budget.Evaluate(
            Metric, "SRV-C", FirstNotice(T0 + TimeSpan.FromMinutes(2), Fingerprint("SRV-C")), Window,
            aggregateRepeats: true, incidents: Incidents("SRV-C"));

        Assert.True(firstNotice.ShouldSend);
        Assert.Equal(new[] { "SRV-B (srvb01234567, 4 occurrence(s))" }, RosterEntries(firstNotice));
    }

    /// <summary>
    /// A fold puts the server AND its fingerprint on the roster, not a tally. Asserted as an exact set, and
    /// with a count-preserving permutation — the same fifteen servers and the same fifteen fingerprints
    /// paired the wrong way round — because a roster built by zipping two independently-ordered sequences
    /// produces the right COUNT of lines with the wrong facts on them, and a contains-count assertion cannot
    /// see that.
    /// </summary>
    [Fact]
    public void TheRosterNamesEachFoldedServerWithItsOwnFingerprint()
    {
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true));

        foreach (var server in Fleet.Skip(1))
        {
            var folded = budget.Evaluate(
                Metric, server, Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint(server)), Window,
                aggregateRepeats: true, incidents: Incidents(server));
            Assert.False(folded.ShouldSend);
        }

        var carrier = budget.Evaluate(
            Metric, "SRV-A", Repeat(T0 + Window, Fingerprint("SRV-A")), Window, aggregateRepeats: true);

        var lines = RosterEntries(carrier).ToHashSet(StringComparer.Ordinal);

        Assert.Equal(14, lines.Count);
        foreach (var server in Fleet.Skip(1))
        {
            Assert.Contains($"{server} ({Fingerprint(server).Substring(0, 12)}, 4 occurrence(s))", lines);
        }

        /* The permutation: every line is present, every fingerprint is present, and no line pairs a server
           with its neighbour's fingerprint. */
        for (int n = 1; n < Fleet.Length; n++)
        {
            var neighbour = Fleet[n == Fleet.Length - 1 ? 1 : n + 1];
            Assert.DoesNotContain(
                $"{Fleet[n]} ({Fingerprint(neighbour).Substring(0, 12)}, 4 occurrence(s))", lines);
        }
    }

    /// <summary>
    /// The count fact states the whole roster, and the enumeration is capped with the remainder named rather
    /// than silently truncated — the one place this mechanism compresses instead of enumerating, so it is
    /// pinned at exactly the boundary.
    /// </summary>
    [Fact]
    public void AboveTheEnumerationCap_TheOmittedCountIsStated()
    {
        const int Extra = 3;
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate(Metric, "CARRIER", Repeat(T0, "carrierfingerprint"), Window, true));

        for (int n = 0; n < RepeatDeliveryBudget.MaxEnumeratedEntries + Extra; n++)
        {
            var folded = budget.Evaluate(
                Metric, "HOST" + n.ToString("D4", System.Globalization.CultureInfo.InvariantCulture),
                Repeat(T0 + TimeSpan.FromMinutes(1), "fingerprintnumber" + n), Window, aggregateRepeats: true);
            Assert.False(folded.ShouldSend);
        }

        var carrier = budget.Evaluate(
            Metric, "CARRIER", Repeat(T0 + Window, "carrierfingerprint"), Window, aggregateRepeats: true);

        Assert.Equal(
            RepeatDeliveryBudget.MaxEnumeratedEntries + Extra,
            carrier.RosterEntryCount);
        Assert.Equal(RepeatDeliveryBudget.MaxEnumeratedEntries, RosterEntries(carrier).Count);
        Assert.Equal(
            Extra.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Fact(carrier, RepeatDeliveryBudget.RosterOmittedFactName));
        Assert.Contains(
            (RepeatDeliveryBudget.MaxEnumeratedEntries + Extra).ToString(System.Globalization.CultureInfo.InvariantCulture),
            Fact(carrier, RepeatDeliveryBudget.RosterCountFactName));
    }

    /// <summary>
    /// Below the cap there is no omitted-count fact at all, so its absence means the list is complete rather
    /// than that nobody checked.
    /// </summary>
    [Fact]
    public void BelowTheEnumerationCap_NoOmittedCountIsStated()
    {
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true));
        budget.Evaluate(
            Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true);

        var carrier = budget.Evaluate(
            Metric, "SRV-A", Repeat(T0 + Window, Fingerprint("SRV-A")), Window, true);

        Assert.Null(Fact(carrier, RepeatDeliveryBudget.RosterOmittedFactName));
    }

    /// <summary>
    /// A commit clears exactly what the post named. Anything folded while that send was in flight belongs to
    /// the NEXT carrier — committing it away would be the only way this mechanism can lose a roster line
    /// that was never reported.
    /// </summary>
    [Fact]
    public void ACommitClearsWhatThePostNamed_AndNothingFoldedAfterIt()
    {
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true));
        budget.Evaluate(
            Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true,
            Incidents("SRV-B"));

        var carrier = budget.Evaluate(
            Metric, "SRV-A", Repeat(T0 + Window, Fingerprint("SRV-A")), Window, true);
        Assert.Equal(new[] { "SRV-B (srvb01234567, 4 occurrence(s))" }, RosterEntries(carrier));

        /* Folded between the decision and the commit — the window the four channel posts occupy. */
        budget.Evaluate(
            Metric, "SRV-C", Repeat(T0 + Window + TimeSpan.FromSeconds(1), Fingerprint("SRV-C")), Window, true,
            Incidents("SRV-C"));
        budget.Commit(carrier);

        var next = budget.Evaluate(
            Metric, "SRV-A", Repeat(T0 + Window + Window, Fingerprint("SRV-A")), Window, true);
        Assert.Equal(new[] { "SRV-C (srvc01234567, 4 occurrence(s))" }, RosterEntries(next));
    }

    /// <summary>
    /// A carrier whose send delivered nothing gives the window back, so the next repeat posts instead of the
    /// metric going quiet for a window on the strength of a card nobody received — the same rule
    /// <see cref="IncidentCooldown"/> applies by stamping only on success. The paired assertion is the point:
    /// without the release the same second offer folds, so this pin fails if <c>Release</c> becomes a no-op.
    /// </summary>
    [Fact]
    public void AReleasedCarrier_LetsTheNextRepeatPost()
    {
        var withRelease = new RepeatDeliveryBudget();
        var failed = withRelease.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true);
        Assert.True(failed.ShouldSend);
        withRelease.Release(failed);

        Assert.True(withRelease
            .Evaluate(Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true)
            .ShouldSend);

        var withoutRelease = new RepeatDeliveryBudget();
        Assert.True(withoutRelease
            .Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true).ShouldSend);
        Assert.False(withoutRelease
            .Evaluate(Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true)
            .ShouldSend);
    }

    /// <summary>
    /// A release only gives back the reservation it made. A newer carrier has already superseded it, and
    /// dropping that one would let a second card post inside the same window — which is the rate this exists
    /// to bound.
    /// </summary>
    [Fact]
    public void AStaleRelease_DoesNotDropANewerCarriersWindow()
    {
        var budget = new RepeatDeliveryBudget();
        var first = budget.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true);
        budget.Release(first);

        var second = budget.Evaluate(
            Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true);
        Assert.True(second.ShouldSend);

        budget.Release(first);

        Assert.False(budget
            .Evaluate(Metric, "SRV-C", Repeat(T0 + TimeSpan.FromMinutes(2), Fingerprint("SRV-C")), Window, true)
            .ShouldSend);
    }

    /// <summary>
    /// A first notice posts outside the budget entirely — it neither folds nor spends the window — so the
    /// metric's next repeat is still free to carry. Otherwise a fault producing a first notice every window
    /// would starve the roster of a carrier.
    /// </summary>
    [Fact]
    public void AFirstNotice_DoesNotSpendTheWindow()
    {
        var budget = new RepeatDeliveryBudget();

        var firstNotice = budget.Evaluate(
            Metric, "SRV-A", FirstNotice(T0, Fingerprint("SRV-A")), Window, true);
        Assert.True(firstNotice.ShouldSend);
        budget.Commit(firstNotice);

        Assert.True(budget
            .Evaluate(Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true)
            .ShouldSend);
    }

    /// <summary>
    /// The budget is per metric. One metric's spent window must not throttle another's, or a fleet-wide
    /// deadlock would silence every low-disk alert on the fleet.
    /// </summary>
    [Fact]
    public void MetricsDoNotShareAWindow()
    {
        var budget = new RepeatDeliveryBudget();
        Assert.True(budget.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true).ShouldSend);
        Assert.False(budget
            .Evaluate(Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true)
            .ShouldSend);

        Assert.True(budget
            .Evaluate("Volume Free Space", "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true)
            .ShouldSend);
    }

    /// <summary>
    /// The same (server, fingerprint) folded on every sweep is ONE roster line, carrying the highest
    /// occurrence gauge seen — not one line per sweep, which on the measured cadence would be seven lines
    /// per server per window.
    /// </summary>
    [Fact]
    public void RepeatedFoldsOfOneFingerprint_AreOneRosterLine()
    {
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true));

        for (int sweep = 1; sweep <= 7; sweep++)
        {
            budget.Evaluate(
                Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(sweep), Fingerprint("SRV-B")), Window,
                aggregateRepeats: true,
                incidents: new[] { new AlertIncident(Fingerprint("SRV-B"), new[] { "SalesDb.dbo.Orders" }, sweep) });
        }

        var carrier = budget.Evaluate(
            Metric, "SRV-A", Repeat(T0 + Window, Fingerprint("SRV-A")), Window, true);

        Assert.Equal(new[] { "SRV-B (srvb01234567, 7 occurrence(s))" }, RosterEntries(carrier));
        Assert.Equal(1, carrier.RosterEntryCount);
    }

    /// <summary>
    /// An alert with no fingerprint at all folds on the metric-level axis and its roster line is the server
    /// alone. CPU, memory, tempdb and low disk are the metrics with the worst fan-out and the ones that
    /// never had an incident to carry, so exempting them for lacking a fingerprint would leave the measured
    /// shape unbounded on exactly the alerts that produce it most.
    /// </summary>
    [Fact]
    public void AMetricLevelAlertWithNoFingerprint_FoldsAndNamesItsServer()
    {
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate("High CPU", "SRV-A", MetricLevelRepeat(T0), Window, true));

        Assert.False(budget
            .Evaluate("High CPU", "SRV-B", MetricLevelRepeat(T0 + TimeSpan.FromMinutes(1)), Window, true)
            .ShouldSend);

        var carrier = budget.Evaluate("High CPU", "SRV-A", MetricLevelRepeat(T0 + Window), Window, true);

        Assert.Equal(new[] { "SRV-B" }, RosterEntries(carrier));
    }

    /// <summary>
    /// Entries quiet for longer than two windows expire, so a metric that stops firing does not carry a
    /// roster of things that ended hours ago. Two windows rather than one for the reason
    /// <c>IncidentCooldown.Evict</c> gives: past one the fingerprint is already re-fire-eligible and re-folds
    /// itself if it is still live, so doubling only adds clock-skew margin.
    /// </summary>
    [Fact]
    public void EntriesQuietForTwoWindows_Expire()
    {
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate(Metric, "SRV-A", Repeat(T0, Fingerprint("SRV-A")), Window, true));
        Assert.False(budget
            .Evaluate(Metric, "SRV-B", Repeat(T0 + TimeSpan.FromMinutes(1), Fingerprint("SRV-B")), Window, true)
            .ShouldSend);

        var carrier = budget.Evaluate(
            Metric, "SRV-A", Repeat(T0 + TimeSpan.FromTicks(Window.Ticks * 3), Fingerprint("SRV-A")), Window, true);

        Assert.Null(carrier.Roster);
        Assert.Equal(0, carrier.RosterEntryCount);
    }

    /// <summary>
    /// One metric's roster is bounded, and the bound drops the entries that have been quiet longest rather
    /// than refusing the newest fold. Memory rather than policy: an entry is a repeat already delivered in
    /// full, so the eviction costs a roster line and never an announcement — but a roster that grew without
    /// limit on a fault nobody cleared would be a leak, and a bound nobody drives is a claim.
    /// </summary>
    [Fact]
    public void OneMetricsRoster_IsBoundedAndKeepsTheLiveliestEntries()
    {
        const int Over = 5;
        var budget = new RepeatDeliveryBudget();
        budget.Commit(budget.Evaluate(Metric, "CARRIER", Repeat(T0, "carrierfingerprint"), Window, true));

        for (int n = 0; n < RepeatDeliveryBudget.MaxTrackedEntriesPerMetric + Over; n++)
        {
            budget.Evaluate(
                Metric, "HOST" + n.ToString("D4", System.Globalization.CultureInfo.InvariantCulture),
                Repeat(T0.AddSeconds(n + 1), "fingerprintnumber" + n), Window, aggregateRepeats: true);
        }

        var carrier = budget.Evaluate(
            Metric, "CARRIER", Repeat(T0 + Window, "carrierfingerprint"), Window, true);

        /* Bounded at the cap rather than at the 605 offered, and the enumeration below it still accounts for
           every entry the roster holds — the two caps compose rather than double-truncating silently. */
        Assert.Equal(RepeatDeliveryBudget.MaxTrackedEntriesPerMetric, carrier.RosterEntryCount);
        Assert.Equal(RepeatDeliveryBudget.MaxEnumeratedEntries, RosterEntries(carrier).Count);
        Assert.Equal(
            (RepeatDeliveryBudget.MaxTrackedEntriesPerMetric - RepeatDeliveryBudget.MaxEnumeratedEntries)
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            Fact(carrier, RepeatDeliveryBudget.RosterOmittedFactName));
    }

    /* ─────────────── the wire ─────────────── */

    /// <summary>
    /// <b>The pin that matters, on the wire.</b> Fourteen servers whose fingerprints the history store
    /// reports as already delivered post ONE card between them; a fifteenth server whose fingerprint was
    /// never delivered posts its own card, and that card names all thirteen folded servers. Two cards for
    /// fifteen affected servers, against fifteen on unmodified <c>dev</c>.
    /// <para>Teams only, so one card is one captured body and the count is the measurement rather than a
    /// division by the number of configured channels. The fifteenth server is a first notice deliberately:
    /// it is the one shape that produces a second post inside one window, which is what makes the roster
    /// observable without sleeping through a fifteen-minute cooldown.</para>
    /// </summary>
    [Fact]
    public async Task OnTheWire_FourteenRepeatingServersPostOnce_AndTheNextCardNamesThemAll()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var repeats = Fleet.Skip(1).ToArray();
        var history = new SeedingSetHistoryStore(
            repeats.Select(Fingerprint), DateTime.UtcNow - StaleDelivery);
        var deliverer = BuildDeliverer(endpoint, history);

        foreach (var server in repeats)
        {
            await deliverer.DeliverAsync(Outcome(server), TestContext.Current.CancellationToken);
        }

        /* The fourteenth-and-first: an unseeded fingerprint, so a first notice, so exempt — and it carries
           the roster the thirteen folds built. */
        await deliverer.DeliverAsync(Outcome(Fleet[0]), TestContext.Current.CancellationToken);

        Assert.Equal(2, endpoint.Bodies.Count);

        var carrier = endpoint.Bodies[0];
        Assert.Contains(repeats[0], carrier, StringComparison.Ordinal);
        Assert.DoesNotContain(RepeatDeliveryBudget.RosterHeading, carrier, StringComparison.Ordinal);

        var withRoster = endpoint.Bodies[1];
        Assert.Contains(RepeatDeliveryBudget.RosterHeading, withRoster, StringComparison.Ordinal);
        Assert.Contains(Fleet[0], withRoster, StringComparison.Ordinal);

        /* Nothing became unannounced: every folded server AND its own fingerprint is on the second card. */
        foreach (var server in repeats.Skip(1))
        {
            Assert.Contains(server, withRoster, StringComparison.Ordinal);
            Assert.Contains(Fingerprint(server).Substring(0, 12), withRoster, StringComparison.Ordinal);
        }

        /* And the roster is a DELIVERY view only: no history row carries it, in either column, so the
           Alerts tab, the MCP reader, the triage page and AlertMuteContext.PopulateFromDetailText read
           exactly what they read before. The same split #3313 drew, asserted at the wire rather than
           inferred from the copy — an append to the caller's own context would be invisible to every
           assertion above and visible only here. */
        Assert.Equal(Fleet.Length, history.Records.Count);
        Assert.All(history.Records, record =>
        {
            Assert.DoesNotContain(
                RepeatDeliveryBudget.RosterHeading, record.DetailText ?? string.Empty, StringComparison.Ordinal);
            Assert.DoesNotContain(
                RepeatDeliveryBudget.RosterHeading, record.ContextJson ?? string.Empty, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// The same fourteen servers in Per-event mode: fourteen cards. That mode exists so downstream
    /// automation gets one message per distinct incident and can count recurrences on the #1140 fingerprint,
    /// and a folded recurrence never reaches that consumer — so the mode's own contract is the boundary, and
    /// this is the pin that says the boundary is where the code puts it.
    /// <para>It doubles as the sharpest control in the suite: one setting differs from the pin above, and
    /// the post count moves from two to fourteen. A budget that folded regardless of mode, or a wiring that
    /// never reached the budget at all, fails exactly one of the pair.</para>
    /// </summary>
    [Fact]
    public async Task OnTheWire_PerEventMode_StillPostsEveryServer()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var repeats = Fleet.Skip(1).ToArray();
        var history = new SeedingSetHistoryStore(
            repeats.Select(Fingerprint), DateTime.UtcNow - StaleDelivery);
        var deliverer = BuildDeliverer(
            endpoint, history, config => config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent);

        foreach (var server in repeats)
        {
            await deliverer.DeliverAsync(Outcome(server), TestContext.Current.CancellationToken);
        }

        Assert.Equal(repeats.Length, endpoint.Bodies.Count);
        Assert.All(endpoint.Bodies, body =>
            Assert.DoesNotContain(RepeatDeliveryBudget.RosterHeading, body, StringComparison.Ordinal));
    }

    /// <summary>
    /// The same fourteen servers with NOTHING seeded: fourteen first notices, fourteen cards. The second
    /// control — it says the folding above came from those servers having been told about before, and not
    /// from the fixture's shape. Removing the first-notice exemption fails this and nothing else.
    /// </summary>
    [Fact]
    public async Task OnTheWire_FourteenFirstNotices_AllPost()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var repeats = Fleet.Skip(1).ToArray();
        var deliverer = BuildDeliverer(endpoint, new DiscardingHistoryStore());

        foreach (var server in repeats)
        {
            await deliverer.DeliverAsync(Outcome(server), TestContext.Current.CancellationToken);
        }

        Assert.Equal(repeats.Length, endpoint.Bodies.Count);
    }

    /// <summary>
    /// Email is bounded too, on its own budget, and this is a separate pin rather than an extra assertion
    /// because a fix applied to the webhook fan-out alone passes every webhook assertion above. The two
    /// channels hold separate cooldown key spaces by design (#1154's <c>keyPrefix</c>), so they hold separate
    /// budgets for the same reason: an email that failed to send left its key unstamped and its incident is
    /// still owed a delivery even where the webhook's is not.
    /// <para>SMTP only, no webhook, so one delivered message is one captured message.</para>
    /// </summary>
    [Fact]
    public async Task OnTheWire_TheEmailChannelIsBoundedOnItsOwnBudget()
    {
        using var smtp = new CapturingSmtpEndpoint();
        var repeats = Fleet.Skip(1).ToArray();
        var history = new SeedingSetHistoryStore(
            repeats.Select(Fingerprint), DateTime.UtcNow - StaleDelivery);
        var deliverer = BuildEmailOnlyDeliverer(smtp, history);

        foreach (var server in repeats)
        {
            await deliverer.DeliverAsync(Outcome(server), TestContext.Current.CancellationToken);
        }

        await deliverer.DeliverAsync(Outcome(Fleet[0]), TestContext.Current.CancellationToken);

        Assert.Equal(2, smtp.Messages.Count);

        var withRoster = smtp.Messages[1];
        Assert.Contains(RepeatDeliveryBudget.RosterHeading, withRoster, StringComparison.Ordinal);
        foreach (var server in repeats.Skip(1))
        {
            Assert.Contains(server, withRoster, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The history row is whole on a folded delivery as well as a carried one. #3313 drew this split for the
    /// render — delivery filters, persistence does not — and a fold is the same split one step earlier: the
    /// card did not go out, and the row still carries the alert, its detail text and its context JSON, so
    /// the Alerts tab, the MCP reader and the triage page are unaffected by the aggregate.
    /// </summary>
    [Fact]
    public async Task AFoldedDelivery_StillRecordsAWholeHistoryRow()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var repeats = new[] { Fleet[1], Fleet[2] };
        var history = new SeedingSetHistoryStore(
            repeats.Select(Fingerprint), DateTime.UtcNow - StaleDelivery);
        var deliverer = BuildDeliverer(endpoint, history);

        foreach (var server in repeats)
        {
            await deliverer.DeliverAsync(Outcome(server), TestContext.Current.CancellationToken);
        }

        Assert.Single(endpoint.Bodies);
        Assert.Equal(2, history.Records.Count);

        var folded = history.Records[1];
        Assert.Contains(Fingerprint(repeats[1]), folded.DetailText!, StringComparison.Ordinal);
        Assert.Contains(Fingerprint(repeats[1]), folded.ContextJson!, StringComparison.Ordinal);

        /* The folded row reports the same disposition a cooldown-throttled one always has: a channel was
           configured, nothing delivered, and no send error. No new notification_type, so no store column
           and no reader anywhere has to learn a third meaning. */
        Assert.False(folded.AlertSent);
        Assert.Equal(AlertDelivery.ChannelUndelivered, folded.NotificationType);
        Assert.Null(folded.SendError);
    }

    /// <summary>
    /// The roster reaches the card as a rendered detail item, and the caller's own context is not mutated to
    /// put it there — the delivery view carries what one message says, persistence carries what happened.
    /// Asserted on the instance the caller passed in, because an append to it would be invisible to every
    /// webhook assertion and visible only in the stored row.
    /// </summary>
    [Fact]
    public void TheRosterIsAppendedToACopy_NeverToTheCallersContext()
    {
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, Incidents("SRV-B"));
        var itemsBefore = context.Details.Count;

        var roster = new AlertDetailItem { Heading = RepeatDeliveryBudget.RosterHeading };
        var render = IncidentDeliveryFilter.ForDelivery(
            context, null, new[] { Fingerprint("SRV-B") }, roster);

        Assert.NotSame(context, render.Context);
        Assert.Equal(itemsBefore, context.Details.Count);
        Assert.Contains(render.Context!.Details, d => ReferenceEquals(d, roster));
    }

    /// <summary>
    /// An alert that never built a context at all still delivers the roster — the metric-level fan-out and
    /// the self-alerts, where the roster is the ONLY thing naming the other affected servers.
    /// </summary>
    [Fact]
    public void ARosterWithNoContext_BecomesAContextCarryingTheRoster()
    {
        var roster = new AlertDetailItem { Heading = RepeatDeliveryBudget.RosterHeading };

        var render = IncidentDeliveryFilter.ForDelivery(null, "Disk is filling.", null, roster);

        Assert.NotNull(render.Context);
        Assert.Contains(render.Context!.Details, d => ReferenceEquals(d, roster));
        Assert.Equal("Disk is filling.", render.Prose);
    }

    /// <summary>
    /// With no roster the filter returns the caller's own instance, so an alert that is not part of an
    /// aggregate renders byte-identically to pre-#3430.
    /// </summary>
    [Fact]
    public void WithNoRoster_TheUnfilteredContextIsReturnedUnchanged()
    {
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, Incidents("SRV-B"));

        var render = IncidentDeliveryFilter.ForDelivery(context, null, new[] { Fingerprint("SRV-B") });

        Assert.Same(context, render.Context);
    }

    /* ─────────────── fixtures ─────────────── */

    /* Longer than the display truncation so the roster's own eliding is exercised, and unique inside the
       first twelve characters so a truncated line still identifies its server. */
    private static string Fingerprint(string server) =>
        (server.Replace("-", "", StringComparison.Ordinal).ToLowerInvariant() + "0123456789abcdef0123456789")
            .Substring(0, 24);

    private static IReadOnlyList<AlertIncident> Incidents(string server) =>
        new[] { new AlertIncident(Fingerprint(server), new[] { "SalesDb.dbo.Orders" }, 4) };

    /* A delivery the cooldown cleared whose fingerprints have all been delivered before. */
    private static IncidentCooldown.Decision Repeat(DateTime atUtc, params string[] dedupKeys) =>
        new(true, dedupKeys.Select(k => "webhook:1:" + Metric + ":" + k).ToList(), atUtc, dedupKeys, false);

    private static IncidentCooldown.Decision FirstNotice(DateTime atUtc, params string[] dedupKeys) =>
        new(true, dedupKeys.Select(k => "webhook:1:" + Metric + ":" + k).ToList(), atUtc, dedupKeys, true);

    /* The non-fingerprinted shape: one metric-level key and a null deliverable set, which is "nothing to
       filter" rather than "nothing to show". */
    private static IncidentCooldown.Decision MetricLevelRepeat(DateTime atUtc) =>
        new(true, new[] { "webhook:1:High CPU" }, atUtc, null, false);

    private static IReadOnlyList<string> RosterEntries(RepeatDeliveryBudget.Decision decision)
    {
        var list = Fact(decision, RepeatDeliveryBudget.RosterListFactName);
        return string.IsNullOrEmpty(list)
            ? Array.Empty<string>()
            : list!.Split("; ", StringSplitOptions.None);
    }

    private static string? Fact(RepeatDeliveryBudget.Decision decision, string label) =>
        decision.Roster?.Fields.Where(f => f.Label == label).Select(f => f.Value).FirstOrDefault();

    private static AlertOutcome Outcome(string serverName)
    {
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, Incidents(serverName));
        return new AlertOutcome(
            serverName, serverName, Metric, "1", "0",
            context, AlertContextBuilders.ContextToDetailText(context),
            NumericCurrentValue: 1, NumericThresholdValue: 0, Muted: false, Severity: null);
    }

    private static DarlingAlertDeliverer BuildDeliverer(
        CapturingWebhookEndpoint endpoint,
        IAlertHistoryStore history,
        Action<DarlingConfig>? configure = null)
    {
        /* Teams only and no SMTP: one delivered card is one captured body, so the assertion counts posts
           rather than posts times configured channels. */
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = endpoint.Url;
        configure?.Invoke(config);

        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);
    }

    /* SMTP only and no webhook, the mirror of BuildDeliverer: one delivered message is one captured
       message, so the email channel's own budget is measured rather than inferred from the webhook's. */
    private static DarlingAlertDeliverer BuildEmailOnlyDeliverer(
        CapturingSmtpEndpoint smtp, IAlertHistoryStore history)
    {
        var config = new DarlingConfig();
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";

        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);
    }

    /// <summary>
    /// Reports a set of fingerprints as delivered at one instant and everything else as never delivered.
    /// A set rather than <c>IncidentDeliveryFilterTests</c>' single key because this issue is about N
    /// servers at once, and one seeded key would make the fixture N = 1 where the two throttles are
    /// indistinguishable.
    /// </summary>
    private sealed class SeedingSetHistoryStore : IAlertHistoryStore
    {
        private readonly HashSet<string> _seeded;
        private readonly DateTime _sentUtc;

        public SeedingSetHistoryStore(IEnumerable<string> seededDedupKeys, DateTime sentUtc)
        {
            _seeded = new HashSet<string>(seededDedupKeys, StringComparer.Ordinal);
            _sentUtc = sentUtc;
        }

        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        private Task<DateTime?> Seed(string? dedupKey) =>
            Task.FromResult<DateTime?>(dedupKey is not null && _seeded.Contains(dedupKey) ? _sentUtc : null);
    }
}
