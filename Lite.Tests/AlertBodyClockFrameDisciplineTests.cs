/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3421: every timestamp an alert BODY carries declares the frame it is in. The defect this closes is a
/// document holding two frames and labelling neither — a failed-job body rendered <c>Failed At</c> on the
/// monitored server's wall clock beside an <c>alert_time</c> and an <c>Incident Since</c> that are UTC, so
/// on a fleet reporting -240 a two-minute-old failure read as four hours old.
///
/// <para><b>Why this is a new instrument rather than an extension of the #3419 guards, which was checked
/// first.</b> <c>ConsumedTimestampFrameDisciplineTests</c> derives its census by reflecting over
/// <c>CollectorCatalog</c> for columns of type <c>Timestamp</c>, and judges a consumer site by pairing the
/// column's frame against a REGISTERED renderer. Neither half reaches here.
/// <c>FailedJobInfo.RunDateTime</c> is not a catalog column at all — the failed-jobs feed is a live msdb
/// query, not a collected table (see <c>FailedJobsQuery</c>) — so the surface that was MEASURED is
/// structurally invisible to a catalog-derived census, and an extension of it would have guarded the
/// unmeasured half of the same defect while staying blind to the measured half. And the alert builders
/// reach no renderer: <c>PerformanceMonitor.Alerting</c>'s whole project closure is
/// {Notifications, Analysis}, with no offset primitive and no display helper in it, so there is nothing to
/// put in that guard's renderer map. <c>McpPayloadClockFrameDisciplineTests</c> is narrower still: its
/// population is a hand list of MCP reads and Lite MCP tool files, and its matchers key on
/// <c>make_interval(mins =&gt; svr.offset_minutes)</c> and <c>ToString("o")</c>, neither of which occurs
/// anywhere in the two alert projects.
/// </para>
///
/// <para><b>So this one judges the OUTPUT instead of the source.</b> It drives the shipped builders,
/// flattens them through the shipped <see cref="AlertDetailText.Flatten"/> — the function that produces
/// <c>detail_text</c> and every channel's facts — and requires every timestamp-shaped token in the result
/// to carry a declared marker. That is frame-source-agnostic: it holds for a live msdb read, a store read,
/// the engine's own clock, and whatever the next feed turns out to be. It also names no field: a guard
/// spelled <c>Failed At</c> would rot the moment another body embedded an instant, which is exactly how
/// the second affected body (<c>Started</c>, on the long-running-job alert) went unnoticed while the first
/// was being measured.
/// </para>
/// </summary>
public class AlertBodyClockFrameDisciplineTests
{
    private const string Server = "SQL2022";

    /// <summary>
    /// The one instant every fixture is anchored on, and the offset every server-sourced fixture is
    /// planted at. -240 is the measured fleet value; it is the offset the arithmetic below discriminates
    /// with, so it must not be zero — asserted in
    /// <see cref="TheFixtureOffset_IsNonZero_OrNoConversionIsDiscriminated"/>.
    /// </summary>
    private static readonly DateTime AnchorUtc = new(2026, 9, 13, 18, 33, 39, DateTimeKind.Unspecified);

    private const int FleetOffsetMinutes = -240;

    /// <summary>The same instant as <see cref="AnchorUtc"/>, on a server reporting
    /// <see cref="FleetOffsetMinutes"/>.</summary>
    private static DateTime AnchorOnServerClock => AnchorUtc.AddMinutes(FleetOffsetMinutes);

    /* ---------------- the discriminator ---------------- */

    /// <summary>
    /// Anything a reader would take for an instant, in either separator form. Deliberately WIDER than the
    /// shape this product emits: matching only <see cref="AlertTimestamp.Format"/> would make the scan a
    /// restatement of the renderer it is checking, and would miss a round-trip (<c>"O"</c>) or
    /// <c>DateTime.ToString()</c> value arriving from some other path — which is how a
    /// <c>System.Text.Json</c> passthrough would deliver one.
    /// </summary>
    private static readonly Regex TimestampToken =
        new(@"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?", RegexOptions.Compiled);

    /// <summary>
    /// The closed set of markers a timestamp may declare its frame with, taken from the shipped constants
    /// rather than retyped — a copy here would be free to drift from what the renderers emit, and the test
    /// would then be checking its own spelling.
    /// </summary>
    private static readonly string[] DeclaredMarkers =
        [AlertTimestamp.UtcMarker, AlertTimestamp.UnknownOffsetMarker];

    /// <summary>
    /// Every timestamp-shaped token in <paramref name="body"/> that is NOT immediately followed by a
    /// declared marker, each returned with the line it sits on so a failure names the field.
    /// <para>Lines whose label is the dedup fingerprint are skipped — see
    /// <see cref="TheDedupKeyExemption_IsReached_AndIsTheOnlyOne"/> for why that exemption exists and the
    /// assertion that keeps it from becoming an exemption asserting nothing.</para>
    /// </summary>
    private static List<string> UndeclaredStamps(string body)
    {
        var offenders = new List<string>();

        foreach (var rawLine in body.Split('\n'))
        {
            var line = rawLine.TrimEnd('\r');
            if (IsDedupKeyLine(line))
            {
                continue;
            }

            foreach (var match in TimestampToken.Matches(line).Cast<Match>())
            {
                var tail = line.Substring(match.Index + match.Length);
                if (!DeclaredMarkers.Any(m => tail.StartsWith(m, StringComparison.Ordinal)))
                {
                    offenders.Add($"{match.Value} in: {line.Trim()}");
                }
            }
        }

        return offenders;
    }

    private static bool IsDedupKeyLine(string line) =>
        line.TrimStart().StartsWith(AlertIncidentRenderer.DedupKeyFactName + ":", StringComparison.Ordinal);

    /* ---------------- the population ---------------- */

    /// <summary>
    /// The pre-render hook every fingerprinted builder takes, used here to stamp the one instant the
    /// ENGINE contributes to a body: <c>Incident Since</c>. Applied to all eight builders that accept it
    /// so the scan sees <see cref="AlertIncidentRenderer"/>'s own rendering through each of their real
    /// render paths, rather than only through the one it was convenient to exercise. Without it the
    /// fixtures produce no engine-clock stamp at all and the scan only ever judges the two server-sourced
    /// fields.
    /// </summary>
    private static IReadOnlyList<AlertIncident> StampIncidentStart(IReadOnlyList<AlertIncident> incidents) =>
        incidents.Select(i => i with { IncidentStartedUtc = AnchorUtc, TotalOccurrences = 7 }).ToList();

    /// <summary>A minimal parseable deadlock graph — one keylock, two processes, one victim — so the
    /// fingerprint resolves an involved-object set and the incident arm renders.</summary>
    private const string DeadlockGraph = @"<deadlock>
  <victim-list><victimProcess id=""process1""/></victim-list>
  <process-list>
    <process id=""process1"" spid=""71"" currentdbname=""AppDb""><inputbuf>UPDATE t SET c = 1</inputbuf></process>
    <process id=""process2"" spid=""55"" currentdbname=""AppDb""><inputbuf>UPDATE u SET c = 2</inputbuf></process>
  </process-list>
  <resource-list>
    <keylock objectname=""AppDb.dbo.t""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock>
  </resource-list>
</deadlock>";

    /// <summary>
    /// One invocation per public context builder, each planting a timestamp wherever its inputs can carry
    /// one. Paired by NAME against the reflected set in
    /// <see cref="TheInvokedBuilders_AreExactlyTheBuildersThatExist"/>, so a builder added later fails
    /// this file rather than quietly escaping the scan.
    /// </summary>
    private static (string Name, Func<AlertContext?> Invoke)[] Builders() =>
    [
        ("BuildBlockingContext", () => AlertContextBuilders.BuildBlockingContext(
            Server,
            [new BlockedProcessAlertRow
            {
                EventTime = AnchorUtc, DatabaseName = "AppDb", BlockedSpid = 71, BlockingSpid = 55,
                WaitTimeMs = 42_000, LockMode = "X", BlockedSqlText = "SELECT 1", BlockingSqlText = "UPDATE t"
            }],
            [], StampIncidentStart)),

        /* With a parseable graph, so the fingerprinted arm renders — that is the path whose items carry
           the forensic DetailFields, and a graphless row exercises only the fallback victim item. */
        ("BuildDeadlockContext", () => AlertContextBuilders.BuildDeadlockContext(
            Server,
            [new DeadlockAlertRow
            {
                VictimProcessId = "process1", VictimSqlText = "UPDATE t SET c = 1",
                DeadlockGraphXml = DeadlockGraph
            }],
            [], StampIncidentStart)),

        ("BuildFileGrowthContext", () => AlertContextBuilders.BuildFileGrowthContext(
            Server,
            [new DatabaseFileGrowthInfo
            {
                DatabaseName = "AppDb", FileName = "AppDb_log", PhysicalName = @"E:\log\AppDb_log.ldf",
                TotalSizeMb = 81_920, GrowthMb = 20_480, GrowthWindowMinutes = 30,
                VolumeMountPoint = "E:\\", VolumeTotalMb = 512_000, VolumeFreeMb = 40_960,
                AutoGrowthMb = 1_024, MaxSizeMb = -1
            }], StampIncidentStart)),

        ("BuildPoisonWaitContext", () => AlertContextBuilders.BuildPoisonWaitContext(
            [new PoisonWaitDelta
            {
                WaitType = "RESOURCE_SEMAPHORE", AvgMsPerWait = 2_000.5, DeltaMs = 100_000, DeltaTasks = 50,
                CollectionTime = AnchorUtc
            }])),

        ("BuildLongRunningQueryContext", () => AlertContextBuilders.BuildLongRunningQueryContext(
            Server,
            [new LongRunningQueryInfo
            {
                SessionId = 91, DatabaseName = "AppDb", ProgramName = "reporting", QueryText = "SELECT 1",
                ElapsedSeconds = 3_600, CpuTimeMs = 120_000, Reads = 10, Writes = 2,
                WaitType = "CXPACKET", BlockingSessionId = 55, QueryHash = "0x1234"
            }], StampIncidentStart)),

        ("BuildVolumeFreeSpaceContext", () => AlertContextBuilders.BuildVolumeFreeSpaceContext(
            Server,
            [new VolumeFreeSpaceInfo { MountPoint = "E:\\", TotalMb = 512_000, FreeMb = 10_240 }], StampIncidentStart)),

        ("BuildPvsPressureContext", () => AlertContextBuilders.BuildPvsPressureContext(
            Server,
            [new PvsPressureInfo
            {
                DatabaseName = "AppDb", PvsSizeMb = 40_960, DatabaseDataSizeMb = 102_400,
                CurrentAbortedTransactionCount = 3, AbortedCleanupOngoing = true
            }], StampIncidentStart)),

        ("BuildTempDbSpaceContext", () => AlertContextBuilders.BuildTempDbSpaceContext(
            new TempDbSpaceInfo
            {
                TotalReservedMb = 40_960, UnallocatedMb = 1_024, UserObjectReservedMb = 30_000,
                InternalObjectReservedMb = 8_000, VersionStoreReservedMb = 2_960,
                TopConsumerSessionId = 91, TopConsumerMb = 12_000, MaxSizeMb = 51_200
            })),

        /* The two server-sourced instants. Planted on the server's clock with the fleet offset, which is
           the shape the defect was measured in. */
        ("BuildAnomalousJobContext", () => AlertContextBuilders.BuildAnomalousJobContext(
            Server,
            [new AnomalousJobInfo
            {
                JobName = "hourly-load", CurrentDurationSeconds = 3_660, AvgDurationSeconds = 90,
                P95DurationSeconds = 45, PercentOfAverage = 350m,
                StartTime = AnchorOnServerClock, UtcOffsetMinutes = FleetOffsetMinutes
            }], StampIncidentStart)),

        ("BuildFailedJobContext", () => AlertContextBuilders.BuildFailedJobContext(
            Server,
            [new FailedJobInfo
            {
                JobName = "nightly-export", RunDateTime = AnchorOnServerClock,
                UtcOffsetMinutes = FleetOffsetMinutes, StepId = 2, StepName = "extract",
                Message = "the step failed"
            }],
            StampIncidentStart, windowEndUtc: AnchorUtc, lookbackMinutes: 60)),
    ];

    /// <summary>
    /// How many timestamp tokens the fixture set must actually produce. Without this the whole scan passes
    /// on a fixture set that renders none, which is the loop-bound failure mode: a check asserting a
    /// property of an empty population. MEASURED at 12 — one <c>Incident Since</c> from each of the eight
    /// fingerprinted builders, plus <c>Started</c>, <c>Failed At</c> and the failure window's two bounds.
    /// A floor rather than an equality so an added compliant stamp does not have to edit it; the
    /// composition is held from the other side by <see cref="MinimumBodiesCarryingATimestamp"/>.
    /// </summary>
    private const int MinimumTimestampTokens = 12;

    /// <summary>
    /// How many of the bodies must carry at least one timestamp. Pinned beside the token total because the
    /// total alone is satisfied by twelve stamps in one body: a decorator that stopped stamping seven of
    /// the eight fingerprinted builders, or a fixture whose incidents stopped resolving, would keep a
    /// plausible-looking total while the scan stopped reaching most of the render paths.
    /// </summary>
    private const int MinimumBodiesCarryingATimestamp = 8;

    [Fact]
    public void TheFixtureOffset_IsNonZero_OrNoConversionIsDiscriminated()
    {
        /* Its own test rather than an inline guard: at an offset of 0 every assertion below passes while
           proving nothing about direction, and a constant edited to 0 in passing is the one mutation a
           residual check is otherwise blind to. */
        Assert.NotEqual(0, FleetOffsetMinutes);
        Assert.NotEqual(AnchorUtc, AnchorOnServerClock);
        Assert.Equal(AnchorUtc, AnchorOnServerClock.AddMinutes(-FleetOffsetMinutes));
    }

    [Fact]
    public void TheInvokedBuilders_AreExactlyTheBuildersThatExist()
    {
        var reflected = typeof(AlertContextBuilders)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name.StartsWith("Build", StringComparison.Ordinal)
                     && m.Name.EndsWith("Context", StringComparison.Ordinal)
                     && m.ReturnType == typeof(AlertContext))
            .Select(m => m.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        var invoked = Builders().Select(b => b.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();

        /* Ordered sequences, not counts: two totals agree under a swap that moved a builder out of the set
           and an unrelated name in. */
        Assert.Equal(reflected, invoked);

        /* And the census is closed, not a floor: a builder that vanishes has to be a deliberate edit here
           rather than a silently shorter scan. */
        Assert.Equal(10, reflected.Count);
    }

    [Fact]
    public void EveryAlertBody_DeclaresTheFrameOfEveryTimestampItCarries()
    {
        int tokens = 0;
        int bodiesWithATimestamp = 0;
        var failures = new List<string>();

        foreach (var (name, invoke) in Builders())
        {
            var context = invoke();
            Assert.NotNull(context);

            var body = AlertDetailText.Flatten(context);
            Assert.False(string.IsNullOrWhiteSpace(body), $"{name} flattened to nothing — the fixture is not exercising it");

            var inThisBody = TimestampToken.Matches(body!).Count;
            tokens += inThisBody;
            if (inThisBody > 0)
            {
                bodiesWithATimestamp++;
            }

            failures.AddRange(UndeclaredStamps(body!).Select(o => $"{name}: {o}"));
        }

        Assert.True(tokens >= MinimumTimestampTokens,
            $"the fixture set rendered only {tokens} timestamp token(s) — below {MinimumTimestampTokens}, so this scan is asserting a property of an almost-empty population");

        Assert.True(bodiesWithATimestamp >= MinimumBodiesCarryingATimestamp,
            $"only {bodiesWithATimestamp} of the bodies carried a timestamp at all — below {MinimumBodiesCarryingATimestamp}, so most of the render paths are unreached");

        Assert.Equal(new List<string>(), failures);
    }

    [Fact]
    public void TheTwoServerSourcedInstants_RenderInUtc_NotTheServersOwnClock()
    {
        /* The arithmetic, stated against the instant rather than against a pinned string: a pinned string
           passes under a sign error as readily as under the right sign, because the expectation and the
           implementation get written from the same belief. */
        var expected = AlertTimestamp.Utc(AnchorUtc);

        var failed = Field(AlertContextBuilders.BuildFailedJobContext(
            Server,
            [new FailedJobInfo
            {
                JobName = "nightly-export", RunDateTime = AnchorOnServerClock,
                UtcOffsetMinutes = FleetOffsetMinutes
            }]), "Failed At");
        Assert.Equal(expected, failed);

        var started = Field(AlertContextBuilders.BuildAnomalousJobContext(
            Server,
            [new AnomalousJobInfo
            {
                JobName = "hourly-load", AvgDurationSeconds = 90,
                StartTime = AnchorOnServerClock, UtcOffsetMinutes = FleetOffsetMinutes
            }]), "Started");
        Assert.Equal(expected, started);

        /* A different offset must move the answer, or the conversion is not reading the offset at all. */
        var eastOfUtc = Field(AlertContextBuilders.BuildFailedJobContext(
            Server,
            [new FailedJobInfo
            {
                JobName = "nightly-export", RunDateTime = AnchorOnServerClock, UtcOffsetMinutes = 330
            }]), "Failed At");
        Assert.NotEqual(expected, eastOfUtc);
        Assert.Equal(AlertTimestamp.Utc(AnchorOnServerClock.AddMinutes(-330)), eastOfUtc);

        /* And a zero offset must leave the instant alone, which is what separates "applies the offset" from
           "applies some constant". */
        Assert.Equal(
            AlertTimestamp.Utc(AnchorOnServerClock),
            Field(AlertContextBuilders.BuildFailedJobContext(
                Server,
                [new FailedJobInfo
                {
                    JobName = "nightly-export", RunDateTime = AnchorOnServerClock, UtcOffsetMinutes = 0
                }]), "Failed At"));
    }

    [Fact]
    public void AnUnconvertibleInstant_SaysSo_RatherThanClaimingUtc()
    {
        /* No offset collected for the server. The instant is still reported — dropping it would lose the
           only thing the reader came for — but it is not dressed as UTC. */
        var failed = Field(AlertContextBuilders.BuildFailedJobContext(
            Server,
            [new FailedJobInfo { JobName = "nightly-export", RunDateTime = AnchorOnServerClock }]), "Failed At");

        Assert.Equal(
            AnchorOnServerClock.ToString(AlertTimestamp.Format, System.Globalization.CultureInfo.InvariantCulture)
                + AlertTimestamp.UnknownOffsetMarker,
            failed);
        Assert.DoesNotContain(AlertTimestamp.Utc(AnchorUtc), failed);

        var started = Field(AlertContextBuilders.BuildAnomalousJobContext(
            Server,
            [new AnomalousJobInfo { JobName = "hourly-load", StartTime = AnchorOnServerClock }]), "Started");
        Assert.EndsWith(AlertTimestamp.UnknownOffsetMarker, started, StringComparison.Ordinal);

        /* A row whose instant column was null arrives as the sentinel default, and shifting that past
           DateTime's floor must not throw out of the render path. */
        var sentinel = Field(AlertContextBuilders.BuildAnomalousJobContext(
            Server,
            [new AnomalousJobInfo { JobName = "hourly-load", StartTime = DateTime.MinValue, UtcOffsetMinutes = 330 }]),
            "Started");
        Assert.EndsWith(AlertTimestamp.UnknownOffsetMarker, sentinel, StringComparison.Ordinal);
    }

    /// <summary>
    /// The conversion is exact over the whole input domain and returns null at the edges rather than
    /// throwing, because the sentinel a null instant column arrives as sits AT the edge — and because a
    /// helper that threw would take the render path down with it on a row nobody could read anyway.
    /// </summary>
    [Fact]
    public void TheConversion_IsExactAtEveryOffset_AndReturnsNullRatherThanOverflowing()
    {
        Assert.Equal(AnchorUtc, AlertTimestamp.ToUtc(AnchorOnServerClock, FleetOffsetMinutes));
        Assert.Equal(AnchorOnServerClock, AlertTimestamp.ToUtc(AnchorOnServerClock, 0));
        Assert.Equal(AnchorUtc.AddMinutes(-810), AlertTimestamp.ToUtc(AnchorUtc, 810));
        Assert.Null(AlertTimestamp.ToUtc(AnchorUtc, null));

        /* Off both edges, by one minute and by the largest offset an int can hold. Null, never a throw:
           the sentinel a null instant column arrives as IS DateTime.MinValue. */
        Assert.Null(AlertTimestamp.ToUtc(DateTime.MinValue, 1));
        Assert.Null(AlertTimestamp.ToUtc(DateTime.MaxValue, -1));
        Assert.Null(AlertTimestamp.ToUtc(DateTime.MinValue, int.MaxValue));
        Assert.Null(AlertTimestamp.ToUtc(DateTime.MaxValue, int.MinValue));

        /* The same extremes in the direction that stays representable are CONVERTED, not refused — the
           bound rejects only what it must, and the shift is still exact 2.1e9 minutes out. Measured by
           differencing in minutes rather than by restating the tick arithmetic, which would assert the
           implementation against itself. */
        foreach (var (from, offset) in new[]
        {
            (DateTime.MinValue, int.MinValue),
            (DateTime.MaxValue, int.MaxValue),
        })
        {
            var shifted = AlertTimestamp.ToUtc(from, offset);
            Assert.NotNull(shifted);
            Assert.Equal(-(long)offset, (long)(shifted!.Value - from).TotalMinutes);
        }

        /* A zero offset at either edge is the identity, so the bound is not rejecting the edges outright. */
        Assert.Equal(DateTime.MinValue, AlertTimestamp.ToUtc(DateTime.MinValue, 0));
        Assert.Equal(DateTime.MaxValue, AlertTimestamp.ToUtc(DateTime.MaxValue, 0));
    }

    [Fact]
    public void TheFailedJobBody_StatesTheWindowItReports_InTheSameFrame()
    {
        /* The overlap decision, pinned: consecutive bodies re-report a failure the previous body carried,
           because the window is longer than the interval between firings. The body is not narrowed to
           "new since last alert" (see BuildFailedJobContext's remarks), so it states the window instead —
           which is what lets a reader tell a recurrence from the same failure in a later window. */
        var context = AlertContextBuilders.BuildFailedJobContext(
            Server,
            [new FailedJobInfo
            {
                JobName = "nightly-export", RunDateTime = AnchorOnServerClock,
                UtcOffsetMinutes = FleetOffsetMinutes
            }],
            windowEndUtc: AnchorUtc, lookbackMinutes: 60);

        var window = Assert.Single(
            context!.Details, d => d.Heading == AlertContextBuilders.FailureWindowHeading);

        Assert.Equal(
            new List<(string, string)>
            {
                (AlertContextBuilders.FailureWindowFromLabel, AlertTimestamp.Utc(AnchorUtc.AddMinutes(-60))),
                (AlertContextBuilders.FailureWindowToLabel, AlertTimestamp.Utc(AnchorUtc))
            },
            window.Fields);

        /* The reported failure has to fall INSIDE the stated window, or the item is decoration. */
        var failedAt = Field(context, "Failed At");
        Assert.True(
            string.CompareOrdinal(failedAt, window.Fields[0].Value) >= 0 &&
            string.CompareOrdinal(failedAt, window.Fields[1].Value) <= 0,
            $"{failedAt} is outside the window this body claims to report ({window.Fields[0].Value} .. {window.Fields[1].Value})");

        /* Omitted when the caller supplies no clock — the window is a fact about the read, so it is stated
           only when it is actually known. */
        var noWindow = AlertContextBuilders.BuildFailedJobContext(
            Server, [new FailedJobInfo { JobName = "nightly-export", RunDateTime = AnchorOnServerClock }]);
        Assert.DoesNotContain(noWindow!.Details, d => d.Heading == AlertContextBuilders.FailureWindowHeading);
    }

    [Fact]
    public void TheDedupKeyExemption_IsReached_AndIsTheOnlyOne()
    {
        /* A dedup key is an opaque fingerprint, not a reported instant, and one shipped key embeds one:
           DarlingWorker's vanished-blocker sentinel builds "0-pid<n>-<captured_at:O>". It is exempt
           because its STABILITY is the contract — it keys the #1154 per-fingerprint cooldown, the
           persisted occurrence state and #3313's delivery filter, so reformatting it would rotate every
           stored key and break cooldown continuity across an upgrade for a purely cosmetic gain.
           Asserted REACHED rather than merely declared: an exemption no input exercises is an exemption
           asserting nothing. */
        var incident = new AlertIncident(
            DedupKey: $"0-pid{4242}-{AnchorUtc:O}",
            InvolvedObjects: ["public.orders"],
            OccurrenceCount: 2,
            WaitRange: "1.5s-4.2s",
            Database: "appdb");

        var item = AlertIncidentRenderer.BuildItem(incident, "Incident", includeDetailFields: false);
        var body = AlertDetailText.Flatten(new AlertContext { Details = { item } })!;

        /* The exemption is reached: the raw scan sees an offender, and the exempting scan does not. */
        var dedupLine = body.Split('\n').Select(l => l.TrimEnd('\r')).Single(IsDedupKeyLine);
        Assert.Matches(TimestampToken, dedupLine);
        Assert.Empty(UndeclaredStamps(body));

        /* And it is scoped to that one fact name: the same token under any other label is still an
           offender, so the exemption cannot be widened by accident. */
        var relabelled = body.Replace(AlertIncidentRenderer.DedupKeyFactName + ":", "Some Other Fact:", StringComparison.Ordinal);
        Assert.NotEmpty(UndeclaredStamps(relabelled));
    }

    [Fact]
    public void TheDiscriminator_FlagsEveryUndeclaredShape_AndPassesTheDeclaredOnes()
    {
        /* Red-first on the subject is not enough — the scan itself has to be shown to discriminate, or a
           crippled regex reports a clean census over every body in the tree. */

        foreach (var undeclared in new[]
        {
            "  Failed At: 2026-09-13 14:31:00",                      /* the shipped defect, verbatim */
            "  Failed At: 2026-09-13 14:31",                         /* minute resolution */
            "  Window: 2026-09-13T14:31:00.0000000",                 /* round-trip, no marker */
            "  Failed At: 2026-09-13 14:31:00 UTC",                  /* a frame word, not a declared marker */
            "  Failed At: 2026-09-13 14:31:00 (server local)",        /* nearly the marker, not it */
            "  Failed At: 2026-09-13 14:31:00 — see Z below",        /* a Z on the line but not on the value */
        })
        {
            Assert.NotEmpty(UndeclaredStamps(undeclared));
        }

        foreach (var declared in new[]
        {
            "  Failed At: " + AlertTimestamp.Utc(AnchorUtc),
            "  Failed At: " + AlertTimestamp.ServerClockUnconverted(AnchorOnServerClock),
            "  Incident Since: 2026-08-12 14:00:00Z",                /* the pre-existing compliant form */
            "  Window: 2026-09-13 17:33:39Z → 2026-09-13 18:33:39Z", /* two markers on one line */
            "  Current Duration: 1h 1m",                             /* a duration is not an instant */
            "  Growth: 20.0 GB in 30 min (40960 MB/hr)",
            "  Query ID: 12345",
        })
        {
            Assert.Empty(UndeclaredStamps(declared));
        }

        /* Both markers must be load-bearing: dropping either from the closed set has to reopen a case the
           full set closes. */
        foreach (var marker in DeclaredMarkers)
        {
            var others = DeclaredMarkers.Where(m => !string.Equals(m, marker, StringComparison.Ordinal)).ToArray();
            var line = "  Failed At: " + AnchorUtc.ToString(AlertTimestamp.Format, System.Globalization.CultureInfo.InvariantCulture) + marker;
            Assert.False(
                TimestampToken.Matches(line).Cast<Match>().All(match =>
                    others.Any(m => line.Substring(match.Index + match.Length).StartsWith(m, StringComparison.Ordinal))),
                $"the marker set closes the same cases without {marker.Trim()} — it is not load-bearing");
        }
    }

    [Fact]
    public void TheDeclaredMarkers_AreTakenFromTheShippedConstants()
    {
        /* The renderers and this scan must agree by construction. If the constants were retyped here, an
           edit to the renderer's spelling would leave the scan green against a body nothing reads
           correctly. */
        Assert.Equal("Z", AlertTimestamp.UtcMarker);
        Assert.Equal("yyyy-MM-dd HH:mm:ss", AlertTimestamp.Format);
        Assert.Contains(AlertTimestamp.UtcMarker, DeclaredMarkers);
        Assert.Contains(AlertTimestamp.UnknownOffsetMarker, DeclaredMarkers);
        Assert.Equal(2, DeclaredMarkers.Length);

        /* The two markers must be distinguishable from each other, not merely different strings: the
           UTC marker must not be a prefix of the other, or "ends with Z" would accept both. */
        Assert.DoesNotContain(AlertTimestamp.UtcMarker, AlertTimestamp.UnknownOffsetMarker);
    }

    private static string Field(AlertContext? context, string label)
    {
        Assert.NotNull(context);
        return Assert.Single(
            context!.Details.SelectMany(d => d.Fields), f => f.Label == label).Value;
    }
}
