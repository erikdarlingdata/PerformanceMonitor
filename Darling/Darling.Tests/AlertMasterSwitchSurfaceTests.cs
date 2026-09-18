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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3464: <c>alerts_enabled</c> is the master switch, and EVERY path that can reach a delivery channel
/// consults it — enforced from source, so a third bypass fails red in CI instead of shipping silently.
///
/// <para>The defect this file exists for was measured, not theorized. With <c>alerts_enabled</c> read back
/// false on both SQL Server stores, two families kept delivering to a live paging channel: an Analysis
/// INFO card 38 minutes into the mute (the notify decision was made from the family toggle ALONE), and a
/// Collector Cost Regression 60 minutes in (the paging apply ran BEFORE the method's only AlertsEnabled
/// consult, which guarded the digest branch) — while the engine sweep's own <c>server_alert_passes</c>
/// counters sat byte-identical across consecutive hourly reads, proving the switch was honored by the
/// sweep and bypassed by its siblings. Closing the two measured bypasses closed two instances of a class;
/// this census is the class-level fix, the same discipline as the #3013 read-failure surface: the call
/// sites are enumerated FROM SOURCE, each must either consult the master switch (inline, or through a
/// verified single-caller chain whose gate is asserted) or carry a stated exemption, and the file set
/// itself is derived by scanning the production tree — so a new delivery path in a new file cannot ship
/// unclassified any more than a new site in an old one can.</para>
///
/// <para><b>What "gated" means is the semantics the switch's own contract promises</b> (Darling/README.md,
/// the alerts section, pinned below): <c>enabled: false</c> turns off all alert EVALUATION — the engine,
/// the self-alerts, the PostgreSQL predictors and the custom rules stop evaluating, record nothing, and
/// freeze their edge state where it stands — and turns off scheduled-analysis finding NOTIFICATIONS while
/// the analysis itself still runs and persists. The one deliberate refinement is the connection edge's
/// "track always, deliver conditionally" split, which predates this issue and is the idiom precedent the
/// AND'd gates follow.</para>
/// </summary>
public sealed class AlertMasterSwitchSurfaceTests
{
    /* ---------------- the delivery-call census ---------------- */

    /// <summary>
    /// A call that can put an alert on a channel: the shared deliverer seam (<c>DeliverAsync</c>, and its
    /// #3580 reporting twin <c>DeliverAndReportAsync</c> — the same send, answering what the channels did,
    /// which the self-alert funnel now calls so the two daily documents can stamp delivered-today), the
    /// analysis notify seam (<c>NotifyAsync</c> / <c>SendFindingAlertAsync</c>), Lite's direct send seam
    /// (<c>TrySendAlertEmailAsync</c>), the shared send core (<c>TrySendAsync</c>), and the deliberate
    /// channel-probe statics (<c>SendTest*</c>). Dot-qualified on purpose: a DECLARATION has no receiver,
    /// so requiring the dot enumerates calls without a parallel exclusion list for signatures. Matched on
    /// comment-and-string-stripped source, so prose mentioning a seam is not a site.
    /// </summary>
    private static readonly Regex s_deliveryCall = new(
        @"\??\.\s*(?:DeliverAsync|DeliverAndReportAsync|NotifyAsync|TrySendAlertEmailAsync|TrySendAsync|SendFindingAlertAsync|SendTestPagerDutyAsync|SendTestTeamsAsync|SendTestSlackAsync|SendTestGenericAsync)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>How a censused site pays for its place on a delivery path.</summary>
    private enum GateMode
    {
        /// <summary>The enclosing member consults the master switch BEFORE its first delivery call.</summary>
        Inline,

        /// <summary>The member is reached only through a caller whose master consult — and sole-caller
        /// status — a dedicated pin in this file asserts.</summary>
        GatedCaller,

        /// <summary>Shared machinery BELOW the gate: every path into it is one of the censused sites
        /// above, so a consult here would be a second answer to a question already answered.</summary>
        Funnel,

        /// <summary>An operator clicked a button whose entire purpose is to verify a channel. A test send
        /// that honored the master switch could not verify the channel while a fleet is muted — which is
        /// exactly when an operator wants to know the webhook still works.</summary>
        OperatorProbe,
    }

    private sealed record CensusEntry(string File, string Member, int Sites, GateMode Mode, string Why);

    /// <summary>
    /// Every (file, member) allowed to hold a delivery call, how many it holds, and why that is paid for.
    /// The counts are exact in both directions: a site ADDED to a gated member still has to sit after the
    /// gate (asserted), a site added anywhere else changes a count or lands in an unlisted member, and a
    /// site REMOVED forces this table to say so. File paths are repo-relative with '/' separators.
    /// </summary>
    private static readonly CensusEntry[] s_census =
    {
        /* The shared engine: one deliver site, in the in-file private funnel every family routes through
           (#1681's logging choke point). The master gate is the ENTRY — EvaluateServerAsync's early return,
           pinned by TheEngineEntry_GatesBeforeEvaluating — and FireAsync is private, so no path reaches it
           except through that entry. This is the consult whose frozen server_alert_passes counters proved
           the sweep genuinely stopped while the bypasses kept delivering. */
        new("PerformanceMonitor.Alerting/AlertEngine.cs", "FireAsync", 1, GateMode.Funnel,
            "in-file private funnel below the pinned EvaluateServerAsync master gate"),

        /* The self-alert evaluator: one deliver site, in ITS in-file funnel. The per-family census —
           every member that calls FireAsync consults the master switch or is a verified callee of one
           that does — is EverySelfAlertFamily_ConsultsTheMasterSwitch below. */
        new("Darling/PerformanceMonitor.Darling.Service/DarlingSelfAlertEvaluator.cs", "FireAsync", 1, GateMode.Funnel,
            "in-file private funnel; every FireAsync caller is censused by EverySelfAlertFamily_ConsultsTheMasterSwitch"),

        /* The PostgreSQL predictors (#3464 closed the gap #3013's comment had named and left open): the
           group entry consults the master switch inline, before RecordPass and before any read; the five
           per-condition helpers below it are called from that entry alone. */
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "EvaluatePostgresAlertsAsync", 1, GateMode.Inline,
            "config.Alerts.Enabled"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "EvaluatePgCpuAsync", 1, GateMode.GatedCaller,
            "sole caller EvaluatePostgresAlertsAsync gates on config.Alerts.Enabled (pinned)"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "EvaluatePgDeadlocksAsync", 1, GateMode.GatedCaller,
            "sole caller EvaluatePostgresAlertsAsync gates on config.Alerts.Enabled (pinned)"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "EvaluatePgBlockingAsync", 1, GateMode.GatedCaller,
            "sole caller EvaluatePostgresAlertsAsync gates on config.Alerts.Enabled (pinned)"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "EvaluatePgLongRunningQueryAsync", 1, GateMode.GatedCaller,
            "sole caller EvaluatePostgresAlertsAsync gates on config.Alerts.Enabled (pinned)"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "EvaluatePgPoisonWaitAsync", 1, GateMode.GatedCaller,
            "sole caller EvaluatePostgresAlertsAsync gates on config.Alerts.Enabled (pinned)"),

        /* Analysis notifications, the FIRST measured bypass: the notify decision reaches this one site
           through the notifyFindings parameter, and both entry points now compute it through
           ShouldNotifyAnalysisFindings — master AND family — pinned in both directions below. */
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "RunAnalysisPassAsync", 1, GateMode.GatedCaller,
            "guarded by notifyFindings, which every entry point computes via ShouldNotifyAnalysisFindings (pinned)"),

        /* #3467: the same-statement-pileup finding's delivery — the collection-cadence analysis path.
           Same family as the scheduled pass's site above and the same predicate, consulted INLINE
           because this member has no notifyFindings parameter to inherit: the finding is produced and
           persisted unconditionally (D0) and the one NotifyAsync sits under
           ShouldNotifyAnalysisFindings(config), master AND family. */
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "EvaluateSameStatementPileupAsync", 1, GateMode.Inline,
            "ShouldNotifyAnalysisFindings(config)"),

        /* The user-authored rules (#3464 gated them): the public entry consults the injected master
           switch before the rule load; the deliver site sits two private hops below it, chain pinned. */
        new("Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs", "DeliverFireAsync", 1, GateMode.GatedCaller,
            "reached only via EvaluateRuleForServerAsync from EvaluateServerAsync, whose _alertsEnabled() gate is pinned"),

        /* Funnels below the gates. */
        new("Darling/PerformanceMonitor.Darling.Service/DarlingAlertDeliverer.cs", "SendAndRecordAsync", 1, GateMode.Funnel,
            "the shared record-and-send seam; every caller of DeliverAsync is a censused site"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingFindingAlertSender.cs", "SendFindingAlertAsync", 1, GateMode.Funnel,
            "the analysis finding sender; reached only through AnalysisNotificationService, whose callers are censused"),
        new("PerformanceMonitor.Notifications/AnalysisNotificationService.cs", "NotifyAsync", 1, GateMode.Funnel,
            "shared by both SKUs; the master consult lives at its two censused call sites, where each SKU's live settings are"),
        new("Lite/Services/EmailAlertService.cs", "TrySendAlertEmailAsync", 1, GateMode.Funnel,
            "Lite's record-and-send seam; every caller is a censused site"),
        new("Lite/Services/LiteAlertDeliverer.cs", "SendAlert", 1, GateMode.Funnel,
            "the shared engine's Lite deliverer, below the pinned EvaluateServerAsync master gate"),

        /* Lite's direct senders: gated by their callers, both chains pinned. */
        new("Lite/MainWindow.AlertEngine.cs", "SendConnectionAlert", 1, GateMode.GatedCaller,
            "called only inside MainWindow.xaml.cs's 'App.AlertsEnabled && App.NotifyConnectionChanges' block (pinned)"),
        new("Lite/MainWindow.AlertEngine.cs", "SendAgAlert", 1, GateMode.GatedCaller,
            "called only from EvaluateAvailabilityGroupAlertsAsync, reached only from the master-gated CheckPerformanceAlerts (pinned)"),

        /* Lite's analysis notify, the same first bypass in the other SKU: the notify local comes from the
           Lite twin of ShouldNotifyAnalysisFindings, pinned in both directions below. */
        new("Lite/Services/CollectionBackgroundService.cs", "RunAnalysisIfDueAsync", 1, GateMode.Inline,
            "ShouldNotifyAnalysisFindings"),

        /* #3467: Lite's pileup sweep — the Darling entry's twin, same family, same predicate, same D0
           split (production unconditional, delivery gated). */
        new("Lite/Services/CollectionBackgroundService.cs", "RunPileupSweepAsync", 1, GateMode.Inline,
            "ShouldNotifyAnalysisFindings"),

        /* The channel probes: four per settings window, one per channel shape. */
        new("Lite/Windows/SettingsWindow.xaml.cs", "TestPagerDutyButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification; must work mid-mute or it cannot verify the channel an un-mute will need"),
        new("Lite/Windows/SettingsWindow.xaml.cs", "TestTeamsButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification"),
        new("Lite/Windows/SettingsWindow.xaml.cs", "TestSlackButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification"),
        new("Lite/Windows/SettingsWindow.xaml.cs", "TestGenericButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification"),
        new("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs", "TestPagerDutyButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification"),
        new("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs", "TestTeamsButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification"),
        new("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs", "TestSlackButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification"),
        new("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs", "TestGenericButton_Click", 1, GateMode.OperatorProbe,
            "explicit channel verification"),
    };

    [Fact]
    public void EveryDeliveryCallSite_IsCensusedAndAccountedFor()
    {
        /* 1) Derive the file set from the production tree, not from the table: a delivery call in a file
           this table has never heard of must fail here, naming the file. */
        var actualFiles = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var path in ProductionSources())
        {
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
            if (s_deliveryCall.IsMatch(stripped))
            {
                actualFiles.Add(Relative(path));
            }
        }

        var censusFiles = new SortedSet<string>(s_census.Select(e => e.File), StringComparer.Ordinal);
        Assert.True(
            censusFiles.SetEquals(actualFiles),
            "#3464 census drift. Files with delivery calls but no census entry: ["
            + string.Join(", ", actualFiles.Except(censusFiles))
            + "]; census entries whose file no longer has a delivery call: ["
            + string.Join(", ", censusFiles.Except(actualFiles))
            + "]. A new delivery path must consult alerts_enabled (the AND'd idiom) and be classified here, "
            + "or carry a stated exemption.");

        /* 2) Within each file, attribute every site to its enclosing member and hold the table to exact
           counts — added, moved and removed sites all surface as a diff a person has to re-justify. */
        foreach (var file in actualFiles)
        {
            var raw = Read(file);
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);
            var members = MemberStarts(stripped);

            var actualSites = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var line in SiteLines(stripped))
            {
                var member = EnclosingMember(members, line);
                Assert.False(member is null,
                    $"#3464 census: {file} line {line + 1} holds a delivery call before any recognizable member "
                    + "declaration — classify it.");
                actualSites[member!] = actualSites.TryGetValue(member!, out var n) ? n + 1 : 1;
            }

            var expected = s_census.Where(e => e.File == file)
                .ToDictionary(e => e.Member, e => e.Sites, StringComparer.Ordinal);

            Assert.True(
                expected.Count == actualSites.Count && expected.All(kv => actualSites.TryGetValue(kv.Key, out var n) && n == kv.Value),
                $"#3464 census drift in {file}. Expected sites: "
                + string.Join(", ", expected.Select(kv => $"{kv.Key}={kv.Value}"))
                + "; found: "
                + string.Join(", ", actualSites.Select(kv => $"{kv.Key}={kv.Value}"))
                + ". Re-justify the change here — an unclassified delivery site is the #3464 failure mode.");

            /* 3) An Inline entry's gate must sit BEFORE its first site, in the same member — a gate below
               the send is a comment, not a gate. */
            foreach (var entry in s_census.Where(e => e.File == file && e.Mode == GateMode.Inline))
            {
                var (start, end) = MemberRange(members, stripped, entry.Member);
                var firstSite = SiteLines(stripped).First(l => l >= start && l < end);
                var gateAt = LineOf(stripped, entry.Why, start, firstSite);
                Assert.True(gateAt >= 0,
                    $"#3464: {file} {entry.Member} must consult '{entry.Why}' before its first delivery call "
                    + $"(line {firstSite + 1}), and does not.");
            }
        }
    }

    /* ---------------- the self-alert per-family census ---------------- */

    /// <summary>
    /// The two families whose master consult lives in <see cref="DarlingSelfAlertEvaluator"/>'s
    /// EvaluateStoreAlertsAsync rather than in the apply itself — the store-polled pair that evaluate is
    /// the sole caller of, pinned as such below.
    /// </summary>
    private static readonly string[] s_evaluateGatedApplies = { "ApplyCollectionStoppedAsync", "ApplyCaptureDownAsync" };

    /// <summary>
    /// #3464 gap 2, generalized: every member of the self-alert evaluator that fires must consult
    /// <c>_settings.AlertsEnabled</c> before its first fire, or be one of the two applies whose gate is
    /// their single verified caller. ApplyCostRegressionsAsync was the member this failed for — its only
    /// consult sat in the digest branch of EvaluateCollectorCostAsync, AFTER the paging apply had already
    /// run — and the measured result was a Collector Cost Regression on a live channel an hour into a
    /// fleet-wide mute.
    /// </summary>
    [Fact]
    public void EverySelfAlertFamily_ConsultsTheMasterSwitch()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            Read("Darling/PerformanceMonitor.Darling.Service/DarlingSelfAlertEvaluator.cs"));
        var members = MemberStarts(stripped);
        var lines = stripped.Split('\n');

        var offenders = new List<string>();
        var firing = new Dictionary<string, int>(StringComparer.Ordinal);

        for (var i = 0; i < lines.Length; i++)
        {
            /* A firing call, not the funnel's own declaration: FireAsync is called bare (same class). The
               declaration's return type is generic since #3580 (Task<AlertDelivery?> — the funnel reports
               what the channels did), so the exclusion allows a type-argument list on the Task. */
            if (!Regex.IsMatch(lines[i], @"(?<![\w.])FireAsync\s*\(") || Regex.IsMatch(lines[i], @"\bTask(?:<[^>]*>)?\s+FireAsync\s*\("))
            {
                continue;
            }

            var member = EnclosingMember(members, i);
            Assert.NotNull(member);
            if (!firing.ContainsKey(member!))
            {
                firing[member!] = i;

                if (s_evaluateGatedApplies.Contains(member))
                {
                    continue;
                }

                var (start, _) = MemberRange(members, stripped, member!);
                if (LineOf(stripped, "_settings.AlertsEnabled", start, i) < 0)
                {
                    offenders.Add(member!);
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "#3464: self-alert member(s) fire without consulting _settings.AlertsEnabled first: ["
            + string.Join(", ", offenders)
            + "]. Gate the apply (the sibling shape) or add a verified-caller entry with its pin.");

        /* The census found the twelve-plus families at all — a regex drift that enumerated nothing would
           otherwise pass vacuously while enforcing nothing. */
        Assert.True(firing.Count >= 14,
            $"#3464: the FireAsync census enumerated only {firing.Count} firing members; the walker regressed.");

        /* The two evaluate-gated applies: their gate is EvaluateStoreAlertsAsync, so that member must
           consult the switch before calling them, and nothing else in production may call them. */
        var (evalStart, evalEnd) = MemberRange(members, stripped, "EvaluateStoreAlertsAsync");
        foreach (var apply in s_evaluateGatedApplies)
        {
            var callAt = LineOf(stripped, apply + "(", evalStart, evalEnd);
            Assert.True(callAt >= 0, $"#3464 pin: EvaluateStoreAlertsAsync no longer calls {apply}");
            Assert.True(LineOf(stripped, "_settings.AlertsEnabled", evalStart, callAt) >= 0,
                $"#3464: EvaluateStoreAlertsAsync must consult _settings.AlertsEnabled before calling {apply}");

            AssertSoleProductionCallers(apply,
                allowedFiles: new[] { "Darling/PerformanceMonitor.Darling.Service/DarlingSelfAlertEvaluator.cs" });
        }
    }

    /* ---------------- gate-shape pins: the chains the census table leans on ---------------- */

    /// <summary>The engine's consult — the one the frozen counters proved works — must stay the entry's
    /// first act, ahead of the per-server serialization and the core sweep.</summary>
    [Fact]
    public void TheEngineEntry_GatesBeforeEvaluating()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(Read("PerformanceMonitor.Alerting/AlertEngine.cs"));
        var members = MemberStarts(stripped);
        var (start, end) = MemberRange(members, stripped, "EvaluateServerAsync");

        var core = LineOf(stripped, "EvaluateCoreAsync(", start, end);
        Assert.True(core >= 0, "#3464 pin: EvaluateServerAsync no longer routes through EvaluateCoreAsync");
        Assert.True(LineOf(stripped, "!_settings.AlertsEnabled", start, core) >= 0,
            "#3464: AlertEngine.EvaluateServerAsync must consult _settings.AlertsEnabled before EvaluateCoreAsync");
        Assert.True(LineOf(stripped, "AlertSweepResult.NotEvaluated", start, core) >= 0,
            "#3464 pin: the master-off return is NotEvaluated, so hosts leave badge state untouched");

        /* The funnel is private, so the entry gate covers every path to the deliver site. */
        Assert.Contains("private async Task FireAsync(", stripped, StringComparison.Ordinal);
    }

    /// <summary>The PostgreSQL predictors' gate (#3464): before the pass is counted, before anything is
    /// read, and ahead of all five per-condition helpers, which have no other production caller.</summary>
    [Fact]
    public void ThePostgresPredictors_GateBeforeCountingOrReading()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(Read("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs"));
        var members = MemberStarts(stripped);
        var (start, end) = MemberRange(members, stripped, "EvaluatePostgresAlertsAsync");

        /* Before RecordPass, so the #3013 denominator never counts a pass the switch stopped — the parity
           with both sibling sites the old comment had to disclaim. */
        var recordPass = LineOf(stripped, "RecordPass(", start, end);
        Assert.True(recordPass >= 0, "#3464 pin: EvaluatePostgresAlertsAsync no longer records its pass");
        Assert.True(LineOf(stripped, "!config.Alerts.Enabled", start, recordPass) >= 0,
            "#3464: the PG predictor gate must sit before RecordPass, or the denominator counts stopped passes");

        foreach (var helper in new[]
        {
            "EvaluatePgCpuAsync", "EvaluatePgDeadlocksAsync", "EvaluatePgBlockingAsync",
            "EvaluatePgLongRunningQueryAsync", "EvaluatePgPoisonWaitAsync",
        })
        {
            var callAt = LineOf(stripped, "await " + helper + "(", start, end);
            Assert.True(callAt >= 0, $"#3464 pin: EvaluatePostgresAlertsAsync no longer calls {helper}");

            AssertSoleProductionCallers(helper,
                allowedFiles: new[] { "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs" });
        }
    }

    /// <summary>
    /// Gap 1's fix, held in both directions: every NotifyAsync site is guarded — the scheduled pass's
    /// through the notifyFindings parameter both entry points compute via ShouldNotifyAnalysisFindings,
    /// and the #3467 pileup sweep's through the same predicate consulted inline; the predicate is the
    /// AND of the master switch and the family toggle; and the family toggle appears in the worker
    /// NOWHERE else — so no future call site can hand the notification service the toggle alone, which
    /// is verbatim how the bypass shipped ("replacing the old alerts.enabled gate").
    /// </summary>
    [Fact]
    public void AnalysisNotifications_AreDecidedByTheMasterSwitchAndTheFamilyToggle_Everywhere()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(Read("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs"));
        var members = MemberStarts(stripped);

        /* The scheduled site is guarded. */
        var (start, end) = MemberRange(members, stripped, "RunAnalysisPassAsync");
        var notifyAt = LineOf(stripped, ".NotifyAsync(", start, end);
        Assert.True(notifyAt >= 0, "#3464 pin: RunAnalysisPassAsync no longer notifies");
        Assert.True(LineOf(stripped, "if (notifyFindings)", start, notifyAt) >= 0,
            "#3464: the NotifyAsync site must sit under the notifyFindings guard");

        /* #3467: the pileup sweep's site is guarded by the same predicate, consulted inline. */
        var (pileStart, pileEnd) = MemberRange(members, stripped, "EvaluateSameStatementPileupAsync");
        var pileNotifyAt = LineOf(stripped, ".NotifyAsync(", pileStart, pileEnd);
        Assert.True(pileNotifyAt >= 0, "#3467 pin: EvaluateSameStatementPileupAsync no longer notifies");
        Assert.True(LineOf(stripped, "if (ShouldNotifyAnalysisFindings(config))", pileStart, pileNotifyAt) >= 0,
            "#3467: the pileup NotifyAsync site must sit under ShouldNotifyAnalysisFindings(config)");

        /* Both scheduled entry points plus the pileup sweep compute the guard through the predicate. */
        Assert.Equal(3, Regex.Matches(stripped, @"ShouldNotifyAnalysisFindings\(config\)").Count);

        /* The predicate is the AND'd idiom, and the family toggle exists nowhere else in the worker. */
        Assert.Contains("config.Alerts.Enabled && config.Analysis.NotificationsEnabled", stripped, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(stripped, Regex.Escape("config.Analysis.NotificationsEnabled")));

        /* Lite's twin, same holds. */
        var lite = CSharpSourceWalker.StripCommentsAndStrings(Read("Lite/Services/CollectionBackgroundService.cs"));
        var liteMembers = MemberStarts(lite);
        var (liteStart, liteEnd) = MemberRange(liteMembers, lite, "RunAnalysisIfDueAsync");
        var liteNotifyAt = LineOf(lite, ".NotifyAsync(", liteStart, liteEnd);
        Assert.True(liteNotifyAt >= 0, "#3464 pin: Lite's RunAnalysisIfDueAsync no longer notifies");
        Assert.True(LineOf(lite, "if (notify)", liteStart, liteNotifyAt) >= 0,
            "#3464: Lite's NotifyAsync site must sit under the notify guard");
        Assert.True(LineOf(lite, "ShouldNotifyAnalysisFindings()", liteStart, liteNotifyAt) >= 0,
            "#3464: Lite's notify local must come from ShouldNotifyAnalysisFindings");

        /* #3467: Lite's pileup sweep, same holds. */
        var (litePileStart, litePileEnd) = MemberRange(liteMembers, lite, "RunPileupSweepAsync");
        var litePileNotifyAt = LineOf(lite, ".NotifyAsync(", litePileStart, litePileEnd);
        Assert.True(litePileNotifyAt >= 0, "#3467 pin: Lite's RunPileupSweepAsync no longer notifies");
        Assert.True(LineOf(lite, "if (notify)", litePileStart, litePileNotifyAt) >= 0,
            "#3467: Lite's pileup NotifyAsync site must sit under the notify guard");
        Assert.True(LineOf(lite, "ShouldNotifyAnalysisFindings()", litePileStart, litePileNotifyAt) >= 0,
            "#3467: Lite's pileup notify local must come from ShouldNotifyAnalysisFindings");

        Assert.Contains("App.AlertsEnabled && App.AnalysisNotificationsEnabled", lite, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(lite, Regex.Escape("App.AnalysisNotificationsEnabled")));
    }

    /// <summary>The Darling predicate's truth table — the fixture half of gap 1: master off suppresses
    /// delivery whatever the family toggle says; the family toggle keeps working under master on.</summary>
    [Fact]
    public void ShouldNotifyAnalysisFindings_RequiresTheMasterSwitch_AndTheFamilyToggle()
    {
        static DarlingConfig Config(bool master, bool family)
        {
            var config = new DarlingConfig();
            config.Alerts.Enabled = master;
            config.Analysis.NotificationsEnabled = family;
            return config;
        }

        /* The measured bypass: family on, master OFF — the combination that paged 38 minutes into the mute. */
        Assert.False(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: false, family: true)));

        /* The family toggle is the narrower knob, not a synonym. */
        Assert.False(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: true, family: false)));
        Assert.False(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: false, family: false)));

        Assert.True(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: true, family: true)));
    }

    /// <summary>The custom-rule chain (#3464): the public entry consults the injected master switch before
    /// the rule load, and the deliver site is reachable only through it.</summary>
    [Fact]
    public void TheCustomRuleSweep_GatesAtItsEntry_AndTheDeliverSiteHasNoOtherPathIn()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            Read("Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs"));
        var members = MemberStarts(stripped);

        var (start, end) = MemberRange(members, stripped, "EvaluateServerAsync");
        var ruleLoad = LineOf(stripped, "GetEnabledRulesAsync(", start, end);
        Assert.True(ruleLoad >= 0, "#3464 pin: EvaluateServerAsync no longer loads rules");
        Assert.True(LineOf(stripped, "!_alertsEnabled()", start, ruleLoad) >= 0,
            "#3464: CustomAlertEvaluator.EvaluateServerAsync must consult the master switch before the rule load");

        /* The two-hop chain to the deliver site, each hop single-callered in production. */
        AssertSoleProductionCallers("EvaluateRuleForServerAsync",
            allowedFiles: new[] { "Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs" });
        AssertSoleProductionCallers("DeliverFireAsync",
            allowedFiles: new[] { "Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs" });

        /* And the worker wires the seam to the same live master switch every other family consults. */
        var worker = CSharpSourceWalker.StripCommentsAndStrings(Read("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs"));
        Assert.Contains("alertsEnabled: () => alertSettings.AlertsEnabled", worker, StringComparison.Ordinal);
    }

    /// <summary>Lite's direct senders: the connection edge's AND'd block — the idiom precedent itself —
    /// and the AG chain under the master-gated sweep entry, each with no other path in.</summary>
    [Fact]
    public void LitesDirectSenders_AreReachedOnlyThroughTheirMasterGatedBlocks()
    {
        /* SendConnectionAlert: every production call sits inside the exact AND'd idiom block. */
        var mainWindow = CSharpSourceWalker.StripCommentsAndStrings(Read("Lite/MainWindow.xaml.cs"));
        var gateAt = mainWindow.IndexOf("if (App.AlertsEnabled && App.NotifyConnectionChanges)", StringComparison.Ordinal);
        Assert.True(gateAt >= 0, "#3464 pin: the connection edge's AND'd master gate is gone from Lite");

        var blockEnd = BraceMatchedEnd(mainWindow, gateAt);
        foreach (Match call in Regex.Matches(mainWindow, @"(?<![\w.])SendConnectionAlert\s*\("))
        {
            Assert.True(call.Index > gateAt && call.Index < blockEnd,
                "#3464: a SendConnectionAlert call escaped the 'App.AlertsEnabled && App.NotifyConnectionChanges' block");
        }

        AssertSoleProductionCallers("SendConnectionAlert",
            allowedFiles: new[] { "Lite/MainWindow.xaml.cs", "Lite/MainWindow.AlertEngine.cs" });

        /* SendAgAlert: only from EvaluateAvailabilityGroupAlertsAsync, which only CheckPerformanceAlerts
           launches — after its master early-return and under the AG family toggle. */
        var engineFile = CSharpSourceWalker.StripCommentsAndStrings(Read("Lite/MainWindow.AlertEngine.cs"));
        var members = MemberStarts(engineFile);
        var (start, end) = MemberRange(members, engineFile, "CheckPerformanceAlerts");
        var agLaunch = LineOf(engineFile, "EvaluateAvailabilityGroupAlertsAsync(", start, end);
        Assert.True(agLaunch >= 0, "#3464 pin: CheckPerformanceAlerts no longer launches the AG evaluation");
        Assert.True(LineOf(engineFile, "!App.AlertsEnabled", start, agLaunch) >= 0,
            "#3464: CheckPerformanceAlerts must gate on the master switch before launching AG alerts");
        Assert.True(LineOf(engineFile, "App.NotifyAgHealth", start, agLaunch) >= 0,
            "#3464 pin: the AG family toggle still participates (master AND family)");

        AssertSoleProductionCallers("EvaluateAvailabilityGroupAlertsAsync",
            allowedFiles: new[] { "Lite/MainWindow.AlertEngine.cs" });
        AssertSoleProductionCallers("SendAgAlert",
            allowedFiles: new[] { "Lite/MainWindow.AlertEngine.cs" });
    }

    /// <summary>The precedent the AND'd gates match, and the contract they enforce: the connection-change
    /// gate's exact idiom, and the README sentence that makes alerts_enabled a promise rather than a
    /// preference. A change to either is a change to what every gate in this census means.</summary>
    [Fact]
    public void ThePrecedentIdiom_AndTheMasterSwitchContract_StillSayWhatTheGatesEnforce()
    {
        Assert.Contains(
            "if (!_settings.AlertsEnabled || !_notifyConnectionChanges())",
            Read("Darling/PerformanceMonitor.Darling.Service/DarlingSelfAlertEvaluator.cs"),
            StringComparison.Ordinal);

        /* #3467 widened this sentence from "scheduled-analysis" to "analysis": the pileup finding is an
           analysis-finding notification produced on the COLLECTION cadence, so the narrower wording
           would have described a promise smaller than the one the gates keep — and an operator reading
           it could reasonably have concluded a non-scheduled finding escapes the master switch, which is
           the exact class of confusion #3464 was about. */
        Assert.Contains(
            "turns off all alert evaluation **and** analysis finding notifications",
            Read("Darling/README.md"),
            StringComparison.Ordinal);
    }

    /* ---------------- source plumbing ---------------- */

    /// <summary>Production .cs files: everything except build output, the deprecated tree, and the test
    /// projects themselves. Deliberately NOT limited to known projects — the file-set derivation is the
    /// property that catches a delivery path added somewhere this census never imagined.</summary>
    private static IEnumerable<string> ProductionSources()
    {
        foreach (var path in Directory.EnumerateFiles(RepoFile.Root, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Relative(path);
            var segments = relative.Split('/');
            if (segments.Any(s => s is "obj" or "bin" or "deprecated" || s.EndsWith(".Tests", StringComparison.Ordinal)))
            {
                continue;
            }

            yield return path;
        }
    }

    /// <summary>Every line index holding at least one delivery-call token (a line with two holds one census
    /// slot per match).</summary>
    private static IEnumerable<int> SiteLines(string stripped)
    {
        var lines = stripped.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            foreach (var _ in s_deliveryCall.Matches(lines[i]))
            {
                yield return i;
            }
        }
    }

    /// <summary>
    /// (line, name) for every method-shaped declaration: an accessibility keyword and a Task/ValueTask/void
    /// return with a parenthesized parameter list. Fields and properties don't match (no parameter list on
    /// the declaring line); a site landing in a member SHAPE this walker cannot see fails the census with
    /// "before any recognizable member", which is red, not silent.
    /// </summary>
    private static List<(int Line, string Name)> MemberStarts(string stripped)
    {
        var declarations = new List<(int, string)>();
        var lines = stripped.Split('\n');
        var decl = new Regex(
            @"^\s*(?:public|internal|private|protected)[^=;]*?\b(?:async\s+)?(?:Task|ValueTask|void)\b",
            RegexOptions.CultureInvariant);

        for (var i = 0; i < lines.Length; i++)
        {
            if (!decl.IsMatch(lines[i]))
            {
                continue;
            }

            /* The member name is the first identifier opening a parameter list or type-argument list that
               is not itself a type constructor — so 'Task<AnalysisPassResult> RunAnalysisPassAsync(' names
               the method, not its return type, and a 'Func<string, Task>' FIELD names nothing at all. */
            foreach (Match name in Regex.Matches(lines[i], @"\b([A-Za-z_]\w*)\s*[(<]"))
            {
                if (name.Groups[1].Value is "Task" or "ValueTask" or "Func" or "Action")
                {
                    continue;
                }

                if (lines[i][(name.Groups[1].Index + name.Groups[1].Length)..].TrimStart().StartsWith('('))
                {
                    declarations.Add((i, name.Groups[1].Value));
                }

                break;
            }
        }

        return declarations;
    }

    private static string? EnclosingMember(List<(int Line, string Name)> members, int line)
    {
        string? found = null;
        foreach (var (memberLine, name) in members)
        {
            if (memberLine <= line)
            {
                found = name;
            }
        }

        return found;
    }

    /// <summary>A member's line range: its declaration to the next declaration (or EOF). Line-granular on
    /// purpose — every assertion built on it is an ORDER claim ("the gate precedes the send"), which a
    /// declaration-to-declaration range answers without a brace matcher's failure modes.</summary>
    private static (int Start, int End) MemberRange(List<(int Line, string Name)> members, string stripped, string name)
    {
        var index = members.FindIndex(m => m.Name == name);
        Assert.True(index >= 0, $"#3464 pin: member '{name}' is gone; re-anchor the census");
        var end = index + 1 < members.Count ? members[index + 1].Line : stripped.Split('\n').Length;
        return (members[index].Line, end);
    }

    /// <summary>First line index in [from, to) whose CODE contains the needle, or -1.</summary>
    private static int LineOf(string stripped, string needle, int from, int to)
    {
        var lines = stripped.Split('\n');
        for (var i = from; i < to && i < lines.Length; i++)
        {
            if (lines[i].Contains(needle, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>The offset just past the brace-matched block opened by the first '{' after
    /// <paramref name="from"/>.</summary>
    private static int BraceMatchedEnd(string stripped, int from)
    {
        var open = stripped.IndexOf('{', from);
        Assert.True(open > from, "#3464 pin: the gated block never opens");

        var depth = 0;
        for (var i = open; i < stripped.Length; i++)
        {
            if (stripped[i] == '{')
            {
                depth++;
            }
            else if (stripped[i] == '}' && --depth == 0)
            {
                return i;
            }
        }

        Assert.Fail("#3464 pin: the gated block never closes");
        return -1;
    }

    /// <summary>
    /// Asserts that call-shaped occurrences of <paramref name="member"/> exist ONLY in the allowed files —
    /// the property that turns "its caller gates" from a belief into a claim: a second caller added
    /// anywhere in the production tree fails here, naming its file.
    /// </summary>
    private static void AssertSoleProductionCallers(string member, string[] allowedFiles)
    {
        var call = new Regex(@"(?<![\w.])" + Regex.Escape(member) + @"\s*\(", RegexOptions.CultureInvariant);
        foreach (var path in ProductionSources())
        {
            var relative = Relative(path);
            if (allowedFiles.Contains(relative))
            {
                continue;
            }

            var stripped = Stripped(path);
            Assert.False(call.IsMatch(stripped),
                $"#3464: {member} gained a caller outside its gated chain: {relative}. Its master gate lives in "
                + "the chain this census verified — a new caller must consult alerts_enabled itself and be "
                + "classified in the census.");
        }
    }

    /// <summary>Stripped-source cache: the sole-caller sweeps re-read the whole tree per verified member,
    /// and the answer for one file never changes within a run.</summary>
    private static readonly Dictionary<string, string> s_strippedCache = new(StringComparer.Ordinal);

    private static string Stripped(string path)
    {
        lock (s_strippedCache)
        {
            if (!s_strippedCache.TryGetValue(path, out var stripped))
            {
                stripped = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
                s_strippedCache[path] = stripped;
            }

            return stripped;
        }
    }

    /* RepoFile is the tree-reading authority (its own header documents the thirty-four private copies
       this project accumulated before it existed); these two thin shims keep this file's call sites in
       the slash-relative idiom the census reports in. */
    private static string Read(string relative) =>
        RepoFile.ReadRepoFile(relative.Split('/'));

    private static string Relative(string path) =>
        Path.GetRelativePath(RepoFile.Root, path).Replace(Path.DirectorySeparatorChar, '/');
}
