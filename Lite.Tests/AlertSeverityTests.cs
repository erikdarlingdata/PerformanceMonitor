using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards the runtime-graded severity for "Volume Free Space" (#1136). The metric used to be absent
/// from the <see cref="AlertSeverity"/> map and fell through to INFO-blue (the lowest tier) for every
/// breach; it now renders WARNING by default and CRITICAL when the alert site sets
/// <see cref="AlertContext.SeverityOverride"/> for a critically-low volume. The email body, Teams card,
/// and Slack sidebar all key severity off the shared map, so this one suite covers both apps' output.
/// </summary>
public class AlertSeverityTests
{
    private static readonly AlertBranding Branding = new("Test Edition", null);

    [Fact]
    public void VolumeFreeSpace_NoOverride_IsWarningNotInfo()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Volume Free Space");
        Assert.Equal("WARNING", badge);
        Assert.Equal("#D97706", hex);
    }

    [Fact]
    public void VolumeFreeSpace_CriticalOverride_IsCritical()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Volume Free Space", AlertSeverityLevel.Critical);
        Assert.Equal("CRITICAL", badge);
        Assert.Equal("#DC2626", hex);
    }

    [Fact]
    public void Override_IsAuthoritativeOverMetricMap()
    {
        // The override wins regardless of the metric's own mapped tier.
        var (_, badge, _) = AlertSeverity.ForMetric("High CPU", AlertSeverityLevel.Critical);
        Assert.Equal("CRITICAL", badge);
    }

    [Fact]
    public void CaptureDown_IsCriticalNotInfo()
    {
        // Capture Down is emailed/webhooked and fires as an Error toast — it must not fall through
        // to INFO-blue like an unmapped metric (same gap class as Volume Free Space, #1136).
        var (hex, badge, _) = AlertSeverity.ForMetric("Capture Down");
        Assert.Equal("CRITICAL", badge);
        Assert.Equal("#DC2626", hex);
    }

    [Fact]
    public void FailedAgentJob_IsWarningNotInfo()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Failed Agent Job");
        Assert.Equal("WARNING", badge);
        Assert.Equal("#D97706", hex);
    }

    /// <summary>#3443's one deliberate INFO arm. The collector-cost digest renders identically to the
    /// unmapped fall-through on purpose - it is a report to read, not a condition to act on - but its arm
    /// is DECLARED in the map so the next #1136/#2090-style sweep does not read the INFO rendering as an
    /// accident and promote it to WARNING. This pin is what fails if that promotion happens. The digest
    /// stays out of <see cref="EveryFiredMetricName_HasASeverityArm_NotTheInfoFallThrough"/>'s inventory
    /// for the same reason: that list asserts fired metrics DIFFER from the fall-through, and this one
    /// matches it by design.</summary>
    [Fact]
    public void CollectorCostDigest_IsInfoBlueOnPurpose()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Collector Cost Digest");
        Assert.Equal("INFO", badge);
        Assert.Equal("#2eaef1", hex);
    }

    /// <summary>#3466 lane 4: the fleet sweep's daily rollup is the second deliberate INFO arm, on the
    /// digest's exact reasoning — a report to read, not a condition to act on, DECLARED so the next
    /// #1136/#2090-style fall-through sweep cannot promote it to WARNING. It stays out of
    /// <see cref="EveryFiredMetricName_HasASeverityArm_NotTheInfoFallThrough"/>'s inventory for the same
    /// reason the digest does: that list asserts fired metrics DIFFER from the fall-through, and this one
    /// matches it by design.</summary>
    [Fact]
    public void FleetSweepRollup_IsInfoBlueOnPurpose()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Fleet Sweep Rollup");
        Assert.Equal("INFO", badge);
        Assert.Equal("#2eaef1", hex);
    }

    /// <summary>The lockstep half of the two pins above (#3448 review). The severity map styles email
    /// and webhooks, but the Alert History grids in BOTH apps style their rows from
    /// <see cref="AlertMetricClassifier.IsWarning"/> — Lite's <c>AlertsHistoryTab</c> and the Darling
    /// Viewer's each bind a DataTrigger to a row <c>IsWarning</c> derived from that one method — and
    /// <c>IsWarning</c> is the complement of resolution/critical, so a metric this map deliberately
    /// keeps INFO/blue still arrived in both grids amber, styled like a live actionable alert. The two
    /// assemblies cannot reference each other's map (<c>AlertSeverity</c> is internal here, downstream
    /// of Common), so these pins are the reference for the KNOWN arms — and only for those: an
    /// InlineData theory exercises the strings someone remembered to list, so the from-source census
    /// (<see cref="TheDeliberateInfoArms_AreExactlyTheHistoryGridCarveOuts"/>) is what fails for a
    /// third arm nobody listed (#3476 review).</summary>
    [Theory]
    [InlineData("Collector Cost Digest")]
    [InlineData("Fleet Sweep Rollup")]
    [InlineData("Analysis Singles Digest")]
    /* #3783: the first two CONDITIONS on the INFO tier — the store's TOAST slack and checkpointer pressure. */
    [InlineData("Store TOAST Slack")]
    [InlineData("Store Checkpointer Pressure")]
    public void DeliberateInfoMetrics_AreNotWarnings_InTheHistoryGridClassifier(string metric)
    {
        var (_, badge, _) = AlertSeverity.ForMetric(metric);
        Assert.Equal("INFO", badge);

        Assert.True(AlertMetricClassifier.IsInformational(metric));
        Assert.False(AlertMetricClassifier.IsWarning(metric));
    }

    /// <summary>The lockstep, held by census rather than by memory (#3476 review). The InlineData pins
    /// above test BEHAVIOR for the arms someone remembered to list, and that is the hole the classifier's
    /// doc used to paper over: a third deliberate INFO arm added to <see cref="AlertSeverity.ForMetric"/>
    /// while forgetting BOTH <see cref="AlertMetricClassifier.IsInformational"/> and a new InlineData row
    /// failed nothing. This census closes it the #3466 way (<c>McpToolTypeRegistrationTests</c>): derive
    /// both lists from SOURCE and assert set-equality, so the both-forgotten case reds here.
    ///
    /// <para><b>The INFO side is enumerated from ForMetric's source and classified by ForMetric
    /// itself:</b> every string-literal switch arm is read out of AlertSeverity.cs (comments stripped
    /// first, so a metric named in prose cannot join the census), and an arm is "deliberately INFO"
    /// exactly when the COMPILED method renders it identically to the unmapped fall-through — the
    /// declared-yet-fall-through-identical rendering IS the design being pinned, and letting the method
    /// classify its own arms means the census cannot go stale if the INFO tier's hex or badge ever
    /// changes, and cannot misread a WARNING arm.</para>
    ///
    /// <para><b>The classifier side is enumerated from ITS source rather than by invocation, and that is
    /// the stronger arm:</b> invoking <c>IsInformational</c> can confirm membership for names already in
    /// hand, but no probe set can ENUMERATE a pattern's members — a stale name kept there after a metric
    /// rename is invisible to every probe derived from ForMetric, and the stale direction is half of what
    /// a set-equality census exists for (the MCP census's stale-registration arm, one assembly over).
    /// Invocation still runs as the belt: both parsed sets must agree with the compiled classifier in
    /// both directions, so the regex cannot silently drift from what the code does.</para></summary>
    [Fact]
    public void TheDeliberateInfoArms_AreExactlyTheHistoryGridCarveOuts()
    {
        /* Every declared metric-name arm in ForMetric's switch, from source. */
        var severitySource = StripComments(File.ReadAllText(RepoSourcePath(
            "PerformanceMonitor.Notifications", "AlertSeverity.cs")));

        var declaredArms = Regex
            .Matches(severitySource, "\"(?<lit>[^\"]+)\"(?:\\s+or\\s+\"(?<lit>[^\"]+)\")*\\s*=>")
            .SelectMany(m => m.Groups["lit"].Captures.Select(c => c.Value))
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(
            declaredArms.Count > 0,
            "Found no string-literal switch arms in AlertSeverity.cs. If ForMetric's shape changed, this "
            + "census needs to learn the new one rather than be deleted — it is the only thing that fails "
            + "when a deliberate INFO arm joins neither IsInformational nor the InlineData pins above.");

        /* The deliberate INFO arms: declared, and rendered by the compiled method exactly like the
           unmapped fall-through. INFO is the fall-through's own rendering, which is why an arm that
           matches it is a declaration rather than an accident (CollectorCostDigest_IsInfoBlueOnPurpose's
           point) — and why source is the only place the declared set can be read from at all. */
        const string probe = "no-such-metric-name";
        Assert.DoesNotContain(probe, declaredArms);
        var fallThrough = AlertSeverity.ForMetric(probe);

        var deliberateInfo = declaredArms
            .Where(name => AlertSeverity.ForMetric(name) == fallThrough)
            .ToHashSet(StringComparer.Ordinal);

        Assert.NotEmpty(deliberateInfo);

        /* IsInformational's names, from ITS source. */
        var classifierSource = StripComments(File.ReadAllText(RepoSourcePath(
            "PerformanceMonitor.Common", "AlertMetricClassifier.cs")));

        var isInformational = Regex.Match(
            classifierSource, @"IsInformational\s*\([^)]*\)\s*=>\s*\w+\s+is\s+(?<body>[^;]+);");

        Assert.True(
            isInformational.Success,
            "Could not read IsInformational's name list out of AlertMetricClassifier.cs. If the method's "
            + "shape changed, teach the census the new one rather than deleting it.");

        var carveOuts = Regex
            .Matches(isInformational.Groups["body"].Value, "\"(?<lit>[^\"]+)\"")
            .Select(m => m.Groups["lit"].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.NotEmpty(carveOuts);

        /* Set-equality, each direction failing with the fix in hand. */
        var missing = deliberateInfo.Except(carveOuts).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var stale = carveOuts.Except(deliberateInfo).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(
            missing.Count == 0,
            "These metrics render INFO-blue on purpose in email and webhooks, but the Alert History grids "
            + "in both apps will style them amber like live actionable alerts, because "
            + "AlertMetricClassifier.IsInformational does not name them: " + string.Join(", ", missing)
            + ". Add them there, and an InlineData row on the behavior theory above.");

        Assert.True(
            stale.Count == 0,
            "AlertMetricClassifier.IsInformational names metrics that are not deliberate INFO arms of "
            + "AlertSeverity.ForMetric — a stale or renamed entry that un-highlights history rows the "
            + "severity map does not keep blue: " + string.Join(", ", stale));

        /* The belt: the parsed sets must agree with the COMPILED classifier, both directions — a
           censused carve-out the method rejects means the parse drifted from the code, and a non-INFO
           arm the method accepts means the code grew reach the parse cannot see. The IsWarning check
           is the user-facing consequence itself, now held for every future arm rather than only the
           two the InlineData theory lists. */
        foreach (var name in deliberateInfo)
        {
            Assert.True(AlertMetricClassifier.IsInformational(name));
            Assert.False(AlertMetricClassifier.IsWarning(name));
        }

        foreach (var name in declaredArms.Except(deliberateInfo))
        {
            Assert.False(AlertMetricClassifier.IsInformational(name));
        }
    }

    /// <summary>Block and line comments removed, so a metric name mentioned in prose — both censused
    /// files name several in theirs — cannot join a source census. No string literal in either file
    /// contains a comment opener, which is what keeps the lexer-free strip honest.</summary>
    private static string StripComments(string source) =>
        Regex.Replace(source, @"/\*.*?\*/|//[^\r\n]*", string.Empty, RegexOptions.Singleline);

    /// <summary>Walks up from the test output directory to the checkout — the
    /// <c>McpToolTypeRegistrationTests.HostSourcePath</c> shape, for the same reason: a coverage
    /// question should be answerable from source without standing up either assembly's host.</summary>
    private static string RepoSourcePath(string project, string file)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);

        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, project, file);

            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new FileNotFoundException(
            $"Could not locate {project}/{file} by walking up from the test output directory.");
    }

    [Fact]
    public void EmailBody_CriticalOverride_RendersCriticalNotInfo()
    {
        var ctx = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical };
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            "Volume Free Space", "S1", "E:\\ 2% free (1.0 GB)", "10% / 5 GB", 15, Branding, ctx);

        Assert.Contains("CRITICAL", html);
        Assert.Contains("#DC2626", html);
        Assert.DoesNotContain("INFO", html);
    }

    [Fact]
    public void EmailBody_NoOverride_RendersWarningNotInfo()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            "Volume Free Space", "S1", "E:\\ 8% free (40.0 GB)", "10% / 5 GB", 15, Branding, context: null);

        Assert.Contains("WARNING", html);
        Assert.DoesNotContain("INFO", html);
    }

    [Fact]
    public void TeamsPayload_CriticalOverride_UsesCriticalColorAndBadge()
    {
        var ctx = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical };
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Volume Free Space", "S1", "E:\\ 2% free (1.0 GB)", "10% / 5 GB", Branding, context: ctx);

        Assert.Contains("CRITICAL", payload);
        Assert.Contains("DC2626", payload); // themeColor renders without the leading '#'
    }

    [Fact]
    public void SlackPayload_CriticalOverride_UsesCriticalColor()
    {
        var ctx = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical };
        var payload = WebhookAlertService.BuildSlackPayload(
            "Volume Free Space", "S1", "E:\\ 2% free (1.0 GB)", "10% / 5 GB", Branding, context: ctx);

        Assert.Contains("CRITICAL", payload);
        Assert.Contains("#DC2626", payload);
    }

    /// <summary>
    /// The #1136 fall-through tripwire (#2090): every metric name any fire site uses must have an arm in
    /// <see cref="AlertSeverity.ForMetric"/>, or an alert-history replay (the renderer with no context)
    /// silently renders INFO-blue for a WARNING/CRITICAL condition. This gap SHIPPED four separate times
    /// (blocking wait, capture down, failed agent job, database state) and #2090 found six more — because
    /// nothing forced a new alert's author to visit the map. This inventory is that forcing function:
    /// firing a NEW metric name means adding it here, and adding it here fails until the map has its arm.
    /// </summary>
    [Fact]
    public void EveryFiredMetricName_HasASeverityArm_NotTheInfoFallThrough()
    {
        var infoFallThrough = AlertSeverity.ForMetric("no-such-metric-name");

        foreach (var metric in new[]
        {
            /* The alert engine's thresholds. */
            "Blocking Detected", "Blocking Wait Time", "Deadlocks Detected", "High CPU", "Poison Wait",
            "Long-Running Query", "tempdb Space", "Long-Running Job", "Failed Agent Job",
            "Database State", "Volume Free Space", "Version Store (PVS)",
            /* Connection-state family. */
            "Server Unreachable", "Server Restored",
            /* Availability Groups (#991). */
            "AG Failover", "AG Replica Disconnected", "AG Replica Reconnected",
            "AG Sync Fell Behind", "AG Database Suspended",
            /* Darling self-alerts (#2090's batch — all fire Critical at their sites). */
            "Capture Down", "Collection Stopped", "Agent Not Running",
            "Store Disk Pressure", "Store Runtime Upgrade", "Compression Job Stuck",
            /* #3816: the policy-job self-heal's other two families. Each fires ONE tier at its site
               (refresh Critical, retention Warning), so each name's arm is the faithful replay colour. The
               issue's fourth alert, "Store Job Failing", is deliberately NOT listed: it is one of the
               declared INFO metrics, like the digests and the store's two physical-health conditions, so it
               is meant to reach the arm this inventory refuses. */
            "Refresh Job Stuck", "Retention Job Stuck",
        })
        {
            var (hex, badge, _) = AlertSeverity.ForMetric(metric);
            Assert.False(
                hex == infoFallThrough.HexColor && badge == infoFallThrough.BadgeText,
                $"'{metric}' hits the INFO fall-through — add its arm to AlertSeverity.ForMetric " +
                "(and if this is a brand-new alert, its fire site should also pass an explicit severity).");
        }
    }

    /// <summary>#2090's root cause, pinned end-to-end: a self-alert-shaped send (context null at the fire
    /// site, severity on the outcome) must NOT render INFO once the deliverer folds the severity in.</summary>
    [Fact]
    public void SelfAlertSeverity_FoldedIntoContext_RendersCriticalNotInfo()
    {
        var ctx = new AlertContext();
        ctx.SeverityOverride ??= AlertSeverityLevel.Critical; /* the deliverer's #2090 fold */
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Collection Stopped", "S1", "5 runs failing", "collecting", Branding, context: ctx);

        Assert.Contains("CRITICAL", payload);
        Assert.DoesNotContain("2eaef1", payload);
    }

}
