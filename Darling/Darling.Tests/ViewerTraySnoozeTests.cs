/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #3570 — "Snoozing an alert in the tray does nothing". The tray toast is the viewer's own alert
/// channel, and until this issue the viewer never applied a mute rule to it: it toasted every polled row the
/// SERVICE had not stamped <c>muted</c>, so a Snooze suppressed the next toast only after the service noticed
/// the reload beacon, re-read its whole config view, refreshed its rule cache, and re-fired the alert through
/// it. Nothing viewer-side observed that chain; when it was slow or broken the rule sat in
/// <c>config_mute_rules</c> while the toasts kept coming.
///
/// <para>The decision logic that closes it — <see cref="AlertToastCoordinator.SelectToasts"/> judging polled
/// rows against the viewer's own rule set — is pinned in <c>ViewerAlertToastCoordinatorTests</c>. This file
/// pins the two ends of the loop that feed it: the rule the Snooze WRITES
/// (<see cref="ViewerDataService.BuildTraySnoozeRule"/>) and the context a row is JUDGED as
/// (<see cref="ViewerAlertRow.ToMuteContext"/>), and that the one matches the other by construction — the
/// spelling question. Plus the honesty of the status line's "within N s", which restates a service constant the
/// viewer cannot reference. The WPF surfaces themselves (the balloon, the status bar) are not unit-testable;
/// everything that decides what they say is.</para>
/// </summary>
public sealed class ViewerTraySnoozeTests
{
    private static readonly DateTime T0 = new(2026, 9, 18, 7, 22, 53, DateTimeKind.Utc);

    private static ViewerAlertRow Row(string serverName, string metric, string? detail = null, bool muted = false) => new()
    {
        AlertTime = T0,
        ServerId = 7,
        ServerName = serverName,
        MetricName = metric,
        CurrentValue = 0,
        ThresholdValue = 0,
        AlertSent = false,
        NotificationType = AlertDelivery.ChannelNoneConfigured,
        Muted = muted,
        DetailText = detail,
    };

    /* ---------------- the rule the Snooze writes ---------------- */

    [Fact]
    public void BuildTraySnoozeRule_ScopesToTheRowsServerAndMetric_ExpiringAfterTheDuration()
    {
        var rule = ViewerDataService.BuildTraySnoozeRule("sql-prod-01", "Agent Not Running", TimeSpan.FromHours(4), T0);

        Assert.Equal("sql-prod-01", rule.ServerName);
        Assert.Equal("Agent Not Running", rule.MetricName);
        Assert.True(rule.Enabled);
        Assert.Equal(T0, rule.CreatedAtUtc);
        Assert.Equal(T0.AddHours(4), rule.ExpiresAtUtc);
        Assert.Equal("Snoozed from tray (4h)", rule.Reason);

        /* A snooze is "this alert on this server" — never narrower. */
        Assert.Null(rule.DatabasePattern);
        Assert.Null(rule.QueryTextPattern);
        Assert.Null(rule.WaitTypePattern);
        Assert.Null(rule.JobNamePattern);

        /* And never broader: a snooze must not be the blanket rule the Stale Mute Rules self-alert flags CRITICAL. */
        Assert.False(rule.MatchesEveryAlert);
        Assert.False(string.IsNullOrEmpty(rule.Id));
    }

    [Theory]
    [InlineData(15, "15m")]
    [InlineData(60, "1h")]
    [InlineData(240, "4h")]
    public void BuildTraySnoozeRule_ReasonNamesTheButtonThatWroteIt(int minutes, string label)
    {
        var rule = ViewerDataService.BuildTraySnoozeRule("s", "m", TimeSpan.FromMinutes(minutes), T0);

        Assert.Equal($"{ViewerDataService.TraySnoozeReasonPrefix} ({label})", rule.Reason);
        Assert.Equal(label, ViewerDataService.FormatSnoozeDuration(TimeSpan.FromMinutes(minutes)));
    }

    /// <summary>A row with no server name yields a rule for every server — the only honest scope for a row
    /// that did not say — never an empty-string server that would match nothing at all.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void BuildTraySnoozeRule_EmptyServerName_BecomesEveryServer(string? serverName)
    {
        var rule = ViewerDataService.BuildTraySnoozeRule(serverName, "Agent Not Running", TimeSpan.FromHours(1), T0);

        Assert.Null(rule.ServerName);
        Assert.Equal("Agent Not Running", rule.MetricName);
        Assert.False(rule.MatchesEveryAlert); /* still metric-scoped */
    }

    /* ---------------- the closed loop: the rule the toast writes matches the row the toast came from ---------------- */

    /// <summary>
    /// The spelling question, answered by construction. Alert rows on a Darling store spell <c>server_name</c>
    /// two ways — the self-alert family writes the server's display name, the shared engine writes the name its
    /// snapshot carries — and a mute rule must match the row's OWN spelling to suppress that row's producer. The
    /// toast captures <see cref="ViewerAlertRow.ServerName"/> and <see cref="ViewerAlertRow.MetricName"/> off the
    /// row, the rule is built from exactly those, and the viewer's filter judges the same row through
    /// <see cref="ViewerAlertRow.ToMuteContext"/>: whatever the spelling, the loop closes. Pinned over both
    /// shapes so a future "normalize the server name" on one side but not the other fails here.
    /// </summary>
    [Theory]
    [InlineData("SQL01", "Agent Not Running")]                       /* self-alert family: display name */
    [InlineData("sql01.corp.example.internal", "Failed Agent Job")]  /* engine family: the snapshot's name */
    [InlineData("sql01,1433", "High CPU")]                           /* a display name with a port */
    public void TheRuleASnoozeWrites_MatchesTheRowItWasSnoozedFrom(string serverName, string metric)
    {
        var row = Row(serverName, metric, detail: "  Job Name: Nightly ETL\n");
        var rule = ViewerDataService.BuildTraySnoozeRule(row.ServerName, row.MetricName, TimeSpan.FromHours(4), T0);

        Assert.True(rule.MatchesAt(row.ToMuteContext(), T0.AddMinutes(5)));
        Assert.True(AlertToastCoordinator.IsMutedByViewerRules(row, new[] { rule }, T0.AddMinutes(5)));

        /* And the SAME rule, judged the way the service judges it — a bare server+metric context, no detail
           text — also matches, so the tray and the service's channels agree about this snooze. */
        Assert.True(rule.MatchesAt(new AlertMuteContext { ServerName = serverName, MetricName = metric }, T0.AddMinutes(5)));
    }

    /// <summary>The rule stops matching the instant it expires, on the clock it is judged with — no ambient <c>UtcNow</c>.</summary>
    [Fact]
    public void TheRuleASnoozeWrites_LapsesExactlyAtItsExpiry()
    {
        var row = Row("SQL01", "Agent Not Running");
        var rule = ViewerDataService.BuildTraySnoozeRule(row.ServerName, row.MetricName, TimeSpan.FromMinutes(15), T0);

        Assert.True(rule.MatchesAt(row.ToMuteContext(), T0.AddMinutes(15).AddTicks(-1)));
        Assert.False(rule.MatchesAt(row.ToMuteContext(), T0.AddMinutes(15)));
    }

    /* ---------------- the context a row is judged as ---------------- */

    [Fact]
    public void ToMuteContext_CarriesTheRowsServerAndMetricVerbatim()
    {
        var context = Row("SQL01", "Agent Not Running").ToMuteContext();

        Assert.Equal("SQL01", context.ServerName);
        Assert.Equal("Agent Not Running", context.MetricName);
        Assert.Null(context.DatabaseName);
        Assert.Null(context.WaitType);
        Assert.Null(context.JobName);
        Assert.Null(context.QueryText);
    }

    /// <summary>The pattern dimensions come from the stored detail text, the same way the "Mute This Alert"
    /// pre-fill has always read them — so a rule authored from a row covers that row's toast.</summary>
    [Fact]
    public void ToMuteContext_ParsesThePatternDimensionsOutOfDetailText()
    {
        var detail = "2 job failure(s)\n  Job Name: Nightly ETL\n  Database: SalesDb\n  Wait Type: LCK_M_X\n  Query: SELECT 1";
        var context = Row("SQL01", "Failed Agent Job", detail).ToMuteContext();

        Assert.Equal("Nightly ETL", context.JobName);
        Assert.Equal("SalesDb", context.DatabaseName);
        Assert.Equal("LCK_M_X", context.WaitType);
        Assert.Equal("SELECT 1", context.QueryText);

        var jobRule = new MuteRule { MetricName = "Failed Agent Job", JobNamePattern = "Nightly" };
        Assert.True(jobRule.MatchesAt(context, T0));
    }

    /// <summary>#3309 holds here too: a custom alert's detail text is a user-authored name, never parsed for
    /// labels, so a crafted "Database: master" line cannot pre-fill or match a pattern dimension.</summary>
    [Fact]
    public void ToMuteContext_CustomAlert_DoesNotParseDetailText()
    {
        var context = Row("SQL01", "Custom:42", detail: "Database: master").ToMuteContext();

        Assert.Equal("Custom:42", context.MetricName);
        Assert.Null(context.DatabaseName);
    }

    /* ---------------- the status line's "within N s" is the service's real cadence ---------------- */

    /// <summary>
    /// The snooze status line tells the operator the service's channels stop "within N s (the service's next
    /// sweep)". N is <see cref="ViewerDataService.ServiceReloadTickSeconds"/>, a restatement of
    /// <c>DarlingWorker.s_sweepInterval</c> — the tick at whose top the service polls the reload beacon — which
    /// the viewer cannot reference. Read out of the service's source (the field is private) so the sentence
    /// cannot outlive the cadence it describes.
    /// </summary>
    [Fact]
    public void ServiceReloadTickSeconds_IsTheServicesSweepTick()
    {
        var sweepTick = CommandPlaneCommandTimeoutTests.SecondsOfPrivateTimeSpan("DarlingWorker.cs", "s_sweepInterval");

        Assert.Equal(ViewerDataService.ServiceReloadTickSeconds, sweepTick);
    }

    /* ---------------- the wiring that makes the coordinator's new argument reach it ---------------- */

    /// <summary>
    /// <see cref="AlertToastCoordinator.SelectToasts"/>'s rule-set parameter is OPTIONAL (null = the pre-#3570
    /// behavior), so a refactor that drops the argument at the one production call site compiles clean and
    /// silently reverts the fix. This reads <c>MainWindow.xaml.cs</c> and asserts the call passes the viewer's
    /// rule set, and that the rule-set refresh (<c>UpdateServerSilencedAsync</c>, which also drives the sidebar
    /// bell) is awaited BEFORE it in the same poll — the ordering the rules-then-toasts contract rests on.
    /// </summary>
    [Fact]
    public void PollAlertsAsync_PassesTheViewersRuleSet_AfterRefreshingIt()
    {
        var body = MemberBody(ViewerSource("MainWindow.xaml.cs"), "PollAlertsAsync");

        var refresh = body.IndexOf("await UpdateServerSilencedAsync()", StringComparison.Ordinal);
        var select = Regex.Match(body, @"SelectToasts\s*\([^;]*?_viewerMuteRules\s*\)", RegexOptions.Singleline);

        Assert.True(refresh >= 0, "PollAlertsAsync no longer awaits UpdateServerSilencedAsync — the toast filter's rule set is never refreshed");
        Assert.True(select.Success, "PollAlertsAsync's SelectToasts call no longer passes _viewerMuteRules — the tray has stopped honoring mute rules (#3570 regressed)");
        Assert.True(refresh < select.Index, "PollAlertsAsync selects toasts BEFORE refreshing the rule set — a rule read this poll reaches the tray a poll late");
    }

    /// <summary>
    /// The two local writers (tray Snooze, server Silence) add their rule to the viewer's set only AFTER the
    /// store write succeeds — persist-then-cache, <see cref="MuteRuleService.AddRuleAsync"/>'s ordering — so a
    /// snooze that did not persist never suppresses toasts on this seat while every other surface says no such
    /// rule exists.
    /// </summary>
    [Fact]
    public void LocalRuleWriters_AddToTheViewersSet_OnlyAfterTheStoreWrite()
    {
        var snooze = MemberBody(ViewerSource("MainWindow.xaml.cs"), "SnoozeAlertAsync");
        AssertPersistThenCache(snooze, "SnoozeAlertAsync");

        var silence = MemberBody(ViewerSource("MainWindow.ServerManagement.cs"), "ServerContextMenu_Silence_Click");
        AssertPersistThenCache(silence, "ServerContextMenu_Silence_Click");
    }

    private static void AssertPersistThenCache(string body, string member)
    {
        var insert = body.IndexOf("InsertMuteRuleAsync(", StringComparison.Ordinal);
        var cache = body.IndexOf("_viewerMuteRules.Add(", StringComparison.Ordinal);

        Assert.True(insert >= 0, $"{member} no longer writes the rule to the store");
        Assert.True(cache >= 0, $"{member} no longer hands its rule to the toast filter (#3570 regressed for this writer)");
        Assert.True(insert < cache, $"{member} caches the rule before persisting it — a failed write would suppress toasts for a rule that does not exist");
    }

    /* ---------------- helpers ---------------- */

    /// <summary>The text of one viewer source file, through the shared root resolver (worktree-safe).</summary>
    private static string ViewerSource(string file) =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    /// <summary>
    /// The CODE of one method, from its declaration to the brace that closes it, over the comment/string-stripped
    /// source (<see cref="CSharpSourceWalker.StripCommentsAndStrings"/> preserves offsets) so a brace or a name
    /// inside a comment or a status-line string can neither close the body early nor satisfy an assertion meant
    /// for code. Anchored on the DECLARATION (<c>Task NAME(</c> / <c>void NAME(</c>) rather than the bare name:
    /// both methods this file reads are also CALLED earlier in their files, and the first bare match would hand
    /// back whatever method happens to follow that call.
    /// </summary>
    private static string MemberBody(string source, string member)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);
        var signature = Regex.Match(
            stripped, @"\b(?:Task|void)\s+" + Regex.Escape(member) + @"\s*\(", RegexOptions.CultureInvariant);
        Assert.True(signature.Success, $"could not find the declaration of {member}( in the viewer source");

        var open = stripped.IndexOf('{', signature.Index);
        Assert.True(open >= 0, $"could not find the opening brace of {member}");

        var depth = 0;
        for (var i = open; i < stripped.Length; i++)
        {
            if (stripped[i] == '{')
            {
                depth++;
            }
            else if (stripped[i] == '}' && --depth == 0)
            {
                return stripped.Substring(signature.Index, i - signature.Index + 1);
            }
        }

        Assert.Fail($"unbalanced braces while reading {member}");
        return "";
    }
}
