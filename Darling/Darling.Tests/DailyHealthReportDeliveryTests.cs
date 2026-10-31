/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Reports;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7041: when the Daily Health Report is due, the once-a-local-day gate across a restart, and what the manual send does and does not
/// touch. The due rule is driven through <see cref="DailyHealthReportSchedule"/> and the evaluator's own check with a controllable clock,
/// a recording deliverer and an in-memory stamp store, the shapes the other daily documents' tests use.
/// </summary>
public class DailyHealthReportDeliveryTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* 2026-09-15 is a Tuesday. The default zone for the evaluator tests is UTC so the clock reads as the local clock. */
    internal static readonly DateTime Day = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);

    internal static DailyHealthReportSchedule Utc(string sendAt = "07:00", bool enabled = true) =>
        new(enabled, TimeOnly.Parse(sendAt), TimeZoneInfo.Utc, "Daily Health Report");

    /* ---------------- the due rule ---------------- */

    [Fact]
    public void BeforeSendAt_IsNotDue_AtAndAfterSendAt_IsDue()
    {
        var s = Utc("07:00");
        Assert.False(s.IsDue(Day.AddHours(6).AddMinutes(59)));
        Assert.True(s.IsDue(Day.AddHours(7)));
        Assert.True(s.IsDue(Day.AddHours(23)));
    }

    [Fact]
    public void ADisabledReport_IsNeverDue()
    {
        Assert.False(Utc(enabled: false).IsDue(Day.AddHours(12)));
    }

    [Fact]
    public void MostRecentSendAt_IsTodaysOnceItHasPassed_AndYesterdaysBefore()
    {
        var s = Utc("07:00");
        Assert.Equal(Day.AddHours(7), s.MostRecentSendAtUtc(Day.AddHours(9)));
        Assert.Equal(Day.AddDays(-1).AddHours(7), s.MostRecentSendAtUtc(Day.AddHours(3)));
    }

    [Fact]
    public void AZoneAheadOfUtc_KeysTheDayOnItsOwnLocalDate()
    {
        var zone = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
        var s = new DailyHealthReportSchedule(true, new TimeOnly(7, 0), zone, "t");

        /* 21:30Z on the 14th is 06:30 on the 15th in Tokyo: not due. 22:00Z is 07:00 on the 15th: due, and the 15th's local date. */
        Assert.False(s.IsDue(new DateTime(2026, 9, 14, 21, 30, 0, DateTimeKind.Utc)));
        var due = new DateTime(2026, 9, 14, 22, 0, 0, DateTimeKind.Utc);
        Assert.True(s.IsDue(due));
        Assert.Equal(new DateOnly(2026, 9, 15), s.LocalDate(due));
        Assert.Equal(due, s.MostRecentSendAtUtc(due.AddHours(5)));
    }

    [Fact]
    public void ASendTimeInsideTheSpringForwardGap_IsDueAtTheFirstLocalMinuteAfterIt()
    {
        var zone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        var s = new DailyHealthReportSchedule(true, new TimeOnly(2, 30), zone, "t");

        /* 2026-03-08: 02:00 EST jumps to 03:00 EDT at 07:00Z, so 02:30 never happens that day. */
        Assert.False(s.IsDue(new DateTime(2026, 3, 8, 6, 59, 0, DateTimeKind.Utc)));
        Assert.True(s.IsDue(new DateTime(2026, 3, 8, 7, 0, 0, DateTimeKind.Utc)));

        /* The period still ends at a real instant: the same minute an hour later, 03:30 EDT = 07:30Z. */
        Assert.Equal(new DateTime(2026, 3, 8, 7, 30, 0, DateTimeKind.Utc), s.MostRecentSendAtUtc(new DateTime(2026, 3, 8, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void AcrossTheFallBackRepeat_TheLocalDateStaysOneDay_SoTheGateSendsOnce()
    {
        var zone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        var s = new DailyHealthReportSchedule(true, new TimeOnly(1, 30), zone, "t");

        /* 2026-11-01: 01:30 happens twice, 05:30Z (EDT) and 06:30Z (EST). Both are the same local date. */
        var first = new DateTime(2026, 11, 1, 5, 30, 0, DateTimeKind.Utc);
        var second = new DateTime(2026, 11, 1, 6, 30, 0, DateTimeKind.Utc);
        Assert.True(s.IsDue(first));
        Assert.Equal(s.LocalDate(first), s.LocalDate(second));
    }

    /* ---------------- the scheduled send ---------------- */

    internal sealed class Source : IDailyHealthReportSource
    {
        public List<DateTime> Periods { get; } = new();
        public bool Throw { get; set; }
        public string Text { get; set; } = "Fleet is healthy.";

        public Task<DailyHealthReportRendered> BuildAsync(DateTime periodEndUtc, CancellationToken ct)
        {
            Periods.Add(periodEndUtc);
            if (Throw)
            {
                throw new InvalidOperationException("store unreachable");
            }

            return Task.FromResult(new DailyHealthReportRendered("Subject line", Text, "<html><body><b>report</b></body></html>", 3));
        }
    }

    internal sealed class Deliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();
        public bool Fail { get; set; }

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            var fanout = new EmailFanoutResult(
                EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
                WebhookOutcome: Fail ? AlertChannelOutcome.Failed : AlertChannelOutcome.Delivered,
                WebhookSendError: Fail ? "Slack: 500" : null, AnyChannelConfigured: true);
            return Task.FromResult<AlertDelivery?>(AlertDelivery.FromFanout(fanout, outcome.Muted, trayChannelPresent: false));
        }
    }

    internal sealed class History : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }

    private sealed class Stamps : ISelfAlertDeliveryStampStore
    {
        public Dictionary<string, DateTime> Values { get; } = new(StringComparer.Ordinal);
        public int Writes { get; private set; }

        public Task<DateTime?> GetDeliveredAtUtcAsync(string stateKey, CancellationToken cancellationToken) =>
            Task.FromResult(Values.TryGetValue(stateKey, out var at) ? at : (DateTime?)null);

        public Task RecordDeliveredAtUtcAsync(string stateKey, DateTime deliveredAtUtc, CancellationToken cancellationToken)
        {
            Writes++;
            Values[stateKey] = deliveredAtUtc;
            return Task.CompletedTask;
        }
    }

    /// <summary>One "process": its own evaluator, deliverer, source and clock over a stamp store that survives it.</summary>
    internal sealed class Process
    {
        public DarlingConfig Config { get; } = new();
        public Deliverer Deliverer { get; } = new();
        public Source Source { get; } = new();
        public DateTime Now { get; set; } = Day;
        public DailyHealthReportSchedule Schedule { get; set; } = Utc();
        public DarlingSelfAlertEvaluator Evaluator { get; }

        public Process(ISelfAlertDeliveryStampStore stamps)
        {
            Evaluator = new DarlingSelfAlertEvaluator(
                new DarlingAlertSettings(Config), Deliverer, new History(), _ => false,
                logger: null, utcNow: () => Now, deliveryStamps: stamps)
            {
                DailyReportSource = Source,
                DailyReportSchedule = () => Schedule,
            };
        }

        public Task TickAsync(DateTime at)
        {
            Now = at;
            return Evaluator.EvaluateDailyHealthReportAsync(Ct);
        }
    }

    [Fact]
    public async Task NothingIsBuiltOrSentBeforeSendAt()
    {
        var p = new Process(new Stamps());
        await p.TickAsync(Day.AddHours(6).AddMinutes(59));

        Assert.Empty(p.Source.Periods);
        Assert.Empty(p.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ItSendsAtSendAt_OnceAlertPipelineShaped_AndOnlyOncePerLocalDay()
    {
        var p = new Process(new Stamps());
        await p.TickAsync(Day.AddHours(7));
        await p.TickAsync(Day.AddHours(7).AddSeconds(15));
        await p.TickAsync(Day.AddHours(20));

        var sent = Assert.Single(p.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.DailyHealthReportMetric, sent.MetricName);
        Assert.Equal("Daily Health Report", sent.MetricName);
        Assert.Null(sent.Severity);
        Assert.Equal(3, sent.NumericCurrentValue);
        Assert.Equal("Fleet is healthy.", sent.DetailText);
        Assert.Equal("<html><body><b>report</b></body></html>", sent.Context!.EmailHtmlBody);
        Assert.Equal("Subject line", sent.Context.EmailSubject);
        Assert.Equal(Day.AddHours(7), Assert.Single(p.Source.Periods));
        Assert.Equal("daily_health_report_delivered_at", PgSelfAlertDeliveryStampStore.DailyHealthReportStateKey);
        Assert.Equal(AlertFamily.Reports, AlertFamily.Of(sent.MetricName));
    }

    [Fact]
    public async Task AServiceDownAtSendAt_SendsWhenItIsBackTheSameLocalDay()
    {
        var p = new Process(new Stamps());
        await p.TickAsync(Day.AddHours(15));

        Assert.Single(p.Deliverer.Outcomes);
        /* The period is the 24 hours that ended at the send time, not at the late check. */
        Assert.Equal(Day.AddHours(7), Assert.Single(p.Source.Periods));
    }

    [Fact]
    public async Task ItSendsAgainTheNextDay_AndNeverBackfillsAMissedOne()
    {
        var p = new Process(new Stamps());
        await p.TickAsync(Day.AddHours(7));
        await p.TickAsync(Day.AddDays(1).AddHours(7));
        /* The 17th is skipped entirely: the service is back on the 18th and sends one report, not two. */
        await p.TickAsync(Day.AddDays(3).AddHours(8));

        Assert.Equal(3, p.Deliverer.Outcomes.Count);
        Assert.Equal(Day.AddDays(3).AddHours(7), p.Source.Periods[2]);
    }

    [Fact]
    public async Task ADisabledReportSendsNothing_AndBuildsNothing()
    {
        var p = new Process(new Stamps()) { Schedule = Utc(enabled: false) };
        await p.TickAsync(Day.AddHours(12));

        Assert.Empty(p.Source.Periods);
        Assert.Empty(p.Deliverer.Outcomes);
    }

    [Fact]
    public async Task TheMasterAlertSwitchOff_SendsNothing()
    {
        var p = new Process(new Stamps());
        p.Config.Alerts.Enabled = false;
        await p.TickAsync(Day.AddHours(12));

        Assert.Empty(p.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ARestartTheSameLocalDay_DoesNotSendASecond_ButTheNextDayDoes()
    {
        var stamps = new Stamps();
        var before = new Process(stamps);
        await before.TickAsync(Day.AddHours(7));
        Assert.Single(before.Deliverer.Outcomes);

        /* A new process over the same stamp store: empty memory, the stamp is what says it went out. */
        var after = new Process(stamps);
        await after.TickAsync(Day.AddHours(9));
        Assert.Empty(after.Deliverer.Outcomes);

        await after.TickAsync(Day.AddDays(1).AddHours(7));
        Assert.Single(after.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AFailedDelivery_WritesNoStamp_AndIsRetriedTheSameDayAfterTheDelay()
    {
        var stamps = new Stamps();
        var p = new Process(stamps);
        p.Deliverer.Fail = true;
        await p.TickAsync(Day.AddHours(7));
        Assert.Equal(0, stamps.Writes);

        /* Inside the retry delay nothing is attempted, so a store that cannot answer is not asked every tick. */
        await p.TickAsync(Day.AddHours(7).AddMinutes(1));
        Assert.Single(p.Deliverer.Outcomes);

        p.Deliverer.Fail = false;
        await p.TickAsync(Day.AddHours(7).Add(DarlingSelfAlertEvaluator.DailyReportRetryDelay));
        Assert.Equal(2, p.Deliverer.Outcomes.Count);
        Assert.Equal(1, stamps.Writes);
    }

    [Fact]
    public async Task AFailedBuild_IsRetriedTheSameDayAfterTheDelay_NotEveryTick()
    {
        var p = new Process(new Stamps());
        p.Source.Throw = true;
        await p.TickAsync(Day.AddHours(7));
        await p.TickAsync(Day.AddHours(7).AddSeconds(15));
        Assert.Single(p.Source.Periods);

        p.Source.Throw = false;
        await p.TickAsync(Day.AddHours(7).Add(DarlingSelfAlertEvaluator.DailyReportRetryDelay));
        Assert.Single(p.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ALongText_IsCutAtALine_ForChatChannels_WhileTheHtmlStaysWhole()
    {
        var p = new Process(new Stamps());
        p.Source.Text = string.Join("\n", System.Linq.Enumerable.Repeat(new string('x', 99), 400));
        await p.TickAsync(Day.AddHours(7));

        var sent = Assert.Single(p.Deliverer.Outcomes);
        Assert.True(sent.DetailText!.Length < DarlingSelfAlertEvaluator.DailyReportMaxTextLength + 200);
        Assert.Contains("Shortened for chat", sent.DetailText, StringComparison.Ordinal);
        Assert.Equal("<html><body><b>report</b></body></html>", sent.Context!.EmailHtmlBody);
    }

    /* ---------------- the manual send ---------------- */

    [Fact]
    public async Task SendNow_BuildsForTheLastSendAt_AndLeavesTheDeliveredTodayStampAlone()
    {
        var stamps = new Stamps();
        var p = new Process(stamps);
        p.Now = Day.AddHours(10);

        var result = await p.Evaluator.SendDailyHealthReportNowAsync(Ct);

        Assert.True(result.Sent);
        Assert.Equal(Day.AddHours(7), result.PeriodEndUtc);
        Assert.Single(p.Deliverer.Outcomes);
        Assert.Equal(0, stamps.Writes);
        Assert.Empty(stamps.Values);

        /* The scheduled one is unaffected: it still goes out, and it is the one that stamps. */
        await p.TickAsync(Day.AddHours(10).AddMinutes(1));
        Assert.Equal(2, p.Deliverer.Outcomes.Count);
        Assert.Equal(1, stamps.Writes);
    }

    [Fact]
    public async Task SendNow_WorksWhileTheReportIsOff_ButNotWithTheMasterSwitchOff()
    {
        var p = new Process(new Stamps()) { Schedule = Utc(enabled: false) };
        p.Now = Day.AddHours(10);
        Assert.True((await p.Evaluator.SendDailyHealthReportNowAsync(Ct)).Sent);

        p.Config.Alerts.Enabled = false;
        var refused = await p.Evaluator.SendDailyHealthReportNowAsync(Ct);
        Assert.False(refused.Sent);
        Assert.Equal("alerts_disabled", refused.Reason);
        Assert.Single(p.Deliverer.Outcomes);
    }
}
