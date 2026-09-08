using System;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Pins what the Dashboard's Alert History grid shows in its Value column (#1913), and — just as
/// importantly — pins WHY the Dashboard does not participate in the em-dash rendering Lite and the
/// Darling Viewer got from #1846/#1881.
///
/// <para><b>The Dashboard's alert history is STRING-typed end to end.</b> The fire site passes display
/// text, <c>JsonAlertHistoryStore</c> copies it into <c>AlertLogEntry.CurrentValue</c> (a string) and
/// discards <c>AlertHistoryRecord</c>'s numeric slots, and the grid binds that string directly. There is
/// no double on this path, so there is no "0.00" to render and nothing for
/// <see cref="AlertMetricClassifier.FormatHistoryValue"/> — which takes a double — to be called with.</para>
///
/// <para>That is not a gap to close. The em dash exists because Lite's and Darling's value columns are
/// NOT NULL doubles, so a metric whose value is a STATE has nowhere to put it; the Dashboard keeps the
/// state and shows it. Making the Dashboard numeric in order to render a dash would REPLACE
/// "Blocking and Deadlock" with "—", which is strictly less information. The tests below fix that
/// reasoning in place so the next person to notice the divergence finds the answer instead of
/// re-deriving it — this suite exists because the divergence was filed as a bug once already.</para>
/// </summary>
public class DashboardAlertHistoryValueTests
{
    private sealed class FakePreferencesService : IUserPreferencesService
    {
        public UserPreferences Preferences { get; } = new();
        public UserPreferences GetPreferences() => Preferences;
        public void SavePreferences(UserPreferences preferences) { }
        public void UpdateWaitStatsRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateCpuRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateMemoryRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateFileIoRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateExpensiveQueriesRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateBlockingRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateCollectionHealthRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
    }

    private const int Threshold = 30;

    /* ---------------- #1913: the Value column carries the incident's actual figures ---------------- */

    [Fact]
    public void CollectionStopped_DisabledJobsBranch_ShortValueKeepsTheTotal()
    {
        /* The fire site used to build this string itself and reported only the disabled count. "2 job(s)
           disabled" reads the same on a 6-job server (partially stopped, some data still landing) and on
           a 2-job server (completely stopped) — and the reason beside it draws exactly that distinction. */
        var r = DatabaseService.DecideCollectionStopped(
            disabledJobs: 2, totalJobs: 6, minutesSince: 1, thresholdMinutes: Threshold);

        Assert.Equal("2 of 6 job(s) disabled", r.ShortValue);
        Assert.Contains("2 of 6", r.Reason);
    }

    [Fact]
    public void CollectionStopped_AllJobsDisabled_ShortValueSaysSoWithoutAmbiguity()
    {
        var r = DatabaseService.DecideCollectionStopped(
            disabledJobs: 6, totalJobs: 6, minutesSince: 2, thresholdMinutes: Threshold);

        Assert.Equal("6 of 6 job(s) disabled", r.ShortValue);
    }

    [Fact]
    public void CollectionStopped_StalenessBranch_ShortValueCarriesTheMinutes()
    {
        /* THE defect this issue is really about. The staleness branch used to report the constant
           "no recent collection" — which restates the metric name and discards the one figure the alert
           was raised to report. The reason and the email both carried the minutes; only the grid did not. */
        var r = DatabaseService.DecideCollectionStopped(
            disabledJobs: 0, totalJobs: 6, minutesSince: 47, thresholdMinutes: Threshold);

        Assert.Equal("no collection in 47m", r.ShortValue);
        Assert.Contains("47 minutes", r.Reason);
        Assert.DoesNotContain("no recent collection", r.ShortValue!, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectionStopped_Healthy_HasNoShortValue()
    {
        var r = DatabaseService.DecideCollectionStopped(
            disabledJobs: 0, totalJobs: 6, minutesSince: 1, thresholdMinutes: Threshold);

        Assert.False(r.Stopped);
        Assert.Null(r.Reason);
        Assert.Null(r.ShortValue);
    }

    [Theory]
    /* disabled-jobs wins over staleness, so both branches are exercised with the other input live. */
    [InlineData(2, 6, 99, "disabled")]
    [InlineData(0, 6, 99, "no collection")]
    [InlineData(6, 6, 0, "disabled")]
    public void ShortValueAndReason_AlwaysDescribeTheSameBranch(
        int disabledJobs, int totalJobs, int? minutesSince, string expectedShape)
    {
        /* The drift guard, and the reason ShortValue is computed in DecideCollectionStopped rather than at
           the fire site. Two renderings of one cause, derived twice, is two chances to disagree about which
           branch fired — and disagree is exactly what they did: the grid said "no recent collection" while
           the email explained a disabled-job count, for one incident. */
        var r = DatabaseService.DecideCollectionStopped(disabledJobs, totalJobs, minutesSince, Threshold);

        Assert.True(r.Stopped);
        Assert.Contains(expectedShape, r.ShortValue!, StringComparison.Ordinal);

        var reasonIsDisabledBranch = r.Reason!.Contains("disabled", StringComparison.Ordinal);
        var valueIsDisabledBranch = r.ShortValue!.Contains("disabled", StringComparison.Ordinal);
        Assert.Equal(reasonIsDisabledBranch, valueIsDisabledBranch);
    }

    [Fact]
    public void ShortValue_StaysShortEnoughForTheGridColumn()
    {
        /* The Value column is 200px and its siblings are compact ("87% (Total CPU)", "Session #12 running
           45m"). The fix was NOT to dump the full reason sentence in there — that is what DetailText, the
           Alert Detail window and the email already carry. Both branches stay in the siblings' range. */
        foreach (var r in new[]
        {
            DatabaseService.DecideCollectionStopped(2, 6, 1, Threshold),
            DatabaseService.DecideCollectionStopped(0, 6, 1440, Threshold),
        })
        {
            Assert.NotNull(r.ShortValue);
            Assert.True(
                r.ShortValue!.Length <= 40,
                $"Value-column text should stay compact; got {r.ShortValue.Length} chars: '{r.ShortValue}'");
            Assert.True(r.Reason!.Length > r.ShortValue.Length, "the reason is the long form, by construction");
        }
    }

    /* ---------------- the Dashboard's store is string-typed, and that is deliberate ---------------- */

    [Fact]
    public async Task StateOnlyMetric_KeepsItsDisplayTextVerbatim_AndIsNotDashed()
    {
        /* "Capture Down" IS state-only by the shared classifier, and on Lite and Darling its row renders an
           em dash because their NOT NULL double columns could only store a 0 for it. The Dashboard stores
           the words. This asserts the words survive — the property #1913 assumed was broken. */
        var store = new JsonAlertHistoryStore(new FakePreferencesService());
        var serverId = "test-" + Guid.NewGuid().ToString("N");

        await store.RecordAlertAsync(new AlertHistoryRecord(
            serverId, "Srv", "Capture Down",
            CurrentValueText: "Blocking and Deadlock", ThresholdValueText: "session running",
            NumericCurrentValue: null, NumericThresholdValue: null,
            Delivery: LegacyTrayDelivery(),
            Muted: false, DetailText: null, ContextJson: null));

        var row = Assert.Single(store.GetAlertHistory(200), e => e.ServerId == serverId);

        Assert.True(AlertMetricClassifier.IsStateOnly("Capture Down"), "premise: the metric IS state-only");
        Assert.Equal("Blocking and Deadlock", row.CurrentValue);
        Assert.Equal("session running", row.ThresholdValue);
        Assert.NotEqual(AlertMetricClassifier.StateOnlyDisplay, row.CurrentValue);
        Assert.DoesNotContain("0.00", row.CurrentValue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheStoreDiscardsTheNumericSlots_SoNothingCanCoerceAValueToZero()
    {
        /* The structural reason the Dashboard never had #1846/#1881's defect, asserted rather than assumed.
           AlertHistoryRecord carries optional doubles for the stores that need them; this one ignores them
           and keeps the text. If a future change starts persisting the numerics, this test fails and
           whoever makes that change has to decide — deliberately — what the grid should then render. */
        var store = new JsonAlertHistoryStore(new FakePreferencesService());
        var serverId = "test-" + Guid.NewGuid().ToString("N");

        await store.RecordAlertAsync(new AlertHistoryRecord(
            serverId, "Srv", "Collection Stopped",
            CurrentValueText: "no collection in 47m", ThresholdValueText: "collecting",
            /* Numerics deliberately supplied AND deliberately expected to be ignored. */
            NumericCurrentValue: 47, NumericThresholdValue: 30,
            Delivery: LegacyTrayDelivery(),
            Muted: false, DetailText: null, ContextJson: null));

        var row = Assert.Single(store.GetAlertHistory(200), e => e.ServerId == serverId);

        Assert.Equal("no collection in 47m", row.CurrentValue);
        Assert.Equal("collecting", row.ThresholdValue);
    }

    [Fact]
    public void TheSharedFormatterIsNotReachableFromAStringStore_ByType()
    {
        /* States the boundary as a compile-checked fact rather than a comment: FormatHistoryValue consumes
           a double. The Dashboard's row exposes strings, so "just route the Dashboard through it" is not a
           small change — it requires inventing the numeric the Dashboard deliberately does not keep. The
           formatter still works, and is still right, for the two stores that DO hold doubles. */
        Assert.Equal(AlertMetricClassifier.StateOnlyDisplay,
                     AlertMetricClassifier.FormatHistoryValue("Capture Down", 0));
        Assert.Equal("87.0%", AlertMetricClassifier.FormatHistoryValue("High CPU", 87));

        var dashboardRow = new AlertLogEntry { MetricName = "Capture Down", CurrentValue = "Blocking and Deadlock" };
        Assert.IsType<string>(dashboardRow.CurrentValue);
    }

    /* The deprecated SKU records a hand-decided disposition; its 26 production callers do the same, so its
       tests state one too rather than deriving it. */
#pragma warning disable CS0618
    private static AlertDelivery LegacyTrayDelivery() =>
        AlertDelivery.FromLegacyStoredColumns(true, "tray", null);
#pragma warning restore CS0618
}
