using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards the Lite server-tab badge acknowledgement logic for the low-disk (#754) and
/// failed-Agent-job (#749) conditions added in #1128. Those conditions are plain booleans with
/// no event timestamp, so the timestamp-based ack clear in
/// <see cref="AlertStateService.UpdateAlertCounts"/> never fires for them. The review fix added
/// <see cref="AlertStateService.ClearAcknowledgementForNewCondition"/> — the false-&gt;true
/// transition hook MainWindow calls so a freshly-breaching disk or a brand-new failed job
/// re-lights an acknowledged badge, matching the Dashboard's re-show behaviour.
///
/// Each test uses a unique server id so the shared (CWD-relative) alert_state.json the service
/// persists to cannot leak state between tests; assertions read in-memory state, so they hold
/// even when the best-effort file write is unavailable.
/// </summary>
public class AlertBadgeAckTests
{
    private static string NewServerId() => "badge-test-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void StandingLowDisk_AfterAck_StaysSuppressed_UntilNewConditionClearsIt()
    {
        var svc = new AlertStateService();
        var server = NewServerId();

        /* A standing low-disk breach lights the badge... */
        Assert.True(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: true, hasFailedJob: false, latestEventTimeUtc: null));

        /* ...the user dismisses it: the badge stays suppressed even though the breach is still
           standing — with no event timestamp, UpdateAlertCounts can never auto-clear the ack. */
        svc.AcknowledgeAlert(server);
        Assert.False(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: true, hasFailedJob: false, latestEventTimeUtc: null));

        /* A fresh false->true transition (re-breach / new job) clears the ack and re-lights. */
        var fired = false;
        svc.SuppressionStateChanged += (_, _) => fired = true;
        svc.ClearAcknowledgementForNewCondition(server);
        Assert.True(fired);
        Assert.True(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: true, hasFailedJob: false, latestEventTimeUtc: null));
    }

    [Fact]
    public void FailedJob_AfterAck_StaysSuppressed_UntilNewConditionClearsIt()
    {
        var svc = new AlertStateService();
        var server = NewServerId();

        Assert.True(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: false, hasFailedJob: true, latestEventTimeUtc: null));

        svc.AcknowledgeAlert(server);
        Assert.False(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: false, hasFailedJob: true, latestEventTimeUtc: null));

        svc.ClearAcknowledgementForNewCondition(server);
        Assert.True(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: false, hasFailedJob: true, latestEventTimeUtc: null));
    }

    [Fact]
    public void ClearAcknowledgementForNewCondition_WhenNothingAcknowledged_DoesNotFireEvent()
    {
        var svc = new AlertStateService();
        var server = NewServerId();

        var fired = false;
        svc.SuppressionStateChanged += (_, _) => fired = true;

        /* No prior ack for this server -> nothing to clear -> no event, no save churn. */
        svc.ClearAcknowledgementForNewCondition(server);
        Assert.False(fired);

        /* And a standing condition still lights the badge (it was never suppressed). */
        Assert.True(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: true, hasFailedJob: false, latestEventTimeUtc: null));
    }

    [Fact]
    public void SilencedServer_NewCondition_StaysSuppressed()
    {
        /* ClearAcknowledgementForNewCondition only clears the *ack*; a fully-silenced server must
           stay dark even when a new disk/job condition appears. */
        var svc = new AlertStateService();
        var server = NewServerId();

        svc.SilenceServer(server);
        svc.ClearAcknowledgementForNewCondition(server);
        Assert.False(svc.UpdateAlertCounts(server, 0, 0, hasLowDisk: true, hasFailedJob: true, latestEventTimeUtc: null));
    }
}
