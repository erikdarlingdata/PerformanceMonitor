/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Darling.Tests;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4795: Lite's connection loop does not wait for a "Server Unreachable" send, and the retry's due time only moves
/// when the send's answer has been recorded, which can be a whole SMTP timeout later. Without a guard the next 30
/// second tick still saw the retry due and sent the alert (and the tray balloon) a second time, and if the slow
/// send then got through, both did. <see cref="ConnectionAlertSendsInFlight"/> holds the retry back for a server
/// while its send is running.
/// <para>
/// The loop that sends lives in <c>MainWindow</c> and cannot be built in a test, so the rig below runs the same
/// steps with the real <see cref="ConnectionAlertPolicy"/>, the real <see cref="FailedSendRetryTracker"/> and the
/// real guard, with each send left pending on a <see cref="TaskCompletionSource{TResult}"/> until the test
/// completes it. The source pins at the end hold <c>MainWindow</c> to those steps.
/// </para>
/// </summary>
public sealed class ConnectionAlertRetryInFlightTests
{
    private const string ServerId = "6f1c2d3e-0000-4000-8000-00000000000a";
    private const string OtherServerId = "6f1c2d3e-0000-4000-8000-00000000000b";
    private static readonly DateTime Start = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(30);

    private static AlertDelivery Failed() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.Failed, SendError: "SMTP: timed out",
            WebhookOutcome: AlertChannelOutcome.NotAttempted, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery Delivered() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.Delivered, SendError: null,
            WebhookOutcome: AlertChannelOutcome.NotAttempted, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    /// <summary>The connection loop's per-server steps, in the order <c>CheckConnectionsAndNotify</c> takes them.</summary>
    private sealed class Rig
    {
        private readonly Dictionary<string, bool> _previousStates = new();
        private readonly Dictionary<string, DateTime> _lastDownAlert = new();
        private readonly List<Task> _notes = new();

        public FailedSendRetryTracker Retries { get; } = new();

        public ConnectionAlertSendsInFlight Sends { get; } = new();

        public DateTime Now { get; set; } = Start;

        /// <summary>Every send the loop started, left pending until the test completes it.</summary>
        public List<TaskCompletionSource<AlertDelivery?>> Pending { get; } = new();

        public int SendCount => Pending.Count;

        public ConnectionAlertDecision Tick(string serverId, bool online, int refireMinutes = 0)
        {
            bool? previouslyOnline = _previousStates.TryGetValue(serverId, out var prev) ? prev : null;
            var decision = ConnectionAlertPolicy.Decide(
                previouslyOnline,
                online,
                alertWhenAlreadyDownAtFirstSight: false,
                refireMinutes > 0 ? TimeSpan.FromMinutes(refireMinutes) : null,
                _lastDownAlert.TryGetValue(serverId, out var lastDown) ? lastDown : null,
                Now,
                Sends.RetryDueUtc(serverId, Retries));

            if (decision == ConnectionAlertDecision.Restored)
            {
                Retries.Clear(serverId);
            }

            if (decision is ConnectionAlertDecision.Lost
                or ConnectionAlertDecision.AlreadyDownAtFirstSight
                or ConnectionAlertDecision.StillDown)
            {
                var send = new TaskCompletionSource<AlertDelivery?>(TaskCreationOptions.RunContinuationsAsynchronously);
                Pending.Add(send);
                _lastDownAlert[serverId] = Now;
                Sends.Begin(serverId);
                _notes.Add(NoteSentAsync(serverId, send.Task));
            }
            else if (decision == ConnectionAlertDecision.Restored)
            {
                _lastDownAlert.Remove(serverId);
            }

            _previousStates[serverId] = online;
            return decision;
        }

        /// <summary>What <c>NoteConnectionAlertSentAsync</c> does with a send's answer.</summary>
        private async Task NoteSentAsync(string serverId, Task<AlertDelivery?> send)
        {
            try
            {
                var delivery = await send;
                if (!_previousStates.TryGetValue(serverId, out var onlineNow) || onlineNow)
                {
                    return;
                }

                Retries.Record(serverId, delivery, Now, Cooldown);
            }
            finally
            {
                Sends.End(serverId);
            }
        }

        /// <summary>Answers one pending send and waits for the loop's note of that answer, and only that one.</summary>
        public async Task CompleteAsync(int sendIndex, AlertDelivery? answer)
        {
            Pending[sendIndex].SetResult(answer);
            await _notes[sendIndex];
        }

        public void At(TimeSpan afterStart) => Now = Start + afterStart;
    }

    /// <summary>A server that was seen online, then goes down: the first "Server Unreachable" is send 0.</summary>
    private static async Task<Rig> RigWithAFirstSendThatFailedAsync()
    {
        var rig = new Rig();
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: true));

        rig.At(TimeSpan.FromSeconds(30));
        Assert.Equal(ConnectionAlertDecision.Lost, rig.Tick(ServerId, online: false));
        await rig.CompleteAsync(0, Failed());
        Assert.Equal(Start.AddSeconds(90), rig.Retries.DueUtc(ServerId));
        return rig;
    }

    [Fact]
    public async Task TwoTicksWhileARetrySendIsPending_GiveOneSend()
    {
        var rig = await RigWithAFirstSendThatFailedAsync();

        /* Not due yet: a tick 30 seconds after the failure stays quiet, guard or no guard. */
        rig.At(TimeSpan.FromSeconds(60));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));
        Assert.Equal(1, rig.SendCount);

        /* Due: the retry goes out, and its answer does not come back. */
        rig.At(TimeSpan.FromSeconds(90));
        Assert.Equal(ConnectionAlertDecision.StillDown, rig.Tick(ServerId, online: false));
        Assert.Equal(2, rig.SendCount);

        /* The next two ticks fall inside the send. The retry is still "due" on the tracker's books, but it is
           already on its way. */
        rig.At(TimeSpan.FromSeconds(120));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));
        rig.At(TimeSpan.FromSeconds(150));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));

        Assert.Equal(2, rig.SendCount);
    }

    [Fact]
    public async Task WhenThePendingSendFails_TheNextDueTickSendsAgain_AndTheBackoffStreakIsKept()
    {
        var rig = await RigWithAFirstSendThatFailedAsync();

        rig.At(TimeSpan.FromSeconds(90));
        Assert.Equal(ConnectionAlertDecision.StillDown, rig.Tick(ServerId, online: false));
        rig.At(TimeSpan.FromSeconds(120));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));

        /* The retry's answer arrives, and again no channel got it: the second failure in a row waits two minutes
           from now. Clearing the tracker when the retry was sent would have started the streak over at a minute. */
        rig.At(TimeSpan.FromSeconds(160));
        await rig.CompleteAsync(1, Failed());
        Assert.Equal(Start.AddSeconds(160).AddMinutes(2), rig.Retries.DueUtc(ServerId));

        rig.At(TimeSpan.FromSeconds(220));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));
        rig.At(TimeSpan.FromSeconds(279));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));
        Assert.Equal(2, rig.SendCount);

        rig.At(TimeSpan.FromSeconds(280));
        Assert.Equal(ConnectionAlertDecision.StillDown, rig.Tick(ServerId, online: false));
        Assert.Equal(3, rig.SendCount);
    }

    [Fact]
    public async Task WhenThePendingSendDelivers_NothingIsSentAgainWhileTheServerStaysDown()
    {
        var rig = await RigWithAFirstSendThatFailedAsync();

        rig.At(TimeSpan.FromSeconds(90));
        Assert.Equal(ConnectionAlertDecision.StillDown, rig.Tick(ServerId, online: false));
        rig.At(TimeSpan.FromSeconds(120));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));

        await rig.CompleteAsync(1, Delivered());
        Assert.Null(rig.Retries.DueUtc(ServerId));

        rig.At(TimeSpan.FromHours(3));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));
        Assert.Equal(2, rig.SendCount);
    }

    [Fact]
    public async Task ARestoreWhileTheRetrySendIsPending_DropsItsAnswer_AndTheNextOutageStartsClean()
    {
        var rig = await RigWithAFirstSendThatFailedAsync();

        rig.At(TimeSpan.FromSeconds(90));
        Assert.Equal(ConnectionAlertDecision.StillDown, rig.Tick(ServerId, online: false));

        rig.At(TimeSpan.FromSeconds(120));
        Assert.Equal(ConnectionAlertDecision.Restored, rig.Tick(ServerId, online: true));

        /* The retry send finally reports that no channel got it. The outage it would retry is over. */
        await rig.CompleteAsync(1, Failed());
        Assert.Null(rig.Retries.DueUtc(ServerId));

        rig.At(TimeSpan.FromSeconds(150));
        Assert.Equal(ConnectionAlertDecision.Lost, rig.Tick(ServerId, online: false));
        Assert.Equal(3, rig.SendCount);
    }

    [Fact]
    public async Task ARetryHeldBackForOneServer_DoesNotHoldBackAnother()
    {
        var rig = await RigWithAFirstSendThatFailedAsync();

        rig.At(TimeSpan.FromSeconds(90));
        Assert.Equal(ConnectionAlertDecision.StillDown, rig.Tick(ServerId, online: false));

        /* A second server goes down and its first send fails while the first server's retry is still running. */
        rig.Tick(OtherServerId, online: true);
        rig.At(TimeSpan.FromSeconds(100));
        Assert.Equal(ConnectionAlertDecision.Lost, rig.Tick(OtherServerId, online: false));
        await rig.CompleteAsync(2, Failed());

        rig.At(TimeSpan.FromSeconds(160));
        Assert.Equal(ConnectionAlertDecision.StillDown, rig.Tick(OtherServerId, online: false));
        Assert.Equal(ConnectionAlertDecision.None, rig.Tick(ServerId, online: false));
    }

    [Fact]
    public void AnOverlappingSend_KeepsTheHold_UntilTheLastOneEnds()
    {
        var sends = new ConnectionAlertSendsInFlight();
        var retries = new FailedSendRetryTracker();
        retries.Record(ServerId, Failed(), Start, Cooldown);
        var due = retries.DueUtc(ServerId);
        Assert.NotNull(due);
        Assert.Equal(due, sends.RetryDueUtc(ServerId, retries));

        sends.Begin(ServerId);
        sends.Begin(ServerId);
        Assert.Null(sends.RetryDueUtc(ServerId, retries));

        sends.End(ServerId);
        Assert.Null(sends.RetryDueUtc(ServerId, retries));

        sends.End(ServerId);
        Assert.Equal(due, sends.RetryDueUtc(ServerId, retries));

        /* An End nobody began is nothing, and does not go negative. */
        sends.End(ServerId);
        sends.Begin(ServerId);
        Assert.Null(sends.RetryDueUtc(ServerId, retries));
        sends.End(ServerId);
        Assert.Equal(due, sends.RetryDueUtc(ServerId, retries));
    }

    /* ---------------- MainWindow is held to the same steps ---------------- */

    private static string Squash(string source) => Regex.Replace(source, @"\s+", " ");

    private static string WindowSource() => ParitySource.ReadFile("Lite/MainWindow.xaml.cs");

    private static string AlertEngineSource() => ParitySource.ReadFile("Lite/MainWindow.AlertEngine.cs");

    /// <summary>The text of one member, whitespace squashed: from its signature to the first line that closes a
    /// member (four spaces, then a brace), found before the squash.</summary>
    private static string Member(string rawSource, string signature)
    {
        var start = rawSource.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"the source has no member starting {signature}");
        var end = rawSource.IndexOf("\n    }", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the member starting {signature} has no closing brace");
        return Squash(rawSource[start..end]);
    }

    [Fact]
    public void TheTick_AsksTheGuardForTheRetryTime_NotTheTrackerDirectly()
    {
        var tick = Member(WindowSource(), "private void CheckConnectionsAndNotify()");

        Assert.Contains(
            "_connectionAlertSends.RetryDueUtc(server.Id, _connectionAlertRetries));", tick, StringComparison.Ordinal);
        Assert.DoesNotContain("_connectionAlertRetries.DueUtc(", tick, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServer_IsMarkedRunning_RightBeforeTheSendIsHandedToTheNote()
    {
        var tick = Member(WindowSource(), "private void CheckConnectionsAndNotify()");

        Assert.Contains(
            "_connectionAlertSends.Begin(server.Id); _ = NoteConnectionAlertSentAsync(server.Id, send);",
            tick, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMark_IsReleasedInAFinally_OfTheNote()
    {
        var note = Member(
            AlertEngineSource(),
            "private async Task NoteConnectionAlertSentAsync(string serverId, Task<PerformanceMonitor.Notifications.AlertDelivery?> send)");

        var caught = note.IndexOf("catch (Exception ex)", StringComparison.Ordinal);
        var finallyBlock = note.IndexOf("finally { _connectionAlertSends.End(serverId); }", StringComparison.Ordinal);
        Assert.True(caught >= 0, "the note has lost its catch");
        Assert.True(finallyBlock > caught, "the guard's End must sit in a finally after the catch, so no exit skips it");
    }

    [Fact]
    public void SendingARetry_DoesNotResetTheBackoffStreak()
    {
        var tick = Member(WindowSource(), "private void CheckConnectionsAndNotify()");
        var send = tick.IndexOf("var send = SendConnectionAlert(server, \"Server Unreachable\"", StringComparison.Ordinal);
        var note = tick.IndexOf("_ = NoteConnectionAlertSentAsync(server.Id, send);", StringComparison.Ordinal);
        Assert.True(send >= 0 && note > send, "the tick no longer sends and then notes the send");

        Assert.DoesNotContain("_connectionAlertRetries.Clear(", tick[send..note], StringComparison.Ordinal);
    }

    [Fact]
    public void ARemovedServer_DropsItsPendingRetry_ItsConnectionMarks_AndItsCollectorErrorAndXeSessionMarks()
    {
        var forget = Member(WindowSource(), "private async Task RemoveServerAsync(ServerConnection server)");

        Assert.Contains("_connectionAlertRetries.Clear(server.Id);", forget, StringComparison.Ordinal);
        Assert.Contains("_previousConnectionStates.Remove(server.Id);", forget, StringComparison.Ordinal);
        Assert.Contains("_lastConnectionDownAlertUtc.Remove(server.Id);", forget, StringComparison.Ordinal);

        /* #4795: the two marks the tick keeps beside the connection state, keyed the same way. Left behind they
           are never read again for a server that is gone and are inherited by a re-add, whose first tick would
           then compare its first collector-error and XE-session readings against the removed server's. */
        Assert.Contains("_previousCollectorErrorStates.Remove(server.Id);", forget, StringComparison.Ordinal);
        Assert.Contains("_previousXeSessionFailureStates.Remove(server.Id);", forget, StringComparison.Ordinal);
    }

    /// <summary>The CODE of one member, whitespace squashed, with comments and string text blanked first so a pin on
    /// the order of statements reads statements and not the prose around them (a comment can say "await").</summary>
    private static string CodeOf(string rawSource, string signature)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(rawSource);
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"the source has no member starting {signature}");
        var end = code.IndexOf("\n    }", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the member starting {signature} has no closing brace");
        return Squash(code[start..end]);
    }

    [Fact]
    public void TheAvailabilityGroupSweep_ReadsTheServersGenerationBeforeItsFirstAwait_AndHandsItToBothEvaluations()
    {
        var sweep = CodeOf(
            AlertEngineSource(),
            "private async Task EvaluateAvailabilityGroupAlertsAsync(int serverId, string serverName, bool suppressed)");

        var capture = sweep.IndexOf("var generation = _agAlertEvaluator.GenerationOf(serverId);", StringComparison.Ordinal);
        var firstAwait = sweep.IndexOf("await ", StringComparison.Ordinal);
        Assert.True(capture >= 0, "the sweep no longer reads the server's generation");
        Assert.True(firstAwait > capture, "the generation must be read before the sweep's first await, or a removal during a read goes unseen");

        Assert.Matches(@"_agAlertEvaluator\.EvaluateReplicas\([^;]*sweepGeneration: generation\)\);", sweep);
        Assert.Matches(@"_agAlertEvaluator\.EvaluateDatabases\([^;]*sweepGeneration: generation\)\);", sweep);
    }

    [Fact]
    public void TheAvailabilityGroupSweep_IsLaunchedOnlyForAServerTheRegistryStillHolds()
    {
        var alerts = CodeOf(AlertEngineSource(), "private async void CheckPerformanceAlerts(ServerSummaryItem summary)");

        /* A summary the timer copied before a server was removed reaches this method after the removal's Forget. The
           live lookup is null for it, and the launch must stand on that: the sweep reads its generation on its first
           line, after the Forget, so it cannot tell that the server is gone. */
        var lookup = alerts.IndexOf("var badgeServer = _serverManager.GetAllServers()", StringComparison.Ordinal);
        var launch = alerts.IndexOf("if (App.NotifyAgHealth && badgeServer != null) {", StringComparison.Ordinal);
        Assert.True(lookup >= 0, "the sweep entry no longer looks the server up in the registry");
        Assert.True(launch > lookup, "the AG launch must be gated on the live registry lookup");
        Assert.Contains(
            "_ = EvaluateAvailabilityGroupAlertsAsync(summary.ServerId, summary.DisplayName, suppressPopups);",
            alerts[launch..], StringComparison.Ordinal);

        /* Nothing is awaited between the lookup and the launch, so a server the lookup found is still registered when
           the sweep reads its generation. */
        Assert.DoesNotContain("await ", alerts[lookup..launch], StringComparison.Ordinal);
    }

    [Fact]
    public void ARemoval_AwaitsNothingBetweenItsFirstDropAndTheRegistryDelete()
    {
        var removal = CodeOf(WindowSource(), "private async Task RemoveServerAsync(ServerConnection server)");

        var lastAwait = removal.LastIndexOf("await ", StringComparison.Ordinal);
        var delete = removal.IndexOf("_serverManager.DeleteServer(server.Id);", StringComparison.Ordinal);
        Assert.True(lastAwait >= 0, "the removal no longer clears the server's tags");
        Assert.True(delete > lastAwait, "the registry delete must come after the removal's last await");

        /* Every drop sits between the last await and the delete: a timer tick that runs while the removal waits still
           sees the server whole, and none runs between the first drop and the delete. */
        string[] drops =
        {
            "_collectorService?.ClearHealthForServer(removedServerId);",
            "_agAlertEvaluator.Forget(removedServerId);",
            "_connectionAlertRetries.Clear(server.Id);",
            "_previousConnectionStates.Remove(server.Id);",
            "_lastConnectionDownAlertUtc.Remove(server.Id);",
            "_previousCollectorErrorStates.Remove(server.Id);",
            "_previousXeSessionFailureStates.Remove(server.Id);",
            "_collectorService?.DeltaCalculator?.ClearServer(removedServerId);",
        };
        foreach (var drop in drops)
        {
            var at = removal.IndexOf(drop, StringComparison.Ordinal);
            Assert.True(at >= 0, $"the removal no longer runs {drop}");
            Assert.True(at > lastAwait, $"{drop} must come after every await in the removal");
            Assert.True(at < delete, $"{drop} must come before the registry delete");
        }
    }

    [Fact]
    public void BothDoors_LeaveTheRegistryDeleteToTheRemoval()
    {
        var sidebar = CodeOf(
            WindowSource(), "private async void ServerContextMenu_Remove_Click(object sender, RoutedEventArgs e)");
        Assert.Contains("await RemoveServerAsync(server);", sidebar, StringComparison.Ordinal);
        Assert.DoesNotContain("DeleteServer", sidebar, StringComparison.Ordinal);

        /* Manage Servers gets the removal from MainWindow, requires it, and has no delete of its own. */
        var manage = CodeOf(
            ParitySource.ReadFile("Lite/Windows/ManageServersWindow.xaml.cs"),
            "private async void DeleteButton_Click(object sender, RoutedEventArgs e)");
        Assert.Contains("await _removeServer(selected);", manage, StringComparison.Ordinal);
        Assert.DoesNotContain("DeleteServer", manage, StringComparison.Ordinal);
        Assert.DoesNotContain("_removeServer is not null", manage, StringComparison.Ordinal);
        Assert.Contains(
            "Func<ServerConnection, Task> removeServer)",
            ParitySource.ReadFile("Lite/Windows/ManageServersWindow.xaml.cs"), StringComparison.Ordinal);
        Assert.Contains(
            "new ManageServersWindow(_serverManager, _profileManager, RemoveServerAsync)",
            CodeOf(WindowSource(), "private void ManageServersButton_Click(object sender, RoutedEventArgs e)"),
            StringComparison.Ordinal);
    }
}
