using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7083 part 2, Lite's half: the shared <see cref="AlertEngine"/> holds a count that reached the bar with every wait under
/// 15 seconds, and Lite's own deliverer records it as one Digest row (no toast, no send). A real page right after still goes out.
/// The engine reads Lite's <c>App</c> alert statics through <see cref="AppAlertEngineSettings"/>, so this shares the
/// <c>app-alert-statics</c> collection.
/// </summary>
[Collection("app-alert-statics")]
public sealed class BlockingHeldRowLiteTests
{
    private static BlockedProcessAlertRow Row(int blockedSpid, long waitMs) => new()
    {
        EventTime = new DateTime(2026, 7, 1, 11, 54, 0, DateTimeKind.Utc).AddMilliseconds(waitMs),
        DatabaseName = "StackOverflow",
        BlockedSpid = blockedSpid,
        BlockingSpid = blockedSpid + 100,
        WaitTimeMs = waitMs,
        LockMode = "X",
        BlockedSqlText = "UPDATE Users SET x = 1",
        BlockingSqlText = "BEGIN TRAN UPDATE Users",
        ContentiousObject = "StackOverflow.dbo.Users",
        Source = BlockedProcessAlertRow.XeReportSource,
    };

    [Fact]
    public async Task FourteenEventsWorstWait9Seconds_RecordOneDigestRow_AndA90SecondEventPagesAtOnce()
    {
        var beforeEnabled = App.AlertsEnabled;
        var beforeBlocking = App.AlertBlockingEnabled;
        var beforeCount = App.AlertBlockingThreshold;
        var beforeWait = App.AlertBlockingWaitSecondsThreshold;
        try
        {
            App.AlertsEnabled = true;
            App.AlertBlockingEnabled = true;
            App.AlertBlockingThreshold = 3;
            App.AlertBlockingWaitSecondsThreshold = 60;

            var rows = new List<BlockedProcessAlertRow>();
            for (var spid = 60; spid < 74; spid++)
            {
                rows.Add(Row(spid, 9000));
            }

            var settings = new AlertWebhookCancelTests.GenericWebhookSettings("http://127.0.0.1:1/");
            var history = new AlertWebhookCancelTests.CapturingHistoryStore();
            var webhooks = new WebhookAlertService(
                settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
            var emailAlerts = new EmailAlertService(
                settings, history, webhooks, new AppLoggerAdapter<EmailAlertService>());
            var toasts = new List<string>();
            var deliverer = new LiteAlertDeliverer(
                (title, _, _, _, _) => toasts.Add(title),
                (metric, server, current, threshold, id, context, numCur, numThr, muted, detail, mode, token) =>
                    emailAlerts.TrySendAlertEmailAsync(
                        metric, server, current, threshold, id, context, numCur, numThr, muted, detail,
                        deliveryMode: mode, cancellationToken: token),
                _ => null,
                (metric, server, current, threshold, id, context, numCur, numThr, detail, hold, token) =>
                    emailAlerts.RecordHeldAlertAsync(
                        metric, server, current, threshold, id, context, numCur, numThr, detail, hold, token));

            var adapter = DispatchProxy.Create<IAlertReadAdapter, ReadAdapterProxy>();
            ((ReadAdapterProxy)(object)adapter).Blocking = rows;
            var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
            var engine = new AlertEngine(
                new AppAlertEngineSettings(), adapter, DispatchProxy.Create<IAlertStateStore, ReadAdapterProxy>(), deliverer,
                isAlertMuted: _ => false, utcNow: () => now);
            var snapshot = new AlertServerSnapshot("1", "SRV-A", true, null, null, false, false, now);
            var ct = TestContext.Current.CancellationToken;

            await engine.EvaluateServerAsync(snapshot, ct);

            var held = Assert.Single(history.Records, r => r.MetricName == "Blocking Detected");
            Assert.False(held.AlertSent);
            Assert.Equal(AlertDelivery.ChannelDigest, held.NotificationType);
            var routing = AlertContextSerializer.TryReadRouting(held.ContextJson);
            Assert.NotNull(routing);
            Assert.Equal("14 blocking events, worst wait 9 s, longest chain 1", routing!.Reason);
            Assert.Empty(toasts);

            /* Inside the cooldown the same pattern writes nothing more. */
            now = now.AddMinutes(2);
            await engine.EvaluateServerAsync(snapshot, ct);
            Assert.Single(history.Records, r => r.MetricName == "Blocking Detected");

            /* One event that has waited 90 s pages at once: the held row stamped no page cooldown. */
            rows.Add(Row(99, 90000));
            now = now.AddMinutes(1);
            await engine.EvaluateServerAsync(snapshot, ct);
            var page = history.Records[^1];
            Assert.Equal("Blocking Detected", page.MetricName);
            Assert.NotEqual(AlertDelivery.ChannelDigest, page.NotificationType);
            Assert.Equal(2, history.Records.FindAll(r => r.MetricName == "Blocking Detected").Count);
            Assert.Single(toasts);
        }
        finally
        {
            App.AlertsEnabled = beforeEnabled;
            App.AlertBlockingEnabled = beforeBlocking;
            App.AlertBlockingThreshold = beforeCount;
            App.AlertBlockingWaitSecondsThreshold = beforeWait;
        }
    }

    /// <summary>Answers every engine read with an empty result, except the blocked-process reports, which come from <see cref="Blocking"/>.</summary>
    public class ReadAdapterProxy : DispatchProxy
    {
        public List<BlockedProcessAlertRow> Blocking { get; set; } = new();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var returnType = targetMethod!.ReturnType;
            if (targetMethod.Name == nameof(IAlertReadAdapter.GetRecentBlockedProcessReportsAsync))
            {
                return Task.FromResult(new List<BlockedProcessAlertRow>(Blocking));
            }

            if (returnType == typeof(Task))
            {
                return Task.CompletedTask;
            }

            if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Task<>))
            {
                var inner = returnType.GetGenericArguments()[0];
                return typeof(Task).GetMethod(nameof(Task.FromResult))!.MakeGenericMethod(inner).Invoke(null, new[] { EmptyOf(inner) });
            }

            return returnType.IsValueType && returnType != typeof(void) ? Activator.CreateInstance(returnType) : null;
        }

        private static object? EmptyOf(Type type)
        {
            if (type.IsValueType)
            {
                return Activator.CreateInstance(type);
            }

            if (type.IsGenericType)
            {
                var definition = type.GetGenericTypeDefinition();
                var argument = type.GetGenericArguments()[0];
                if (definition == typeof(List<>))
                {
                    return Activator.CreateInstance(type);
                }

                if (definition == typeof(IReadOnlyList<>))
                {
                    return Array.CreateInstance(argument, 0);
                }
            }

            return type.GetConstructor(Type.EmptyTypes) is not null ? Activator.CreateInstance(type) : null;
        }
    }
}
