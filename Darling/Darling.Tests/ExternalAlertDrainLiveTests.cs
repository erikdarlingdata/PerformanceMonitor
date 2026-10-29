/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#pragma warning disable CS0618 // the fake deliverer states each send disposition directly; FromFanout needs a real fan-out

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The external alert drain against a real store with a fake deliverer that writes the history row the way the real one
/// does, after its send (#5654): a fire is sent once and leaves one history row, a fire and its clear arrive in order, a muted
/// fire is recorded as muted and sends nothing, a fire every channel refused is tried again after 1, 2 and 4 minutes and marked failed on the fourth attempt, an unregistered server lands in bucket 0,
/// a restart finds a claimed row delivered when the history has its row and interrupted when not (neither is sent again), and
/// the PagerDuty resolve is asked for only on a clear of a firing key.
/// </summary>
public sealed class ExternalAlertDrainLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AFire_IsSentOnce_AndLeavesOneHistoryRow()
    {
        await WithDrainAsync(async rig =>
        {
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01", value: 91.5);
            var pass = await rig.Drain.RunPassAsync(rig.Ct);
            var again = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Delivered);
            Assert.Equal(0, again.Claimed);
            var sent = Assert.Single(rig.Sent);
            Assert.Equal("External:nagios:disk", sent.MetricName);
            Assert.Equal(AlertFamily.External, AlertFamily.Of(sent.MetricName));
            Assert.Equal(id, sent.Context!.External!.PendingId);
            Assert.Equal("delivered", await rig.StatusAsync(id));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log WHERE metric_name = 'External:nagios:disk'"));
            Assert.Equal("nagios", AlertContextSerializer.TryReadExternalSource(await rig.ScalarAsync("SELECT context_json FROM config.config_alert_log LIMIT 1")));
        });
    }

    [Fact]
    public async Task AFireThenItsClear_ArriveInOrder_AndTheClearWritesAResolutionRow()
    {
        await WithDrainAsync(async rig =>
        {
            var fire = await rig.SubmitAsync("fire", "cpu", server: "SQL01");
            var clear = await rig.SubmitAsync("clear", "cpu", server: "SQL01");

            var first = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, first.Claimed);
            Assert.Equal("pending", await rig.StatusAsync(clear));
            var second = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, second.Claimed);

            Assert.Equal("delivered", await rig.StatusAsync(fire));
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Single(rig.Sent);
            Assert.Equal(new[] { "cpu" }, rig.PagerDutyResolves);
            Assert.Equal("External:nagios:cpu Resolved",
                await rig.ScalarAsync("SELECT metric_name FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log"));
            Assert.True(AlertMetricClassifier.IsResolution("External:nagios:cpu Resolved"));
            Assert.False(AlertMetricClassifier.IsResolution("External:nagios:backup_resolved"));
        });
    }

    /// <summary>#6003, D7: the clear's resolution row names the metric that fired in <c>Resolves</c>, which is what the
    /// investigation <c>resolved</c> detector matches, and it resolves a request linked to that metric on the same server.</summary>
    [Fact]
    public async Task AClearsResolutionRow_NamesTheFiredMetric_AndResolvesALinkedInvestigationRequest()
    {
        await WithDrainAsync(async rig =>
        {
            var at = DateTime.UtcNow.AddMinutes(-10);
            var requests = new InvestigationRequestStore(rig.DataSource, 30, () => at);
            var id = (await requests.OpenOrJoinAsync(new InvestigationAlert(1, 7, "SQL01", "resources", "External:nagios:cpu", at,
                Array.Empty<string>(), null, 50, 500, LeaseMinutes: 15, ExpiryMinutes: 240), rig.Ct)).RequestId!.Value;

            await rig.SubmitAsync("fire", "cpu", server: "SQL01");
            await rig.SubmitAsync("clear", "cpu", server: "SQL01");
            await rig.Drain.RunPassAsync(rig.Ct);
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("External:nagios:cpu", await rig.ScalarAsync(
                "SELECT context_json::jsonb ->> 'Resolves' FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
            at = DateTime.UtcNow;
            Assert.Equal(1, (await requests.SweepAsync(rig.Ct)).Resolved);
            Assert.Equal("resolved", await rig.ScalarAsync(
                $"SELECT event FROM config.investigation_journal WHERE request_id = '{id}' ORDER BY journal_id DESC LIMIT 1"));
        });
    }

    [Fact]
    public async Task OneKeyFiredForTwoUnregisteredNames_IsTwoIncidents_AndEachClearResolvesItsOwn()
    {
        await WithDrainAsync(async rig =>
        {
            var fireA = await rig.SubmitAsync("fire", "disk", server: "BOX-A");
            var fireB = await rig.SubmitAsync("fire", "disk", server: "BOX-B");
            var clearA = await rig.SubmitAsync("clear", "disk", server: "BOX-A");
            var clearB = await rig.SubmitAsync("clear", "disk", server: "BOX-B");

            for (var pass = 0; pass < 4; pass++)
            {
                await rig.Drain.RunPassAsync(rig.Ct);
            }

            Assert.Equal("delivered", await rig.StatusAsync(fireA));
            Assert.Equal("delivered", await rig.StatusAsync(fireB));
            Assert.Equal("delivered", await rig.StatusAsync(clearA));
            Assert.Equal("delivered", await rig.StatusAsync(clearB));
            Assert.Equal(new[] { "BOX-A|external:BOX-A", "BOX-B|external:BOX-B" }, rig.PagerDutyIdentities.OrderBy(i => i, StringComparer.Ordinal).ToArray());
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
            Assert.Equal("BOX-A,BOX-B", await rig.ScalarAsync("SELECT string_agg(server_name, ',' ORDER BY server_name) FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM config.external_alert_state WHERE firing"));
        });
    }

    [Fact]
    public async Task AClearForOneNameWhenOnlyAnotherFired_IsIgnored_AndLeavesTheOtherFiring()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "disk", server: "BOX-B");
            await rig.Drain.RunPassAsync(rig.Ct);
            var clearA = await rig.SubmitAsync("clear", "disk", server: "BOX-A");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("ignored", await rig.StatusAsync(clearA));
            Assert.Empty(rig.PagerDutyResolves);
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM config.external_alert_state WHERE firing"));
        });
    }

    [Fact]
    public async Task AServerRegisteredBetweenAFireAndItsClear_StillResolvesTheIncidentTheFireOpened()
    {
        await WithDrainAsync(async rig =>
        {
            var fire = await rig.SubmitAsync("fire", "cpu", server: "NEWBOX");
            await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal("NEWBOX|external:NEWBOX", rig.Sent.Select(o => o.ServerName + "|" + o.ServerKey).Single());

            rig.Registry["NEWBOX"] = new ExternalAlertServer(9, "NEWBOX");
            var clear = await rig.SubmitAsync("clear", "cpu", server: "NEWBOX");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("delivered", await rig.StatusAsync(fire));
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new[] { "NEWBOX|external:NEWBOX" }, rig.PagerDutyIdentities.ToArray());
            Assert.Equal("0", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
        });
    }

    [Fact]
    public async Task AServerRegisteredBetweenTwoFiresOfOneEpisode_RetriggersTheSameIncident_AndTheClearResolvesIt()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "cpu", server: "NEWBOX");
            await rig.Drain.RunPassAsync(rig.Ct);

            rig.Registry["NEWBOX"] = new ExternalAlertServer(9, "NEWBOX");
            await rig.SubmitAsync("fire", "cpu", server: "NEWBOX");
            await rig.Drain.RunPassAsync(rig.Ct);

            /* Both triggers carry the first fire's identity, and the state still holds it. */
            Assert.Equal(new[] { "NEWBOX|external:NEWBOX", "NEWBOX|external:NEWBOX" }, rig.Sent.Select(o => o.ServerName + "|" + o.ServerKey).ToArray());
            Assert.Equal("external:NEWBOX", await rig.ScalarAsync("SELECT server_key FROM config.external_alert_state"));
            Assert.Equal("0", await rig.ScalarAsync("SELECT server_id::text FROM config.external_alert_state"));

            var clear = await rig.SubmitAsync("clear", "cpu", server: "NEWBOX");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new[] { "NEWBOX|external:NEWBOX" }, rig.PagerDutyIdentities.ToArray());
        });
    }

    [Fact]
    public async Task AServerRenamedOrRemovedBetweenAFireAndItsClear_StillResolvesTheIncidentTheFireOpened()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "cpu", server: "sql01");
            await rig.Drain.RunPassAsync(rig.Ct);

            rig.Registry.Clear();
            var clear = await rig.SubmitAsync("clear", "cpu", server: "SQL01");
            await rig.Drain.RunPassAsync(rig.Ct);

            /* The clear differs from the fire in case and in what the registry says; it resolves under the fire's key and name. */
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new[] { "SQL01|7" }, rig.PagerDutyIdentities.ToArray());
            Assert.Equal("7", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
        });
    }

    /// <summary>#6928: the PagerDuty close is routed at the severity the fire went out at, so a severity filter on a route sends the close
    /// to the route the fire used. A critical fire closes as Critical and a warning fire as Warning.</summary>
    [Theory]
    [InlineData("critical", AlertSeverityLevel.Critical)]
    [InlineData("warning", AlertSeverityLevel.Warning)]
    public async Task TheClear_AsksForThePagerDutyCloseAtTheSeverityTheFireWentOutAt(string fireSeverity, AlertSeverityLevel expected)
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "disk", server: "SQL01", severity: fireSeverity);
            await rig.Drain.RunPassAsync(rig.Ct);
            var clear = await rig.SubmitAsync("clear", "disk", server: "SQL01");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new AlertSeverityLevel?[] { expected }, rig.PagerDutySeverities.ToArray());
        });
    }

    [Fact]
    public async Task AClearWithNoFire_IsIgnored_AndAsksForNoResolve()
    {
        await WithDrainAsync(async rig =>
        {
            var clear = await rig.SubmitAsync("clear", "never-fired");
            await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal("ignored", await rig.StatusAsync(clear));
            Assert.Empty(rig.PagerDutyResolves);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log"));
        });
    }

    [Fact]
    public async Task AMutedFire_IsRecordedAsMuted_AndSendsNothing()
    {
        await WithDrainAsync(async rig =>
        {
            rig.Muted = true;
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");
            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Muted);
            Assert.Equal("muted", await rig.StatusAsync(id));
            Assert.DoesNotContain(rig.Sent, s => !s.Muted);
            Assert.Equal("true", (await rig.ScalarAsync("SELECT muted::text FROM config.config_alert_log LIMIT 1")));
        });
    }

    [Fact]
    public async Task AMuteRuleNamingOneSource_SilencesThatSourcesFire_AndNotAnotherSources()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.AddSourceAsync("zabbix");
            rig.Rules.Add(new MuteRule { ExternalSource = "nagios" });
            var fromNagios = await rig.SubmitAsync("fire", "disk", server: "SQL01");
            var fromZabbix = await rig.SubmitAsync("fire", "disk", server: "SQL01", source: "zabbix");
            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            /* Same key, same server, same text: the source is the only thing that tells the two apart. */
            Assert.Equal(1, pass.Muted);
            Assert.Equal(1, pass.Delivered);
            Assert.Equal("muted", await rig.StatusAsync(fromNagios));
            Assert.Equal("delivered", await rig.StatusAsync(fromZabbix));
            Assert.Equal(2, rig.Sent.Count);
            Assert.True(rig.Sent.Single(s => s.MetricName.StartsWith("External:nagios:", StringComparison.Ordinal)).Muted);
            Assert.False(rig.Sent.Single(s => s.MetricName.StartsWith("External:zabbix:", StringComparison.Ordinal)).Muted);
        });
    }

    [Fact]
    public async Task AMuteRuleNamingOnlyAServer_StillSilencesAnExternalFireForThatServer()
    {
        await WithDrainAsync(async rig =>
        {
            /* The kept server identity is SQL01's store id, so a rule keyed on that id matches with no source named. */
            rig.Rules.Add(new MuteRule { ServerId = 7 });
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");
            var other = await rig.SubmitAsync("fire", "disk2", server: "SQL02");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("muted", await rig.StatusAsync(id));
            Assert.Equal("delivered", await rig.StatusAsync(other));
        });
    }

    [Fact]
    public async Task AFireEveryChannelFailed_GoesBackToTheQueue_WaitsOneTwoFourMinutes_AndIsMarkedFailedOnTheFourthAttempt()
    {
        /* #5654: delivery is at least once. A webhook that timed out after its request was sent reads the same as a refused
           connection, so a repeat can post twice, and a brief channel outage must not lose the alert. */
        await WithDrainAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, "failed", "boom");
            var id = await rig.SubmitAsync("fire", "disk");
            var expectedWaitSeconds = new[] { 60, 120, 240 };

            for (var attempt = 1; attempt <= ExternalAlertDrain.MaxSendAttempts; attempt++)
            {
                var pass = await rig.Drain.RunPassAsync(rig.Ct);
                Assert.Equal(1, pass.Claimed);
                Assert.Equal(attempt, rig.Sent.Count);
                if (attempt < ExternalAlertDrain.MaxSendAttempts)
                {
                    Assert.Equal(1, pass.Retried);
                    Assert.Equal(0, pass.Failed);
                    Assert.Equal("pending", await rig.StatusAsync(id));
                    Assert.Equal(attempt.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        await rig.ScalarAsync($"SELECT attempts::text FROM config.external_alert_pending WHERE pending_id = {id}"));
                    var waitSeconds = double.Parse(
                        (await rig.ScalarAsync($"SELECT extract(epoch FROM next_attempt_at - now())::text FROM config.external_alert_pending WHERE pending_id = {id}"))!,
                        System.Globalization.CultureInfo.InvariantCulture);
                    Assert.InRange(waitSeconds, expectedWaitSeconds[attempt - 1] - 15, expectedWaitSeconds[attempt - 1] + 1);
                    /* Not due yet: the next pass takes nothing until the wait is over. */
                    Assert.Equal(0, (await rig.Drain.RunPassAsync(rig.Ct)).Claimed);
                    await rig.ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {id}");
                }
                else
                {
                    Assert.Equal(1, pass.Failed);
                    Assert.Equal(0, pass.Retried);
                }
            }

            Assert.Equal("failed", await rig.StatusAsync(id));
            Assert.Equal(ExternalAlertDrain.MaxSendAttempts, rig.Sent.Count);
            Assert.Equal(0, (await rig.Drain.RunPassAsync(rig.Ct)).Claimed);
        });
    }

    [Fact]
    public async Task AnUnregisteredServer_LandsInBucketZero_AndARegisteredOneInItsOwnId()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "a", server: "NOT-MONITORED");
            await rig.SubmitAsync("fire", "b", server: "sql01");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("0", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name = 'External:nagios:a'"));
            Assert.Equal("NOT-MONITORED", await rig.ScalarAsync("SELECT server_name FROM config.config_alert_log WHERE metric_name = 'External:nagios:a'"));
            Assert.Equal("7", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name = 'External:nagios:b'"));
            Assert.Equal("SQL01", await rig.ScalarAsync("SELECT server_name FROM config.config_alert_log WHERE metric_name = 'External:nagios:b'"));
        });
    }

    [Fact]
    public async Task ARestartMidDelivery_FindsTheRowDeliveredWithAHistoryRow_AndInterruptedWithout_AndSendsNeither()
    {
        await WithDrainAsync(async rig =>
        {
            var logged = await rig.SubmitAsync("fire", "logged");
            var lost = await rig.SubmitAsync("fire", "lost");
            /* A dead run claimed both; only the first got as far as its history row. */
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET status = 'claimed', claimed_at = now() - interval '10 minutes' WHERE pending_id IN ({logged}, {lost})");
            var loggedContext = AlertContextSerializer.Serialize(new AlertContext { External = new AlertExternalOrigin("nagios", "logged", logged) });
            await rig.History.RecordAlertAsync(new AlertHistoryRecord(
                "7", "SQL01", "External:nagios:logged", "1", "", 1, null,
                AlertDelivery.FromLegacyStoredColumns(true, "webhook", null), false, "x", loggedContext));

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.ReapedDelivered);
            Assert.Equal(1, pass.ReapedInterrupted);
            Assert.Equal("delivered", await rig.StatusAsync(logged));
            Assert.Equal("interrupted", await rig.StatusAsync(lost));
            Assert.Empty(rig.Sent);
        });
    }

    [Fact]
    public async Task AReapedRow_IsFoundByItsOwnPendingId_AndNotByAHistoryRowForALongerId()
    {
        await WithDrainAsync(async rig =>
        {
            /* Ids are sequential, so burn ones until a 1-digit id and a longer id that starts with it both exist. */
            var ids = new List<long>();
            for (var i = 0; i < 12; i++)
            {
                ids.Add(await rig.SubmitAsync("fire", "k" + i.ToString(CultureInfo.InvariantCulture)));
            }

            var short1 = ids[0];
            var longer = ids.First(id => id.ToString(CultureInfo.InvariantCulture).StartsWith(short1.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal) && id != short1);
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET status = 'claimed', claimed_at = now() - interval '10 minutes' WHERE pending_id IN ({short1})");

            /* Only the longer id has a history row, so the short id's row must not be called delivered by it. */
            var context = AlertContextSerializer.Serialize(new AlertContext { External = new AlertExternalOrigin("nagios", "k", longer) });
            await rig.History.RecordAlertAsync(new AlertHistoryRecord(
                "7", "SQL01", "External:nagios:k", "1", "", 1, null,
                AlertDelivery.FromLegacyStoredColumns(true, "webhook", null), false, "x", context));

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(0, pass.ReapedDelivered);
            Assert.Equal(1, pass.ReapedInterrupted);
            Assert.Equal("interrupted", await rig.StatusAsync(short1));
        });
    }

    [Fact]
    public async Task TheReap_MatchesHistoryRowsByTheCallersPattern_SoTheSerializerHelperIsTheOnlyDefinition()
    {
        await WithDrainAsync(async rig =>
        {
            var id = await rig.SubmitAsync("fire", "k");
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET status = 'claimed', claimed_at = now() - interval '10 minutes' WHERE pending_id = {id}");
            var context = AlertContextSerializer.Serialize(new AlertContext { External = new AlertExternalOrigin("nagios", "k", id) });
            await rig.History.RecordAlertAsync(new AlertHistoryRecord(
                "7", "SQL01", "External:nagios:k", "1", "", 1, null,
                AlertDelivery.FromLegacyStoredColumns(true, "webhook", null), false, "x", context));

            /* A pattern that matches nothing finds no history row, so the row is interrupted: the SQL carries no pattern of its own. */
            Assert.Equal((0, 1), await rig.Store.ReapWithHistoryAsync(TimeSpan.Zero, _ => "%no such history row%", rig.Ct));
            Assert.Equal("interrupted", await rig.StatusAsync(id));

            await rig.ExecAsync($"UPDATE config.external_alert_pending SET status = 'claimed', completed_at = NULL WHERE pending_id = {id}");
            Assert.Equal((1, 0), await rig.Store.ReapWithHistoryAsync(TimeSpan.Zero, AlertContextSerializer.BuildPendingIdLikePattern, rig.Ct));
            Assert.Equal("delivered", await rig.StatusAsync(id));
        });
    }

    [Fact]
    public async Task AFireWithAValue_IsSentMeasured_AndAFireWithoutOne_IsSentAsAState()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "disk", value: 91.5);
            await rig.SubmitAsync("fire", "svc");
            await rig.Drain.RunPassAsync(rig.Ct);

            var valued = Assert.Single(rig.Sent, o => o.MetricName == "External:nagios:disk");
            var valueless = Assert.Single(rig.Sent, o => o.MetricName == "External:nagios:svc");
            Assert.Equal(91.5, valued.NumericCurrentValue);
            Assert.Equal("91.5", valued.CurrentValue);
            Assert.Equal(0.0, valueless.NumericCurrentValue);
            Assert.Equal("warning", valueless.CurrentValue);

            /* What the history grids make of the stored numbers: the measurement as a number, the state as a dash. */
            Assert.Equal("91.50", AlertMetricClassifier.FormatHistoryValue(valued.MetricName, valued.NumericCurrentValue!.Value));
            Assert.Equal(AlertMetricClassifier.StateOnlyDisplay, AlertMetricClassifier.FormatHistoryValue(valueless.MetricName, valueless.NumericCurrentValue!.Value));
        });
    }

    [Fact]
    public async Task AFireThatGaveUpBeforeAnySendWentOut_LeavesNothingFiring_SoItsClearResolvesNothing()
    {
        await WithDrainAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, "failed", "boom");
            var fire = await rig.SubmitAsync("fire", "disk", server: "BOX-A");
            await rig.GiveUpAsync(fire);
            Assert.Equal("failed", await rig.StatusAsync(fire));
            Assert.Equal("false", await rig.ScalarAsync("SELECT firing::text FROM config.external_alert_state WHERE alert_key = 'disk'"));

            var clear = await rig.SubmitAsync("clear", "disk", server: "BOX-A");
            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Ignored);
            Assert.Equal("ignored", await rig.StatusAsync(clear));
            Assert.Empty(rig.PagerDutyResolves);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
        });
    }

    [Fact]
    public async Task ARepeatFireThatGaveUp_LeavesTheIncidentAnEarlierDeliveredFireOpened_SoItsClearStillResolvesIt()
    {
        await WithDrainAsync(async rig =>
        {
            var first = await rig.SubmitAsync("fire", "disk", server: "BOX-A");
            await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal("delivered", await rig.StatusAsync(first));

            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, "failed", "boom");
            var repeat = await rig.SubmitAsync("fire", "disk", server: "BOX-A");
            await rig.GiveUpAsync(repeat);
            Assert.Equal("failed", await rig.StatusAsync(repeat));
            Assert.Equal("true", await rig.ScalarAsync("SELECT firing::text FROM config.external_alert_state WHERE alert_key = 'disk'"));

            var clear = await rig.SubmitAsync("clear", "disk", server: "BOX-A");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new[] { "disk" }, rig.PagerDutyResolves);
        });
    }

    [Fact]
    public async Task AFireOpenedByARetryThatSucceeds_IsConfirmed_AndItsClearResolvesIt()
    {
        await WithDrainAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, "failed", "boom");
            var fire = await rig.SubmitAsync("fire", "disk", server: "BOX-A");
            Assert.Equal(1, (await rig.Drain.RunPassAsync(rig.Ct)).Retried);
            Assert.Equal("true", await rig.ScalarAsync("SELECT firing::text FROM config.external_alert_state WHERE alert_key = 'disk'"));

            rig.Answer = AlertDelivery.FromLegacyStoredColumns(true, "webhook", null);
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {fire}");
            await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal("delivered", await rig.StatusAsync(fire));
            Assert.Equal("warning", await rig.ScalarAsync("SELECT fired_severity FROM config.external_alert_state WHERE alert_key = 'disk'"));

            var clear = await rig.SubmitAsync("clear", "disk", server: "BOX-A");
            await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new[] { "disk" }, rig.PagerDutyResolves);
        });
    }

    [Fact]
    public async Task ADisabledMasterSwitch_ClaimsNothing_ButExpiryStillRuns()
    {
        await WithDrainAsync(async rig =>
        {
            var id = await rig.SubmitAsync("fire", "disk");
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET expires_at = now() - interval '1 minute' WHERE pending_id = {id}");
            rig.Enabled = false;
            var pass = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, pass.Expired);
            Assert.Equal(0, pass.Claimed);
            Assert.Equal("expired", await rig.StatusAsync(id));
        });
    }

    private static async Task WithDrainAsync(Func<Rig, Task> body)
    {
        /* #5755: the live classes skip when DARLING_TEST_PG is unset (CI's whole-tree guard job has no store). The skip comes before
           ScratchPostgres.CreateAsync, so neither the create nor its teardown ever sees a null connection string. */
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert drain's live facts (each mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var seed = new NpgsqlConnection(scratch.ConnectionString);
        await seed.OpenAsync(ct);
        await PgMigrations.MigrateAsync(seed, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var rig = new Rig(seed, dataSource, ct);
        await rig.InitAsync();
        var bodySucceeded = false;
        try
        {
            await body(rig);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private sealed class Rig
    {
        private readonly NpgsqlConnection _seed;
        private readonly Dictionary<string, int> _sourceIds = new(StringComparer.Ordinal);

        public Rig(NpgsqlConnection seed, NpgsqlDataSource dataSource, CancellationToken ct)
        {
            _seed = seed;
            Ct = ct;
            DataSource = dataSource;
            History = new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            Store = new ExternalAlertStore(dataSource, 30);
            Drain = new ExternalAlertDrain(
                Store,
                () => Enabled,
                async (outcome, token) =>
                {
                    Sent.Add(outcome);
                    var delivery = outcome.Muted ? AlertDelivery.FromLegacyStoredColumns(false, AlertDelivery.ChannelMuted, null) : Answer;
                    await History.RecordAlertAsync(new AlertHistoryRecord(
                        outcome.ServerKey, outcome.ServerName, outcome.MetricName, outcome.CurrentValue, outcome.ThresholdValue,
                        outcome.NumericCurrentValue, outcome.NumericThresholdValue, delivery, outcome.Muted, outcome.DetailText,
                        AlertContextSerializer.Serialize(outcome.Context!)));
                    return delivery;
                },
                (metric, serverName, serverKey, _, firedSeverity, _) =>
                {
                    PagerDutyResolves.Add(metric.Split(':')[2]);
                    PagerDutySeverities.Add(firedSeverity);
                    PagerDutyIdentities.Add(serverName + "|" + serverKey);
                    return Task.FromResult(AlertChannelOutcome.Delivered);
                },
                History,
                context => Muted || Rules.Any(rule => rule.Matches(context)),
                name => Registry.TryGetValue(name, out var server) ? server : null,
                "test:1",
                NullLogger.Instance);
        }

        public CancellationToken Ct { get; }
        public NpgsqlDataSource DataSource { get; }
        public PgAlertHistoryStore History { get; }
        public ExternalAlertStore Store { get; }
        public ExternalAlertDrain Drain { get; }
        public List<AlertOutcome> Sent { get; } = new();
        public List<string> PagerDutyResolves { get; } = new();

        /// <summary>The severity each PagerDuty resolve was asked to route at (#6928): the tier the fire went out at.</summary>
        public List<AlertSeverityLevel?> PagerDutySeverities { get; } = new();

        /// <summary>The server name and server key each PagerDuty resolve was asked to close, as <c>name|key</c>.</summary>
        public List<string> PagerDutyIdentities { get; } = new();

        /// <summary>The registered servers, by the name the drain looks up (case-insensitive); a test edits it between a fire and its clear.</summary>
        public Dictionary<string, ExternalAlertServer> Registry { get; } =
            new(StringComparer.OrdinalIgnoreCase) { ["SQL01"] = new ExternalAlertServer(7, "SQL01") };
        public bool Muted { get; set; }

        /// <summary>Mute rules the test's mute check applies, through <see cref="MuteRule.Matches"/> as the service's does.</summary>
        public List<MuteRule> Rules { get; } = new();
        public bool Enabled { get; set; } = true;
        public AlertDelivery Answer { get; set; } = AlertDelivery.FromLegacyStoredColumns(true, "webhook", null);

        public Task InitAsync() => AddSourceAsync("nagios");

        public async Task AddSourceAsync(string name) =>
            _sourceIds[name] = Convert.ToInt32(
                await ScalarAsync($"INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('{name}', 'role_{name}') RETURNING source_id"),
                CultureInfo.InvariantCulture);

        public async Task<long> SubmitAsync(string kind, string key, string? server = null, double? value = null, string source = "nagios", string severity = "warning")
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, title, server_name, value, expires_at)"
                + " VALUES ($1, $2, $3, $6, 'a title', $4, $5, now() + interval '15 minutes') RETURNING pending_id", _seed);
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = _sourceIds[source] });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = kind });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key });
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)server ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)value ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = severity });
            return Convert.ToInt64(await command.ExecuteScalarAsync(Ct), CultureInfo.InvariantCulture);
        }

        /// <summary>Runs passes, making the row due again after each, until the drain has given up on it.</summary>
        public async Task GiveUpAsync(long id)
        {
            for (var attempt = 1; attempt <= ExternalAlertDrain.MaxSendAttempts; attempt++)
            {
                await Drain.RunPassAsync(Ct);
                await ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {id} AND status = 'pending'");
            }
        }

        public Task<string?> StatusAsync(long id) => ScalarAsync($"SELECT status FROM config.external_alert_pending WHERE pending_id = {id}");

        public async Task<string?> ScalarAsync(string sql)
        {
            await using var command = new NpgsqlCommand(sql, _seed);
            var value = await command.ExecuteScalarAsync(Ct);
            return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        public async Task ExecAsync(string sql)
        {
            await using var command = new NpgsqlCommand(sql, _seed);
            await command.ExecuteNonQueryAsync(Ct);
        }
    }
}
