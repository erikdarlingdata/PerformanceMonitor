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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
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
/// The external alert drain's lifecycle against a real store (#5654): a state write that waits on a lock held by a submit
/// left open gives up after two seconds and the row goes back for a retry (it is not dropped as interrupted); a pass that is
/// cancelled midway gives the rows it never reached back to the queue; a row stuck claimed with no owner is reaped by a
/// later pass of the same run; a pass restamps its claims, so a live pass's own rows are never reaped; a row the reap took
/// is not sent; and a fire every channel failed to send is tried again (delivery is at least once), with the same PagerDuty dedup key.
/// </summary>
public sealed class ExternalAlertDrainLifecycleLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AStateWriteBlockedByASubmitHoldingTheSourceRow_GivesUpAfterTwoSeconds_AndTheRowIsRetried()
    {
        await WithRigAsync(async rig =>
        {
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");

            /* A submit left open in a transaction: it holds the source row FOR UPDATE, and the state insert's foreign key
               check needs a share lock on the same row. */
            await using var holder = new NpgsqlConnection(rig.ConnectionString);
            await holder.OpenAsync(rig.Ct);
            await using (var begin = new NpgsqlCommand(
                $"BEGIN; SELECT 1 FROM config.external_alert_source WHERE source_id = {rig.SourceId} FOR UPDATE", holder))
            {
                await begin.ExecuteNonQueryAsync(rig.Ct);
            }

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            /* The store command deadline in this rig is 30 s: a retry (not an interrupted row) is the two-second lock timeout. */
            Assert.Equal(1, pass.Claimed);
            Assert.Equal(1, pass.Retried);
            Assert.Equal(0, pass.Interrupted);
            Assert.Empty(rig.Sent);
            Assert.Equal("pending", await rig.StatusAsync(id));
            Assert.Equal("1", await rig.ScalarAsync($"SELECT attempts::text FROM config.external_alert_pending WHERE pending_id = {id}"));
            Assert.Null(await rig.ScalarAsync($"SELECT claimed_at::text FROM config.external_alert_pending WHERE pending_id = {id}"));

            await using (var rollback = new NpgsqlCommand("ROLLBACK", holder))
            {
                await rollback.ExecuteNonQueryAsync(rig.Ct);
            }

            await rig.ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {id}");
            var later = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, later.Delivered);
            Assert.Equal("delivered", await rig.StatusAsync(id));
            Assert.Single(rig.Sent);
        });
    }

    [Fact]
    public async Task AStateWriteThatKeepsWaitingOnALock_IsRetriedThreeTimes_ThenMarkedFailed()
    {
        await WithRigAsync(async rig =>
        {
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");
            await using var holder = new NpgsqlConnection(rig.ConnectionString);
            await holder.OpenAsync(rig.Ct);
            await using (var begin = new NpgsqlCommand(
                $"BEGIN; SELECT 1 FROM config.external_alert_source WHERE source_id = {rig.SourceId} FOR UPDATE", holder))
            {
                await begin.ExecuteNonQueryAsync(rig.Ct);
            }

            for (var attempt = 1; attempt <= ExternalAlertDrain.MaxSendAttempts; attempt++)
            {
                var pass = await rig.Drain.RunPassAsync(rig.Ct);
                Assert.Equal(1, pass.Claimed);
                if (attempt < ExternalAlertDrain.MaxSendAttempts)
                {
                    Assert.Equal(1, pass.Retried);
                    Assert.Equal("pending", await rig.StatusAsync(id));
                    /* Not due yet: the next pass takes nothing until the wait is over. */
                    Assert.Equal(0, (await rig.Drain.RunPassAsync(rig.Ct)).Claimed);
                    await rig.ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {id}");
                }
                else
                {
                    Assert.Equal(1, pass.Failed);
                }
            }

            Assert.Equal("failed", await rig.StatusAsync(id));
            Assert.Empty(rig.Sent);
        });
    }

    [Fact]
    public async Task APassCancelledMidBatch_GivesTheRowsItNeverReachedBackToPending_AndALaterPassSendsEachOnce()
    {
        await WithRigAsync(async rig =>
        {
            var first = await rig.SubmitAsync("fire", "a", server: "SQL01");
            var second = await rig.SubmitAsync("fire", "b", server: "SQL01");
            var third = await rig.SubmitAsync("fire", "c", server: "SQL01");

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(rig.Ct);
            rig.AfterSend = _ =>
            {
                /* The service is asked to stop while the first row's send is finishing. */
                stop.Cancel();
                return Task.CompletedTask;
            };

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rig.Drain.RunPassAsync(stop.Token));

            Assert.Equal("delivered", await rig.StatusAsync(first));
            foreach (var id in new[] { second, third })
            {
                Assert.Equal("pending", await rig.StatusAsync(id));
                Assert.Equal("0", await rig.ScalarAsync($"SELECT attempts::text FROM config.external_alert_pending WHERE pending_id = {id}"));
                Assert.Null(await rig.ScalarAsync($"SELECT claimed_at::text FROM config.external_alert_pending WHERE pending_id = {id}"));
                Assert.Null(await rig.ScalarAsync($"SELECT service_instance FROM config.external_alert_pending WHERE pending_id = {id}"));
            }

            rig.AfterSend = null;
            var next = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(2, next.Claimed);
            Assert.Equal(2, next.Delivered);
            Assert.Equal(3, rig.Sent.Count);
            Assert.Equal(3, rig.Sent.Select(s => s.MetricName).Distinct().Count());
        });
    }

    [Fact]
    public async Task ARowStuckClaimedWithNoOwner_IsReapedByALaterPassOfTheSameRun_OnceItsClaimIsOldEnough()
    {
        await WithRigAsync(async rig =>
        {
            var stuck = await rig.SubmitAsync("fire", "disk", server: "SQL01");
            var clear = await rig.SubmitAsync("clear", "disk", server: "SQL01");

            /* A claim a failed mark left behind in a live run. It blocks every later row of its key. */
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET status = 'claimed', claimed_at = now() - interval '1 minute', service_instance = 'gone:1' WHERE pending_id = {stuck}");
            var young = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(0, young.ReapedDelivered + young.ReapedInterrupted);
            Assert.Equal(0, young.Claimed);
            Assert.Equal("claimed", await rig.StatusAsync(stuck));
            Assert.Equal("pending", await rig.StatusAsync(clear));

            await rig.ExecAsync($"UPDATE config.external_alert_pending SET claimed_at = now() - interval '6 minutes' WHERE pending_id = {stuck}");
            var old = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, old.ReapedDelivered + old.ReapedInterrupted);
            Assert.Equal("interrupted", await rig.StatusAsync(stuck));
            Assert.Equal("ignored", await rig.StatusAsync(clear));
            Assert.Equal(1, old.Claimed);
            Assert.Empty(rig.Sent);
        });
    }

    [Fact]
    public async Task ALongPass_RestampsItsClaims_SoAnotherSessionsReapNeverTakesItsRowsInFlight()
    {
        await WithRigAsync(async rig =>
        {
            var a = await rig.SubmitAsync("fire", "a", server: "SQL01");
            var b = await rig.SubmitAsync("fire", "b", server: "SQL01");
            var c = await rig.SubmitAsync("fire", "c", server: "SQL01");

            var reapedDuringSecondSend = (-1, -1);
            var sends = 0;
            rig.BeforeSend = async _ =>
            {
                sends++;
                if (sends == 1)
                {
                    /* The first send is slow, so the other two claims are older than the reap age by the time they are reached. */
                    await Task.Delay(TimeSpan.FromSeconds(4), rig.Ct);
                }
                else if (sends == 2)
                {
                    /* Another instance's reap, with an age the claim time alone would already exceed. */
                    reapedDuringSecondSend = await rig.Store.ReapWithHistoryAsync(TimeSpan.FromSeconds(3), AlertContextSerializer.BuildPendingIdLikePattern, rig.Ct);
                }
            };

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal((0, 0), reapedDuringSecondSend);
            Assert.Equal(3, pass.Delivered);
            foreach (var id in new[] { a, b, c })
            {
                Assert.Equal("delivered", await rig.StatusAsync(id));
            }
        });
    }

    [Fact]
    public async Task ARowTheReapTookDuringThePass_IsNotSent()
    {
        await WithRigAsync(async rig =>
        {
            var first = await rig.SubmitAsync("fire", "a", server: "SQL01");
            var second = await rig.SubmitAsync("fire", "b", server: "SQL01");
            rig.BeforeSend = async _ =>
            {
                /* Another instance closes the second row while the first is being sent. */
                await rig.ExecAsync($"UPDATE config.external_alert_pending SET status = 'interrupted', completed_at = now() WHERE pending_id = {second}");
                rig.BeforeSend = null;
            };

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Delivered);
            Assert.Equal("delivered", await rig.StatusAsync(first));
            Assert.Equal("interrupted", await rig.StatusAsync(second));
            Assert.Single(rig.Sent);
        });
    }

    [Fact]
    public async Task AFailedPagerDutyResolve_GoesBackToTheQueue_AndTheClearIsHandledOnce()
    {
        /* A repeat resolve leaves the incident as it is. */
        await WithRigAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "disk", server: "SQL01");
            await rig.Drain.RunPassAsync(rig.Ct);
            var clear = await rig.SubmitAsync("clear", "disk", server: "SQL01");

            rig.PagerDutyAnswer = AlertChannelOutcome.Failed;
            var failedPass = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, failedPass.Retried);
            Assert.Equal("pending", await rig.StatusAsync(clear));

            rig.PagerDutyAnswer = AlertChannelOutcome.Delivered;
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {clear}");
            var retry = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, retry.Delivered);
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
        });
    }

    [Theory]
    [InlineData("failed", "the webhook post timed out after 10 seconds")]
    [InlineData("email", "the SMTP session was reset")]
    public async Task AFireThatFailedOnEveryChannel_GoesBackToTheQueue_WhateverTheChannel_AndIsSentAgainWhenDue(string channel, string error)
    {
        /* Delivery is at least once: a timeout after the request left reads the same as a refused connection. */
        await WithRigAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, channel, error);
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(0, pass.Failed);
            Assert.Equal(1, pass.Retried);
            Assert.Equal("pending", await rig.StatusAsync(id));
            Assert.Equal("1", await rig.ScalarAsync($"SELECT attempts::text FROM config.external_alert_pending WHERE pending_id = {id}"));
            Assert.Contains("attempt 1 of 4", await rig.ScalarAsync($"SELECT reason FROM config.external_alert_pending WHERE pending_id = {id}"), StringComparison.Ordinal);
            Assert.Equal(0, (await rig.Drain.RunPassAsync(rig.Ct)).Claimed);

            rig.Answer = AlertDelivery.FromLegacyStoredColumns(true, "webhook", null);
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {id}");
            var retry = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, retry.Delivered);
            Assert.Equal("delivered", await rig.StatusAsync(id));
            Assert.Equal(2, rig.Sent.Count);
        });
    }

    [Fact]
    public async Task ARetriedFire_CarriesTheSamePagerDutyDedupKeyOnEveryAttempt()
    {
        /* PagerDuty drops a repeat trigger that carries the dedup key of an open incident, so a fire sent again after a
           timeout does not open a second one. The key comes from the server key and the metric name alone. */
        await WithRigAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, "failed", "the webhook post timed out after 10 seconds");
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");

            for (var attempt = 1; attempt <= 3; attempt++)
            {
                Assert.Equal(1, (await rig.Drain.RunPassAsync(rig.Ct)).Retried);
                await rig.ExecAsync($"UPDATE config.external_alert_pending SET next_attempt_at = now() - interval '1 second' WHERE pending_id = {id}");
            }

            rig.Answer = AlertDelivery.FromLegacyStoredColumns(true, "webhook", null);
            Assert.Equal(1, (await rig.Drain.RunPassAsync(rig.Ct)).Delivered);

            Assert.Equal(4, rig.Sent.Count);
            var keys = rig.Sent
                .Select(o => WebhookAlertService.DerivePagerDutyDedupKey(o.ServerKey, o.MetricName, o.Context, autoResolve: true))
                .Distinct()
                .ToList();
            Assert.Single(keys);
            Assert.Equal(rig.Sent[0].ServerKey + ":" + rig.Sent[0].MetricName, keys[0]);
            var plain = rig.Sent
                .Select(o => WebhookAlertService.DerivePagerDutyDedupKey(o.ServerKey, o.MetricName, o.Context))
                .Distinct()
                .ToList();
            Assert.Single(plain);
        });
    }

    [Fact]
    public async Task AStopRightAfterAFireFailedOnEveryChannel_KeepsTheRetry()
    {
        /* The release that records the retry runs with no token: the stop that cancels the pass must not leave the row claimed. */
        await WithRigAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, "webhook", "refused");
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(rig.Ct);
            rig.AfterSend = _ =>
            {
                stop.Cancel();
                return Task.CompletedTask;
            };

            try
            {
                await rig.Drain.RunPassAsync(stop.Token);
            }
            catch (OperationCanceledException)
            {
                /* A stop may end the pass with the exception or let it finish the row; the row is the same either way. */
            }

            Assert.Equal("pending", await rig.StatusAsync(id));
            Assert.Equal("1", await rig.ScalarAsync($"SELECT attempts::text FROM config.external_alert_pending WHERE pending_id = {id}"));
            Assert.Null(await rig.ScalarAsync($"SELECT claimed_at::text FROM config.external_alert_pending WHERE pending_id = {id}"));
            Assert.Equal("true", await rig.ScalarAsync(
                $"SELECT (next_attempt_at > now())::text FROM config.external_alert_pending WHERE pending_id = {id}"));
        });
    }

    [Fact]
    public async Task AMuteOnTheEpisodesKeptServerIdentity_SilencesARepeatFire_AfterTheServersNameChanged()
    {
        await WithRigAsync(async rig =>
        {
            var first = await rig.SubmitAsync("fire", "disk", server: "SQL01");
            var firstPass = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, firstPass.Delivered);
            Assert.Equal("delivered", await rig.StatusAsync(first));

            /* The server is renamed, and the operator mutes the name the episode went out under. */
            rig.Sql01DisplayName = "SQL01-renamed";
            rig.IsMuted = context => string.Equals(context.ServerName, "SQL01", StringComparison.Ordinal) && context.ServerId == 7;
            var repeat = await rig.SubmitAsync("fire", "disk", server: "SQL01");

            var repeatPass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, repeatPass.Muted);
            Assert.Equal("muted", await rig.StatusAsync(repeat));
            Assert.Equal("SQL01", rig.Sent[^1].ServerName);
            Assert.True(rig.Sent[^1].Muted);
        });
    }

    [Fact]
    public async Task APartialFanOut_OneChannelDeliveredAndOneFailed_IsDelivered_AndNeverRetried()
    {
        await WithRigAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(true, "email+webhook", "the SMTP session was reset");
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Delivered);
            Assert.Equal(0, pass.Retried);
            Assert.Equal("delivered", await rig.StatusAsync(id));
            Assert.Single(rig.Sent);
        });
    }

    [Fact]
    public void TheStateWriteLockTimeoutSql_AndItsConstant_Agree()
    {
        Assert.Equal(
            ExternalAlertStore.StateWriteLockTimeoutSql,
            $"SET LOCAL lock_timeout = '{ExternalAlertStore.StateWriteLockTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture)}ms'");
    }

    [Fact]
    public void TheDrainRunsOnItsOwnLoop_StartedWithTheService_AndNeverInsideTheFleetLoop()
    {
        var worker = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        var calls = Regex.Matches(worker, @"\bRunPassAsync\(").Count;
        Assert.Equal(1, calls);

        var loopStart = worker.IndexOf("private async Task RunExternalAlertLoopAsync(", StringComparison.Ordinal);
        Assert.True(loopStart > 0, "the external alert loop method is missing");
        var loopEnd = worker.IndexOf("    /// <summary>", loopStart, StringComparison.Ordinal);
        var call = worker.IndexOf("RunPassAsync(", StringComparison.Ordinal);
        Assert.InRange(call, loopStart, loopEnd);

        /* Started, not awaited, where the drain is built; awaited once at shutdown. */
        Assert.Contains("var externalAlertLoop = RunExternalAlertLoopAsync(externalAlertDrain, stoppingToken);", worker, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(worker, @"await externalAlertLoop;"));

        var interval = Regex.Match(worker, @"s_externalAlertInterval\s*=\s*TimeSpan\.FromSeconds\((\d+)\)");
        Assert.True(interval.Success);
        Assert.Equal(15, int.Parse(interval.Groups[1].Value, CultureInfo.InvariantCulture));

        /* The loop skips a tick while the service is paused, and reads the master switch inside the drain. */
        Assert.Contains("ShouldRunCollection(_paused)", worker[loopStart..loopEnd], StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private static async Task WithRigAsync(Func<Rig, Task> body)
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

        var rig = new Rig(seed, dataSource, scratch.ConnectionString, ct);
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

        public Rig(NpgsqlConnection seed, NpgsqlDataSource dataSource, string connectionString, CancellationToken ct)
        {
            _seed = seed;
            ConnectionString = connectionString;
            Ct = ct;
            History = new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            Store = new ExternalAlertStore(dataSource, 30);
            Drain = new ExternalAlertDrain(
                Store,
                () => true,
                async (outcome, token) =>
                {
                    if (BeforeSend is { } before)
                    {
                        await before(outcome);
                    }

                    Sent.Add(outcome);
                    var delivery = Answer;
                    await History.RecordAlertAsync(new AlertHistoryRecord(
                        outcome.ServerKey, outcome.ServerName, outcome.MetricName, outcome.CurrentValue, outcome.ThresholdValue,
                        outcome.NumericCurrentValue, outcome.NumericThresholdValue, delivery, outcome.Muted, outcome.DetailText,
                        AlertContextSerializer.Serialize(outcome.Context!)));
                    if (AfterSend is { } after)
                    {
                        await after(outcome);
                    }

                    return delivery;
                },
                (_, _, _, _, _, _) => Task.FromResult(PagerDutyAnswer),
                History,
                context => IsMuted(context),
                name => string.Equals(name, "SQL01", StringComparison.OrdinalIgnoreCase) ? new ExternalAlertServer(7, Sql01DisplayName) : null,
                "test:1",
                NullLogger.Instance);
        }

        public string ConnectionString { get; }
        public CancellationToken Ct { get; }
        public PgAlertHistoryStore History { get; }
        public ExternalAlertStore Store { get; }
        public ExternalAlertDrain Drain { get; }
        public int SourceId { get; private set; }
        public List<AlertOutcome> Sent { get; } = new();
        public Func<AlertOutcome, Task>? BeforeSend { get; set; }
        public Func<AlertOutcome, Task>? AfterSend { get; set; }
        public Func<AlertMuteContext, bool> IsMuted { get; set; } = _ => false;
        public string Sql01DisplayName { get; set; } = "SQL01";
        public AlertChannelOutcome PagerDutyAnswer { get; set; } = AlertChannelOutcome.Delivered;
        public AlertDelivery Answer { get; set; } = AlertDelivery.FromLegacyStoredColumns(true, "webhook", null);

        public async Task InitAsync() =>
            SourceId = Convert.ToInt32(
                await ScalarAsync("INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('nagios', 'role_nagios') RETURNING source_id"),
                CultureInfo.InvariantCulture);

        public async Task<long> SubmitAsync(string kind, string key, string? server = null)
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, title, server_name, expires_at)"
                + " VALUES ($1, $2, $3, 'warning', 'a title', $4, now() + interval '15 minutes') RETURNING pending_id", _seed);
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = SourceId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = kind });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key });
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)server ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            return Convert.ToInt64(await command.ExecuteScalarAsync(Ct), CultureInfo.InvariantCulture);
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
