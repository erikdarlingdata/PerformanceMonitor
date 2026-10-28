/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6866 end to end against targets whose OWN <c>log_timezone</c> is a named local zone. The server writes a real
/// ERROR into its own log in local time with a zone abbreviation, the production log-events read collects it through
/// the real route with a resolver that asks the same target, and the stored instant has to be the true UTC moment the
/// error happened, hours away from the local text. Before the change the read refused the whole target.
///
/// <para>Two clusters, because a cluster has one <c>log_timezone</c>: <c>DARLING_TEST_PG_LOGZONE_BERLIN</c> started
/// with <c>logging_collector = on</c>, <c>log_destination = 'stderr'</c> and <c>log_timezone = 'Europe/Berlin'</c>
/// (the stderr route), and <c>DARLING_TEST_PG_LOGZONE_LA</c> with <c>log_destination = 'csvlog'</c> and
/// <c>log_timezone = 'America/Los_Angeles'</c> (the csvlog route). Both need <c>log_min_messages</c> no stricter than
/// the default, and the login must be able to read the log directory.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgLogNamedZoneLiveTests
{
    [Fact]
    public async Task AStderrLogInEuropeBerlin_IsStoredAtTheTrueUtcInstant()
    {
        await RunAsync("DARLING_TEST_PG_LOGZONE_BERLIN", "Europe/Berlin", usesCsvlog: false);
    }

    [Fact]
    public async Task ACsvlogInAmericaLosAngeles_IsStoredAtTheTrueUtcInstant()
    {
        await RunAsync("DARLING_TEST_PG_LOGZONE_LA", "America/Los_Angeles", usesCsvlog: true);
    }

    private static async Task RunAsync(string variable, string expectedSetting, bool usesCsvlog)
    {
        var target = Environment.GetEnvironmentVariable(variable);
        Assert.SkipWhen(string.IsNullOrEmpty(target),
            $"Set {variable} to a PostgreSQL started with logging_collector = on and log_timezone = '{expectedSetting}' "
            + "whose log the login can read, to run the #6866 end-to-end test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(target);
        await connection.OpenAsync(ct);

        await using (var setting = new NpgsqlCommand("SELECT pg_catalog.current_setting('log_timezone')", connection))
        {
            Assert.Equal(expectedSetting, (string?)await setting.ExecuteScalarAsync(ct));
        }

        /* The true UTC instant, read from the server's own clock either side of the error, so the window is the
           server's and not this machine's. */
        await using (var clock = new NpgsqlCommand(
            "SELECT pg_catalog.clock_timestamp() AT TIME ZONE 'UTC'", connection))
        {
            var before = (DateTime)(await clock.ExecuteScalarAsync(ct))!;

            await using (var fail = new NpgsqlCommand("SELECT 1 / 0 AS named_zone_6866", connection))
            {
                await Assert.ThrowsAsync<PostgresException>(async () => await fail.ExecuteScalarAsync(ct));
            }

            var after = (DateTime)(await clock.ExecuteScalarAsync(ct))!;

            var resolverCalls = 0;
            var context = new CollectorContext
            {
                LogHashKey = TestLogHashKeys.Fixed,
                ServerId = 1,
                ServerName = "named-zone-6866",
                CollectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
                Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                PgLogUsesCsvlog = usesCsvlog,
                PgLogZoneResolver = async (zone, pairs, token) =>
                {
                    Interlocked.Increment(ref resolverCalls);
                    await using var own = new NpgsqlConnection(target);
                    await own.OpenAsync(token);
                    return await PgLogNamedZone.ResolveAsync(own, zone, pairs, 30, token);
                },
            };

            /* The syslogger flushes asynchronously, so read until this run's own error is on disk. */
            List<PgLogEvent> mine = new();
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                var events = await ReadAsync(connection, context, ct);
                mine = events.Where(e => e.Severity == "ERROR"
                                         && e.Message.Contains("division by zero", StringComparison.Ordinal)
                                         && e.OccurredAtUtc >= before.AddHours(-12)
                                         && e.OccurredAtUtc <= after.AddHours(12)).ToList();
                if (mine.Any(e => Math.Abs((e.OccurredAtUtc - before).TotalSeconds) < 120))
                {
                    break;
                }

                await Task.Delay(300, ct);
            }

            Assert.True(resolverCalls > 0, "the read never asked the target to prove a stamp");
            Assert.NotEmpty(mine);

            /* The local text is one to nine hours away from UTC, so landing inside this window cannot be a
               stamp read as UTC or as the wrong offset. Earlier runs' errors stay in the log and are older. */
            var ours = mine.Where(e => e.OccurredAtUtc >= before.AddSeconds(-2)).ToList();
            Assert.NotEmpty(ours);
            Assert.All(ours, e =>
            {
                Assert.Equal(DateTimeKind.Utc, e.OccurredAtUtc.Kind);
                Assert.InRange(e.OccurredAtUtc, before.AddSeconds(-2), after.AddSeconds(2));
            });
        }
    }

    private static async Task<List<PgLogEvent>> ReadAsync(NpgsqlConnection connection, CollectorContext context, CancellationToken ct)
    {
        var definition = PgLogEventsCollector.Instance;
        await using var command = PostgresTargetProvider.Instance.CreateCommand(definition.BuildQuery(context), connection, 30);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await definition.ReadAsync(reader, context, ct);
    }
}
