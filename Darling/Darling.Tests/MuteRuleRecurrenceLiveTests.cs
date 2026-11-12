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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #7040 against a live store: <see cref="PgMuteRuleStore"/> round-trips the four schedule columns, the V231 CHECK refuses a
/// half schedule, and <see cref="MuteRuleRecurrenceSql.InWindow"/> answers the same as <see cref="MuteRule.IsInWindowAt"/> for
/// every half hour of two weeks that hold the New York daylight-saving changes. The SQL form and the C# form are two
/// implementations of one rule, so a disagreement is a rule that mutes in one reader and not in another.
/// </summary>
public sealed class MuteRuleRecurrenceLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheStore_RoundTripsTheScheduleColumns_AndTheCheckRefusesAHalfSchedule()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var store = new PgMuteRuleStore(dataSource);
            var scheduled = new MuteRule
            {
                Reason = "nightly", MetricName = "Blocking", RecurDays = 0b0100010,
                RecurStartTime = new TimeOnly(22, 0), RecurEndTime = new TimeOnly(2, 30), RecurTimeZone = "America/New_York",
            };
            var plain = new MuteRule { Reason = "one off", MetricName = "Blocking" };
            await store.InsertAsync(scheduled);
            await store.InsertAsync(plain);

            var loaded = (await store.LoadAllAsync(ct)).ToDictionary(r => r.Id);
            Assert.Equal(0b0100010, loaded[scheduled.Id].RecurDays);
            Assert.Equal(new TimeOnly(22, 0), loaded[scheduled.Id].RecurStartTime);
            Assert.Equal(new TimeOnly(2, 30), loaded[scheduled.Id].RecurEndTime);
            Assert.Equal("America/New_York", loaded[scheduled.Id].RecurTimeZone);
            Assert.Null(loaded[plain.Id].RecurDays);
            Assert.Null(loaded[plain.Id].RecurStartTime);
            Assert.Null(loaded[plain.Id].RecurEndTime);
            Assert.Null(loaded[plain.Id].RecurTimeZone);

            /* An update changes all four, and clearing writes all four back to null. */
            scheduled.RecurDays = null;
            scheduled.RecurStartTime = new TimeOnly(1, 15);
            scheduled.RecurEndTime = new TimeOnly(3, 0);
            scheduled.RecurTimeZone = "UTC";
            await store.UpdateAsync(scheduled);
            var updated = (await store.LoadAllAsync(ct)).Single(r => r.Id == scheduled.Id);
            Assert.Null(updated.RecurDays);
            Assert.Equal(new TimeOnly(1, 15), updated.RecurStartTime);
            Assert.Equal(new TimeOnly(3, 0), updated.RecurEndTime);
            Assert.Equal("UTC", updated.RecurTimeZone);

            scheduled.RecurStartTime = null;
            scheduled.RecurEndTime = null;
            scheduled.RecurTimeZone = null;
            await store.UpdateAsync(scheduled);
            var cleared = (await store.LoadAllAsync(ct)).Single(r => r.Id == scheduled.Id);
            Assert.False(cleared.IsRecurring);
            Assert.Null(cleared.RecurTimeZone);

            /* The CHECK: both times or neither. */
            var half = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var command = new NpgsqlCommand(
                    "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, recur_start_time) VALUES ('half', TRUE, now() AT TIME ZONE 'UTC', 'half', '22:00')", connection);
                await command.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("23514", half.SqlState);
        });
    }

    [Fact]
    public async Task TheSqlWindowTest_AgreesWithTheCSharpOne_EveryHalfHourAcrossBothDaylightSavingChanges()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var store = new PgMuteRuleStore(dataSource);
            /* Mon = bit 1, Tue = bit 2, Fri = bit 5, Sat = bit 6, Sun = bit 0. */
            var rules = new List<(string Name, MuteRule Rule)>
            {
                ("same-day weekdays", Rule(0b0111110, 9, 0, 17, 0, "America/New_York")),
                ("crosses midnight", Rule(0b1100000, 22, 0, 2, 0, "America/New_York")),
                ("start day only, Monday", Rule(0b0000010, 22, 0, 2, 0, "America/New_York")),
                ("Saturday into Sunday", Rule(0b1000000, 23, 0, 1, 0, "UTC")),
                ("every day, crosses midnight", Rule(null, 23, 30, 1, 30, "UTC")),
                ("inside the skipped spring hour", Rule(null, 2, 30, 3, 30, "America/New_York")),
                ("inside the repeated autumn hour", Rule(null, 1, 15, 1, 45, "America/New_York")),
                ("half-hour offset zone", Rule(0b0111110, 9, 0, 17, 0, "Asia/Kolkata")),
            };
            var oneOff = new MuteRule { Reason = "one off" };
            foreach (var (name, rule) in rules)
            {
                rule.Reason = name;
                await store.InsertAsync(rule);
            }

            await store.InsertAsync(oneOff);

            var loaded = (await store.LoadAllAsync(ct)).ToDictionary(r => r.Reason!);
            var starts = new[] { new DateTime(2026, 3, 5, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 10, 29, 0, 0, 0, DateTimeKind.Utc) };
            var sql = $"SELECT g, {MuteRuleRecurrenceSql.InWindow("m", "g")} FROM config.config_mute_rules m, generate_series($1::timestamptz, $1::timestamptz + interval '7 days', interval '30 minutes') AS g WHERE m.id = $2 ORDER BY g";

            foreach (var name in rules.Select(r => r.Name).Append("one off"))
            {
                var rule = loaded[name];
                var trues = 0;
                var falses = 0;
                foreach (var start in starts)
                {
                    await using var command = new NpgsqlCommand(sql, connection);
                    command.Parameters.Add(new NpgsqlParameter { Value = start, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
                    command.Parameters.AddWithValue(rule.Id);
                    await using var reader = await command.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct))
                    {
                        var instant = DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
                        var inSql = reader.GetBoolean(1);
                        Assert.True(inSql == rule.IsInWindowAt(instant),
                            $"'{name}' disagrees at {instant:u}: SQL {inSql}, C# {rule.IsInWindowAt(instant)}.");
                        if (inSql) trues++; else falses++;
                    }
                }

                /* Not vacuous: a scheduled rule is in its window some of the time and outside it the rest. */
                if (name == "one off")
                    Assert.Equal(0, falses);
                else
                    Assert.True(trues > 0 && falses > 0, $"'{name}' never changed answer: {trues} in, {falses} out.");
            }
        });
    }

    /* A zone the store's tzdata lacks, typed straight into the table past every write check. The names stand for the two ways it happens:
       an IANA zone newer than the store's tzdata, and a Windows id the host could not map to an IANA one. */
    private static readonly string[] UnknownZones =
    [
        "Mars/Olympus_Mons", "Mid-Atlantic Standard Time", "Eastern Standard Time", "America/New York", "Not A Zone",
    ];

    [Fact]
    public async Task AZoneThePostgresStoreLacks_ReadsAsOutsideTheWindow_AndTheFleetStatementStillRuns()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            async Task Exec(string sql)
            {
                await using var command = new NpgsqlCommand(sql, connection);
                await command.ExecuteNonQueryAsync(ct);
            }

            await Exec($"SET search_path TO {PgSchemaGenerator.SearchPath}");
            foreach (var id in new[] { 1, 2, 3 })
            {
                await Exec($@"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({id}, 'srv-{id}', 'srv-{id}', TRUE, 15, now(), now())");
            }

            /* Server 1: a window that is open nearly all day in a zone PostgreSQL lacks (it would be open now were the zone known).
               Server 3: a plain whole-server rule, silenced. The unknown-zone rule must neither raise nor silence server 1. */
            var bypass = UnknownZones.ToList();
            for (var i = 0; i < bypass.Count; i++)
            {
                await Exec($@"INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, server_id, server_name, recur_start_time, recur_end_time, recur_time_zone)
VALUES ('bad-{i}', TRUE, now() AT TIME ZONE 'UTC', 1, 'srv-1', '00:01', '00:00', '{bypass[i]}')");
            }

            await Exec(@"INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, server_id, server_name)
VALUES ('plain', TRUE, now() AT TIME ZONE 'UTC', 3, 'srv-3')");

            /* The statement the fleet page and get_fleet_overview run. */
            await using (var fleet = new NpgsqlCommand(DarlingFleetReader.FleetServersSql, connection))
            await using (var reader = await fleet.ExecuteReaderAsync(ct))
            {
                var silenced = new Dictionary<int, bool>();
                while (await reader.ReadAsync(ct))
                {
                    silenced[reader.GetInt32(0)] = reader.GetBoolean(reader.GetOrdinal("is_silenced"));
                }

                Assert.False(silenced[1], "A rule in a zone the store lacks mutes nothing.");
                Assert.False(silenced[2]);
                Assert.True(silenced[3]);
            }

            /* And the SQL window test agrees with the C# one on each such rule across a week. */
            var store = new PgMuteRuleStore(dataSource);
            var loaded = (await store.LoadAllAsync(ct)).Where(r => r.Id.StartsWith("bad-", StringComparison.Ordinal)).ToList();
            Assert.Equal(bypass.Count, loaded.Count);
            var sql = $"SELECT g, {MuteRuleRecurrenceSql.InWindow("m", "g")} FROM config.config_mute_rules m, generate_series($1::timestamptz, $1::timestamptz + interval '7 days', interval '30 minutes') g WHERE m.id = $2";
            foreach (var rule in loaded)
            {
                await using var command = new NpgsqlCommand(sql, connection);
                command.Parameters.Add(new NpgsqlParameter { Value = new DateTime(2026, 3, 5, 0, 0, 0, DateTimeKind.Utc), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
                command.Parameters.AddWithValue(rule.Id);
                await using var reader = await command.ExecuteReaderAsync(ct);
                var rows = 0;
                while (await reader.ReadAsync(ct))
                {
                    var instant = DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
                    Assert.False(reader.GetBoolean(1), $"'{rule.RecurTimeZone}' read as inside a window in SQL at {instant:u}.");
                    /* A host with ICU resolves a Windows id that has an IANA equivalent, so only the SQL answer is asserted for it. */
                    if (rule.RecurTimeZone != "Eastern Standard Time")
                        Assert.False(rule.IsInWindowAt(instant), $"'{rule.RecurTimeZone}' read as inside a window in C# at {instant:u}.");
                    rows++;
                }

                Assert.True(rows > 300);
            }
        });
    }

    [Fact]
    public async Task TheStore_RefusesAZoneItDoesNotKnow_OnInsertAndUpdate_AndAcceptsTheIanaZones()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var store = new PgMuteRuleStore(dataSource);
            foreach (var zone in UnknownZones)
            {
                var rule = Rule(null, 22, 0, 2, 0, zone);
                Assert.NotNull(await store.RecurrenceRefusalAsync(rule));
                var inserted = await Assert.ThrowsAsync<ArgumentException>(() => store.InsertAsync(rule));
                Assert.Contains(zone, inserted.Message);
                Assert.Contains("PostgreSQL", inserted.Message);
            }

            Assert.Empty(await store.LoadAllAsync(ct));

            var kept = Rule(null, 22, 0, 2, 0, "America/New_York");
            await store.InsertAsync(kept);
            foreach (var zone in new[] { "UTC", "Europe/Kyiv", "Asia/Kolkata", "Etc/GMT+5", "america/chicago" })
            {
                Assert.Null(await store.RecurrenceRefusalAsync(Rule(null, 22, 0, 2, 0, zone)));
            }

            kept.RecurTimeZone = "Mars/Olympus_Mons";
            var updated = await Assert.ThrowsAsync<ArgumentException>(() => store.UpdateAsync(kept));
            Assert.Contains("Mars/Olympus_Mons", updated.Message);
            Assert.Equal("America/New_York", (await store.LoadAllAsync(ct)).Single().RecurTimeZone);

            /* A rule with no schedule has no zone to check, and disabling or deleting a rule never asks. */
            Assert.Null(await store.RecurrenceRefusalAsync(new MuteRule { Reason = "one off" }));
        });
    }

    private static MuteRule Rule(int? days, int startHour, int startMinute, int endHour, int endMinute, string zone) =>
        new()
        {
            RecurDays = days, RecurStartTime = new TimeOnly(startHour, startMinute), RecurEndTime = new TimeOnly(endHour, endMinute),
            RecurTimeZone = zone,
        };

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the recurring mute rule live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(dataSource, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
