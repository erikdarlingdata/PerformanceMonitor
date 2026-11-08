/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8322: <c>get_pg_xmin_horizon</c> prescribed the old-snapshot remedy ("find it in pg_stat_activity ... fix the client")
/// for a session holder whose xmin age peaked at 2 transactions. An xmin one or two transactions behind is every live
/// transaction on a busy server: nothing is old and nothing needs fixing. The read applies the same age floor
/// <c>get_pg_session_states</c> grades a holder against (<see cref="DarlingPgSessionStatesReader.MinSustainedHolderHorizonAge"/>,
/// #8116) before <c>holder_present</c> carries the remedy; under it the answer says a holder was seen and that its age never
/// became material. At the floor the remedy still stands.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPgXminAgeFloorLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SessionDetail = "state=idle in transaction application=app xact_start=2026-01-01T00:00:00 query_start=2026-01-01T00:00:00";
    private const string ActiveDetail = "state=active application=app xact_start=2026-01-01T00:00:00 query_start=2026-01-01T00:00:00";
    private const string Rider = "still worth closing";

    private static Task Exec(NpgsqlConnection c, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, sql, values);

    private static Task SeedHolder(NpgsqlConnection c, string name, DateTime at, string source, long age, string holder, string detail, bool winner) =>
        Exec(c,
            "INSERT INTO pg_xmin_horizon (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerIdHelper.GetDeterministicHashCode(name), name, source, age, holder, detail, winner);

    private static Task RunAsync(string name, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_xmin_horizon"], [name], ["pg_xmin_horizon"], async (c, ds, end) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-30), "pg_xmin_horizon", end.AddDays(-2), 30, end, ["pg_xmin_horizon"], TestContext.Current.CancellationToken);
            await body(c, ds, end);
        });

    private static Task<string> Call(NpgsqlDataSource ds, string name, DateTime end) =>
        DarlingMcpPgXminTools.GetPgXminHorizon(ds, name, 4, as_of: WebDataStartNote.FormatWindowEnd(end));

    private static string Floor => DarlingPgSessionStatesReader.MinSustainedHolderHorizonAge.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>The issue's repro: a session holder seen at age 2 and then at age 1 across a four-hour window.</summary>
    [Fact]
    public async Task AHolderAtAgeOneAndTwo_IsReportedWithoutTheRemedy_AgainstDevPostgres()
    {
        await RunAsync("xmin-floor-under", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-floor-under", end.AddMinutes(-20), "session", 2, "4242", SessionDetail, true);
            await SeedHolder(c, "xmin-floor-under", end.AddMinutes(-10), "session", 1, "4242", SessionDetail, true);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-floor-under", end));

            /* The holder is still present: the read does not pretend the row is not there. */
            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            Assert.Equal("session", root.GetProperty("winning_source").GetString());
            Assert.Equal(1, root.GetProperty("winning_xmin_age").GetInt64());

            var action = root.GetProperty("recommended_action").GetString()!;
            Assert.DoesNotContain("holding an old snapshot", action, StringComparison.Ordinal);
            Assert.DoesNotContain("pg_stat_activity", action, StringComparison.Ordinal);
            Assert.DoesNotContain("fix the client", action, StringComparison.Ordinal);
            /* It names the age it saw and the floor it fell short of, so the answer explains itself. */
            Assert.Contains("2 transactions", action, StringComparison.Ordinal);
            Assert.Contains(Floor, action, StringComparison.Ordinal);
            /* The floor settles only what VACUUM lost. This holder is idle in transaction, so the sentence carries no
               all-clear and says the session is still worth closing, pointing at detail's state and xact_start and at
               get_pg_session_states for how long it has been idle. */
            Assert.DoesNotContain("Nothing needs fixing", action, StringComparison.Ordinal);
            Assert.Contains(Rider, action, StringComparison.Ordinal);
            Assert.Contains("xact_start", action, StringComparison.Ordinal);
            Assert.Contains("get_pg_session_states", action, StringComparison.Ordinal);

            var holder = Assert.Single(root.GetProperty("holders").EnumerateArray());
            Assert.Equal(2, holder.GetProperty("peak_xmin_age").GetInt64());
            Assert.Equal(action, holder.GetProperty("remedy").GetString());
        });
    }

    /// <summary>An active session under the floor is in-flight work: the floor sentence, no rider and no all-clear.</summary>
    [Fact]
    public async Task AnActiveSessionHolderUnderTheFloor_GetsNoIdleRider_AgainstDevPostgres()
    {
        await RunAsync("xmin-floor-active", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-floor-active", end.AddMinutes(-10), "session", 3, "4343", ActiveDetail, true);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-floor-active", end));

            var action = root.GetProperty("recommended_action").GetString()!;
            Assert.Contains(Floor, action, StringComparison.Ordinal);
            Assert.DoesNotContain("holding an old snapshot", action, StringComparison.Ordinal);
            Assert.DoesNotContain(Rider, action, StringComparison.Ordinal);
            Assert.DoesNotContain("Nothing needs fixing", action, StringComparison.Ordinal);
        });
    }

    /// <summary>The rider is about a session's open transaction: a slot or a prepared transaction under the floor never carries it.</summary>
    [Fact]
    public async Task ASlotOrPreparedHolderUnderTheFloor_GetsNoIdleRider_AgainstDevPostgres()
    {
        await RunAsync("xmin-floor-nonsession", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-floor-nonsession", end.AddMinutes(-10), "replication_slot", 900, "slot_a", "slot detail", true);
            await SeedHolder(c, "xmin-floor-nonsession", end.AddMinutes(-10), "prepared_transaction", 50, "gid-1", "state=idle in transaction prepared", false);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-floor-nonsession", end));

            var holders = root.GetProperty("holders").EnumerateArray().ToList();
            Assert.Equal(2, holders.Count);
            foreach (var holder in holders)
            {
                var remedy = holder.GetProperty("remedy").GetString()!;
                Assert.Contains(Floor, remedy, StringComparison.Ordinal);
                Assert.DoesNotContain(Rider, remedy, StringComparison.Ordinal);
                Assert.DoesNotContain("Nothing needs fixing", remedy, StringComparison.Ordinal);
                Assert.DoesNotContain("abandoned", remedy, StringComparison.Ordinal);
            }
        });
    }

    /// <summary>Exactly at the floor the holder is material and the source's remedy is the answer.</summary>
    [Fact]
    public async Task AHolderAtTheFloor_StillCarriesTheRemedy_AgainstDevPostgres()
    {
        await RunAsync("xmin-floor-at", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-floor-at", end.AddMinutes(-10), "session", DarlingPgSessionStatesReader.MinSustainedHolderHorizonAge, "77", SessionDetail, true);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-floor-at", end));

            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            var action = root.GetProperty("recommended_action").GetString()!;
            Assert.Contains("holding an old snapshot", action, StringComparison.Ordinal);
            Assert.Contains("pg_stat_activity", action, StringComparison.Ordinal);
            Assert.Equal(DarlingMcpPgXminTools.RemedyFor("session"), action);
        });
    }

    /// <summary>
    /// The floor is per source: a slot that reached the floor earlier in the window keeps its remedy even while an
    /// immaterial session holder is the current winner, and the two holders do not share one sentence.
    /// </summary>
    [Fact]
    public async Task EachHolderIsGradedOnItsOwnPeak_AgainstDevPostgres()
    {
        await RunAsync("xmin-floor-mixed", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-floor-mixed", end.AddMinutes(-20), "replication_slot", DarlingPgSessionStatesReader.MinSustainedHolderHorizonAge + 500, "slot_a", "slot detail", true);
            await SeedHolder(c, "xmin-floor-mixed", end.AddMinutes(-10), "replication_slot", 900, "slot_a", "slot detail", false);
            await SeedHolder(c, "xmin-floor-mixed", end.AddMinutes(-10), "session", 1, "4242", SessionDetail, true);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-floor-mixed", end));

            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            foreach (var holder in root.GetProperty("holders").EnumerateArray())
            {
                var remedy = holder.GetProperty("remedy").GetString()!;
                if (holder.GetProperty("source").GetString() == "replication_slot")
                {
                    Assert.Equal(DarlingMcpPgXminTools.RemedyFor("replication_slot"), remedy);
                }
                else
                {
                    Assert.DoesNotContain("holding an old snapshot", remedy, StringComparison.Ordinal);
                    Assert.Contains(Floor, remedy, StringComparison.Ordinal);
                }
            }
        });
    }
}
