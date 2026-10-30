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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// Time follow-ups on a live store (#6008, D16 of #5632 Phase C): a report sets <c>follow_up_at</c>; the sweep, as the store
/// owner, opens a request that names the report's request in <c>follow_up_of</c>, whose brief carries the earlier report, and
/// journals one <c>follow_up</c> event; a time over 30 days out, or a 21st pending follow-up on a route, is refused.
/// </summary>
public sealed class InvestigationFollowUpLiveTests
{
    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\",\"basis\":\"verified\"}]";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private DateTime _now = DateTime.UtcNow.AddSeconds(-5);

    private InvestigationAlert Alert(string family, string metric = "Blocking") =>
        new(1, 7, "srv-7", family, metric, _now, new[] { "key-" + family }, null, 500, 500, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>Opens a request for <paramref name="family"/>, claims it and returns its id and claim. A later call with the same
    /// <paramref name="metric"/> would join the reported request quietly (#6778), so a caller that needs several requests passes a new metric for each.</summary>
    private async Task<(Guid Id, Guid Claim)> ClaimedAsync(InvestigationRequestStore store, string family, CancellationToken ct, string metric = "Blocking")
    {
        var id = (await store.OpenOrJoinAsync(Alert(family, metric), ct)).RequestId!.Value;
        return (id, (await store.ClaimAsync(id, "agent-a", ct)).ClaimId!.Value);
    }

    private static Task<string> SubmitAsync(InvestigationRequestStore store, (Guid Id, Guid Claim) request, string? followUpAt, CancellationToken ct) =>
        DarlingMcpInvestigationTools.SubmitAsync(store, request.Id.ToString(), request.Claim.ToString(), 1, "needs_person",
            "a blocking chain", Evidence, "summary", null, null, ct, follow_up_at: followUpAt);

    private static string At(DateTime utc) => utc.ToString("o", CultureInfo.InvariantCulture);

    [Fact]
    public async Task ADueFollowUp_Opens_NamingTheEarlierRequest_AndItsBriefCarriesTheEarlierReport()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var earlier = await ClaimedAsync(store, "blocking", ct);
            var due = DateTime.UtcNow.AddHours(1);
            Assert.Equal("reported", Root(await SubmitAsync(store, earlier, At(due), ct)).GetProperty("status").GetString());

            /* Not yet due: nothing opens. */
            Assert.Equal(0, (await store.SweepAsync(ct)).FollowUps);

            var later = new InvestigationRequestStore(dataSource, 30, () => due.AddMinutes(1));
            Assert.Equal(1, (await later.SweepAsync(ct)).FollowUps);

            var followUpId = Guid.Parse((await ScalarAsync(connection,
                $"SELECT request_id::text FROM config.investigation_requests WHERE follow_up_of = '{earlier.Id}'", ct))!);
            var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => due.AddMinutes(1)).ReadAsync(followUpId, ct);
            Assert.NotNull(sources);
            Assert.Equal("blocking", sources!.Request.Family);
            Assert.Equal(earlier.Id, Assert.Single(sources.EarlierReports).RequestId);
        });
    }

    [Fact]
    public async Task The21stPendingFollowUpOnARoute_IsRefused_AndNothingIsStored()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var at = At(DateTime.UtcNow.AddDays(1));
            for (var i = 0; i < InvestigationReportFields.MaxPendingFollowUpsPerRoute; i++)
            {
                var request = await ClaimedAsync(store, "blocking", ct, metric: "Blocking " + i.ToString(CultureInfo.InvariantCulture));
                Assert.Equal("reported", Root(await SubmitAsync(store, request, at, ct)).GetProperty("status").GetString());
            }

            var twentyFirst = await ClaimedAsync(store, "blocking", ct, metric: "Blocking last");
            var refused = Root(await SubmitAsync(store, twentyFirst, at, ct));
            Assert.Equal("refused", refused.GetProperty("status").GetString());
            Assert.Contains("pending follow-ups", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_reports WHERE request_id = '{twentyFirst.Id}'", ct));

            /* The same report without a follow-up time still goes in. */
            Assert.Equal("reported", Root(await SubmitAsync(store, twentyFirst, null, ct)).GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task AFollowUpOverThirtyDaysOut_IsRefused_AndNothingIsStored()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var request = await ClaimedAsync(store, "blocking", ct);
            var refused = Root(await SubmitAsync(store, request, At(DateTime.UtcNow.AddDays(31)), ct));
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
            Assert.Contains("follow_up_at", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_reports WHERE request_id = '{request.Id}'", ct));

            Assert.Equal("reported", Root(await SubmitAsync(store, request, At(DateTime.UtcNow.AddDays(29)), ct)).GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task TheFollowUpEvent_IsWrittenOnce_WhateverHowOftenTheSweepRuns()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var earlier = await ClaimedAsync(store, "blocking", ct);
            var due = DateTime.UtcNow.AddHours(1);
            await SubmitAsync(store, earlier, At(due), ct);

            var later = new InvestigationRequestStore(dataSource, 30, () => due.AddMinutes(1));
            Assert.Equal(1, (await later.SweepAsync(ct)).FollowUps);
            Assert.Equal(0, (await later.SweepAsync(ct)).FollowUps);

            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(*)::text FROM config.investigation_journal WHERE event = 'follow_up' AND actor = 'service'", ct));
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_requests WHERE follow_up_of = '{earlier.Id}'", ct));
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the follow-up live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, dataSource, connection, ct);
    }
}
