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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9879, end to end on a real store: an on-demand <c>analyze_server</c> call whose family budget has already ended
/// names the four heavy query-history families in <c>families_left_out</c>, the scheduled pass of the same service reads
/// every family, and a call with no budget returns a reply that does not carry the field. The budget is zero, which the
/// service fires before the pass starts, so no assert here waits on a clock.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalyzeServerFamilyBudgetLiveTests
{
    private const string ServerName = "darling-analyze-family-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* The four families the pass runs under its budget, in the order the pass reads them. */
    private static readonly string[] HeavyFamilies = ["parameter_sensitivity", "bad_actor", "procedure_stats", "plan_advisory"];

    [Fact]
    public async Task AnEndedBudget_NamesTheFourHeavyFamiliesInTheReply_AndTheScheduledPassLeavesNoneOut()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live family-budget test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            /* More than a day of history, and a reading every fifteen minutes across the default four-hour window, so the
               pass gets past its history gate and reads an observed window. */
            var now = DateTime.UtcNow;
            await PlantWaitAsync(connection, Naive(now.AddHours(-30)), ct);
            for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
                await PlantWaitAsync(connection, Naive(now.AddMinutes(-minutesAgo)), ct);

            var service = new DarlingAnalysisService(postgres) { OnDemandFamilyBudget = TimeSpan.Zero };

            /* The on-demand call: the budget ended before the family stage, so none of the four was read. */
            var cut = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(cut))
            {
                var entries = FamiliesLeftOut(doc.RootElement);
                Assert.Equal(HeavyFamilies, entries.Select(e => e.GetProperty("family").GetString()!));
                Assert.All(entries, e => Assert.Equal(FamilyTimeBudget.Reason, e.GetProperty("reason").GetString()));
                Assert.All(entries, e => Assert.Equal(0.0, e.GetProperty("budget_seconds").GetDouble()));

                /* A family the budget stopped is not a failed read, and the reply's prose carries the same sentence the
                   structured entries stand for. */
                Assert.DoesNotContain(service.LastCollectionFailures, failure => HeavyFamilies.Contains(failure.Family));
                Assert.Contains(
                    FamilyTimeBudget.Describe(HeavyFamilies, TimeSpan.Zero)!,
                    doc.RootElement.GetProperty("message").GetString()!,
                    StringComparison.Ordinal);
            }
            Assert.Equal(HeavyFamilies, service.LastFamiliesLeftOutForTime);
            Assert.Equal(TimeSpan.Zero, service.LastFamilyBudget);

            /* The scheduled pass of the same service has no budget, whatever the on-demand one is set to. */
            await service.AnalyzeAsync(ServerId, ServerName, 4, ct, persistCollectionCaveats: true);
            Assert.Empty(service.LastFamiliesLeftOutForTime);
            Assert.Null(service.LastFamilyBudget);

            /* An on-demand call with no budget reads all four, and its reply does not carry the field. */
            service.OnDemandFamilyBudget = null;
            var whole = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(whole))
            {
                Assert.Empty(FamiliesLeftOut(doc.RootElement));
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }
            Assert.Empty(service.LastFamiliesLeftOutForTime);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private const double FourHoursMs = 4 * 3_600_000.0;
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Stands in for the fact collector over a window with no observed collection. It runs one family that reads until the
    /// token it is handed ends, and one that does not, through the real family stage with the context the service built.
    /// </summary>
    private sealed class UnobservedWindowCollector(bool withHeldFamily) : IFactCollector
    {
        public TaskCompletionSource HeldReadStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The held read: waits on the token its context hands it, which ends at the family deadline or at the pass's own stop.</summary>
        private async Task CollectSlowThingFactsAsync(AnalysisContext context, List<Fact> facts)
        {
            HeldReadStarted.TrySetResult();
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
        }

        public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
        {
            Func<AnalysisContext, List<Fact>, Task> quick = (_, facts) =>
            {
                facts.Add(new Fact { Source = "test", Key = "FB9879_QUICK" });
                return Task.CompletedTask;
            };
            var families = new List<Func<AnalysisContext, List<Fact>, Task>> { quick };
            if (withHeldFamily)
                families.Add(CollectSlowThingFactsAsync);

            context.CollectionFamilyCount = families.Count;
            context.Coverage = WindowCoverage.Unobserved(FourHoursMs);
            var collected = new List<Fact>();
            await PgFactCollector.RunFamiliesAsync(context, collected, families, family => ReferenceEquals(family.Target, this));
            return collected;
        }
    }

    /// <summary>
    /// A family the budget stops over a window with no observed collection is named in the unavailable reply. The collector
    /// runs every family on such a pass, so the budget can cut one there as it does on an observed window, and a reply that
    /// named it only on the other branches would leave that hole unsaid. The held read waits on its deadline token and
    /// never finishes by itself, so the reply can only come from a read the deadline stopped, and no assert here depends on
    /// how long anything takes.
    /// </summary>
    [Fact]
    public async Task AnUnobservedWindow_NamesAFamilyTheBudgetStopped_InItsUnavailableReply()
    {
        await OnAnUnobservedWindowAsync(async (postgres, ct) =>
        {
            var collector = new UnobservedWindowCollector(withHeldFamily: true);
            var budget = TimeSpan.FromMilliseconds(100);
            var service = new DarlingAnalysisService(postgres, sqlServerCollectorWrapper: _ => collector) { OnDemandFamilyBudget = budget };

            var reply = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName).WaitAsync(Limit, ct);

            using var doc = JsonDocument.Parse(reply);
            Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
            Assert.NotNull(service.WindowEmptyMessage);

            var entries = FamiliesLeftOut(doc.RootElement);
            var entry = Assert.Single(entries);
            Assert.Equal("slow_thing", entry.GetProperty("family").GetString());
            Assert.Equal(FamilyTimeBudget.Reason, entry.GetProperty("reason").GetString());
            Assert.Equal(0.1, entry.GetProperty("budget_seconds").GetDouble());
            Assert.Contains(
                FamilyTimeBudget.Describe(["slow_thing"], budget)!,
                doc.RootElement.GetProperty("message").GetString()!,
                StringComparison.Ordinal);

            /* A budget cut is not a failed read. */
            Assert.Empty(service.LastCollectionFailures);
        });
    }

    /// <summary>The unavailable reply of a pass with no family left out carries no families_left_out and no sentence about one.</summary>
    [Fact]
    public async Task AnUnobservedWindow_WithNoFamilyStopped_CarriesNoFamiliesLeftOut()
    {
        await OnAnUnobservedWindowAsync(async (postgres, ct) =>
        {
            var service = new DarlingAnalysisService(postgres, sqlServerCollectorWrapper: _ => new UnobservedWindowCollector(withHeldFamily: false));

            var reply = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName).WaitAsync(Limit, ct);

            using var doc = JsonDocument.Parse(reply);
            Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
            Assert.Empty(FamiliesLeftOut(doc.RootElement));
            Assert.Empty(service.LastFamiliesLeftOutForTime);
            Assert.DoesNotContain("left out for time", doc.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// Registers the test's server with more than a day of wait history and no reading across the last four hours, so a pass
    /// gets past its history gate onto an unobserved window, runs the body, and removes the rows again.
    /// </summary>
    private static async Task OnAnUnobservedWindowAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live family-budget test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            await PlantWaitAsync(connection, Naive(DateTime.UtcNow.AddHours(-30)), ct);

            await body(postgres, ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The reply's <c>families_left_out</c> entries, from the top level or from <c>hints</c>, and none when the reply has no such field.</summary>
    private static List<JsonElement> FamiliesLeftOut(JsonElement reply)
    {
        if (reply.TryGetProperty("families_left_out", out var top))
            return top.EnumerateArray().ToList();
        if (reply.TryGetProperty("hints", out var hints) && hints.ValueKind == JsonValueKind.Object
            && hints.TryGetProperty("families_left_out", out var nested))
            return nested.EnumerateArray().ToList();
        return [];
    }

    /// <summary>Naive-UTC, the Kind every timestamp column in this store is bound with.</summary>
    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, 'FB9879_BENIGN_WAIT', 50, 100)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.analysis_collection_caveats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
