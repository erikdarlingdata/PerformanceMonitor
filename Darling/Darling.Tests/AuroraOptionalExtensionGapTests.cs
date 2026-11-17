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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8913: an Aurora server does not offer pg_stat_kcache or pg_qualstats, so the kernel and predicate collectors are an engine limit there,
/// not a setup step. The decision is a pure function of the engine kind and the extensions the server's census shows as absent. An Aurora
/// server that offers the extension (available but not created), and a vanilla PostgreSQL server that does not offer it, keep the setup wording.
/// </summary>
public sealed class AuroraOptionalExtensionGapTests
{
    private static readonly string[] Collectors = { "pg_kernel_stats", "pg_predicate_stats" };

    private static IReadOnlyCollection<string> Absent => new[] { "pg_stat_kcache", "pg_qualstats" };

    private static string? Message(string? engineKind, string collector, IReadOnlyCollection<string>? absent) =>
        CollectorEngineCapability.NotCollectedMessage("srv", CollectorEngineCapability.UnknownEngineEdition, engineKind, collector, AwsRdsKind.None, absent);

    [Theory]
    [InlineData("pg_kernel_stats", "pg_stat_kcache")]
    [InlineData("pg_predicate_stats", "pg_qualstats")]
    public void Aurora_with_the_extension_absent_is_an_engine_limit_with_no_setup_step(string collector, string extension)
    {
        var message = Message(MonitoredEngineKind.AuroraPostgres, collector, Absent);

        Assert.NotNull(message);
        Assert.Contains(extension, message, StringComparison.Ordinal);
        Assert.Contains("does not offer", message, StringComparison.Ordinal);
        Assert.Contains("never will", message, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE EXTENSION", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pg_kernel_stats")]
    [InlineData("pg_predicate_stats")]
    public void Aurora_that_offers_the_extension_keeps_the_setup_wording(string collector)
    {
        /* Available but not created: the census says "available", so nothing is absent and the read keeps its precondition. */
        Assert.Null(Message(MonitoredEngineKind.AuroraPostgres, collector, Array.Empty<string>()));
        Assert.Null(Message(MonitoredEngineKind.AuroraPostgres, collector, null));
    }

    [Theory]
    [InlineData("pg_kernel_stats")]
    [InlineData("pg_predicate_stats")]
    public void Vanilla_postgres_with_the_extension_absent_keeps_the_setup_wording(string collector)
    {
        Assert.Null(Message(MonitoredEngineKind.Postgres, collector, Absent));
    }

    [Fact]
    public void Aurora_with_only_the_other_extension_absent_gates_only_the_other_collector()
    {
        var onlyKcache = new[] { "pg_stat_kcache" };

        Assert.NotNull(Message(MonitoredEngineKind.AuroraPostgres, "pg_kernel_stats", onlyKcache));
        Assert.Null(Message(MonitoredEngineKind.AuroraPostgres, "pg_predicate_stats", onlyKcache));
    }

    [Fact]
    public void Absent_extensions_never_gate_a_collector_outside_the_two()
    {
        Assert.Null(Message(MonitoredEngineKind.AuroraPostgres, "pg_buffer_usage", new[] { "pg_buffercache", "pg_stat_kcache" }));
    }

    [Fact]
    public void The_gated_set_and_the_need_setup_count_follow_the_status()
    {
        var gated = FleetEngineGatedCollectors.For(
            CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.AuroraPostgres, AwsRdsKind.None, Absent);
        var offered = FleetEngineGatedCollectors.For(
            CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.AuroraPostgres, AwsRdsKind.None, Array.Empty<string>());
        var vanilla = FleetEngineGatedCollectors.For(
            CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.Postgres, AwsRdsKind.None, Absent);

        foreach (var collector in Collectors)
        {
            Assert.Contains(collector, gated);
            Assert.DoesNotContain(collector, offered);
            Assert.DoesNotContain(collector, vanilla);
        }

        /* The same rows the status bar tallies: gated they are not collected, ungated they are "need setup". */
        var tally = new CollectorHealthClassifier.BandTally();
        var ungated = new CollectorHealthClassifier.BandTally();
        foreach (var collector in Collectors)
        {
            tally = tally.Add(collector, CollectorHealthClassifier.ExtensionMissing, alwaysOnOff: false, engineGated: gated.Contains(collector));
            ungated = ungated.Add(collector, CollectorHealthClassifier.ExtensionMissing, alwaysOnOff: false, engineGated: offered.Contains(collector));
        }

        Assert.Equal(0, tally.NoPermission);
        Assert.Equal(2, tally.NotCollected);
        Assert.Equal(2, ungated.NoPermission);
    }

    private static PostgresException MissingFunction() => new("function public.f() does not exist", "ERROR", "ERROR", "42883");

    [Theory]
    [InlineData("pg_kernel_stats")]
    [InlineData("pg_predicate_stats")]
    public void The_missing_extension_sentence_names_the_stamped_database_when_there_is_one(string collector)
    {
        /* The fan-out collector stamps the database that failed on the exception, and the call site passes the stamp ahead of the entry database. */
        var fault = MissingFunction();
        CollectorFaultDatabase.Stamp(fault, "postgres");

        var (status, explanation) = DarlingWorker.PostgresFaultOutcome(fault, collector, CollectorFaultDatabase.For(fault, "appdb"));

        Assert.Equal("EXTENSION_MISSING", status);
        Assert.Contains("database 'postgres'", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("'appdb'", explanation, StringComparison.Ordinal);
    }

    [Fact]
    public void The_missing_extension_sentence_names_the_entry_database_when_nothing_is_stamped()
    {
        var fault = MissingFunction();

        var (status, explanation) = DarlingWorker.PostgresFaultOutcome(fault, "pg_kernel_stats", CollectorFaultDatabase.For(fault, "appdb"));

        Assert.Equal("EXTENSION_MISSING", status);
        Assert.Contains("database 'appdb', which is the one this collector connects to", explanation, StringComparison.Ordinal);
    }

    [Fact]
    public void A_per_database_collector_is_told_to_create_the_extension_in_each_database_and_names_the_one_that_failed()
    {
        var fault = MissingFunction();
        CollectorFaultDatabase.Stamp(fault, "postgres");

        var (status, explanation) = DarlingWorker.PostgresFaultOutcome(fault, "pg_predicate_stats", CollectorFaultDatabase.For(fault, "appdb"));

        Assert.Equal("EXTENSION_MISSING", status);
        Assert.Contains("CREATE EXTENSION pg_qualstats in each database this collector reads", explanation, StringComparison.Ordinal);
        Assert.Contains("it failed in database 'postgres'", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("which is the one this collector connects to", explanation, StringComparison.Ordinal);

        /* No stamp: the entry database stands in for the one that failed, and the sentence still says each database. */
        var unstamped = MissingFunction();
        var (_, fallback) = DarlingWorker.PostgresFaultOutcome(unstamped, "pg_predicate_stats", CollectorFaultDatabase.For(unstamped, "appdb"));
        Assert.Contains("in each database this collector reads", fallback, StringComparison.Ordinal);
        Assert.Contains("it failed in database 'appdb'", fallback, StringComparison.Ordinal);
    }

    [Fact]
    public void A_per_database_collector_without_a_declared_extension_gets_the_same_each_database_sentence_on_the_generic_arm()
    {
        var fault = new PostgresException("relation \"public.t\" does not exist", "ERROR", "ERROR", "42P01");
        CollectorFaultDatabase.Stamp(fault, "postgres");

        var perDatabase = CollectorCatalog.All.First(c => c.TargetEngine == CollectorTargetEngine.PostgreSql
            && c.RunsPerDatabase(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql })
            && c.RequiredPgExtensions.Count == 0);

        var (_, explanation) = DarlingWorker.PostgresFaultOutcome(fault, perDatabase.Name, CollectorFaultDatabase.For(fault, "appdb"));

        Assert.Contains("in each database this collector reads", explanation, StringComparison.Ordinal);
        Assert.Contains("it failed in database 'postgres'", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("which is the one this collector connects to", explanation, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_reads_the_gate_rows_and_the_fleet_count_follow_the_stored_census()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live census test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var now = DateTime.UtcNow;

            /* 1: Aurora, both absent. 2: Aurora, kcache available (one CREATE EXTENSION away), qualstats absent. 3: vanilla, both absent. 4: Aurora, no census. */
            foreach (var (id, kind) in new[] { (1, "aurora-postgres"), (2, "aurora-postgres"), (3, "postgres"), (4, "aurora-postgres") })
            {
                await ExecAsync(source, $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) VALUES ({id}, 'srv{id}', 'srv{id}', TRUE, '{kind}', now() at time zone 'utc', now() at time zone 'utc')", ct);
            }

            foreach (var (id, extension, state) in new[]
            {
                (1, "pg_stat_kcache", "absent"), (1, "pg_qualstats", "absent"),
                (2, "pg_stat_kcache", "available"), (2, "pg_qualstats", "absent"),
                (3, "pg_stat_kcache", "absent"), (3, "pg_qualstats", "absent"),
            })
            {
                /* Two database rows per capture, as the per-database census writes, and an older capture that disagrees: only the newest counts. */
                foreach (var database in new[] { "postgres", "appdb" })
                {
                    await InsertCensusAsync(source, now.AddHours(-1), id, database, extension, state, ct);
                }

                await InsertCensusAsync(source, now.AddDays(-2), id, "postgres", extension, state == "absent" ? "available" : "absent", ct);
            }

            var all = await ExtensionsNotOffered.ReadAsync(source, null, 30, ct);
            Assert.Equal(new[] { "pg_qualstats", "pg_stat_kcache" }, all[1].Order(StringComparer.Ordinal));
            Assert.Equal(new[] { "pg_qualstats" }, all[2]);
            Assert.False(all.ContainsKey(4));
            Assert.Equal(new[] { 1, 2, 3 }, all.Keys.Order());
            Assert.Single(await ExtensionsNotOffered.ReadAsync(source, 2, 30, ct));

            /* The reads: not_collected on the absent Aurora server, and no claim where the census says available, the server is vanilla or never censused. */
            Assert.Equal("not_collected", Status(await DarlingEngineCapability.NotCollectedStatusAsync(source, 1, "srv1", "pg_kernel_stats", ct)));
            Assert.Equal("not_collected", Status(await DarlingEngineCapability.NotCollectedStatusAsync(source, 1, "srv1", "pg_predicate_stats", ct)));
            Assert.Null(await DarlingEngineCapability.NotCollectedStatusAsync(source, 2, "srv2", "pg_kernel_stats", ct));
            Assert.Equal("not_collected", Status(await DarlingEngineCapability.NotCollectedStatusAsync(source, 2, "srv2", "pg_predicate_stats", ct)));
            Assert.Null(await DarlingEngineCapability.NotCollectedStatusAsync(source, 3, "srv3", "pg_kernel_stats", ct));
            Assert.Null(await DarlingEngineCapability.NotCollectedStatusAsync(source, 4, "srv4", "pg_kernel_stats", ct));

            /* The fleet count and the per-server gate rows. */
            var fleet = await FleetEngineGatedCollectors.ReadAsync(source, 30, ct);
            Assert.Contains("pg_kernel_stats", fleet[1]);
            Assert.Contains("pg_predicate_stats", fleet[1]);
            Assert.DoesNotContain("pg_kernel_stats", fleet[2]);
            Assert.Contains("pg_predicate_stats", fleet[2]);
            foreach (var unclaimed in new[] { 3, 4 })
            {
                Assert.DoesNotContain("pg_kernel_stats", fleet[unclaimed]);
                Assert.DoesNotContain("pg_predicate_stats", fleet[unclaimed]);
            }

            var gate = await DarlingGatedCollectorRows.ReadEngineGateAsync(source, 1, "srv1", ct);
            Assert.NotNull(gate);
            Assert.True(gate!.IsGated("pg_kernel_stats"));
            Assert.False((await DarlingGatedCollectorRows.ReadEngineGateAsync(source, 3, "srv3", ct))!.IsGated("pg_kernel_stats"));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static string? Status(string? envelope)
    {
        Assert.NotNull(envelope);
        using var document = JsonDocument.Parse(envelope!);
        return document.RootElement.GetProperty("status").GetString();
    }

    private static async Task InsertCensusAsync(NpgsqlDataSource source, DateTime when, int serverId, string database, string extension, string state, CancellationToken ct)
    {
        await using var command = source.CreateCommand(
            "INSERT INTO pg_extension_availability (collection_id, collection_time, server_id, server_name, database_name, extension_name, state, is_monitoring_relevant) " +
            "VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)");
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = CollectionIdGenerator.Next() });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(when, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "srv" + serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = database });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = extension });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = state });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
