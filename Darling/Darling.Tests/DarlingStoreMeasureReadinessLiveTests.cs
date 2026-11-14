/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8787 part D2 against a real store: the measured empty space of one table and its verdict, the refusals (outside the product
/// schemas, a view, an injection-shaped name, a partitioned parent), the maintenance readiness verdict under a held lock, the job
/// crash read, and who owns PostgreSQL.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class DarlingStoreMeasureReadinessLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Measure_ReadsMostlyEmptyAsRewrite_ThenVacuumCanTruncate_AndRefusesWhatItShould()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* A live row at block 0 and the last hundred rows at the end of the file, everything between deleted. */
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_measure (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_measure SELECT g, repeat('x', 200) FROM generate_series(1, 3000) AS g;
DELETE FROM collect.zz_measure WHERE id > 1 AND id < 2900;
CREATE VIEW collect.zz_measure_view AS SELECT id FROM collect.zz_measure;", ct);

        var first = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "collect.zz_measure", ct);
        Assert.Equal(DarlingStoreTableMeasureReader.StoreTableMeasureOutcome.Measured, first.Outcome);
        var mostlyEmpty = first.Measure!;
        Assert.True(mostlyEmpty.EmptyShare > 0.8, $"share {mostlyEmpty.EmptyShare}");
        Assert.Equal(102, mostlyEmpty.Rows);
        Assert.Equal(DarlingStoreTableMeasureReader.VerdictRewriteNeeded, mostlyEmpty.VerdictKind);
        Assert.Contains("only a rewrite", mostlyEmpty.Verdict, StringComparison.Ordinal);
        Assert.Equal(mostlyEmpty.TotalPages - 1, mostlyEmpty.LastFileBlock);
        Assert.Equal(mostlyEmpty.TotalPages, mostlyEmpty.PagesWithRows + mostlyEmpty.EmptyPages);

        /* Delete the tail rows and vacuum without truncating: the empty pages now all sit after the last used block. */
        await ExecAsync(connection, "DELETE FROM collect.zz_measure WHERE id >= 2900", ct);
        await ExecAsync(connection, "VACUUM (TRUNCATE false) collect.zz_measure", ct);
        var second = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "collect.zz_measure", ct);
        var truncatable = second.Measure!;
        Assert.Equal(DarlingStoreTableMeasureReader.VerdictVacuumCanTruncate, truncatable.VerdictKind);
        Assert.Contains("VACUUM can truncate", truncatable.Verdict, StringComparison.Ordinal);
        Assert.Equal(0, truncatable.LastUsedBlock);
        Assert.Equal(truncatable.TotalPages - 1, truncatable.TrailingEmptyPages);

        /* The refusals. Each is a sentence, none touches the table. */
        foreach (var name in new[]
        {
            "pg_catalog.pg_class",
            "collect.zz_measure_view",
            "collect.zz_measure; DROP TABLE collect.zz_measure",
            "collect.zz_measure\"; DROP TABLE collect.zz_measure; --",
            "collect.zz_measure WHERE 1=1",
            "collect.no_such_table",
            "a.b.c.d",
            "",
        })
        {
            var refused = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, name, ct);
            Assert.True(refused.Outcome == DarlingStoreTableMeasureReader.StoreTableMeasureOutcome.Refused, $"'{name}' was not refused: {refused.Outcome}");
            Assert.False(string.IsNullOrWhiteSpace(refused.Sentence));
        }

        var outside = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "pg_catalog.pg_class", ct);
        Assert.Contains("not a product table", outside.Sentence, StringComparison.Ordinal);
        var view = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "collect.zz_measure_view", ct);
        Assert.Contains("a view", view.Sentence, StringComparison.Ordinal);
        var parent = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "collect.query_store_interval_wide", ct);
        Assert.Equal(DarlingStoreTableMeasureReader.StoreTableMeasureOutcome.Refused, parent.Outcome);
        Assert.Contains("partitioned table", parent.Sentence, StringComparison.Ordinal);

        /* The injection attempts never ran: the table is still there and still measures. */
        var again = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "collect.zz_measure", ct);
        Assert.Equal(DarlingStoreTableMeasureReader.StoreTableMeasureOutcome.Measured, again.Outcome);

        /* The tool: a refusal is an invalid envelope, a measurement carries the verdict first. */
        using var refusalDoc = JsonDocument.Parse(await DarlingMcpStoreTableMeasureTools.MeasureStoreTable(dataSource, "pg_catalog.pg_class", ct));
        Assert.Equal("invalid", refusalDoc.RootElement.GetProperty("status").GetString());
        using var measureDoc = JsonDocument.Parse(await DarlingMcpStoreTableMeasureTools.MeasureStoreTable(dataSource, "collect.zz_measure", ct));
        Assert.Contains("VACUUM can truncate", measureDoc.RootElement.GetProperty("verdict").GetString(), StringComparison.Ordinal);
        Assert.Equal(truncatable.TotalPages, measureDoc.RootElement.GetProperty("total_pages").GetInt64());
    }

    [Fact]
    public async Task Readiness_AHeldStrongLock_IsNotSafe_AndNamesTheTable_AndReleasingItClearsIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var setup = new NpgsqlConnection(scratch.ConnectionString);
        await setup.OpenAsync(ct);
        await ExecAsync(setup, "CREATE TABLE collect.zz_locked (id integer)", ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var holder = new NpgsqlConnection(scratch.ConnectionString + ";Application Name=zz-maintenance-holder"))
        {
            await holder.OpenAsync(ct);
            await using var transaction = await holder.BeginTransactionAsync(ct);
            await ExecAsync(holder, "LOCK TABLE collect.zz_locked IN ACCESS EXCLUSIVE MODE", ct);

            var blocked = await DarlingStoreMaintenanceReadinessReader.GetAsync(dataSource, cancellationToken: ct);
            Assert.False(blocked.Safe);
            Assert.Contains("collect.zz_locked", blocked.Verdict, StringComparison.Ordinal);
            Assert.StartsWith("Not safe for maintenance now", blocked.Verdict, StringComparison.Ordinal);
            var held = Assert.Single(blocked.StrongLocks, l => l.Table == "collect.zz_locked");
            Assert.True(held.Granted);
            Assert.Equal("AccessExclusiveLock", held.Mode);
            Assert.Equal("zz-maintenance-holder", held.ApplicationName);

            using var doc = JsonDocument.Parse(await DarlingMcpStoreMaintenanceReadinessTools.GetStoreMaintenanceReadiness(dataSource, ct));
            Assert.False(doc.RootElement.GetProperty("safe").GetBoolean());
            Assert.Contains("collect.zz_locked", doc.RootElement.GetProperty("verdict").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain("LOCK TABLE", doc.RootElement.GetRawText(), StringComparison.Ordinal);

            await transaction.RollbackAsync(ct);
        }

        var clear = await DarlingStoreMaintenanceReadinessReader.GetAsync(dataSource, cancellationToken: ct);
        Assert.DoesNotContain(clear.Reasons, r => r.Contains("collect.zz_locked", StringComparison.Ordinal));
        Assert.DoesNotContain(clear.StrongLocks, l => l.Table == "collect.zz_locked");
    }

    [Fact]
    public async Task JobCrashRead_ReturnsTheCounters_AndAJobThatNeverCrashedReadsZero()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var read = await DarlingStoreJobCrashReader.ReadAsync(dataSource, ct);
        Assert.True(read.Available, read.UnavailableReason);
        Assert.All(read.Jobs, j => Assert.True(j.ConsecutiveCrashes == 0 && j.TotalCrashes == 0, $"job {j.JobId} crashed"));
        Assert.Empty(read.Crashing);
    }

    [Fact]
    public void WhoOwnsPostgres_IsReadFromServiceState_BothWays()
    {
        var state = new CollectorRuntimeState(null, Microsoft.Extensions.Logging.Abstractions.NullLogger<CollectorRuntimeState>.Instance);
        Assert.Equal(CollectorRuntimeState.StoreOwnership.Unknown, state.Ownership);
        Assert.Contains("not known yet", CollectorRuntimeState.DescribeOwnership(true, state.Ownership), StringComparison.Ordinal);

        state.PublishStoreOwnership(startedByThisProcess: true);
        Assert.Equal(CollectorRuntimeState.StoreOwnership.StartedByThisService, state.Ownership);
        Assert.Contains("stops PostgreSQL with it", CollectorRuntimeState.DescribeOwnership(true, state.Ownership), StringComparison.Ordinal);

        state.PublishStoreOwnership(startedByThisProcess: false);
        Assert.Equal(CollectorRuntimeState.StoreOwnership.AdoptedRunning, state.Ownership);
        Assert.Contains("leaves PostgreSQL running", CollectorRuntimeState.DescribeOwnership(true, state.Ownership), StringComparison.Ordinal);

        Assert.Contains("never stops the store", CollectorRuntimeState.DescribeOwnership(false, state.Ownership), StringComparison.Ordinal);
    }
}
