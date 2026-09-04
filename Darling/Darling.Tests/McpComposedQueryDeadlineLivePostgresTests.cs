/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of the composed-query deadline (#2874, group A): the resolver reads the operator's ACTUAL
/// <c>config.config_service.compose_statement_timeout_seconds</c>, and it SEES A CHANGE to it.
///
/// <para><b>Why this has to be a live test rather than another band assertion.</b> The defect it guards is
/// not a wrong number — it is a right number read at the wrong time.
/// <c>compose_statement_timeout_seconds</c> is hot-swappable: #2918 has a control-plane reload re-assert it
/// onto the viewer/mcp roles with no restart. A deadline captured at host start, or memoised per data source
/// the way <c>ComposeStoreAvailability.GetRollupsAsync</c> in the very same method deliberately is, compiles,
/// passes every band assertion, and is simply wrong after the first config change. Nothing that reads only
/// source text or only a constant can tell the two implementations apart. The structural half is
/// <see cref="McpReadCommandTimeoutTests.TheComposedQueryRunner_ResolvesItsDeadlinePerRun"/>; this is the
/// half that proves the value actually moves.</para>
///
/// <para><b>The unseeded case is the one that was found by running this rather than reasoning about it.</b>
/// <see cref="PgMigrations.MigrateAsync"/> creates <c>config_service</c>; the ROW comes from
/// <c>StoreConfigProvider.SeedIfEmptyAsync</c>, which is a separate act. So on a migrated-but-unseeded store
/// the read returns no rows at all, and the resolver has to land on its fallback rather than throw or return
/// zero — pinned below, because "brand new store, first panel run" is a real state and not an edge case.</para>
/// </summary>
/*  #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here MUTATES
    config.config_service.compose_statement_timeout_seconds, and one asserts the behaviour of a store whose
    row is NOT seeded, so this cannot share the collection's long-lived database with sixty other classes —
    it reaches DARLING_TEST_PG only to CREATE and DROP a scratch database of its own, exactly as
    DarlingAlertTuningKnobsTests and QueryStoreCorrectedRollupLiveTests do. Placed immediately above the
    declaration on purpose: LivePostgresCollectionHygieneTests only reads the 25 lines above it, and this
    marker started 26 lines up, above the doc comment, where it was invisible to the very rule it answers.  */
public sealed class McpComposedQueryDeadlineLivePostgresTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the compose-deadline round-trip "
        + "(the test mints its own scratch database).";

    /// <summary>
    /// The change-seeing proof. Two DIFFERENT in-band values, neither of them the fallback, resolved on the
    /// SAME data source: a capture-at-start or cache-per-data-source implementation returns the first value
    /// twice and fails the second assertion.
    /// </summary>
    [Fact]
    public async Task TheResolver_ReadsTheConfiguredValue_AndSeesItChange()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var postgres = await SeededStoreAsync(scratch, ct);

        await WriteColumnAsync(postgres, 47, ct);
        var first = await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, ct);
        Assert.Equal(47, first);

        await WriteColumnAsync(postgres, 132, ct);
        var second = await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, ct);
        Assert.Equal(132, second);

        Assert.NotEqual(first, second);
    }

    /// <summary>
    /// A hand-edited row outside the clamp cannot widen this deadline, exactly as it cannot widen the role
    /// GUC — the resolver goes through the store's own clamp rather than trusting the column. Written past
    /// the ceiling directly, because nothing stops a DBA, or a backup restored from a build whose ceiling
    /// differed, putting an out-of-range value in the table.
    /// </summary>
    [Theory]
    [InlineData(6000, StoreConfigProvider.MaxComposeStatementTimeoutSeconds)]
    [InlineData(1, StoreConfigProvider.MinComposeStatementTimeoutSeconds)]
    [InlineData(0, McpCommandDeadlines.ComposedQueryFallbackSeconds)]
    public async Task TheResolver_ClampsAnOutOfRangeRow(int written, int expected)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var postgres = await SeededStoreAsync(scratch, ct);

        await WriteColumnAsync(postgres, written, ct);

        Assert.Equal(expected, await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, ct));
    }

    /// <summary>
    /// A migrated store whose config row was never seeded — the state a brand-new store is in before
    /// <c>SeedIfEmptyAsync</c> runs. The read returns NO ROWS, so the resolver must land on its fallback
    /// rather than throw or hand a zero to <c>CommandTimeout</c>, where zero means NO LIMIT and would be the
    /// worst possible reading of "I could not find out".
    /// </summary>
    [Fact]
    public async Task TheResolver_FallsBackOnAMigratedButUnseededStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var postgres = await MigratedStoreAsync(scratch, ct);

        /* The precondition IS the scenario: migrated, so the table exists; unseeded, so the row does not. */
        await using (var probe = postgres.CreateCommand("SELECT count(*) FROM config.config_service"))
        {
            probe.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            Assert.Equal(0L, await probe.ExecuteScalarAsync(ct));
        }

        Assert.Equal(
            McpCommandDeadlines.ComposedQueryFallbackSeconds,
            await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, ct));
    }

    /// <summary>
    /// A store with no <c>config_service</c> table at all — a pre-V78 store, or one whose schema the
    /// resolver cannot reach. The read raises <c>42P01</c>, and the resolver's catch has to turn that into
    /// the fallback: the deadline-setter must not be the one command on this surface that throws.
    /// </summary>
    [Fact]
    public async Task TheResolver_FallsBackWhenTheTableIsAbsent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Not migrated at all, so config.config_service does not exist. */
        Assert.Equal(
            McpCommandDeadlines.ComposedQueryFallbackSeconds,
            await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, ct));
    }

    /// <summary>
    /// Npgsql's undocumented default, asserted on a real command object so the premise of this whole group
    /// is measured rather than quoted: an unset <c>CommandTimeout</c> reads back as 30 and looks deliberate.
    /// </summary>
    [Fact]
    public async Task AnUnsetCommandTimeout_ReadsBackAsNpgsqlsUndocumentedThirtySeconds()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var command = postgres.CreateCommand("SELECT 1");

        Assert.Equal(30, command.CommandTimeout);

        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        Assert.Equal(McpCommandDeadlines.ReadSeconds, command.CommandTimeout);
        Assert.Equal(1, await command.ExecuteScalarAsync(ct));
    }

    /// <summary>A scratch store at the current schema version, row NOT seeded.</summary>
    private static async Task<NpgsqlDataSource> MigratedStoreAsync(ScratchPostgres scratch, CancellationToken cancellationToken)
    {
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(cancellationToken);
            await PgMigrations.MigrateAsync(connection, cancellationToken);
        }

        return NpgsqlDataSource.Create(scratch.ConnectionString);
    }

    /// <summary>A scratch store at the current schema version with its config row seeded by the product's
    /// own <c>SeedIfEmptyAsync</c>, so the column starts at whatever the shipped default is rather than at a
    /// value this test chose.</summary>
    private static async Task<NpgsqlDataSource> SeededStoreAsync(ScratchPostgres scratch, CancellationToken cancellationToken)
    {
        var postgres = await MigratedStoreAsync(scratch, cancellationToken);
        await new StoreConfigProvider(postgres).SeedIfEmptyAsync(new DarlingConfig(), cancellationToken);

        Assert.Equal(
            McpCommandDeadlines.ComposedQueryFallbackSeconds,
            await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, cancellationToken));

        return postgres;
    }

    private static async Task WriteColumnAsync(NpgsqlDataSource postgres, int seconds, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(
            "UPDATE config.config_service SET compose_statement_timeout_seconds = $1 WHERE id = 1");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = seconds });

        Assert.Equal(1, await command.ExecuteNonQueryAsync(cancellationToken));
    }
}
