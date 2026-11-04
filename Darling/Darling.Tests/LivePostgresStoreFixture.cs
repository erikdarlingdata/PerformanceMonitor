/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Establishes the shared <c>DARLING_TEST_PG</c> store ONCE, before the first class in the
/// <c>live-postgres</c> collection runs (#1862). Wired by <see cref="LivePostgresCollection"/>.
///
/// <para><b>The bug this closes.</b> Until now "the store is established" was an EMERGENT property of test
/// order: every live class did its own <see cref="PgMigrations.MigrateAsync"/>, and a class needing
/// TimescaleDB did its own <see cref="TimescaleSupport.TryEnableAsync"/> — so whether the extension existed
/// when a given test ran depended entirely on which class xUnit happened to schedule first. Both are
/// PERSISTENT, database-level changes, so the FIRST class to make them silently established the store for
/// every class after it. <c>PayloadDimensionLiveTests.DimensionGc_DefersWhenAFactFloorIsUnmeasurable_…</c>
/// reads <c>timescaledb_information.continuous_aggregates</c> without enabling the extension itself; run
/// first against a fresh database it died 3-5ms in with <c>42P01</c>, and run after any of its three
/// siblings (which DO enable) it passed. Same test, same code, opposite outcome.</para>
///
/// <para><b>Why that shape is expensive out of proportion to the bug.</b> The failure MOVES. It lands on
/// whichever class drew the short straw this run, so it reads as "the change under test broke something it
/// never touched" and gets re-run rather than fixed — the same cost <see cref="ViewerTimeStaticsCollection"/>
/// and <see cref="LivePostgresCollectionHygieneTests"/> were written to stop. And CI is no help: it builds a
/// throwaway cluster per run, so a green <c>darling-pg</c> job means the scheduling lottery came up good, not
/// that the suite is order-independent.</para>
///
/// <para><b>Why a collection fixture rather than fixing the one test.</b> Adding the missing
/// <c>TryEnableAsync</c> call to that one method would turn this run green and leave the defect in place: the
/// next live class to read a Timescale catalog, or to assume a migrated store, re-opens it, and the next
/// person pays the same diagnosis. xUnit constructs a collection fixture and awaits its
/// <see cref="IAsyncLifetime.InitializeAsync"/> before ANY class in the collection runs, which is exactly the
/// ordering guarantee that was missing. Classes do not need to inject it — the sixty-odd existing
/// <c>[Collection("live-postgres")]</c> classes are unchanged and simply find an established store.</para>
///
/// <para><b>Migrate BEFORE enabling the extension, and that order is load-bearing.</b> It mirrors what the
/// service does on every start (<c>DarlingWorker</c>: migrate, then <c>TryEnableAsync</c>), and the V23
/// migration branches on whether the extension exists — <c>IF EXISTS (SELECT 1 FROM pg_extension WHERE
/// extname = 'timescaledb')</c>. Enabling first would make V23 convert <c>collection_log</c> to a hypertable
/// during migration on a fresh store, which is the UPGRADE path, not the fresh-store path this fixture is
/// standing in for. <c>TimescaleSupportTests</c> reads the fresh-store premise directly ("on a store whose
/// migrations ran BEFORE CREATE EXTENSION (this shared test database, and any fresh managed store) V23's
/// guard skips"), so a reordering here would quietly retire the coverage of the authoritative runtime
/// conversion path.</para>
///
/// <para><b>What it deliberately does NOT do:</b> hypertable conversion, continuous aggregates, retention
/// policies. Those are what the live classes are TESTING — several create and snapshot-restore aggregates,
/// and <c>TimescaleSupportTests</c> asserts on the un-converted starting state. Establishing them here would
/// trade an ordering bug for a fixture that silently answers the questions the tests are asking. Migrate plus
/// <c>CREATE EXTENSION</c> is the whole of what every live class may ASSUME; everything past it stays the
/// test's own business.</para>
/// </summary>
public sealed class LivePostgresStoreFixture : IAsyncLifetime
{
    /// <summary>
    /// True once the store has been migrated and the extension attempted — i.e. this fixture actually ran
    /// against a real store. False when <c>DARLING_TEST_PG</c> is unset, which is the normal ungated run.
    /// </summary>
    public bool Established { get; private set; }

    /// <summary>
    /// Whether <c>CREATE EXTENSION timescaledb</c> succeeded. False on a plain-PostgreSQL rig, which is a
    /// supported configuration — the live classes that require TimescaleDB assert on it themselves.
    /// </summary>
    public bool TimescaleAvailable { get; private set; }

    /// <summary>The store this fixture established, or null when the suite is running ungated.</summary>
    public string? ConnectionString { get; private set; }

    /// <summary>
    /// Every relation in <c>collect</c> as the run began. The end-of-collection residue check (#1873) diffs
    /// against this rather than against a hard-coded list, so it stays correct on a fresh CI database and on
    /// an operator's long-lived store that legitimately already carries aggregates.
    /// </summary>
    private string[] _baselineRelations = [];

    public async ValueTask InitializeAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(connectionString))
        {
            /* Ungated run: every live test skips itself, so there is nothing to establish. Deliberately
               silent rather than throwing — the gate is the env var, and it lives on the tests. */
            return;
        }

        /* No cancellation token: a collection fixture initializes outside any test, so there is no
           TestContext.Current.CancellationToken to thread. A migration that hangs is a broken rig, and the
           runner's own timeout is the backstop. */
        var cancellationToken = CancellationToken.None;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        /* Both calls are idempotent, so this stays a no-op on a store an earlier run already established —
           and the per-class MigrateAsync calls the live classes still make stay correct and cost nothing.
           A THROW here is the right outcome, not a swallow: it fails every test in the collection with the
           real reason, which is strictly better than the moving 42P01 this replaces. */
        await PgMigrations.MigrateAsync(connection, cancellationToken);

        /* #1922: on its OWN connection, because a CREATE EXTENSION that finds the library unpreloaded kills
           the backend rather than merely erroring. TryEnableAsync then correctly returns false while the
           connection it was handed is dead, and the three calls below would fail with "Connection is not
           open" pointing at RelationsAsync — naming neither the cause nor the file that caused it, which is
           how this presented and what it cost to diagnose. */
        TimescaleAvailable = await LiveTimescaleProbe.TryEnableAsync(connectionString, cancellationToken);

        /* The other relation no migration creates and the SERVICE establishes at runtime (DarlingWorker's
           maintenance sweep calls exactly this), so it belongs with CREATE EXTENSION rather than with the
           hypertables and aggregates the fixture deliberately leaves to the tests. Establishing it here is
           what keeps the residue baseline below honest: DarlingModuleMapTests creates it through the product
           and correctly deletes only its ROWS, so a run that started without it ended one relation richer and
           the #1873 check read product infrastructure as test debris — which it did, on the first full run
           after the check landed. CREATE TABLE IF NOT EXISTS, so the test's own EnsureTableAsync call still
           returns true and still covers the path. */
        _ = await DarlingModuleMap.EnsureTableAsync(connection, null, cancellationToken);

        /* DIAG7415 (diagnostic only, never merged): log every CREATE EXTENSION in this database. Created BEFORE the
           baseline so the log table is not residue. */
        try
        {
            await using var diag = new NpgsqlCommand(@"
CREATE TABLE IF NOT EXISTS public.diag7415_log (id serial PRIMARY KEY, logged_at timestamptz NOT NULL DEFAULT clock_timestamp(), pid int, application_name text, tag text, query text);
CREATE OR REPLACE FUNCTION public.diag7415_fn() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $f$
BEGIN
  INSERT INTO public.diag7415_log (logged_at, pid, application_name, tag, query)
  VALUES (clock_timestamp(), pg_backend_pid(), current_setting('application_name'), tg_tag, current_query());
END $f$;
DROP EVENT TRIGGER IF EXISTS diag7415_trg;
CREATE EVENT TRIGGER diag7415_trg ON ddl_command_end WHEN TAG IN ('CREATE EXTENSION') EXECUTE FUNCTION public.diag7415_fn();", connection);
            await diag.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("DIAG7415 setup failed: " + ex.Message);
        }

        /* AFTER establishing: the baseline has to describe the store the tests will actually see, and
           migration plus the runtime setup above is what creates everything in it. */
        _baselineRelations = await RelationsAsync(connection, cancellationToken);

        ConnectionString = connectionString;
        Established = true;
    }

    /// <summary>
    /// The residue check (#1873): every relation the run created in <c>collect</c> and did not remove.
    ///
    /// <para><b>Why here and not in the tests.</b> A live class that fails to clean up after itself cannot
    /// report it from its own <c>finally</c> without lying about its result — a throw from a finally replaces
    /// the body's in-flight exception, which is the masking #1794 was filed for. So the cleanups record
    /// residue to <see cref="LiveCleanupBatch.Ledger"/> and stay silent, and the accusation is made HERE,
    /// after the last test in the collection has reported, where there is no result left to poison. Verified
    /// against xUnit v3 3.2.2: a throw from a collection fixture's <c>DisposeAsync</c> is reported as
    /// <c>[Test Collection Cleanup Failure (live-postgres)]</c> and exits non-zero — the tests themselves
    /// still pass, which is the correct attribution, since the run is what is broken, not any one of them.</para>
    ///
    /// <para><b>What it compares.</b> Relations, not just continuous aggregates. An aggregate is the residue
    /// that motivated the ticket, but a leaked throwaway hypertable or fallback view is the same debris by a
    /// different name. Hypertable CONVERSIONS are deliberately invisible to it: converting adds no relation to
    /// <c>collect</c> (chunks live in <c>_timescaledb_internal</c>), and several classes convert on their way
    /// past and never convert back — so the diff sees what tests OWN, not what they merely reshaped.</para>
    ///
    /// <para>The ledger is reported alongside, because the catalog knows WHAT leaked and only the ledger knows
    /// WHICH TEST could not remove it. Either one alone sends the next reader to the wrong place.</para>
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        var ledger = LiveCleanupBatch.Ledger;

        if (!Established || ConnectionString is null)
        {
            /* Ungated run: nothing was established, so nothing can have leaked. */
            return;
        }

        string[] leaked;
        await using (var connection = new NpgsqlConnection(ConnectionString))
        {
            await connection.OpenAsync(CancellationToken.None);
            leaked = [.. (await RelationsAsync(connection, CancellationToken.None))
                .Except(_baselineRelations, StringComparer.Ordinal)
                .OrderBy(name => name, StringComparer.Ordinal)];
        }

        var report = BuildResidueReport(leaked, ledger);
        if (report is not null)
        {
            throw new LiveStoreResidueException(report);
        }
    }

    /// <summary>
    /// The residue verdict and its wording, separated from the store so it can be pinned without one
    /// (<see cref="LiveStoreResidueReportTests"/>). Returns null when the run left nothing behind.
    ///
    /// <para>Both inputs can accuse alone. Leaked relations with an empty ledger means something creates
    /// objects and never tries to remove them — a different defect from a losing race, and the report says so
    /// rather than leaving the reader to notice the absence. A ledger entry with nothing leaked is still a
    /// failure: an armed retention policy that could not be removed leaves no relation behind and will drop
    /// another test's chunks on its own schedule.</para>
    /// </summary>
    internal static string? BuildResidueReport(IReadOnlyList<string> leaked, IReadOnlyList<string> ledger)
    {
        if (leaked.Count == 0 && ledger.Count == 0)
        {
            return null;
        }

        var report = new StringBuilder();
        report.AppendLine(
            "The live-postgres suite left the shared DARLING_TEST_PG store dirtier than it found it (#1873).");
        report.AppendLine(
            "On a reused database this residue is inherited by every later run: a surviving continuous "
            + "aggregate changes compose's tier routing, feeds the #1784 coverage gate, and makes "
            + "EnsureBaselineFallbackViewsAsync a no-op for whatever it shadows.");

        if (leaked.Count > 0)
        {
            report.AppendLine();
            report.AppendLine(CultureInfo.InvariantCulture, $"Relations the run created and did not remove ({leaked.Count}):");
            foreach (var relation in leaked)
            {
                report.AppendLine(CultureInfo.InvariantCulture, $"  - {relation}");
            }
        }

        if (ledger.Count > 0)
        {
            report.AppendLine();
            report.AppendLine(CultureInfo.InvariantCulture, $"Cleanups that gave up, and the test each belongs to ({ledger.Count}):");
            foreach (var entry in ledger)
            {
                report.AppendLine(CultureInfo.InvariantCulture, $"  - {entry}");
            }
        }
        else if (leaked.Count > 0)
        {
            report.AppendLine();
            report.AppendLine(
                "No cleanup reported giving up, so nothing above was dropped by a losing race — some test "
                + "creates these and never tries to remove them at all.");
        }

        return report.ToString();
    }

    private static async Task<string[]> RelationsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        /* Ordinary relations, views and materialized views in the schemas a test can leave something in.
           `public` earns its place: DarlingSecuritySplitLiveTests CREATEs its compressed hypertable there
           before moving it to collect, so a failure between the two would strand it somewhere a
           collect-only sweep cannot see. Chunks are excluded for free — they live in _timescaledb_internal,
           which is the extension's business and not a schema tests write to. */
        using var command = new NpgsqlCommand(@"
SELECT n.nspname || '.' || c.relname
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN ('collect', 'config', 'public')
AND   c.relkind IN ('r', 'p', 'v', 'm', 'f')", connection);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var names = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        return [.. names];
    }
}

/// <summary>
/// Thrown from <see cref="LivePostgresStoreFixture.DisposeAsync"/> when the live suite leaked shared-store
/// state (#1873). Its own type, so the failure reads as what it is in a trx or a detailed log rather than as
/// a generic <c>InvalidOperationException</c> from a teardown path.
/// </summary>
public sealed class LiveStoreResidueException : Exception
{
    public LiveStoreResidueException()
    {
    }

    public LiveStoreResidueException(string message)
        : base(message)
    {
    }

    public LiveStoreResidueException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
