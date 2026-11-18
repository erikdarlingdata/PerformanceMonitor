/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8998: <c>docs/darling/store-tables.csv</c> lists every relation in the <c>collect</c> schema, one row
/// each, with what it holds (<c>kind</c>) and whether a copy of the store needs it (<c>export</c>). A tool
/// that copies store data elsewhere reads that list instead of keeping its own, so a relation added without a
/// row would be silently left out of every copy. This class builds a store the way the service does, reads
/// the relations that exist, and fails on any relation with no row and on any row with no relation.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (it runs the service's whole store-object
/// list, which creates hypertables and continuous aggregates the shared store deliberately leaves to the
/// tests), so it cannot race the shared store and is not serialized against the <c>live-postgres</c>
/// collection.</para>
/// </summary>
public sealed class StoreTableInventoryLiveTests
{
    private const string InventoryPath = "docs/darling/store-tables.csv";

    private const string Header = "relation,kind,export,note";

    private static readonly string[] AllowedKinds = ["collected", "derived", "internal"];

    private static readonly string[] AllowedExports = ["yes", "derived", "no"];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The relations in <c>collect</c> that a built store has: ordinary and partitioned tables that are not
    /// themselves a partition, plus views and materialized views (the rollups).
    /// </summary>
    private const string ListRelationsSql = @"
SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'collect'
  AND (c.relkind IN ('v', 'm') OR (c.relkind IN ('r', 'p') AND NOT c.relispartition))
ORDER BY c.relname COLLATE ""C""";

    [Fact]
    public async Task EveryCollectRelationOfABuiltStore_HasARowInTheInventory_AndEveryRowHasARelation()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the store inventory test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.True(timescaleEnabled, "TimescaleDB must be available to build the store the way the service does (hypertables and rollups)");

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
            {
                await stop.ExecuteNonQueryAsync(ct);
            }

            /* The start path's store setup: the service's own list of idempotent store-object steps, in its
               order, run through the same one-step runner both the start path and the hourly pass use. */
            var tally = new DarlingWorker.StoreObjectConvergenceTally();
            foreach (var step in StoreObjectSteps())
            {
                await DarlingWorker.RunStoreObjectConvergenceStepAsync(connection, step, tally, NullLogger.Instance, ct);
            }

            Assert.True(tally.Failed.Count == 0,
                "A store-object step failed while building the store: " + string.Join(", ", tally.Failed));

            /* The one relation the start path creates outside that list. */
            Assert.True(await DarlingModuleMap.EnsureTableAsync(connection, null, ct));

            var built = await ListRelationsAsync(connection, ct);

            var rows = ReadInventory(out var problems);
            Assert.True(problems.Count == 0, "docs/darling/store-tables.csv is not valid:" + Environment.NewLine + string.Join(Environment.NewLine, problems));

            var listed = rows.Select(r => r.Relation).ToHashSet(StringComparer.Ordinal);
            var missingRows = built.Where(r => !listed.Contains(r))
                .Select(r => $"add a row to {InventoryPath} for collect.{r}: kind (collected, derived or internal), export (yes, derived or no)");
            var missingRelations = rows.Where(r => !built.Contains(r.Relation, StringComparer.Ordinal))
                .Select(r => $"{InventoryPath} lists collect.{r.Relation}, but a built store has no such relation: remove the row, or fix the name");

            var failures = missingRows.Concat(missingRelations).ToList();
            Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The CSV on its own: the header, the allowed <c>kind</c> and <c>export</c> values, no duplicate relation,
    /// sorted by relation. Needs no store.
    /// </summary>
    [Fact]
    public void TheInventoryFile_HasTheHeader_AllowedValues_NoDuplicates_AndIsSorted()
    {
        _ = ReadInventory(out var problems);
        Assert.True(problems.Count == 0, "docs/darling/store-tables.csv is not valid:" + Environment.NewLine + string.Join(Environment.NewLine, problems));
    }

    private sealed record InventoryRow(string Relation, string Kind, string Export, string Note);

    private static List<InventoryRow> ReadInventory(out List<string> problems)
    {
        problems = [];
        var rows = new List<InventoryRow>();
        var path = Path.Combine(FindRepoRoot(), InventoryPath);
        if (!File.Exists(path))
        {
            problems.Add($"{InventoryPath} does not exist");
            return rows;
        }

        var lines = File.ReadAllLines(path);
        if (lines.Length == 0 || lines[0] != Header)
        {
            problems.Add($"line 1: the header must be exactly '{Header}'");
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        string? previous = null;
        for (var i = 1; i < lines.Length; i++)
        {
            var line = lines[i];
            var lineNumber = i + 1;
            if (line.Length == 0)
            {
                problems.Add($"line {lineNumber}: blank line");
                continue;
            }

            var parts = line.Split(',');
            if (parts.Length != 4)
            {
                problems.Add($"line {lineNumber}: expected 4 fields (relation,kind,export,note, with no comma inside a field), found {parts.Length}");
                continue;
            }

            var row = new InventoryRow(parts[0], parts[1], parts[2], parts[3]);
            if (row.Relation.Length == 0)
            {
                problems.Add($"line {lineNumber}: the relation is empty");
                continue;
            }

            if (!AllowedKinds.Contains(row.Kind))
            {
                problems.Add($"line {lineNumber} ({row.Relation}): kind '{row.Kind}' is not one of {string.Join(", ", AllowedKinds)}");
            }

            if (!AllowedExports.Contains(row.Export))
            {
                problems.Add($"line {lineNumber} ({row.Relation}): export '{row.Export}' is not one of {string.Join(", ", AllowedExports)}");
            }

            if (!seen.Add(row.Relation))
            {
                problems.Add($"line {lineNumber}: {row.Relation} is listed twice");
            }

            if (previous is not null && string.CompareOrdinal(previous, row.Relation) > 0)
            {
                problems.Add($"line {lineNumber}: {row.Relation} is out of order (rows are sorted by relation, so it belongs before {previous})");
            }

            previous = row.Relation;
            rows.Add(row);
        }

        return rows;
    }

    private static async Task<List<string>> ListRelationsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var relations = new List<string>();
        await using var command = new NpgsqlCommand(ListRelationsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            relations.Add(reader.GetString(0));
        }

        return relations;
    }

    /// <summary>
    /// The service's store-object list, in order. The list is a private field of the worker, so it is read
    /// through reflection: that is what keeps this test building the store with whatever steps the service
    /// runs today, rather than a copy that would drift from it.
    /// </summary>
    private static DarlingWorker.StoreObjectConvergenceStep[] StoreObjectSteps()
    {
        var field = typeof(DarlingWorker).GetField("s_storeObjectConvergence", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        return (DarlingWorker.StoreObjectConvergenceStep[])field!.GetValue(null)!;
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "docs", "darling", "README.md")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not find the repository root from " + AppContext.BaseDirectory);
    }
}
