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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// VACUUM (ANALYZE) over a table until the catalog shows every page of it all-visible (#8204). A plan pin that needs an
/// index-only scan depends on the visibility map: the planner prices that scan by the share of pages the map marks, and
/// a vacuum cannot mark a page while a snapshot older than the rows is open anywhere in the database (an autoanalyze
/// over a catalog the migration churned, TimescaleDB's scheduler starting on a new database). One pass is not enough on
/// its own, so this runs up to <see cref="Passes"/> and asserts the premise exactly: a pin below it is then decided by
/// the indexes and not by whoever held a snapshot a moment ago.
/// </summary>
internal static class LiveVisibilityMap
{
    /// <summary>The most VACUUM passes spent on getting every page marked before the premise fails.</summary>
    public const int Passes = 10;

    /// <summary>The pause before each pass after the first. A small table's VACUUM takes tens of milliseconds, so passes
    /// run back to back would all fall inside one autoanalyze or scheduler snapshot that stays open for a second or two;
    /// with the pause, the ten passes span a few seconds.</summary>
    public static readonly TimeSpan PassPause = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Vacuums <paramref name="table"/> until every page is all-visible, and asserts that it got there. The pages are
    /// summed over the table and everything that inherits from it, so a plain table, a partitioned table and a hypertable
    /// are all read the same way. <paramref name="afterFirstPass"/> runs once, right after the first pass: a test that holds
    /// a snapshot through that pass ends it there.
    /// </summary>
    public static async Task<(long Pages, long AllVisible)> VacuumUntilAllVisibleAsync(
        NpgsqlConnection connection, string table, CancellationToken ct, Func<Task>? afterFirstPass = null)
    {
        long pages = 0;
        long allVisible = -1;
        for (var pass = 0; pass < Passes && allVisible != pages; pass++)
        {
            if (pass > 0)
            {
                await Task.Delay(PassPause, ct);
            }

            await using (var vacuum = new NpgsqlCommand($"VACUUM (ANALYZE) {table}", connection))
            {
                await vacuum.ExecuteNonQueryAsync(ct);
            }

            if (pass == 0 && afterFirstPass is not null)
            {
                await afterFirstPass();
            }

            (pages, allVisible) = await PagesAsync(connection, table, ct);
        }

        Assert.True(pages > 0 && allVisible == pages,
            $"VACUUM left {pages - allVisible} of {pages} pages of {table} not all-visible after {Passes} passes, so an index-only scan of it is priced at random heap fetches");
        return (pages, allVisible);
    }

    /* relpages and relallvisible summed over the table and its inheritance tree: a partition and a hypertable chunk are
       both pg_inherits children, and a parent that holds no rows adds nothing. */
    private static async Task<(long Pages, long AllVisible)> PagesAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
WITH RECURSIVE rels AS (
    SELECT to_regclass($1) AS oid
    UNION ALL
    SELECT i.inhrelid FROM pg_inherits AS i JOIN rels AS r ON i.inhparent = r.oid)
SELECT COALESCE(SUM(c.relpages), 0)::bigint, COALESCE(SUM(c.relallvisible), 0)::bigint
FROM rels JOIN pg_class AS c ON c.oid = rels.oid
WHERE c.relkind = 'r'", connection);
        command.Parameters.AddWithValue(table);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }
}
