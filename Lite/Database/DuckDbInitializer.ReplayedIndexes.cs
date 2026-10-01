/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Database;

public partial class DuckDbInitializer
{
    /// <summary>
    /// Makes every explicit index hold every row of its table, at each open (startup and the reopen after a fatal
    /// error), before anything can delete or update an indexed row.
    ///
    /// <para><b>duckdb#26106.</b> On the DuckDB releases Lite ships, rows that WAL replay restores when a file opens
    /// after an unclean close (a crash, a killed process, a fatal error) are lost from the ART indexes on their table
    /// by the next automatic or shutdown checkpoint. The rows stay in the table, index lookups miss them, and a later
    /// DELETE over them (the archive delete, a server removal) fails with a FATAL error that invalidates the whole
    /// database. Two steps answer it, in this order:</para>
    ///
    /// <para>1. An explicit CHECKPOINT right after the open writes the replayed rows' index entries correctly, before
    /// any later checkpoint can lose them. It is the precondition of step 2: when it fails, the file cannot take a
    /// write just now, so no index is dropped.</para>
    ///
    /// <para>2. Then every explicit index is dropped and created again from its own definition, which builds it from
    /// the rows the table holds. That repairs an index an earlier session's checkpoint already damaged.</para>
    ///
    /// <para><b>Each DROP and each CREATE commits on its own. Never put them in one transaction:</b> a transaction
    /// that drops and re-creates an index loaded from the file fails at COMMIT when the commit is written to the
    /// WAL, and DuckDB.NET then dies with a native access violation that no catch can stop. Lite's
    /// checkpoint_threshold=1GB sends every such commit to the WAL. Nothing else uses the file between a DROP and
    /// its CREATE, because the open holds the write lock.</para>
    ///
    /// <para>The rebuild covers every explicit index in the file, not only the ones Lite declares: an index an
    /// older build created and this one no longer declares can hold the same damage. If a CREATE fails, that index
    /// stays dropped. The schema's CREATE INDEX IF NOT EXISTS statements, which run after this on every open, put
    /// back an index Lite declares. An index it no longer declares stays dropped, which costs only speed: a missing
    /// index cannot be inconsistent, and Lite has no unique index. The ERROR line carries the index's full
    /// definition, so it can be restored by hand. A failure of either step is logged and the open carries on.</para>
    ///
    /// <para>Remove both steps when Lite ships a DuckDB release that fixes duckdb#26106 on both the shutdown and
    /// the automatic checkpoint.</para>
    /// </summary>
    private async Task CheckpointAndRebuildIndexesAsync(DuckDBConnection connection)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            await ExecuteNonQueryAsync(connection, "CHECKPOINT");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Could not checkpoint {Path} after opening it, so its indexes were not rebuilt", _databasePath);
            return;
        }

        var indexes = new List<(string Schema, string Name, string Sql)>();
        var rebuilt = 0;

        /* The index dropped and not yet created again, for the ERROR line if its CREATE fails. */
        (string Schema, string Name, string Sql)? dropped = null;

        try
        {
            using (var command = connection.CreateCommand())
            {
                /* Explicit indexes only: a primary key or unique constraint has no definition here and cannot be
                   dropped on its own. */
                command.CommandText = @"
SELECT schema_name, index_name, sql
FROM duckdb_indexes()
WHERE database_name = current_database()
AND   NOT is_primary
AND   sql IS NOT NULL
ORDER BY schema_name, index_name";
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    indexes.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                }
            }

            foreach (var index in indexes)
            {
                /* Two statements and two commits, never one transaction (see the summary). */
                await ExecuteNonQueryAsync(connection, $"DROP INDEX {QuoteIdentifier(index.Schema)}.{QuoteIdentifier(index.Name)}");
                dropped = index;
                await ExecuteNonQueryAsync(connection, index.Sql);
                dropped = null;
                rebuilt++;
            }

            _logger?.LogInformation(
                "Rebuilt {Count} indexes on {Path} in {ElapsedMs} ms after the open checkpoint (duckdb#26106)",
                rebuilt, _databasePath, stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex) when (dropped is { } missing)
        {
            _logger?.LogError(ex,
                "Rebuilt {Rebuilt} of {Count} indexes on {Path}, then dropped {Index} and could not create it again. "
                + "Lite's schema statements put it back during this open if Lite declares it; otherwise it stays dropped, "
                + "which costs only speed. To restore it by hand, run: {Sql}",
                rebuilt, indexes.Count, _databasePath, missing.Name, missing.Sql);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex,
                "Rebuilt {Rebuilt} of {Count} indexes on {Path} before an error; the others are as they were",
                rebuilt, indexes.Count, _databasePath);
        }
    }

    private static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
