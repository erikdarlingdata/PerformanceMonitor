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
    /* Whether the database file existed before the current open created it. Set by InitializeCoreAsync before it
       opens the file, and read by the declared index statements (CreateDeclaredIndexAsync). */
    private bool _openedExistingFile;

    /* Test seam: runs on the open's connection right before the CHECKPOINT below. */
    internal Action<DuckDBConnection>? BeforeOpenCheckpointForTests { get; set; }

    /// <summary>
    /// Makes every explicit index hold every row of its table, at each open (startup and the reopen after a fatal
    /// error), before anything can delete or update an indexed row.
    ///
    /// <para><b>duckdb#26106.</b> On the DuckDB releases Lite ships, rows that WAL replay restores when a file opens
    /// after an unclean close (a crash, a killed process, a fatal error) are lost from the ART indexes on their table
    /// by the next automatic or shutdown checkpoint. The rows stay in the table, index lookups miss them, and a later
    /// DELETE over them (the archive step's delete, or a duplicate cleanup) fails with a FATAL error that invalidates
    /// the whole database. Two steps answer it, in this order:</para>
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
    /// checkpoint_threshold=1GB sends every such commit to the WAL. Nothing else writes to the file between a DROP
    /// and its CREATE. The open holds the write lock, which every connection outside a collection takes. At startup
    /// no collection has started yet, and a reopen after a fatal error also holds the collection gate, which waits
    /// for running collections and keeps new ones out (<see cref="ReopenAfterFatalErrorAsync"/>).</para>
    ///
    /// <para>The rebuild covers every explicit index in the file, not only the ones Lite declares: an index an
    /// older build created and this one no longer declares can hold the same damage. If a CREATE fails, that index
    /// stays dropped. The schema's CREATE INDEX IF NOT EXISTS statements, which run after this on every open, try
    /// again for an index Lite declares, and if that fails too they log an ERROR and the open carries on
    /// (<see cref="CreateDeclaredIndexAsync"/>). An index Lite no longer declares stays dropped. Either way the
    /// cost is speed only: a missing index cannot be inconsistent, and Lite has no unique index. The ERROR line
    /// carries the index's full definition, so it can be restored by hand.</para>
    ///
    /// <para><b>Failures.</b> When either step fails with an ordinary error, the failure is logged and the open
    /// carries on. A FATAL error is different: it invalidates the database, so the open cannot carry on. The step
    /// then throws an error that names it, so the start fails with that message instead of the next statement's
    /// "database has been invalidated", and a reopen counts it as a failed attempt.</para>
    ///
    /// <para><b>Cost.</b> Both steps run on every open, a clean one included, because damage from an earlier
    /// session cannot be told from a healthy index without reading it. The size-triggered archive and reset at
    /// 512 MB (<c>CollectionBackgroundService.ArchiveSizeThresholdMb</c>) keeps the file near that size, and the
    /// rebuild measured 622 to 637 ms for 45 indexes on a 402 MB copy of a field database, so it stays at about a
    /// second at most.</para>
    ///
    /// <para>Remove both steps when Lite ships a DuckDB release that fixes duckdb#26106 on both the shutdown and
    /// the automatic checkpoint.</para>
    /// </summary>
    private async Task CheckpointAndRebuildIndexesAsync(DuckDBConnection connection)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            BeforeOpenCheckpointForTests?.Invoke(connection);
            await ExecuteNonQueryAsync(connection, "CHECKPOINT");
        }
        catch (Exception ex) when (IsDatabaseInvalidated(ex))
        {
            throw new InvalidOperationException(
                $"The CHECKPOINT Lite runs right after opening {_databasePath} failed with a fatal error, so the database cannot be used: {ex.Message}",
                ex);
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
        catch (Exception ex) when (IsDatabaseInvalidated(ex))
        {
            throw new InvalidOperationException(
                $"Rebuilding the indexes on {_databasePath} after the open checkpoint failed with a fatal error, so the database cannot be used: {ex.Message}",
                ex);
        }
        catch (Exception ex) when (dropped is { } missing)
        {
            _logger?.LogError(ex,
                "Rebuilt {Rebuilt} of {Count} indexes on {Path}, then dropped {Index} and could not create it again. "
                + "If Lite declares it, its schema statements try again later in this open; otherwise it stays dropped, "
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

    /// <summary>
    /// Runs one of the schema's CREATE INDEX IF NOT EXISTS statements. On a fresh file a failure throws, as it
    /// always did: a declared index that cannot be built on an empty table is a bug for the tests to catch. On an
    /// existing file a failure logs one ERROR and the start carries on, so the next start tries again (the #4727
    /// pattern). Before the repair above, these statements found every index in place and did nothing. Now an index
    /// the repair dropped and could not create again reaches them, and a lasting cause, such as an index too large
    /// to build within the memory limit, would otherwise stop every start and every reopen attempt here. A FATAL
    /// error still throws, because the database is invalidated and the start cannot carry on.
    ///
    /// <para><paramref name="existingFile"/> is <see cref="_openedExistingFile"/>: whether the file existed before
    /// the open, not whether a schema version was read, since a failed version read reads as 0.</para>
    /// </summary>
    private async Task CreateDeclaredIndexAsync(DuckDBConnection connection, string statement, bool existingFile)
    {
        if (!existingFile)
        {
            await ExecuteNonQueryAsync(connection, statement);
            return;
        }

        try
        {
            /* Not ExecuteNonQueryAsync: that helper logs its own Error before it rethrows, and a failure here logs
               exactly one. */
            using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex) when (IsDatabaseInvalidated(ex))
        {
            throw new InvalidOperationException(
                $"Creating a declared index on {_databasePath} failed with a fatal error, so the database cannot be used: {statement.Trim()}",
                ex);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex,
                "Could not create a declared index on {Path}. The start continues without it, which costs only speed, "
                + "and the next start tries again: {Statement}",
                _databasePath, statement.Trim());
        }
    }

    private static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
