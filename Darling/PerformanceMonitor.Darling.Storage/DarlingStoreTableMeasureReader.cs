/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// MEASURES one store table's empty space (#8787 part D2): how many of the file's pages hold no row at all, how the used
/// pages fill, and whether a VACUUM could cut the file shorter. It is the measured figure that sits beside the ESTIMATE in
/// <see cref="DarlingStoreTableHealthReader"/>, and it needs no extension: a scan of the table's <c>ctid</c> column only
/// (no column is detoasted, no heap tuple is returned to the client), grouped by block number. On a field store it took
/// 0.6 to 0.9 seconds for 250,000 pages. That is cheap for one table and wrong to run over every table, so it is on demand for ONE
/// table per call and never fanned out. The MCP tool <c>measure_store_table</c> and its web read wrap this class, and the
/// Viewer calls it directly.
///
/// <para><b>Which tables are accepted.</b> The caller's text is never spliced into SQL. It is resolved with
/// <c>to_regclass</c>, the resolved relation's schema and kind are checked, and the statement is built from the catalog's own
/// <c>quote_ident</c> output. Accepted: an ordinary table (<c>relkind r</c>, a partition leaf included) in a product schema
/// (<see cref="AllowedSchemas"/>), or a chunk of a product hypertable. Refused with a sentence, never an exception: a name that
/// does not resolve, a relation outside those schemas, a view or any other kind, a partitioned parent (the sentence names its
/// largest leaves), a hypertable (the sentence names its largest chunks), and a heap larger than
/// <see cref="MaxHeapBytes"/>.</para>
///
/// <para><b>The bound.</b> The scan runs under a server-side <c>statement_timeout</c> of <see cref="TimeoutSeconds"/> seconds,
/// set for that transaction only. A trip comes back as <see cref="StoreTableMeasureOutcome.TimedOut"/> with a sentence that says
/// so. Nothing is written, and the read holds only an <c>ACCESS SHARE</c> lock.</para>
///
/// <para><b>Reading the numbers.</b> <see cref="StoreTableMeasure.EmptyPages"/> counts pages with no live row, so a page holding
/// only dead tuples counts as empty (a VACUUM would free it). <see cref="StoreTableMeasure.LastUsedBlock"/> against
/// <see cref="StoreTableMeasure.LastFileBlock"/> is what decides the verdict: VACUUM returns only the empty pages AFTER the last
/// used block (it truncates the tail), so a table whose last page holds one live row cannot shrink however empty the rest is. Only a
/// rewrite (<c>VACUUM FULL</c>, <c>CLUSTER</c> or a copy into a new table) returns that space.</para>
/// </summary>
public static class DarlingStoreTableMeasureReader
{
    /// <summary>The schemas the product creates tables in (<see cref="PgSchemaGenerator.CollectSchema"/> and
    /// <see cref="PgSchemaGenerator.ConfigSchema"/>). A chunk of a hypertable in one of them is accepted too.</summary>
    public static readonly IReadOnlyList<string> AllowedSchemas = new[] { PgSchemaGenerator.CollectSchema, PgSchemaGenerator.ConfigSchema };

    /// <summary>The largest heap this read will scan, 20 GiB. At the field rate (about 2 GiB in under a second) that is under
    /// ten seconds of scanning, so the cap is not about time: it keeps one call from reading most of a very large store's cache
    /// out. A hypertable is measured chunk by chunk, so a large hypertable is never refused by size.</summary>
    public const long MaxHeapBytes = 20L * 1024 * 1024 * 1024;

    /// <summary>The server-side statement timeout for the scan, in seconds. Generous next to the measured cost of a 20 GiB heap and
    /// bounded, so a stalled read cannot hold its connection for good.</summary>
    public const int TimeoutSeconds = 60;

    /// <summary>The empty share at and above which a table is called mostly empty: half, the same line the table health rewrite
    /// candidate flag uses (<see cref="DarlingStoreTableHealthReader.RewriteCandidateMinShare"/>).</summary>
    public const double MostlyEmptyShare = 0.5;

    /// <summary>The longest caller text this read resolves. A longer one is refused before it reaches the database.</summary>
    public const int MaxNameLength = 300;

    /// <summary>The verdict kind for a table whose pages are mostly in use.</summary>
    public const string VerdictDense = "dense";

    /// <summary>The verdict kind for a mostly empty table whose empty pages sit mostly at the end of the file, so VACUUM can cut it shorter.</summary>
    public const string VerdictVacuumCanTruncate = "vacuum_can_truncate";

    /// <summary>The verdict kind for a mostly empty table with live rows near the end of the file, so only a rewrite returns the space.</summary>
    public const string VerdictRewriteNeeded = "rewrite_needed";

    /// <summary>The verdict kind for a table whose file has no pages.</summary>
    public const string VerdictNoPages = "no_pages";

    private const string ResolveColumns = @"
    c.oid,
    n.nspname,
    c.relname,
    c.relkind::text,
    quote_ident(n.nspname) || '.' || quote_ident(c.relname),
    pg_catalog.pg_relation_size(c.oid),
    coalesce(c.reloptions @> ARRAY['vacuum_truncate=false'], false),";

    private const string ResolvePlainSql = @"
SELECT" + ResolveColumns + @"
    false,
    false
FROM (SELECT pg_catalog.to_regclass($1)::oid AS oid) AS r
JOIN pg_catalog.pg_class AS c ON c.oid = r.oid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace";

    private const string ResolveTimescaleSql = @"
SELECT" + ResolveColumns + @"
    EXISTS (SELECT 1 FROM timescaledb_information.hypertables AS h WHERE h.hypertable_schema = n.nspname AND h.hypertable_name = c.relname),
    EXISTS (SELECT 1 FROM timescaledb_information.chunks AS ch
            WHERE ch.chunk_schema = n.nspname AND ch.chunk_name = c.relname AND ch.hypertable_schema = ANY ($2))
FROM (SELECT pg_catalog.to_regclass($1)::oid AS oid) AS r
JOIN pg_catalog.pg_class AS c ON c.oid = r.oid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace";

    private const string LargestChunksSql = @"
SELECT quote_ident(ch.chunk_schema) || '.' || quote_ident(ch.chunk_name), pg_catalog.pg_relation_size(c.oid)
FROM timescaledb_information.chunks AS ch
JOIN pg_catalog.pg_namespace AS n ON n.nspname = ch.chunk_schema
JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = ch.chunk_name
WHERE ch.hypertable_schema = $1 AND ch.hypertable_name = $2
ORDER BY 2 DESC, 1
LIMIT 3";

    private const string LargestLeavesSql = @"
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname), pg_catalog.pg_relation_size(c.oid)
FROM pg_catalog.pg_partition_tree($1::oid) AS t
JOIN pg_catalog.pg_class AS c ON c.oid = t.relid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE t.isleaf
ORDER BY 2 DESC, 1
LIMIT 3";

    /// <summary>The measurement, in two halves around the resolved, quoted table name. <c>ONLY</c> keeps an inheritance child out of the
    /// scan. The page figures come from the file's size, the row figures from the grouped scan.</summary>
    private const string MeasureSqlHead = @"
WITH p AS (
    SELECT (ctid::text::point)[0]::bigint AS blk, count(*) AS n
    FROM ONLY ";

    private const string MeasureSqlTail = @"
    GROUP BY 1
)
SELECT
    count(*) AS pages_with_rows,
    coalesce(sum(n), 0)::bigint AS row_count,
    coalesce(max(blk), -1)::bigint AS last_used_block,
    coalesce(round(avg(n), 2), 0)::float8 AS rows_per_used_page,
    pg_catalog.pg_relation_size($1::oid) / current_setting('block_size')::bigint AS total_pages,
    count(*) FILTER (WHERE n = 1) AS pages_1_row,
    count(*) FILTER (WHERE n BETWEEN 2 AND 4) AS pages_2_to_4_rows,
    count(*) FILTER (WHERE n BETWEEN 5 AND 10) AS pages_5_to_10_rows,
    count(*) FILTER (WHERE n > 10) AS pages_over_10_rows
FROM p";

    /// <summary>What a call came to.</summary>
    public enum StoreTableMeasureOutcome
    {
        /// <summary>The table was measured; <see cref="StoreTableMeasureResult.Measure"/> is set.</summary>
        Measured,

        /// <summary>The table was refused; <see cref="StoreTableMeasureResult.Sentence"/> says why and what to measure instead.</summary>
        Refused,

        /// <summary>The scan passed <see cref="TimeoutSeconds"/>; <see cref="StoreTableMeasureResult.Sentence"/> says so.</summary>
        TimedOut,
    }

    /// <summary>The measured figures for one table. Pages are file pages of <c>block_size</c> bytes.</summary>
    /// <param name="QualifiedName">The resolved, quoted <c>schema.table</c>.</param>
    /// <param name="TotalPages">The pages in the file (its size over the block size).</param>
    /// <param name="PagesWithRows">Pages holding at least one live row.</param>
    /// <param name="EmptyPages">Pages holding no live row (<paramref name="TotalPages"/> minus <paramref name="PagesWithRows"/>, never below zero).</param>
    /// <param name="EmptyShare">Empty pages over total pages, 0 to 1. Zero for a file with no pages.</param>
    /// <param name="Rows">Live rows.</param>
    /// <param name="RowsPerUsedPage">Average live rows on a page that holds any, rounded to two places.</param>
    /// <param name="LastUsedBlock">The block number of the last page holding a live row, minus one when there is none.</param>
    /// <param name="LastFileBlock">The file's last block number (<paramref name="TotalPages"/> minus one).</param>
    /// <param name="TrailingEmptyPages">Pages after the last used block: the most a VACUUM can return.</param>
    /// <param name="PagesWith1Row">Used pages holding exactly one row.</param>
    /// <param name="PagesWith2To4Rows">Used pages holding two to four rows.</param>
    /// <param name="PagesWith5To10Rows">Used pages holding five to ten rows.</param>
    /// <param name="PagesWithOver10Rows">Used pages holding more than ten rows.</param>
    /// <param name="VacuumTruncateDisabled">True when the table's <c>vacuum_truncate</c> storage option is off, which stops VACUUM cutting the tail.</param>
    /// <param name="VerdictKind">One of <see cref="VerdictDense"/>, <see cref="VerdictVacuumCanTruncate"/>, <see cref="VerdictRewriteNeeded"/>, <see cref="VerdictNoPages"/>.</param>
    /// <param name="Verdict">The one-line verdict, a sentence.</param>
    /// <param name="ElapsedMilliseconds">How long the scan took.</param>
    public sealed record StoreTableMeasure(
        string QualifiedName,
        long TotalPages,
        long PagesWithRows,
        long EmptyPages,
        double EmptyShare,
        long Rows,
        double RowsPerUsedPage,
        long LastUsedBlock,
        long LastFileBlock,
        long TrailingEmptyPages,
        long PagesWith1Row,
        long PagesWith2To4Rows,
        long PagesWith5To10Rows,
        long PagesWithOver10Rows,
        bool VacuumTruncateDisabled,
        string VerdictKind,
        string Verdict,
        long ElapsedMilliseconds);

    /// <summary>The result of <see cref="MeasureAsync"/>.</summary>
    /// <param name="Outcome">Measured, refused or timed out.</param>
    /// <param name="Sentence">The refusal or timeout sentence, null when measured.</param>
    /// <param name="Measure">The figures, null unless measured.</param>
    public sealed record StoreTableMeasureResult(StoreTableMeasureOutcome Outcome, string? Sentence, StoreTableMeasure? Measure);

    /// <summary>
    /// Measures one table. <paramref name="table"/> is the caller's text, schema-qualified (<c>collect.store_metrics</c>). Never
    /// throws for a refused or slow table; a store error that is not one of those propagates.
    /// </summary>
    public static async Task<StoreTableMeasureResult> MeasureAsync(NpgsqlDataSource postgres, string? table, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        cancellationToken.ThrowIfCancellationRequested();

        var text = table?.Trim();
        if (string.IsNullOrEmpty(text))
        {
            return Refuse("Name the table to measure as schema.table, for example collect.store_metrics.");
        }

        if (text.Length > MaxNameLength || text.Contains('\0', StringComparison.Ordinal))
        {
            return Refuse($"The table name is not valid: give one schema-qualified name of at most {MaxNameLength} characters, for example collect.store_metrics.");
        }

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        bool timescale;
        await using (var probe = new NpgsqlCommand(DarlingStoreTableHealthReader.TimescaleProbeSql, connection))
        {
            probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            timescale = (bool)(await probe.ExecuteScalarAsync(cancellationToken))!;
        }

        Resolved? resolved;
        try
        {
            resolved = await ResolveAsync(connection, text, timescale, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState is "42601" or "22023" or "42602" or "22021")
        {
            /* to_regclass reads its argument as a qualified name and rejects text that is not one (too many dotted parts,
               an unbalanced quote, an invalid byte sequence). The text never reaches a statement. */
            return Refuse($"'{Clip(text)}' is not a valid table name: give one schema-qualified name, for example collect.store_metrics.");
        }

        if (resolved is null)
        {
            return Refuse($"No table named '{Clip(text)}' was found. Give the schema-qualified name, for example collect.store_metrics.");
        }

        var refusal = await RefusalForAsync(connection, resolved, cancellationToken);
        return refusal is not null ? Refuse(refusal) : await RunAsync(connection, resolved, cancellationToken);
    }

    private sealed record Resolved(
        uint Oid, string Schema, string Table, string Kind, string QualifiedName, long HeapBytes, bool VacuumTruncateOff, bool IsHypertable, bool IsProductChunk);

    private static async Task<Resolved?> ResolveAsync(NpgsqlConnection connection, string text, bool timescale, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(timescale ? ResolveTimescaleSql : ResolvePlainSql, connection)
        {
            CommandTimeout = StorageCommandDeadlines.McpReadSeconds,
        };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = text });
        if (timescale)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = AllowedSchemas.ToArray() });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new Resolved(
            Oid: reader.GetFieldValue<uint>(0),
            Schema: reader.GetString(1),
            Table: reader.GetString(2),
            Kind: reader.GetString(3),
            QualifiedName: reader.GetString(4),
            HeapBytes: reader.GetInt64(5),
            VacuumTruncateOff: reader.GetBoolean(6),
            IsHypertable: reader.GetBoolean(7),
            IsProductChunk: reader.GetBoolean(8));
    }

    /// <summary>The sentence that refuses the resolved relation, or null when it can be measured.</summary>
    private static async Task<string?> RefusalForAsync(NpgsqlConnection connection, Resolved resolved, CancellationToken cancellationToken)
    {
        if (!AllowedSchemas.Contains(resolved.Schema, StringComparer.Ordinal) && !resolved.IsProductChunk)
        {
            return $"{resolved.QualifiedName} is not a product table. This read measures only tables in the {string.Join(" and ", AllowedSchemas)} schemas and the chunks of their hypertables.";
        }

        if (resolved.IsHypertable)
        {
            var chunks = await LargestChunksAsync(connection, resolved, cancellationToken);
            return $"{resolved.QualifiedName} is a hypertable: its rows live in its chunks, so measure a chunk instead. "
                + (chunks.Count == 0 ? "It has no chunks yet." : $"Its largest chunks are {Describe(chunks)}.");
        }

        if (resolved.Kind == "p")
        {
            var leaves = await LargestLeavesAsync(connection, resolved, cancellationToken);
            return $"{resolved.QualifiedName} is a partitioned table: its rows live in its partitions, so measure a partition instead. "
                + (leaves.Count == 0 ? "It has no partitions yet." : $"Its largest partitions are {Describe(leaves)}.");
        }

        if (resolved.Kind != "r")
        {
            return $"{resolved.QualifiedName} is {DescribeKind(resolved.Kind)}, not an ordinary table. This read measures ordinary tables, partitions and hypertable chunks only.";
        }

        if (resolved.HeapBytes > MaxHeapBytes)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"{resolved.QualifiedName} has a heap of {resolved.HeapBytes} bytes, above the {MaxHeapBytes} byte limit for one measurement. The scan reads every row's location, so a table this large is not measured in one call.");
        }

        return null;
    }

    private static async Task<StoreTableMeasureResult> RunAsync(NpgsqlConnection connection, Resolved resolved, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await using (var setTimeout = new NpgsqlCommand("SELECT set_config('statement_timeout', $1, true)", connection, transaction))
            {
                setTimeout.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (TimeoutSeconds * 1000).ToString(CultureInfo.InvariantCulture) });
                await setTimeout.ExecuteNonQueryAsync(cancellationToken);
            }

            /* The name is the catalog's quote_ident output for the relation the caller's text resolved to, never the caller's text. */
            var sql = MeasureSqlHead + resolved.QualifiedName + MeasureSqlTail;
            long pagesWithRows, rows, lastUsed, totalPages, one, twoToFour, fiveToTen, over10;
            double rowsPerPage;
            await using (var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = TimeoutSeconds + 15 })
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = resolved.Oid });
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                pagesWithRows = reader.GetInt64(0);
                rows = reader.GetInt64(1);
                lastUsed = reader.GetInt64(2);
                rowsPerPage = reader.GetDouble(3);
                totalPages = reader.GetInt64(4);
                one = reader.GetInt64(5);
                twoToFour = reader.GetInt64(6);
                fiveToTen = reader.GetInt64(7);
                over10 = reader.GetInt64(8);
            }

            await transaction.CommitAsync(cancellationToken);
            stopwatch.Stop();
            return new StoreTableMeasureResult(StoreTableMeasureOutcome.Measured, null,
                Build(resolved.QualifiedName, totalPages, pagesWithRows, rows, rowsPerPage, lastUsed, one, twoToFour, fiveToTen, over10,
                    stopwatch.ElapsedMilliseconds, resolved.VacuumTruncateOff));
        }
        catch (PostgresException ex) when (ex.SqlState == "57014" && !cancellationToken.IsCancellationRequested)
        {
            return TimedOut(resolved);
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01")
        {
            return Refuse($"{resolved.QualifiedName} no longer exists: it was dropped after it was found.");
        }
        catch (NpgsqlException ex) when (ex.InnerException is TimeoutException && !cancellationToken.IsCancellationRequested)
        {
            return TimedOut(resolved);
        }
    }

    private static StoreTableMeasureResult TimedOut(Resolved resolved) =>
        new(StoreTableMeasureOutcome.TimedOut,
            string.Create(CultureInfo.InvariantCulture,
                $"The measurement of {resolved.QualifiedName} did not finish in {TimeoutSeconds} seconds and was stopped. Nothing was changed. The store is busy or the table is very large: try again when it is quieter, or measure one of its partitions or chunks."),
            null);

    /// <summary>Builds the figures and the verdict from the raw scan. Public so the verdict rule is testable without a table of that shape.</summary>
    public static StoreTableMeasure Build(
        string qualifiedName, long totalPages, long pagesWithRows, long rows, double rowsPerUsedPage, long lastUsedBlock,
        long pages1Row, long pages2To4, long pages5To10, long pagesOver10, long elapsedMilliseconds, bool vacuumTruncateDisabled = false)
    {
        var empty = Math.Max(0, totalPages - pagesWithRows);
        var share = totalPages > 0 ? (double)empty / totalPages : 0.0;
        var lastFile = totalPages - 1;
        var trailing = Math.Max(0, lastFile - lastUsedBlock);
        var (kind, sentence) = Judge(totalPages, empty, share, trailing, lastUsedBlock, lastFile, rowsPerUsedPage, vacuumTruncateDisabled);
        return new StoreTableMeasure(qualifiedName, totalPages, pagesWithRows, empty, share, rows, rowsPerUsedPage, lastUsedBlock, lastFile,
            trailing, pages1Row, pages2To4, pages5To10, pagesOver10, vacuumTruncateDisabled, kind, sentence, elapsedMilliseconds);
    }

    /// <summary>The verdict rule. Mostly empty means at least <see cref="MostlyEmptyShare"/> of the pages hold no row. A mostly empty
    /// table whose empty pages are mostly (half or more) after the last used block can be cut shorter by VACUUM. Otherwise the space is
    /// stuck behind live rows and only a rewrite returns it.</summary>
    private static (string Kind, string Sentence) Judge(
        long totalPages, long empty, double share, long trailing, long lastUsed, long lastFile, double rowsPerUsedPage, bool truncateOff)
    {
        var pct = (share * 100).ToString("0.#", CultureInfo.InvariantCulture);
        if (totalPages == 0)
        {
            return (VerdictNoPages, "The table's file has no pages, so there is no space to return.");
        }

        if (share < MostlyEmptyShare)
        {
            return (VerdictDense, string.Create(CultureInfo.InvariantCulture,
                $"Dense: {pct}% of the file's {totalPages} pages hold no row, with {rowsPerUsedPage} rows per used page. There is little space to return."));
        }

        if (trailing > 0 && trailing * 2 >= empty)
        {
            var off = truncateOff
                ? " The table's vacuum_truncate option is off, so VACUUM will not do it until that option is turned on."
                : string.Empty;
            return (VerdictVacuumCanTruncate, string.Create(CultureInfo.InvariantCulture,
                $"Mostly empty, and VACUUM can truncate the tail: {pct}% of the {totalPages} pages hold no row and {trailing} of the {empty} empty pages are after the last used block ({lastUsed} of {lastFile}).{off}"));
        }

        return (VerdictRewriteNeeded, string.Create(CultureInfo.InvariantCulture,
            $"Mostly empty, and only a rewrite returns the space: {pct}% of the {totalPages} pages hold no row, but live rows reach block {lastUsed} of {lastFile}, so VACUUM can cut off only {trailing} pages."));
    }

    private static StoreTableMeasureResult Refuse(string sentence) => new(StoreTableMeasureOutcome.Refused, sentence, null);

    private static string Clip(string text) => text.Length <= 80 ? text : text[..80] + "...";

    private static string DescribeKind(string relkind) => relkind switch
    {
        "v" => "a view",
        "m" => "a materialized view",
        "f" => "a foreign table",
        "i" or "I" => "an index",
        "S" => "a sequence",
        "c" => "a composite type",
        "t" => "a TOAST table",
        _ => "not an ordinary table",
    };

    private static string Describe(IReadOnlyList<(string Name, long Bytes)> relations) =>
        string.Join(", ", relations.Select(r => string.Create(CultureInfo.InvariantCulture, $"{r.Name} ({r.Bytes} bytes)")));

    private static async Task<List<(string Name, long Bytes)>> LargestChunksAsync(NpgsqlConnection connection, Resolved resolved, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(LargestChunksSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = resolved.Schema });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = resolved.Table });
        return await ReadPairsAsync(command, cancellationToken);
    }

    private static async Task<List<(string Name, long Bytes)>> LargestLeavesAsync(NpgsqlConnection connection, Resolved resolved, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(LargestLeavesSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = resolved.Oid });
        return await ReadPairsAsync(command, cancellationToken);
    }

    private static async Task<List<(string Name, long Bytes)>> ReadPairsAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var pairs = new List<(string, long)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            pairs.Add((reader.GetString(0), reader.GetInt64(1)));
        }

        return pairs;
    }
}
