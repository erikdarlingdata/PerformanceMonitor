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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// MEASURES one store table's empty space (#8787 part D2): how much of the file's page space the live rows take, how many pages
/// hold no row at all, and whether a VACUUM could cut the file shorter. It is the measured figure that sits beside the ESTIMATE in
/// <see cref="DarlingStoreTableHealthReader"/>, and it needs no extension: one scan of the table, grouped by block number, that
/// counts rows and adds up <c>pg_column_size</c> of every column. <c>pg_column_size</c> of a toasted value is its stored size and
/// does not detoast it, so the TOAST table is never read, and no row is returned to the client. On a field store the
/// row-location part of that scan took 0.6 to 0.9 seconds for 250,000 pages. That is cheap for one table and wrong to run over every
/// table, so it is on demand for ONE table per call and never fanned out. The MCP tool <c>measure_store_table</c> and its web read wrap this class, and the
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
/// <para><b>Reading the numbers.</b> The free share is what the live rows leave free of the file's page space. After scattered
/// deletes and a VACUUM nearly every page can keep a few rows, so counting only pages with NO row calls a mostly empty table dense:
/// the live bytes are what tell them apart. A live row is its column sizes plus a 24 byte tuple header plus a 4 byte line pointer,
/// against a page's block size less its 24 byte header. Column alignment padding and the null bitmap are not counted, so the free
/// space reads slightly high. The share is judged after the table's fillfactor: the space a fillfactor of 70 keeps free on purpose is
/// not waste. <see cref="StoreTableMeasure.EmptyPages"/> counts pages with no live row, so a page holding only dead tuples counts as
/// empty (a VACUUM would free it). <see cref="StoreTableMeasure.LastUsedBlock"/> against <see cref="StoreTableMeasure.LastFileBlock"/>
/// decides whether VACUUM can return the space: it returns only the empty pages AFTER the last used block (it truncates the tail), so
/// a table whose last page holds one live row cannot shrink however empty the rest is. Only a rewrite (<c>VACUUM FULL</c>,
/// <c>CLUSTER</c> or a copy into a new table) returns that space. A rewrite is called for only past the same floors as the table
/// health rewrite candidate flag: at least half free and at least <see cref="RewriteMinFreeBytes"/> free.</para>
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

    /// <summary>The free bytes below which a mostly empty table is not worth a rewrite: the same floor the table health rewrite
    /// candidate flag uses (<see cref="DarlingStoreTableHealthReader.RewriteCandidateMinEmptyBytes"/>), 64 MiB.</summary>
    public const long RewriteMinFreeBytes = DarlingStoreTableHealthReader.RewriteCandidateMinEmptyBytes;

    /// <summary>Bytes in a heap page before its first tuple (PostgreSQL's <c>SizeOfPageHeaderData</c>).</summary>
    public const int PageHeaderBytes = 24;

    /// <summary>Bytes in a heap tuple's fixed header (<c>SizeofHeapTupleHeader</c>, without the null bitmap).</summary>
    public const int TupleHeaderBytes = 24;

    /// <summary>Bytes in a page's line pointer for one tuple (<c>sizeof(ItemIdData)</c>).</summary>
    public const int LinePointerBytes = 4;

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
    " + DarlingVacuumTruncateRule.TruncateOffSql + @",";

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

    /// <summary>The table's column-size expression and storage facts, read before the scan. The expression is built from the catalog's
    /// <c>quote_ident</c> names (never caller text) and sums <c>pg_column_size</c> of each live column. It never uses a whole-row value
    /// (<c>t.*</c> or <c>ROW(...)</c>), because a whole-row value flattens TOAST and reads it. A dropped column and a virtual
    /// generated column hold no stored bytes, so neither is summed.</summary>
    private const string LayoutSql = @"
SELECT
    coalesce((SELECT string_agg('coalesce(pg_catalog.pg_column_size(t.' || quote_ident(a.attname) || '), 0)', ' + ' ORDER BY a.attnum)
              FROM pg_catalog.pg_attribute AS a
              WHERE a.attrelid = $1::oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> 'v'), '0'),
    coalesce((SELECT split_part(o, '=', 2)::int FROM pg_catalog.pg_class AS c, unnest(c.reloptions) AS o
              WHERE c.oid = $1::oid AND o LIKE 'fillfactor=%'), 100),
    current_setting('block_size')::bigint";

    /// <summary>The measurement, in three parts around the column-size expression and the resolved, quoted table name. <c>ONLY</c> keeps
    /// an inheritance child out of the scan. The page figures come from the file's size, the row figures from the grouped scan.</summary>
    private const string MeasureSqlHead = @"
WITH p AS (
    SELECT (t.ctid::text::point)[0]::bigint AS blk, count(*) AS n, sum(";

    private const string MeasureSqlMid = @")::bigint AS data_bytes
    FROM ONLY ";

    private const string MeasureSqlTail = @" AS t
    GROUP BY 1
)
SELECT
    count(*) AS pages_with_rows,
    coalesce(sum(n), 0)::bigint AS rows_total,
    coalesce(max(blk), -1)::bigint AS last_used_block,
    coalesce(round(avg(n), 2), 0)::float8 AS rows_per_used_page,
    pg_catalog.pg_relation_size($1::oid) / current_setting('block_size')::bigint AS total_pages,
    count(*) FILTER (WHERE n = 1) AS pages_1_row,
    count(*) FILTER (WHERE n BETWEEN 2 AND 4) AS pages_2_to_4_rows,
    count(*) FILTER (WHERE n BETWEEN 5 AND 10) AS pages_5_to_10_rows,
    count(*) FILTER (WHERE n > 10) AS pages_over_10_rows,
    coalesce(sum(data_bytes), 0)::bigint AS data_bytes
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
    /// <param name="EmptyShare">Pages with no live row over total pages, 0 to 1. Zero for a file with no pages. Not what the verdict is judged on: see <paramref name="FreeShare"/>.</param>
    /// <param name="Rows">Live rows.</param>
    /// <param name="RowsPerUsedPage">Average live rows on a page that holds any, rounded to two places.</param>
    /// <param name="LastUsedBlock">The block number of the last page holding a live row, minus one when there is none.</param>
    /// <param name="LastFileBlock">The file's last block number (<paramref name="TotalPages"/> minus one).</param>
    /// <param name="TrailingEmptyPages">Pages after the last used block: the most a VACUUM can return.</param>
    /// <param name="PagesWith1Row">Used pages holding exactly one row.</param>
    /// <param name="PagesWith2To4Rows">Used pages holding two to four rows.</param>
    /// <param name="PagesWith5To10Rows">Used pages holding five to ten rows.</param>
    /// <param name="PagesWithOver10Rows">Used pages holding more than ten rows.</param>
    /// <param name="LiveBytes">The bytes the live rows take in pages: their column sizes, plus <see cref="TupleHeaderBytes"/> and
    /// <see cref="LinePointerBytes"/> per row. Column alignment padding is not counted.</param>
    /// <param name="FreeBytes">The page space the live rows leave free, after the table's fillfactor reserve, never below zero.</param>
    /// <param name="FreeShare"><paramref name="FreeBytes"/> over the file's page space at the table's fillfactor, 0 to 1. The share the
    /// verdict is judged on.</param>
    /// <param name="Fillfactor">The table's fillfactor percent, 100 when it has none.</param>
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
        long LiveBytes,
        long FreeBytes,
        double FreeShare,
        int Fillfactor,
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
            await using (var setTimeout = new NpgsqlCommand("SELECT set_config('statement_timeout', $1, true)", connection, transaction) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
            {
                setTimeout.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (TimeoutSeconds * 1000).ToString(CultureInfo.InvariantCulture) });
                await setTimeout.ExecuteNonQueryAsync(cancellationToken);
            }

            string sizeExpression;
            int fillfactor;
            long blockSize;
            await using (var layout = new NpgsqlCommand(LayoutSql, connection, transaction) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
            {
                layout.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = resolved.Oid });
                await using var layoutReader = await layout.ExecuteReaderAsync(cancellationToken);
                await layoutReader.ReadAsync(cancellationToken);
                sizeExpression = layoutReader.GetString(0);
                fillfactor = layoutReader.GetInt32(1);
                blockSize = layoutReader.GetInt64(2);
            }

            /* The name and every column in the size expression are the catalog's quote_ident output, never the caller's text. */
            var sql = MeasureSqlHead + sizeExpression + MeasureSqlMid + resolved.QualifiedName + MeasureSqlTail;
            long pagesWithRows, rows, lastUsed, totalPages, one, twoToFour, fiveToTen, over10, dataBytes;
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
                dataBytes = reader.GetInt64(9);
            }

            await transaction.CommitAsync(cancellationToken);
            stopwatch.Stop();
            return new StoreTableMeasureResult(StoreTableMeasureOutcome.Measured, null,
                Build(resolved.QualifiedName, totalPages, pagesWithRows, rows, rowsPerPage, lastUsed, one, twoToFour, fiveToTen, over10,
                    stopwatch.ElapsedMilliseconds, dataBytes, blockSize, fillfactor, resolved.VacuumTruncateOff));
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

    /// <summary>Builds the figures and the verdict from the raw scan. Public so the verdict rule is testable without a table of that shape.
    /// <paramref name="dataBytes"/> is the sum of the live rows' column sizes, before the per-row header and line pointer.</summary>
    public static StoreTableMeasure Build(
        string qualifiedName, long totalPages, long pagesWithRows, long rows, double rowsPerUsedPage, long lastUsedBlock,
        long pages1Row, long pages2To4, long pages5To10, long pagesOver10, long elapsedMilliseconds,
        long dataBytes, long blockSize = 8192, int fillfactor = 100, bool vacuumTruncateDisabled = false)
    {
        var empty = Math.Max(0, totalPages - pagesWithRows);
        var emptyShare = totalPages > 0 ? (double)empty / totalPages : 0.0;
        var lastFile = totalPages - 1;
        var trailing = Math.Max(0, lastFile - lastUsedBlock);
        var live = dataBytes + rows * (TupleHeaderBytes + LinePointerBytes);

        var usablePerPage = Math.Max(1, blockSize - PageHeaderBytes);
        var ff = Math.Clamp(fillfactor, 10, 100);
        var fileSpace = (double)totalPages * usablePerPage;
        var capacity = fileSpace * ff / 100.0;
        var free = (long)Math.Max(0.0, capacity - live);
        var freeShare = capacity > 0 ? Math.Min(1.0, free / capacity) : 0.0;
        var fillPercent = fileSpace > 0 ? Math.Min(100.0, live * 100.0 / fileSpace) : 0.0;
        var trailingBytes = trailing * usablePerPage * ff / 100.0;

        var (kind, sentence) = Judge(totalPages, empty, trailing, lastUsedBlock, lastFile, free, freeShare, fillPercent, trailingBytes, ff, vacuumTruncateDisabled);
        return new StoreTableMeasure(qualifiedName, totalPages, pagesWithRows, empty, emptyShare, rows, rowsPerUsedPage, lastUsedBlock, lastFile,
            trailing, pages1Row, pages2To4, pages5To10, pagesOver10, live, free, freeShare, ff, vacuumTruncateDisabled, kind, sentence, elapsedMilliseconds);
    }

    /// <summary>The verdict rule. Mostly empty means the live rows leave at least <see cref="MostlyEmptyShare"/> of the page space free
    /// after the fillfactor reserve. A mostly empty table whose trailing empty pages hold half or more of that free space can be cut
    /// shorter by VACUUM. Otherwise only a rewrite returns it, and only when at least <see cref="RewriteMinFreeBytes"/> is free: a
    /// smaller table reads dense with a sentence that says why.</summary>
    private static (string Kind, string Sentence) Judge(
        long totalPages, long empty, long trailing, long lastUsed, long lastFile, long free, double freeShare,
        double fillPercent, double trailingBytes, int fillfactor, bool truncateOff)
    {
        if (totalPages == 0)
        {
            return (VerdictNoPages, "The table's file has no pages, so there is no space to return.");
        }

        var pages = CountText.Plural(totalPages, "page");
        var fill = fillPercent.ToString("0", CultureInfo.InvariantCulture);
        var reserve = fillfactor < 100
            ? string.Create(CultureInfo.InvariantCulture, $" beyond the {fillfactor}% fillfactor the table keeps free on purpose")
            : string.Empty;
        var freeText = RollupBackfillPlan.FormatBytes(free);

        if (freeShare < MostlyEmptyShare)
        {
            return (VerdictDense, $"Dense: live rows fill {fill}% of {pages} and {freeText} is free{reserve}. There is little space to return.");
        }

        if (DarlingVacuumTruncateRule.CanReturnSpace(totalPages, trailing, trailingBytes, free, truncateOff))
        {
            return (VerdictVacuumCanTruncate, string.Create(CultureInfo.InvariantCulture,
                $"Mostly empty, and VACUUM can truncate the tail. Live rows fill {fill}% of {pages} and {freeText} is free{reserve}. {CountText.Grouped(trailing)} of its {CountText.Plural(empty, "page")} with no row {(trailing == 1 ? "is" : "are")} after the last used block ({lastUsed} of {lastFile})."));
        }

        if (free < RewriteMinFreeBytes)
        {
            return (VerdictDense, $"Mostly empty but small: live rows fill {fill}% of {pages} and {freeText} is free{reserve}, under the {RollupBackfillPlan.FormatBytes(RewriteMinFreeBytes)} that is worth a rewrite.");
        }

        return (VerdictRewriteNeeded, string.Create(CultureInfo.InvariantCulture,
            $"Mostly empty, and only a rewrite returns the space. Live rows fill {fill}% of {pages} and {freeText} is free inside pages{reserve}. Live rows reach block {lastUsed} of {lastFile}, {WhyVacuumCannotReturn(totalPages, trailing, trailingBytes, free, truncateOff)}."));
    }

    /// <summary>The clause that says why VACUUM cannot return the space of a mostly empty table, one per condition of
    /// <see cref="DarlingVacuumTruncateRule.CanReturnSpace"/> that failed.</summary>
    private static string WhyVacuumCannotReturn(long totalPages, long trailing, double trailingBytes, long free, bool truncateOff)
    {
        if (truncateOff)
        {
            return "and the table's vacuum_truncate option is off, so VACUUM will not cut the file";
        }

        if (trailing == 0)
        {
            return "and a live row sits in the last page, so VACUUM cannot shrink the file";
        }

        var minimum = DarlingVacuumTruncateRule.MinimumTrailingPages(totalPages);
        if (trailing < minimum)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"and only {CountText.Plural(trailing, "page")} at the end {(trailing == 1 ? "is" : "are")} empty. VACUUM needs {CountText.Plural(minimum, "page")} before it cuts a file shorter, so it cuts nothing");
        }

        return "so VACUUM can cut off only " + CountText.Plural(trailing, "page");
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
