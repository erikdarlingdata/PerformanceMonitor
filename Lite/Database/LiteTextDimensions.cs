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

namespace PerformanceMonitorLite.Database;

/// <summary>
/// The text columns Lite keeps ONCE in a dimension table instead of inline on every collected row (#9241). Query text and plan XML
/// were about 90 percent of the hot store: DuckDB does not dedup a string across rows (dictionary compression works inside one
/// segment, and a string of 4,096 bytes or more leaves its whole segment uncompressed), and a collector writes the same cached text
/// again on every cycle. Mirrors Darling's <c>PayloadDimensions</c> (#1767), including the key.
///
/// <para><b>The key is a content digest, <c>unhex(sha256(text))</c>, a 32-byte BLOB computed in DuckDB.</b> Not <c>query_hash</c>,
/// <c>query_plan_hash</c>, <c>plan_handle</c> or Query Store's <c>query_id</c>: those are shape hashes, or ids Query Store reuses
/// after a reset, so one key can stand for different text. Not DuckDB's <c>hash()</c> or <c>md5_number</c> either: those are not
/// collision-safe for a key that stands in for the content.</para>
///
/// <para><b>Zero rewrite.</b> The digest columns are added with <c>ADD COLUMN</c> and a row stored before them keeps its inline
/// text, so every reader coalesces the inline column with the dimension (the <c>vh_</c> views). The inline column is never dropped.</para>
/// </summary>
internal static class LiteTextDimensions
{
    /// <summary>One diverted text column: where it lives, which column carries its digest, and the table that holds the content.</summary>
    internal sealed record TextDimension(string Table, string TextColumn, string DigestColumn, string DimTable);

    /// <summary>The dimension table for query text, keyed by content digest.</summary>
    internal const string QueryTextDimTable = "query_text_dim";

    /// <summary>The digest column of the dimension table and of every fact table that references it.</summary>
    internal const string QueryTextDigestColumn = "query_text_digest";

    /// <summary>The diverted columns. A table appears once per text column it diverts.</summary>
    internal static readonly IReadOnlyList<TextDimension> All = new[]
    {
        new TextDimension("query_store_stats", "query_text", QueryTextDigestColumn, QueryTextDimTable),
        new TextDimension("query_stats", "query_text", QueryTextDigestColumn, QueryTextDimTable),
        new TextDimension("plan_correction", "query_text", QueryTextDigestColumn, QueryTextDimTable),
    };

    /// <summary>The dimension table's DDL. No key constraint: the writer keeps one row per digest, under the store's write lock.</summary>
    internal const string CreateQueryTextDimTable = @"
CREATE TABLE IF NOT EXISTS query_text_dim (
    digest BLOB NOT NULL,
    query_text VARCHAR NOT NULL
)";

    /// <summary>Whether any text column of <paramref name="table"/> is diverted.</summary>
    internal static bool IsDiverted(string table) => All.Any(d => d.Table == table);

    /// <summary>The columns of one diverted table, in the order they are listed in <see cref="All"/>.</summary>
    internal static IEnumerable<TextDimension> For(string table) => All.Where(d => d.Table == table);

    /// <summary>The name of the view that reads the hot table with its text resolved (<c>vh_&lt;table&gt;</c>).</summary>
    internal static string ViewName(string table) => "vh_" + table;

    /// <summary>
    /// What a statement reads for the HOT rows of <paramref name="table"/>: the <c>vh_</c> view for a diverted table, so a row
    /// that carries only a digest still returns its text, and the bare table for every other one.
    /// </summary>
    internal static string HotSource(string table) => IsDiverted(table) ? ViewName(table) : table;

    /// <summary>
    /// The digest columns as <c>AddMissingColumnsAsync</c> entries. Kept apart from <c>DuckDbInitializer.AddedColumns</c>, which
    /// follows the payload order shared with Darling's store: these columns exist in Lite only. They run on every start, a fresh
    /// file included, because the generated table statements do not carry them.
    /// </summary>
    internal static IEnumerable<(int Version, string Table, string Column, string Type)> DigestColumns(int version) =>
        All.Select(d => (version, d.Table, d.DigestColumn, "BLOB")).Distinct();

    /// <summary>The expression that digests <paramref name="textExpression"/>: SHA-256 over its UTF-8 bytes, as 32 raw bytes.</summary>
    internal static string DigestSql(string textExpression) => $"unhex(sha256({textExpression}))";

    /// <summary>
    /// The <c>vh_&lt;table&gt;</c> view: the hot table with every diverted text column resolved through its dimension, and the digest
    /// columns left out, so the columns' names, types and order equal the table's before the digest columns existed. A row with
    /// inline text keeps it, which is every row stored before the digest columns, and a NULL text with no digest stays NULL.
    /// </summary>
    internal static string CreateViewSql(string table)
    {
        var dims = For(table).ToList();
        if (dims.Count == 0)
        {
            throw new ArgumentException($"{table} has no diverted text column.", nameof(table));
        }

        var excluded = string.Join(", ", dims.Select(d => d.DigestColumn).Distinct());
        var replaced = string.Join(", ", dims.Select((d, i) => $"COALESCE(f.{d.TextColumn}, d{i}.query_text) AS {d.TextColumn}"));
        var joins = string.Join("\n", dims.Select((d, i) => $"LEFT JOIN {d.DimTable} d{i} ON d{i}.digest = f.{d.DigestColumn}"));
        return $@"CREATE OR REPLACE VIEW {ViewName(table)} AS
SELECT f.* EXCLUDE ({excluded}) REPLACE ({replaced})
FROM {table} f
{joins}";
    }
}
