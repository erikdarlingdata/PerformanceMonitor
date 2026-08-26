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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The subject of a hypothetical-index experiment (#2612): one candidate index, and one stored statement
/// to re-plan with and without it.
///
/// <para>
/// <b>On demand only, never scheduled</b> — the decision recorded on the issue before any code was written.
/// There is no collector, no cadence and no sweep: this is a tool invocation with a caller and a subject,
/// driven from a <c>pg_predicate_stats</c> row a human is already looking at. That collector separates the
/// two reasons a column looks interesting, and only one of them is this experiment's business: poor
/// selectivity, where an index might help. A large estimate error means the planner does not understand
/// the column, and an index will not fix a plan built on a wrong row count.
/// </para>
///
/// <para>
/// <b>Which server.</b> The registered server the caller names, and no other. The statistics that made the
/// column a candidate are that server's statistics, and a hypothetical index tested against different ones
/// answers a different question. An operator who wants the experiment run on a replica registers the
/// replica — which is a decision they can see, rather than a substitution the product made quietly.
/// </para>
///
/// <para>
/// Property names are camelCase to match the viewer's serialization (parsed case-insensitively). Pure args
/// model plus validation; never throws.
/// </para>
/// </summary>
public sealed record HypotheticalIndexRequest(
    [property: JsonPropertyName("queryid")] string? QueryId,
    [property: JsonPropertyName("schemaName")] string? SchemaName,
    [property: JsonPropertyName("tableName")] string? TableName,
    [property: JsonPropertyName("columns")] IReadOnlyList<string>? Columns,
    [property: JsonPropertyName("databaseName")] string? DatabaseName)
{
    /// <summary>
    /// How many columns one candidate may carry. A bound rather than a judgment about index design: the
    /// column list is spliced into DDL text handed to <c>hypopg_create_index</c>, and an unbounded list is
    /// an unbounded statement.
    /// </summary>
    public const int MaxColumns = 8;

    /// <summary>
    /// <c>queryid</c> travels as a STRING, both directions, like every other queryid on this surface: it is
    /// a signed 64-bit value and a JSON number would round it in any double-decoding parser, producing an
    /// id that resolves to no stored statement.
    /// </summary>
    public bool TryGetQueryId(out long queryId)
        => long.TryParse(QueryId, System.Globalization.NumberStyles.Integer,
            System.Globalization.CultureInfo.InvariantCulture, out queryId);

    /// <summary>
    /// True when the request names a statement AND a candidate. Both halves are required and neither has a
    /// sensible default: without the statement there is nothing to re-plan, and without the candidate there
    /// is no experiment — only an EXPLAIN of somebody's query, which this is not for.
    /// </summary>
    public bool IsComplete =>
        TryGetQueryId(out _)
        && IsSafeIdentifier(SchemaName)
        && IsSafeIdentifier(TableName)
        && Columns is { Count: > 0 and <= MaxColumns }
        && Columns.All(IsSafeIdentifier);

    /// <summary>
    /// Identifier acceptance, and it is deliberately narrow.
    ///
    /// <para>These names are spliced into a DDL string that <c>hypopg_create_index</c> parses, so they are
    /// the one place in this feature where caller-supplied text reaches SQL text. They cannot be passed as
    /// parameters — the function takes a whole CREATE INDEX statement as a string — so the defence is that
    /// nothing but an unqualified, unquoted identifier is accepted at all. A name needing quoting is
    /// refused rather than escaped: refusing is auditable, and escaping is the thing that gets one case
    /// wrong three years later.</para>
    /// </summary>
    public static bool IsSafeIdentifier(string? value)
        => !string.IsNullOrWhiteSpace(value)
           && value.Length <= 63
           && (char.IsLetter(value[0]) || value[0] == '_')
           && value.All(c => char.IsLetterOrDigit(c) || c == '_');

    private static readonly JsonSerializerOptions s_options = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    /// <summary>
    /// Parses <c>args_json</c>, returning false for null, blank, malformed, or incomplete input. Never
    /// throws — the dispatch turns a false here into a command failure with a message, which is the only
    /// way a caller finds out they sent something unusable.
    /// </summary>
    public static bool TryParse(string? argsJson, out HypotheticalIndexRequest request)
    {
        request = new HypotheticalIndexRequest(null, null, null, null, null);

        if (string.IsNullOrWhiteSpace(argsJson))
        {
            return false;
        }

        try
        {
            var parsed = JsonSerializer.Deserialize<HypotheticalIndexRequest>(argsJson, s_options);

            if (parsed is null || !parsed.IsComplete)
            {
                return false;
            }

            request = parsed;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// The <c>CREATE INDEX</c> text handed to <c>hypopg_create_index</c>. Composed only from identifiers
    /// that already passed <see cref="IsSafeIdentifier"/>, and asserted again here so a future caller that
    /// skips <see cref="TryParse"/> cannot compose one.
    /// </summary>
    public string BuildCreateIndexStatement()
    {
        if (!IsComplete)
        {
            throw new InvalidOperationException("An incomplete hypothetical-index request cannot compose DDL.");
        }

        return $"CREATE INDEX ON {SchemaName}.{TableName} ({string.Join(", ", Columns!)})";
    }
}
