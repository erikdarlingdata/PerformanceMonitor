/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Which store table the actual-plan request's IDENTIFIER resolves the re-executable query text from.</summary>
public enum ActualPlanSource
{
    /// <summary>No usable identifier — the dispatch fails the request.</summary>
    None,

    /// <summary>Top Queries / Query-Stats history / FinOps High Impact — resolves <c>query_stats</c> by query_hash + database.</summary>
    QueryStats,

    /// <summary>Query Store history — resolves <c>query_store_stats</c> by query_id + database.</summary>
    QueryStore,

    /// <summary>Wait drill-down — resolves <c>query_snapshots</c> by collection_time + session_id.</summary>
    QuerySnapshot,
}

/// <summary>
/// The parsed <c>execute_actual_plan</c> command payload (the viewer builds it, the service parses it). It
/// carries an IDENTIFIER ONLY — a stored-row key — and deliberately NO SQL text. Exactly one identifier kind is
/// populated (see <see cref="Source"/>), because "Get Actual Plan" lives on several viewer surfaces whose rows
/// are keyed differently: Top Queries / Query-Stats history / FinOps High Impact by <see cref="QueryHash"/>;
/// Query Store history by <see cref="QueryId"/>; Wait drill-down by <see cref="SnapshotCollectionTime"/> +
/// <see cref="SnapshotSessionId"/>. The service resolves the query text and estimated plan from its OWN store by
/// the identifier before re-executing.
///
/// <para><b>Why identifier-only is a hard security requirement:</b> the target server id rides on the command
/// row and the query is RE-EXECUTED as the service's stored monitoring credential. If this payload could carry
/// SQL text, any writer of <c>config.config_command</c> could make the service run arbitrary SQL against every
/// monitored target — a privilege escalation through the command plane. Carrying only a key means the worst a
/// command writer can do is ask the service to re-run a query the collector ALREADY captured (and only a
/// read-write seat can enqueue at all). A <c>query_stats</c> request (the one source where the service picks the
/// LATEST stored text for a hash + database) also carries <see cref="ConfirmedSha256"/>, the SHA-256 digest of the
/// full text the operator confirmed. It is a digest, not SQL, and the service refuses to run text that does not
/// match it (#5639). Property names are camelCase to match the viewer's serialization
/// (parsed case-insensitively). Pure args model + validation (<see cref="TryParse"/>); never throws.</para>
/// </summary>
public sealed record ActualPlanRequest(
    string? QueryHash,
    long? QueryId,
    DateTime? SnapshotCollectionTime,
    int? SnapshotSessionId,
    string? DatabaseName,
    string? ConfirmedSha256 = null)
{
    /// <summary>Which store table the identifier resolves from (the first populated kind wins — deterministic).</summary>
    public ActualPlanSource Source =>
        !string.IsNullOrWhiteSpace(QueryHash) ? ActualPlanSource.QueryStats
        : QueryId is not null ? ActualPlanSource.QueryStore
        : SnapshotCollectionTime is not null && SnapshotSessionId is not null ? ActualPlanSource.QuerySnapshot
        : ActualPlanSource.None;

    /// <summary>True when a usable stored-row key is present — the dispatch fails a request without one.</summary>
    public bool HasIdentifier => Source != ActualPlanSource.None;

    private static readonly JsonSerializerOptions s_options = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    /// <summary>
    /// Parses <c>args_json</c> into a request, returning false for null/blank/malformed JSON or a request that
    /// carries no usable identifier. Never throws.
    /// </summary>
    public static bool TryParse(string? argsJson, out ActualPlanRequest request)
    {
        request = new ActualPlanRequest(null, null, null, null, null);
        if (string.IsNullOrWhiteSpace(argsJson))
        {
            return false;
        }

        Dto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<Dto>(argsJson, s_options);
        }
        catch (JsonException)
        {
            return false;
        }

        if (dto is null)
        {
            return false;
        }

        var parsed = new ActualPlanRequest(
            NullIfBlank(dto.QueryHash),
            dto.QueryId,
            dto.SnapshotCollectionTime,
            dto.SnapshotSessionId,
            NullIfBlank(dto.DatabaseName),
            NullIfBlank(dto.ConfirmedSha256));

        if (!parsed.HasIdentifier)
        {
            return false;
        }

        request = parsed;
        return true;
    }

    private static string? NullIfBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    /// <summary>The wire shape of <c>args_json</c> — identifiers only; NO query-text field exists by design.</summary>
    private sealed class Dto
    {
        public string? QueryHash { get; set; }
        public long? QueryId { get; set; }
        public DateTime? SnapshotCollectionTime { get; set; }
        public int? SnapshotSessionId { get; set; }
        public string? DatabaseName { get; set; }
        public string? ConfirmedSha256 { get; set; }
    }
}
