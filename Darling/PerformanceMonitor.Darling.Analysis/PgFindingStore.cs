/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// One row of the <c>analysis_muted</c> registry, as the viewer's Recommendations tab needs it
/// to mark a displayed finding muted and to offer the unmute (it needs the <c>mute_id</c>, which
/// <see cref="PgFindingStore.GetMutedHashesSql"/> deliberately omits). <c>ServerId</c> is nullable
/// because a mute can be global (<c>server_id IS NULL</c>).
/// </summary>
public sealed record MutedStory(
    long MuteId,
    int? ServerId,
    string StoryPathHash,
    string StoryPath,
    DateTime MutedDate,
    string? Reason);

/// <summary>
/// Persists analysis findings to Darling's Postgres store (the V4 <c>analysis_findings</c> /
/// <c>analysis_muted</c> tables) and checks for muted story hashes — the write side of the
/// analysis pipeline, ported with the DASHBOARD twin's method surface and semantics
/// (SqlServerFindingStore): the two-phase <see cref="FilterMutedFindingsAsync"/> →
/// <see cref="InsertFindingsAsync"/> write (recommendations rebuild D2/P2 — survivors are
/// enriched and get their RemediationAction BUILT between the phases, so the action is
/// persisted as <c>remediation_action_json</c> via the shared <see cref="AlertContextSerializer"/>),
/// plus Lite's single-pass <see cref="SaveFindingsAsync"/> (its AnalysisService's surface),
/// implemented as the composition of the two phases.
///
/// <para>
/// Postgres discipline (see PgCollectorRowWriter/PgAlertHistoryStore): every timestamp is
/// written naive-UTC Kind-Unspecified (Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>)
/// and tagged Utc on read; "now" is always a bound parameter, never a bare <c>now()</c>
/// (timestamptz — it would compare in the PG server's time zone). Finding/mute ids come from
/// the shared <see cref="CollectionIdGenerator"/> instead of the twins' per-instance tick
/// counters. Schema is migration-owned (V4) — unlike the Dashboard twin there is no
/// EnsureTablesExist: the service migrates on startup, so the store carries no DDL.
/// </para>
///
/// <para>
/// Deliberate reconciliations with the twins, both toward the richer behavior:
/// <see cref="FilterMutedFindingsAsync"/> copies <c>story.DatabaseName</c> onto the finding
/// (Lite does; the Dashboard twin drops it and always persists NULL database_name), and
/// <see cref="GetLatestFindingsAsync"/> selects <c>remediation_action_json</c> too (the
/// Dashboard twin omits it from that one read and guards its reader by field count) — both
/// reads here share one uniform column list.
/// </para>
///
/// <para>
/// Error discipline mirrors the Dashboard twin: no public method throws — writes log and
/// degrade, reads log and return empty. The mute-hash read fails OPEN (an unreadable mute
/// registry lets findings through rather than suppressing them), exactly like both twins.
/// </para>
/// </summary>
public sealed class PgFindingStore
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgFindingStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /* SQL is exposed const so Darling.Tests can pin the dialect ungated ($N positional
       parameters, no bare now(), no N'' literals) — the DarlingAlertReadAdapter pattern. */

    public const string InsertFindingSql = @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)";

    public const string GetRecentFindingsSql = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
       incident_id, remediation_action_json, drill_down_json
FROM analysis_findings
WHERE server_id = $1
AND   analysis_time >= $2
ORDER BY analysis_time DESC, severity DESC
LIMIT $3";

    public const string GetLatestFindingsSql = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
       incident_id, remediation_action_json, drill_down_json
FROM analysis_findings
WHERE server_id = $1
AND   analysis_time = (
    SELECT MAX(analysis_time) FROM analysis_findings WHERE server_id = $1
)
ORDER BY severity DESC";

    /* server_id = 0 rows are legacy all-servers mutes written by the pre-fix MCP tool path
       (no real server has id 0); honor them as global, alongside the canonical NULL. */
    public const string GetMutedHashesSql = @"
SELECT story_path_hash FROM analysis_muted
WHERE server_id = $1 OR server_id IS NULL OR server_id = 0";

    /* Same per-server + global span as GetMutedHashesSql (NULL, plus legacy server_id = 0 all-servers
       rows), but carries mute_id/story_path so the viewer can mark a finding muted and delete the exact
       registry row on unmute. */
    public const string GetMutedStoriesSql = @"
SELECT mute_id, server_id, story_path_hash, story_path, muted_date, reason
FROM analysis_muted
WHERE server_id = $1 OR server_id IS NULL OR server_id = 0";

    public const string MuteStorySql = @"
INSERT INTO analysis_muted (mute_id, server_id, story_path_hash, story_path, muted_date, reason)
VALUES ($1, $2, $3, $4, $5, $6)";

    public const string UnmuteStorySql = "DELETE FROM analysis_muted WHERE mute_id = $1";

    public const string CleanupOldFindingsSql = "DELETE FROM analysis_findings WHERE analysis_time < $1";

    /// <summary>
    /// Mute-filters the stories and materializes the SURVIVING findings, WITHOUT inserting
    /// them (the Dashboard twin's D2/P2 reorder). The orchestrator then enriches these
    /// survivors and builds + attaches each finding's RemediationAction before calling
    /// <see cref="InsertFindingsAsync"/>, so the BUILT action is persisted on the row.
    /// Absolution stories (severity 0) and muted hashes are dropped here and never enriched.
    /// The context window arrives in the SERVER's local clock (Dashboard semantics — windowed
    /// reads match the collectors' SYSDATETIME rows); ServerUtcOffset converts it back to UTC
    /// for persistence. A Lite-shaped caller that leaves ServerUtcOffset at Zero (host-UTC
    /// windows) gets an identity conversion, so both twins' callers are served.
    /// </summary>
    public async Task<List<AnalysisFinding>> FilterMutedFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        if (stories is null)
        {
            throw new ArgumentNullException(nameof(stories));
        }
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        var analysisTime = NaiveUtcNow();
        var survivors = new List<AnalysisFinding>();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            var mutedHashes = await GetMutedHashesAsync(connection, context.ServerId);

            foreach (var story in stories)
            {
                /* Skip absolution stories (severity 0) — they confirm health, not problems. */
                if (story.Severity <= 0)
                {
                    continue;
                }

                if (mutedHashes.Contains(story.StoryPathHash))
                {
                    continue;
                }

                survivors.Add(new AnalysisFinding
                {
                    FindingId = CollectionIdGenerator.Next(),
                    AnalysisTime = analysisTime,
                    ServerId = context.ServerId,
                    ServerName = context.ServerName,
                    DatabaseName = story.DatabaseName,
                    TimeRangeStart = context.TimeRangeStart - context.ServerUtcOffset,
                    TimeRangeEnd = context.TimeRangeEnd - context.ServerUtcOffset,
                    Severity = story.Severity,
                    Confidence = story.Confidence,
                    Category = story.Category,
                    StoryPath = story.StoryPath,
                    StoryPathHash = story.StoryPathHash,
                    IncidentId = story.IncidentId,
                    StoryText = story.StoryText,
                    RootFactKey = story.RootFactKey,
                    RootFactValue = story.RootFactValue,
                    LeafFactKey = story.LeafFactKey,
                    LeafFactValue = story.LeafFactValue,
                    FactCount = story.FactCount,
                    /* Carried in-memory only; no analysis_findings column for it. */
                    RootFactMetadata = story.RootFactMetadata
                });
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsShutdownAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgFindingStore] FilterMutedFindingsAsync failed: {Message}", ex.Message);
        }

        return survivors;
    }

    /// <summary>
    /// Inserts the (already mute-filtered, enriched, and action-attached) findings in one
    /// batched pass on a single connection. Each row persists its BUILT
    /// <see cref="AnalysisFinding.Remediation"/> as <c>remediation_action_json</c> via the
    /// shared <see cref="AlertContextSerializer"/>, so a Darling finding's persisted action
    /// round-trips byte-identically to a Dashboard one. Returns the same list for caller
    /// convenience; the in-memory findings are unchanged.
    /// </summary>
    public async Task<List<AnalysisFinding>> InsertFindingsAsync(
        List<AnalysisFinding> findings, AnalysisContext context)
    {
        if (findings is null)
        {
            throw new ArgumentNullException(nameof(findings));
        }

        if (findings.Count == 0)
        {
            return findings;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            foreach (var finding in findings)
            {
                await InsertFindingAsync(connection, finding);
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsShutdownAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgFindingStore] InsertFindingsAsync failed: {Message}", ex.Message);
        }

        return findings;
    }

    /// <summary>
    /// Saves analysis stories as findings in one pass — Lite's FindingStore surface (its
    /// AnalysisService calls this shape). Implemented as
    /// <see cref="FilterMutedFindingsAsync"/> + <see cref="InsertFindingsAsync"/>, so a
    /// Lite-shaped pipeline port and a Dashboard-shaped one persist through the same code.
    /// NOTE: a caller that wants remediation_action_json persisted must use the two-phase
    /// shape and attach actions between the phases — this single pass inserts the findings
    /// exactly as filtered (Remediation is null unless the caller pre-set it on the stories'
    /// findings, which Lite's pipeline does not).
    /// </summary>
    public async Task<List<AnalysisFinding>> SaveFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        var survivors = await FilterMutedFindingsAsync(stories, context);
        return await InsertFindingsAsync(survivors, context);
    }

    /// <summary>
    /// Returns the most recent findings for a server within the given time range, newest and
    /// most severe first, including each finding's persisted remediation action.
    /// </summary>
    public async Task<List<AnalysisFinding>> GetRecentFindingsAsync(
        int serverId, int hoursBack = 24, int limit = 100)
    {
        var findings = new List<AnalysisFinding>();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(GetRecentFindingsSql, connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NaiveUtcNow().AddHours(-hoursBack));
            command.Parameters.AddWithValue(limit);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                findings.Add(ReadFinding(reader));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] GetRecentFindingsAsync failed: {Message}", ex.Message);
        }

        return findings;
    }

    /// <summary>
    /// Returns the latest analysis run's findings for a server (most recent analysis_time),
    /// most severe first. Unlike the Dashboard twin this read also returns
    /// remediation_action_json — both reads share one column list and one reader.
    /// </summary>
    public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)
    {
        var findings = new List<AnalysisFinding>();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(GetLatestFindingsSql, connection);
            command.Parameters.AddWithValue(serverId);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                findings.Add(ReadFinding(reader));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] GetLatestFindingsAsync failed: {Message}", ex.Message);
        }

        return findings;
    }

    /// <summary>
    /// Mutes a story pattern so it won't appear in future analysis runs.
    /// </summary>
    public async Task MuteStoryAsync(int serverId, string storyPathHash, string storyPath, string? reason = null)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(MuteStorySql, connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            // serverId 0 is the MCP "mute across all servers" sentinel; persist it as NULL, the
            // canonical global marker every reader filters on (legacy 0 rows are still honored).
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId == 0 ? (object)DBNull.Value : serverId });
            command.Parameters.AddWithValue(storyPathHash);
            command.Parameters.AddWithValue(storyPath);
            command.Parameters.AddWithValue(NaiveUtcNow());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)reason ?? DBNull.Value });

            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] MuteStoryAsync failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Unmutes a story pattern (Dashboard-twin surface).
    /// </summary>
    public async Task UnmuteStoryAsync(long muteId)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(UnmuteStorySql, connection);
            command.Parameters.AddWithValue(muteId);
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] UnmuteStoryAsync failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Reads the muted-story registry rows visible to a server (its own plus the global
    /// <c>server_id IS NULL</c> rows — the same span <see cref="GetMutedHashesSql"/> filters),
    /// carrying each row's <c>mute_id</c> so the viewer can offer an unmute. Reads log and
    /// degrade to an empty list like the store's other reads.
    /// The MCP "mute across all servers" path now persists <c>server_id = NULL</c> (the canonical
    /// global marker); legacy <c>server_id = 0</c> rows written before that fix are honored as global
    /// too (<see cref="GetMutedStoriesSql"/> filters both), so an all-servers mute is visible to every
    /// server. A NULL/0 (global) row here is flagged muted but left un-unmutable from the per-server
    /// viewer, since deleting it would unmute the pattern everywhere.
    /// </summary>
    public async Task<List<MutedStory>> GetMutedStoriesAsync(int serverId)
    {
        var stories = new List<MutedStory>();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(GetMutedStoriesSql, connection);
            command.Parameters.AddWithValue(serverId);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                stories.Add(new MutedStory(
                    reader.GetInt64(0),
                    reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    AsUtc(reader.GetDateTime(4)),
                    reader.IsDBNull(5) ? null : reader.GetString(5)));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] GetMutedStoriesAsync failed: {Message}", ex.Message);
        }

        return stories;
    }

    /// <summary>
    /// Cleans up old findings beyond the retention period.
    /// </summary>
    public async Task CleanupOldFindingsAsync(int retentionDays = 30)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(CleanupOldFindingsSql, connection);
            command.Parameters.AddWithValue(NaiveUtcNow().AddDays(-retentionDays));
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] CleanupOldFindingsAsync failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Reads muted story hashes for a server on an already-open connection (the caller owns
    /// it, so the mute-filter read reuses the filter call's connection). Fails OPEN like both
    /// twins: an unreadable mute registry returns the hashes read so far (usually empty) and
    /// findings flow through unfiltered rather than being suppressed.
    /// </summary>
    private async Task<HashSet<string>> GetMutedHashesAsync(NpgsqlConnection connection, int serverId)
    {
        var hashes = new HashSet<string>();

        try
        {
            using var command = new NpgsqlCommand(GetMutedHashesSql, connection);
            command.Parameters.AddWithValue(serverId);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                hashes.Add(reader.GetString(0));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] GetMutedHashesAsync failed: {Message}", ex.Message);
        }

        return hashes;
    }

    /// <summary>
    /// Inserts one finding on an already-open connection (the caller owns it, so a batch
    /// shares a single connection). Per-row failure isolation like the Dashboard twin: one
    /// bad row logs and the batch continues.
    /// </summary>
    private async Task InsertFindingAsync(NpgsqlConnection connection, AnalysisFinding finding)
    {
        try
        {
            using var command = new NpgsqlCommand(InsertFindingSql, connection);
            command.Parameters.AddWithValue(finding.FindingId);
            command.Parameters.AddWithValue(AsNaive(finding.AnalysisTime));
            command.Parameters.AddWithValue(finding.ServerId);
            command.Parameters.AddWithValue(finding.ServerName);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)finding.DatabaseName ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = finding.TimeRangeStart is { } rangeStart ? AsNaive(rangeStart) : (object)DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = finding.TimeRangeEnd is { } rangeEnd ? AsNaive(rangeEnd) : (object)DBNull.Value });
            command.Parameters.AddWithValue(finding.Severity);
            command.Parameters.AddWithValue(finding.Confidence);
            command.Parameters.AddWithValue(finding.Category);
            command.Parameters.AddWithValue(finding.StoryPath);
            command.Parameters.AddWithValue(finding.StoryPathHash);
            command.Parameters.AddWithValue(finding.StoryText);
            command.Parameters.AddWithValue(finding.RootFactKey);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (object?)finding.RootFactValue ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)finding.LeafFactKey ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (object?)finding.LeafFactValue ?? DBNull.Value });
            command.Parameters.AddWithValue(finding.FactCount);
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Text,
                Value = string.IsNullOrEmpty(finding.IncidentId) ? (object)DBNull.Value : finding.IncidentId
            });
            /* D2: persist the BUILT action (mirrors the alert path's ContextJson) so the
               Recommendations reader can drive Apply + consent from a stored finding. */
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Text,
                Value = (object?)AlertContextSerializer.SerializeAction(finding.Remediation) ?? DBNull.Value
            });
            /* #2060: persist the CAPPED drill-down beside the built action — same D2 rationale
               (the evidence rows exist only on the write path), same degrade-to-NULL discipline. */
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Text,
                Value = (object?)DrillDownSerializer.Serialize(finding.DrillDown) ?? DBNull.Value
            });

            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] InsertFindingAsync failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Reads a single AnalysisFinding from a data reader row. Ordinals match the shared
    /// column list of both SELECTs; timestamps are stored naive-UTC and tagged Utc here so
    /// the kind is explicit (the PgAlertHistoryStore read discipline).
    /// </summary>
    private static AnalysisFinding ReadFinding(NpgsqlDataReader reader)
    {
        return new AnalysisFinding
        {
            FindingId = reader.GetInt64(0),
            AnalysisTime = AsUtc(reader.GetDateTime(1)),
            ServerId = reader.GetInt32(2),
            ServerName = reader.GetString(3),
            DatabaseName = reader.IsDBNull(4) ? null : reader.GetString(4),
            TimeRangeStart = reader.IsDBNull(5) ? null : AsUtc(reader.GetDateTime(5)),
            TimeRangeEnd = reader.IsDBNull(6) ? null : AsUtc(reader.GetDateTime(6)),
            Severity = reader.GetDouble(7),
            Confidence = reader.GetDouble(8),
            Category = reader.GetString(9),
            StoryPath = reader.GetString(10),
            StoryPathHash = reader.GetString(11),
            StoryText = reader.GetString(12),
            RootFactKey = reader.GetString(13),
            RootFactValue = reader.IsDBNull(14) ? null : reader.GetDouble(14),
            LeafFactKey = reader.IsDBNull(15) ? null : reader.GetString(15),
            LeafFactValue = reader.IsDBNull(16) ? null : reader.GetDouble(16),
            FactCount = reader.GetInt32(17),
            IncidentId = reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
            /* D2: the BUILT action deserialized via the SAME serializer the alert path uses;
               null/garbage degrades to "no Apply affordance" inside DeserializeAction. */
            Remediation = reader.IsDBNull(19) ? null : AlertContextSerializer.DeserializeAction(reader.GetString(19)),
            /* #2060: the capped drill-down survives read-back; null/garbage degrades to
               "no drill-down" inside the serializer, mirroring the action's discipline. */
            DrillDown = reader.IsDBNull(20) ? null : DrillDownSerializer.Deserialize(reader.GetString(20))
        };
    }

    /// <summary>Naive-UTC now, Kind-Unspecified — the product's PG timestamp discipline.</summary>
    private static DateTime NaiveUtcNow() =>
        DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    /// <summary>Kind-Unspecified for writes — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>Columns are written as naive UTC; tag them Utc on read so the kind is explicit.</summary>
    private static DateTime AsUtc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
