using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Persists analysis findings to DuckDB and checks for muted story hashes.
/// Handles the write side of the analysis pipeline — after the engine produces
/// stories, FindingStore saves them and filters out muted patterns.
/// </summary>
public class FindingStore
{
    private readonly DuckDbInitializer _duckDb;
    private long _nextId;

    public FindingStore(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
        _nextId = DateTime.UtcNow.Ticks;
    }

    /// <summary>
    /// Saves analysis stories as findings, filtering out any that match muted hashes.
    /// Returns the list of findings that were actually saved (non-muted).
    /// </summary>
    public async Task<List<AnalysisFinding>> SaveFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        var mutedHashes = await GetMutedHashesAsync(context.ServerId);
        var analysisTime = DateTime.UtcNow;
        var saved = new List<AnalysisFinding>();

        foreach (var story in stories)
        {
            // Skip absolution stories (severity 0) — they confirm health, not problems
            if (story.Severity <= 0)
                continue;

            if (mutedHashes.Contains(story.StoryPathHash))
                continue;

            var finding = new AnalysisFinding
            {
                FindingId = _nextId++,
                AnalysisTime = analysisTime,
                ServerId = context.ServerId,
                ServerName = context.ServerName,
                TimeRangeStart = context.TimeRangeStart,
                TimeRangeEnd = context.TimeRangeEnd,
                Severity = story.Severity,
                Confidence = story.Confidence,
                Category = story.Category,
                StoryPath = story.StoryPath,
                StoryPathHash = story.StoryPathHash,
                StoryText = story.StoryText,
                RootFactKey = story.RootFactKey,
                RootFactValue = story.RootFactValue,
                LeafFactKey = story.LeafFactKey,
                LeafFactValue = story.LeafFactValue,
                FactCount = story.FactCount,
                RootFactMetadata = story.RootFactMetadata
            };

            await InsertFindingAsync(finding);
            saved.Add(finding);
        }

        return saved;
    }

    /// <summary>
    /// Returns the most recent findings for a server within the given time range.
    /// </summary>
    public async Task<List<AnalysisFinding>> GetRecentFindingsAsync(
        int serverId, int hoursBack = 24, int limit = 100)
    {
        var findings = new List<AnalysisFinding>();

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count
FROM analysis_findings
WHERE server_id = $1
AND   analysis_time >= $2
ORDER BY analysis_time DESC, severity DESC
LIMIT $3";

        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hoursBack) });
        cmd.Parameters.Add(new DuckDBParameter { Value = limit });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            findings.Add(new AnalysisFinding
            {
                FindingId = reader.GetInt64(0),
                AnalysisTime = reader.GetDateTime(1),
                ServerId = reader.GetInt32(2),
                ServerName = reader.GetString(3),
                DatabaseName = reader.IsDBNull(4) ? null : reader.GetString(4),
                TimeRangeStart = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                TimeRangeEnd = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
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
                FactCount = reader.GetInt32(17)
            });
        }

        return findings;
    }

    /// <summary>
    /// Returns the latest analysis run's findings for a server (most recent analysis_time).
    /// </summary>
    public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)
    {
        var findings = new List<AnalysisFinding>();

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count
FROM analysis_findings
WHERE server_id = $1
AND   analysis_time = (
    SELECT MAX(analysis_time) FROM analysis_findings WHERE server_id = $1
)
ORDER BY severity DESC";

        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            findings.Add(new AnalysisFinding
            {
                FindingId = reader.GetInt64(0),
                AnalysisTime = reader.GetDateTime(1),
                ServerId = reader.GetInt32(2),
                ServerName = reader.GetString(3),
                DatabaseName = reader.IsDBNull(4) ? null : reader.GetString(4),
                TimeRangeStart = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                TimeRangeEnd = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
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
                FactCount = reader.GetInt32(17)
            });
        }

        return findings;
    }

    /// <summary>
    /// Mutes a story pattern so it won't appear in future analysis runs.
    /// </summary>
    public async Task MuteStoryAsync(int serverId, string storyPathHash, string storyPath, string? reason = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO analysis_muted (mute_id, server_id, story_path_hash, story_path, muted_date, reason)
VALUES ($1, $2, $3, $4, $5, $6)";

        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPath });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = reason ?? (object)DBNull.Value });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Unmutes a story pattern.
    /// </summary>
    public async Task UnmuteStoryAsync(long muteId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM analysis_muted WHERE mute_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = muteId });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Cleans up old findings beyond the retention period.
    /// </summary>
    public async Task CleanupOldFindingsAsync(int retentionDays = 30)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM analysis_findings WHERE analysis_time < $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-retentionDays) });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<HashSet<string>> GetMutedHashesAsync(int serverId)
    {
        var hashes = new HashSet<string>();

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT story_path_hash FROM analysis_muted
WHERE server_id = $1 OR server_id IS NULL";

        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            hashes.Add(reader.GetString(0));

        return hashes;
    }

    private async Task InsertFindingAsync(AnalysisFinding finding)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)";

        cmd.Parameters.Add(new DuckDBParameter { Value = finding.FindingId });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.AnalysisTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.DatabaseName ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.TimeRangeStart ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.TimeRangeEnd ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.Severity });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.Confidence });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.Category });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.StoryPath });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.StoryPathHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.StoryText });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.RootFactKey });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.RootFactValue ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.LeafFactKey ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.LeafFactValue ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.FactCount });

        await cmd.ExecuteNonQueryAsync();
    }
}
