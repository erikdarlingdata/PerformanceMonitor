using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Persists analysis findings to SQL Server and checks for muted story hashes.
/// Handles the write side of the analysis pipeline -- after the engine produces
/// stories, SqlServerFindingStore saves them and filters out muted patterns.
/// Port of Lite's FindingStore -- uses SQL Server instead of DuckDB.
/// Auto-creates config.analysis_findings and config.analysis_muted tables if missing.
/// </summary>
public class SqlServerFindingStore
{
    private readonly string _connectionString;
    private long _nextId;

    public SqlServerFindingStore(string connectionString)
    {
        _connectionString = connectionString;
        _nextId = DateTime.UtcNow.Ticks;
    }

    /// <summary>
    /// Ensures the analysis_findings and analysis_muted tables exist.
    /// Called before any read/write operation. Uses IF NOT EXISTS for idempotency.
    /// </summary>
    private async Task EnsureTablesExistAsync(SqlConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
IF OBJECT_ID(N'config.analysis_findings', N'U') IS NULL
BEGIN
    CREATE TABLE config.analysis_findings
    (
        finding_id bigint NOT NULL,
        analysis_time datetime2(7) NOT NULL,
        server_id integer NOT NULL,
        server_name nvarchar(256) NOT NULL,
        database_name nvarchar(256) NULL,
        time_range_start datetime2(7) NULL,
        time_range_end datetime2(7) NULL,
        severity float NOT NULL,
        confidence float NOT NULL,
        category nvarchar(256) NOT NULL,
        story_path nvarchar(2000) NOT NULL,
        story_path_hash nvarchar(256) NOT NULL,
        story_text nvarchar(4000) NOT NULL,
        root_fact_key nvarchar(256) NOT NULL,
        root_fact_value float NULL,
        leaf_fact_key nvarchar(256) NULL,
        leaf_fact_value float NULL,
        fact_count integer NOT NULL,
        CONSTRAINT PK_analysis_findings PRIMARY KEY CLUSTERED (finding_id)
            WITH (DATA_COMPRESSION = PAGE)
    );

    CREATE INDEX IX_analysis_findings_server_time
    ON config.analysis_findings (server_id, analysis_time DESC)
        WITH (DATA_COMPRESSION = PAGE);
END;

IF OBJECT_ID(N'config.analysis_muted', N'U') IS NULL
BEGIN
    CREATE TABLE config.analysis_muted
    (
        mute_id bigint NOT NULL,
        server_id integer NULL,
        story_path_hash nvarchar(256) NOT NULL,
        story_path nvarchar(2000) NOT NULL,
        muted_date datetime2(7) NOT NULL,
        reason nvarchar(1000) NULL,
        CONSTRAINT PK_analysis_muted PRIMARY KEY CLUSTERED (mute_id)
            WITH (DATA_COMPRESSION = PAGE)
    );

    CREATE INDEX IX_analysis_muted_server_hash
    ON config.analysis_muted (server_id, story_path_hash)
        WITH (DATA_COMPRESSION = PAGE);
END;";

        await cmd.ExecuteNonQueryAsync();
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
            // Skip absolution stories (severity 0) -- they confirm health, not problems
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
                FactCount = story.FactCount
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

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (@limit)
    finding_id, analysis_time, server_id, server_name, database_name,
    time_range_start, time_range_end, severity, confidence, category,
    story_path, story_path_hash, story_text,
    root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count
FROM config.analysis_findings
WHERE server_id = @serverId
AND   analysis_time >= @cutoff
ORDER BY analysis_time DESC, severity DESC;";

            cmd.Parameters.Add(new SqlParameter("@serverId", serverId));
            cmd.Parameters.Add(new SqlParameter("@cutoff", DateTime.UtcNow.AddHours(-hoursBack)));
            cmd.Parameters.Add(new SqlParameter("@limit", limit));

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                findings.Add(ReadFinding(reader));
            }
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] GetRecentFindingsAsync failed: {ex.Message}");
        }

        return findings;
    }

    /// <summary>
    /// Returns the latest analysis run's findings for a server (most recent analysis_time).
    /// </summary>
    public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)
    {
        var findings = new List<AnalysisFinding>();

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    finding_id, analysis_time, server_id, server_name, database_name,
    time_range_start, time_range_end, severity, confidence, category,
    story_path, story_path_hash, story_text,
    root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count
FROM config.analysis_findings
WHERE server_id = @serverId
AND   analysis_time = (
    SELECT MAX(analysis_time) FROM config.analysis_findings WHERE server_id = @serverId
)
ORDER BY severity DESC;";

            cmd.Parameters.Add(new SqlParameter("@serverId", serverId));

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                findings.Add(ReadFinding(reader));
            }
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] GetLatestFindingsAsync failed: {ex.Message}");
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
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO config.analysis_muted (mute_id, server_id, story_path_hash, story_path, muted_date, reason)
VALUES (@muteId, @serverId, @storyPathHash, @storyPath, @mutedDate, @reason);";

            cmd.Parameters.Add(new SqlParameter("@muteId", _nextId++));
            cmd.Parameters.Add(new SqlParameter("@serverId", serverId));
            cmd.Parameters.Add(new SqlParameter("@storyPathHash", storyPathHash));
            cmd.Parameters.Add(new SqlParameter("@storyPath", storyPath));
            cmd.Parameters.Add(new SqlParameter("@mutedDate", DateTime.UtcNow));
            cmd.Parameters.Add(new SqlParameter("@reason", (object?)reason ?? DBNull.Value));

            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] MuteStoryAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Unmutes a story pattern.
    /// </summary>
    public async Task UnmuteStoryAsync(long muteId)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "DELETE FROM config.analysis_muted WHERE mute_id = @muteId;";
            cmd.Parameters.Add(new SqlParameter("@muteId", muteId));
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] UnmuteStoryAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Cleans up old findings beyond the retention period.
    /// </summary>
    public async Task CleanupOldFindingsAsync(int retentionDays = 30)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "DELETE FROM config.analysis_findings WHERE analysis_time < @cutoff;";
            cmd.Parameters.Add(new SqlParameter("@cutoff", DateTime.UtcNow.AddDays(-retentionDays)));
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] CleanupOldFindingsAsync failed: {ex.Message}");
        }
    }

    private async Task<HashSet<string>> GetMutedHashesAsync(int serverId)
    {
        var hashes = new HashSet<string>();

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT story_path_hash FROM config.analysis_muted
WHERE server_id = @serverId OR server_id IS NULL;";

            cmd.Parameters.Add(new SqlParameter("@serverId", serverId));

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                hashes.Add(reader.GetString(0));
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] GetMutedHashesAsync failed: {ex.Message}");
        }

        return hashes;
    }

    private async Task InsertFindingAsync(AnalysisFinding finding)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO config.analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count)
VALUES
    (@findingId, @analysisTime, @serverId, @serverName, @databaseName,
     @timeRangeStart, @timeRangeEnd, @severity, @confidence, @category,
     @storyPath, @storyPathHash, @storyText,
     @rootFactKey, @rootFactValue, @leafFactKey, @leafFactValue, @factCount);";

            cmd.Parameters.Add(new SqlParameter("@findingId", finding.FindingId));
            cmd.Parameters.Add(new SqlParameter("@analysisTime", finding.AnalysisTime));
            cmd.Parameters.Add(new SqlParameter("@serverId", finding.ServerId));
            cmd.Parameters.Add(new SqlParameter("@serverName", finding.ServerName));
            cmd.Parameters.Add(new SqlParameter("@databaseName", (object?)finding.DatabaseName ?? DBNull.Value));
            cmd.Parameters.Add(new SqlParameter("@timeRangeStart", (object?)finding.TimeRangeStart ?? DBNull.Value));
            cmd.Parameters.Add(new SqlParameter("@timeRangeEnd", (object?)finding.TimeRangeEnd ?? DBNull.Value));
            cmd.Parameters.Add(new SqlParameter("@severity", finding.Severity));
            cmd.Parameters.Add(new SqlParameter("@confidence", finding.Confidence));
            cmd.Parameters.Add(new SqlParameter("@category", finding.Category));
            cmd.Parameters.Add(new SqlParameter("@storyPath", finding.StoryPath));
            cmd.Parameters.Add(new SqlParameter("@storyPathHash", finding.StoryPathHash));
            cmd.Parameters.Add(new SqlParameter("@storyText", finding.StoryText));
            cmd.Parameters.Add(new SqlParameter("@rootFactKey", finding.RootFactKey));
            cmd.Parameters.Add(new SqlParameter("@rootFactValue", (object?)finding.RootFactValue ?? DBNull.Value));
            cmd.Parameters.Add(new SqlParameter("@leafFactKey", (object?)finding.LeafFactKey ?? DBNull.Value));
            cmd.Parameters.Add(new SqlParameter("@leafFactValue", (object?)finding.LeafFactValue ?? DBNull.Value));
            cmd.Parameters.Add(new SqlParameter("@factCount", finding.FactCount));

            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] InsertFindingAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Reads a single AnalysisFinding from a data reader row.
    /// </summary>
    private static AnalysisFinding ReadFinding(SqlDataReader reader)
    {
        return new AnalysisFinding
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
        };
    }
}
