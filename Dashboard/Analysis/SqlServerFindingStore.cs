using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
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
        story_text nvarchar(max) NOT NULL,
        root_fact_key nvarchar(256) NOT NULL,
        root_fact_value float NULL,
        leaf_fact_key nvarchar(256) NULL,
        leaf_fact_value float NULL,
        fact_count integer NOT NULL,
        incident_id nvarchar(64) NULL,
        remediation_action_json nvarchar(max) NULL,
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
END;

/* Recommendations rebuild D2: existing DBs created before this column get it added
   idempotently. The Recommendations surface reads it back to drive Apply + the
   two-sided consent gate (the built RemediationAction, not raw drill-down). */
IF COL_LENGTH(N'config.analysis_findings', N'remediation_action_json') IS NULL
    ALTER TABLE config.analysis_findings ADD remediation_action_json nvarchar(max) NULL;

/* Compose-from-facts: story_text now carries the serialized value-stated advice for EVERY finding
   (it was previously written empty), so widen it from nvarchar(4000) to nvarchar(max) on existing
   DBs — removes the truncation cliff and matches Lite's unbounded story_text. COL_LENGTH returns
   -1 for nvarchar(max); any other value means the column still needs widening. NOT NULL is kept. */
IF COL_LENGTH(N'config.analysis_findings', N'story_text') <> -1
    ALTER TABLE config.analysis_findings ALTER COLUMN story_text nvarchar(max) NOT NULL;

/* Correlate-and-focus slice 2: the incident grouping id, added idempotently on existing DBs. */
IF COL_LENGTH(N'config.analysis_findings', N'incident_id') IS NULL
    ALTER TABLE config.analysis_findings ADD incident_id nvarchar(64) NULL;";

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Mute-filters the stories and materializes the SURVIVING findings, WITHOUT inserting
    /// them (recommendations rebuild D2 / P2 reorder). The orchestrator then enriches these
    /// survivors and builds + attaches each finding's RemediationAction before calling
    /// <see cref="InsertFindingsAsync"/>, so the BUILT action is persisted on the row.
    /// Mute/dedup/ordering and the <c>_nextId</c> sequencing are identical to the previous
    /// single-pass save; only the insert is deferred. Muted (and absolution) findings are
    /// dropped here and never enriched (no enrich-then-discard).
    /// </summary>
    public async Task<List<AnalysisFinding>> FilterMutedFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        var analysisTime = DateTime.UtcNow;
        var survivors = new List<AnalysisFinding>();

        try
        {
            /* One connection for the mute read. EnsureTablesExistAsync runs ONCE here (not
               per finding). Mandatory under D0's default-on analysis. */
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            var mutedHashes = await GetMutedHashesAsync(connection, context.ServerId);

            foreach (var story in stories)
            {
                // Skip absolution stories (severity 0) -- they confirm health, not problems
                if (story.Severity <= 0)
                    continue;

                if (mutedHashes.Contains(story.StoryPathHash))
                    continue;

                survivors.Add(new AnalysisFinding
                {
                    FindingId = _nextId++,
                    AnalysisTime = analysisTime,
                    ServerId = context.ServerId,
                    ServerName = context.ServerName,
                    // The context window is in the SERVER's local clock (so windowed reads match
                    // the collectors' SYSDATETIME rows); convert back to UTC for persistence so the
                    // stored time_range_* stay UTC — the reader's AsUtc, the deep-link offset math,
                    // and the retention purge all continue to assume UTC. (offset = SYSDATETIME −
                    // SYSUTCDATETIME, so local − offset = UTC.)
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
                    // Carried in-memory only; no analysis_findings column for it.
                    RootFactMetadata = story.RootFactMetadata
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] FilterMutedFindingsAsync failed: {ex.Message}");
        }

        return survivors;
    }

    /// <summary>
    /// Inserts the (already mute-filtered, enriched, and action-attached) findings in one
    /// batched pass (recommendations rebuild D2 / P2 reorder). Each row persists its BUILT
    /// <see cref="AnalysisFinding.Remediation"/> as <c>remediation_action_json</c> via the
    /// shared <see cref="AlertContextSerializer"/>. One connection + one
    /// EnsureTablesExistAsync for the whole batch (PR-1's discipline). Returns the same list
    /// for caller convenience; the in-memory findings are unchanged.
    /// </summary>
    public async Task<List<AnalysisFinding>> InsertFindingsAsync(
        List<AnalysisFinding> findings, AnalysisContext context)
    {
        if (findings.Count == 0)
            return findings;

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);

            foreach (var finding in findings)
                await InsertFindingAsync(connection, finding);
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] InsertFindingsAsync failed: {ex.Message}");
        }

        return findings;
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
    root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
    incident_id, remediation_action_json
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
    root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
    incident_id
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

    /// <summary>
    /// Reads muted story hashes for a server on an already-open connection. The caller
    /// owns the connection and is responsible for EnsureTablesExistAsync. Used by
    /// FilterMutedFindingsAsync so the mute-filter read reuses its connection.
    /// </summary>
    private static async Task<HashSet<string>> GetMutedHashesAsync(SqlConnection connection, int serverId)
    {
        var hashes = new HashSet<string>();

        try
        {
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

    /// <summary>
    /// Inserts one finding on an already-open connection. The caller owns the connection
    /// and has already run EnsureTablesExistAsync, so a batch of inserts in one
    /// InsertFindingsAsync call shares a single connection and a single schema check.
    /// </summary>
    private static async Task InsertFindingAsync(SqlConnection connection, AnalysisFinding finding)
    {
        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO config.analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json)
VALUES
    (@findingId, @analysisTime, @serverId, @serverName, @databaseName,
     @timeRangeStart, @timeRangeEnd, @severity, @confidence, @category,
     @storyPath, @storyPathHash, @storyText,
     @rootFactKey, @rootFactValue, @leafFactKey, @leafFactValue, @factCount,
     @incidentId, @remediationActionJson);";

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
            cmd.Parameters.Add(new SqlParameter("@incidentId",
                (object?)(string.IsNullOrEmpty(finding.IncidentId) ? null : finding.IncidentId) ?? DBNull.Value));
            // D2: persist the BUILT action (mirrors the alert path's ContextJson) so the
            // Recommendations reader can drive Apply + consent from a stored finding.
            cmd.Parameters.Add(new SqlParameter("@remediationActionJson",
                (object?)AlertContextSerializer.SerializeAction(finding.Remediation) ?? DBNull.Value));

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
        var finding = new AnalysisFinding
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
            FactCount = reader.GetInt32(17),
            // incident_id is ordinal 18 in BOTH SELECTs (correlate-and-focus slice 2).
            IncidentId = reader.FieldCount > 18 && !reader.IsDBNull(18) ? reader.GetString(18) : string.Empty
        };

        // D2: GetRecentFindingsAsync selects remediation_action_json (now ordinal 19, after
        // incident_id); GetLatestFindingsAsync omits it, so guard by field count before reading. The
        // BUILT action is deserialized via the SAME serializer the alert path uses, so the
        // Recommendations surface can drive Apply + the two-sided consent gate from storage.
        if (reader.FieldCount > 19 && !reader.IsDBNull(19))
            finding.Remediation = AlertContextSerializer.DeserializeAction(reader.GetString(19));

        return finding;
    }
}
