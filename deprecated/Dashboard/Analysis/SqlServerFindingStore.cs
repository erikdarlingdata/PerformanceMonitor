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

                survivors.Add(MapStoryToFinding(story, context, analysisTime, _nextId++));
            }
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] FilterMutedFindingsAsync failed: {ex.Message}");
        }

        return survivors;
    }

    /// <summary>
    /// Maps a scored story to the finding shape this store persists. Internal so the mapping is
    /// unit-testable — the DatabaseName drop this extraction fixed survived unnoticed because the
    /// mapping was buried in a connection-opening method.
    /// </summary>
    internal static AnalysisFinding MapStoryToFinding(AnalysisStory story, AnalysisContext context, DateTime analysisTime, long findingId)
    {
        return new AnalysisFinding
        {
            FindingId = findingId,
            AnalysisTime = analysisTime,
            ServerId = context.ServerId,
            ServerName = context.ServerName,
            // Lite's FindingStore has always persisted the story's database; the Dashboard twin
            // dropped it here and stored NULL database_name on every finding (surfaced by the
            // Darling PgFindingStore port), breaking per-database display and the database-scoped
            // mute path for stored rows.
            DatabaseName = story.DatabaseName,
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
        };
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
    incident_id, remediation_action_json
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
    /// Mutes a story pattern so it won't appear in future analysis runs. Returns whether the mute row
    /// LANDED (#3653, #3615's class): the write swallows and logs its failure so the Recommendations
    /// button keeps its no-throw contract, but the MCP <c>mute_analysis_finding</c> used to answer
    /// <c>"muted"</c> for any hash on any outcome — the caller now learns which of the two things happened.
    /// </summary>
    public async Task<bool> MuteStoryAsync(int serverId, string storyPathHash, string storyPath, string? reason = null)
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
            return true;
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] MuteStoryAsync failed: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// How many stored findings for <paramref name="serverId"/> carry <paramref name="storyPathHash"/> right
    /// now — the <c>matched_now</c> disclosure behind <c>mute_analysis_finding</c> (#3653, #3615's class). The
    /// mute registry is a PATTERN registry (no row references a finding; the filter phase consults it by
    /// hash on every pass), so a hash matching nothing today is a legitimate registration — the pattern may
    /// return after retention purged its history — AND the most likely shape of a typo. The count is a
    /// disclosure, not a gate. A plain equality on <c>config.analysis_findings</c>, which this store indexes by
    /// <c>(server_id, analysis_time)</c> only: the read walks one server's retained findings (30 days by
    /// default), off the alert path, on an operator-initiated write. Throws on failure like the other reads
    /// here do not — deliberately: the caller is reporting what happened, and a count it could not take is
    /// not zero.
    /// </summary>
    public async Task<long> CountStoredFindingsAsync(int serverId, string storyPathHash)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await EnsureTablesExistAsync(connection);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    matched_now = COUNT_BIG(*)
FROM config.analysis_findings
WHERE server_id = @serverId
AND   story_path_hash = @storyPathHash;";

        cmd.Parameters.Add(new SqlParameter("@serverId", serverId));
        cmd.Parameters.Add(new SqlParameter("@storyPathHash", storyPathHash));

        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
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
            cmd.CommandText = CleanupOldFindingsSql;
            cmd.Parameters.Add(new SqlParameter("@cutoff", DateTime.UtcNow.AddDays(-retentionDays)));
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] CleanupOldFindingsAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Muted-hash reader SQL. server_id = 0 rows are legacy all-servers mutes written by the pre-fix
    /// MCP tool path (no real server has id 0); the reader honors them as global, alongside the
    /// canonical NULL. Exposed as a const so Dashboard.Tests can pin the compat without a live SQL
    /// Server, mirroring the Darling twin's <c>PgFindingStore.GetMutedHashesSql</c>.
    /// </summary>
    public const string GetMutedHashesSql = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT story_path_hash FROM config.analysis_muted
WHERE server_id = @serverId OR server_id IS NULL OR server_id = 0;";

    /// <summary>
    /// The findings-retention purge: deletes findings older than the caller's cutoff. Exposed as a
    /// const so Dashboard.Tests can pin the retention predicate (by analysis_time, correct table)
    /// without a live SQL Server, mirroring the Darling twin's <c>PgFindingStore.CleanupOldFindingsSql</c>.
    /// Scheduled once per 24h by <c>AnalysisScheduler</c> — the store declared this cleanup but nothing
    /// invoked it, so config.analysis_findings previously grew unbounded.
    /// </summary>
    public const string CleanupOldFindingsSql = "DELETE FROM config.analysis_findings WHERE analysis_time < @cutoff;";

    /// <summary>
    /// #3916 PR B: the muted story hashes for one server on a connection of its own — the read the shared
    /// <c>AnalysisNotificationService</c> re-checks at its hold-back flush, so a mute applied inside the
    /// window drops the queued page. Fails OPEN (an empty set, logged): the finding already passed the
    /// queue-time mute filter, so an unreadable registry means "not muted".
    /// </summary>
    public async Task<IReadOnlySet<string>> GetMutedStoryHashesAsync(int serverId)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await EnsureTablesExistAsync(connection);
            return await GetMutedHashesAsync(connection, serverId);
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerFindingStore] GetMutedStoryHashesAsync failed (treated as not muted): {ex.Message}");
            return new HashSet<string>();
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
            cmd.CommandText = GetMutedHashesSql;

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

        // D2: both GetRecentFindingsAsync and GetLatestFindingsAsync now select
        // remediation_action_json at ordinal 19 (GetLatest previously omitted it, forcing the
        // Recommendations reader onto GetRecent + a manual latest-run trim); the field-count guard
        // stays for safety. The BUILT action is deserialized via the SAME serializer the alert
        // path uses, so the Recommendations surface can drive Apply + the two-sided consent gate
        // from storage.
        if (reader.FieldCount > 19 && !reader.IsDBNull(19))
            finding.Remediation = AlertContextSerializer.DeserializeAction(reader.GetString(19));

        return finding;
    }
}
