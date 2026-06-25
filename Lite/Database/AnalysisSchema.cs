using System.Collections.Generic;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// DuckDB table schema definitions for the analysis engine.
/// Separate from main Schema.cs with independent versioning.
/// </summary>
public static class AnalysisSchema
{
    /// <summary>
    /// Analysis schema version. Independent of main schema version.
    /// </summary>
    public const int CurrentVersion = 3;

    public const string CreateAnalysisFindingsTable = @"
CREATE TABLE IF NOT EXISTS analysis_findings (
    finding_id BIGINT PRIMARY KEY,
    analysis_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR,
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    severity DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    category VARCHAR NOT NULL,
    story_path VARCHAR NOT NULL,
    story_path_hash VARCHAR NOT NULL,
    story_text VARCHAR NOT NULL,
    root_fact_key VARCHAR NOT NULL,
    root_fact_value DOUBLE,
    leaf_fact_key VARCHAR,
    leaf_fact_value DOUBLE,
    fact_count INTEGER NOT NULL,
    incident_id VARCHAR
)";

    public const string CreateAnalysisMutedTable = @"
CREATE TABLE IF NOT EXISTS analysis_muted (
    mute_id BIGINT PRIMARY KEY,
    server_id INTEGER,
    database_name VARCHAR,
    story_path_hash VARCHAR NOT NULL,
    story_path VARCHAR NOT NULL,
    muted_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason VARCHAR
)";

    public const string CreateAnalysisExclusionsTable = @"
CREATE TABLE IF NOT EXISTS analysis_exclusions (
    exclusion_id BIGINT PRIMARY KEY,
    exclusion_type VARCHAR NOT NULL,
    exclusion_value VARCHAR NOT NULL,
    server_id INTEGER,
    database_name VARCHAR,
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description VARCHAR
)";

    public const string CreateAnalysisThresholdsTable = @"
CREATE TABLE IF NOT EXISTS analysis_thresholds (
    threshold_id BIGINT PRIMARY KEY,
    category VARCHAR NOT NULL,
    fact_key VARCHAR NOT NULL,
    threshold_type VARCHAR NOT NULL,
    threshold_value DOUBLE NOT NULL,
    server_id INTEGER,
    database_name VARCHAR,
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)";

    public const string CreateAnalysisFindingsTimeIndex = @"
CREATE INDEX IF NOT EXISTS idx_analysis_findings_time
    ON analysis_findings(server_id, analysis_time)";

    public const string CreateAnalysisFindingsHashIndex = @"
CREATE INDEX IF NOT EXISTS idx_analysis_findings_hash
    ON analysis_findings(story_path_hash)";

    public const string CreateAnalysisMutedHashIndex = @"
CREATE INDEX IF NOT EXISTS idx_analysis_muted_hash
    ON analysis_muted(story_path_hash)";

    public const string CreateAnalysisThresholdsLookupIndex = @"
CREATE INDEX IF NOT EXISTS idx_analysis_thresholds_lookup
    ON analysis_thresholds(category, fact_key)";

    /// <summary>
    /// Returns all analysis table creation statements.
    /// </summary>
    public static IEnumerable<string> GetAllTableStatements()
    {
        yield return CreateAnalysisFindingsTable;
        yield return CreateAnalysisMutedTable;
        yield return CreateAnalysisExclusionsTable;
        yield return CreateAnalysisThresholdsTable;
    }

    /// <summary>
    /// Returns migration statements for analysis schema upgrades.
    /// </summary>
    public static IEnumerable<string> GetMigrationStatements(int fromVersion)
    {
        if (fromVersion < 2)
        {
            // v2: Add server metadata columns for edition-aware analysis
            yield return "ALTER TABLE servers ADD COLUMN IF NOT EXISTS sql_engine_edition INTEGER DEFAULT 0";
            yield return "ALTER TABLE servers ADD COLUMN IF NOT EXISTS sql_major_version INTEGER DEFAULT 0";
        }

        if (fromVersion < 3)
        {
            // v3: incident grouping (correlate-and-focus slice 2). Nullable — DuckDB ADD COLUMN
            // cannot be NOT NULL; the writer always supplies a value (empty for healthy runs).
            yield return "ALTER TABLE analysis_findings ADD COLUMN IF NOT EXISTS incident_id VARCHAR";
        }
    }

    /// <summary>
    /// Returns all analysis index creation statements.
    /// </summary>
    public static IEnumerable<string> GetAllIndexStatements()
    {
        yield return CreateAnalysisFindingsTimeIndex;
        yield return CreateAnalysisFindingsHashIndex;
        yield return CreateAnalysisMutedHashIndex;
        yield return CreateAnalysisThresholdsLookupIndex;
    }
}
