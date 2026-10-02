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
    public const int CurrentVersion = 5;

    public const string CreateAnalysisFindingsTable = @"
CREATE TABLE IF NOT EXISTS analysis_findings (
    finding_id BIGINT PRIMARY KEY,
    analysis_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR,
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    severity DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    category VARCHAR NOT NULL,
    story_path VARCHAR NOT NULL,
    story_path_hash VARCHAR NOT NULL,
    story_text VARCHAR NOT NULL,
    root_fact_key VARCHAR NOT NULL,
    root_fact_value DOUBLE PRECISION,
    leaf_fact_key VARCHAR,
    leaf_fact_value DOUBLE PRECISION,
    fact_count INTEGER NOT NULL,
    incident_id VARCHAR,
    remediation_action_json VARCHAR,
    drill_down_json VARCHAR
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

    public const string CreateAnalysisFindingsTimeIndex = @"
CREATE INDEX IF NOT EXISTS idx_analysis_findings_time
    ON analysis_findings(server_id, analysis_time)";

    public const string CreateAnalysisFindingsHashIndex = @"
CREATE INDEX IF NOT EXISTS idx_analysis_findings_hash
    ON analysis_findings(story_path_hash)";

    public const string CreateAnalysisMutedHashIndex = @"
CREATE INDEX IF NOT EXISTS idx_analysis_muted_hash
    ON analysis_muted(story_path_hash)";

    /// <summary>
    /// Returns all analysis table creation statements.
    /// </summary>
    public static IEnumerable<string> GetAllTableStatements()
    {
        yield return CreateAnalysisFindingsTable;
        yield return CreateAnalysisMutedTable;
    }

    /// <summary>
    /// Returns migration statements for analysis schema upgrades.
    /// </summary>
    public static IEnumerable<string> GetMigrationStatements(int fromVersion)
    {
        if (fromVersion < 2)
        {
            // v2: the server-metadata columns the config audit reads (reported for context; no check branches on them)
            yield return "ALTER TABLE servers ADD COLUMN IF NOT EXISTS sql_engine_edition INTEGER DEFAULT 0";
            yield return "ALTER TABLE servers ADD COLUMN IF NOT EXISTS sql_major_version INTEGER DEFAULT 0";
        }

        if (fromVersion < 3)
        {
            // v3: incident grouping (correlate-and-focus slice 2). Nullable — DuckDB ADD COLUMN
            // cannot be NOT NULL; the writer always supplies a value (empty for healthy runs).
            yield return "ALTER TABLE analysis_findings ADD COLUMN IF NOT EXISTS incident_id VARCHAR";
        }

        if (fromVersion < 4)
        {
            // v4: the built RemediationAction persisted as JSON (remediation copy-paste command,
            // mirroring Darling's remediation_action_json). Nullable — DuckDB ADD COLUMN cannot be
            // NOT NULL; the writer supplies NULL for findings that carry no execution shape. On
            // read-back the Recommendations reader deserializes it and the shared renderer turns it
            // into the copy-paste T-SQL, so a Lite card produces the SAME command a Darling card does.
            yield return "ALTER TABLE analysis_findings ADD COLUMN IF NOT EXISTS remediation_action_json VARCHAR";
        }

        if (fromVersion < 5)
        {
            /* v5 (#2060): the persisted CAPPED drill-down beside the built action — the evidence rows
               (parameter-sensitive plans, spill queries) previously existed only on the write path, so
               get_analysis_findings could count them but never enumerate them. Nullable, no backfill:
               pre-upgrade findings honestly return no drill-down and age out with finding retention. */
            yield return "ALTER TABLE analysis_findings ADD COLUMN IF NOT EXISTS drill_down_json VARCHAR";
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
    }
}
