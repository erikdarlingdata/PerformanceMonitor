namespace PerformanceMonitor.Exporter;

/// <summary>One table's entry inside a run manifest.</summary>
public sealed class ManifestTableEntry
{
    public required string Table { get; init; }
    public required string OutputFile { get; init; }
    public required long RowCount { get; init; }
    public required long ByteSize { get; init; }
    public required string Sha256 { get; init; }
    public required List<ManifestColumn> Columns { get; init; }
    public required double StoreQueryElapsedMs { get; init; }
    public required double WriteElapsedMs { get; init; }

    /// <summary>
    /// The split of <see cref="StoreQueryElapsedMs"/>: store reads, connect, window-cache build, pacing sleeps,
    /// parsing, content fetched. Null for a table whose exporter does not measure them; each field inside is null when
    /// that one is not measured.
    /// </summary>
    public ExportTimingSnapshot? Timings { get; init; }

    /// <summary>
    /// Count of keys whose content (plan XML or query text) was NULL in the output because
    /// the dimension row had already been purged or was never written. Zero for every table
    /// other than the two content dimension tables.
    /// </summary>
    public required long MissingContentCount { get; init; }

    /// <summary>
    /// Count of dimension keys (plan_shape / query_text) that were skipped because the local
    /// state store already recorded them as exported for this source_store. Zero for every
    /// table other than the two content dimension tables, and zero whenever --ignore-state
    /// is passed.
    /// </summary>
    public long KnownKeysSkipped { get; init; }

    /// <summary>
    /// Set on the `plan_shape` entry only, and only when the plan-fetch gate ran (not under --ignore-state or
    /// --plan-fetch-all): keys held back this run because their accumulated executions are under 5 and their
    /// accumulated CPU is under 1 s. Their plan XML is fetched in a later hour if they cross the threshold.
    /// </summary>
    public long? BelowThresholdDeferred { get; init; }

    /// <summary>
    /// Set on the `plan_shape` entry only, and only when the plan-fetch gate ran: keys that qualified this run
    /// after being held back in an earlier hour (their plan_shape row arrives later than their first stats rows).
    /// </summary>
    public long? DeferredNowFetched { get; init; }

    /// <summary>Set on the `plan_features` entry only: distinct plan digests parsed by this run (not already known).</summary>
    public long? FeatureDigestsParsed { get; init; }

    /// <summary>Set on the `plan_features` entry only: distinct plan digests skipped because a prior run already parsed them at the same feature_version.</summary>
    public long? FeatureDigestsSkipped { get; init; }

    /// <summary>
    /// Set on the `procedure_plan` entry only: count of procedure_stats rows in the window with no plan digest
    /// and no inline XML, but a byte size recorded -- rows over Darling's plan-capture size cap.
    /// </summary>
    public long? OversizedPlanRows { get; init; }

    /// <summary>
    /// Set on the `procedure_plan` entry only, and only when procedure plan reuse ran in `shadow` or `on`. Null when
    /// the mode is off, so the entry keeps its old JSON shape.
    /// </summary>
    public ProcedurePlanReuseStats? ProcedurePlanReuse { get; init; }

    /// <summary>
    /// Set on the `query_store_stats` entry only, and only when `--query-store-source` is `wide` or `compare`. Null in
    /// raw mode, so the entry keeps its old JSON shape.
    /// </summary>
    public QueryStoreSourceStats? QueryStoreSource { get; init; }

    /// <summary>
    /// The column this table's rows are filtered/ordered by. Null only for a dimension table with no per-row time
    /// column of its own. For every core table, this equals <see cref="CoreTableMetadata.TimeColumn"/>, which is
    /// fixed by the file contract.
    /// </summary>
    public string? TimeColumn { get; init; }

    /// <summary>`window`, `snapshot` or `dimension`. Defaults to `window`; every exporter's entry is corrected to its
    /// real kind by <see cref="CoreTableMetadata.WithMetadata"/> or by the optional-table path before it lands in a
    /// manifest.</summary>
    public string Kind { get; init; } = "window";

    /// <summary>The dimension key columns for a `dimension`-kind table, in the loader's key order. Null for `window`/`snapshot` tables.</summary>
    public IReadOnlyList<string>? KeyColumns { get; init; }

    /// <summary>
    /// Set on the `query_snapshots` optional-manifest entry only: the count of rows in the window whose `query_plan`
    /// text was present but the plan's `QueryPlanHash` could not be extracted (malformed XML). Null for every other
    /// table.
    /// </summary>
    public long? UnparsedPlanRows { get; init; }

    /// <summary>
    /// Set on the `pg_statement_text` entry only: count of keys whose `server_name` did not resolve to a
    /// `collect.servers` row. These keys are dropped rather than failing the table. Null for every other table.
    /// </summary>
    public long? UnresolvedKeys { get; init; }

    /// <summary>
    /// `pg_statement_text` only: keys dropped this run by the fetch threshold (calls &gt;= 2 or total exec time
    /// &gt;= 100 ms). Null for every other table, and when the gate was off for the run.
    /// </summary>
    public long? BelowThresholdSkipped { get; init; }

    /// <summary>
    /// The <see cref="TablePolicy.Version"/> constant applied by <see cref="ColumnPolicies.For"/>. Set for every
    /// optional table that has a policy; null otherwise, including every core table. A loader
    /// may refuse a manifest that is missing this for job_history, config_alert_log, pg_log_events or analysis_findings.
    /// </summary>
    public int? PolicyVersion { get; init; }

    /// <summary>The grain this entry belongs to (e.g. `daily`, `change`, `hourly`). Null for tables that predate the grains split.</summary>
    public string? Grain { get; init; }

    /// <summary>The store-side source table this entry was read from, when it differs from <see cref="Table"/>. Null otherwise.</summary>
    public string? SourceTable { get; init; }

    /// <summary>The inclusive lower bound of the window actually read for this table. Null when the entry has no per-table window of its own.</summary>
    public DateTime? ReadFrom { get; init; }

    /// <summary>The exclusive upper bound of the window actually read for this table. Null when the entry has no per-table window of its own.</summary>
    public DateTime? ReadTo { get; init; }

    /// <summary>Count of servers missing from this daily entry's expected set. Null outside the daily grain.</summary>
    public long? DailyMissingServers { get; init; }

    /// <summary>The change-tracking key columns used to detect row changes for this table. Null outside the change grain.</summary>
    public IReadOnlyList<string>? ChangeKeyColumns { get; init; }

    /// <summary>True when this entry is a full-baseline change-grain export rather than an incremental delta. Null outside the change grain.</summary>
    public bool? ChangeBaseline { get; init; }

    /// <summary>Count of change-grain rows suppressed by dedup/collapse logic. Null outside the change grain.</summary>
    public long? ChangeRowsSuppressed { get; init; }

    /// <summary>The rollup version applied when this entry was produced by an hourly rollup. Null otherwise.</summary>
    public int? RollupVersion { get; init; }

    /// <summary>A fingerprint of this table's source schema at export time, used to detect drift. Null when not computed.</summary>
    public string? SchemaFingerprint { get; init; }
}

/// <summary>One column's name and DuckDB type, as recorded in the manifest for a table.</summary>
public sealed class ManifestColumn
{
    public required string Name { get; init; }
    public required string DuckDbType { get; init; }
}

/// <summary>
/// The run manifest, written once per (store, window) after every requested table has
/// exported successfully. A failed table means no manifest is written at all: a manifest's
/// existence is the run's success signal for the downstream loader.
/// </summary>
public sealed class Manifest
{
    public required string Store { get; init; }
    public required DateTime FromUtc { get; init; }
    public required DateTime ToUtc { get; init; }
    public required string ExporterVersion { get; init; }
    public required int? DarlingSchemaVersion { get; init; }
    public required List<ManifestTableEntry> Tables { get; init; }

    /// <summary>
    /// The straggler guard's last observed count of open writing transactions that started before the window's
    /// collection cutoff. 0 when the first check already found none, or when the run's table set needed no straggler
    /// check (dry-run). This never fails a run; it only records what the guard saw.
    /// </summary>
    public long StragglerWarning { get; init; }

    /// <summary>Total seconds the straggler guard spent waiting (0, 60, 120, ... up to 240; never more than 5 minutes).</summary>
    public double StragglerWaitSeconds { get; init; }

    /// <summary>
    /// The `--also`/`--tables` optional names that were skipped rather than exported: an unknown registry name, or a
    /// table absent on this store (`to_regclass(...) IS NULL`). Null when no optional table was requested or every
    /// requested optional table ran. Written on the SEPARATE optional manifest, never the core one -- the core
    /// manifest is written and can be uploaded before any optional table is even attempted.
    /// </summary>
    public List<SkippedOptionalTable>? SkippedOptional { get; init; }

    /// <summary>
    /// Optional tables that were requested and attempted but failed. Recorded on the optional manifest, alongside the
    /// tables that succeeded. Null when no optional table failed.
    /// </summary>
    public List<FailedOptionalTable>? FailedOptional { get; init; }
}

/// <summary>One `--also`/`--tables` optional name the exporter declined to run.</summary>
public sealed class SkippedOptionalTable
{
    public required string Table { get; init; }

    /// <summary>`unknown` (not in the registry) or `absent` (not on this store).</summary>
    public required string Reason { get; init; }
}

/// <summary>One optional table that ran but failed. It gets no output file.</summary>
public sealed class FailedOptionalTable
{
    public required string Table { get; init; }
    public required string Error { get; init; }
}
