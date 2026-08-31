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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// How a measure's underlying column behaves over time — the single fact that decides which
/// aggregations are legal and which physical column the compiler aggregates. The collector schema
/// (<see cref="PerformanceMonitor.Collectors.CollectorColumn"/>) carries ONLY {name, type,
/// precision, scale}; the archetype (and unit, dimension roles) are this HAND-AUTHORED layer on top,
/// pinned to the collector's real columns by <c>DarlingComposeTests</c>.
/// </summary>
public enum MeasureArchetype
{
    /// <summary>An ever-increasing counter (e.g. <c>wait_time_ms</c>) that ships with a paired
    /// <c>delta_*</c> column. Aggregation operates on the DELTA (summing a raw counter is meaningless);
    /// SUM(delta) over a bucket is throughput, AVG/MIN/MAX(delta) the per-sample spread.</summary>
    Cumulative,

    /// <summary>A column that is ALREADY a per-interval delta (e.g. <c>delta_wait_time_ms</c> read as a
    /// first-class measure). Aggregation operates on the column itself.</summary>
    Delta,

    /// <summary>A point-in-time reading (e.g. <c>sqlserver_cpu_utilization</c> percent). AVG/MIN/MAX only —
    /// summing a gauge over time is a category error (the grain-trap the cpu measure exists to prove).</summary>
    Gauge,

    /// <summary>One row per event (e.g. <c>long_query_completions</c>). COUNT(*) for the event rate, plus
    /// AVG/MIN/MAX and <c>percentile_cont</c> over an event column. The only archetype percentile is legal on.</summary>
    PerEvent,
}

/// <summary>Whether a measure is a single column-backed metric or a numerator/denominator ratio.</summary>
public enum MeasureKind
{
    /// <summary>A single column (or its delta) aggregated by one function.</summary>
    Scalar,

    /// <summary>A ratio of two scalar measures on the SAME source — compiled as
    /// <c>SUM(num)::float / NULLIF(SUM(den), 0)</c>.</summary>
    Ratio,
}

/// <summary>How a <see cref="MeasureKind.Ratio"/> combines its operands over the window.</summary>
public enum MeasureRatioMode
{
    /// <summary><c>SUM(num) / NULLIF(SUM(den), 0)</c> — the cumulative/delta form (operands have a non-null
    /// <see cref="ComposeMeasure.AggregationColumn"/>): a throughput or execution-weighted ratio.</summary>
    Sum,

    /// <summary><c>AVG(num) / NULLIF(AVG(den), 0)</c> — the gauge form: a ratio of two point-in-time readings
    /// (both operands must be <see cref="MeasureArchetype.Gauge"/>, whose AggregationColumn is null).</summary>
    Avg,

    /// <summary><c>SUM(value * weight) / NULLIF(SUM(weight), 0)</c> — the execution-WEIGHTED average of a
    /// column that is ITSELF a pre-aggregated per-interval average (e.g. Query Store <c>avg_duration_us</c>,
    /// weighted by <c>execution_count</c>). This is the correct weighted mean across pre-aggregated intervals,
    /// NOT a plain avg-of-avgs (which over-weights sparse intervals — design §2). Its two operands are RAW
    /// source columns — <see cref="ComposeMeasure.WeightedValueColumn"/> (the per-interval average) and
    /// <see cref="ComposeMeasure.WeightColumn"/> (the interval's execution weight) — not numerator/denominator
    /// measures, because the pre-aggregated averages are deliberately NOT exposed as summable scalar measures.</summary>
    Weighted,

    /// <summary><c>SUM(value * weight)</c> — <see cref="Weighted"/>'s numerator with NO denominator: the window
    /// TOTAL of a pre-aggregated per-interval average times its weight (e.g. Query Store total CPU =
    /// SUM(<c>avg_cpu_time_us</c> * <c>execution_count</c>), each interval's total consumption). Rides the Ratio
    /// kind — not a Scalar — because its aggregation is part of the definition, exactly like a ratio's
    /// (<see cref="ComposeMeasure.ValidAggs"/> is empty), and because its operands are the same RAW
    /// <see cref="ComposeMeasure.WeightedValueColumn"/> / <see cref="ComposeMeasure.WeightColumn"/> pair,
    /// not numerator/denominator measures (#2732).</summary>
    WeightedSum,
}

/// <summary>The fixed aggregation vocabulary. <c>percentile_cont</c> is legal ONLY on
/// <see cref="MeasureArchetype.PerEvent"/> measures (the compiler enforces it regardless of the
/// measure's declared <see cref="ComposeMeasure.ValidAggs"/>).</summary>
public enum ComposeAggregate
{
    Sum,
    Avg,
    Min,
    Max,
    Count,
    PercentileCont,
}

/// <summary>The fixed time-bucket vocabulary. <see cref="None"/> is the ranked / scalar mode (no
/// bucketing); the rest map 1:1 to a Postgres <c>date_trunc</c> unit.</summary>
public enum ComposeTimeBucket
{
    None,
    Minute,
    Hour,
    Day,

    /// <summary>Adaptive: the compiler resolves this to a concrete grain (minute/hour/day) from the panel's
    /// window at compile time (see <see cref="MeasureCatalog.ResolveBucket"/>), so any range renders a readable,
    /// bounded point count. Never reaches <c>date_trunc</c> as-is.</summary>
    Auto,
}

/// <summary>The fixed filter-operator vocabulary — each maps to a compile-time SQL operator string
/// (never a user string), and the value is ALWAYS a bound parameter.</summary>
public enum ComposeFilterOp
{
    /// <summary><c>col = ANY($n)</c> — value is one literal or a list (multi-select).</summary>
    Eq,

    /// <summary><c>col &lt;&gt; ALL($n)</c>.</summary>
    Neq,

    /// <summary><c>col LIKE $n</c> — only on a <see cref="ComposeDimension.Likeable"/> dimension.</summary>
    Like,

    Gt,
    Gte,
    Lt,
    Lte,
}

/// <summary>One unit inside a <see cref="ComposeUnitFamily"/>. <see cref="BaseFactor"/> is how many of
/// the family's smallest unit this unit equals, so converting a value from unit A to unit B is
/// <c>value * A.BaseFactor / B.BaseFactor</c> (both in the same family).</summary>
public sealed record ComposeUnit(string Name, double BaseFactor);

/// <summary>A convertible unit family — the compiler validates a spec's <c>unit</c> is one of these and
/// scales the aggregate by the from→to factor; the catalog serves the list so the composer's unit
/// picker matches.</summary>
public sealed record ComposeUnitFamily(string Name, IReadOnlyList<ComposeUnit> Units)
{
    public ComposeUnit? Unit(string name) =>
        Units.FirstOrDefault(u => string.Equals(u.Name, name, StringComparison.Ordinal));

    public bool Has(string name) => Unit(name) is not null;
}

/// <summary>
/// One dimension a source can be filtered or grouped by. <see cref="Column"/> is the physical column
/// (usually == <see cref="Name"/>); <see cref="Likeable"/> gates the <c>LIKE</c> operator;
/// <see cref="ViaModuleJoin"/> marks the query_stats dimensions stitched read-time from
/// <c>procedure_stats</c> via the #1568 sql_handle join (window-bounded by the compiler).
///
/// <para>A module-join dimension's misses are NULL (every ad-hoc statement), so the compiler never emits
/// the bare joined column (#2737): it folds NULL into <see cref="FallbackColumn"/> when one is declared
/// (<c>COALESCE(m.col, f.fallback)</c> — the <c>statement</c> dimension, which keeps ad-hoc statements
/// distinct by query_hash), else into the <see cref="MeasureCatalog.AdHocLabel"/> sentinel — so the
/// dimension's VALUE is never NULL, groups are always labeled, and every filter operator (eq/neq/like/…)
/// acts on that value with ordinary text semantics. <see cref="FallbackColumn"/> must be a real payload
/// column of <see cref="SourceTable"/> (pinned by test); it is only meaningful with
/// <see cref="ViaModuleJoin"/>.</para>
/// </summary>
public sealed record ComposeDimension(string SourceTable, string Name, string Column, bool Likeable, bool ViaModuleJoin = false, string? FallbackColumn = null);

/// <summary>
/// One event-annotation SOURCE (design D5): a collector event table whose rows can be overlaid as point
/// markers on a time-series panel. <see cref="TimeColumn"/> is the event's OWN time column (when the event
/// happened — e.g. <c>deadlock_time</c>, not the collector's <c>collection_time</c>) and
/// <see cref="LabelColumn"/> a short text column for the marker tooltip. Both — like a measure's columns —
/// are pinned to the owning collector's <c>PayloadColumns</c> by <c>DarlingComposeTests</c>, so the annotation
/// compiler emits them schema-qualified (<c>collect.&lt;table&gt;</c>) and never touches a caller string.
/// </summary>
public sealed record ComposeAnnotationSource(
    string Key,
    string DisplayName,
    string Category,
    string SourceTable,
    string TimeColumn,
    string LabelColumn);

/// <summary>
/// One measure — a named, composable metric. Everything identifier-bearing (<see cref="SourceTable"/>,
/// <see cref="Column"/>, <see cref="DeltaColumn"/>, <see cref="AllowedDimensions"/>) is validated against
/// the collector catalog by test, so the compiler can trust every field is a real column and emit it
/// schema-qualified without ever touching a user string.
/// </summary>
public sealed record ComposeMeasure
{
    public required string Key { get; init; }
    public required string DisplayName { get; init; }
    public required string Category { get; init; }
    public required string SourceTable { get; init; }
    public MeasureKind Kind { get; init; } = MeasureKind.Scalar;

    /// <summary>Scalar only — decides the summable column + legal aggregations.</summary>
    public MeasureArchetype Archetype { get; init; }

    /// <summary>Scalar base column (the raw counter / gauge / delta / event column). Null for a ratio.</summary>
    public string? Column { get; init; }

    /// <summary>For a <see cref="MeasureArchetype.Cumulative"/> measure, its paired <c>delta_*</c> column —
    /// the column aggregation actually operates on. Null otherwise.</summary>
    public string? DeltaColumn { get; init; }

    /// <summary>Ratio numerator/denominator — both must be Scalar measures on the SAME source.</summary>
    public string? NumeratorKey { get; init; }
    public string? DenominatorKey { get; init; }

    /// <summary>How a ratio combines its operands (<see cref="MeasureKind.Ratio"/> only): the default
    /// <see cref="MeasureRatioMode.Sum"/> for cumulative/delta operands, <see cref="MeasureRatioMode.Avg"/>
    /// for gauge operands, or <see cref="MeasureRatioMode.Weighted"/> for the execution-weighted average of a
    /// pre-aggregated per-interval average.</summary>
    public MeasureRatioMode RatioMode { get; init; } = MeasureRatioMode.Sum;

    /// <summary>For a <see cref="MeasureRatioMode.Weighted"/> or <see cref="MeasureRatioMode.WeightedSum"/>
    /// ratio: the raw source column that is itself a pre-aggregated per-interval AVERAGE (e.g. Query Store
    /// <c>avg_duration_us</c>), whose execution-weighted mean is computed as <c>SUM(value * weight) /
    /// SUM(weight)</c> (Weighted) or whose window total as <c>SUM(value * weight)</c> (WeightedSum). A real
    /// payload column of the source (pinned by test); null for every other kind/mode.</summary>
    public string? WeightedValueColumn { get; init; }

    /// <summary>For a <see cref="MeasureRatioMode.Weighted"/> or <see cref="MeasureRatioMode.WeightedSum"/>
    /// ratio: the raw source column each interval's <see cref="WeightedValueColumn"/> is weighted by (the
    /// interval's execution count, e.g. <c>execution_count</c>). A real payload column of the source (pinned
    /// by test); null otherwise.</summary>
    public string? WeightColumn { get; init; }

    public required string NativeUnit { get; init; }
    public required string DefaultUnit { get; init; }
    public required string UnitFamily { get; init; }

    public ComposeAggregate DefaultTimeAgg { get; init; }
    public required IReadOnlyList<ComposeAggregate> ValidAggs { get; init; }
    public required IReadOnlyList<string> AllowedDimensions { get; init; }

    /// <summary>
    /// The column a SUM/AVG/MIN/MAX operates on for a scalar measure: the delta for a Cumulative counter,
    /// the column itself for a Delta/PerEvent measure, and null for a Gauge (never summable — the
    /// grain-trap) or a Ratio (handled through its numerator/denominator).
    /// </summary>
    public string? AggregationColumn => Archetype switch
    {
        MeasureArchetype.Cumulative => DeltaColumn,
        MeasureArchetype.Delta => Column,
        MeasureArchetype.PerEvent => Column,
        _ => null,
    };
}

/// <summary>
/// The hand-authored catalog of named measures + dimensions + unit families that Custom Views v2 composes
/// from — the v2 twin of <c>DarlingWebEndpoints.CatalogDescriptors</c> (the v1 read allowlist). It is the
/// SOLE source of identifiers the compiler will emit: a measure names a real collector table + column, so
/// the compiler never accepts a table/column from the caller. <c>DarlingComposeTests</c> pins every
/// (table, column) against the owning collector's <c>PayloadColumns</c>, every measure's time column ==
/// the collector's <c>PrefixTimeColumnName</c>, gauges as non-summable, and ratios as referencing real
/// same-source scalars — so the catalog can never drift from the collectors it reads.
///
/// <para>B1 authors a vertical SLICE (six tables) proving every archetype end-to-end; B3 fills the
/// remaining ~30 collector tables against this same model.</para>
/// </summary>
public static class MeasureCatalog
{
    /* ─────────────────────────── unit families ─────────────────────────── */

    public const string FamilyDuration = "duration";
    public const string FamilyBytes = "bytes";
    public const string FamilyPercent = "percent";
    public const string FamilyCount = "count";
    public const string FamilyFraction = "fraction";

    /// <summary>The convertible unit families. Duration base = microsecond, bytes base = byte, fraction
    /// base = percent (so ratio = 100 percent). percent/count are single-unit (no conversion).</summary>
    public static readonly IReadOnlyList<ComposeUnitFamily> UnitFamilies = new[]
    {
        new ComposeUnitFamily(FamilyDuration, new[]
        {
            new ComposeUnit("us", 1),
            new ComposeUnit("ms", 1_000),
            new ComposeUnit("s", 1_000_000),
            new ComposeUnit("min", 60_000_000),
        }),
        new ComposeUnitFamily(FamilyBytes, new[]
        {
            new ComposeUnit("bytes", 1),
            new ComposeUnit("kb", 1024),
            new ComposeUnit("mb", 1024L * 1024),
            new ComposeUnit("gb", 1024L * 1024 * 1024),
            /* 8-KB data pages, for page-count columns (none in the B1 slice; here for B3). */
            new ComposeUnit("pages", 8192),
        }),
        new ComposeUnitFamily(FamilyPercent, new[] { new ComposeUnit("percent", 1) }),
        new ComposeUnitFamily(FamilyCount, new[] { new ComposeUnit("count", 1) }),
        new ComposeUnitFamily(FamilyFraction, new[]
        {
            new ComposeUnit("percent", 1),
            new ComposeUnit("ratio", 100),
        }),
    };

    private static readonly Dictionary<string, ComposeUnitFamily> s_familyByName =
        UnitFamilies.ToDictionary(f => f.Name, StringComparer.Ordinal);

    public static ComposeUnitFamily? Family(string name) =>
        s_familyByName.TryGetValue(name, out var f) ? f : null;

    /* ─────────────────────────── dimensions ─────────────────────────── */

    /// <summary>The label the compiler folds a module-join NULL into (#2737): the value of
    /// <c>object_name</c> for every ad-hoc statement, so the group is visibly "(ad hoc)" instead of a
    /// null-named row, and <c>eq</c>/<c>neq</c> on this literal filter to/away-from the ad-hoc population.
    /// Parenthesized so it cannot be an UNQUOTED identifier (a bracketed <c>[(ad hoc)]</c> module is
    /// technically creatable and would merge with the bucket — accepted as vanishingly unlikely). It is
    /// deliberately NOT the viewer's display literal (<c>ad hoc</c>) or query_store_stats' stored
    /// <c>Adhoc</c>: those are a computed grid column and a collected value; this one is a filterable
    /// sentinel where non-collision matters most. A compile-time constant emitted as a SQL literal —
    /// never a caller string.</summary>
    public const string AdHocLabel = "(ad hoc)";

    /// <summary>Every dimension a slice source can be filtered / grouped by. Keyed by (source, name).
    /// <c>object_name</c> and <c>statement</c> on query_stats are the only
    /// <see cref="ComposeDimension.ViaModuleJoin"/> entries — neither is a query_stats column; the
    /// compiler stitches them from procedure_stats (#1568) and folds the join's NULL misses per the
    /// <see cref="ComposeDimension"/> doc (#2737).</summary>
    public static readonly IReadOnlyList<ComposeDimension> Dimensions = new[]
    {
        new ComposeDimension("wait_stats", "wait_type", "wait_type", Likeable: true),

        new ComposeDimension("procedure_stats", "database_name", "database_name", Likeable: true),
        new ComposeDimension("procedure_stats", "schema_name", "schema_name", Likeable: true),
        new ComposeDimension("procedure_stats", "object_name", "object_name", Likeable: true),

        new ComposeDimension("query_stats", "database_name", "database_name", Likeable: true),
        new ComposeDimension("query_stats", "query_hash", "query_hash", Likeable: false),
        /* #1568: not a query_stats column — stitched from procedure_stats.object_name via sql_handle.
           NULL misses (ad-hoc statements) fold into ONE AdHocLabel group per (database, bucket) — honest
           and filterable, but still a blob; 'statement' below is the per-statement identity. */
        new ComposeDimension("query_stats", "object_name", "object_name", Likeable: true, ViaModuleJoin: true),
        /* #2737: the fallback-identity twin of object_name — procedures keep their module name, ad-hoc
           statements stay DISTINCT by query_hash instead of collapsing into the AdHocLabel bucket. This is
           the "top statements" grouping; hash labels are ugly but resolvable (query_hash is itself a
           dimension, and the stored query text keys on it). */
        new ComposeDimension("query_stats", "statement", "object_name", Likeable: true, ViaModuleJoin: true, FallbackColumn: "query_hash"),

        new ComposeDimension("file_io_stats", "database_name", "database_name", Likeable: true),
        new ComposeDimension("file_io_stats", "file_name", "file_name", Likeable: true),

        new ComposeDimension("long_query_completions", "database_name", "database_name", Likeable: true),
        new ComposeDimension("long_query_completions", "object_name", "object_name", Likeable: true),
        new ComposeDimension("long_query_completions", "result", "result", Likeable: false),

        /* ── B3 dimensions (every column pinned to the collector's PayloadColumns by test) ── */
        new ComposeDimension("latch_stats", "latch_class", "latch_class", Likeable: true),
        new ComposeDimension("spinlock_stats", "spinlock_name", "spinlock_name", Likeable: true),
        new ComposeDimension("memory_clerks", "clerk_type", "clerk_type", Likeable: true),
        new ComposeDimension("plan_cache_stats", "cacheobjtype", "cacheobjtype", Likeable: true),
        new ComposeDimension("plan_cache_stats", "objtype", "objtype", Likeable: true),

        new ComposeDimension("database_size_stats", "database_name", "database_name", Likeable: true),
        new ComposeDimension("database_size_stats", "file_name", "file_name", Likeable: true),
        new ComposeDimension("database_size_stats", "file_type_desc", "file_type_desc", Likeable: true),
        new ComposeDimension("database_size_stats", "recovery_model_desc", "recovery_model_desc", Likeable: true),

        new ComposeDimension("pvs_stats", "database_name", "database_name", Likeable: true),

        new ComposeDimension("plan_correction", "database_name", "database_name", Likeable: true),
        new ComposeDimension("plan_correction", "recommendation_state", "recommendation_state", Likeable: true),

        new ComposeDimension("index_object_stats", "database_name", "database_name", Likeable: true),
        new ComposeDimension("index_object_stats", "schema_name", "schema_name", Likeable: true),
        new ComposeDimension("index_object_stats", "table_name", "table_name", Likeable: true),
        new ComposeDimension("index_object_stats", "index_name", "index_name", Likeable: true),
        new ComposeDimension("index_object_stats", "index_type_desc", "index_type_desc", Likeable: true),

        new ComposeDimension("session_stats", "program_name", "program_name", Likeable: true),

        new ComposeDimension("waiting_tasks", "wait_type", "wait_type", Likeable: true),
        new ComposeDimension("waiting_tasks", "database_name", "database_name", Likeable: true),

        new ComposeDimension("query_snapshots", "database_name", "database_name", Likeable: true),
        new ComposeDimension("query_snapshots", "status", "status", Likeable: true),
        new ComposeDimension("query_snapshots", "wait_type", "wait_type", Likeable: true),
        new ComposeDimension("query_snapshots", "program_name", "program_name", Likeable: true),
        new ComposeDimension("query_snapshots", "login_name", "login_name", Likeable: true),
        new ComposeDimension("query_snapshots", "host_name", "host_name", Likeable: true),

        new ComposeDimension("blocked_process_reports", "database_name", "database_name", Likeable: true),
        new ComposeDimension("blocked_process_reports", "contentious_object", "contentious_object", Likeable: true),
        new ComposeDimension("blocked_process_reports", "lock_mode", "lock_mode", Likeable: true),

        new ComposeDimension("dmv_blocking_snapshots", "database_name", "database_name", Likeable: true),
        new ComposeDimension("dmv_blocking_snapshots", "contentious_object", "contentious_object", Likeable: true),
        new ComposeDimension("dmv_blocking_snapshots", "lock_mode", "lock_mode", Likeable: true),
        new ComposeDimension("dmv_blocking_snapshots", "blocking_status", "blocking_status", Likeable: true),

        new ComposeDimension("deadlocks", "database_name", "database_name", Likeable: true),

        new ComposeDimension("system_health_events", "event_type", "event_type", Likeable: true),

        new ComposeDimension("default_trace_events", "event_name", "event_name", Likeable: true),
        new ComposeDimension("default_trace_events", "database_name", "database_name", Likeable: true),
        new ComposeDimension("default_trace_events", "object_name", "object_name", Likeable: true),
        new ComposeDimension("default_trace_events", "login_name", "login_name", Likeable: true),

        new ComposeDimension("running_jobs", "job_name", "job_name", Likeable: true),

        new ComposeDimension("job_history", "job_name", "job_name", Likeable: true),
        new ComposeDimension("job_history", "step_name", "step_name", Likeable: true),
        new ComposeDimension("job_history", "run_status_desc", "run_status_desc", Likeable: true),
        new ComposeDimension("job_history", "category_name", "category_name", Likeable: true),

        new ComposeDimension("perfmon_stats", "object_name", "object_name", Likeable: true),
        new ComposeDimension("perfmon_stats", "counter_name", "counter_name", Likeable: true),
        new ComposeDimension("perfmon_stats", "instance_name", "instance_name", Likeable: true),

        new ComposeDimension("query_store_stats", "database_name", "database_name", Likeable: true),
        new ComposeDimension("query_store_stats", "module_name", "module_name", Likeable: true),
        new ComposeDimension("query_store_stats", "query_hash", "query_hash", Likeable: false),

        /* #991 AG health. Only the database grain carries measures — the replica-grain table is all
           state strings with nothing numeric to aggregate — so only its dimensions appear here.
           synchronization_state_desc / suspend_reason_desc are here for a specific reason, not for
           completeness: secondary_lag_seconds reads 0 (not NULL) while data movement is SUSPENDED, so
           without a way to filter on suspension a lag panel cannot tell a healthy zero from a suspended
           one — the exact misread the measure's own comment warns about. These two are the text columns
           that make that filterable (is_suspended cannot be a dimension: the compiler binds filter values
           as text, which would not match a boolean column). */
        new ComposeDimension("ag_database_replica_states", "ag_name", "ag_name", Likeable: true),
        new ComposeDimension("ag_database_replica_states", "database_name", "database_name", Likeable: true),
        new ComposeDimension("ag_database_replica_states", "replica_server_name", "replica_server_name", Likeable: true),
        new ComposeDimension("ag_database_replica_states", "synchronization_state_desc", "synchronization_state_desc", Likeable: true),
        new ComposeDimension("ag_database_replica_states", "suspend_reason_desc", "suspend_reason_desc", Likeable: true),
    };

    private static readonly Dictionary<(string Source, string Name), ComposeDimension> s_dimByKey =
        Dimensions.ToDictionary(d => (d.SourceTable, d.Name));

    /// <summary>
    /// The universal <c>server</c> dimension name. <c>server_name</c> is a prefix column on EVERY collector
    /// table, so server is filterable + group-by-able on every measure (the fleet / multi-server axis, Erik's
    /// Decision 1 + acceptance Flow B). It is a virtual dimension — allowed for every measure without appearing
    /// in each one's <see cref="ComposeMeasure.AllowedDimensions"/> — resolved by <see cref="Dimension"/> for
    /// any source and validated as always-allowed by <see cref="ComposeSpec"/>.
    /// </summary>
    public const string ServerDimensionName = "server";

    /// <summary>The virtual <c>server</c> dimension for a source (server_name; exact-match only, not LIKE-able).</summary>
    public static ComposeDimension ServerDimension(string source) =>
        new(source, ServerDimensionName, "server_name", Likeable: false);

    /// <summary>The dimension for (<paramref name="source"/>, <paramref name="name"/>), or null. The universal
    /// <c>server</c> dimension resolves for any known source.</summary>
    public static ComposeDimension? Dimension(string source, string name)
    {
        if (string.Equals(name, ServerDimensionName, StringComparison.Ordinal))
        {
            return IsKnownSource(source) ? ServerDimension(source) : null;
        }

        return s_dimByKey.TryGetValue((source, name), out var d) ? d : null;
    }

    /* ─────────────────────────── measures ─────────────────────────── */

    private static readonly ComposeAggregate[] CumulativeAggs = { ComposeAggregate.Sum, ComposeAggregate.Avg, ComposeAggregate.Min, ComposeAggregate.Max };
    private static readonly ComposeAggregate[] GaugeAggs = { ComposeAggregate.Avg, ComposeAggregate.Min, ComposeAggregate.Max };
    private static readonly ComposeAggregate[] PerEventAggs = { ComposeAggregate.Count, ComposeAggregate.Sum, ComposeAggregate.Avg, ComposeAggregate.Min, ComposeAggregate.Max, ComposeAggregate.PercentileCont };
    private static readonly ComposeAggregate[] NoAggs = Array.Empty<ComposeAggregate>();
    /* Pure event-count tables (deadlocks, system_health) carry no numeric per-event column worth aggregating —
       only COUNT(*) is meaningful. Their measure names a real (non-numeric) column to satisfy the schema pin;
       the compiler's COUNT path never reads it. */
    private static readonly ComposeAggregate[] CountOnlyAggs = { ComposeAggregate.Count };

    private const string CatWaits = "Waits";
    private const string CatCpu = "CPU";
    private const string CatProcedures = "Procedures";
    private const string CatQueries = "Queries";
    private const string CatFileIo = "File I/O";
    private const string CatLongQueries = "Long Queries";
    /* ── B3 categories ── */
    private const string CatLatch = "Latches & Spinlocks";
    private const string CatScheduler = "CPU Scheduler";
    private const string CatPlanCache = "Plan Cache";
    private const string CatMemory = "Memory";
    private const string CatTempdb = "TempDB";
    private const string CatDatabaseSize = "Database Size";
    private const string CatIndexes = "Indexes & Objects";
    private const string CatSessions = "Sessions";
    private const string CatBlocking = "Blocking & Deadlocks";
    private const string CatSystemHealth = "System Health";
    private const string CatDefaultTrace = "Default Trace";
    private const string CatJobs = "Agent Jobs";
    private const string CatPerfmon = "Perfmon Counters";
    private const string CatQueryStore = "Query Store";
    private const string CatAvailabilityGroups = "Availability Groups";

    private static readonly string[] WaitDims = { "wait_type" };
    private static readonly string[] ProcDims = { "database_name", "schema_name", "object_name" };
    private static readonly string[] QueryDims = { "database_name", "query_hash", "object_name", "statement" };
    private static readonly string[] FileIoDims = { "database_name", "file_name" };
    private static readonly string[] LqcDims = { "database_name", "object_name", "result" };
    private static readonly string[] NoDims = Array.Empty<string>();
    /* ── B3 per-table dimension allow-lists (server is universal, added separately) ── */
    private static readonly string[] LatchDims = { "latch_class" };
    private static readonly string[] SpinlockDims = { "spinlock_name" };
    private static readonly string[] ClerkDims = { "clerk_type" };
    private static readonly string[] PlanCacheDims = { "cacheobjtype", "objtype" };
    private static readonly string[] DbSizeDims = { "database_name", "file_name", "file_type_desc", "recovery_model_desc" };

    /* pvs_stats is per DATABASE, not per file, so it carries only the database dimension. */
    private static readonly string[] PvsDims = { "database_name" };
    private static readonly string[] IndexDims = { "database_name", "schema_name", "table_name", "index_name", "index_type_desc" };
    private static readonly string[] SessionStatsDims = { "program_name" };
    private static readonly string[] WaitingTaskDims = { "wait_type", "database_name" };
    private static readonly string[] QuerySnapshotDims = { "database_name", "status", "wait_type", "program_name", "login_name", "host_name" };
    private static readonly string[] BlockingDims = { "database_name", "contentious_object", "lock_mode" };
    private static readonly string[] DmvBlockingDims = { "database_name", "contentious_object", "lock_mode", "blocking_status" };
    private static readonly string[] DeadlockDims = { "database_name" };
    private static readonly string[] PlanCorrectionDims = { "database_name", "recommendation_state" };
    private static readonly string[] SysHealthDims = { "event_type" };
    private static readonly string[] DefaultTraceDims = { "event_name", "database_name", "object_name", "login_name" };
    private static readonly string[] RunningJobDims = { "job_name" };
    private static readonly string[] JobHistoryDims = { "job_name", "step_name", "run_status_desc", "category_name" };
    private static readonly string[] PerfmonDims = { "object_name", "counter_name", "instance_name" };
    private static readonly string[] QueryStoreDims = { "database_name", "module_name", "query_hash" };
    private static readonly string[] AgDatabaseDims = { "ag_name", "database_name", "replica_server_name", "synchronization_state_desc", "suspend_reason_desc" };

    /// <summary>The catalog. Every measure's SourceTable is a real collector; every Column/DeltaColumn is a
    /// real payload column of that collector (pinned by test).</summary>
    public static readonly IReadOnlyList<ComposeMeasure> Measures = new[]
    {
        /* ── wait_stats (server-grain; dimension wait_type) — Cumulative + Delta + Ratio ── */
        new ComposeMeasure
        {
            Key = "wait_time_ms", DisplayName = "Wait time", Category = CatWaits, SourceTable = "wait_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "wait_time_ms", DeltaColumn = "delta_wait_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = WaitDims,
        },
        new ComposeMeasure
        {
            Key = "wait_time_delta_ms", DisplayName = "Wait time (per-sample delta)", Category = CatWaits, SourceTable = "wait_stats",
            Archetype = MeasureArchetype.Delta, Column = "delta_wait_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = WaitDims,
        },
        new ComposeMeasure
        {
            Key = "signal_wait_time_ms", DisplayName = "Signal wait time", Category = CatWaits, SourceTable = "wait_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "signal_wait_time_ms", DeltaColumn = "delta_signal_wait_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = WaitDims,
        },
        new ComposeMeasure
        {
            Key = "waiting_tasks", DisplayName = "Waiting tasks", Category = CatWaits, SourceTable = "wait_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "waiting_tasks_count", DeltaColumn = "delta_waiting_tasks",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = WaitDims,
        },
        new ComposeMeasure
        {
            Key = "signal_wait_pct", DisplayName = "Signal wait %", Category = CatWaits, SourceTable = "wait_stats",
            Kind = MeasureKind.Ratio, NumeratorKey = "signal_wait_time_ms", DenominatorKey = "wait_time_ms",
            NativeUnit = "ratio", DefaultUnit = "percent", UnitFamily = FamilyFraction,
            ValidAggs = NoAggs, AllowedDimensions = WaitDims,
        },

        /* ── cpu_utilization_stats (server-grain; NO dimensions) — Gauge (the grain-trap) ── */
        new ComposeMeasure
        {
            Key = "sqlserver_cpu_utilization", DisplayName = "SQL Server CPU %", Category = CatCpu, SourceTable = "cpu_utilization_stats",
            Archetype = MeasureArchetype.Gauge, Column = "sqlserver_cpu_utilization",
            NativeUnit = "percent", DefaultUnit = "percent", UnitFamily = FamilyPercent,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "other_process_cpu_utilization", DisplayName = "Other-process CPU %", Category = CatCpu, SourceTable = "cpu_utilization_stats",
            Archetype = MeasureArchetype.Gauge, Column = "other_process_cpu_utilization",
            NativeUnit = "percent", DefaultUnit = "percent", UnitFamily = FamilyPercent,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },

        /* ── procedure_stats (database + object grain) — Cumulative (µs) ── */
        new ComposeMeasure
        {
            Key = "proc_elapsed_us", DisplayName = "Procedure elapsed time", Category = CatProcedures, SourceTable = "procedure_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "total_elapsed_time", DeltaColumn = "delta_elapsed_time",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = ProcDims,
        },
        new ComposeMeasure
        {
            Key = "proc_worker_us", DisplayName = "Procedure CPU (worker) time", Category = CatProcedures, SourceTable = "procedure_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "total_worker_time", DeltaColumn = "delta_worker_time",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = ProcDims,
        },
        new ComposeMeasure
        {
            Key = "proc_executions", DisplayName = "Procedure executions", Category = CatProcedures, SourceTable = "procedure_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "execution_count", DeltaColumn = "delta_execution_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = ProcDims,
        },

        /* ── query_stats (database + query_hash grain; object_name via #1568 join) — Cumulative (µs) ── */
        new ComposeMeasure
        {
            Key = "query_elapsed_us", DisplayName = "Query elapsed time", Category = CatQueries, SourceTable = "query_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "total_elapsed_time", DeltaColumn = "delta_elapsed_time",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = QueryDims,
        },
        new ComposeMeasure
        {
            Key = "query_worker_us", DisplayName = "Query CPU (worker) time", Category = CatQueries, SourceTable = "query_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "total_worker_time", DeltaColumn = "delta_worker_time",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = QueryDims,
        },

        /* ── file_io_stats (database + file grain) — Cumulative (bytes + ms) ── */
        new ComposeMeasure
        {
            Key = "file_read_bytes", DisplayName = "File bytes read", Category = CatFileIo, SourceTable = "file_io_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "read_bytes", DeltaColumn = "delta_read_bytes",
            NativeUnit = "bytes", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = FileIoDims,
        },
        new ComposeMeasure
        {
            Key = "file_write_bytes", DisplayName = "File bytes written", Category = CatFileIo, SourceTable = "file_io_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "write_bytes", DeltaColumn = "delta_write_bytes",
            NativeUnit = "bytes", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = FileIoDims,
        },
        new ComposeMeasure
        {
            Key = "file_io_stall_read_ms", DisplayName = "File read stall", Category = CatFileIo, SourceTable = "file_io_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "io_stall_read_ms", DeltaColumn = "delta_stall_read_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = FileIoDims,
        },
        new ComposeMeasure
        {
            Key = "file_io_stall_write_ms", DisplayName = "File write stall", Category = CatFileIo, SourceTable = "file_io_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "io_stall_write_ms", DeltaColumn = "delta_stall_write_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = FileIoDims,
        },

        /* ── long_query_completions (per-event) — PerEvent (proves count + percentile_cont) ── */
        new ComposeMeasure
        {
            Key = "lqc_duration_us", DisplayName = "Completion duration", Category = CatLongQueries, SourceTable = "long_query_completions",
            Archetype = MeasureArchetype.PerEvent, Column = "duration_microseconds",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = PerEventAggs, AllowedDimensions = LqcDims,
        },
        new ComposeMeasure
        {
            Key = "lqc_cpu_time_us", DisplayName = "Completion CPU time", Category = CatLongQueries, SourceTable = "long_query_completions",
            Archetype = MeasureArchetype.PerEvent, Column = "cpu_time_microseconds",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = PerEventAggs, AllowedDimensions = LqcDims,
        },

        /* ═══════════════════ B3 full catalog fill (remaining collector tables) ═══════════════════ */

        /* ── latch_stats (Cumulative + delta; dim latch_class) ── */
        new ComposeMeasure
        {
            Key = "latch_wait_time_ms", DisplayName = "Latch wait time", Category = CatLatch, SourceTable = "latch_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "wait_time_ms", DeltaColumn = "delta_wait_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = LatchDims,
        },
        new ComposeMeasure
        {
            Key = "latch_waiting_requests", DisplayName = "Latch waiting requests", Category = CatLatch, SourceTable = "latch_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "waiting_requests_count", DeltaColumn = "delta_waiting_requests_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = LatchDims,
        },

        /* ── spinlock_stats (Cumulative + delta; dim spinlock_name) ── */
        new ComposeMeasure
        {
            Key = "spinlock_collisions", DisplayName = "Spinlock collisions", Category = CatLatch, SourceTable = "spinlock_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "collisions", DeltaColumn = "delta_collisions",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = SpinlockDims,
        },
        new ComposeMeasure
        {
            Key = "spinlock_spins", DisplayName = "Spinlock spins", Category = CatLatch, SourceTable = "spinlock_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "spins", DeltaColumn = "delta_spins",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = SpinlockDims,
        },
        new ComposeMeasure
        {
            Key = "spinlock_backoffs", DisplayName = "Spinlock backoffs", Category = CatLatch, SourceTable = "spinlock_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "backoffs", DeltaColumn = "delta_backoffs",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = SpinlockDims,
        },

        /* ── cpu_scheduler_stats (Gauge, server-grain) ── */
        new ComposeMeasure
        {
            Key = "scheduler_runnable_pct", DisplayName = "Runnable-tasks %", Category = CatScheduler, SourceTable = "cpu_scheduler_stats",
            Archetype = MeasureArchetype.Gauge, Column = "runnable_percent",
            NativeUnit = "percent", DefaultUnit = "percent", UnitFamily = FamilyPercent,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "scheduler_runnable_tasks", DisplayName = "Runnable tasks", Category = CatScheduler, SourceTable = "cpu_scheduler_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_runnable_tasks_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },

        /* ── plan_cache_stats (Gauge; dims cacheobjtype, objtype) ── */
        new ComposeMeasure
        {
            Key = "plan_cache_total_plans", DisplayName = "Cached plans", Category = CatPlanCache, SourceTable = "plan_cache_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_plans",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = PlanCacheDims,
        },
        new ComposeMeasure
        {
            Key = "plan_cache_single_use_plans", DisplayName = "Single-use plans", Category = CatPlanCache, SourceTable = "plan_cache_stats",
            Archetype = MeasureArchetype.Gauge, Column = "single_use_plans",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = PlanCacheDims,
        },
        new ComposeMeasure
        {
            Key = "plan_cache_size_mb", DisplayName = "Plan-cache size", Category = CatPlanCache, SourceTable = "plan_cache_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_size_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = PlanCacheDims,
        },

        /* ── memory_stats (Gauge MB, server-grain) ── */
        new ComposeMeasure
        {
            Key = "mem_total_server_mb", DisplayName = "Total server memory", Category = CatMemory, SourceTable = "memory_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_server_memory_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "mem_target_server_mb", DisplayName = "Target server memory", Category = CatMemory, SourceTable = "memory_stats",
            Archetype = MeasureArchetype.Gauge, Column = "target_server_memory_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "mem_buffer_pool_mb", DisplayName = "Buffer pool", Category = CatMemory, SourceTable = "memory_stats",
            Archetype = MeasureArchetype.Gauge, Column = "buffer_pool_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "mem_available_physical_mb", DisplayName = "Available physical memory", Category = CatMemory, SourceTable = "memory_stats",
            Archetype = MeasureArchetype.Gauge, Column = "available_physical_memory_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },

        /* ── memory_clerks (Gauge; dim clerk_type) ── */
        new ComposeMeasure
        {
            Key = "clerk_memory_mb", DisplayName = "Memory-clerk allocation", Category = CatMemory, SourceTable = "memory_clerks",
            Archetype = MeasureArchetype.Gauge, Column = "memory_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = ClerkDims,
        },

        /* ── memory_grant_stats (Gauge MB + cumulative counts, server-grain) ── */
        new ComposeMeasure
        {
            Key = "grant_granted_mb", DisplayName = "Granted query memory", Category = CatMemory, SourceTable = "memory_grant_stats",
            Archetype = MeasureArchetype.Gauge, Column = "granted_memory_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "grant_used_mb", DisplayName = "Used query memory", Category = CatMemory, SourceTable = "memory_grant_stats",
            Archetype = MeasureArchetype.Gauge, Column = "used_memory_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "grant_waiters", DisplayName = "Memory-grant waiters", Category = CatMemory, SourceTable = "memory_grant_stats",
            Archetype = MeasureArchetype.Gauge, Column = "waiter_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "grant_timeouts", DisplayName = "Memory-grant timeouts", Category = CatMemory, SourceTable = "memory_grant_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "timeout_error_count", DeltaColumn = "timeout_error_count_delta",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = NoDims,
        },

        /* ── tempdb_stats (Gauge MB, server-grain) ── */
        new ComposeMeasure
        {
            Key = "tempdb_total_reserved_mb", DisplayName = "TempDB reserved", Category = CatTempdb, SourceTable = "tempdb_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_reserved_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "tempdb_version_store_mb", DisplayName = "TempDB version store", Category = CatTempdb, SourceTable = "tempdb_stats",
            Archetype = MeasureArchetype.Gauge, Column = "version_store_reserved_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "tempdb_user_object_mb", DisplayName = "TempDB user objects", Category = CatTempdb, SourceTable = "tempdb_stats",
            Archetype = MeasureArchetype.Gauge, Column = "user_object_reserved_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },

        /* ── database_size_stats (Gauge MB; per database/file) ── */
        new ComposeMeasure
        {
            Key = "dbsize_total_mb", DisplayName = "Database file size", Category = CatDatabaseSize, SourceTable = "database_size_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_size_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = DbSizeDims,
        },
        new ComposeMeasure
        {
            Key = "dbsize_used_mb", DisplayName = "Database space used", Category = CatDatabaseSize, SourceTable = "database_size_stats",
            Archetype = MeasureArchetype.Gauge, Column = "used_size_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = DbSizeDims,
        },
        new ComposeMeasure
        {
            Key = "dbsize_volume_free_mb", DisplayName = "Volume free space", Category = CatDatabaseSize, SourceTable = "database_size_stats",
            Archetype = MeasureArchetype.Gauge, Column = "volume_free_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Min, ValidAggs = GaugeAggs, AllowedDimensions = DbSizeDims,
        },
        new ComposeMeasure
        {
            Key = "dbsize_vlf_count", DisplayName = "VLF count", Category = CatDatabaseSize, SourceTable = "database_size_stats",
            Archetype = MeasureArchetype.Gauge, Column = "vlf_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = DbSizeDims,
        },

        /* ── pvs_stats (#1951 ADR persistent version store; Gauge, per database). Sizes are OFF-ROW
             versions only — MS documents persistent_version_store_size_kb as excluding in-row versions —
             so the display names say so rather than promising total version space. Max is the default time
             aggregate for the sizes, matching database_size_stats: a pressure peak inside the bucket is the
             thing worth seeing, and averaging it away is how a spike disappears at coarse grain. ── */
        new ComposeMeasure
        {
            Key = "pvs_size_mb", DisplayName = "PVS off-row size", Category = CatDatabaseSize, SourceTable = "pvs_stats",
            Archetype = MeasureArchetype.Gauge, Column = "persistent_version_store_size_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = PvsDims,
        },
        new ComposeMeasure
        {
            Key = "pvs_online_index_size_mb", DisplayName = "PVS online-index version store", Category = CatDatabaseSize, SourceTable = "pvs_stats",
            Archetype = MeasureArchetype.Gauge, Column = "online_index_version_store_size_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = PvsDims,
        },
        new ComposeMeasure
        {
            Key = "pvs_aborted_transaction_count", DisplayName = "PVS aborted transactions", Category = CatDatabaseSize, SourceTable = "pvs_stats",
            Archetype = MeasureArchetype.Gauge, Column = "current_aborted_transaction_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = PvsDims,
        },

        /* ── index_object_stats (daily; sizes + usage as a current-snapshot value — no delta column exists,
             so the cumulative usage counters are exposed as gauges over the daily snapshots) ── */
        new ComposeMeasure
        {
            Key = "index_reserved_mb", DisplayName = "Index reserved size", Category = CatIndexes, SourceTable = "index_object_stats",
            Archetype = MeasureArchetype.Gauge, Column = "reserved_mb",
            NativeUnit = "mb", DefaultUnit = "mb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = IndexDims,
        },
        new ComposeMeasure
        {
            Key = "index_total_rows", DisplayName = "Index/table rows", Category = CatIndexes, SourceTable = "index_object_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_rows",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = IndexDims,
        },
        new ComposeMeasure
        {
            Key = "index_user_seeks", DisplayName = "Index seeks (cumulative)", Category = CatIndexes, SourceTable = "index_object_stats",
            Archetype = MeasureArchetype.Gauge, Column = "user_seeks",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = IndexDims,
        },
        new ComposeMeasure
        {
            Key = "index_user_updates", DisplayName = "Index updates (cumulative)", Category = CatIndexes, SourceTable = "index_object_stats",
            Archetype = MeasureArchetype.Gauge, Column = "user_updates",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = IndexDims,
        },

        /* ── session_stats (per application; point-in-time connection counts) ── */
        new ComposeMeasure
        {
            Key = "session_connection_count", DisplayName = "Connections", Category = CatSessions, SourceTable = "session_stats",
            Archetype = MeasureArchetype.Gauge, Column = "connection_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = SessionStatsDims,
        },
        new ComposeMeasure
        {
            Key = "session_running_count", DisplayName = "Running sessions", Category = CatSessions, SourceTable = "session_stats",
            Archetype = MeasureArchetype.Gauge, Column = "running_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = SessionStatsDims,
        },

        /* ── session_summary_stats (server-wide session gauges) ── */
        new ComposeMeasure
        {
            Key = "sessum_total_sessions", DisplayName = "Total sessions", Category = CatSessions, SourceTable = "session_summary_stats",
            Archetype = MeasureArchetype.Gauge, Column = "total_sessions",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "sessum_idle_over_30min", DisplayName = "Idle sessions (>30 min)", Category = CatSessions, SourceTable = "session_summary_stats",
            Archetype = MeasureArchetype.Gauge, Column = "idle_sessions_over_30min",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = NoDims,
        },

        /* ── waiting_tasks (point-in-time current waits) ── */
        new ComposeMeasure
        {
            Key = "waiting_task_duration_ms", DisplayName = "Current wait duration", Category = CatWaits, SourceTable = "waiting_tasks",
            Archetype = MeasureArchetype.Gauge, Column = "wait_duration_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = WaitingTaskDims,
        },

        /* ── query_snapshots (point-in-time running requests) ── */
        new ComposeMeasure
        {
            Key = "snapshot_cpu_time_ms", DisplayName = "Active query CPU time", Category = CatQueries, SourceTable = "query_snapshots",
            Archetype = MeasureArchetype.Gauge, Column = "cpu_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = QuerySnapshotDims,
        },
        new ComposeMeasure
        {
            Key = "snapshot_elapsed_ms", DisplayName = "Active query elapsed time", Category = CatQueries, SourceTable = "query_snapshots",
            Archetype = MeasureArchetype.Gauge, Column = "total_elapsed_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = QuerySnapshotDims,
        },
        new ComposeMeasure
        {
            Key = "snapshot_granted_memory_gb", DisplayName = "Active query granted memory", Category = CatQueries, SourceTable = "query_snapshots",
            Archetype = MeasureArchetype.Gauge, Column = "granted_query_memory_gb",
            NativeUnit = "gb", DefaultUnit = "gb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = QuerySnapshotDims,
        },

        /* ── blocked_process_reports (per-event; wait_time_ms is the per-event value) ── */
        new ComposeMeasure
        {
            Key = "bpr_wait_time_ms", DisplayName = "Blocked-process report", Category = CatBlocking, SourceTable = "blocked_process_reports",
            Archetype = MeasureArchetype.PerEvent, Column = "wait_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Count, ValidAggs = PerEventAggs, AllowedDimensions = BlockingDims,
        },

        /* ── dmv_blocking_snapshots (per-event; wait_time_ms) ── */
        new ComposeMeasure
        {
            Key = "dmvblock_wait_time_ms", DisplayName = "DMV blocking snapshot", Category = CatBlocking, SourceTable = "dmv_blocking_snapshots",
            Archetype = MeasureArchetype.PerEvent, Column = "wait_time_ms",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Count, ValidAggs = PerEventAggs, AllowedDimensions = DmvBlockingDims,
        },

        /* ── deadlocks (per-event, count-only — no numeric per-event column) ── */
        new ComposeMeasure
        {
            Key = "deadlock_count", DisplayName = "Deadlocks", Category = CatBlocking, SourceTable = "deadlocks",
            Archetype = MeasureArchetype.PerEvent, Column = "deadlock_time",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Count, ValidAggs = CountOnlyAggs, AllowedDimensions = DeadlockDims,
        },

        /* ── plan_correction (#2028, per-event, count-only — no numeric per-event column worth charting).
           HONESTY NOTE: the collector snapshots sys.dm_db_tuning_recommendations on a schedule, so a
           long-lived recommendation lands one row PER CAPTURE — this counts captures (APC activity
           observed), not distinct recommendations, and the recommendation_state dimension is what makes
           it useful (Active vs Success vs Reverted over time). Enablement-only rows (a database with
           nothing to recommend) are included; they carry a NULL recommendation_state and group apart. */
        new ComposeMeasure
        {
            Key = "plan_correction_captures", DisplayName = "APC recommendation captures", Category = CatQueries, SourceTable = "plan_correction",
            Archetype = MeasureArchetype.PerEvent, Column = "recommendation_state",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Count, ValidAggs = CountOnlyAggs, AllowedDimensions = PlanCorrectionDims,
        },

        /* ── system_health_events (per-event, count-only) ── */
        new ComposeMeasure
        {
            Key = "syshealth_event_count", DisplayName = "system_health events", Category = CatSystemHealth, SourceTable = "system_health_events",
            Archetype = MeasureArchetype.PerEvent, Column = "event_time",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Count, ValidAggs = CountOnlyAggs, AllowedDimensions = SysHealthDims,
        },

        /* ── default_trace_events (per-event; duration_us is the per-event value) ── */
        new ComposeMeasure
        {
            Key = "deftrace_duration_us", DisplayName = "Default-trace event", Category = CatDefaultTrace, SourceTable = "default_trace_events",
            Archetype = MeasureArchetype.PerEvent, Column = "duration_us",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Count, ValidAggs = PerEventAggs, AllowedDimensions = DefaultTraceDims,
        },

        /* ── running_jobs (point-in-time current run duration; dim job_name) ── */
        new ComposeMeasure
        {
            Key = "runningjob_current_duration_s", DisplayName = "Running-job duration", Category = CatJobs, SourceTable = "running_jobs",
            Archetype = MeasureArchetype.Gauge, Column = "current_duration_seconds",
            NativeUnit = "s", DefaultUnit = "s", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = RunningJobDims,
        },

        /* ── job_history (per job-run; run_duration_seconds is the per-event value) ── */
        new ComposeMeasure
        {
            Key = "jobhistory_run_duration_s", DisplayName = "Job run", Category = CatJobs, SourceTable = "job_history",
            Archetype = MeasureArchetype.PerEvent, Column = "run_duration_seconds",
            NativeUnit = "s", DefaultUnit = "s", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Count, ValidAggs = PerEventAggs, AllowedDimensions = JobHistoryDims,
        },

        /* ── perfmon_stats (EAV: the counter value; the unit VARIES per counter — cntr_type is not stored — so
             it is tagged 'count' as an honest placeholder, and the user picks the counter via counter_name) ── */
        new ComposeMeasure
        {
            Key = "perfmon_value", DisplayName = "Perfmon counter value", Category = CatPerfmon, SourceTable = "perfmon_stats",
            Archetype = MeasureArchetype.Gauge, Column = "cntr_value",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = PerfmonDims,
        },
        new ComposeMeasure
        {
            Key = "perfmon_value_delta", DisplayName = "Perfmon counter (per-interval delta)", Category = CatPerfmon, SourceTable = "perfmon_stats",
            Archetype = MeasureArchetype.Delta, Column = "delta_cntr_value",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = PerfmonDims,
        },

        /* ── query_store_stats. execution_count is a per-interval count (SUM over the window = total executions);
             max_duration/max_cpu are per-interval maxima (MAX over the window = the window peak). The
             pre-aggregated AVG columns (avg_duration_us / avg_cpu_time_us) are NOT summable — a plain avg-of-avgs
             over-weights sparse intervals (design §2) — but their correct execution-WEIGHTED average across
             intervals now ships as a Weighted ratio (SUM(avg * execution_count) / SUM(execution_count)), the
             product-sum form MeasureRatioMode.Weighted was added for. ── */
        new ComposeMeasure
        {
            Key = "qs_executions", DisplayName = "Query Store executions", Category = CatQueryStore, SourceTable = "query_store_stats",
            Archetype = MeasureArchetype.Delta, Column = "execution_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = QueryStoreDims,
        },
        new ComposeMeasure
        {
            Key = "qs_max_duration_us", DisplayName = "Query Store peak duration", Category = CatQueryStore, SourceTable = "query_store_stats",
            Archetype = MeasureArchetype.Gauge, Column = "max_duration_us",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = QueryStoreDims,
        },
        new ComposeMeasure
        {
            Key = "qs_max_cpu_us", DisplayName = "Query Store peak CPU time", Category = CatQueryStore, SourceTable = "query_store_stats",
            Archetype = MeasureArchetype.Gauge, Column = "max_cpu_time_us",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = QueryStoreDims,
        },
        /* The execution-weighted averages of the pre-aggregated per-interval averages (see the Weighted note
           above): SUM(avg_duration_us * execution_count) / SUM(execution_count), the correct mean per execution
           across the window — not an avg-of-avgs. Ratio measures, so ValidAggs is empty (a ratio's aggregation
           is fixed). */
        new ComposeMeasure
        {
            Key = "qs_avg_duration_us", DisplayName = "Query Store avg duration / execution", Category = CatQueryStore, SourceTable = "query_store_stats",
            Kind = MeasureKind.Ratio, RatioMode = MeasureRatioMode.Weighted,
            WeightedValueColumn = "avg_duration_us", WeightColumn = "execution_count",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = QueryStoreDims,
        },
        new ComposeMeasure
        {
            Key = "qs_avg_cpu_us", DisplayName = "Query Store avg CPU / execution", Category = CatQueryStore, SourceTable = "query_store_stats",
            Kind = MeasureKind.Ratio, RatioMode = MeasureRatioMode.Weighted,
            WeightedValueColumn = "avg_cpu_time_us", WeightColumn = "execution_count",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = QueryStoreDims,
        },
        /* TOTAL consumption over the window (#2732): SUM(avg * execution_count) — the Weighted ratios'
           numerator with no denominator (MeasureRatioMode.WeightedSum), since avg * execution_count IS the
           interval's total. This is what "what consumed the most CPU" ranks by — a per-execution average
           would rank a query that ran once at 30s over one that burned an hour at 200ms a call. Default
           unit 's' rather than 'ms': a window total across a whole store runs to seconds-to-hours. */
        new ComposeMeasure
        {
            Key = "qs_total_duration_us", DisplayName = "Query Store total duration", Category = CatQueryStore, SourceTable = "query_store_stats",
            Kind = MeasureKind.Ratio, RatioMode = MeasureRatioMode.WeightedSum,
            WeightedValueColumn = "avg_duration_us", WeightColumn = "execution_count",
            NativeUnit = "us", DefaultUnit = "s", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = QueryStoreDims,
        },
        new ComposeMeasure
        {
            Key = "qs_total_cpu_us", DisplayName = "Query Store total CPU time", Category = CatQueryStore, SourceTable = "query_store_stats",
            Kind = MeasureKind.Ratio, RatioMode = MeasureRatioMode.WeightedSum,
            WeightedValueColumn = "avg_cpu_time_us", WeightColumn = "execution_count",
            NativeUnit = "us", DefaultUnit = "s", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = QueryStoreDims,
        },

        /* ── ag_database_replica_states (#991). Every one is a Gauge: the DMV recomputes queue depth, rate
             and lag from the CURRENT backlog each read, so none is a counter with a delta — SUM over a
             window would be a category error, exactly the trap the cpu_utilization gauge documents. The
             queues default to Max (the worst backlog in the bucket is the signal), the rates to Avg
             (sustained throughput), and lag to Max (the worst lag is what an RPO conversation is about).

             The two rates are KB/SECOND but the catalog has no per-second family, so they ride the bytes
             family — correct under conversion, since kb/s → mb/s scales by the same factor — with the
             per-second nature stated in the display name rather than implied by a unit the picker would
             render as a plain size.

             SUSPENSION is the trap under all of these, and it does NOT hit them the same way. Measured on a
             live 2022 AG: secondary_lag_seconds ACCRUES while suspended (the docs claim it reads 0 — either
             way a lag panel is safe, it can only over-report), but every other measure here goes STALE.
             log_send_queue_size reads NULL, redo_queue_size FREEZES at its last value, and — the sharp one —
             ag_est_redo_drain_min is queue ÷ rate with both frozen, so it holds a small, static, reassuring
             number (0.0144 min, flat across a 45 s suspension) when the true answer is "never, movement is
             stopped". So a suspended replica can make a drain or queue panel look HEALTHIER than reality,
             never worse. That is why the two state columns are dimensions rather than just documented:
             filter to synchronization_state_desc <> 'NOT SYNCHRONIZING', or group by suspend_reason_desc,
             before trusting any panel on this source to say something has recovered. ── */
        new ComposeMeasure
        {
            Key = "ag_log_send_queue", DisplayName = "AG log send queue", Category = CatAvailabilityGroups, SourceTable = "ag_database_replica_states",
            Archetype = MeasureArchetype.Gauge, Column = "log_send_queue_size",
            NativeUnit = "kb", DefaultUnit = "kb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = AgDatabaseDims,
        },
        new ComposeMeasure
        {
            Key = "ag_redo_queue", DisplayName = "AG redo queue", Category = CatAvailabilityGroups, SourceTable = "ag_database_replica_states",
            Archetype = MeasureArchetype.Gauge, Column = "redo_queue_size",
            NativeUnit = "kb", DefaultUnit = "kb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = AgDatabaseDims,
        },
        new ComposeMeasure
        {
            Key = "ag_log_send_rate", DisplayName = "AG log send rate (per second)", Category = CatAvailabilityGroups, SourceTable = "ag_database_replica_states",
            Archetype = MeasureArchetype.Gauge, Column = "log_send_rate",
            NativeUnit = "kb", DefaultUnit = "kb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = AgDatabaseDims,
        },
        new ComposeMeasure
        {
            Key = "ag_redo_rate", DisplayName = "AG redo rate (per second)", Category = CatAvailabilityGroups, SourceTable = "ag_database_replica_states",
            Archetype = MeasureArchetype.Gauge, Column = "redo_rate",
            NativeUnit = "kb", DefaultUnit = "kb", UnitFamily = FamilyBytes,
            DefaultTimeAgg = ComposeAggregate.Avg, ValidAggs = GaugeAggs, AllowedDimensions = AgDatabaseDims,
        },
        new ComposeMeasure
        {
            Key = "ag_secondary_lag", DisplayName = "AG secondary lag", Category = CatAvailabilityGroups, SourceTable = "ag_database_replica_states",
            Archetype = MeasureArchetype.Gauge, Column = "secondary_lag_seconds",
            NativeUnit = "s", DefaultUnit = "s", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = AgDatabaseDims,
        },

        /* The two drain-time ESTIMATES (#991 addendum). Computed server-side per row, at the sample's own
           instant, rather than composed here as a queue/rate ratio — and that is the whole point: a ratio
           of two window AGGREGATES (avg queue ÷ avg rate) is not the average of the per-sample ratios, and
           the two diverge badly exactly when rates swing, which is when anyone is looking. Storing the
           per-row ratio and averaging THAT is the honest read. NULL where no rate exists (idle, suspended,
           caught up); NULL is never coerced to 0, which would read as "drains instantly". */
        new ComposeMeasure
        {
            Key = "ag_est_redo_drain_min", DisplayName = "AG estimated redo drain time", Category = CatAvailabilityGroups, SourceTable = "ag_database_replica_states",
            Archetype = MeasureArchetype.Gauge, Column = "est_redo_completion_time_min",
            NativeUnit = "min", DefaultUnit = "min", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = AgDatabaseDims,
        },
        new ComposeMeasure
        {
            Key = "ag_est_send_drain_min", DisplayName = "AG estimated send drain time", Category = CatAvailabilityGroups, SourceTable = "ag_database_replica_states",
            Archetype = MeasureArchetype.Gauge, Column = "est_send_drain_time_min",
            NativeUnit = "min", DefaultUnit = "min", UnitFamily = FamilyDuration,
            DefaultTimeAgg = ComposeAggregate.Max, ValidAggs = GaugeAggs, AllowedDimensions = AgDatabaseDims,
        },

        /* ═══════════ Same-source ratios (design §2c) — the bread-and-butter derived metrics ═══════════ */
        /* Supporting execution-count scalars the "average per execution" ratios divide by (query_stats had no
           executions measure; procedure_stats already exposes proc_executions). */
        new ComposeMeasure
        {
            Key = "query_executions", DisplayName = "Query executions", Category = CatQueries, SourceTable = "query_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "execution_count", DeltaColumn = "delta_execution_count",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = QueryDims,
        },
        new ComposeMeasure
        {
            Key = "file_reads", DisplayName = "File reads", Category = CatFileIo, SourceTable = "file_io_stats",
            Archetype = MeasureArchetype.Cumulative, Column = "num_of_reads", DeltaColumn = "delta_reads",
            NativeUnit = "count", DefaultUnit = "count", UnitFamily = FamilyCount,
            DefaultTimeAgg = ComposeAggregate.Sum, ValidAggs = CumulativeAggs, AllowedDimensions = FileIoDims,
        },

        /* Average duration PER EXECUTION — the classic query/proc metric, and the correct (execution-weighted)
           form of "avg elapsed": SUM(delta_elapsed) / SUM(delta_executions), NOT an avg-of-per-sample-avgs. */
        new ComposeMeasure
        {
            Key = "query_avg_elapsed_us", DisplayName = "Query avg duration / execution", Category = CatQueries, SourceTable = "query_stats",
            Kind = MeasureKind.Ratio, NumeratorKey = "query_elapsed_us", DenominatorKey = "query_executions",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = QueryDims,
        },
        new ComposeMeasure
        {
            Key = "query_avg_cpu_us", DisplayName = "Query avg CPU / execution", Category = CatQueries, SourceTable = "query_stats",
            Kind = MeasureKind.Ratio, NumeratorKey = "query_worker_us", DenominatorKey = "query_executions",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = QueryDims,
        },
        new ComposeMeasure
        {
            Key = "proc_avg_elapsed_us", DisplayName = "Procedure avg duration / execution", Category = CatProcedures, SourceTable = "procedure_stats",
            Kind = MeasureKind.Ratio, NumeratorKey = "proc_elapsed_us", DenominatorKey = "proc_executions",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = ProcDims,
        },
        new ComposeMeasure
        {
            Key = "proc_avg_cpu_us", DisplayName = "Procedure avg CPU / execution", Category = CatProcedures, SourceTable = "procedure_stats",
            Kind = MeasureKind.Ratio, NumeratorKey = "proc_worker_us", DenominatorKey = "proc_executions",
            NativeUnit = "us", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = ProcDims,
        },

        /* Latch average wait per waiting request (ms/request) and file average read latency (ms/read). */
        new ComposeMeasure
        {
            Key = "latch_avg_wait_ms", DisplayName = "Latch avg wait / request", Category = CatLatch, SourceTable = "latch_stats",
            Kind = MeasureKind.Ratio, NumeratorKey = "latch_wait_time_ms", DenominatorKey = "latch_waiting_requests",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = LatchDims,
        },
        new ComposeMeasure
        {
            Key = "file_avg_read_latency_ms", DisplayName = "File avg read latency", Category = CatFileIo, SourceTable = "file_io_stats",
            Kind = MeasureKind.Ratio, NumeratorKey = "file_io_stall_read_ms", DenominatorKey = "file_reads",
            NativeUnit = "ms", DefaultUnit = "ms", UnitFamily = FamilyDuration,
            ValidAggs = NoAggs, AllowedDimensions = FileIoDims,
        },

        /* ══════════ Gauge-operand ratios (AVG/NULLIF(AVG)) — a ratio of two point-in-time readings, shown as a
             percent (native 'ratio' × 100). Both operands are gauges on the same source (RatioMode = Avg). ══════════ */
        new ComposeMeasure
        {
            Key = "grant_used_pct", DisplayName = "Query-memory grant used %", Category = CatMemory, SourceTable = "memory_grant_stats",
            Kind = MeasureKind.Ratio, RatioMode = MeasureRatioMode.Avg, NumeratorKey = "grant_used_mb", DenominatorKey = "grant_granted_mb",
            NativeUnit = "ratio", DefaultUnit = "percent", UnitFamily = FamilyFraction,
            ValidAggs = NoAggs, AllowedDimensions = NoDims,
        },
        new ComposeMeasure
        {
            Key = "single_use_plan_pct", DisplayName = "Single-use plans %", Category = CatPlanCache, SourceTable = "plan_cache_stats",
            Kind = MeasureKind.Ratio, RatioMode = MeasureRatioMode.Avg, NumeratorKey = "plan_cache_single_use_plans", DenominatorKey = "plan_cache_total_plans",
            NativeUnit = "ratio", DefaultUnit = "percent", UnitFamily = FamilyFraction,
            ValidAggs = NoAggs, AllowedDimensions = PlanCacheDims,
        },
    };

    private static readonly Dictionary<string, ComposeMeasure> s_measureByKey =
        Measures.ToDictionary(m => m.Key, StringComparer.Ordinal);

    /// <summary>The measure with this key, or null.</summary>
    public static ComposeMeasure? Measure(string? key) =>
        key is not null && s_measureByKey.TryGetValue(key, out var m) ? m : null;

    /// <summary>Whether <paramref name="source"/> is a real measure source (a collector table the catalog serves).</summary>
    public static bool IsKnownSource(string? source) =>
        source is not null && Measures.Any(m => string.Equals(m.SourceTable, source, StringComparison.Ordinal));

    /* ─────────────────────────── annotation sources (design D5) ─────────────────────────── */

    /// <summary>The event sources a composed time-series panel may overlay as annotation markers (design D5).
    /// Each names a real collector event table plus its event-time and label columns — every (table, timeColumn,
    /// labelColumn) pinned to the owning collector's <c>PayloadColumns</c> by <c>DarlingComposeTests</c>, exactly
    /// like a measure's columns. These are the SOLE identifiers the annotation compiler emits; a caller supplies
    /// only a KEY from this list, never a table/column.</summary>
    public static readonly IReadOnlyList<ComposeAnnotationSource> AnnotationSources = new[]
    {
        new ComposeAnnotationSource("deadlocks", "Deadlocks", CatBlocking, "deadlocks", "deadlock_time", "database_name"),
        new ComposeAnnotationSource("blocked_process_reports", "Blocked-process reports", CatBlocking, "blocked_process_reports", "event_time", "contentious_object"),
        new ComposeAnnotationSource("long_query_completions", "Long-query completions", CatLongQueries, "long_query_completions", "event_time", "object_name"),
        new ComposeAnnotationSource("default_trace_events", "Default-trace events", CatDefaultTrace, "default_trace_events", "event_time", "event_name"),
        new ComposeAnnotationSource("system_health_events", "system_health events", CatSystemHealth, "system_health_events", "event_time", "event_type"),
    };

    private static readonly Dictionary<string, ComposeAnnotationSource> s_annotationByKey =
        AnnotationSources.ToDictionary(a => a.Key, StringComparer.Ordinal);

    /// <summary>The annotation source with this key, or null.</summary>
    public static ComposeAnnotationSource? AnnotationSource(string? key) =>
        key is not null && s_annotationByKey.TryGetValue(key, out var a) ? a : null;

    /* ─────────────────────────── wire-name maps for the enums ─────────────────────────── */

    private static readonly (ComposeAggregate Value, string Wire)[] s_aggWire =
    {
        (ComposeAggregate.Sum, "sum"), (ComposeAggregate.Avg, "avg"), (ComposeAggregate.Min, "min"),
        (ComposeAggregate.Max, "max"), (ComposeAggregate.Count, "count"), (ComposeAggregate.PercentileCont, "percentile_cont"),
    };

    private static readonly (ComposeTimeBucket Value, string Wire, int Seconds)[] s_bucketWire =
    {
        (ComposeTimeBucket.None, "none", 0), (ComposeTimeBucket.Minute, "minute", 60),
        (ComposeTimeBucket.Hour, "hour", 3600), (ComposeTimeBucket.Day, "day", 86_400),
        (ComposeTimeBucket.Auto, "auto", 0),
    };

    private static readonly (ComposeFilterOp Value, string Wire)[] s_opWire =
    {
        (ComposeFilterOp.Eq, "eq"), (ComposeFilterOp.Neq, "neq"), (ComposeFilterOp.Like, "like"),
        (ComposeFilterOp.Gt, "gt"), (ComposeFilterOp.Gte, "gte"), (ComposeFilterOp.Lt, "lt"), (ComposeFilterOp.Lte, "lte"),
    };

    public static IReadOnlyList<string> AggregateWireNames { get; } = s_aggWire.Select(x => x.Wire).ToArray();
    public static IReadOnlyList<string> TimeBucketWireNames { get; } = s_bucketWire.Select(x => x.Wire).ToArray();
    public static IReadOnlyList<string> FilterOpWireNames { get; } = s_opWire.Select(x => x.Wire).ToArray();

    public static bool TryParseAggregate(string? wire, out ComposeAggregate value)
    {
        foreach (var (v, w) in s_aggWire)
        {
            if (string.Equals(w, wire, StringComparison.Ordinal)) { value = v; return true; }
        }
        value = default;
        return false;
    }

    public static string WireName(ComposeAggregate value) => s_aggWire.First(x => x.Value == value).Wire;

    public static bool TryParseTimeBucket(string? wire, out ComposeTimeBucket value)
    {
        foreach (var (v, w, _) in s_bucketWire)
        {
            if (string.Equals(w, wire, StringComparison.Ordinal)) { value = v; return true; }
        }
        value = default;
        return false;
    }

    public static string WireName(ComposeTimeBucket value) => s_bucketWire.First(x => x.Value == value).Wire;

    /// <summary>Seconds per bucket (0 for <see cref="ComposeTimeBucket.None"/>) — drives the window×resolution ceiling.</summary>
    public static int BucketSeconds(ComposeTimeBucket value) => s_bucketWire.First(x => x.Value == value).Seconds;

    /// <summary>Resolve <see cref="ComposeTimeBucket.Auto"/> to a concrete grain for a window of
    /// <paramref name="windowSeconds"/>: minute up to 2 days, hour up to 60 days, day beyond — so any range
    /// yields a readable, bounded point count (≤ ~2880, well under <see cref="ComposeLimits.MaxBuckets"/>).
    /// Any non-Auto bucket is returned unchanged, so existing panels compile byte-for-byte as before.</summary>
    public static ComposeTimeBucket ResolveBucket(ComposeTimeBucket value, double windowSeconds)
    {
        if (value != ComposeTimeBucket.Auto)
        {
            return value;
        }

        if (windowSeconds <= 2d * 86_400d)
        {
            return ComposeTimeBucket.Minute;
        }

        return windowSeconds <= 60d * 86_400d ? ComposeTimeBucket.Hour : ComposeTimeBucket.Day;
    }

    /// <summary>The Postgres <c>date_trunc</c> field for a real bucket (a compile-time constant, never a user string).</summary>
    public static string DateTruncField(ComposeTimeBucket value) => value switch
    {
        ComposeTimeBucket.Minute => "minute",
        ComposeTimeBucket.Hour => "hour",
        ComposeTimeBucket.Day => "day",
        _ => throw new ArgumentOutOfRangeException(nameof(value), value, "None has no date_trunc field"),
    };

    public static bool TryParseFilterOp(string? wire, out ComposeFilterOp value)
    {
        foreach (var (v, w) in s_opWire)
        {
            if (string.Equals(w, wire, StringComparison.Ordinal)) { value = v; return true; }
        }
        value = default;
        return false;
    }

    public static string WireName(ComposeFilterOp value) => s_opWire.First(x => x.Value == value).Wire;
}
