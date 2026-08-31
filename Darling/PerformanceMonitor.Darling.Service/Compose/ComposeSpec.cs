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
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The abuse bounds a composed panel must stay within — the compile-time half of the DoS controls
/// (the DB-side half is the <c>statement_timeout</c> on the viewer/mcp roles). Because the compiler only
/// ever emits aggregated queries (GROUP BY bucket/dims, or a single scalar) — never raw rows — output is
/// bounded by construction; these caps bound the WORK: the window, the bucket resolution, and the number
/// of filter/group-by terms.
/// </summary>
public static class ComposeLimits
{
    /// <summary>The widest window a panel may query (90 days) — a hard ceiling on the scan.</summary>
    public const int MaxWindowHours = 24 * 90;

    /// <summary>The most buckets a time series may resolve to (window ÷ bucket) — bounds a 1-minute
    /// bucket over a long window from fanning out to hundreds of thousands of buckets.</summary>
    public const int MaxBuckets = 5_000;

    /// <summary>The ceiling on a ranked panel's <c>topN</c> (mirrors the read surface's row clamp).</summary>
    public const int MaxTopN = 1_000;

    /// <summary>The final safety <c>LIMIT</c> on a time-series query (a grouped series × buckets backstop).</summary>
    public const int HardRowCap = 10_000;

    public const int MaxFilters = 12;
    public const int MaxGroupBy = 4;

    /// <summary>The most render-only reference lines (thresholds) a panel may carry — a handful of guide lines,
    /// never a data series. Render-only: thresholds are validated at write time but never enter the compiler.</summary>
    public const int MaxThresholds = 4;

    /// <summary>The most event-annotation sources a time-series panel may overlay (design D5) — a handful of
    /// marker layers, never a data series. Each is a separate bounded event query alongside the measure query.</summary>
    public const int MaxAnnotations = 4;

    /// <summary>The most annotation events one source returns for a panel (design D5) — the hard <c>LIMIT</c> on
    /// each overlay query, so a dense window draws a capped set of markers rather than fanning out. Bounds OUTPUT;
    /// the viewer role's <see cref="StatementTimeout"/> bounds the WORK.</summary>
    public const int MaxAnnotationEvents = 200;

    /// <summary>
    /// The per-session <c>statement_timeout</c> applied to the <c>viewer</c>/<c>mcp</c> roles (the compose
    /// surface's DB identities) — the hard backstop a composed query can never exceed, whatever the compile-time
    /// window/bucket caps reason around (a <c>LIMIT</c> bounds OUTPUT, a group-by scans+sorts before it). Applied
    /// in the role PROVISIONING DDL (<see cref="DarlingManagedRoles"/> + <c>tools/provision-roles.sql</c>), NOT a
    /// versioned migration: a role's statement_timeout has no probeable schema footprint, so tying it to
    /// <see cref="!:StorageVersion.SchemaVersion"/> would break the viewer's connect-time version gate. A Postgres
    /// interval literal.
    /// </summary>
    public const string StatementTimeout = "15s";

    /// <summary>The fixed percentile for <c>percentile_cont</c> (p95) — a hardcoded default per the
    /// defaults-over-speculative-config rule; a per-panel percentile knob is a clean later add.</summary>
    public const double DefaultPercentile = 0.95;
}

/// <summary>A view-level variable: a named, dimension-typed slot a panel filter can reference as
/// <c>"$name"</c>. A variable whose dimension is <see cref="ServerDimension"/> scopes the run's server
/// (the compile+run endpoint resolves it to the serverId); the rest resolve to a bound filter value.</summary>
public sealed record ComposeVariable(string Name, string Dimension, string? Default)
{
    /// <summary>The reserved dimension marking the ambient server scope (resolved to serverId, not a filter).</summary>
    public const string ServerDimension = "server";
}

/// <summary>A panel filter's value — exactly one of a literal set (one value, or many for a multi-select
/// <c>eq</c>/<c>neq</c>) or a reference to a declared <see cref="ComposeVariable"/>. Either way it is
/// resolved to bound parameters at compile time; a value is NEVER interpolated into SQL.</summary>
public sealed record ComposeFilterValue(IReadOnlyList<string>? Literals, string? VariableRef)
{
    public bool IsVariable => VariableRef is not null;
}

/// <summary>One parsed, validated filter: a real dimension for the source, a legal operator, and a value
/// that resolves to bound parameters.</summary>
public sealed record ComposeFilter(ComposeDimension Dimension, ComposeFilterOp Op, ComposeFilterValue Value);

/// <summary>Which shape the compiler emits — chosen by the presence of a time bucket vs. a topN.</summary>
public enum PanelMode
{
    /// <summary>Bucketed over time: <c>date_trunc</c> + GROUP BY bucket [+ dims], ordered by bucket.</summary>
    TimeSeries,

    /// <summary>Ranked: GROUP BY dims, ordered by the aggregate descending, LIMIT topN.</summary>
    Ranked,

    /// <summary>A single aggregate over the whole window (one row) — the stat tile.</summary>
    Scalar,
}

/// <summary>
/// A panel's optional SECOND measure (#1606 reporting layer): the y axis of a scatter, or the right-hand
/// axis of a dual-axis line/area. Same-source only — it compiles as a second select expression
/// (<c>AS value2</c>) over the SAME fact rows, the ratio two-operand precedent generalized — so it can
/// never add a join, a parameter, or a second query. Aggregate/unit obey exactly the primary's rules.
/// </summary>
public sealed record ComposeOverlay(ComposeMeasure Measure, ComposeAggregate Aggregate, string Unit);

/// <summary>
/// A fully-parsed, fully-validated panel — everything the compiler needs, with every identifier already
/// resolved to a catalog object (never a raw string). Produced by <see cref="ComposeSpec.TryParsePanel"/>
/// (the write-time authority and the compiler's input alike), so a <see cref="PanelPlan"/> in hand is by
/// construction safe to compile.
/// </summary>
public sealed record PanelPlan
{
    public required ComposeMeasure Measure { get; init; }
    public ComposeAggregate Aggregate { get; init; }
    public required string Unit { get; init; }
    public PanelMode Mode { get; init; }
    public ComposeTimeBucket TimeBucket { get; init; }
    public int TopN { get; init; }
    public required IReadOnlyList<ComposeFilter> Filters { get; init; }
    public required IReadOnlyList<ComposeDimension> GroupBy { get; init; }
    public required string Viz { get; init; }

    /// <summary>Optional render-only reference-line values, in the panel's unit (0-4 finite numbers, design D3).
    /// Validated at parse time but NEVER compiled — the frontend draws them, so they never enter the SQL.</summary>
    public IReadOnlyList<double> Thresholds { get; init; } = Array.Empty<double>();

    /// <summary>Optional event-annotation sources to overlay as markers on a TIME-SERIES panel (design D5):
    /// 0-<see cref="ComposeLimits.MaxAnnotations"/> catalog-resolved sources. Only ever non-empty for a
    /// <see cref="PanelMode.TimeSeries"/> panel (a marker overlay needs a time axis — parse rejects them
    /// otherwise). They do NOT change the measure query: each is compiled to its own bounded event query by
    /// <see cref="ComposeCompiler.CompileAnnotations"/> and returned alongside the panel's rows.</summary>
    public IReadOnlyList<ComposeAnnotationSource> Annotations { get; init; } = Array.Empty<ComposeAnnotationSource>();

    /// <summary>Optional second measure (#1606): scatter's y axis, or a dual-axis line/area's right axis.
    /// Only ever non-null when the viz is scatter (where it is REQUIRED) or an ungrouped line/area —
    /// parse rejects every other combination, so a stored def is never un-renderable.</summary>
    public ComposeOverlay? Overlay { get; init; }

    /// <summary>True when any filter/groupBy dimension is stitched from the #1568 module join, so the
    /// compiler must emit (and window-bound) the module CTE.</summary>
    public bool UsesModuleJoin =>
        Filters.Any(f => f.Dimension.ViaModuleJoin) || GroupBy.Any(d => d.ViaModuleJoin);
}

/// <summary>
/// Parses + validates the Custom Views v2 spec JSON into typed, catalog-resolved plans — the SINGLE
/// authority both the write path (<c>DarlingWebEndpoints.ValidateDefinition</c>) and the run path
/// (<c>/api/compose/run</c>) route through, so a definition that stored can always compile and vice
/// versa. Cross-checks EVERY identifier-bearing field against <see cref="MeasureCatalog"/> and rejects
/// anything off-catalog; the compiler then trusts the plan completely.
/// </summary>
public static class ComposeSpec
{
    /// <summary>Whether a panel object is a v2 (composed) panel — it names a <c>source</c>. A v1 panel
    /// names a <c>read</c> instead; the two coexist in one definition, dispatched per panel.</summary>
    public static bool IsComposedPanel(JsonObject panel) => panel["source"] is not null;

    /// <summary>The v2 composed-panel viz vocabulary (design §4) — distinct from v1 read panels' KnownViz
    /// (table/line/stat/bandlist). The composer's viz picker serves this; every stored composed panel's viz
    /// must be in it AND coherent with the panel's mode (<see cref="ValidateVizMode"/>).</summary>
    public static readonly IReadOnlyList<string> ComposeVizList = new[] { "line", "area", "bar", "stacked", "stacked-bar", "pie", "scatter", "table", "stat" };

    /// <summary>Set form of <see cref="ComposeVizList"/> for O(1) membership.</summary>
    public static readonly IReadOnlySet<string> KnownComposeViz = new HashSet<string>(ComposeVizList, StringComparer.Ordinal);

    /* ─────────────────────────── unknown keys (#2733) ─────────────────────────── */

    /* The write path is STRICT about keys; the parse/run path is not. TryParsePanel positive-reads the keys
       it knows and defaults every optional one on absence, so a typo'd key ("filter", "Filters") used to
       yield a syntactically-valid DIFFERENT panel that validated {valid:true} — a dropped filter silently
       widening a query to the whole fleet (#2733). The write-time validators (ValidateDefinition /
       ValidateNotebookDefinition) now reject any key outside these sets BEFORE parsing, naming the stray and
       suggesting the near-miss. The sets live HERE, beside the parser that reads them, and cannot drift
       silently in either direction: a key listed but not parsed is caught by the every-key acceptance test,
       and a key parsed but not listed can never reach the parser through the write path — the new feature's
       own first write-path test rejects it. TryParsePanel itself stays lenient ON PURPOSE: the run/read path
       must keep rendering definitions that stored before the strictness existed. */

    /// <summary>The composed-panel key universe: exactly the keys <see cref="TryParsePanel"/> reads. The
    /// write path rejects anything else (plus the caller's presentation extras — title/span/hours — which the
    /// frontend owns and the parser never sees).</summary>
    public static readonly IReadOnlySet<string> ComposedPanelKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "source", "measure", "ratio", "aggregate", "unit", "timeBucket", "topN", "viz",
        "filters", "groupBy", "overlay", "thresholds", "annotations",
    };

    /// <summary>The keys a panel filter object may carry (<see cref="ParseFilters"/>).</summary>
    public static readonly IReadOnlySet<string> ComposedFilterKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "dimension", "op", "value",
    };

    /// <summary>The keys an overlay object may carry (the #1606 second measure).</summary>
    public static readonly IReadOnlySet<string> ComposedOverlayKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "measure", "aggregate", "unit",
    };

    /// <summary>The keys a view-level variable object may carry (<see cref="ParseVariables"/>).</summary>
    public static readonly IReadOnlySet<string> VariableKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "name", "dimension", "default",
    };

    /// <summary>The keys a <c>range</c> object may carry (<see cref="ParseRange"/>).</summary>
    public static readonly IReadOnlySet<string> RangeKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "hours",
    };

    /// <summary>The keys the object form of <c>viz</c> may carry (<see cref="ParseViz"/>).</summary>
    private static readonly IReadOnlySet<string> s_vizObjectKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "type",
    };

    /// <summary>The one mis-shape worth a targeted message instead of a generic did-you-mean: nesting the
    /// spec under a <c>panel</c> key is <c>run_custom_view_panel</c>'s RUN-SPEC wrapper leaking into a stored
    /// definition — the natural guess, since that tool nests while a stored panel/cell is flat (#2733). ONE
    /// constant, because the same mis-shape is also named by the write path's v1 arm (a nested panel has no
    /// <c>source</c>, so it never reaches the composed strict check) — two independently-worded copies would
    /// drift on the next tweak.</summary>
    internal const string RunSpecNestingHint =
        "a stored panel is flat (the {\"panel\":{...}} wrapper belongs to run_custom_view_panel's spec); put the panel's keys (source, measure, ...) directly on this object.";

    /// <summary>Targeted guidance per stray key — see <see cref="RunSpecNestingHint"/>.</summary>
    private static readonly IReadOnlyDictionary<string, string> s_panelKeyHints = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["panel"] = RunSpecNestingHint,
    };

    /// <summary>
    /// The first unknown key in <paramref name="obj"/> as a caller-facing error, or null when every key is in
    /// <paramref name="knownKeys"/> (or <paramref name="extraAllowedKeys"/>). <paramref name="path"/> names the
    /// object in the error ("panel", "overlay", "filter 0", ...). A stray key close to a known one gets a
    /// did-you-mean; a key in <paramref name="keyHints"/> gets its targeted guidance instead. Write-path only
    /// by design — see the strictness note above.
    /// </summary>
    public static string? UnknownKeyError(
        JsonObject obj,
        IReadOnlySet<string> knownKeys,
        string path,
        IReadOnlySet<string>? extraAllowedKeys = null,
        IReadOnlyDictionary<string, string>? keyHints = null)
    {
        foreach (var property in obj)
        {
            var key = property.Key;
            if (knownKeys.Contains(key) || (extraAllowedKeys?.Contains(key) ?? false))
            {
                continue;
            }

            if (keyHints is not null && keyHints.TryGetValue(key, out var hint))
            {
                return $"{path} has unknown key '{key}' — {hint}";
            }

            var candidates = extraAllowedKeys is null ? knownKeys : knownKeys.Concat(extraAllowedKeys);
            return NearestKnownKey(key, candidates) is string suggestion
                ? $"{path} has unknown key '{key}' — did you mean '{suggestion}'?"
                : $"{path} has unknown key '{key}'.";
        }

        return null;
    }

    /// <summary>
    /// The write-time strict-key walk over one COMPOSED panel: the panel object itself (against
    /// <see cref="ComposedPanelKeys"/> + the caller's presentation extras), the object form of <c>viz</c>,
    /// each filter object, and the overlay object. Returns the first stray-key error, or null. Only the
    /// shapes the parser understands are walked — a filters value that is not an array (say) is left for
    /// <see cref="TryParsePanel"/> to reject with its structural message.
    /// </summary>
    public static string? UnknownComposedPanelKeyError(JsonObject panel, IReadOnlySet<string>? extraAllowedKeys = null)
    {
        if (UnknownKeyError(panel, ComposedPanelKeys, "panel", extraAllowedKeys, s_panelKeyHints) is string topError)
        {
            return topError;
        }

        if (panel["viz"] is JsonObject vizObject
            && UnknownKeyError(vizObject, s_vizObjectKeys, "panel.viz") is string vizError)
        {
            return vizError;
        }

        if (panel["filters"] is JsonArray filters)
        {
            for (var i = 0; i < filters.Count; i++)
            {
                if (filters[i] is JsonObject filterObject
                    && UnknownKeyError(filterObject, ComposedFilterKeys, $"filter {i}") is string filterError)
                {
                    return filterError;
                }
            }
        }

        if (panel["overlay"] is JsonObject overlayObject
            && UnknownKeyError(overlayObject, ComposedOverlayKeys, "overlay") is string overlayError)
        {
            return overlayError;
        }

        return null;
    }

    /// <summary>The known key nearest to <paramref name="unknown"/> when it is plausibly a typo — a
    /// case-insensitive match ("Filters"), or within edit distance 2 ("filter", "defalut") — else null.</summary>
    internal static string? NearestKnownKey(string unknown, IEnumerable<string> candidates)
    {
        string? best = null;
        var bestDistance = int.MaxValue;
        foreach (var candidate in candidates)
        {
            if (string.Equals(unknown, candidate, StringComparison.OrdinalIgnoreCase))
            {
                return candidate;
            }

            var distance = EditDistance(unknown, candidate);
            if (distance < bestDistance)
            {
                bestDistance = distance;
                best = candidate;
            }
        }

        return bestDistance <= 2 ? best : null;
    }

    /// <summary>Case-insensitive Levenshtein distance — keys are short, so the O(len²) two-row form is fine.</summary>
    private static int EditDistance(string a, string b)
    {
        if (Math.Abs(a.Length - b.Length) > 2)
        {
            /* Distance is at least the length difference; past the suggestion threshold, skip the work. */
            return int.MaxValue;
        }

        var previous = new int[b.Length + 1];
        var current = new int[b.Length + 1];
        for (var j = 0; j <= b.Length; j++)
        {
            previous[j] = j;
        }

        for (var i = 1; i <= a.Length; i++)
        {
            current[0] = i;
            for (var j = 1; j <= b.Length; j++)
            {
                var substitution = char.ToLowerInvariant(a[i - 1]) == char.ToLowerInvariant(b[j - 1]) ? 0 : 1;
                current[j] = Math.Min(
                    Math.Min(current[j - 1] + 1, previous[j] + 1),
                    previous[j - 1] + substitution);
            }

            (previous, current) = (current, previous);
        }

        return previous[b.Length];
    }

    /* ─────────────────────────── variables ─────────────────────────── */

    /// <summary>The set of dimension names a variable may be typed as: every catalog dimension name plus
    /// the reserved <c>server</c> scope.</summary>
    private static readonly HashSet<string> s_variableDimensionNames = BuildVariableDimensionNames();

    private static HashSet<string> BuildVariableDimensionNames()
    {
        var names = MeasureCatalog.Dimensions.Select(d => d.Name).ToHashSet(StringComparer.Ordinal);
        names.Add(ComposeVariable.ServerDimension);
        return names;
    }

    /// <summary>Parses + validates a definition's <c>variables</c> array (absent = none). Each entry must be
    /// an object with a non-empty <c>name</c> (unique) and a <c>dimension</c> that is a known dimension or
    /// <c>server</c>; <c>default</c> is optional text.</summary>
    public static (IReadOnlyList<ComposeVariable>? Variables, string? Error) ParseVariables(JsonNode? node)
    {
        if (node is null)
        {
            return (Array.Empty<ComposeVariable>(), null);
        }

        if (node is not JsonArray array)
        {
            return (null, "definition.variables must be an array.");
        }

        var variables = new List<ComposeVariable>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < array.Count; i++)
        {
            if (array[i] is not JsonObject obj)
            {
                return (null, $"variable {i} must be an object.");
            }

            var name = Str(obj, "name");
            if (string.IsNullOrWhiteSpace(name))
            {
                return (null, $"variable {i} is missing 'name'.");
            }

            if (!seen.Add(name))
            {
                return (null, $"variable '{name}' is declared more than once.");
            }

            var dimension = Str(obj, "dimension");
            if (string.IsNullOrEmpty(dimension) || !s_variableDimensionNames.Contains(dimension))
            {
                return (null, $"variable '{name}' has an unknown dimension '{dimension}'.");
            }

            variables.Add(new ComposeVariable(name, dimension, Str(obj, "default")));
        }

        return (variables, null);
    }

    /* ─────────────────────────── range ─────────────────────────── */

    /// <summary>Validates a definition/run <c>range</c> object (absent = the caller's default). Only
    /// <c>{hours}</c> is understood today; hours must be a positive int within the window ceiling.</summary>
    public static (int? Hours, string? Error) ParseRange(JsonNode? node)
    {
        if (node is null)
        {
            return (null, null);
        }

        if (node is not JsonObject obj)
        {
            return (null, "range must be an object.");
        }

        if (obj["hours"] is not JsonValue hoursValue || !hoursValue.TryGetValue<int>(out var hours))
        {
            return (null, "range.hours must be an integer.");
        }

        if (hours < 1 || hours > ComposeLimits.MaxWindowHours)
        {
            return (null, $"range.hours must be between 1 and {ComposeLimits.MaxWindowHours}.");
        }

        return (hours, null);
    }

    /* ─────────────────────────── panels ─────────────────────────── */

    /// <summary>
    /// Parses + validates one v2 (<c>source</c>) panel against the catalog and the view's declared
    /// <paramref name="declaredVariables"/>, returning a compiler-ready <see cref="PanelPlan"/> or a
    /// caller-facing error. The full identifier cross-check: source ∈ catalog; measure|ratio ∈ catalog and
    /// on that source; aggregate ∈ the measure's ValidAggs ∧ legal-for-archetype; unit ∈ the measure's
    /// family; each filter/groupBy dimension ∈ the measure's AllowedDimensions; LIKE only on a LIKE-able
    /// dimension; each variable-ref ∈ the declared variables; and the per-panel caps.
    /// </summary>
    public static (PanelPlan? Plan, string? Error) TryParsePanel(JsonObject panel, IReadOnlyCollection<string> declaredVariables)
    {
        var source = Str(panel, "source");
        if (string.IsNullOrEmpty(source))
        {
            /* Say "missing", not "unknown source ''" — the misdirecting error #2733 was reported on: a
               mis-shaped object (the panel nested under a 'panel' key, say) has no source at all, and
               quoting an empty string sent the author hunting the catalog instead of the shape. */
            return (null, "panel is missing 'source'.");
        }

        if (!MeasureCatalog.IsKnownSource(source))
        {
            return (null, $"panel references unknown source '{source}'.");
        }

        /* measure XOR ratio. */
        var measureKey = Str(panel, "measure");
        var ratioKey = Str(panel, "ratio");
        if (measureKey is not null && ratioKey is not null)
        {
            return (null, "panel must set exactly one of 'measure' or 'ratio', not both.");
        }

        var key = measureKey ?? ratioKey;
        var measure = MeasureCatalog.Measure(key);
        if (measure is null)
        {
            return (null, $"panel references unknown measure '{key}'.");
        }

        if (!string.Equals(measure.SourceTable, source, StringComparison.Ordinal))
        {
            return (null, $"measure '{measure.Key}' is not on source '{source}' (it is on '{measure.SourceTable}').");
        }

        var isRatio = measure.Kind == MeasureKind.Ratio;
        if (isRatio && measureKey is not null)
        {
            return (null, $"'{measure.Key}' is a ratio; reference it as 'ratio', not 'measure'.");
        }

        if (!isRatio && ratioKey is not null)
        {
            return (null, $"'{measure.Key}' is not a ratio; reference it as 'measure', not 'ratio'.");
        }

        /* aggregate + unit — the shared rules (ValidAggs, the percentile gate, the count-is-unitless
           rule, the unit family), extracted so the overlay (#1606) validates through the SAME authority
           and the two can never drift. */
        var (aggregate, unit, aggUnitError) = ResolveAggregateAndUnit(
            measure, isRatio, Str(panel, "aggregate"), Str(panel, "unit"), "panel");
        if (aggUnitError is not null)
        {
            return (null, aggUnitError);
        }

        /* mode: a real timeBucket => time series; else topN => ranked; else scalar. */
        var bucket = ComposeTimeBucket.None;
        var bucketWire = Str(panel, "timeBucket");
        if (bucketWire is not null && !MeasureCatalog.TryParseTimeBucket(bucketWire, out bucket))
        {
            return (null, $"panel has unknown timeBucket '{bucketWire}'.");
        }

        var hasBucket = bucket != ComposeTimeBucket.None;
        var hasTopN = panel["topN"] is JsonValue;
        var topN = 0;
        if (hasTopN)
        {
            if (panel["topN"] is not JsonValue topNValue || !topNValue.TryGetValue<int>(out topN) || topN < 1)
            {
                return (null, "panel.topN must be a positive integer.");
            }

            topN = Math.Min(topN, ComposeLimits.MaxTopN);
        }

        if (hasBucket && hasTopN)
        {
            return (null, "panel cannot set both 'timeBucket' and 'topN' (a ranked panel is not a time series).");
        }

        var mode = hasBucket ? PanelMode.TimeSeries : hasTopN ? PanelMode.Ranked : PanelMode.Scalar;

        /* viz — the v2 composed-panel vocabulary (distinct from v1 read panels' KnownViz); coherence with the
           panel's mode is checked below once groupBy is known (design §4). */
        var viz = ParseViz(panel["viz"]);
        if (viz is null || !KnownComposeViz.Contains(viz))
        {
            return (null, $"panel has an unknown or missing viz type (one of {string.Join(", ", ComposeVizList)}).");
        }

        /* filters. */
        var (filters, filterError) = ParseFilters(panel["filters"], source, measure, declaredVariables);
        if (filterError is not null)
        {
            return (null, filterError);
        }

        /* groupBy. */
        var (groupBy, groupError) = ParseGroupBy(panel["groupBy"], source, measure);
        if (groupError is not null)
        {
            return (null, groupError);
        }

        /* viz ↔ mode coherence, so a stored def can never be un-renderable (design §4): line/area/stacked are
           time series, bar/pie are ranked, stat is a single scalar (table works in any mode). */
        var vizModeError = ValidateVizMode(viz, mode, groupBy!.Count);
        if (vizModeError is not null)
        {
            return (null, vizModeError);
        }

        /* overlay — the optional SECOND measure (#1606): scatter's y axis, or a dual-axis line/area's
           right axis. Same-source only, validated by the SAME aggregate/unit authority as the primary.
           Coherence lives HERE (not ValidateVizMode, whose table early-return would leak table+overlay):
           scatter REQUIRES an overlay; everything except scatter and an UNGROUPED line/area rejects one —
           silence would hide author error. */
        ComposeOverlay? overlay = null;
        if (panel["overlay"] is JsonNode overlayNode)
        {
            if (overlayNode is not JsonObject overlayObject)
            {
                return (null, "panel.overlay must be an object: {\"measure\": ..., \"aggregate\": ..., \"unit\": ...}.");
            }

            var overlayKey = Str(overlayObject, "measure");
            var overlayMeasure = MeasureCatalog.Measure(overlayKey);
            if (overlayMeasure is null)
            {
                return (null, $"overlay references unknown measure '{overlayKey}'.");
            }

            if (!string.Equals(overlayMeasure.SourceTable, source, StringComparison.Ordinal))
            {
                return (null, $"overlay measure '{overlayMeasure.Key}' is on source '{overlayMeasure.SourceTable}', not the panel's '{source}' — an overlay must share the panel's source.");
            }

            var overlayIsRatio = overlayMeasure.Kind == MeasureKind.Ratio;
            if (overlayIsRatio && Str(overlayObject, "aggregate") is not null)
            {
                return (null, $"overlay measure '{overlayMeasure.Key}' is a ratio; its aggregation is fixed — omit 'aggregate'.");
            }

            var (overlayAggregate, overlayUnit, overlayError) = ResolveAggregateAndUnit(
                overlayMeasure, overlayIsRatio, Str(overlayObject, "aggregate"), Str(overlayObject, "unit"), "overlay");
            if (overlayError is not null)
            {
                return (null, overlayError);
            }

            var overlayAllowed =
                string.Equals(viz, "scatter", StringComparison.Ordinal)
                || (mode == PanelMode.TimeSeries
                    && (string.Equals(viz, "line", StringComparison.Ordinal) || string.Equals(viz, "area", StringComparison.Ordinal))
                    && groupBy!.Count == 0);
            if (!overlayAllowed)
            {
                /* Name the REAL blocker: a viz that never carries an overlay reports that, and only a
                   line/area whose sole problem is the groupBy gets the dual-axis-grouping message. */
                var vizCarriesOverlay = string.Equals(viz, "line", StringComparison.Ordinal)
                    || string.Equals(viz, "area", StringComparison.Ordinal);
                return (null, vizCarriesOverlay && mode == PanelMode.TimeSeries && groupBy!.Count > 0
                    ? "an overlay (dual-axis) time series cannot also group by a dimension — two value axes times many series is unreadable; drop the groupBy or the overlay."
                    : $"a '{viz}' panel cannot carry an overlay; overlays belong to scatter and ungrouped line/area panels.");
            }

            overlay = new ComposeOverlay(overlayMeasure, overlayAggregate, overlayUnit);
        }

        if (string.Equals(viz, "scatter", StringComparison.Ordinal) && overlay is null)
        {
            return (null, "a scatter panel needs an 'overlay' second measure — the primary measure ranks the points (x), the overlay is the y axis.");
        }

        /* thresholds — optional render-only reference lines (design D3). Validated here so a stored def can't
           carry a non-number/NaN/over-long list, but NOT compiled: the frontend draws them, so they never enter
           the compiler/SQL. */
        var (thresholds, thresholdError) = ParseThresholds(panel["thresholds"]);
        if (thresholdError is not null)
        {
            return (null, thresholdError);
        }

        /* annotations — optional event-marker overlays (design D5), each a known annotation-source KEY, capped,
           and only meaningful on a time-series panel (a marker overlay needs a time axis — rejected on a
           ranked/scalar panel so a stored def is never un-renderable). They do NOT change the measure query: the
           compiler emits a separate bounded event query per source. */
        var (annotations, annotationError) = ParseAnnotations(panel["annotations"], mode);
        if (annotationError is not null)
        {
            return (null, annotationError);
        }

        var plan = new PanelPlan
        {
            Measure = measure,
            Aggregate = aggregate,
            Unit = unit,
            Mode = mode,
            TimeBucket = bucket,
            TopN = topN,
            Filters = filters!,
            GroupBy = groupBy!,
            Viz = viz,
            Thresholds = thresholds!,
            Annotations = annotations!,
            Overlay = overlay,
        };

        return (plan, null);
    }

    private static (IReadOnlyList<ComposeFilter>? Filters, string? Error) ParseFilters(
        JsonNode? node, string source, ComposeMeasure measure, IReadOnlyCollection<string> declaredVariables)
    {
        if (node is null)
        {
            return (Array.Empty<ComposeFilter>(), null);
        }

        if (node is not JsonArray array)
        {
            return (null, "panel.filters must be an array.");
        }

        if (array.Count > ComposeLimits.MaxFilters)
        {
            return (null, $"panel has {array.Count} filters; the maximum is {ComposeLimits.MaxFilters}.");
        }

        var filters = new List<ComposeFilter>();
        for (var i = 0; i < array.Count; i++)
        {
            if (array[i] is not JsonObject obj)
            {
                return (null, $"filter {i} must be an object.");
            }

            var dimName = Str(obj, "dimension");
            var dimension = dimName is null ? null : MeasureCatalog.Dimension(source, dimName);
            if (dimension is null || !IsDimensionAllowed(dimName!, measure))
            {
                return (null, $"filter {i} references dimension '{dimName}', which is not allowed for measure '{measure.Key}'.");
            }

            var opWire = Str(obj, "op");
            if (opWire is null || !MeasureCatalog.TryParseFilterOp(opWire, out var op))
            {
                return (null, $"filter {i} has unknown op '{opWire}'.");
            }

            if (op == ComposeFilterOp.Like && !dimension.Likeable)
            {
                return (null, $"filter {i}: op 'like' is not allowed on dimension '{dimName}'.");
            }

            var (value, valueError) = ParseFilterValue(obj["value"], op, i, declaredVariables);
            if (valueError is not null)
            {
                return (null, valueError);
            }

            filters.Add(new ComposeFilter(dimension, op, value!));
        }

        return (filters, null);
    }

    private static (ComposeFilterValue? Value, string? Error) ParseFilterValue(
        JsonNode? node, ComposeFilterOp op, int index, IReadOnlyCollection<string> declaredVariables)
    {
        if (node is null)
        {
            return (null, $"filter {index} is missing 'value'.");
        }

        var allowsMany = op is ComposeFilterOp.Eq or ComposeFilterOp.Neq;

        if (node is JsonValue value)
        {
            if (!value.TryGetValue<string>(out var text) || text is null)
            {
                /* A numeric/boolean literal is coerced to its text form (the columns are text). */
                text = value.ToString();
            }

            if (text.StartsWith('$'))
            {
                var varName = text.Substring(1);
                if (varName.Length == 0 || !declaredVariables.Contains(varName))
                {
                    return (null, $"filter {index} references undeclared variable '{text}'.");
                }

                return (new ComposeFilterValue(null, varName), null);
            }

            return (new ComposeFilterValue(new[] { text }, null), null);
        }

        if (node is JsonArray array)
        {
            if (!allowsMany)
            {
                return (null, $"filter {index}: op '{MeasureCatalog.WireName(op)}' takes a single value, not a list.");
            }

            if (array.Count == 0)
            {
                return (null, $"filter {index} has an empty value list.");
            }

            var literals = new List<string>(array.Count);
            foreach (var item in array)
            {
                if (item is not JsonValue itemValue)
                {
                    return (null, $"filter {index} value list must contain only strings.");
                }

                literals.Add(itemValue.TryGetValue<string>(out var s) && s is not null ? s : itemValue.ToString());
            }

            return (new ComposeFilterValue(literals, null), null);
        }

        return (null, $"filter {index} has an invalid 'value'.");
    }

    private static (IReadOnlyList<ComposeDimension>? GroupBy, string? Error) ParseGroupBy(
        JsonNode? node, string source, ComposeMeasure measure)
    {
        if (node is null)
        {
            return (Array.Empty<ComposeDimension>(), null);
        }

        if (node is not JsonArray array)
        {
            return (null, "panel.groupBy must be an array.");
        }

        if (array.Count > ComposeLimits.MaxGroupBy)
        {
            return (null, $"panel has {array.Count} groupBy dimensions; the maximum is {ComposeLimits.MaxGroupBy}.");
        }

        var dims = new List<ComposeDimension>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < array.Count; i++)
        {
            var name = (array[i] as JsonValue)?.GetValue<string>();
            var dimension = name is null ? null : MeasureCatalog.Dimension(source, name);
            if (dimension is null || !IsDimensionAllowed(name!, measure))
            {
                return (null, $"groupBy references dimension '{name}', which is not allowed for measure '{measure.Key}'.");
            }

            if (!seen.Add(name!))
            {
                return (null, $"groupBy lists dimension '{name}' more than once.");
            }

            dims.Add(dimension);
        }

        return (dims, null);
    }

    /// <summary>Parses + validates a panel's optional <c>thresholds</c> array (design D3): 0-4 finite numbers,
    /// in the panel's chosen unit, for render-only reference lines. Rejects a non-array, a non-number,
    /// NaN/Infinity, or an over-long list. The values are validated but never compiled — the frontend draws
    /// them — so a stored definition can never carry an un-renderable threshold.</summary>
    private static (IReadOnlyList<double>? Thresholds, string? Error) ParseThresholds(JsonNode? node)
    {
        if (node is null)
        {
            return (Array.Empty<double>(), null);
        }

        if (node is not JsonArray array)
        {
            return (null, "panel.thresholds must be an array.");
        }

        if (array.Count > ComposeLimits.MaxThresholds)
        {
            return (null, $"panel has {array.Count} thresholds; the maximum is {ComposeLimits.MaxThresholds}.");
        }

        var values = new List<double>(array.Count);
        for (var i = 0; i < array.Count; i++)
        {
            if (array[i] is not JsonValue value || !value.TryGetValue<double>(out var number) || !double.IsFinite(number))
            {
                return (null, $"threshold {i} must be a finite number.");
            }

            values.Add(number);
        }

        return (values, null);
    }

    /// <summary>Parses + validates a panel's optional <c>annotations</c> array (design D5): 0-<see
    /// cref="ComposeLimits.MaxAnnotations"/> known annotation-source KEYS (deduped) to overlay as event markers.
    /// Rejects a non-array, a non-string/unknown key, a duplicate, or an over-long list — and rejects ANY
    /// annotation on a non-time-series panel (a marker overlay needs a time axis, so a stored def is never
    /// un-renderable). Returns the catalog-resolved sources, which the compiler emits schema-qualified; the
    /// caller supplies only a key, never a table/column.</summary>
    private static (IReadOnlyList<ComposeAnnotationSource>? Annotations, string? Error) ParseAnnotations(JsonNode? node, PanelMode mode)
    {
        if (node is null)
        {
            return (Array.Empty<ComposeAnnotationSource>(), null);
        }

        if (node is not JsonArray array)
        {
            return (null, "panel.annotations must be an array.");
        }

        if (array.Count == 0)
        {
            return (Array.Empty<ComposeAnnotationSource>(), null);
        }

        if (mode != PanelMode.TimeSeries)
        {
            return (null, "annotations are only valid on a time-series panel (add a timeBucket).");
        }

        if (array.Count > ComposeLimits.MaxAnnotations)
        {
            return (null, $"panel has {array.Count} annotations; the maximum is {ComposeLimits.MaxAnnotations}.");
        }

        var sources = new List<ComposeAnnotationSource>(array.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < array.Count; i++)
        {
            var key = array[i] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;
            var source = MeasureCatalog.AnnotationSource(key);
            if (source is null)
            {
                return (null, $"annotation {i} references unknown source '{key}'.");
            }

            if (!seen.Add(key!))
            {
                return (null, $"annotation source '{key}' is listed more than once.");
            }

            sources.Add(source);
        }

        return (sources, null);
    }

    private static string? ParseViz(JsonNode? node) => node switch
    {
        JsonValue value when value.TryGetValue<string>(out var s) => s,
        JsonObject obj => Str(obj, "type"),
        _ => null,
    };

    /// <summary>The universal <c>server</c> axis (fleet / multi-server) is usable by EVERY measure; any other
    /// dimension must be in the measure's <see cref="ComposeMeasure.AllowedDimensions"/>.</summary>
    private static bool IsDimensionAllowed(string dimensionName, ComposeMeasure measure) =>
        string.Equals(dimensionName, MeasureCatalog.ServerDimensionName, StringComparison.Ordinal)
        || measure.AllowedDimensions.Contains(dimensionName);

    /// <summary>Rejects a viz that cannot render the panel's shape (design §4 shape-steering, enforced so a
    /// stored def is never un-renderable): line/area/stacked/stacked-bar are time series; bar/pie are ranked
    /// (topN) with a categorical group; stat is a single scalar; table renders any shape. A stacked (or
    /// stacked-bar) chart also needs a group-by (the parts that stack), and a single-value panel cannot group.</summary>
    private static string? ValidateVizMode(string viz, PanelMode mode, int groupByCount)
    {
        if (string.Equals(viz, "table", StringComparison.Ordinal))
        {
            return null;
        }

        switch (mode)
        {
            case PanelMode.TimeSeries:
                if (viz is not ("line" or "area" or "stacked" or "stacked-bar"))
                {
                    return $"a '{viz}' chart is not a time series; use line/area/stacked/stacked-bar (or drop the timeBucket).";
                }

                if (viz is ("stacked" or "stacked-bar") && groupByCount == 0)
                {
                    return "a stacked chart needs a group-by dimension (the parts that stack).";
                }

                return null;

            case PanelMode.Ranked:
                if (viz is not ("bar" or "pie" or "scatter"))
                {
                    return $"a '{viz}' chart cannot render a ranked (topN) panel; use bar, pie, or scatter.";
                }

                if (groupByCount == 0)
                {
                    return $"a '{viz}' chart needs a group-by dimension (the categories to rank).";
                }

                return null;

            case PanelMode.Scalar:
                if (groupByCount > 0)
                {
                    return "a single-value panel cannot group by a dimension; add a timeBucket (time series) or a topN (ranked).";
                }

                return string.Equals(viz, "stat", StringComparison.Ordinal)
                    ? null
                    : $"a '{viz}' chart needs a group or time bucket; a single value uses stat (or table).";

            default:
                return null;
        }
    }

    /// <summary>
    /// The ONE aggregate/unit rulebook (#1606): ValidAggs membership, the percentile-only-on-per-event
    /// defense, the count-is-unitless rule, and the unit-family check — shared verbatim by the primary
    /// measure and the overlay so the two validations can never drift. A ratio's aggregation is fixed
    /// (<see cref="ComposeMeasure.DefaultTimeAgg"/>); a non-ratio requires an explicit aggregate.
    /// <paramref name="owner"/> names the failing field in errors ("panel" or "overlay").
    /// </summary>
    private static (ComposeAggregate Aggregate, string Unit, string? Error) ResolveAggregateAndUnit(
        ComposeMeasure measure, bool isRatio, string? aggWire, string? unitWire, string owner)
    {
        var aggregate = measure.DefaultTimeAgg;
        if (!isRatio)
        {
            if (aggWire is null)
            {
                return (default, string.Empty, $"{owner} is missing 'aggregate' (one of {string.Join(", ", MeasureCatalog.AggregateWireNames)}).");
            }

            if (!MeasureCatalog.TryParseAggregate(aggWire, out aggregate))
            {
                return (default, string.Empty, $"{owner} has unknown aggregate '{aggWire}'.");
            }

            if (!measure.ValidAggs.Contains(aggregate))
            {
                return (default, string.Empty, $"aggregate '{aggWire}' is not valid for measure '{measure.Key}'.");
            }

            /* Defense in depth over ValidAggs: percentile_cont is legal ONLY on a per-event measure. */
            if (aggregate == ComposeAggregate.PercentileCont && measure.Archetype != MeasureArchetype.PerEvent)
            {
                return (default, string.Empty, "percentile_cont is only valid on per-event measures.");
            }
        }

        /* unit ∈ the measure's family — except COUNT, which is a plain row count (unitless) regardless
           of the measure's family, so its only legal unit is 'count'. */
        string unit;
        if (!isRatio && aggregate == ComposeAggregate.Count)
        {
            unit = unitWire ?? MeasureCatalog.FamilyCount;
            if (!string.Equals(unit, MeasureCatalog.FamilyCount, StringComparison.Ordinal))
            {
                return (default, string.Empty, "a count aggregate is unitless; its unit must be 'count'.");
            }
        }
        else
        {
            unit = unitWire ?? measure.DefaultUnit;
            var family = MeasureCatalog.Family(measure.UnitFamily);
            if (family is null || !family.Has(unit))
            {
                return (default, string.Empty, $"unit '{unit}' is not valid for measure '{measure.Key}' (family '{measure.UnitFamily}').");
            }
        }

        return (aggregate, unit, null);
    }

    private static string? Str(JsonObject obj, string key) =>
        obj[key] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;
}
