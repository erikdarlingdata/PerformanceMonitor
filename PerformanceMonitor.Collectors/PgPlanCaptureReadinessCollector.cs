/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Whether this PostgreSQL target could capture execution plans at all, and if not, exactly which step is
/// missing (#2564, the shippable first slice of #2538).
///
/// <para><b>The failure this fixes is SILENCE.</b> A PostgreSQL target has no plans today, and someone
/// evaluating us against DBM on Aurora cannot tell whether the product does not support plans, their server
/// is not configured for them, or something is broken. Those need three different actions and we say nothing.
/// This collector does not capture a plan; it records whether capture is POSSIBLE, so the read surface can
/// answer the question instead of returning an honest-looking empty.</para>
///
/// <para><b>Why <c>auto_explain</c> and not something else.</b> #2538 has not settled the capture mechanism,
/// and this deliberately does not settle it either — it records facts that are true regardless. But
/// <c>auto_explain</c> is the one that matters on Aurora and RDS, it is detectable read-only with no
/// configuration change, and the remedy is specific enough to act on: it goes in a custom CLUSTER parameter
/// group and needs a writer reboot. That is advice, not a shrug.</para>
///
/// <para><b>The trap this exists to report.</b> <c>auto_explain.log_min_duration = -1</c> means the library is
/// loaded and capturing NOTHING. From the outside that is indistinguishable from not-loaded — same absence of
/// plans — but the remedy is completely different: one is a parameter-group change plus a reboot, the other
/// is a single setting. Reporting them as one state would send half the readers to the wrong fix, so the two
/// are separate rows.</para>
///
/// <para><b>What it must NOT claim.</b> That the library is loaded is not the same fact as plans being
/// captured, and neither is the same as us being able to READ them — auto_explain writes to the server log,
/// which is a separate problem (#2566/#2567). This records readiness only, and the column names say so.</para>
///
/// <para>Core catalog surfaces only (<c>pg_settings</c>, <c>pg_available_extensions</c>), both readable by any
/// role, so this runs on every PostgreSQL target including standbys — a replica's parameter group can differ
/// from its writer's, and that difference is exactly the kind of thing nobody notices.</para>
/// </summary>
public sealed class PgPlanCaptureReadinessCollector : PostgresCollectorDefinitionBase<PgPlanCaptureReadinessCollector.Row>
{
    public static PgPlanCaptureReadinessCollector Instance { get; } = new();

    private PgPlanCaptureReadinessCollector()
    {
    }

    /// <param name="Facet">Which fact this row carries — see the query's own comments.</param>
    /// <param name="IsSatisfied">Whether that fact is in the state plan capture needs.</param>
    /// <param name="Observed">What the server actually answered, verbatim, so a reader can see the value
    /// rather than trust our interpretation of it.</param>
    /// <param name="Detail">The operator-facing consequence and remedy for this facet.</param>
    public readonly record struct Row(
        string Facet,
        bool IsSatisfied,
        string? Observed,
        string? Detail);

    /* Four facets, each a separate row, because each has a DIFFERENT remedy and collapsing them would
       produce the one thing this collector exists to prevent: a single "plans unavailable" that tells
       nobody what to do.

         library_loaded      - is auto_explain in shared_preload_libraries? A parameter-group change plus a
                               reboot on Aurora/RDS; not a SET, which is the misconception worth heading off.
         capture_threshold   - auto_explain.log_min_duration. -1 is the trap: loaded and capturing nothing,
                               visually identical to not-loaded from where the user is standing.
         extension_available - is auto_explain even present in pg_available_extensions? Answers "could this
                               server ever do it" as distinct from "is it doing it", which is the difference
                               between a configuration task and a platform limitation.
         plan_text_setting   - auto_explain.log_format. Recorded rather than judged: any format proves
                               capture is happening, and #2565 has not chosen what we would read.

       current_setting(..., true) throughout — the MISSING_OK form. Reading a GUC that does not exist because
       the library was never loaded is the NORMAL case here, and the two-argument form answers NULL instead of
       raising 42704, which would turn the collector's entire purpose into an error every cycle.

       No unit arithmetic anywhere. log_min_duration is a memory-less integer of milliseconds, but the
       skill's own scar tissue applies: current_setting renders GUCs WITH their unit on some settings, so the
       value is stored as TEXT exactly as the server rendered it and interpreted downstream. Casting it here
       is how a collector starts failing on one major version and not another. */
    private const string QueryText = @"
SELECT
    'library_loaded'::text                                                        AS facet,
    coalesce(current_setting('shared_preload_libraries', true), '') LIKE '%auto_explain%' AS is_satisfied,
    coalesce(current_setting('shared_preload_libraries', true), '(unreadable)')    AS observed,
    'auto_explain must be listed in shared_preload_libraries. On Aurora/RDS this is a custom CLUSTER '
        || 'parameter group plus a WRITER REBOOT - it cannot be turned on with SET or ALTER SYSTEM.'::text AS detail

UNION ALL

SELECT
    'capture_threshold'::text,
    /* Satisfied means a non-negative threshold: 0 captures everything, a positive value captures the slow
       ones. -1 is loaded-but-capturing-nothing, and NULL is the GUC not existing at all because the library
       was never loaded. Both unsatisfied, deliberately different in `observed`. */
    coalesce(current_setting('auto_explain.log_min_duration', true), '-1') <> '-1'
        AND current_setting('auto_explain.log_min_duration', true) IS NOT NULL,
    coalesce(current_setting('auto_explain.log_min_duration', true), '(setting absent - library not loaded)'),
    /* CONDITIONAL, because one fixed sentence is WRONG in three of the four states this facet reaches.
       Measured against a live PostgreSQL 17 while writing this: with the threshold at 0 the static text
       still read 'log_min_duration = -1 means ... capturing NOTHING' beside is_satisfied = true, so the row
       contradicted itself. A remedy that does not depend on what was observed is not a remedy. */
    CASE
        WHEN current_setting('auto_explain.log_min_duration', true) IS NULL
            THEN 'This setting does not exist because auto_explain is not loaded. Fix library_loaded first - '
                 || 'the threshold cannot be set independently of the library.'
        WHEN current_setting('auto_explain.log_min_duration', true) = '-1'
            THEN 'auto_explain IS loaded but log_min_duration is -1, so it captures NOTHING - which looks '
                 || 'identical to not being loaded from the outside. Set it to 0 to capture every statement, '
                 || 'or a millisecond threshold to capture only the slow ones.'
        ELSE 'auto_explain is loaded and capturing at this threshold. Capture is not the same fact as the '
             || 'plans being readable by this product, which is tracked separately.'
    END

UNION ALL

SELECT
    'extension_available'::text,
    EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'auto_explain'),
    (SELECT coalesce(max(default_version), '(not present)') FROM pg_available_extensions WHERE name = 'auto_explain'),
    'Whether this server COULD load auto_explain at all, which is a different question from whether it '
        || 'currently does. If it is absent, plan capture by this mechanism is a platform limitation rather '
        || 'than a configuration step.'

UNION ALL

SELECT
    'plan_text_setting'::text,
    current_setting('auto_explain.log_format', true) IS NOT NULL,
    coalesce(current_setting('auto_explain.log_format', true), '(setting absent - library not loaded)'),
    'The format auto_explain writes plans in. Recorded, not judged: any value proves capture is configured, '
        || 'and which format is readable is settled separately.'";

    public override string Name => "pg_plan_capture_readiness";

    public override string TargetTable => "pg_plan_capture_readiness";

    /// <summary>
    /// Every PostgreSQL target, standbys included. A replica can carry a different parameter group from its
    /// writer, and gating this to writers would hide precisely that divergence — which is the sort of thing
    /// nobody notices until they wonder why plans appear for one node and not another.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("facet", CollectorColumnType.Varchar),
        new CollectorColumn("is_satisfied", CollectorColumnType.Boolean),
        new CollectorColumn("observed", CollectorColumnType.Varchar),
        new CollectorColumn("detail", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var facets = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            facets.Add(new Row(
                Facet: reader.GetString(0),
                /* A NULL is_satisfied cannot happen for these expressions, but reading it defensively costs
                   nothing and an unsatisfied default is the safe direction: claiming readiness we have not
                   proven is the failure mode that matters. */
                IsSatisfied: !reader.IsDBNull(1) && reader.GetBoolean(1),
                Observed: reader.IsDBNull(2) ? null : reader.GetString(2),
                Detail: reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return facets;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Every facet is a current configuration state, not a counter — what matters is what the
           server is set to now, and the history exists so somebody can see when it changed. */
        writer
            .Value(row.Facet)
            .Value(row.IsSatisfied)
            .Value(row.Observed)
            .Value(row.Detail);
    }
}
