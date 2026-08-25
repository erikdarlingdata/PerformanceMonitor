/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// Execution plans captured by <c>auto_explain</c>, read out of the server log (#2566, part of #2538).
///
/// <para><b>Why the log at all.</b> <c>auto_explain</c> has no view, no function and no table — it writes to
/// the server log and nowhere else. #2565 settled that it is nonetheless the right mechanism: on-demand
/// <c>EXPLAIN ANALYZE</c> was rejected because it EXECUTES the statement (demonstrated there against a
/// DELETE), and plain <c>EXPLAIN</c> returns estimates with no actual rows, which is the information a plan
/// is opened for.</para>
///
/// <para><b>This is a SELF-HOSTED capability and the collector says so rather than failing vaguely.</b>
/// Reading the log needs <c>pg_read_server_files</c> AND an explicit
/// <c>GRANT EXECUTE ON FUNCTION pg_read_file</c> — measured: the role alone is NOT enough, because
/// <c>pg_read_file</c>'s ACL is <c>postgres=X/postgres</c> and the role does not carry EXECUTE. On Aurora and
/// RDS there is no filesystem and the role is not grantable at all; logs come from the RDS API, which is a
/// different integration entirely (#2538). Where the grant is absent this degrades to a named non-fatal
/// skip, which is the honest outcome — not a hidden failure.</para>
///
/// <para><b>No query text, and no literals anywhere. This is the part to not undo.</b> <c>auto_explain</c>
/// emits <c>Query Text</c> verbatim — <c>WHERE datname = 'postgres'</c> — and
/// <c>auto_explain.log_parameter_max_length = 0</c> does NOT suppress it (that setting only covers bind
/// parameters; measured on #2565). Literals also appear INSIDE the plan tree, in <c>Filter</c> and its
/// relatives: <c>(datname = 'postgres'::name)</c>. So <c>Query Text</c> is dropped entirely — the statement
/// identity is <c>query_id</c>, and its normalised text already lives in <c>pg_statement_stats</c> — and
/// every remaining string is redacted before it reaches the store. That matches what this product does
/// everywhere else: <c>pg_session_states</c> carries no query text and <c>pg_column_stats</c> drops
/// <c>most_common_vals</c>, both for this reason.</para>
///
/// <para><b>Redaction is deliberately asymmetric.</b> Quoted literals are stripped from EVERY string, which
/// is safe because relation and alias names are not quoted. Bare numbers are stripped only inside known
/// condition fields, because a blanket numeric strip would rewrite a table genuinely named
/// <c>transactionitems1</c> into <c>transactionitems?</c> — mangling identity to hide a value that was
/// never there.</para>
/// </summary>
public sealed class PgPlanCaptureCollector : PostgresCollectorDefinitionBase<PgPlanCaptureCollector.Row>
{
    public static readonly PgPlanCaptureCollector Instance = new();

    private PgPlanCaptureCollector()
    {
    }

    /// <param name="QueryId">From <c>%Q</c> in <c>log_line_prefix</c>, which is the ONLY place auto_explain
    /// exposes it — the plan JSON contains no identifier of its own, even with <c>compute_query_id</c> on.
    /// Joins <c>pg_statement_stats</c>. Zero means the prefix was not configured, and the plan is an orphan
    /// (<c>pg_plan_capture_readiness</c> reports this as the <c>plan_attribution</c> facet).</param>
    /// <param name="PlanHash">Of the REDACTED plan, so the same shape recurs to the same hash regardless of
    /// the values it ran with — which is what makes dedup work at all.</param>
    /// <param name="PlanJson">Redacted. See the type header.</param>
    public readonly record struct Row(
        long QueryId,
        string PlanHash,
        double DurationMs,
        int NodeCount,
        string? TopNodeType,
        string PlanJson);

    /// <summary>
    /// How much of the log tail to read per cycle. Bounded because the file can reach hundreds of megabytes
    /// — #2565 measured 772 MB in twenty seconds at capture-everything — and reading it whole would turn a
    /// monitoring collector into the server's biggest reader.
    /// </summary>
    private const int TailBytes = 4 * 1024 * 1024;

    /* Spliced into the query text as a literal. A const string keeps QueryText a compile-time constant,
       which every other collector here relies on, and keeps the number in ONE place. */
    private const string TailBytesLiteral = "4194304";

    /* pg_ls_logdir() to find the CURRENT file rather than a configured name: log_filename is a strftime
       pattern, so the actual name is only knowable by asking. pg_monitor can call this one; it is
       pg_read_file that needs the extra grant.

       The tail is read from a negative offset via greatest(size - TailBytes, 0), so a fresh small log is
       read whole and a large one is read from its end.

       Plans are extracted with regexp_matches rather than parsed line by line because auto_explain writes
       the JSON tab-indented under its LOG line, so the block is recognisable as a unit. The tabs are
       stripped to make it valid JSON. */
    private const string QueryText = @"
WITH newest AS (
    SELECT name, size
    FROM pg_catalog.pg_ls_logdir()
    ORDER BY modification DESC
    LIMIT 1
),
tail AS (
    SELECT pg_catalog.pg_read_file(
               'log/' || n.name,
               greatest(n.size - " + TailBytesLiteral + @", 0),
               " + TailBytesLiteral + @") AS body
    FROM newest AS n
)
SELECT
    (m[1])::bigint                                   AS query_id,
    (m[2])::double precision                         AS duration_ms,
    replace(m[3], chr(9), '')                        AS plan_json
FROM tail,
     regexp_matches(
         tail.body,
         '\[\d+\] (-?\d+) LOG:  duration: ([0-9.]+) ms  plan:\s*\n((?:\t[^\n]*\n)+)',
         'g') AS m
LIMIT 2000";

    public override string Name => "pg_plan_capture";

    public override string TargetTable => "pg_plan_capture";

    /// <summary>
    /// Every PostgreSQL target — including Aurora and RDS, which reach the same table by a different road.
    ///
    /// <para><b>This deliberately does NOT gate on the engine, and the reason is worth stating.</b> Gating
    /// here would make the capability model report plan capture as a PERMANENT GAP on Aurora, which is a
    /// lie: those targets do capture plans, through the RDS log API (<c>RdsPlanIngestor</c>, #2538). The
    /// route is chosen at dispatch, so this definition never actually executes against a managed target and
    /// cannot produce the permission failure that gating was meant to avoid.</para>
    ///
    /// <para>An absent grant, an unloaded module or an unlistable log directory all raise errors the host
    /// classifies as non-fatal skips, and <c>pg_plan_capture_readiness</c> already reports which
    /// precondition is missing.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>Server-wide: one log holds every database's plans.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("plan_hash", CollectorColumnType.Varchar),
        new CollectorColumn("duration_ms", CollectorColumnType.Double),
        new CollectorColumn("node_count", CollectorColumnType.Integer),
        new CollectorColumn("top_node_type", CollectorColumnType.Varchar),
        /* No database_name and no query_text, both deliberately. The log line prefix is not guaranteed to
           carry %d, so a database column would be a claim the source cannot support (#2599); the text is
           dropped for the reason in the type header. */
        new CollectorColumn("plan_json", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            /* Extraction, redaction and hashing live in PgPlanLogParser, shared with the RDS log-API
               transport (#2538). Two implementations of the redaction would eventually disagree, and the
               cost of THAT divergence is customer data rather than a wrong number. */
            var parsed = PgPlanLogParser.FromBlock(
                reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetString(2));

            /* Null is a block the bounded tail read cut in half, which is ordinary rather than
               exceptional: the window can begin mid-plan. Skipped, not stored half-parsed. */
            if (parsed is not null)
            {
                rows.Add(new Row(
                    QueryId: parsed.Value.QueryId,
                    PlanHash: parsed.Value.PlanHash,
                    DurationMs: parsed.Value.DurationMs,
                    NodeCount: parsed.Value.NodeCount,
                    TopNodeType: parsed.Value.TopNodeType,
                    PlanJson: parsed.Value.PlanJson));
            }
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.QueryId)
            .Value(row.PlanHash)
            .Value(row.DurationMs)
            .Value(row.NodeCount)
            .Value(row.TopNodeType)
            .Value(row.PlanJson);
    }
}
