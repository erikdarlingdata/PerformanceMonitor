/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// One collected row whose cached-plan XML measured over
/// <see cref="QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes"/>, carrying everything a later out-of-band
/// fetch needs to ask the monitored server for that plan again (#3392).
///
/// <para><b>Cache coordinates, not a content key.</b> A capped row ships NULL for its XML, so it produces no
/// plan-dimension row at all — there is no content to digest and nothing already in the store to point at.
/// What remains is what the collector already read:
/// <c>sys.dm_exec_text_query_plan(plan_handle, statement_start_offset, statement_end_offset)</c> is the exact
/// call whose result was measured, so re-issuing it with these three values is the route back to the same
/// document.</para>
///
/// <para><b>Both handles are required rather than nullable.</b> Without <see cref="PlanHandle"/> there is no
/// fetch to make, and without <see cref="SqlHandle"/> there is no complete row identity to dedupe repeated
/// sightings on — so a definition that cannot supply both describes NO observation rather than a partial one
/// that would occupy a backlog slot forever without ever resolving.</para>
///
/// <para>The offsets are whatever the definition's own plan fetch passes — the statement offsets for
/// <c>query_stats</c>, the module-grain literals for <c>procedure_stats</c>, whose three DMVs aggregate at the
/// whole-object grain and expose no offsets. Carrying them rather than re-deriving them later is what keeps
/// the deferred fetch the same call the collector made.</para>
/// </summary>
/// <param name="PlanHandle">The plan cache handle, in the <c>0x</c>-prefixed form the collector stores.</param>
/// <param name="SqlHandle">The batch text handle, same form — part of the row identity, not of the fetch.</param>
/// <param name="StatementStartOffset">First argument the definition's plan fetch passes after the handle.</param>
/// <param name="StatementEndOffset">Second argument the definition's plan fetch passes after the handle.</param>
/// <param name="DatabaseName">The database the row was attributed to, for reading the backlog; may be null.</param>
/// <param name="QueryHash">
/// The hash-grained identity this collector's stored-plan readers key on, or null for a collector whose
/// readers key on the handles instead. <c>query_stats</c> supplies it and <c>procedure_stats</c> does not,
/// and that asymmetry is the READ surface's, not this record's: <c>get_plan_xml</c> and
/// <c>analyze_query_plan</c> take a <c>query_hash</c>, while <c>analyze_procedure_plan</c> takes a
/// <c>sql_handle</c> this record already carries. Recording it here is what lets the deferred content be
/// found again by the identity a caller actually has.
/// </param>
/// <param name="ObservedBytes">The measured <c>DATALENGTH</c> of the plan XML that was not captured.</param>
public readonly record struct OversizedPlanObservation(
    string PlanHandle,
    string SqlHandle,
    int StatementStartOffset,
    int StatementEndOffset,
    string? DatabaseName,
    string? QueryHash,
    long ObservedBytes);
