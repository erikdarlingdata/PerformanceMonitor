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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Collects facts for a POSTGRESQL-TARGET analysis pass from Darling's store (#3542) — the engine-sibling of
/// <see cref="PgFactCollector"/>, which reads the SQL Server collector tables and is Lite's
/// <c>DuckDbFactCollector</c> ported method-for-method. This class is deliberately NOT a set of methods added
/// to that one (design decision D1): Lite has no PostgreSQL target, so there is no twin to port from, and the
/// Lite-parity census <c>PgFactCollectorTests</c> encodes would be poisoned by methods with no Lite
/// counterpart. The census here is a SELF-pin instead — its value is "changes are deliberate", not "matches
/// Lite" — and the SQL is free to use full PostgreSQL dialect (<c>FILTER</c>, <c>IS DISTINCT FROM</c>,
/// named windows), because no DuckDB engine will ever run it.
///
/// <para><b>What is shared with the SQL Server collector, and how.</b> Everything that is a discipline
/// rather than a query: <see cref="FactCommandTimeoutSeconds"/> is the SAME constant (one number in one
/// budget — <see cref="DarlingAnalysisService.AnalysisCommandTimeoutSeconds"/>'s note explains why two would
/// be a trap); <see cref="ReportCollectionFailure"/> is the same three-outcome degrade (timeout WARN /
/// missing table-or-column DEBUG / else ERROR), classified through the one
/// <see cref="PgBaselineProvider.IsCommandTimeout"/>; the coverage stamp is finished through the same
/// <see cref="PgFactCollector.BuildCoverage"/>; every parameter binds positionally as <c>$N</c> and every
/// <see cref="DateTime"/> naive-UTC Kind-Unspecified; every store call carries
/// <c>context.CancellationToken</c>; no query names <c>now()</c>. The directory-wide pins
/// (<c>AnalysisPassCommandTimeoutTests</c>, <c>AnalysisPassTokenThreadingTests</c>,
/// <c>StoreSqlClockDisciplineTests</c>) hold all of that over this file exactly as over its sibling.</para>
///
/// <para><b>Shape: one collect method per family, each in its own partial file, pre-declared in emission
/// order.</b> The plumbing lane ships every method as a stub that returns immediately, so the content lanes
/// (2–9 in the v1 plan) fill their own file and never edit this one, the census, or each other. The two
/// methods that are NOT stubs are the plumbing's own: the coverage witness (step 0 — without it every
/// PostgreSQL pass is structurally <c>unavailable</c> under the #3538 A2 rule) and the registry metadata fact
/// (the one point-in-time fact a skeleton pass can honestly emit).</para>
/// </summary>
public sealed partial class PgTargetFactCollector : IFactCollector
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    /// <summary>
    /// The per-command deadline for every read here — <see cref="PgFactCollector.FactCommandTimeoutSeconds"/>
    /// by reference, not a second number. Both collectors' reads share the one 120 s pass budget with the
    /// baseline computations, the detector and the drill-down, and a deadline that differed by engine would
    /// have to be reasoned about twice every time either moved; the argument for 60 s (half the budget: one
    /// stalled read still leaves half the pass for everything else) is engine-independent.
    /// </summary>
    internal const int FactCommandTimeoutSeconds = PgFactCollector.FactCommandTimeoutSeconds;

    /// <summary>The logger is optional and defaulted, as on every analysis component in this project.</summary>
    public PgTargetFactCollector(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /// <summary>
    /// Reports a fact-collection failure that is being swallowed, so a collector that CANNOT run is
    /// distinguishable from one that ran and found nothing (#2826) — <see cref="PgFactCollector"/>'s
    /// three-outcome classification, verbatim, because the three reactions are the same on either engine:
    /// <list type="bullet">
    /// <item><description><b>Command timeout</b> — WARNING, classified STRUCTURALLY by
    /// <see cref="PgBaselineProvider.IsCommandTimeout"/> (57014 or a wrapped <see cref="TimeoutException"/>),
    /// never by message text: Npgsql renders its own client-side deadline as "Exception while reading from
    /// stream", which read literally says the network broke.</description></item>
    /// <item><description><b>The schema does not have what the query asked for</b> — DEBUG, and BOTH 42P01
    /// undefined_table and 42703 undefined_column: a rolling deploy that puts this service ahead of a
    /// not-yet-migrated store raises 42703 for a column a later rung added (<c>postgres_major_version</c>,
    /// V100), and classifying only the table case would log an ERROR every pass for the whole migration
    /// window.</description></item>
    /// <item><description><b>Anything else</b> — ERROR. An unrecognised failure is a fault until someone says
    /// otherwise.</description></item>
    /// </list>
    /// The site names itself via <see cref="CallerMemberNameAttribute"/> so a copied collect method cannot
    /// report under the wrong name.
    /// </summary>
    private void ReportCollectionFailure(
        Exception ex,
        AnalysisContext context,
        [CallerMemberName] string collectMethod = "")
    {
        if (PgBaselineProvider.IsCommandTimeout(ex))
        {
            _logger?.LogWarning(
                "[PgTargetFactCollector] {CollectMethod} did not finish within its command timeout on server {ServerId} ({ServerName}) — that analysis input is MISSING for this pass, which is not the same as the server having none. The store side logs this as 'canceling statement due to user request'. If it repeats, the window this query scans has outgrown the timeout: {Message}",
                collectMethod, context.ServerId, context.ServerName, ex.Message);
        }
        else if (ex is PostgresException { SqlState: "42P01" or "42703" } pgEx)
        {
            _logger?.LogDebug(
                "[PgTargetFactCollector] {CollectMethod} skipped on server {ServerId} ({ServerName}): the store does not have a table or column it reads (SQLSTATE {SqlState}), which is the pre-migration / version-skew case, so it contributes no facts. {Message}",
                collectMethod, context.ServerId, context.ServerName,
                pgEx.SqlState, ex.Message);
        }
        else
        {
            _logger?.LogError(
                "[PgTargetFactCollector] {CollectMethod} failed on server {ServerId} ({ServerName}) and contributes no facts this pass: {Message}",
                collectMethod, context.ServerId, context.ServerName, ex.Message);
        }
    }

    /// <summary>
    /// The collect surface (v1 + the v2 families of #3691), in emission order. Every method below exists today; the ones a content lane
    /// owns return immediately until that lane lands, and the census in <c>PgTargetFactCollectorTests</c>
    /// names this exact list so that adding, removing or renaming a family is a visible decision.
    /// </summary>
    public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var facts = new List<Fact>();

        /* #3538 A2: the coverage stamp comes FIRST, because every rate and fraction fact below divides by
           it. Nothing may run ahead of it — and for a PostgreSQL target the witness is pg_database_stats,
           the one universal one-minute series every flavour writes (D3), not the wait table. */
        await CollectObservedCoverageAsync(context, facts);
        await CollectServerMetadataFactsAsync(context, facts);
        await CollectConfigFactsAsync(context, facts);
        await CollectPostureFactsAsync(context, facts);
        await CollectDatabaseFactsAsync(context, facts);
        await CollectWriteFactsAsync(context, facts);
        await CollectBufferFactsAsync(context, facts);
        await CollectSessionFactsAsync(context, facts);
        await CollectVacuumFactsAsync(context, facts);
        await CollectWaitFactsAsync(context, facts);
        await CollectQueryFactsAsync(context, facts);
        await CollectCpuFactsAsync(context, facts);
        /* v2 (#3691): the three new families, after every v1 family so a v2 fact composed at collect time can
           read a v1 context fact already in the list (the Config-first rule, extended). Stubs until lanes 11 /
           12 / 13 land. */
        await CollectIoFactsAsync(context, facts);
        await CollectReplicationFactsAsync(context, facts);
        await CollectBloatFactsAsync(context, facts);

        return facts;
    }

    /// <summary>
    /// Every query this collector executes, for the ungated dialect / hygiene pins in Darling.Tests
    /// (<c>$N</c> positional only, no bare <c>now()</c> / <c>CURRENT_TIMESTAMP</c>, every <c>FROM</c> /
    /// <c>JOIN</c> target a collector table or <c>servers</c>).
    ///
    /// <para>DERIVED by reflection over this class's <c>public const string *Sql</c> fields rather than
    /// hand-listed as <see cref="PgFactCollector.AllSql"/> is, and the difference is deliberate: that list
    /// lives in one file that one lane owns, while THIS collector's SQL is written by up to eight lanes in
    /// parallel, each in its own partial file. A hand list would make this file the merge conflict every
    /// lane hits. The convention the derivation rests on — every executed query is a <c>public const string</c>
    /// whose name ends in <c>Sql</c> — is itself pinned: <c>PgTargetFactCollectorTests</c> scans every
    /// <c>NpgsqlCommand</c> construction in the <c>PgTargetFactCollector.*.cs</c> files and asserts its SQL
    /// argument names such a const, so a query executed from a private literal fails a test rather than
    /// escaping the dialect pins.</para>
    /// </summary>
    public static IReadOnlyList<string> AllSql { get; } = typeof(PgTargetFactCollector)
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Sql", StringComparison.Ordinal))
        .OrderBy(f => f.Name, StringComparer.Ordinal)
        .Select(f => (string)f.GetRawConstantValue()!)
        .ToList();

    /// <summary>Kind-Unspecified for parameter binds — Npgsql 6+ rejects Kind-Utc against
    /// <c>timestamp</c> (the PgCollectorRowWriter / PgFindingStore discipline).</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static long ToInt64(object value) => PgBlockingPairRowQuery.ToInt64(value);

    /* ── Family partials, one per file, in the emission order above. The plumbing lane fills Coverage and
       Metadata; each other file names the lane that fills it. ── */

    private partial Task CollectObservedCoverageAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectServerMetadataFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectConfigFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectPostureFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectDatabaseFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectWriteFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectBufferFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectSessionFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectVacuumFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectWaitFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectQueryFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectCpuFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectIoFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectReplicationFactsAsync(AnalysisContext context, List<Fact> facts);
    private partial Task CollectBloatFactsAsync(AnalysisContext context, List<Fact> facts);
}
