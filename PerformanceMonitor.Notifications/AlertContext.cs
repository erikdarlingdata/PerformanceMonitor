/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Optional detail context attached to alert emails.
/// Populated from blocking/deadlock detail queries at alert time.
/// </summary>
public class AlertContext
{
    public List<AlertDetailItem> Details { get; set; } = new();
    public string? AttachmentXml { get; set; }
    public string? AttachmentFileName { get; set; }

    /// <summary>
    /// Forces the rendered severity tier (email badge/color, Teams/Slack accent) regardless of
    /// metric name, for metrics graded at runtime — low-disk fires WARNING normally and CRITICAL
    /// when critically low (#1136). <c>null</c> = use the per-metric <see cref="AlertSeverity"/>
    /// map. Deliberately not persisted (like <see cref="AttachmentXml"/>): it drives the live
    /// email/webhook render only, and the alert-history UI does not re-derive severity, so the
    /// JSON projection (<see cref="AlertContextSerializer"/>) need not carry it.
    /// </summary>
    public AlertSeverityLevel? SeverityOverride { get; set; }

    /// <summary>
    /// One entry per distinct grouped incident this alert covers (#1140). Carries the stable
    /// dedup fingerprint + human-readable involved objects that downstream automation uses to
    /// collapse recurrences of the same incident. <c>null</c>/empty = no fingerprintable incident
    /// (alert type not wired, or no objects resolvable). Persisted in the alert-history context JSON.
    /// </summary>
    public List<AlertIncident>? Incidents { get; set; }
}

/// <summary>
/// A single dedup-able incident within an alert (#1140): a stable fingerprint, the fully-qualified
/// objects it involves (human-readable, for the ticket body), the grouped occurrence count, and an
/// optional display-only wait range. Volatile per-sample fields (wait time, durations, SPIDs) are
/// never part of <see cref="DedupKey"/> — only the identity members hashed by
/// <see cref="AlertFingerprint"/>.
/// </summary>
/// <param name="OccurrenceCount">
/// How many events with this fingerprint are in the CURRENT read window (the rolling hour the
/// groupers counted). A gauge, not a total: it rises as events arrive and falls as they age out.
/// </param>
/// <param name="TotalOccurrences">
/// #2216: occurrences of this fingerprint accumulated across the whole incident — monotonic for as
/// long as the incident lasts, so a consumer that only sees throttled deliveries can still recover
/// how many events actually happened between two of them. <c>null</c> on any path with no
/// accumulator behind it (a host that does not persist occurrence state, or an alert whose incidents
/// are built outside the engine), which reads as "no total available" rather than a false zero.
/// Accumulated by <c>IncidentOccurrenceAccumulator</c>; see its remarks for the exactness bound.
/// </param>
/// <param name="IncidentStartedUtc">
/// #2216: when this fingerprint's current incident was first observed. The incident identity that
/// makes <paramref name="TotalOccurrences"/> interpretable — a consumer seeing the total go
/// backwards can tell a genuine new incident (this moved) from a service restart or a dropped
/// store (this did not).
/// </param>
public sealed record AlertIncident(
    string DedupKey,
    IReadOnlyList<string> InvolvedObjects,
    int OccurrenceCount = 1,
    string? WaitRange = null,
    IReadOnlyList<AlertIncidentField>? DetailFields = null,
    long? TotalOccurrences = null,
    DateTime? IncidentStartedUtc = null,
    /* #2361: the incident's database scope, as a discrete member rather than something a consumer has to
       string-search Details[] for. That search is exact only for deadlocks -- they are self-contained, built
       with includeDetailFields: true -- while every other fingerprinted alert appends a BARE Incident item
       beside its data item, so the incident's own section carries no Database at all and the fallback becomes
       "any Database anywhere in the payload". On a multi-incident alert spanning databases that is not an
       approximation, it is the wrong value with nothing marking it wrong.

       Distinct from InvolvedObjects, which is what the incident is ABOUT (tables, mount points, job names).
       This is where it lives. Null when the alert is not database-scoped -- a volume or a job is not. */
    string? Database = null,
    /* #2361: the newest event in this incident, the counterpart to IncidentStartedUtc. Populated from
       IncidentOccurrenceState.LastObservedUtc, which the accumulator already computes and persists as the
       value its staleness horizon compares against -- so this is a projection of something that existed, not
       a new measurement. Null on any alert the accumulator does not run for. */
    DateTime? LastEventUtc = null);

/// <summary>
/// A forensic label/value pair carried on an <see cref="AlertIncident"/> for #1141 Per-event delivery
/// (e.g. Victim SQL / Processes for a deadlock; Database / Blocked Query / Blocking Query / Lock Mode
/// for a blocking chain). Transient: populated by the incident groupers, rendered into the per-event
/// card by <see cref="PerEventNotification"/>, and deliberately NOT persisted (the Summary card already
/// lists the per-incident detail via the builders, and the per-event card's rendered Details are what
/// get saved). Summary rendering ignores it, so that path is unchanged.
/// </summary>
public sealed record AlertIncidentField(string Label, string Value);

/// <summary>
/// A single detail item (e.g., one blocking chain or one deadlock participant).
/// </summary>
public class AlertDetailItem
{
    public string Heading { get; set; } = "";
    public List<(string Label, string Value)> Fields { get; set; } = new();

    /// <summary>
    /// Multi-paragraph prose for this item (advice Investigation / Remediation).
    /// When non-null, renderers emit this as flowing paragraph text rather than
    /// label/value rows.
    /// </summary>
    public string? Body { get; set; }

    /// <summary>
    /// Marks this item as a copy-paste code block. Renderers emit a monospace
    /// &lt;pre&gt; (HTML) / fenced (plain text) / Consolas TextBox with a copy
    /// button (dialog). Webhooks render only the heading + a "see email or in-app
    /// dialog for the copy-paste T-SQL" hint when this flag is true.
    /// </summary>
    public bool IsCodeBlock { get; set; }

    /// <summary>
    /// Structured, typed remediation payload that lets the in-app dialog drive a
    /// parameterised Apply from this code block (PLAN_REGRESSION force-plan in
    /// v1). Null for every item that is not an applicable remediation T-SQL block
    /// — and null for legacy persisted contexts written before this field existed,
    /// which is the no-Apply-button case.
    /// </summary>
    public RemediationAction? Remediation { get; set; }
}

/// <summary>
/// Serialization DTO for persisting <see cref="AlertContext"/> as JSON.
/// <see cref="AlertDetailItem.Fields"/> is a <c>List&lt;(string,string)&gt;</c>
/// ValueTuple, which System.Text.Json will not round-trip (tuple elements are
/// fields, not properties); these DTOs name every member explicitly so the
/// persisted context survives the round-trip into the in-app dialog.
/// <see cref="AlertContext.AttachmentXml"/>/<see cref="AlertContext.AttachmentFileName"/>
/// are deliberately not persisted (the dialog has no attachment surface).
/// </summary>
public record AlertContextDto(List<AlertDetailItemDto> Details, List<AlertIncidentDto>? Incidents = null);
public record AlertDetailItemDto(string Heading, List<FieldDto> Fields, string? Body, bool IsCodeBlock, RemediationActionDto? Remediation = null);
public record FieldDto(string Label, string Value);

/// <summary>
/// JSON mirror of <see cref="AlertIncident"/> (#1140). The trailing optional <c>Incidents</c>
/// member on <see cref="AlertContextDto"/> keeps the round-trip backward-compatible: legacy
/// contextJson written before this field existed deserializes <c>Incidents</c> to null.
/// <para>
/// #2216's two members are trailing and nullable for the same reason: a history row written before
/// they existed rehydrates them as null, which is exactly "this alert carried no total" rather than
/// a fabricated zero. <see cref="AlertIncident.DetailFields"/> remains unpersisted.
/// </para>
/// </summary>
public record AlertIncidentDto(
    string DedupKey,
    List<string> InvolvedObjects,
    int OccurrenceCount = 1,
    string? WaitRange = null,
    long? TotalOccurrences = null,
    DateTime? IncidentStartedUtc = null,
    /* #2361. Trailing and optional for the same reason the rest of this DTO is: legacy contextJson written
       before these existed deserializes them to null rather than failing the round trip. */
    string? Database = null,
    DateTime? LastEventUtc = null);

/// <summary>
/// JSON mirror of <see cref="RemediationAction"/> / <see cref="ForcePlanTarget"/>
/// (PerformanceMonitor.Analysis). The trailing optional member on
/// <see cref="AlertDetailItemDto"/> plus the reference-type nullability here make
/// the round-trip backward-compatible: legacy contextJson with no Remediation
/// property deserializes the field to null.
/// </summary>
public record RemediationActionDto(
    string FactKey,
    string Action,
    List<ForcePlanTargetDto> Targets,
    List<DbConfigTargetDto>? DbConfigTargets = null,
    RcsiInactionFiguresDto? RcsiFigures = null,
    List<ClearPlanTargetDto>? ClearPlanTargets = null,
    ClearPlanFiguresDto? ClearPlanFigures = null,
    List<FileGrowthTargetDto>? FileGrowthTargets = null,
    List<RcsiTargetDto>? RcsiTargets = null,
    List<ServerConfigTargetDto>? ServerConfigTargets = null,
    List<MissingIndexTargetDto>? MissingIndexTargets = null);

/// <summary>
/// JSON mirror of <see cref="RcsiTarget"/>. The per-database RCSI targets are carried on a
/// DB_CONFIG action PURELY so the Recommendations reader can fan per-db RCSI cards on read
/// (the drill-down is ephemeral); they are never executed from the DB_CONFIG action itself.
/// The trailing optional <c>RcsiTargets</c> member on <see cref="RemediationActionDto"/> keeps
/// the round-trip backward-compatible: legacy/non-DB_CONFIG contextJson without it deserializes
/// to null.
/// </summary>
public record RcsiTargetDto(
    string Database,
    RcsiInactionFiguresDto Figures);

/// <summary>
/// JSON mirror of <see cref="ClearPlanTarget"/> (clear-cached-plan, PR-B). The
/// <c>QueryHash</c> is the only execution input; the remaining members are display/
/// disclosure only. The trailing optional <c>ClearPlanTargets</c> member on
/// <see cref="RemediationActionDto"/> keeps the round-trip backward-compatible: legacy/
/// non-CLEAR_PLAN contextJson without it deserializes to null.
/// </summary>
public record ClearPlanTargetDto(
    string Database,
    string QueryHash,
    double CurrentCpuPerExecMs,
    double BaselineCpuPerExecMs,
    double AnomalyRatio,
    string? LatestPlanHandle);

/// <summary>
/// JSON mirror of <see cref="ClearPlanFigures"/> (clear-cached-plan, PR-B). Carried on
/// the persisted CLEAR_PLAN action so the informed-consent dialog shows the REAL anomaly
/// figures (incl. the window CPU%, LOW-1) at apply time, when the UI apply call site has
/// no finding. Trailing optional → backward-compatible.
/// </summary>
public record ClearPlanFiguresDto(
    double CurrentCpuPerExecMs,
    double BaselineCpuPerExecMs,
    double AnomalyRatio,
    int CpuPercent,
    bool PlanRegressionCoFired,
    bool ParameterSensitivityCoFired);

/// <summary>
/// JSON mirror of <see cref="RcsiInactionFigures"/> (B3 Phase 3). Carried on the
/// persisted RCSI action so the informed-consent dialog shows the REAL blocking/
/// deadlock/reader-writer figures at apply time (the UI apply call site has no
/// finding). The trailing optional member on <see cref="RemediationActionDto"/>
/// keeps the round-trip backward-compatible: legacy/non-RCSI contextJson without it
/// deserializes to null.
/// </summary>
public record RcsiInactionFiguresDto(
    int BlockingEvents,
    int Deadlocks,
    int? ReaderWriterPct);
/// <summary>
/// JSON mirror of <c>ForcePlanTarget</c>. <see cref="ReplicaRole"/> is appended LAST and defaulted so
/// the round-trip stays backward-compatible in both directions: legacy <c>remediation_action_json</c>
/// written before #1882 has no such property and deserializes to null, which is the same thing the
/// extractor produces for a server that does not attribute replicas — so an old row and a
/// non-AG row are indistinguishable, as they should be.
/// <see cref="ParameterSensitivityCoFired"/> (#2138 gap 3) follows the same appended-and-defaulted
/// discipline — and it MUST be mirrored here, not just on the record: both apps render their
/// copy-paste command from the DESERIALIZED action, so a flag dropped by this DTO never reaches the
/// pasted surface at all, and the future auto-force bot reading persisted actions would see false
/// for every flagged target (review catch on #2140).
/// </summary>
public record ForcePlanTargetDto(
    string Database,
    long QueryId,
    long PlanId,
    string? BestPlanHash,
    string? LatestPlanHash,
    double LatestCpuPerExecUs,
    double BestCpuPerExecUs,
    double RegressionFactor,
    string? ReplicaRole = null,
    bool ParameterSensitivityCoFired = false);

/// <summary>
/// JSON mirror of <see cref="DbConfigTarget"/>. <see cref="Setting"/> is persisted
/// as the enum's int value. The trailing optional <c>DbConfigTargets</c> member on
/// <see cref="RemediationActionDto"/> keeps the round-trip backward-compatible:
/// legacy contextJson without it deserializes to null.
/// </summary>
public record DbConfigTargetDto(
    string Database,
    int Setting,
    string? CurrentValue);

/// <summary>
/// JSON mirror of <see cref="FileGrowthTarget"/> (WS3 percent-autogrowth advisory). The
/// trailing optional <c>FileGrowthTargets</c> member on <see cref="RemediationActionDto"/>
/// keeps the round-trip backward-compatible: legacy/non-autogrowth contextJson without it
/// deserializes to null. Carried so the copy-paste MODIFY FILE statements survive the
/// persisted-action round-trip the Recommendations reader renders from (the drill-down is
/// ephemeral). Advisory only — there is no handler, so it never drives Apply.
/// </summary>
public record FileGrowthTargetDto(
    string Database,
    string LogicalFileName,
    double CurrentSizeMb,
    int CurrentGrowthPercent,
    int RecommendedGrowthMb);

/// <summary>
/// JSON mirror of <see cref="ServerConfigTarget"/> (WS3 server-level config). <see cref="Setting"/>
/// is persisted as the enum's int value. The trailing optional <c>ServerConfigTargets</c> member on
/// <see cref="RemediationActionDto"/> keeps the round-trip backward-compatible: legacy/non-
/// SERVER_CONFIG contextJson without it deserializes to null. Carried so the Recommendations reader
/// can fan the server-config cards on read (the drill-down is ephemeral); MAXDOP/CTFP drive Apply,
/// the two memory targets are copy-paste only.
/// </summary>
public record ServerConfigTargetDto(
    int Setting,
    long CurrentValue,
    long RecommendedValue);

/// <summary>
/// JSON mirror of <see cref="MissingIndexTarget"/> (WS4 missing-index advisory). The trailing
/// optional <c>MissingIndexTargets</c> member on <see cref="RemediationActionDto"/> keeps the
/// round-trip backward-compatible: legacy/non-MISSING_INDEX contextJson without it deserializes to
/// null. Carried so the Recommendations reader can render the suggested CREATE on read (the
/// drill-down is ephemeral). Copy-paste only — there is no handler, so it never drives Apply.
/// </summary>
public record MissingIndexTargetDto(
    string Table,
    double Impact,
    string CreateStatement);

/// <summary>
/// Maps <see cref="AlertContext"/> to/from the <see cref="AlertContextDto"/> JSON projection
/// persisted alongside the flat detail_text. Centralizes the DTO mapping so the persistence
/// write (EmailAlertService) and the dialog read (AlertDetailWindow) cannot drift.
/// </summary>
public static class AlertContextSerializer
{
    /// <summary>
    /// True when the persisted <paramref name="contextJson"/> carries the given #1140 dedup fingerprint
    /// (#1154 per-fingerprint cooldown seed). Anchored substring match on the serialized
    /// <c>"DedupKey":"&lt;value&gt;"</c> property. Originally documented as safe only for lowercase
    /// SHA-256 hex fingerprints (no JSON escaping, no collision with any other serialized field) — #2716
    /// added a caller (Postgres Tier-0-predictor cooldown seeding) that passes a raw, human-readable
    /// database/slot name instead of a hash, which a bare string-concatenation match gets wrong for any
    /// name containing a quote, backslash, or non-ASCII character (the value <see cref="Serialize"/>
    /// actually emits is JSON-escaped; the naively-built search pattern was not). Fixed at the root via
    /// <see cref="BuildDedupKeyJsonFragment"/> rather than by keeping this hex-only and pushing an
    /// escaping obligation onto every caller — so it is now correct for ANY dedup key, hashed or not.
    /// Returns false on null/blank input — the Dashboard scan visits many rows whose <c>ContextJson</c> is
    /// null (tray/muted/server-reachability rows), and an un-guarded match would NRE. Centralizes the JSON
    /// shape so the Dashboard store cannot drift; the Lite/Darling stores re-state the same anchor in
    /// their SQL <c>LIKE</c> via <see cref="BuildDedupKeyLikePattern"/> for push-down and are guarded by a
    /// store round-trip test.
    /// </summary>
    public static bool ContextJsonContainsDedupKey(string? contextJson, string? dedupKey)
    {
        if (string.IsNullOrEmpty(contextJson) || string.IsNullOrEmpty(dedupKey))
            return false;
        return contextJson.Contains(BuildDedupKeyJsonFragment(dedupKey), StringComparison.Ordinal);
    }

    /// <summary>
    /// The exact JSON fragment <see cref="Serialize"/> would produce for this dedup key's
    /// <c>"DedupKey":"..."</c> property — computed by round-tripping <paramref name="dedupKey"/> through
    /// <see cref="JsonSerializer.Serialize{TValue}(TValue, System.Text.Json.JsonSerializerOptions?)"/>
    /// itself (default options, the same ones <see cref="Serialize"/> uses) rather than hand-rolling the
    /// escaping rules, so this can never drift from what actually gets written. A hex fingerprint
    /// round-trips unchanged; a raw name with a quote, backslash, or non-ASCII character comes back
    /// correctly escaped the same way the persisted value was.
    /// </summary>
    public static string BuildDedupKeyJsonFragment(string dedupKey)
    {
        var quoted = JsonSerializer.Serialize(dedupKey);
        // JsonSerializer.Serialize(string) always returns a quoted JSON string literal (e.g. "café"),
        // so stripping exactly one leading and one trailing quote yields the escaped inner content.
        var escaped = quoted.Substring(1, quoted.Length - 2);
        return "\"DedupKey\":\"" + escaped + "\"";
    }

    /// <summary>
    /// The full <c>LIKE</c> pattern (with <c>%</c> wildcards) a SQL store should use to find
    /// <paramref name="dedupKey"/> inside a persisted <c>ContextJson</c> column — pair with
    /// <c>ESCAPE '\'</c> on the query. Starts from the same JSON-escaped fragment
    /// <see cref="BuildDedupKeyJsonFragment"/> computes, then additionally escapes the three characters
    /// <c>LIKE</c> itself treats specially (<c>\</c>, <c>%</c>, <c>_</c>) so a database/slot name
    /// containing an underscore (e.g. <c>orders_db</c>) matches only itself instead of also matching
    /// <c>ordersXdb</c> for any character X. Escaping the backslash FIRST is load-bearing: it must run
    /// before the <c>%</c>/<c>_</c> escaping so the backslashes those two insert are not themselves
    /// re-escaped.
    /// </summary>
    public static string BuildDedupKeyLikePattern(string dedupKey)
    {
        var fragment = BuildDedupKeyJsonFragment(dedupKey);
        var likeSafe = fragment
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal);
        return "%" + likeSafe + "%";
    }

    public static string Serialize(AlertContext context)
    {
        var dto = new AlertContextDto(
            context.Details.ConvertAll(d => new AlertDetailItemDto(
                d.Heading,
                d.Fields.ConvertAll(f => new FieldDto(f.Label, f.Value)),
                d.Body,
                d.IsCodeBlock,
                ToDto(d.Remediation))),
            ToDto(context.Incidents));
        return JsonSerializer.Serialize(dto);
    }

    /// <summary>
    /// #2302: just the incidents array, for the generic webhook's <c>{{incidents_json}}</c> token —
    /// the SAME <see cref="AlertIncidentDto"/> projection <see cref="Serialize"/> embeds in the
    /// persisted ContextJson (one shape for every consumer, this method and the full write cannot
    /// drift because both go through the same mapping). <c>"[]"</c> for a null context or an alert
    /// with no fingerprintable incident, so a template's <c>"incidents": {{incidents_json}}</c>
    /// stays well-formed JSON either way.
    /// </summary>
    public static string SerializeIncidents(AlertContext? context)
    {
        var incidents = ToDto(context?.Incidents);
        return incidents is null ? "[]" : JsonSerializer.Serialize(incidents);
    }

    private static List<AlertIncidentDto>? ToDto(List<AlertIncident>? incidents) =>
        incidents?.ConvertAll(i => new AlertIncidentDto(
            i.DedupKey,
            new List<string>(i.InvolvedObjects),
            i.OccurrenceCount,
            i.WaitRange,
            i.TotalOccurrences,
            i.IncidentStartedUtc,
            i.Database,
            i.LastEventUtc));

    /// <summary>
    /// Serializes a single <see cref="RemediationAction"/> to JSON for persistence on a
    /// finding row (recommendations rebuild D2). Reuses the SAME private
    /// <see cref="ToDto(RemediationAction?)"/> projection the alert-context write already
    /// uses, so a finding's persisted action round-trips byte-identically to one carried
    /// in an alert's ContextJson (incl. RcsiInactionFigures / ClearPlanFigures / all
    /// target lists). Returns null when the action is null.
    /// </summary>
    public static string? SerializeAction(RemediationAction? action)
    {
        if (action is null)
            return null;
        return JsonSerializer.Serialize(ToDto(action));
    }

    /// <summary>
    /// Deserializes a finding's persisted <c>remediation_action_json</c> back into a
    /// <see cref="RemediationAction"/> via the SAME private
    /// <see cref="FromDto(RemediationActionDto?)"/> the alert-context read uses. Returns
    /// null for null/blank/garbage JSON (try-catch, mirroring <see cref="TryDeserialize"/>),
    /// so a corrupt column degrades to "no Apply affordance" rather than throwing.
    /// </summary>
    public static RemediationAction? DeserializeAction(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;
        try
        {
            return FromDto(JsonSerializer.Deserialize<RemediationActionDto>(json));
        }
        catch
        {
            return null;
        }
    }

    public static bool TryDeserialize(string? json, out AlertContext context)
    {
        context = new AlertContext();
        if (string.IsNullOrWhiteSpace(json))
            return false;

        try
        {
            var dto = JsonSerializer.Deserialize<AlertContextDto>(json);
            if (dto?.Details is null)
                return false;

            foreach (var d in dto.Details)
            {
                var item = new AlertDetailItem
                {
                    Heading = d.Heading,
                    Body = d.Body,
                    IsCodeBlock = d.IsCodeBlock,
                    Remediation = FromDto(d.Remediation)
                };
                if (d.Fields is not null)
                {
                    foreach (var f in d.Fields)
                        item.Fields.Add((f.Label, f.Value));
                }
                context.Details.Add(item);
            }

            /* #1140: rehydrate the dedup incidents. Legacy contextJson without the field leaves
               dto.Incidents null -> context.Incidents stays null (backward-compatible). */
            if (dto.Incidents is { Count: > 0 })
            {
                context.Incidents = new List<AlertIncident>(dto.Incidents.Count);
                foreach (var i in dto.Incidents)
                {
                    context.Incidents.Add(new AlertIncident(
                        i.DedupKey ?? string.Empty,
                        i.InvolvedObjects ?? new List<string>(),
                        i.OccurrenceCount,
                        i.WaitRange,
                        TotalOccurrences: i.TotalOccurrences,
                        IncidentStartedUtc: i.IncidentStartedUtc));
                }
            }
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static RemediationActionDto? ToDto(RemediationAction? action)
    {
        if (action is null)
            return null;

        var targets = new List<ForcePlanTargetDto>(action.Targets.Count);
        foreach (var t in action.Targets)
        {
            targets.Add(new ForcePlanTargetDto(
                t.Database,
                t.QueryId,
                t.PlanId,
                t.BestPlanHash,
                t.LatestPlanHash,
                t.LatestCpuPerExecUs,
                t.BestCpuPerExecUs,
                t.RegressionFactor,
                t.ReplicaRole,
                t.ParameterSensitivityCoFired));
        }

        List<DbConfigTargetDto>? dbConfigTargets = null;
        if (action.DbConfigTargets is not null)
        {
            dbConfigTargets = new List<DbConfigTargetDto>(action.DbConfigTargets.Count);
            foreach (var t in action.DbConfigTargets)
                dbConfigTargets.Add(new DbConfigTargetDto(t.Database, (int)t.Setting, t.CurrentValue));
        }

        var rcsiFigures = action.RcsiFigures is { } f
            ? new RcsiInactionFiguresDto(f.BlockingEvents, f.Deadlocks, f.ReaderWriterPct)
            : null;

        // Clear-cached-plan (PR-B): persist the targets + carried figures so the affordance
        // survives the contextJson round-trip and the dialog shows the REAL numbers at apply.
        List<ClearPlanTargetDto>? clearPlanTargets = null;
        if (action.ClearPlanTargets is not null)
        {
            clearPlanTargets = new List<ClearPlanTargetDto>(action.ClearPlanTargets.Count);
            foreach (var t in action.ClearPlanTargets)
                clearPlanTargets.Add(new ClearPlanTargetDto(
                    t.Database, t.QueryHash, t.CurrentCpuPerExecMs, t.BaselineCpuPerExecMs,
                    t.AnomalyRatio, t.LatestPlanHandle));
        }

        var clearPlanFigures = action.ClearPlanFigures is { } cf
            ? new ClearPlanFiguresDto(cf.CurrentCpuPerExecMs, cf.BaselineCpuPerExecMs, cf.AnomalyRatio,
                cf.CpuPercent, cf.PlanRegressionCoFired, cf.ParameterSensitivityCoFired)
            : null;

        // WS3 (percent-autogrowth advisory): persist the file targets so the Recommendations
        // reader can render the copy-paste MODIFY FILE statements on read (the drill-down is
        // ephemeral). Null for every other fact key -> backward-compatible.
        List<FileGrowthTargetDto>? fileGrowthTargets = null;
        if (action.FileGrowthTargets is not null)
        {
            fileGrowthTargets = new List<FileGrowthTargetDto>(action.FileGrowthTargets.Count);
            foreach (var t in action.FileGrowthTargets)
                fileGrowthTargets.Add(new FileGrowthTargetDto(
                    t.Database, t.LogicalFileName, t.CurrentSizeMb, t.CurrentGrowthPercent, t.RecommendedGrowthMb));
        }

        // Per-db RCSI targets (carried on a DB_CONFIG action for the read-time card fan-out).
        // Persist them so the Recommendations reader can fan per-db RCSI cards after the round-
        // trip (the drill-down they came from is ephemeral). Null for every other fact key ->
        // backward-compatible. Never executed from the DB_CONFIG action.
        List<RcsiTargetDto>? rcsiTargets = null;
        if (action.RcsiTargets is not null)
        {
            rcsiTargets = new List<RcsiTargetDto>(action.RcsiTargets.Count);
            foreach (var t in action.RcsiTargets)
                rcsiTargets.Add(new RcsiTargetDto(
                    t.Database,
                    new RcsiInactionFiguresDto(t.Figures.BlockingEvents, t.Figures.Deadlocks, t.Figures.ReaderWriterPct)));
        }

        // WS3 (server-level config): persist the targets so the Recommendations reader can fan the
        // server-config cards on read (the drill-down is ephemeral) AND the ServerConfigHandler can
        // re-derive the MAXDOP/CTFP target at apply. Null for every other fact key -> backward-compatible.
        List<ServerConfigTargetDto>? serverConfigTargets = null;
        if (action.ServerConfigTargets is not null)
        {
            serverConfigTargets = new List<ServerConfigTargetDto>(action.ServerConfigTargets.Count);
            foreach (var t in action.ServerConfigTargets)
                serverConfigTargets.Add(new ServerConfigTargetDto((int)t.Setting, t.CurrentValue, t.RecommendedValue));
        }

        // WS4 (missing-index advisory): persist the suggested CREATE statements so the Recommendations
        // reader can render them as copy-paste on read (the drill-down is ephemeral). Copy-paste only —
        // no handler, never Apply. Null for every other fact key -> backward-compatible.
        List<MissingIndexTargetDto>? missingIndexTargets = null;
        if (action.MissingIndexTargets is not null)
        {
            missingIndexTargets = new List<MissingIndexTargetDto>(action.MissingIndexTargets.Count);
            foreach (var t in action.MissingIndexTargets)
                missingIndexTargets.Add(new MissingIndexTargetDto(t.Table, t.Impact, t.CreateStatement));
        }

        return new RemediationActionDto(action.FactKey, action.Action, targets, dbConfigTargets,
            rcsiFigures, clearPlanTargets, clearPlanFigures, fileGrowthTargets, rcsiTargets,
            serverConfigTargets, missingIndexTargets);
    }

    private static RemediationAction? FromDto(RemediationActionDto? dto)
    {
        if (dto is null)
            return null;

        var targets = new List<ForcePlanTarget>(dto.Targets?.Count ?? 0);
        if (dto.Targets is not null)
        {
            foreach (var t in dto.Targets)
            {
                targets.Add(new ForcePlanTarget(
                    t.Database,
                    t.QueryId,
                    t.PlanId,
                    t.BestPlanHash,
                    t.LatestPlanHash,
                    t.LatestCpuPerExecUs,
                    t.BestCpuPerExecUs,
                    t.RegressionFactor,
                    t.ReplicaRole,
                    t.ParameterSensitivityCoFired));
            }
        }

        // m-A: deserialize the DB-config targets and PASS them to the ctor. The
        // RemediationAction ctor's trailing DbConfigTargets defaults to null, so a
        // 3-arg call here would silently drop a DB_CONFIG action's targets on the
        // round-trip (un-applyable from any persisted context). Legacy JSON without
        // the field deserializes dto.DbConfigTargets to null -> dbConfigTargets null
        // -> backward-compatible.
        List<DbConfigTarget>? dbConfigTargets = null;
        if (dto.DbConfigTargets is not null)
        {
            dbConfigTargets = new List<DbConfigTarget>(dto.DbConfigTargets.Count);
            foreach (var t in dto.DbConfigTargets)
                dbConfigTargets.Add(new DbConfigTarget(t.Database, (DbConfigSetting)t.Setting, t.CurrentValue));
        }

        // B3 Phase 3: the RCSI risk figures must survive the round-trip so the dialog
        // shows the REAL numbers at apply time. Legacy/non-RCSI JSON without the field
        // deserializes to null -> the disclosure falls back to the finding/weak-case.
        var rcsiFigures = dto.RcsiFigures is { } f
            ? new RcsiInactionFigures(f.BlockingEvents, f.Deadlocks, f.ReaderWriterPct)
            : null;

        // Clear-cached-plan (PR-B): rebuild the targets + carried figures from the DTO and
        // PASS them to the ctor (the trailing ClearPlan* members default to null, so a short
        // call would silently drop a CLEAR_PLAN action's targets on the round-trip). Legacy
        // JSON without the fields deserializes to null → backward-compatible.
        List<ClearPlanTarget>? clearPlanTargets = null;
        if (dto.ClearPlanTargets is not null)
        {
            clearPlanTargets = new List<ClearPlanTarget>(dto.ClearPlanTargets.Count);
            foreach (var t in dto.ClearPlanTargets)
                clearPlanTargets.Add(new ClearPlanTarget(
                    t.Database, t.QueryHash, t.CurrentCpuPerExecMs, t.BaselineCpuPerExecMs,
                    t.AnomalyRatio, t.LatestPlanHandle));
        }

        var clearPlanFigures = dto.ClearPlanFigures is { } cf
            ? new ClearPlanFigures(cf.CurrentCpuPerExecMs, cf.BaselineCpuPerExecMs, cf.AnomalyRatio,
                cf.CpuPercent, cf.PlanRegressionCoFired, cf.ParameterSensitivityCoFired)
            : null;

        // WS3: rebuild the percent-autogrowth file targets from the DTO and PASS them to the
        // ctor (the trailing FileGrowthTargets member defaults to null, so a short call would
        // silently drop them on the round-trip). Legacy JSON without the field -> null.
        List<FileGrowthTarget>? fileGrowthTargets = null;
        if (dto.FileGrowthTargets is not null)
        {
            fileGrowthTargets = new List<FileGrowthTarget>(dto.FileGrowthTargets.Count);
            foreach (var t in dto.FileGrowthTargets)
                fileGrowthTargets.Add(new FileGrowthTarget(
                    t.Database, t.LogicalFileName, t.CurrentSizeMb, t.CurrentGrowthPercent, t.RecommendedGrowthMb));
        }

        // Per-db RCSI targets: rebuild from the DTO and PASS them to the ctor (the trailing
        // RcsiTargets member defaults to null, so a short call would silently drop them on the
        // round-trip — the reader would then fan no RCSI cards). Legacy JSON without the field
        // deserializes to null → backward-compatible.
        List<RcsiTarget>? rcsiTargets = null;
        if (dto.RcsiTargets is not null)
        {
            rcsiTargets = new List<RcsiTarget>(dto.RcsiTargets.Count);
            foreach (var t in dto.RcsiTargets)
                rcsiTargets.Add(new RcsiTarget(
                    t.Database,
                    new RcsiInactionFigures(t.Figures.BlockingEvents, t.Figures.Deadlocks, t.Figures.ReaderWriterPct)));
        }

        // WS3: rebuild the server-config targets from the DTO and PASS them to the ctor (the trailing
        // ServerConfigTargets member defaults to null, so a short call would silently drop them on the
        // round-trip — the reader would then fan no server-config cards and the handler could not
        // re-derive the MAXDOP/CTFP target). Legacy JSON without the field deserializes to null →
        // backward-compatible.
        List<ServerConfigTarget>? serverConfigTargets = null;
        if (dto.ServerConfigTargets is not null)
        {
            serverConfigTargets = new List<ServerConfigTarget>(dto.ServerConfigTargets.Count);
            foreach (var t in dto.ServerConfigTargets)
                serverConfigTargets.Add(new ServerConfigTarget((ServerConfigSetting)t.Setting, t.CurrentValue, t.RecommendedValue));
        }

        // WS4: rebuild the missing-index targets from the DTO and PASS them to the ctor (the trailing
        // MissingIndexTargets member defaults to null, so a short call would silently drop them on the
        // round-trip — the reader would then render no missing-index copy-paste). Legacy JSON without
        // the field deserializes to null → backward-compatible.
        List<MissingIndexTarget>? missingIndexTargets = null;
        if (dto.MissingIndexTargets is not null)
        {
            missingIndexTargets = new List<MissingIndexTarget>(dto.MissingIndexTargets.Count);
            foreach (var t in dto.MissingIndexTargets)
                missingIndexTargets.Add(new MissingIndexTarget(t.Table, t.Impact, t.CreateStatement));
        }

        return new RemediationAction(dto.FactKey, dto.Action, targets, dbConfigTargets, rcsiFigures,
            clearPlanTargets, clearPlanFigures, fileGrowthTargets, rcsiTargets, serverConfigTargets,
            missingIndexTargets);
    }
}
