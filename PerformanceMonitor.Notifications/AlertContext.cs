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
using System.Text.Json;
using System.Text.Json.Serialization;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Notifications;

/// <summary>The newest sample behind an alert (#5642): <paramref name="Source"/> is a short label ("CPU",
/// "Wait stats"), <paramref name="InstantUtc"/> the newest sample's UTC instant, and <paramref name="Age"/>
/// the gap to the fire time, computed once when the alert fires.</summary>
public sealed record AlertNewestSample(string Source, DateTime InstantUtc, TimeSpan Age);

/// <summary>Where an external alert came from (#5654): the source's registered name, the alert key it chose, and the
/// id of the queue row Darling drained, which is how a restart tells whether a claimed row was delivered.</summary>
public sealed record AlertExternalOrigin(string Source, string Key, long PendingId);

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
    /// #7041: a finished HTML email body, for a scheduled report that renders its own page (the Daily Health Report).
    /// <see cref="EmailSendCore"/> sends it as the HTML view in place of the shared alert template when it is set; the
    /// plain view stays the alert's own detail text. Unpersisted like <see cref="AttachmentXml"/>: live-render data,
    /// not row data, so the alert-history context JSON never carries a page.
    /// </summary>
    public string? EmailHtmlBody { get; set; }

    /// <summary>
    /// #7041: the email subject line to use in place of the shared <c>[SQL Monitor Alert] ... on ...</c> subject
    /// when it is set. Unpersisted for the same reason as <see cref="EmailHtmlBody"/>.
    /// </summary>
    public string? EmailSubject { get; set; }

    /// <summary>
    /// Forces the rendered severity tier (email badge/color, Teams/Slack accent) regardless of
    /// metric name, for metrics graded at runtime — low-disk fires WARNING normally and CRITICAL
    /// when critically low (#1136). <c>null</c> = use the per-metric <see cref="AlertSeverity"/>
    /// map.
    ///
    /// <para><b>Persisted since #3539 A8e</b>, as the trailing <c>Severity</c> member of the JSON projection
    /// (<see cref="AlertContextSerializer"/>), so the alert-history row carries the tier the alert actually
    /// FIRED at. It was deliberately not persisted before, on the argument that it drove the live
    /// email/webhook render only and the alert-history UI did not re-derive severity — which was true, and
    /// was the defect: the history grids styled a row by its metric NAME
    /// (<c>AlertMetricClassifier.IsCritical</c>), so once Poison Wait became graded (#2711 on
    /// PostgreSQL, #3539 A4 on SQL Server) a WARNING-graded fire rendered red in every grid, and a
    /// CRITICAL-graded low-disk fire rendered amber. <c>AlertHistoryRowSeverity</c> (Alerting) is the read side:
    /// the row's own tier where one was persisted, the by-name classifier for rows that predate the member.
    /// <see cref="AttachmentXml"/> stays unpersisted for its own reason (no dialog surface, and size).</para>
    /// </summary>
    public AlertSeverityLevel? SeverityOverride { get; set; }

    /// <summary>
    /// True when this firing is a tier ESCALATION the engine wants delivered at once (#5641): an open incident that
    /// was delivered at Warning has just been graded Critical. The email and webhook cooldowns
    /// (<see cref="IncidentCooldown"/>) and the repeat ceiling (<see cref="RepeatDeliveryBudget"/>) read it and let
    /// the firing through inside the window; a successful send still stamps the cooldown, so it restarts the
    /// clock. Set by the engine's <c>TierEscalation</c> helper, for the alerts that opt in (Runnable Queue, Client
    /// Timeouts, High CPU, Deadlocks Detected, Poison Wait and Volume Free Space). Transient: never persisted (the serializer's projection does not carry it) and never rendered.
    /// </summary>
    public bool EscalationBypassesCooldown { get; set; }

    /// <summary>
    /// One entry per distinct grouped incident this alert covers (#1140). Carries the stable
    /// dedup fingerprint + human-readable involved objects that downstream automation uses to
    /// collapse recurrences of the same incident. <c>null</c>/empty = no fingerprintable incident
    /// (alert type not wired, or no objects resolvable). Persisted in the alert-history context JSON.
    /// </summary>
    public List<AlertIncident>? Incidents { get; set; }

    /// <summary>
    /// Where this firing's posts went (#3598, design point 3): the alert's family, the route that matched
    /// it, and each delivered channel with the route that supplied its destination. Set by the deliverer
    /// from the send result AFTER the channels ran — the fan-out resolves it once per firing, post-cooldown
    /// — and persisted as the trailing <c>Route</c> member of the JSON projection so the alert-history row
    /// can answer "which destination matched" when a routing mistake is being reconstructed. Null on a row
    /// that never reached resolution (throttled, folded, muted, no channel configured) and on every row
    /// written before the member existed; never rendered to a channel.
    /// </summary>
    public AlertRouteDto? Route { get; set; }

    /// <summary>
    /// The corroboration gate's decision for an analysis finding (#3712): <c>page</c> or <c>digest</c>, with
    /// the reason naming the corroboration components it read. Set by <c>AnalysisNotificationService</c> on
    /// every finding it routes — the paged ones too, so "why DID this page" is as answerable as "why didn't
    /// it" — and persisted as the trailing <c>Routing</c> member of the JSON projection, the way
    /// <see cref="SeverityOverride"/> is persisted so <c>severity_source</c> can be read off the row. Null
    /// on every engine alert (the gate does not apply to them) and on every row written before the member
    /// existed. Never rendered to a channel: a paged finding's card is unchanged, and a digest-routed finding
    /// reaches no card.
    /// <para>Distinct from <see cref="Route"/>, which records WHERE the posts went after the fan-out ran;
    /// this records whether the fan-out was allowed to run at all, one decision upstream of it.</para>
    /// </summary>
    public AlertRoutingDto? Routing { get; set; }

    /// <summary>
    /// The open engine alert an analysis finding was attached to instead of paging (#7084 part 4), or null: every other
    /// row, and every row written before the member existed. An alert-history row has no id, so the reference is the
    /// server (the finding's own row), the engine alert's metric and its fire time. Persisted as the trailing
    /// <c>AttachedTo</c> member of the JSON projection.
    /// </summary>
    public AlertAttachedToDto? AttachedTo { get; set; }

    /// <summary>
    /// The wait type this firing is ABOUT (#4223 Poison Wait notebook), as structured data rather than
    /// something a reader has to regex out of <see cref="Details"/>'s prose heading or <c>DetailText</c>.
    /// Set by the Poison Wait fire sites on both engines — the SQL Server evaluator's worst-graded
    /// <c>WaitType</c>, the PostgreSQL evaluator's <c>Subject</c> (its own <c>type:event</c> display string,
    /// e.g. <c>"IPC:BtreePage"</c>) — the same value each engine's mute context already keys on
    /// (<c>AlertMuteContext.WaitType</c>). Null on every other alert and on any row written before this
    /// member existed.
    /// </summary>
    public string? WaitType { get; set; }

    /// <summary>
    /// The collector this firing is ABOUT (#4223 self-monitor notebook), as structured data rather than
    /// something a reader has to regex out of <see cref="Details"/>'s prose. Set by the Collector Cost
    /// Regression fire site (<c>DarlingSelfAlertEvaluator</c>'s <c>regression.CollectorName</c>, the same
    /// value the row's cooldown key (<c>cost:{serverId}:{collector}</c>) already carries). Null on every
    /// other alert and on any row written before this member existed.
    /// </summary>
    public string? CollectorName { get; set; }

    /// <summary>
    /// The availability group and replica this firing is ABOUT, for edge alerts that need a per-replica
    /// incident identity (the AG disconnect/reconnect pair — one server can host many replicas of many
    /// groups). Set by both engines' AG fire sites to the replica grain the row itself is keyed on, so
    /// <see cref="WebhookAlertService.DerivePagerDutyDedupKey"/> can scope that pair's dedup key per
    /// replica instead of per server: with auto-resolve on, one replica's reconnect must close only ITS
    /// incident, not the one a still-disconnected sibling is still holding open. Read only through
    /// <see cref="WebhookAlertService.PairedReplicaScope"/>, only for that pair: the dedup key and the
    /// webhook cooldown key (#5469) both take it from there, under the same auto-resolve gate. No renderer,
    /// budget or email cooldown reads it. Null on every other alert and on any
    /// row written before this member existed. Unpersisted: like <see cref="AttachmentXml"/> this is
    /// live-render data, not row data. The pairing name is the literal "AG:replica", because that is
    /// exactly the shape consumers can reason about — see the helper for why.
    /// </summary>
    public string? AgReplicaIdentity { get; set; }

    /// <summary>
    /// Every file this Database File Growth firing is about (#7068), as (database, file) pairs, where the card lists only
    /// the first five breached files in <see cref="Details"/>. It is stored with the context so the standing-state seed after a
    /// restart can tell that a sixth or eighth file of a tempdb layout was already announced. Read only through
    /// <see cref="AlertContextSerializer.ContextJsonNamesFile"/>. No renderer shows it. Null on every other alert and on any
    /// row written before this member existed, which the seed answers from <see cref="Details"/>.
    /// </summary>
    public List<AlertFileIdentity>? FileGrowthFiles { get; set; }

    /// <summary>
    /// The step a TimescaleDB policy job alert announced (#7076): <see cref="AlertContextSerializer.PolicyJobStepReArmed"/> for the
    /// first-detection page after the automatic re-arm, <see cref="AlertContextSerializer.PolicyJobStepEscalated"/> for the escalation
    /// and every repeat after it. The restart seed reads it to tell a job that is owed its "re-hung" page from one already escalated.
    /// Read only through <see cref="AlertContextSerializer.TryReadPolicyJobStep"/>. No renderer shows it. Null on every other alert
    /// and on any row written before this member existed, which the seed treats as escalated.
    /// </summary>
    public string? PolicyJobStep { get; set; }

    /// <summary>The newest source sample behind this alert (#5642): its source label, its UTC instant, and its
    /// age computed ONCE at fire time. Null when the alert has no UTC instant to age (a pre-V134 local stamp, a
    /// live read, or a check not yet wired). Persisted with the alert's context, so the age never drifts at
    /// render time. The renderers print it as one line per body.</summary>
    public AlertNewestSample? NewestSample { get; set; }

    /// <summary>The external source, key and queue row this alert came from (#5654), or null on every alert Darling
    /// raises itself. Darling builds it from the claimed row; the submitting service never supplies context. Persisted
    /// with the alert's context, which is how the history rows are tied back to the queue row and to a source.</summary>
    public AlertExternalOrigin? External { get; set; }

    /// <summary>
    /// A copy of this context that shares its member objects (#5320): the alert statement filter replaces the
    /// members it rewrites and leaves the rest, so a member added to this class later is carried without the
    /// filter being edited. A field-by-field copy would silently drop it.
    /// </summary>
    public AlertContext ShallowCopy() => (AlertContext)MemberwiseClone();
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
    DateTime? LastEventUtc = null,
    /* #3330: the forensic XML file THIS incident's representative event produced -- the deadlock graph or
       the blocked-process report. Transient, like DetailFields: absent from AlertIncidentDto, so the
       history row and the {{incidents_json}} token are unchanged.

       It rides on the incident because AlertContext.AttachmentXml is one string for a whole alert while the
       cards that carry it are per-incident, and a bare string has no fingerprint beside it for a splitter or
       a delivery filter to select on. Null is a normal outcome, not a gap: a blocking incident grouped
       entirely from DMV-snapshot rows has no report to attach (see BlockedProcessAlertRow.HasReportXml), and
       an unlabelled graph belonging to a different fingerprint is worse than none. */
    AlertIncidentAttachment? Attachment = null);

/// <summary>
/// An incident's forensic XML file (#3330) — the deadlock graph or blocked-process report an email attaches.
/// <para>One value carrying both halves rather than two members, because
/// <see cref="AlertContext.AttachmentXml"/> and <see cref="AlertContext.AttachmentFileName"/> are read by
/// two consumers with different rules — <c>EmailSendCore</c> attaches only when BOTH are non-empty, while
/// <c>EmailTemplateBuilder</c> prints "Attached: &lt;name&gt;" on the FILENAME alone — so a half-populated
/// pair is an email that advertises a file it does not carry. A record cannot be half-assigned.</para>
/// </summary>
/// <param name="Xml">The file's contents, as captured.</param>
/// <param name="FileName">The attachment name, one of the two constants below.</param>
public sealed record AlertIncidentAttachment(string Xml, string FileName)
{
    /// <summary>Attachment name for a deadlock graph. Spelled once for every producer of one.</summary>
    public const string DeadlockGraphFileName = "deadlock_graph.xml";

    /// <summary>Attachment name for a blocked-process report. Spelled once for every producer of one.</summary>
    public const string BlockedProcessReportFileName = "blocked_process_report.xml";

    /// <summary>
    /// True when both halves are present, which is the condition <c>EmailSendCore</c> actually attaches on.
    /// An incomplete pair is treated as absent everywhere rather than carried: with only a filename the
    /// email prints "Attached: &lt;name&gt;" and carries nothing, and with only XML it carries nothing and
    /// says nothing. A record cannot be half-ASSIGNED, but it can be constructed with an empty string, so
    /// the producers' "did this row have one" test lives here rather than at each of them.
    /// </summary>
    public bool IsComplete => !string.IsNullOrEmpty(Xml) && !string.IsNullOrEmpty(FileName);

    /// <summary>
    /// The attachment a card rendering <paramref name="incidents"/> should carry: the first one of them that
    /// has an attachment of its own, or <c>null</c> when none does.
    /// <para>The one selection rule, shared by <see cref="PerEventNotification"/>'s splitter and
    /// <see cref="IncidentDeliveryFilter"/>'s copy so the two cannot disagree about which graph belongs to a
    /// card. It deliberately has NO fallback to the alert-level attachment: falling back is how every
    /// per-event card came to carry the first graph in the window, and a card that describes one fingerprint
    /// while attaching another's evidence misleads harder than one that attaches nothing.</para>
    /// <para>Selects from the incident objects themselves, not by re-matching dedup keys against a key list —
    /// an incident with a blank <see cref="AlertIncident.DedupKey"/> (legal; it simply never enters a
    /// cooldown) would be unaddressable by key.</para>
    /// </summary>
    public static AlertIncidentAttachment? ForIncidents(IReadOnlyList<AlertIncident>? incidents)
    {
        if (incidents is null)
        {
            return null;
        }

        foreach (var incident in incidents)
        {
            if (incident.Attachment is { IsComplete: true } attachment)
            {
                return attachment;
            }
        }

        return null;
    }
}

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
    /// The same content as <see cref="Fields"/>, regrouped by the record it came from (#3644) — populated
    /// ONLY when the item was flattened from an array of objects (a drill-down's top-N rows: the High CPU
    /// card's Top Cpu Queries, Queries At Spike, Top Spilling Queries, Parameter Sensitive Queries,
    /// Regressed Queries, Tempdb Breakdown). Empty for every other item.
    /// <para><b>Why a second projection of the same pairs, rather than a replacement.</b> The flat pairs are
    /// correct on every surface that lays them out in ONE column: the persisted <c>context_json</c> and the
    /// in-app Alert Details grid that reads it, <see cref="AlertDetailText.Flatten"/> (the persisted
    /// <c>detail_text</c> and the redundancy oracle behind <c>ProseForDelivery</c>), the email table, the
    /// Teams fact list, PagerDuty's <c>custom_details</c> dictionary, the generic webhook's parts, and the
    /// incident-roster match in <c>IncidentDeliveryFilter</c>. Every one of those reads <c>#1 Database</c>,
    /// <c>#1 Query Hash</c>, … <c>#2 Database</c> top to bottom and the record stays together. The one
    /// surface that does NOT is a Slack section's <c>fields</c> array, which Slack lays out in a two-across
    /// grid filled left to right in submission order: seven attributes per query means query #1's text
    /// lands beside query #2's hash, #3's database floats beside #2's SQL, and a multi-line text inflates
    /// its grid row so the label/value adjacency below it breaks. Read live on a production High CPU page
    /// (#3644), the card a person reads while production is on fire. Fields are Slack's tool for short,
    /// genuinely PAIRED scalars — the card's own <c>Current Value / Threshold</c> pair renders fine — and
    /// the wrong tool for a repeating record.</para>
    /// <para>So a record-shaped item carries both: <see cref="Fields"/> for the flat surfaces, unchanged to
    /// the byte, and this list for the renderers that can keep a record together as one visual unit (Slack
    /// renders each as a bold summary line over the record's text in a code block; email and Teams render
    /// the same compact list). A renderer that knows this list prefers it and skips the fields; one that
    /// does not sees exactly what it saw before. Not persisted: it is derivable from the same drill-down
    /// rows the fields already persist, nothing re-renders a rehydrated context to Slack, and doubling the
    /// drill-down payload of every analysis row for a reader that renders the fields would buy nothing.
    /// Producers that build items by hand (the engine alerts, the incident renderer) never populate it.</para>
    /// </summary>
    public List<AlertDetailRecord> Records { get; set; } = new();

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

    /// <summary>A copy that shares its member objects (#5320); see <see cref="AlertContext.ShallowCopy"/>.</summary>
    public AlertDetailItem ShallowCopy() => (AlertDetailItem)MemberwiseClone();
}

/// <summary>
/// One record of a record-shaped detail item (#3644): the row's scalar attributes packed into ONE
/// summary line, and its long text(s) — the query text, a blocked/blocking SQL pair, a CREATE or ALTER
/// statement — carried separately so a renderer can set them in a code block under the summary rather
/// than beside it.
/// <para><see cref="Ordinal"/> is the row's 1-based position (<c>#1</c>, <c>#2</c>, …), the same number
/// the flat fields prefix their labels with, so a reader moving between Slack and the email finds the same
/// row under the same number. <see cref="Summary"/> is the non-empty scalar properties in the record's OWN
/// order — the collectors write identity first (database, hash, id) and measures after — as
/// <c>Label: value</c> pairs joined by <c>·</c>, numbers with group separators; it never carries the
/// ordinal, which each renderer sets in its own idiom. <see cref="Texts"/> is every string property whose
/// name says it is SQL (<c>query_text</c>, <c>blocked_sql</c>, <c>blocking_sql</c>, <c>victim_sql</c>,
/// <c>create_statement</c>, <c>alter_statement</c>), labelled the way the flat field is, in property
/// order; empty texts are not carried. The text is the row's full value as the collector bounded it (500
/// characters on the query-store drill-downs), NOT the 300-character cut the flat field applies — the
/// renderer that has a ceiling states its own cut.</para>
/// </summary>
public sealed record AlertDetailRecord(int Ordinal, string Summary, List<(string Label, string Text)> Texts);

/// <summary>One file a Database File Growth firing is about (#7068): the database and file name, matched by exact equality. Doubles as the persisted shape.</summary>
public record AlertFileIdentity(string Database, string File);

/// <summary>
/// Serialization DTO for persisting <see cref="AlertContext"/> as JSON.
/// <see cref="AlertDetailItem.Fields"/> is a <c>List&lt;(string,string)&gt;</c>
/// ValueTuple, which System.Text.Json will not round-trip (tuple elements are
/// fields, not properties); these DTOs name every member explicitly so the
/// persisted context survives the round-trip into the in-app dialog.
/// <see cref="AlertContext.AttachmentXml"/>/<see cref="AlertContext.AttachmentFileName"/>
/// are deliberately not persisted (the dialog has no attachment surface).
/// <para>
/// <c>Severity</c> (#3539 A8e) is the tier the alert fired at — <see cref="AlertContext.SeverityOverride"/>
/// — trailing and nullable like <c>Incidents</c>, so a row written before it existed rehydrates to null,
/// which reads as "this row carries no tier" and sends the grids to the by-name fallback. Serialized as the
/// enum's NAME rather than its ordinal: the column outlives any build, and a persisted <c>1</c> would change
/// meaning the day a member is inserted ahead of it, where <c>"Critical"</c> cannot.
/// </para>
/// </summary>
public record AlertContextDto(
    List<AlertDetailItemDto> Details,
    List<AlertIncidentDto>? Incidents = null,
    [property: JsonConverter(typeof(JsonStringEnumConverter))] AlertSeverityLevel? Severity = null,
    AlertRouteDto? Route = null,
    AlertRoutingDto? Routing = null,
    /* #4223 Poison Wait: trailing and nullable like Route/Routing, so a row written before this member
       existed rehydrates to null ("this row carries no wait type") rather than a fabricated value. */
    string? WaitType = null,
    /* #4223 self-monitor notebook: trailing and nullable like WaitType, so a row written before this
       member existed rehydrates to null ("this row carries no collector name") rather than a fabricated
       value. */
    string? CollectorName = null,
    /* #5642 sample age: trailing and nullable like CollectorName, so a row written before this member
       existed rehydrates to null ("this row carries no sample age") rather than a fabricated age. */
    AlertNewestSampleDto? NewestSample = null,
    /* #5654 external alerts: trailing and nullable like NewestSample, so a row written before this member
       existed rehydrates to null ("this row came from no external source"). */
    AlertExternalDto? External = null,
    /* #7068 file growth: trailing and nullable like External, so a row written before this member existed rehydrates
       to null ("this row lists no files beyond its details"). */
    List<AlertFileIdentity>? FileGrowthFiles = null,
    /* #7076 policy job step: trailing and nullable like FileGrowthFiles, so a row written before this member existed rehydrates
       to null ("this row does not say which step it announced"). */
    string? PolicyJobStep = null,
    /* #7084 part 4: trailing and nullable like External, so a row written before this member existed rehydrates to
       null ("this finding is not attached to an engine alert"). */
    AlertAttachedToDto? AttachedTo = null);

/// <summary>
/// The engine alert a held analysis finding was attached to (#7084 part 4): the metric of the delivered row (High CPU,
/// Blocking Detected, Blocking Wait Time or Deadlocks Detected) and its fire time, UTC at the store's microsecond
/// precision. With the server id of the finding's own row it names the engine alert, since the log has no row id.
/// </summary>
public record AlertAttachedToDto(string Metric, DateTime AlertTimeUtc);

/// <summary>JSON mirror of <see cref="AlertExternalOrigin"/> (#5654). <c>PendingId</c> is the LAST member on purpose:
/// <see cref="AlertContextSerializer.BuildPendingIdLikePattern"/> anchors on <c>"PendingId":&lt;n&gt;}</c>.</summary>
public record AlertExternalDto(string Source, string Key, long PendingId);

/// <summary>
/// JSON mirror of <see cref="AlertNewestSample"/> (#5642): the age rides as whole-and-fractional seconds,
/// computed ONCE when the alert fired, so the history row, the web pages, the MCP read and the Viewer all say
/// what the notification said and never re-measure against a later clock.
/// </summary>
public record AlertNewestSampleDto(string Source, DateTime InstantUtc, double AgeSeconds);

/// <summary>
/// The persisted routing DECISION for an analysis finding (#3712): trailing and nullable on
/// <see cref="AlertContextDto"/> like <c>Route</c>, so a row written before it existed rehydrates to null,
/// which reads as "this row carries no routing decision" (every engine alert, every pre-#3712 row).
/// <c>Route</c> is the persisted spelling <see cref="FindingRouting.RouteText"/> produces — <c>page</c> or
/// <c>digest</c> — a NAME rather than an ordinal for the reason <c>Severity</c> is; <c>Reason</c> is the one
/// sentence the gate wrote naming the corroboration components it read.
/// </summary>
public record AlertRoutingDto(string Route, string Reason);

/// <summary>
/// The persisted routing provenance of one alert-history row (#3598): trailing and nullable on
/// <see cref="AlertContextDto"/> like <c>Incidents</c> and <c>Severity</c>, so a row written before it
/// existed rehydrates to null, which reads as "this row carries no routing record". <c>Family</c> is the
/// alert's <see cref="AlertFamily"/>; <c>RouteId</c> the most specific route that matched (null = the parent
/// defaults answered everything); <c>Destinations</c> one entry per channel that RESOLVED to a destination,
/// each naming the route that supplied it (null = the parent default), the level it came from, spelled as
/// the <see cref="RouteSource"/> member's NAME for the same reason <c>Severity</c> is: the column outlives
/// any build and an ordinal would change meaning the day a member is inserted, and (#4750) what the send to
/// it did.
/// </summary>
public record AlertRouteDto(string Family, int? RouteId, List<AlertRouteDestinationDto> Destinations);

/// <summary>
/// One channel of an <see cref="AlertRouteDto"/> (#3598). <c>Outcome</c> (#4750) is what the send to that
/// channel did, spelled as one of <see cref="AlertRouteOutcomes"/>'s constants, so an alert that reached one
/// channel and failed on another says so on the row instead of reading as one delivery. It is trailing and
/// nullable like <c>AlertContextDto.Route</c>: a row written before it existed rehydrates to null, which reads
/// as "this row does not say", and so does a row whose fan-out reported no per-channel outcomes. It carries NO
/// error text — a webhook failure message can name the endpoint URL, which is a secret — so the row's
/// <c>send_error</c> stays the only place a failure's reason is written.
///
/// <para><c>SentKey</c> (#6718) is the <c>{{dedup_key}}</c> the generic webhook's post rendered, set on the generic
/// destination, and the <c>dedup_key</c> the PagerDuty trigger carried, set on the PagerDuty destination. Both are derived
/// from the #3313 delivery-filtered context, which the row does not keep, so each is stored as sent and a resolution after a
/// restart closes the key the receiver opened. Trailing and nullable like <c>Outcome</c>: a row written before it existed,
/// and every other channel, read null. Not named <c>DedupKey</c>, so the
/// cooldown seed's <c>"DedupKey":"..."</c> match never reads it as an incident.</para>
/// </summary>
public record AlertRouteDestinationDto(string Channel, int? RouteId, string Source, string? Outcome = null, string? SentKey = null);

/// <summary>
/// The spellings of <see cref="AlertRouteDestinationDto.Outcome"/> (#4750). A persisted contract: the
/// alert-history row's <c>context_json</c> outlives any build, and the MCP history reads hand the text to
/// callers as-is, so a value is never renamed and a new one is only ever added.
/// </summary>
public static class AlertRouteOutcomes
{
    /// <summary>The channel's send went out.</summary>
    public const string Delivered = "delivered";

    /// <summary>The channel's send was attempted and did not succeed. The reason is not stored here.</summary>
    public const string Failed = "failed";

    /// <summary>The channel resolved to a destination but nothing was sent to it on this firing: a cooldown
    /// or a fold held the send, the alert was muted, or the fan-out never reached the channel.</summary>
    public const string NotAttempted = "not attempted";
}

public record AlertDetailItemDto(string Heading, List<FieldDto> Fields, string? Body, bool IsCodeBlock, RemediationActionDto? Remediation = null);
public record FieldDto(string Label, string Value);

/// <summary>
/// JSON mirror of <see cref="AlertIncident"/> (#1140). The trailing optional <c>Incidents</c>
/// member on <see cref="AlertContextDto"/> keeps the round-trip backward-compatible: legacy
/// contextJson written before this field existed deserializes <c>Incidents</c> to null.
/// <para>
/// #2216's two members are trailing and nullable for the same reason: a history row written before
/// they existed rehydrates them as null, which is exactly "this alert carried no total" rather than
/// a fabricated zero. <see cref="AlertIncident.DetailFields"/> and <see cref="AlertIncident.Attachment"/>
/// remain unpersisted — the latter for the same reason
/// <see cref="AlertContext.AttachmentXml"/> is not (the dialog has no attachment surface), and because a
/// deadlock graph per incident would put multiple megabytes of XML in every history row.
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
/// <see cref="BestPlanLastSeenUtc"/> (#3953) and <see cref="BestPlanAgeDays"/> (#4736) are appended the same
/// way and mirrored for the same reason: the MCP findings read reports each target's best plan age from the
/// DESERIALIZED action, so a member missing here reads back null for every persisted target. A row written
/// before they existed has neither property and reads back null, the same as a drill-down row that had no
/// best_plan_last_seen.
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
    bool ParameterSensitivityCoFired = false,
    DateTime? BestPlanLastSeenUtc = null,
    double? BestPlanAgeDays = null);

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
    /// #7068: true when ONE detail item of the persisted <paramref name="contextJson"/> carries every one of
    /// <paramref name="fields"/> as a field whose label and value EQUAL the given ones (ordinal). Equality, not a substring
    /// match: a context that names database <c>Sales2</c> does not name <c>Sales</c>, and a card that lists database A's file
    /// <c>data</c> beside database B's file <c>log</c> does not name database A's <c>log</c>. False on null, blank or unparseable
    /// JSON and on a context with no details.
    /// </summary>
    public static bool ContextJsonHasDetailFields(string? contextJson, params (string Label, string Value)[] fields)
    {
        if (string.IsNullOrWhiteSpace(contextJson) || fields.Length == 0)
            return false;

        try
        {
            return DetailsHaveFields(JsonSerializer.Deserialize<AlertContextDto>(contextJson), fields);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool DetailsHaveFields(AlertContextDto? dto, params (string Label, string Value)[] fields) =>
        dto?.Details is { } details
        && details.Any(item => item.Fields is { } present
            && fields.All(want => present.Any(f =>
                string.Equals(f.Label, want.Label, StringComparison.Ordinal)
                && string.Equals(f.Value, want.Value, StringComparison.Ordinal))));

    /// <summary>
    /// #7068: true when the persisted Database File Growth <paramref name="contextJson"/> names the file
    /// <paramref name="fileName"/> of database <paramref name="databaseName"/> (both equal, ordinal). A row that stores its
    /// full file list (<see cref="AlertContext.FileGrowthFiles"/>) is answered from that list, because its card shows only
    /// five files. A row written before the list existed is answered from its detail items, the way
    /// <see cref="ContextJsonHasDetailFields"/> reads them. False on null, blank or unparseable JSON.
    /// </summary>
    public static bool ContextJsonNamesFile(string? contextJson, string databaseName, string fileName)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return false;

        try
        {
            var dto = JsonSerializer.Deserialize<AlertContextDto>(contextJson);
            if (dto?.FileGrowthFiles is { } files)
            {
                return files.Any(f =>
                    string.Equals(f.Database, databaseName, StringComparison.Ordinal)
                    && string.Equals(f.File, fileName, StringComparison.Ordinal));
            }

            return DetailsHaveFields(dto, ("Database", databaseName), ("File", fileName));
        }
        catch (JsonException)
        {
            return false;
        }
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

    /// <summary>
    /// The <c>LIKE</c> pattern (with <c>%</c> wildcards, no escape character needed) that finds the history rows an
    /// external alert's queue row wrote (#5654): the serialized <c>"PendingId":&lt;n&gt;}</c> that closes the nested
    /// <c>External</c> object. The closing brace stops <c>12</c> from matching <c>123</c>.
    /// </summary>
    public static string BuildPendingIdLikePattern(long pendingId) =>
        "%\"PendingId\":" + pendingId.ToString(System.Globalization.CultureInfo.InvariantCulture) + "}%";

    /// <summary>The external origin a persisted history row carries (#5654), or null when it carries none.</summary>
    public static AlertExternalOrigin? TryReadExternal(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.External), out var external)
                || external.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var dto = external.Deserialize<AlertExternalDto>();
            return dto is not null && !string.IsNullOrWhiteSpace(dto.Source)
                ? new AlertExternalOrigin(dto.Source, dto.Key, dto.PendingId)
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The external source name a persisted history row carries (#5654), or null. Reads the one property, like
    /// <see cref="TryReadNewestSample"/>.</summary>
    public static string? TryReadExternalSource(string? contextJson) => TryReadExternal(contextJson)?.Source;

    public static string Serialize(AlertContext context)
    {
        var dto = new AlertContextDto(
            context.Details.ConvertAll(d => new AlertDetailItemDto(
                d.Heading,
                d.Fields.ConvertAll(f => new FieldDto(f.Label, f.Value)),
                d.Body,
                d.IsCodeBlock,
                ToDto(d.Remediation))),
            ToDto(context.Incidents),
            /* #3539 A8e: the tier the alert fired at rides the row. Both SKUs' deliverers fold
               AlertOutcome.Severity into this property before serializing (#2090), so a graded fire on
               either engine persists its grade here with no store change on either side. */
            context.SeverityOverride,
            /* #3598: where the posts went. Already the persisted shape, so it rides through as-is. */
            context.Route,
            /* #3712: whether the fan-out was allowed to run, and why. Already the persisted shape. */
            context.Routing,
            /* #4223: the wait type this firing is about, as structured data. Already the persisted shape. */
            context.WaitType,
            /* #4223: the collector this firing is about, as structured data. Already the persisted shape. */
            context.CollectorName,
            /* #5642: the newest sample's source, instant and fire-time age. */
            context.NewestSample is { } sample
                ? new AlertNewestSampleDto(sample.Source, DateTime.SpecifyKind(sample.InstantUtc, DateTimeKind.Utc), sample.Age.TotalSeconds)
                : null,
            /* #5654: the external source, key and queue row. */
            context.External is { } external ? new AlertExternalDto(external.Source, external.Key, external.PendingId) : null,
            /* #7068: every file a file growth firing is about, beyond the five its card lists. */
            context.FileGrowthFiles,
            /* #7076: the policy job step this firing announced. */
            context.PolicyJobStep,
            /* #7084 part 4: the open engine alert this finding was attached to. Already the persisted shape. */
            context.AttachedTo);
        return JsonSerializer.Serialize(dto);
    }

    /// <summary>
    /// The wait type a persisted alert-history row carries (#4223 Poison Wait notebook), or <c>null</c> when
    /// it carries none — any non-Poison-Wait alert, a row written before the member existed, or unparseable
    /// JSON. Reads the one property rather than rehydrating the whole context, for the reason
    /// <see cref="TryReadRoute"/> gives.
    /// </summary>
    public static string? TryReadWaitType(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.WaitType), out var waitType)
                || waitType.ValueKind != JsonValueKind.String)
            {
                return null;
            }

            return waitType.GetString();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The <see cref="AlertContext.PolicyJobStep"/> of a job that was automatically re-armed and announced (#7076).</summary>
    public const string PolicyJobStepReArmed = "ReArmed";

    /// <summary>The <see cref="AlertContext.PolicyJobStep"/> of a job that was escalated, or whose repeat page was sent (#7076).</summary>
    public const string PolicyJobStepEscalated = "Escalated";

    /// <summary>
    /// The policy job step a persisted alert-history row carries (#7076), or <c>null</c> when it carries none: any other alert, a row
    /// written before the member existed, or unparseable JSON. Reads the one property rather than rehydrating the whole context, for
    /// the reason <see cref="TryReadRoute"/> gives.
    /// </summary>
    public static string? TryReadPolicyJobStep(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.PolicyJobStep), out var step)
                || step.ValueKind != JsonValueKind.String)
            {
                return null;
            }

            return step.GetString();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// The collector name a persisted alert-history row carries (#4223 self-monitor notebook), or
    /// <c>null</c> when it carries none — any non-Collector-Cost-Regression alert, a row written before the
    /// member existed, or unparseable JSON. Reads the one property rather than rehydrating the whole
    /// context, for the reason <see cref="TryReadRoute"/> gives.
    /// </summary>
    public static string? TryReadCollectorName(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.CollectorName), out var collectorName)
                || collectorName.ValueKind != JsonValueKind.String)
            {
                return null;
            }

            return collectorName.GetString();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// The engine alert a persisted analysis-finding row was attached to (#7084 part 4), or <c>null</c> when it carries
    /// none: every other row, a row written before the member existed, or unparseable JSON. Reads the one property, for
    /// the reason <see cref="TryReadRoute"/> gives.
    /// </summary>
    public static AlertAttachedToDto? TryReadAttachedTo(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.AttachedTo), out var attached)
                || attached.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var dto = attached.Deserialize<AlertAttachedToDto>();
            return dto is { Metric: not null } ? dto : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// The routing decision a persisted alert-history row carries (#3712), or <c>null</c> when it carries none
    /// — an engine alert, a row written before the member existed, or unparseable JSON. Reads the one property
    /// rather than rehydrating the whole context, for the reason <see cref="TryReadRoute"/> gives: the MCP
    /// history read calls this once per row, and the digest reader once per digest-routed row.
    /// </summary>
    public static AlertRoutingDto? TryReadRouting(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.Routing), out var routing)
                || routing.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var dto = routing.Deserialize<AlertRoutingDto>();
            /* A record with a null Route is a hand-edited or foreign shape; "no decision" is the honest read. */
            return dto is { Route: not null } ? dto : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// The routing record a persisted alert-history row carries (#3598), or <c>null</c> when it carries none
    /// — written before the member existed, a row that never reached resolution, a resolution row, or
    /// unparseable JSON. Reads the one property rather than rehydrating the whole context, for the reason
    /// <see cref="TryReadSeverity"/> gives: the MCP history read calls this once per row.
    /// </summary>
    public static AlertRouteDto? TryReadRoute(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.Route), out var route)
                || route.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            return route.Deserialize<AlertRouteDto>();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// The newest-sample carrier a persisted alert-history row holds (#5642): the sample's source label, UTC
    /// instant and the age computed ONCE when the alert fired. <c>null</c> when the row carries none (an alert
    /// with no UTC sample instant, a row written before the member existed, a resolution row, or unparseable
    /// JSON). Reads the one property rather than rehydrating the whole context, for the reason
    /// <see cref="TryReadRoute"/> gives; every surface that shows the age reads it here and never measures it
    /// again against a later clock.
    /// </summary>
    public static AlertNewestSample? TryReadNewestSample(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.NewestSample), out var sample)
                || sample.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var dto = sample.Deserialize<AlertNewestSampleDto>();
            return dto is not null && !string.IsNullOrWhiteSpace(dto.Source)
                ? new AlertNewestSample(dto.Source, DateTime.SpecifyKind(dto.InstantUtc, DateTimeKind.Utc),
                    TimeSpan.FromSeconds(Math.Max(0, dto.AgeSeconds)))
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// The tier a persisted alert-history row FIRED at (#3539 A8e), or <c>null</c> when the row carries none
    /// — written before the member existed, an alert that fired with no override (so the per-metric map
    /// decided), a resolution row (persisted with a null context), or unparseable JSON. Reads the one
    /// property rather than rehydrating the whole context: the grids call this once per row, and a Details
    /// list with remediation actions is the expensive part of a row it does not need.
    /// </summary>
    public static AlertSeverityLevel? TryReadSeverity(string? contextJson)
    {
        if (string.IsNullOrWhiteSpace(contextJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(contextJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(nameof(AlertContextDto.Severity), out var severity)
                || severity.ValueKind != JsonValueKind.String)
            {
                return null;
            }

            /* Name match only, exact: the writer emits the enum's name, and Enum.TryParse on its own would
               also accept a bare digit ("1"), which is the ordinal coupling the string form exists to avoid
               — so the parsed member must spell itself back to the stored text. */
            var text = severity.GetString();
            return Enum.TryParse<AlertSeverityLevel>(text, ignoreCase: false, out var level)
                && string.Equals(Enum.GetName(level), text, StringComparison.Ordinal)
                ? level
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
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

            /* #3539 A8e: the tier the alert fired at. A row written before the member existed rehydrates it
               null, which is the same state a fire with no override left it in. */
            context.SeverityOverride = dto.Severity;

            /* #3598: the routing record, null on every row written before it existed. */
            context.Route = dto.Route;

            /* #3712: the corroboration gate's decision, null on engine alerts and pre-#3712 rows. */
            context.Routing = dto.Routing;

            /* #4223: the wait type this firing is about, null on every non-Poison-Wait alert and every row
               written before this member existed. */
            context.WaitType = dto.WaitType;

            /* #4223: the collector this firing is about, null on every non-Collector-Cost-Regression alert
               and every row written before this member existed. */
            context.CollectorName = dto.CollectorName;

            /* #5642: the fire-time sample age, null on every alert without a UTC sample instant and every row
               written before this member existed. */
            context.NewestSample = dto.NewestSample is { } newest && !string.IsNullOrWhiteSpace(newest.Source)
                ? new AlertNewestSample(newest.Source, DateTime.SpecifyKind(newest.InstantUtc, DateTimeKind.Utc),
                    TimeSpan.FromSeconds(Math.Max(0, newest.AgeSeconds)))
                : null;

            /* #7068: the files a file growth firing is about, null on every other alert and every row written before this
               member existed. */
            context.FileGrowthFiles = dto.FileGrowthFiles;

            /* #7076: the policy job step this firing announced, null on every other alert and every row written before this
               member existed. */
            context.PolicyJobStep = dto.PolicyJobStep;

            /* #5654: the external source that raised this firing, null on every alert not raised through the store's
               submission function and every row written before this member existed. */
            context.External = dto.External is { } external && !string.IsNullOrWhiteSpace(external.Source)
                ? new AlertExternalOrigin(external.Source, external.Key, external.PendingId)
                : null;

            /* #7084 part 4: the engine alert this finding was attached to, null on every other row and every row
               written before this member existed. */
            context.AttachedTo = dto.AttachedTo is { } attached && !string.IsNullOrWhiteSpace(attached.Metric) ? attached : null;

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
                t.ParameterSensitivityCoFired,
                t.BestPlanLastSeenUtc,
                t.BestPlanAgeDays));
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
                    t.ParameterSensitivityCoFired,
                    t.BestPlanLastSeenUtc,
                    t.BestPlanAgeDays));
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
