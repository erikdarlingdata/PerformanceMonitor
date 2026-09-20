/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The <c>CONFIG_CHANGED</c> analysis finding (#3653 A10, ruling Q2): when a server configuration setting
/// changed inside the pass window, compare the four hours before the change with the four hours after it
/// and SAY what moved — in the server's own dispersion units where a baseline exists, on the scorer's
/// ladder otherwise — or say that nothing did. Shared by both SKUs so Lite and Darling attribute the same
/// change the same way; each service reads its own store's <c>server_config</c> snapshots, diffs them
/// through the shared <c>ConfigChangeDiff</c>, calls its own <c>ComparePeriodsAsync</c>, and hands the
/// results here. This assembly stays a leaf (no project references — <c>PerformanceMonitor.Common</c> would
/// bring the MCP SDK and the credential package with it), so the diff's records are mapped onto
/// <see cref="SettingChange"/> at the call site rather than consumed here.
///
/// <para><b>The gap this closes.</b> Every prior reference to configuration changes in the product was a
/// history READ (<c>get_server_config_changes</c>, the Configuration Changes tabs): the analysis engine
/// scored the CURRENT value of seven settings and never once asked whether a change had a consequence.
/// <c>ForcePlanSelfReview</c> already proves the verify-after shape in-repo for forced plans; this is the
/// same shape for <c>sp_configure</c>. The engine review called it "the single biggest credibility upgrade
/// available", because an operator who changed MAXDOP at 14:02 and sees CPU fall at 14:10 today has to
/// join two tools by hand to say so.</para>
///
/// <para><b>There is no change table.</b> The issue text speaks of <c>server_config_changes</c> rows; no such
/// table exists on either SKU. <c>server_config</c> is an APPEND-ONLY snapshot table that the config
/// collector fills ON CONNECT (<c>CollectorScheduleDefaults["server_config"]</c> is frequency 0, both
/// SKUs), and a "change" is what <c>ConfigChangeDiff.DiffServerConfigChanges</c> derives from two
/// consecutive snapshots. Two consequences are load-bearing and both are disclosed on the fact:</para>
/// <list type="bullet">
///   <item><description>The change TIME the snapshot can give is the capture that first OBSERVED the new
///   value, not the moment <c>RECONFIGURE</c> ran. The real change landed somewhere between the previous
///   capture and this one — <see cref="MetaObservationGapHours"/> is that span, and when it exceeds the
///   before-window the "before" half may already include the new value. On that anchor the finding says
///   "first observed at", never "changed at". <b>#3740 adds the clock the snapshot lacks</b>: the default
///   trace records the error-log line <c>sp_configure</c> writes at the instant of the change (msg 15457,
///   "Configuration option '%ls' changed from %ld to %ld. Run the RECONFIGURE statement to install.", an
///   <c>ErrorLog</c> event with the real <c>StartTime</c>), and <see cref="ResolveTraceAnchor"/> joins the
///   diffed change to that row. When one exists for the same option in <c>(previous capture, this
///   capture]</c>, the ±4 h compare anchors on the trace's time, the span collapses to nothing, and the
///   prose says "changed at … (default trace)"; <see cref="MetaAnchorClock"/> says which clock the fact
///   used. When none exists — trace off, Azure SQL Database (no default trace), the row aged out of the
///   trace's rollover files before the collector saw it, or a non-English instance whose message text the
///   parser does not read — the observation anchor and every one of its disclosures stand unchanged. The
///   snapshot diff stays the source of WHAT changed (the trace line carries the values too, but the diff
///   is what the history tools already trust); the trace is only ever the source of WHEN.</description></item>
///   <item><description>Every setting that changed between the same two captures shares ONE change time,
///   so they share ONE compare — the data cannot attribute an outcome to one of two settings that were
///   observed together, and this class does not pretend to. One fact per change EVENT (capture), naming
///   every setting that moved at it.</description></item>
/// </list>
///
/// <para><b>Why one fact, keyed exactly <see cref="FactKey"/>.</b> The engine is one-fact-per-key end to
/// end: <see cref="FactCollectionExtensions.ToFactLookup"/> keeps the first fact per key,
/// <c>InferenceEngine.BuildStories</c> marks a key consumed after its first story, and
/// <c>FactAdvice.PopulateStoryText</c> composes from the same lookup. Two same-key facts would silently
/// lose the second. So when several change events fall inside one pass window — two reconnects with
/// changes between them inside four hours, which the connect cadence makes rare — the MOST RECENT event is
/// the fact's subject and the earlier ones are counted in <see cref="MetaEarlierEventsInWindow"/>; their
/// own ±4 h compares belonged to the passes that ran while they were the most recent. (A per-setting key
/// suffix, <c>BAD_ACTOR_&lt;hash&gt;</c>-style, was considered and rejected for slice one: the ruling names
/// the key, and the shared compare makes per-setting facts carry identical numbers.)</para>
///
/// <para><b>Why it never meets the scorer.</b> <c>FactScorer.ScoreConfigFact</c> returns 0 for a
/// <c>"config"</c> key it does not know, and <c>ScoreAll</c> zeroes the severity of every base-0 fact — a
/// fact appended BEFORE scoring would be dropped from the working set. The services therefore append this
/// fact AFTER <c>_scorer.ScoreAll(facts)</c> with <see cref="Fact.BaseSeverity"/> and
/// <see cref="Fact.Severity"/> already set to <see cref="InformationSeverity"/>, and
/// <c>InferenceEngine.ConfigAdvisoryRootKeys</c> lists the key so a sub-0.5 fact roots its own story. The
/// scorer file is untouched by design (#3691's lane owns it).</para>
///
/// <para><b>Rooted at Information, which attributes and does not accuse.</b> Both SKUs' readers band a
/// finding's severity identically (<c>LiteRecommendationsReader.SeverityBand</c>,
/// <c>ViewerDataService.SeverityBand</c>): <c>&gt;= 1.5</c> CRITICAL, <c>&gt;= 0.75</c> WARNING, else INFO.
/// <see cref="InformationSeverity"/> sits inside INFO and BELOW every standing-misconfiguration advisory
/// (the 0.4 <c>CONFIG_*</c> base, the 0.3 <c>FILE_AUTOGROWTH_PERCENT</c> base), so a change that had no
/// effect sorts under a setting that is wrong today; above 0 so it roots, persists, recurs and mutes like
/// any other finding. <c>AnalysisNotificationService</c> filters <c>Severity &gt;= AnalysisNotifySeverity</c>
/// before grouping, so this finding never e-mails — intended.</para>
///
/// <para><b>What it does not do.</b> It is not a causal test: one window against one window cannot show
/// that a change caused anything (the compare tool's own description says so, citing the OtterTune field
/// study's 4× DB-time variance on an unchanged configuration), and the remediation prose repeats it. It does
/// not fold into an incident — a configuration change is context for whatever else the pass found, not a
/// symptom of it — and <c>AnomalyIncidentReconciler</c> is not touched. It does not reach
/// <c>get_analysis_facts</c>, which runs the fenced <c>CollectAndScoreFactsAsync</c> and not the pass.</para>
///
/// <para><b>Slice two: the other two configuration families, same fact (#3653 A10).</b> The store keeps three
/// on-connect snapshot families and the history tools diff all three — <c>server_config</c> (slice one),
/// <c>database_config</c> (<c>sys.databases</c>, one WIDE row per database, 27 option columns) and
/// <c>trace_flags</c> (<c>DBCC TRACESTATUS(-1)</c>, a row per ENABLED flag). Slice two attributes the last two
/// through the same fact, the same compare and the same prose grammar, keyed exactly <see cref="FactKey"/>
/// so the one-fact-per-key rule above holds across families: a pass window holding a server change and a
/// database change does not fire twice. Each <see cref="SettingChange"/> carries its <see cref="ChangeFamily"/>,
/// the fact carries the event's families as <see cref="MetaChangeFamily"/> (a bit per family, since the
/// metadata map is doubles-only and an event can span families — see the next paragraph), and the
/// composer branches on the family per change: a database option reads "<c>`AdventureWorks` recovery_model
/// FULL → SIMPLE</c>", a flag "<c>trace flag 4199 enabled (GLOBAL)</c>". Text values (a recovery model, a
/// collation, a flag's scope) cannot ride in the doubles-only metadata, so they ride in
/// <see cref="Fact.ObjectName"/> under the segment grammar <see cref="EncodeSegment"/> spells — the server
/// family's segment is the bare option name slice one wrote, byte for byte — and a database option whose text
/// parses as a number or a boolean ALSO lands in the numeric keys so an MCP reader can compare it. A fact
/// whose every change belongs to ONE database carries that database in <see cref="Fact.DatabaseName"/>, the
/// seam the finding stores already persist and the cards already show.</para>
///
/// <para><b>Changes observed at one connect are one event, whatever family they belong to.</b> The three
/// config collectors run serially on every connect (frequency 0, both SKUs), each stamping its own
/// <c>capture_time</c>, so a MAXDOP change and a trace-flag change made in the same maintenance window
/// surface at the same reconnect as two captures seconds apart. Slice one's own argument applies across
/// families exactly as it applied within one: two changes observed together share one observation time and
/// one compare, and the data cannot say which of them moved a metric. Treating them as two events would make
/// the later collector's family the card's subject by accident of run order and count the other as an
/// "earlier event" whose own pass never existed. <see cref="MergeSameConnectEvents"/> therefore folds
/// events of DIFFERENT families whose captures fall within <see cref="SameConnectToleranceMinutes"/> of each
/// other into one event anchored on the LATEST capture (the moment the observed configuration was
/// complete) with the EARLIEST previous capture (the widest honest span). Same-family events are never
/// merged: two captures of one family are two connects, and the span between them is real information.</para>
///
/// <para><b>Which clock, per family — measured, not assumed.</b> The #3740 trace anchor is the server_config
/// slice's alone, and <see cref="ResolveTraceAnchor"/> joins only server-family changes. Measured on SQL
/// Server 2022 (2026-09-20): <c>DBCC TRACEON</c> / <c>TRACEOFF</c> DO write an <c>ErrorLog</c> trace event
/// (msgs 17550 / 17551, severity 10, "DBCC TRACEON 1222, server process ID (SPID) 94. …") stamped with the
/// SESSION's database — 1 from master — so <c>DefaultTraceEventsCollector</c>'s ErrorLog arm, which sits
/// inside the master/model/msdb exclusion with 15457 as its only exemption, DROPS the line in the common
/// case; and the line carries no old/new state, so a re-run on an already-enabled flag (which still writes
/// 17550, measured) would mis-date the change to the re-run with no way to tell. <c>ALTER DATABASE SET
/// &lt;option&gt;</c> writes msg 5084 to the ERRORLOG file but the default trace records NO ErrorLog event for
/// it at all — only <c>Object:Altered</c> begin/commit pairs on the database with a NULL ObjectName and NULL
/// TextData, a row that says a database-level DDL happened and not which option. Neither family has a trace
/// subject the store holds today, so both anchor on the observation, with every observation disclosure in
/// force; the collector-side arm for 17550/17551 is a follow-up, stated in the lane's report, not invented
/// here.</para>
///
/// <para><b>Two blind spots inherited from the snapshots, stated.</b> (1) <c>log_reuse_wait_desc</c> is one
/// of the 27 <c>database_config</c> columns and is not a configuration — it is the engine's live reason the
/// log cannot truncate (NOTHING → LOG_BACKUP → NOTHING) and flips with no operator action; the history tools
/// list it, but a finding that ran a ±4 h compare on it would attribute an outcome to weather, and because
/// the most recent event is the fact's subject, such a flip would DISPLACE a real change from the card.
/// <see cref="IsAttributableDatabaseSetting"/> excludes it; the other 26 are options an <c>ALTER DATABASE</c>
/// (or a restore, an encryption scan, a state change) sets. (2) <c>TraceFlagsCollector</c> writes NO row when
/// <c>DBCC TRACESTATUS(-1)</c> returns none, so a capture with zero enabled flags is invisible to the
/// set-diff: "the last flag was disabled" and "the first flag was enabled" are both undetectable, and a
/// flag's previous-capture span can be wider than the truth. The history tool shares the blind spot; this
/// class inherits it rather than guessing, and the report names the sentinel-row fix as a follow-up.</para>
/// </summary>
public static class ConfigChangeAttribution
{
    /// <summary>The fact key and the finding's root key. Named by the Q2 ruling.</summary>
    public const string FactKey = "CONFIG_CHANGED";

    /// <summary>The existing <c>"config"</c> source — <c>FactScorer.KnownSources</c> is a census of every
    /// <c>Source = "..."</c> literal in the three analysis assemblies, and a new spelling would fail it.</summary>
    public const string FactSource = "config";

    /// <summary>
    /// The Information root. 0.25: inside the readers' INFO band (below 0.75), below the 0.4 config-advisory
    /// and 0.3 autogrowth bases so an attribution never outranks a standing misconfiguration, above 0 so the
    /// story roots and the mute filter (<c>story.Severity &lt;= 0</c> is skipped) keeps it. Not a measured
    /// number — a position in an ordering, stated so the ordering is the thing reviewed.
    /// </summary>
    public const double InformationSeverity = 0.25;

    /// <summary>Hours on each side of the observed change. The ruling's ±4 h, and the pass's own window length.</summary>
    public const int CompareWindowHours = 4;

    /// <summary>
    /// The error-log message <c>sp_configure</c> writes at the instant a server setting changes —
    /// "Configuration option '%ls' changed from %ld to %ld. Run the RECONFIGURE statement to install." —
    /// which the default trace records as an <c>ErrorLog</c> event carrying this number in its <c>Error</c>
    /// column (<c>default_trace_events.error_number</c>). Both services' trace reads key on the NUMBER, not
    /// the text: it is populated on the row (measured on SQL Server 2022, severity 10) and it does not
    /// change with the instance's language, while the text does. The collector keeps this row outside its
    /// database predicates for the same reason (<c>DefaultTraceEventsCollector</c>, #3740).
    /// </summary>
    public const int ReconfigureMessageNumber = 15457;

    /// <summary>
    /// <see cref="MetaAnchorClock"/> value: the fact's change time is the config capture that first observed
    /// the new value, with every observation disclosure in force.
    /// </summary>
    public const double AnchorSourceObservation = 0;

    /// <summary>
    /// <see cref="MetaAnchorClock"/> value: the fact's change time is the default trace's <c>StartTime</c> for
    /// the sp_configure line that made the change — the moment it happened, to the trace's millisecond.
    /// </summary>
    public const double AnchorSourceDefaultTrace = 1;

    /// <summary>
    /// Joins the changed settings' segments (<see cref="EncodeSegment"/>) into <see cref="Fact.ObjectName"/> —
    /// the one string slot a fact has, since <see cref="Fact.Metadata"/> is doubles-only by contract. No
    /// <c>sys.configurations</c> name contains a semicolon, so the composer can split on it; a DATABASE name
    /// could (any bracketed identifier can), and a database named with "; " in it would mis-split the card's
    /// change list — the data on the fact is unaffected, only that card's rendering, and it is stated here
    /// rather than guarded with a separator slice one's pins do not carry.
    /// </summary>
    public const string SettingSeparator = "; ";

    /// <summary>
    /// Captures from DIFFERENT families this close together are one connect, hence one event
    /// (<see cref="MergeSameConnectEvents"/>). The on-load collectors run serially at connect, each under the
    /// 60 s per-command deadline, and a healthy server stamps all three within seconds; five minutes covers a
    /// pathological one with room, and two genuine reconnects five minutes apart with changes between them
    /// is a reconnect storm whose two ±4 h compares are the same compare to within two per cent anyway.
    /// Same-family captures are never merged whatever their spacing.
    /// </summary>
    public const int SameConnectToleranceMinutes = 5;

    /// <summary>
    /// The configuration families the fact can name — a bit each, because <see cref="MetaChangeFamily"/> is
    /// a double and one event can span families (<see cref="MergeSameConnectEvents"/>). The numeric values
    /// are the wire contract: <c>1</c> server (sys.configurations), <c>2</c> database (sys.databases options),
    /// <c>4</c> trace flags (DBCC TRACESTATUS); a two-family event carries their sum.
    /// </summary>
    [Flags]
    public enum ChangeFamily
    {
        /// <summary>A <c>sys.configurations</c> option (slice one).</summary>
        ServerConfig = 1,
        /// <summary>A <c>sys.databases</c> option on one database (slice two).</summary>
        DatabaseConfig = 2,
        /// <summary>A trace flag enabled, disabled or re-scoped (slice two).</summary>
        TraceFlags = 4,
    }

    /// <summary>
    /// The one <c>database_config</c> column the diff tracks that is NOT a configuration: the engine's live
    /// reason the log cannot be truncated, which flips with no operator action. Excluded from attribution
    /// (see the class remarks); the history tools still list it. A set so the day another status column
    /// joins the wide row it is one line here.
    /// </summary>
    public static readonly IReadOnlySet<string> DatabaseStatusColumns = new HashSet<string>(StringComparer.Ordinal)
    {
        "log_reuse_wait_desc",
    };

    /// <summary>True for a <c>database_config</c> setting the attribution treats as a configuration change.</summary>
    public static bool IsAttributableDatabaseSetting(string? settingName) =>
        !string.IsNullOrEmpty(settingName) && !DatabaseStatusColumns.Contains(settingName);

    /// <summary>
    /// Per-key metadata beyond this many moved rows is dropped and counted in <see cref="MetaMovedKeysOmitted"/>.
    /// The banding orders rows worst-first, so what survives is the head of the verdict list.
    /// </summary>
    public const int MaxMovedKeysInMetadata = 12;

    /* ── Metadata keys (doubles). Shared with the composer and the tests so the spelling lives once. ── */

    /// <summary>How many settings changed at this event, across families (also the fact's Value).</summary>
    public const string MetaChangedSettings = "changed_settings";
    /// <summary>Which configuration families the event's changes belong to: the <see cref="ChangeFamily"/> bits
    /// summed (1 server, 2 database, 4 trace flags; 5 = a server setting and a trace flag observed at one connect).
    /// One key, one word, because the readers band and mute by key and a per-family key would split the family
    /// that the one-fact-per-key rule keeps together; the per-change family is recoverable from the ObjectName
    /// segments (<see cref="Changes"/>).</summary>
    public const string MetaChangeFamily = "change_family";
    /// <summary>The change time the compare is ANCHORED on, as Unix seconds — a DateTime cannot ride in a
    /// double map. The trace's <c>StartTime</c> when <see cref="MetaAnchorClock"/> is
    /// <see cref="AnchorSourceDefaultTrace"/>, the observing capture's time otherwise (#3740). Readers that
    /// want the capture regardless of anchor take <see cref="MetaObservedAtUnix"/>.</summary>
    public const string MetaChangeTimeUnix = "change_time_unix";
    /// <summary>Which clock <see cref="MetaChangeTimeUnix"/> is in: <see cref="AnchorSourceObservation"/> (0) or
    /// <see cref="AnchorSourceDefaultTrace"/> (1). A number because the metadata map is doubles-only; the two
    /// named constants are the whole domain, and the composer reads the key through them. The key on the
    /// wire is <c>anchor_source</c>; the constant is not named after it because <c>FactSourceRegistryTests</c>
    /// sweeps every <c>Source = "…"</c> literal in the analysis assemblies as a fact-source stamp, and a
    /// constant ending in <c>Source</c> would be read as one and fail the registry's set equality.</summary>
    public const string MetaAnchorClock = "anchor_source";
    /// <summary>The config capture that first observed the new value, as Unix seconds — always present, and
    /// equal to <see cref="MetaChangeTimeUnix"/> on the observation anchor.</summary>
    public const string MetaObservedAtUnix = "observed_at_unix";
    /// <summary>On the trace anchor only: hours from the trace's change time to the capture that first observed
    /// it — how late the snapshot was, which is a disclosure about this CARD's timing, not about the compare's
    /// windows (those are anchored on the trace).</summary>
    public const string MetaObservedLagHours = "observed_lag_hours";
    /// <summary>Hours from the previous config capture to the one that observed the change: the span the
    /// real change landed in. 0 on the trace anchor (#3740) — the trace stamps the change to the
    /// millisecond, and a span of a few milliseconds is not a disclosure; the composer's span sentences
    /// are gated on this being positive, so they fall silent by construction.</summary>
    public const string MetaObservationGapHours = "observation_gap_hours";
    /// <summary>Nominal hours in the before half (always <see cref="CompareWindowHours"/>).</summary>
    public const string MetaBeforeHours = "before_hours";
    /// <summary>Hours of the after half that exist yet: <c>min(4, observedThrough − changeTime)</c>.</summary>
    public const string MetaAfterHoursObserved = "after_hours_observed";
    /// <summary>1 when the after half was clamped to the pass end — the change is still less than 4 h old.</summary>
    public const string MetaAfterWindowClamped = "after_window_clamped";
    /// <summary>The collector's observed fraction of the before half (<see cref="WindowCoverage.Fraction"/>).</summary>
    public const string MetaBeforeCoverageFraction = "before_coverage_fraction";
    /// <summary>The collector's observed fraction of the after half as it exists so far.</summary>
    public const string MetaAfterCoverageFraction = "after_coverage_fraction";
    /// <summary>Other change events inside the pass window that this fact does NOT compare (see the class remarks).</summary>
    public const string MetaEarlierEventsInWindow = "earlier_change_events_in_window";
    /// <summary>1 when the compare could not run (both collections threw); the fact still records the change.</summary>
    public const string MetaCompareUnavailable = "compare_unavailable";
    /// <summary>Keys the compare banded (present on either side).</summary>
    public const string MetaComparedKeys = "compared_keys";
    public const string MetaWorse = "worse";
    public const string MetaBetter = "better";
    public const string MetaStable = "stable";
    /// <summary>Moved rows beyond <see cref="MaxMovedKeysInMetadata"/> that carry no per-key entry.</summary>
    public const string MetaMovedKeysOmitted = "moved_keys_omitted";

    /// <summary>Per-setting old/new values. <c>|</c> separates the setting from the field; no setting name contains it.</summary>
    public static string OldInUseKey(string setting) => $"{setting}|old_value_in_use";
    public static string NewInUseKey(string setting) => $"{setting}|new_value_in_use";
    public static string OldConfiguredKey(string setting) => $"{setting}|old_value_configured";
    public static string NewConfiguredKey(string setting) => $"{setting}|new_value_configured";
    /// <summary>1 when the setting is non-dynamic and its configured value has not yet reached in-use
    /// (<c>ConfigChangeDiff.ServerConfigChange.RequiresRestart</c>) — the change has not happened to the
    /// engine yet, and nothing SHOULD have moved.</summary>
    public static string RequiresRestartKey(string setting) => $"{setting}|requires_restart";
    /// <summary>The default trace's change time for this setting as Unix seconds — present only for a setting
    /// the trace anchor matched (#3740). On a multi-setting event the fact's anchor is the LATEST of these;
    /// a setting without this key had no matching trace line in the span and rides the event's anchor.</summary>
    public static string TraceChangeTimeUnixKey(string setting) => $"{setting}|trace_change_time_unix";

    /// <summary>Per-moved-key verdict: +1 worse, −1 better. Only non-stable rows get entries.</summary>
    public static string StatusKey(string factKey) => $"{factKey}|status";
    /// <summary>The value delta in the bucket's robust sigma — present only for baseline-banded rows.</summary>
    public static string DeltaSigmaKey(string factKey) => $"{factKey}|delta_sigma";
    /// <summary>|after − before| over the larger side, 0..1 (1 for a one-sided row).</summary>
    public static string RelativeMoveKey(string factKey) => $"{factKey}|relative_move";
    /// <summary>after − before in the key's own unit — present only when both sides had the key.</summary>
    public static string ValueDeltaKey(string factKey) => $"{factKey}|value_delta";

    private const string StatusSuffix = "|status";

    /// <summary>
    /// One setting's move between two consecutive captures, in any of the three families. For the server
    /// family this is <c>ConfigChangeDiff.ServerConfigChange</c> without the change time (which is the
    /// event's) and without the display strings — the six positional fields slice one defined, unchanged;
    /// <see cref="RequiresRestart"/> is the diff's derivation (non-dynamic AND configured ≠ in-use): the
    /// configured value moved but the engine is still running the old one. The trailing optional fields are
    /// slice two's, filled by <see cref="ForDatabase"/> and <see cref="ForTraceFlag"/> and left at their
    /// defaults for a server setting. Mapped at the service, one line of LINQ per family, so this assembly
    /// need not reference the diff's home.
    ///
    /// <para><see cref="Name"/> is the change's identity on the fact: the metadata key stem
    /// (<see cref="OldInUseKey"/> and friends) and what <see cref="SettingNames"/> returns for a server
    /// setting. A database option is named <c>{database}.{setting}</c>, a flag <c>trace flag {n}</c>, so the
    /// per-change keys of a mixed event cannot collide.</para>
    /// </summary>
    public sealed record SettingChange(
        string Name,
        long? OldValueConfigured,
        long? NewValueConfigured,
        long? OldValueInUse,
        long? NewValueInUse,
        bool RequiresRestart,
        ChangeFamily Family = ChangeFamily.ServerConfig,
        string? DatabaseName = null,
        string? Setting = null,
        string? OldText = null,
        string? NewText = null,
        string? Scope = null,
        string? ChangeType = null)
    {
        /// <summary>True when the value the engine actually runs on moved — the case a compare can speak to.</summary>
        public bool InUseMoved => OldValueInUse != NewValueInUse;

        /// <summary>
        /// A <c>sys.databases</c> option on one database, from <c>ConfigChangeDiff.DatabaseConfigChange</c>: the
        /// collected values are TEXT (the wide row's 27 columns cast to text so the diff walks them uniformly),
        /// and they ride here as <see cref="OldText"/> / <see cref="NewText"/> (null = the column was NULL at
        /// that capture). When both parse as a number or a boolean (<c>compatibility_level</c> 150 → 160,
        /// <c>is_read_committed_snapshot_on</c> false → true) they land in the numeric in-use slots too, so the
        /// fact's doubles carry them; a recovery model or a collation stays text-only. Never requires a restart.
        /// </summary>
        public static SettingChange ForDatabase(string databaseName, string setting, string? oldText, string? newText)
        {
            ArgumentNullException.ThrowIfNull(databaseName);
            ArgumentNullException.ThrowIfNull(setting);
            return new SettingChange(
                $"{databaseName}.{setting}",
                OldValueConfigured: null, NewValueConfigured: null,
                OldValueInUse: TryNumeric(oldText), NewValueInUse: TryNumeric(newText),
                RequiresRestart: false,
                Family: ChangeFamily.DatabaseConfig,
                DatabaseName: databaseName,
                Setting: setting,
                OldText: oldText,
                NewText: newText);
        }

        /// <summary>
        /// A trace flag's set-diff outcome, from <c>ConfigChangeDiff.TraceFlagChange</c>: <paramref name="changeType"/>
        /// is the diff's word (<c>enabled</c> / <c>disabled</c> / <c>modified</c>), <paramref name="scope"/> its
        /// GLOBAL / SESSION / UNKNOWN (the flag's CURRENT scope for enabled and modified, its LAST for disabled —
        /// the diff carries one), and the statuses ride in the in-use slots as 0 / 1 (a flag absent from a
        /// capture is off: null → 0) so the fact's doubles say which way it went. <see cref="Setting"/> is the
        /// flag number as text.
        /// </summary>
        public static SettingChange ForTraceFlag(int traceFlag, string changeType, string scope, bool? previousStatus, bool? newStatus) =>
            new(
                $"trace flag {traceFlag}",
                OldValueConfigured: null, NewValueConfigured: null,
                OldValueInUse: previousStatus == true ? 1 : 0, NewValueInUse: newStatus == true ? 1 : 0,
                RequiresRestart: false,
                Family: ChangeFamily.TraceFlags,
                Setting: traceFlag.ToString(CultureInfo.InvariantCulture),
                OldText: previousStatus == true ? "ON" : "OFF",
                NewText: newStatus == true ? "ON" : "OFF",
                Scope: scope,
                ChangeType: changeType);

        /// <summary>A collected text value as a number when it is one: an integer, or a boolean (the casts on both
        /// stores spell <c>true</c> / <c>false</c>) as 1 / 0. Null for anything else, including null.</summary>
        internal static long? TryNumeric(string? text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return null;
            var t = text.Trim();
            if (long.TryParse(t, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n))
                return n;
            if (string.Equals(t, "true", StringComparison.OrdinalIgnoreCase)) return 1;
            if (string.Equals(t, "false", StringComparison.OrdinalIgnoreCase)) return 0;
            return null;
        }
    }

    /// <summary>
    /// One connect at which one or more settings were first seen with a new value — one capture for a
    /// single-family event, the LATEST of the captures for an event <see cref="MergeSameConnectEvents"/>
    /// folded across families. <see cref="PreviousCaptureTime"/> is the capture before it — the other edge
    /// of the span the real change landed in (the EARLIEST previous capture for a folded event: the widest
    /// honest span).
    /// </summary>
    public sealed record ChangeEvent(
        DateTime ChangeTime,
        DateTime PreviousCaptureTime,
        IReadOnlyList<SettingChange> Changes)
    {
        public TimeSpan ObservationGap => ChangeTime - PreviousCaptureTime;

        /// <summary>The families this event's changes belong to, as the summed bits <see cref="MetaChangeFamily"/> carries.</summary>
        public ChangeFamily Families => Changes.Aggregate((ChangeFamily)0, (acc, c) => acc | c.Family);
    }

    /// <summary>
    /// One stored default-trace row as the services hand it here: the event time ALREADY de-skewed to naive
    /// UTC (the stored <c>event_time</c> is the monitored server's local wall clock — <c>fn_trace_gettable</c>'s
    /// <c>StartTime</c> — and each service subtracts the collected <c>server_properties.utc_offset_minutes</c>
    /// exactly as every other reader of that column does; see <c>ServerLocalReadFrameDisciplineTests</c>), and
    /// the raw <c>text_data</c>. Nothing else about the row matters to the join.
    /// </summary>
    public sealed record TraceLine(DateTime EventTimeUtc, string? TextData);

    /// <summary>
    /// One trace line read as a change: WHICH subject it names (a <c>sys.configurations</c> option for the
    /// server_config slice; a database option or a trace flag for slice two), the values it states, and
    /// when. Produced by a parser such as <see cref="ParseReconfigureLine"/>; consumed by
    /// <see cref="ResolveTraceAnchor"/>, which is written against this record and not against the message
    /// so that slice two supplies a parser and reuses the join.
    /// </summary>
    public sealed record TraceChange(string Subject, long? OldValue, long? NewValue, DateTime ChangedAtUtc);

    /// <summary>
    /// The trace's answer to WHEN: <see cref="ChangedAtUtc"/> is the anchor the compare uses — the LATEST
    /// matched line, i.e. the moment the configuration the capture observed was complete — and
    /// <see cref="Matched"/> is one line per setting the join found, keyed by the setting's own name (the
    /// diff's spelling, not the message's), so the fact can say per setting which ones the trace dated.
    /// </summary>
    public sealed record TraceAnchor(DateTime ChangedAtUtc, IReadOnlyDictionary<string, TraceChange> Matched);

    /// <summary>The two halves of the compare, in the order <c>ComparePeriodsAsync</c> takes them.</summary>
    public sealed record CompareWindows(
        DateTime BeforeStart, DateTime BeforeEnd, DateTime AfterStart, DateTime AfterEnd, bool AfterClamped)
    {
        public double AfterHoursObserved => Math.Max(0, (AfterEnd - AfterStart).TotalHours);
    }

    /// <summary>
    /// Groups already-diffed changes (each tagged with the capture time that observed it) into change
    /// events, NEWEST FIRST. The caller runs <c>ConfigChangeDiff.DiffServerConfigChanges(snapshots,
    /// windowStart, windowEnd)</c> — so the window rule is the diff's, the same one every history reader
    /// applies — and its snapshot read must include the last capture BEFORE the window (the diff baseline)
    /// or a change on the first in-window capture is invisible.
    ///
    /// <para><paramref name="captureTimes"/> are every capture the snapshots held (duplicates fine): the
    /// previous capture time is derived from them rather than carried on the change record, because every
    /// configuration row is present in every capture, so the capture immediately before the event's is
    /// the same for every setting in it.</para>
    /// </summary>
    public static List<ChangeEvent> GroupIntoEvents(
        IEnumerable<(DateTime ChangeTime, SettingChange Change)> changes, IEnumerable<DateTime> captureTimes)
    {
        ArgumentNullException.ThrowIfNull(changes);
        ArgumentNullException.ThrowIfNull(captureTimes);

        var changeList = changes.ToList();
        if (changeList.Count == 0)
            return [];

        var captures = captureTimes.Distinct().OrderBy(t => t).ToList();

        return changeList
            .GroupBy(c => c.ChangeTime)
            .Select(g =>
            {
                var previous = captures.LastOrDefault(t => t < g.Key);
                /* A change record exists only because a previous capture existed, so this arm cannot fire on
                   the diff's own output; it guards a caller that hands a change list from elsewhere. A gap of
                   zero then reads as "unknown", and the composer says nothing about the span. */
                if (previous == default)
                    previous = g.Key;
                return new ChangeEvent(
                    g.Key,
                    previous,
                    g.Select(c => c.Change).OrderBy(c => c.Name, StringComparer.Ordinal).ToList());
            })
            .OrderByDescending(e => e.ChangeTime)
            .ToList();
    }

    /// <summary>
    /// Folds events of DIFFERENT families observed at one connect into one event, NEWEST FIRST (slice two;
    /// the class remarks say why). The input is each family's <see cref="GroupIntoEvents"/> output
    /// concatenated in any order. Walking newest first, an event joins the open group when its capture is
    /// within <see cref="SameConnectToleranceMinutes"/> of the group's LATEST capture and the group holds
    /// none of its families yet; otherwise it opens a new group. The folded event's <see cref="ChangeEvent.ChangeTime"/>
    /// is the latest member's (the moment the observed configuration was complete — the rule the trace anchor
    /// applies to a multi-setting event), its <see cref="ChangeEvent.PreviousCaptureTime"/> the earliest
    /// member's (a span that holds every member's real change is true for each of them; the narrowest would
    /// be false for the one whose family last captured earlier), and its changes are ordered family, then
    /// name, so a mixed event reads server setting, database option, trace flag. A single-family input
    /// returns unchanged, so slice one's callers see exactly what they saw.
    /// </summary>
    public static List<ChangeEvent> MergeSameConnectEvents(IEnumerable<ChangeEvent> events)
    {
        ArgumentNullException.ThrowIfNull(events);

        var ordered = events.OrderByDescending(e => e.ChangeTime).ToList();
        var merged = new List<ChangeEvent>(ordered.Count);
        var tolerance = TimeSpan.FromMinutes(SameConnectToleranceMinutes);

        foreach (var evt in ordered)
        {
            if (merged.Count > 0)
            {
                var open = merged[^1];
                var sameConnect = open.ChangeTime - evt.ChangeTime <= tolerance;
                var newFamily = (open.Families & evt.Families) == 0;
                if (sameConnect && newFamily)
                {
                    merged[^1] = new ChangeEvent(
                        open.ChangeTime,
                        open.PreviousCaptureTime < evt.PreviousCaptureTime ? open.PreviousCaptureTime : evt.PreviousCaptureTime,
                        open.Changes.Concat(evt.Changes)
                            .OrderBy(c => c.Family)
                            .ThenBy(c => c.Name, StringComparer.Ordinal)
                            .ToList());
                    continue;
                }
            }

            merged.Add(evt);
        }

        return merged;
    }

    /// <summary>
    /// The ±<see cref="CompareWindowHours"/> h halves around <paramref name="changeTime"/>, the after half
    /// clamped to <paramref name="observedThrough"/> — the pass's window end, which is "now" for a scheduled
    /// pass and the anchor for an anchored one (#2506), so an anchored pass never compares against rows it
    /// is not supposed to see. <see cref="CompareWindows.AfterClamped"/> is true when the clamp bit: the
    /// after half is an honest partial and the fact says so, rather than a fabricated full four hours.
    ///
    /// <para>Both halves share the boundary instant. The collectors read <c>collection_time &gt;= start AND
    /// &lt;= end</c>, so a row stamped at exactly the change time would land in both; the change time is a
    /// config capture's own stamp and the wait rows carry their own, so a same-millisecond coincidence is
    /// not a case worth an off-by-one-tick on the public API.</para>
    /// </summary>
    public static CompareWindows WindowsFor(DateTime changeTime, DateTime observedThrough)
    {
        var nominalAfterEnd = changeTime.AddHours(CompareWindowHours);
        var clamped = observedThrough < nominalAfterEnd;
        var afterEnd = clamped ? observedThrough : nominalAfterEnd;
        if (afterEnd < changeTime)
            afterEnd = changeTime;
        return new CompareWindows(
            changeTime.AddHours(-CompareWindowHours), changeTime,
            changeTime, afterEnd,
            clamped);
    }

    /* ── the trace anchor (#3740) ── */

    /// <summary>
    /// Msg 15457 as the default trace stores it. The <c>TextData</c> of an <c>ErrorLog</c> trace event is the
    /// RAW error-log line — <c>2026-09-20 01:32:16.91 spid95      Configuration option 'max degree of
    /// parallelism' changed from 0 to 8. Run the RECONFIGURE statement to install.</c> (measured) — so the
    /// pattern is anchored on the message's fixed words wherever they fall, never on the start of the
    /// text. The option name is the <c>%ls</c> the engine formats in: the <c>sys.configurations</c> name,
    /// which is what <c>server_config.configuration_name</c> holds, so the join needs no alias table. No
    /// option name contains an apostrophe. The values are <c>%ld</c>: integers, matched with a sign so a
    /// future negative could not silently fail the whole line. English text only — the number in
    /// <c>error_number</c> is what selects the row, and a non-English instance's line simply does not
    /// parse, which leaves that setting on the observation anchor rather than mis-dating it.
    /// </summary>
    private static readonly Regex ReconfigureLine = new(
        @"Configuration option '(?<name>[^']+)' changed from (?<old>-?\d+) to (?<new>-?\d+)\.",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Reads one stored trace line as a server-configuration change, or null when the text is not msg 15457's
    /// shape (any other ErrorLog write the caller's read let through, or a localized message). The subject is
    /// the option name exactly as the message spells it; <see cref="ResolveTraceAnchor"/> compares it to the
    /// diff's name case-insensitively because <c>sys.configurations</c> names are lower-case and the message
    /// repeats them verbatim, but a match that depended on it would be brittle for no gain.
    /// </summary>
    public static TraceChange? ParseReconfigureLine(TraceLine line)
    {
        ArgumentNullException.ThrowIfNull(line);
        if (string.IsNullOrEmpty(line.TextData))
            return null;

        var m = ReconfigureLine.Match(line.TextData);
        if (!m.Success)
            return null;

        return new TraceChange(
            m.Groups["name"].Value.Trim(),
            long.TryParse(m.Groups["old"].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var oldValue) ? oldValue : null,
            long.TryParse(m.Groups["new"].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var newValue) ? newValue : null,
            line.EventTimeUtc);
    }

    /// <summary>
    /// The join, written once for every config slice: for each setting the diff says changed at
    /// <paramref name="change"/>, find the trace line that MADE that change, and anchor the event on the
    /// latest of them. A line qualifies when all of the following hold, each for a reason:
    /// <list type="bullet">
    ///   <item><description>Its time is in <c>(previous capture, this capture]</c>. The previous capture saw the
    ///   old value and this one saw the new, so the change that produced what this capture observed happened
    ///   in that span and nowhere else. The upper edge is inclusive because a line stamped at the capture's own
    ///   instant was seen by it; the lower edge is exclusive because a line stamped at the previous capture's
    ///   instant was seen by THAT capture. No padding on either side: the stored event time is de-skewed by a
    ///   whole-minute offset, and padding the upper edge would let a change made just AFTER the capture — one
    ///   this capture did not observe — anchor a compare of the wrong change.</description></item>
    ///   <item><description>Its subject is the setting's name (ordinal, case-insensitive, trimmed).</description></item>
    ///   <item><description>Its values are a real move that lands on what the capture observed:
    ///   <c>old ≠ new</c> and <c>new</c> equals the diff's new configured value (in-use when the diff has no
    ///   configured value). Both halves are measured necessities. <c>sp_configure</c> re-run with the current
    ///   value still writes the line ("changed from 50 to 50", measured), and that no-op is not the change;
    ///   and a span can hold several real moves (0 → 8 at 10:00, 8 → 4 at 11:00), of which the one that
    ///   produced the observed value is the last whose <c>new</c> is that value. A line whose values do not
    ///   parse cannot be shown to be the change and is not used.</description></item>
    /// </list>
    /// Among qualifying lines the LATEST wins (0 → 8, 8 → 4, 4 → 8 observed as 0 → 8 anchors on the last
    /// 4 → 8: that is when the value the capture saw was installed). The event's anchor is the latest matched
    /// setting's time — the moment the observed configuration was complete — and a setting no line matched
    /// rides that anchor, disclosed per setting through <see cref="TraceChangeTimeUnixKey"/>. Returns null
    /// when no setting matched, and the caller keeps the observation anchor.
    ///
    /// <para><paramref name="parse"/> is the slice's reader of a line — <see cref="ParseReconfigureLine"/> for
    /// server_config, the only family with a trace subject the store holds (the class remarks carry the
    /// measurement for the other two), so the join considers <see cref="ChangeFamily.ServerConfig"/> changes
    /// only: a folded event's database option or trace flag rides the event's anchor and is named as undated.
    /// Nothing else here knows what a subject is. The lines the caller hands in should already be the slice's
    /// candidates (the services select <c>error_number = 15457</c> in SQL) — the parser is the second filter,
    /// not the first.</para>
    /// </summary>
    public static TraceAnchor? ResolveTraceAnchor(
        ChangeEvent change, IEnumerable<TraceLine> lines, Func<TraceLine, TraceChange?> parse)
    {
        ArgumentNullException.ThrowIfNull(change);
        ArgumentNullException.ThrowIfNull(lines);
        ArgumentNullException.ThrowIfNull(parse);

        var inSpan = lines
            .Where(l => l.EventTimeUtc > change.PreviousCaptureTime && l.EventTimeUtc <= change.ChangeTime)
            .Select(parse)
            .Where(c => c is not null)
            .Select(c => c!)
            .ToList();
        if (inSpan.Count == 0)
            return null;

        var matched = new Dictionary<string, TraceChange>(StringComparer.Ordinal);
        foreach (var setting in change.Changes.Where(c => c.Family == ChangeFamily.ServerConfig))
        {
            var observedNew = setting.NewValueConfigured ?? setting.NewValueInUse;
            if (observedNew is null)
                continue;

            var line = inSpan
                .Where(c => string.Equals(c.Subject, setting.Name.Trim(), StringComparison.OrdinalIgnoreCase))
                .Where(c => c.OldValue is not null && c.NewValue is not null && c.OldValue != c.NewValue && c.NewValue == observedNew)
                .OrderByDescending(c => c.ChangedAtUtc)
                .FirstOrDefault();
            if (line is not null)
                matched[setting.Name] = line;
        }

        return matched.Count == 0
            ? null
            : new TraceAnchor(matched.Values.Max(c => c.ChangedAtUtc), matched);
    }

    /// <summary>The server_config slice's join: <see cref="ResolveTraceAnchor"/> over <see cref="ParseReconfigureLine"/>.</summary>
    public static TraceAnchor? ResolveServerConfigTraceAnchor(ChangeEvent change, IEnumerable<TraceLine> lines) =>
        ResolveTraceAnchor(change, lines, ParseReconfigureLine);

    /// <summary>The instant the compare and the fact anchor on: the trace's when there is one, the observing capture's otherwise.</summary>
    public static DateTime AnchorTime(ChangeEvent change, TraceAnchor? anchor)
    {
        ArgumentNullException.ThrowIfNull(change);
        return anchor?.ChangedAtUtc ?? change.ChangeTime;
    }

    /// <summary>
    /// The coverage caveat the compare is flagged with — the same rule <c>compare_analysis</c> applies on
    /// both SKUs: a side that was partly observed or unobserved flags every verdict row. A null coverage
    /// (collection threw) is not a caveat here; it is <see cref="MetaCompareUnavailable"/>.
    /// </summary>
    public static bool CoverageCaveatFor(WindowCoverage? before, WindowCoverage? after) =>
        (before is not null && (before.IsPartial || !before.IsObserved))
        || (after is not null && (after.IsPartial || !after.IsObserved));

    /// <summary>
    /// Builds the fact. <paramref name="compare"/> is null when the compare could not run at all (both
    /// collections threw, so both coverages are null and both fact lists are empty); an EMPTY compare over
    /// observed windows is not null — it is the "nothing moved" answer, and that is a finding too.
    /// <paramref name="anchor"/> is the trace's answer to WHEN (#3740) or null for the observation anchor;
    /// <paramref name="windows"/> must have been computed over <see cref="AnchorTime"/> of the same pair,
    /// so the fact's change time and the compare's halves are one instant.
    /// </summary>
    public static Fact BuildFact(
        int serverId,
        ChangeEvent change,
        int earlierEventsInWindow,
        CompareWindows windows,
        ComparisonResult? compare,
        WindowCoverage? beforeCoverage,
        WindowCoverage? afterCoverage,
        TraceAnchor? anchor = null)
    {
        ArgumentNullException.ThrowIfNull(change);
        ArgumentNullException.ThrowIfNull(windows);

        var anchorTime = AnchorTime(change, anchor);
        var metadata = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            [MetaChangedSettings] = change.Changes.Count,
            [MetaChangeFamily] = (double)(int)change.Families,
            [MetaChangeTimeUnix] = Unix(anchorTime),
            [MetaAnchorClock] = anchor is null ? AnchorSourceObservation : AnchorSourceDefaultTrace,
            [MetaObservedAtUnix] = Unix(change.ChangeTime),
            /* The span disclosure belongs to the observation anchor alone: with the trace's time in hand the
               "somewhere in the N h since the previous snapshot" sentence would be false, and the composer
               gates it on this value. */
            [MetaObservationGapHours] = anchor is null ? Math.Max(0, change.ObservationGap.TotalHours) : 0,
            [MetaBeforeHours] = CompareWindowHours,
            [MetaAfterHoursObserved] = windows.AfterHoursObserved,
            [MetaAfterWindowClamped] = windows.AfterClamped ? 1 : 0,
            [MetaEarlierEventsInWindow] = Math.Max(0, earlierEventsInWindow),
            [MetaCompareUnavailable] = compare is null ? 1 : 0,
        };

        if (beforeCoverage is not null)
            metadata[MetaBeforeCoverageFraction] = beforeCoverage.Fraction;
        if (afterCoverage is not null)
            metadata[MetaAfterCoverageFraction] = afterCoverage.Fraction;
        if (anchor is not null)
            metadata[MetaObservedLagHours] = Math.Max(0, (change.ChangeTime - anchor.ChangedAtUtc).TotalHours);

        foreach (var c in change.Changes)
        {
            if (c.OldValueInUse is { } oldInUse) metadata[OldInUseKey(c.Name)] = oldInUse;
            if (c.NewValueInUse is { } newInUse) metadata[NewInUseKey(c.Name)] = newInUse;
            if (c.OldValueConfigured is { } oldCfg) metadata[OldConfiguredKey(c.Name)] = oldCfg;
            if (c.NewValueConfigured is { } newCfg) metadata[NewConfiguredKey(c.Name)] = newCfg;
            /* requires_restart is a sys.configurations concept (is_dynamic); a database option and a flag take
               effect when set, so the key is the server family's alone rather than a 0 on every row. */
            if (c.Family == ChangeFamily.ServerConfig)
                metadata[RequiresRestartKey(c.Name)] = c.RequiresRestart ? 1 : 0;
            if (anchor is not null && anchor.Matched.TryGetValue(c.Name, out var traced))
                metadata[TraceChangeTimeUnixKey(c.Name)] = Unix(traced.ChangedAtUtc);
        }

        if (compare is not null)
        {
            metadata[MetaComparedKeys] = compare.Rows.Count;
            metadata[MetaWorse] = compare.Worse;
            metadata[MetaBetter] = compare.Better;
            metadata[MetaStable] = compare.Stable;

            /* Rows arrive worse → better → stable, larger move first (ComparisonBanding.Compare). */
            var moved = compare.Rows.Where(r => r.Status != ComparisonBanding.StatusStable).ToList();
            foreach (var row in moved.Take(MaxMovedKeysInMetadata))
            {
                metadata[StatusKey(row.Key)] = row.Status == ComparisonBanding.StatusWorse ? 1 : -1;
                if (row.RelativeMove is { } rel) metadata[RelativeMoveKey(row.Key)] = rel;
                if (row.DeltaSigma is { } sigma) metadata[DeltaSigmaKey(row.Key)] = sigma;
                if (row.ValueDelta is { } delta) metadata[ValueDeltaKey(row.Key)] = delta;
            }
            metadata[MetaMovedKeysOmitted] = Math.Max(0, moved.Count - MaxMovedKeysInMetadata);
        }

        /* The finding's database seam (persisted, shown on cards): filled only when EVERY change is a
           database option on ONE database. A mixed event has a server-scoped member, and a two-database
           event has no single database to claim; both stay server-scoped, and the prose names each database. */
        var databases = change.Changes.Select(c => c.Family == ChangeFamily.DatabaseConfig ? c.DatabaseName : null).Distinct().ToList();
        var databaseName = databases.Count == 1 && databases[0] is not null ? databases[0] : null;

        return new Fact
        {
            Source = FactSource,
            Key = FactKey,
            Value = change.Changes.Count,
            ServerId = serverId,
            DatabaseName = databaseName,
            ObjectName = string.Join(SettingSeparator, change.Changes.Select(EncodeSegment)),
            /* Preset, never scored: see the class remarks. Both fields, because the mute filter and the
               story reader read Severity while ComparisonBanding's ladder reads BaseSeverity. */
            BaseSeverity = InformationSeverity,
            Severity = InformationSeverity,
            Metadata = metadata
        };
    }

    /// <summary>Naive UTC to Unix seconds. The store's timestamps are naive UTC (Kind Unspecified off both
    /// readers), so the kind is stamped rather than converted.</summary>
    private static double Unix(DateTime utc) =>
        new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeSeconds();

    /* ── the ObjectName segment grammar (slice two) ── */

    /// <summary>Prefix of a database-option segment: <c>db|{setting}|{old}|{new}|{database}</c>. The database name
    /// is LAST so a name containing the field separator survives a bounded split (a column name and a collected
    /// option value never contain one).</summary>
    public const string DatabaseSegmentPrefix = "db|";
    /// <summary>Prefix of a trace-flag segment: <c>tf|{flag}|{changeType}|{scope}</c>.</summary>
    public const string TraceFlagSegmentPrefix = "tf|";
    private const char SegmentFieldSeparator = '|';

    /// <summary>
    /// One change as its ObjectName segment. A server setting is its bare option name — slice one's grammar,
    /// unchanged, and no <c>sys.configurations</c> name starts with either prefix. A database option and a
    /// flag carry the text the doubles-only metadata cannot: the values, the scope, the diff's change word.
    /// A null collected value is an empty field, which <see cref="DecodeSegment"/> reads back as null.
    /// </summary>
    public static string EncodeSegment(SettingChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        return change.Family switch
        {
            ChangeFamily.DatabaseConfig =>
                $"{DatabaseSegmentPrefix}{change.Setting}{SegmentFieldSeparator}{change.OldText}{SegmentFieldSeparator}{change.NewText}{SegmentFieldSeparator}{change.DatabaseName}",
            ChangeFamily.TraceFlags =>
                $"{TraceFlagSegmentPrefix}{change.Setting}{SegmentFieldSeparator}{change.ChangeType}{SegmentFieldSeparator}{change.Scope}",
            _ => change.Name,
        };
    }

    /// <summary>
    /// A segment read back as the change it encodes — the TEXT form: a server setting comes back as its name
    /// with null values (the composer reads those off the metadata, as slice one did), a database option and a
    /// flag with their text fields and the numeric slots re-derived the way <see cref="SettingChange.ForDatabase"/>
    /// and <see cref="SettingChange.ForTraceFlag"/> derive them. A malformed prefixed segment (too few fields)
    /// is read as a server setting named by the whole segment rather than thrown on: a card is not worth a pass.
    /// </summary>
    public static SettingChange DecodeSegment(string segment)
    {
        ArgumentNullException.ThrowIfNull(segment);
        if (segment.StartsWith(DatabaseSegmentPrefix, StringComparison.Ordinal))
        {
            var parts = segment[DatabaseSegmentPrefix.Length..].Split(SegmentFieldSeparator, 4);
            if (parts.Length == 4)
                return SettingChange.ForDatabase(parts[3], parts[0], NullIfEmpty(parts[1]), NullIfEmpty(parts[2]));
        }
        else if (segment.StartsWith(TraceFlagSegmentPrefix, StringComparison.Ordinal))
        {
            var parts = segment[TraceFlagSegmentPrefix.Length..].Split(SegmentFieldSeparator, 3);
            if (parts.Length == 3 && int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var flag))
            {
                var enabled = string.Equals(parts[1], "enabled", StringComparison.Ordinal);
                var disabled = string.Equals(parts[1], "disabled", StringComparison.Ordinal);
                return SettingChange.ForTraceFlag(flag, parts[1], parts[2], previousStatus: !enabled, newStatus: !disabled);
            }
        }

        return new SettingChange(segment, null, null, null, null, false);
    }

    private static string? NullIfEmpty(string s) => s.Length == 0 ? null : s;

    /// <summary>The changes a fact's ObjectName carries, decoded, in the order they were joined.</summary>
    public static IReadOnlyList<SettingChange> Changes(Fact fact) =>
        SettingNames(fact).Select(DecodeSegment).ToList();

    /// <summary>The segments a fact's ObjectName carries, in the order they were joined — for a server-family
    /// fact, the setting names (slice one's contract); for the other families, the encoded segments
    /// <see cref="Changes"/> decodes.</summary>
    public static IReadOnlyList<string> SettingNames(Fact fact) =>
        string.IsNullOrEmpty(fact?.ObjectName)
            ? []
            : fact.ObjectName.Split(SettingSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>The compared keys that moved, read back from the fact's metadata. The banding's own row order
    /// is not recoverable from a dictionary, so this re-derives the same rule: worse first, then better, the
    /// larger relative move first within each, then by key.</summary>
    public static IReadOnlyList<string> MovedKeys(Fact fact)
    {
        if (fact?.Metadata is null)
            return [];
        return fact.Metadata.Keys
            .Where(k => k.EndsWith(StatusSuffix, StringComparison.Ordinal))
            .Select(k => k[..^StatusSuffix.Length])
            .OrderBy(k => fact.Metadata[StatusKey(k)] > 0 ? 0 : 1)
            .ThenByDescending(k => fact.Metadata.GetValueOrDefault(RelativeMoveKey(k)))
            .ThenBy(k => k, StringComparer.Ordinal)
            .ToList();
    }
}
