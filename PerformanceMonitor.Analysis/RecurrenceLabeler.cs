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
/// One PRIOR persisted occurrence of a diagnostic chain, as the finding stores return it for
/// <see cref="RecurrenceLabeler"/> — collapsed to one row per (chain, root key, local hour bucket), because
/// the engine re-persists the same story every analysis cycle (<see cref="FindingOccurrences"/> measured
/// 27.9x mean duplication) and "the chain fired in that hour" is a fact about the hour, not about how many
/// passes ran inside it. <see cref="LocalBucket"/> is the persisted <c>analysis_time</c> shifted onto the
/// target's clock by the store and truncated to the hour, so its <see cref="DateTime.Hour"/>,
/// <see cref="DateTime.DayOfWeek"/> and <see cref="DateTime.Date"/> ARE the hour×weekday slot and the
/// calendar day it fell on; its Kind is Unspecified on purpose — it is not UTC and must not be tagged as
/// such. <see cref="StoryText"/> is carried ONLY for <c>RUNNING_JOBS</c>-rooted rows (the store reads it
/// through <c>MAX(CASE WHEN root_fact_key = 'RUNNING_JOBS' …)</c>) and is null otherwise: the frozen job
/// card is the one place a prior week's job NAME survives, since <see cref="Fact.ObjectName"/> is not a
/// finding-row column.
/// </summary>
public sealed record PriorOccurrence(string StoryPathHash, string RootFactKey, DateTime LocalBucket, string? StoryText);

/// <summary>
/// What one finding-store read of the prior weeks returns to <see cref="RecurrenceLabeler.Label"/>.
/// <see cref="UtcOffsetMinutes"/> is the target's UTC offset the store found on
/// <c>server_properties.utc_offset_minutes</c> — the same column and the same latest-row read the
/// parameter-sensitivity SQL in both fact collectors use — or null when the store carries none (a server
/// whose on-load collector has not run, or a PostgreSQL target, which has no <c>server_properties</c>
/// row at all). Null means every <see cref="PriorOccurrence.LocalBucket"/> is UTC, and the labeler SAYS so
/// in the sentence it writes rather than presenting a UTC hour as the server's.
/// </summary>
public sealed record PriorOccurrenceRead(int? UtcOffsetMinutes, IReadOnlyList<PriorOccurrence> Occurrences)
{
    /// <summary>The read that labels nothing: no offset, no rows. What a store returns when it could not read.</summary>
    public static PriorOccurrenceRead Empty { get; } = new(null, Array.Empty<PriorOccurrence>());
}

/// <summary>
/// The recurrence label a finding carries, read back off its frozen story text by
/// <see cref="RecurrenceLabeler.TryReadLabel"/> so the read surfaces (the MCP <c>get_analysis_findings</c>
/// twins) can publish it as fields. <see cref="RecurrenceWeeks"/> is the consecutive-week count the
/// sentence states (null when the chain was not labelled recurring); <see cref="MaintenanceWindowMoved"/>
/// is whether the moved-window sentence is present. Both false/null is the ordinary unlabelled finding.
/// </summary>
public sealed record RecurrenceLabel(int? RecurrenceWeeks, bool MaintenanceWindowMoved)
{
    /// <summary>True when the recurring-at-this-hour sentence is present — the flag the ruling names.</summary>
    public bool RecurringAtThisHour => RecurrenceWeeks is not null;
}

/// <summary>
/// Labels — and ONLY labels — the stories of one analysis pass whose diagnostic chain keeps a weekly
/// schedule (#3653 item 3, the A9 structural half; ruling Q3). Two labels, each one sentence appended to
/// the story's frozen advice plus a few metadata keys, and neither touches <see cref="AnalysisStory.Severity"/>.
///
/// <para><b>The ruling.</b> #3538 A9 proposed recurrence-AWARE SCORING: a finding that fires at the same
/// hour×weekday for three consecutive weeks is "expected" and its severity should be discounted. Erik
/// ruled the other way (Q3, verbatim: <i>"Label, not discount: 'recurring at this hour' at unchanged
/// severity. A weekly problem is still a problem."</i>). So the chain that fires every Tuesday at 14:00 is
/// still rated by what it did this Tuesday; what changes is that the card now SAYS it is the third Tuesday
/// running, which is the fact an operator needs to go looking for the schedule behind it (a job, a report,
/// a batch) instead of treating each week as a fresh incident. A test pins that a labelled story's Severity
/// is byte-identical to the same story unlabelled, because "unchanged" is the load-bearing half of the
/// ruling and the half a later "small discount" would erode first.</para>
///
/// <para><b>What "recurring at this hour" means, exactly.</b> The chain is <see cref="AnalysisStory.StoryPathHash"/>
/// — the identity muting keys on, a function of the path alone. The slot is the hour×weekday of the pass's
/// reference instant (the analysis window's end, which is "now" for a scheduled pass and the anchor for an
/// as-of one) on the TARGET's clock: the pass's UTC instant plus <c>server_properties.utc_offset_minutes</c>
/// when the store carries it, else UTC with the sentence saying so. Q6 ruled hour-of-week buckets key on the
/// target's local clock and that lane re-buckets the baselines; this lane does not build a UTC-only rule for
/// Q6 to invalidate, and it does not pretend the offset is a zone either — one offset is applied to the whole
/// 21-day read, so a DST change inside it smears the older week by an hour, which is exactly the Q6/Q8 defect
/// (the zone-id rung) and not something a fixed offset can fix. "Fired in week W's slot" is at least ONE
/// persisted row for the chain whose shifted <c>analysis_time</c> falls in that hour×weekday W weeks back;
/// the store collapses the per-cycle duplication (<see cref="FindingOccurrences"/>) so a chain that fired
/// once and one that fired in all sixteen passes of the hour count the same. The label needs
/// <see cref="MinimumConsecutiveWeeks"/> CONSECUTIVE weeks counting this one — this pass plus the two
/// immediately prior; a week with a gap resets the run (three hits over four weeks with a hole is two
/// consecutive, not three). The read spans <see cref="LookbackDays"/>, so the stated count tops out at
/// four; "for 4 consecutive weeks" on a chain that has fired every week for a year is a floor stated
/// honestly, not a measurement of the year.</para>
///
/// <para><b>What "maintenance window moved" means.</b> The maintenance edges (#3632) and the anomaly fold
/// (#3709) put a long-running Agent job's symptoms on one incident and name the job (#3693,
/// <see cref="Fact.ObjectName"/> on <c>RUNNING_JOBS</c>). A job that runs weekly but SLIDES — Thursday 02:00
/// last week, Tuesday 03:00 this week — defeats the recurrence label by construction (different slot), and
/// defeats the operator the same way: last week's symptom "went away" at the old hour and a new one appeared
/// at the new one. So when this pass carries the fired job by name, and LAST week's persisted
/// <c>RUNNING_JOBS</c>-rooted card names the SAME job in a different slot and not in this one, every story
/// this pass ties to the job — its path carries <c>RUNNING_JOBS</c>, or its frozen text carries the job link
/// <see cref="JobLinkMarker"/> that <c>FactAdvice.LinkedJobClause</c> and the #3709 fold write — gets one
/// sentence naming the job and both slots. Last week only, by design: the sentence compares two weeks and a
/// job that has moved twice is two moves, each said in its own week. The prior week's name is read back off
/// the frozen job card (<see cref="TryReadJobName"/>) because the finding row has no object-name column and a
/// rung for one is not this lane's; a pre-#3693 row carries no name, matches nothing, and labels nothing —
/// the honest outcome for a week whose job the store cannot name.</para>
///
/// <para><b>Where it runs and why here.</b> After <c>FactAdvice.PopulateStoryText</c> has frozen each story's
/// advice, after <c>InferenceEngine.ClusterIntoIncidents</c> / <c>IncidentId.StampClusters</c> and after
/// <c>AnomalyIncidentReconciler.Reconcile</c> — and BEFORE the finding store copies StoryText onto the
/// persisted finding, so the sentence is on the row every card, e-mail and MCP read renders (the #3709
/// pattern: read back with <see cref="FactAdvice.TryReadStoryText"/>, re-freeze with
/// <see cref="FactAdvice.SerializeForStoryText"/> — the same <c>{h,i,r}</c> blob, so no renderer changes).
/// It is a step of its own rather than an arm of the reconciler or the scorer because it needs a STORE read
/// the rest of the story pipeline does not have — the prior weeks — and because the scorer is precisely the
/// thing the ruling said not to involve. The read is one statement per pass, not per story
/// (<c>PgFindingStore.GetPriorOccurrencesAsync</c> / Lite <c>FindingStore.GetPriorOccurrencesAsync</c>),
/// and this class is pure over its result, so it is testable without a store. A store that could not read
/// returns <see cref="PriorOccurrenceRead.Empty"/> and the pass labels nothing — a label is presentation,
/// and an unreadable history must not cost the pass its findings.</para>
///
/// <para><b>Metadata rides on the story's root-fact metadata</b> (<see cref="AnalysisStory.RootFactMetadata"/>),
/// which the stores copy onto the in-memory finding for the notification layer — a COPY of the dictionary,
/// because the engine hands the story the root FACT's own dictionary by reference and the fact is not this
/// class's to mutate. Nothing here is persisted except through the sentence; the finding row has no metadata
/// column and the MCP twins recover the flag from the sentence with <see cref="TryReadLabel"/>.</para>
/// </summary>
public static class RecurrenceLabeler
{
    /// <summary>
    /// How far back the store reads, in days: three weeks, so the slot three weeks ago is the oldest one
    /// examined and the stated count can reach four. Well inside the findings retention default (30 days,
    /// <c>AnalysisRetentionDefaults.FindingsRetentionDays</c>); a shorter retention truncates the count
    /// honestly rather than inventing weeks.
    /// </summary>
    public const int LookbackDays = 21;

    /// <summary>
    /// Consecutive weeks (counting this pass) before a chain is labelled recurring: three, the number the
    /// issue proposed and the ruling kept. Two is a coincidence; three same-slot weeks in a row is a schedule.
    /// </summary>
    public const int MinimumConsecutiveWeeks = 3;

    /// <summary>The long-running-job fact key — the maintenance edges' destination (#3632) and the moved-window arm's subject.</summary>
    public const string JobKey = "RUNNING_JOBS";

    /// <summary>
    /// The leading text of the sentence that ties a card to the fired job — written by
    /// <c>FactAdvice.LinkedJobClause</c> onto the SCH_M / IO_WRITE_LATENCY_MS / WRITELOG cards and by
    /// <c>AnomalyIncidentReconciler.JobSentenceMarker</c> onto a folded maintenance-family anomaly (#3709).
    /// A story whose frozen Investigation carries it is one the engine linked to the job even when its path
    /// does not (the job was consumed by another story first), so the moved-window sentence belongs on it too.
    /// Restated here rather than referenced so this file depends on no other lane's constant; a test pins the
    /// two spellings equal.
    /// </summary>
    public const string JobLinkMarker = "RUNNING_JOBS fired in the same window";

    /// <summary>Every recurring sentence starts with this — the ruling's own words — so the append is idempotent and a reader can find it without pinning the prose.</summary>
    public const string RecurringMarker = "Recurring at this hour:";

    /// <summary>Every moved-window sentence starts with this, for the same two reasons as <see cref="RecurringMarker"/>.</summary>
    public const string MovedWindowMarker = "Maintenance window moved:";

    /// <summary>Metadata key: 1 when the story is labelled recurring. Doubles-only dictionary, so a flag is 1, never true.</summary>
    public const string RecurringMetadataKey = "recurring_at_this_hour";

    /// <summary>Metadata key: the consecutive-week count the sentence states (this pass included).</summary>
    public const string RecurrenceWeeksMetadataKey = "recurrence_weeks";

    /// <summary>Metadata key: the slot's hour (0–23) on the clock the sentence names.</summary>
    public const string RecurrenceHourMetadataKey = "recurrence_local_hour";

    /// <summary>Metadata key: the slot's weekday, 0 = Sunday … 6 = Saturday — <c>EXTRACT(DOW)</c>'s and <see cref="System.DayOfWeek"/>'s shared convention.</summary>
    public const string RecurrenceDayOfWeekMetadataKey = "recurrence_local_dow";

    /// <summary>Metadata key: 1 when the story carries the moved-window sentence.</summary>
    public const string MovedWindowMetadataKey = "maintenance_window_moved";

    /// <summary>Metadata key: the hour the job's earliest slot fell in LAST week.</summary>
    public const string MovedFromHourMetadataKey = "maintenance_window_last_week_hour";

    /// <summary>Metadata key: the weekday (0 = Sunday) the job's earliest slot fell on LAST week.</summary>
    public const string MovedFromDayOfWeekMetadataKey = "maintenance_window_last_week_dow";

    /// <summary>
    /// The LOWER bound of the store read for a pass anchored at <paramref name="referenceUtc"/>: the lookback
    /// plus one hour of slack, because the slot three weeks ago STARTS up to an hour before
    /// <c>referenceUtc − 21 days</c> (the reference sits somewhere inside its own hour) and a bound at the
    /// instant would cut that slot's earliest passes. The store applies the hour×weekday filter, so the slack
    /// admits no wrong-slot row; it only completes the oldest right one. The upper bound is the reference
    /// itself — this pass has not persisted yet, and earlier passes inside the same hour today are the same
    /// week's slot, which the labeler ignores (a chain fired an hour ago is not "recurring").
    /// </summary>
    public static DateTime ReadLowerBoundUtc(DateTime referenceUtc) =>
        referenceUtc.AddDays(-LookbackDays).AddHours(-1);

    /// <summary>
    /// Labels this pass's <paramref name="stories"/> from the prior weeks the store read. Pure over its
    /// arguments; mutates only <see cref="AnalysisStory.StoryText"/> and <see cref="AnalysisStory.RootFactMetadata"/>
    /// (replaced with a copy) on the stories it labels, and never <see cref="AnalysisStory.Severity"/>.
    /// Absolution stories and stories at severity 0 are skipped — the stores drop them before they become
    /// findings, so a label on one would be written to nothing. <paramref name="facts"/> is the run's FULL
    /// scored fact list, read only for the fired <c>RUNNING_JOBS</c> fact's <see cref="Fact.ObjectName"/>
    /// (the #3709 predicate: base severity above zero); null skips the moved-window arm and nothing else.
    /// <paramref name="referenceUtc"/> is the pass's window end — <c>AnalysisContext.TimeRangeEnd</c>, which is
    /// the persisted <c>analysis_time</c>'s hour in every pass that does not straddle an hour boundary, and the
    /// anchor for an as-of pass. A null or empty <paramref name="prior"/> labels nothing.
    /// </summary>
    public static void Label(
        IReadOnlyList<AnalysisStory> stories,
        IReadOnlyList<Fact>? facts,
        DateTime referenceUtc,
        PriorOccurrenceRead? prior)
    {
        if (stories is null || stories.Count == 0 || prior is null || prior.Occurrences is null || prior.Occurrences.Count == 0)
            return;

        var offsetKnown = prior.UtcOffsetMinutes.HasValue;
        var referenceLocal = referenceUtc.AddMinutes(prior.UtcOffsetMinutes ?? 0);
        var referenceBucket = TruncateToHour(referenceLocal);

        // The prior weeks, indexed two ways off one read: by chain -> the weeks-ago it fired in THIS slot
        // (the recurrence arm), and the job-rooted rows with their slot and the name their frozen card
        // carries (the moved-window arm). Week 0 — earlier passes today, in this hour or another — is
        // dropped here: it is this week, not a prior one, on either arm.
        var sameSlotWeeksByChain = new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        var priorJobRows = new List<(int WeeksAgo, DateTime Bucket, string? JobName)>();

        foreach (var occurrence in prior.Occurrences)
        {
            if (occurrence is null)
                continue;

            var weeksAgo = WeeksAgo(referenceBucket.Date, occurrence.LocalBucket.Date);
            if (weeksAgo < 1)
                continue;

            if (SameSlot(occurrence.LocalBucket, referenceBucket) && !string.IsNullOrEmpty(occurrence.StoryPathHash))
            {
                if (!sameSlotWeeksByChain.TryGetValue(occurrence.StoryPathHash, out var weeks))
                    sameSlotWeeksByChain[occurrence.StoryPathHash] = weeks = new HashSet<int>();
                weeks.Add(weeksAgo);
            }

            if (string.Equals(occurrence.RootFactKey, JobKey, StringComparison.Ordinal))
                priorJobRows.Add((weeksAgo, occurrence.LocalBucket, TryReadJobName(occurrence.StoryText)));
        }

        // This pass's fired job, by name — the same fact and the same predicate the #3709 fold reads. Null
        // (no facts, no fact, a quiet job, or a store whose job_name was NULL) means the moved-window arm has
        // no name to compare last week's against, and says nothing rather than guessing.
        var jobFact = facts?.FirstOrDefault(f => f is not null && string.Equals(f.Key, JobKey, StringComparison.Ordinal) && f.BaseSeverity > 0);
        var jobName = string.IsNullOrEmpty(jobFact?.ObjectName) ? null : jobFact!.ObjectName;

        // Last week's slots for THIS job, by name. "Moved" needs at least one, and none of them in this
        // pass's slot — a job in the same slot last week is recurring, not moved, and the recurrence arm
        // above is the one that speaks to it.
        List<DateTime>? lastWeekSlotsForJob = null;
        if (jobName is not null)
        {
            lastWeekSlotsForJob = priorJobRows
                .Where(r => r.WeeksAgo == 1 && r.JobName is not null && string.Equals(r.JobName, jobName, StringComparison.OrdinalIgnoreCase))
                .Select(r => r.Bucket)
                .ToList();
            if (lastWeekSlotsForJob.Count == 0 || lastWeekSlotsForJob.Any(b => SameSlot(b, referenceBucket)))
                lastWeekSlotsForJob = null;
        }

        foreach (var story in stories)
        {
            if (story is null || story.IsAbsolution || story.Severity <= 0)
                continue;

            if (!string.IsNullOrEmpty(story.StoryPathHash) && sameSlotWeeksByChain.TryGetValue(story.StoryPathHash, out var weeks))
            {
                // Consecutive from this week backwards: this pass is week 0 and counts; each prior week counts
                // only while the one before it did. {1,2} -> 3; {1} -> 2; {1,3} -> 2 (the gap at 2 ends the run).
                var consecutive = 1;
                while (weeks.Contains(consecutive))
                    consecutive++;

                if (consecutive >= MinimumConsecutiveWeeks)
                    ApplyRecurring(story, consecutive, referenceBucket, offsetKnown);
            }

            if (lastWeekSlotsForJob is not null && CarriesTheJob(story))
                ApplyMovedWindow(story, jobName!, lastWeekSlotsForJob.Min(), referenceBucket, offsetKnown);
        }
    }

    /// <summary>
    /// Reads the recurrence label back off a finding's frozen story text — for the read surfaces, which hold
    /// the persisted row and nothing else. Null for empty, legacy (non-JSON) or unlabelled text. The weeks
    /// count is parsed from the sentence this class wrote, off its own marker and its own fixed phrasing
    /// (<c>for N consecutive weeks.</c>), which is why the phrasing is fixed: this is the sentence reading
    /// itself, not a renderer parsing prose it does not own.
    /// </summary>
    public static RecurrenceLabel? TryReadLabel(string? storyText)
    {
        var advice = FactAdvice.TryReadStoryText(storyText);
        if (advice is null)
            return null;

        var investigation = advice.Investigation ?? string.Empty;
        int? weeks = null;
        var recurring = RecurringWeeksPattern.Match(investigation);
        if (recurring.Success && int.TryParse(recurring.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
            weeks = parsed;

        var moved = investigation.Contains(MovedWindowMarker, StringComparison.Ordinal);
        if (weeks is null && !moved)
            return null;

        return new RecurrenceLabel(weeks, moved);
    }

    /// <summary>
    /// The job name a frozen <c>RUNNING_JOBS</c> card carries, or null. <c>FactAdvice.ComposeRunningJobs</c>
    /// (#3693) opens the headline with <c>Agent job `name`</c> whenever the collector put a name on the fact;
    /// the backticked token after that phrase is the name, read off the HEADLINE because it is the shortest
    /// and most stable of the three fields (the Investigation restates it inside a longer sentence whose
    /// shape varies with the count). A card with no name — pre-#3693 rows, or a window where jobs ran but none
    /// ran long — returns null and matches nothing. A test pins this reader against the composer's live output
    /// so the two cannot drift apart silently.
    /// </summary>
    public static string? TryReadJobName(string? storyText)
    {
        var advice = FactAdvice.TryReadStoryText(storyText);
        if (advice is null || string.IsNullOrEmpty(advice.Headline))
            return null;

        var match = JobNamePattern.Match(advice.Headline);
        return match.Success && match.Groups[1].Value.Length > 0 ? match.Groups[1].Value : null;
    }

    /// <summary>
    /// Whole weeks between two calendar days, nearest: 7 days is 1, 14 is 2, and a job row four to ten days
    /// back is "last week" (a weekly job that slid a day or two is still the weekly job). For two rows in the
    /// SAME hour×weekday slot the difference is an exact multiple of seven and the rounding is inert — the
    /// recurrence arm never depends on it; only the moved-window arm, whose rows are in other slots, does.
    /// Days 0–3 round to 0, which both arms drop as "this week".
    /// </summary>
    internal static int WeeksAgo(DateTime referenceDate, DateTime occurrenceDate) =>
        (int)Math.Round((referenceDate.Date - occurrenceDate.Date).TotalDays / 7.0, MidpointRounding.AwayFromZero);

    private static bool SameSlot(DateTime a, DateTime b) =>
        a.Hour == b.Hour && a.DayOfWeek == b.DayOfWeek;

    /// <summary>The slot's start on the target's wall clock — Kind-Unspecified, like the store's buckets: it is not a UTC instant.</summary>
    private static DateTime TruncateToHour(DateTime value) =>
        new(value.Year, value.Month, value.Day, value.Hour, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// A story this pass ties to the fired job: its path consumed <c>RUNNING_JOBS</c> (the job-rooted story,
    /// or a symptom story that reached the job through a #3632 edge), or its frozen Investigation carries the
    /// job link (<see cref="JobLinkMarker"/>) — the SCH_M/write/log cards' clause and the #3709 fold sentence.
    /// </summary>
    private static bool CarriesTheJob(AnalysisStory story)
    {
        if (story.Path is { Count: > 0 } && story.Path.Contains(JobKey))
            return true;
        if (string.Equals(story.RootFactKey, JobKey, StringComparison.Ordinal))
            return true;
        var advice = FactAdvice.TryReadStoryText(story.StoryText);
        return advice is not null && advice.Investigation.Contains(JobLinkMarker, StringComparison.Ordinal);
    }

    private static void ApplyRecurring(AnalysisStory story, int weeks, DateTime slot, bool offsetKnown)
    {
        var advice = FactAdvice.TryReadStoryText(story.StoryText);
        if (advice is null || advice.Investigation.Contains(RecurringMarker, StringComparison.Ordinal))
            return; // legacy/empty text is not this class's format to guess at; an existing sentence is not repeated

        // Fixed phrasing on purpose: TryReadLabel parses "for N consecutive weeks." back out of it.
        var sentence =
            $" {RecurringMarker} this chain has fired in this hour×weekday slot — {Slot(slot, offsetKnown)} — for {weeks.ToString(CultureInfo.InvariantCulture)} consecutive weeks. " +
            "The severity above is unchanged (a weekly problem is still a problem); the label says the cause keeps a schedule, so look at what runs in this slot — a job, a report, a batch — before treating this as a new incident.";

        story.StoryText = FactAdvice.SerializeForStoryText(advice with { Investigation = advice.Investigation + sentence });

        var metadata = CopyMetadata(story);
        metadata[RecurringMetadataKey] = 1;
        metadata[RecurrenceWeeksMetadataKey] = weeks;
        metadata[RecurrenceHourMetadataKey] = slot.Hour;
        metadata[RecurrenceDayOfWeekMetadataKey] = (int)slot.DayOfWeek;
        story.RootFactMetadata = metadata;
    }

    private static void ApplyMovedWindow(AnalysisStory story, string jobName, DateTime lastWeekSlot, DateTime thisWeekSlot, bool offsetKnown)
    {
        var advice = FactAdvice.TryReadStoryText(story.StoryText);
        if (advice is null || advice.Investigation.Contains(MovedWindowMarker, StringComparison.Ordinal))
            return;

        var clock = offsetKnown ? "server local time" : "UTC — the store carries no UTC offset for this server";
        var sentence =
            $" {MovedWindowMarker} Agent job `{jobName}` ran at {HourAndDay(lastWeekSlot)} last week, {HourAndDay(thisWeekSlot)} this week ({clock}; hour buckets, the job's earliest hour last week). " +
            "The same weekly job in a different slot is why a symptom that \"went away\" at the old hour has appeared at this one — it moved with the job. The severity above is unchanged; check the job's schedule before treating either hour as a new incident.";

        story.StoryText = FactAdvice.SerializeForStoryText(advice with { Investigation = advice.Investigation + sentence });

        var metadata = CopyMetadata(story);
        metadata[MovedWindowMetadataKey] = 1;
        metadata[MovedFromHourMetadataKey] = lastWeekSlot.Hour;
        metadata[MovedFromDayOfWeekMetadataKey] = (int)lastWeekSlot.DayOfWeek;
        story.RootFactMetadata = metadata;
    }

    /// <summary>A copy, never the engine's dictionary: <see cref="AnalysisStory.RootFactMetadata"/> is the root fact's own <see cref="Fact.Metadata"/> by reference.</summary>
    private static Dictionary<string, double> CopyMetadata(AnalysisStory story) =>
        story.RootFactMetadata is null
            ? new Dictionary<string, double>(StringComparer.Ordinal)
            : new Dictionary<string, double>(story.RootFactMetadata, StringComparer.Ordinal);

    private static string Slot(DateTime slot, bool offsetKnown) =>
        offsetKnown
            ? $"{HourAndDay(slot)}, server local time"
            : $"{HourAndDay(slot)} UTC (the store carries no UTC offset for this server, so the slot is keyed on UTC)";

    /// <summary>"14:00 Tuesday" — invariant culture, so the sentence and its reader agree on every host.</summary>
    private static string HourAndDay(DateTime slot) =>
        $"{slot.Hour.ToString("00", CultureInfo.InvariantCulture)}:00 {CultureInfo.InvariantCulture.DateTimeFormat.GetDayName(slot.DayOfWeek)}";

    private static readonly Regex RecurringWeeksPattern = new(
        Regex.Escape(RecurringMarker) + @".*? for (\d+) consecutive weeks\.",
        RegexOptions.Compiled | RegexOptions.Singleline);

    private static readonly Regex JobNamePattern = new(
        @"^Agent job `([^`]+)`",
        RegexOptions.Compiled);
}
