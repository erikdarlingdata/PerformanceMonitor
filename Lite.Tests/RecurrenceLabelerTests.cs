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
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the shared <see cref="RecurrenceLabeler"/> (#3653 item 3, ruling Q3): a chain that fired in the same
/// hour×weekday slot for three consecutive weeks is LABELLED "recurring at this hour" at unchanged severity;
/// a fired Agent job whose slot moved since last week labels every story tied to it "maintenance window
/// moved"; and both labels are one sentence in the frozen advice plus metadata, readable back off the row by
/// <see cref="RecurrenceLabeler.TryReadLabel"/>. The labeler is pure over the store's read result, so every
/// case here is a fixture — the store reads themselves are pinned in their own SKU's tests.
///
/// <para>The severity pin is the one that matters most: the ruling's load-bearing half is "unchanged", and
/// it is asserted BIT-for-bit rather than approximately, because a future "small discount" is exactly the
/// change that would pass an approximate equality.</para>
/// </summary>
public class RecurrenceLabelerTests
{
    /* A Tuesday 14:37 UTC reference on a UTC-4 server: 10:37 local, slot 10:00 Tuesday. */
    private static readonly DateTime ReferenceUtc = new(2026, 9, 15, 14, 37, 12, DateTimeKind.Utc);
    private const int Offset = -240;
    private static readonly DateTime SlotLocal = new(2026, 9, 15, 10, 0, 0);

    private static AnalysisStory Story(string rootKey, string hash, double severity = 1.1, IEnumerable<string>? path = null, Dictionary<string, double>? metadata = null)
    {
        var s = new AnalysisStory
        {
            RootFactKey = rootKey,
            StoryPathHash = hash,
            StoryPath = path is null ? rootKey : string.Join(" → ", path),
            Severity = severity,
            Confidence = 0.4,
            Category = "test",
            Path = path is null ? [rootKey] : new List<string>(path),
            IncidentId = "inc-1",
            RootFactMetadata = metadata,
        };
        s.StoryText = FactAdvice.SerializeForStoryText(new AdviceBlock($"{rootKey} headline", $"{rootKey} investigation.", $"{rootKey} remediation."));
        return s;
    }

    /// <summary>A prior row for <paramref name="hash"/> in THIS slot, <paramref name="weeksAgo"/> weeks back.</summary>
    private static PriorOccurrence InSlot(string hash, int weeksAgo, string rootKey = "X") =>
        new(hash, rootKey, SlotLocal.AddDays(-7 * weeksAgo), null);

    /// <summary>A prior RUNNING_JOBS-rooted row at an arbitrary local bucket whose frozen headline names <paramref name="jobName"/>.</summary>
    private static PriorOccurrence JobRow(DateTime localBucket, string? jobName) =>
        new("job-hash", RecurrenceLabeler.JobKey, localBucket, FactAdvice.SerializeForStoryText(new AdviceBlock(
            jobName is null ? "2 Agent jobs running well past normal — likely stuck, not busy" : $"Agent job `{jobName}` running well past normal — likely stuck, not busy",
            "job investigation.", "job remediation.")));

    private static PriorOccurrenceRead Read(params PriorOccurrence[] rows) => new(Offset, rows);

    private static string Investigation(AnalysisStory s) => FactAdvice.TryReadStoryText(s.StoryText)!.Investigation;

    private static Fact FiredJob(string? name) => new()
    {
        Key = RecurrenceLabeler.JobKey,
        Source = "jobs",
        Value = 1,
        BaseSeverity = 0.5,
        Severity = 0.5,
        ObjectName = name,
        Metadata = new() { ["running_long_count"] = 1, ["max_percent_of_average"] = 340, ["max_duration_seconds"] = 5400 },
    };

    /* ---------------- the recurrence arm ---------------- */

    [Fact]
    public void ThreeConsecutiveWeeks_LabelsRecurring_StatesTheSlotOnTheServersClock_AndReadsBack()
    {
        var story = Story("SOS_SCHEDULER_YIELD", "h1");

        RecurrenceLabeler.Label([story], facts: null, ReferenceUtc, Read(InSlot("h1", 1), InSlot("h1", 2)));

        var inv = Investigation(story);
        Assert.Contains(RecurrenceLabeler.RecurringMarker, inv, StringComparison.Ordinal);
        Assert.Contains("for 3 consecutive weeks.", inv, StringComparison.Ordinal);
        Assert.Contains("10:00 Tuesday, server local time", inv, StringComparison.Ordinal);
        Assert.StartsWith("SOS_SCHEDULER_YIELD investigation.", inv, StringComparison.Ordinal); // appended, not replaced
        Assert.Equal("SOS_SCHEDULER_YIELD headline", FactAdvice.TryReadStoryText(story.StoryText)!.Headline); // headline untouched

        Assert.NotNull(story.RootFactMetadata);
        Assert.Equal(1, story.RootFactMetadata![RecurrenceLabeler.RecurringMetadataKey]);
        Assert.Equal(3, story.RootFactMetadata[RecurrenceLabeler.RecurrenceWeeksMetadataKey]);
        Assert.Equal(10, story.RootFactMetadata[RecurrenceLabeler.RecurrenceHourMetadataKey]);
        Assert.Equal((int)DayOfWeek.Tuesday, story.RootFactMetadata[RecurrenceLabeler.RecurrenceDayOfWeekMetadataKey]);

        var label = RecurrenceLabeler.TryReadLabel(story.StoryText);
        Assert.NotNull(label);
        Assert.True(label!.RecurringAtThisHour);
        Assert.Equal(3, label.RecurrenceWeeks);
        Assert.False(label.MaintenanceWindowMoved);
    }

    [Fact]
    public void TwoWeeks_IsACoincidence_NoLabel()
    {
        var story = Story("SOS_SCHEDULER_YIELD", "h1");
        var before = story.StoryText;

        RecurrenceLabeler.Label([story], null, ReferenceUtc, Read(InSlot("h1", 1)));

        Assert.Equal(before, story.StoryText);
        Assert.Null(story.RootFactMetadata);
        Assert.Null(RecurrenceLabeler.TryReadLabel(story.StoryText));
    }

    [Fact]
    public void GapWeek_ResetsTheRun_NoLabel()
    {
        /* Fired this week, two weeks ago and three weeks ago — with LAST week missing. Three hits in four
           weeks is not three consecutive; the run counts from this week backwards and stops at the first hole. */
        var story = Story("SOS_SCHEDULER_YIELD", "h1");
        var before = story.StoryText;

        RecurrenceLabeler.Label([story], null, ReferenceUtc, Read(InSlot("h1", 2), InSlot("h1", 3)));
        Assert.Equal(before, story.StoryText);

        /* And the other hole: this week, last week, a gap at two, a hit at three — two consecutive, not three. */
        RecurrenceLabeler.Label([story], null, ReferenceUtc, Read(InSlot("h1", 1), InSlot("h1", 3)));
        Assert.Equal(before, story.StoryText);
        Assert.Null(RecurrenceLabeler.TryReadLabel(story.StoryText));
    }

    [Fact]
    public void FourWeeks_StatesFour_TheReadsCeiling()
    {
        var story = Story("SOS_SCHEDULER_YIELD", "h1");

        RecurrenceLabeler.Label([story], null, ReferenceUtc, Read(InSlot("h1", 1), InSlot("h1", 2), InSlot("h1", 3)));

        Assert.Contains("for 4 consecutive weeks.", Investigation(story), StringComparison.Ordinal);
        Assert.Equal(4, RecurrenceLabeler.TryReadLabel(story.StoryText)!.RecurrenceWeeks);
    }

    [Fact]
    public void Label_LeavesSeverityBitIdentical_AndConfidenceToo()
    {
        /* The ruling: LABEL, not discount. Two identical stories, one labelled — every scored field equal
           to the bit. Severity is compared as its 64-bit pattern so a "small discount" cannot hide inside a
           tolerance. */
        var unlabelled = Story("SOS_SCHEDULER_YIELD", "h1", severity: 1.2345678901234567);
        var labelled = Story("SOS_SCHEDULER_YIELD", "h1", severity: 1.2345678901234567);

        RecurrenceLabeler.Label([labelled], null, ReferenceUtc, Read(InSlot("h1", 1), InSlot("h1", 2)));

        Assert.Contains(RecurrenceLabeler.RecurringMarker, Investigation(labelled), StringComparison.Ordinal);
        Assert.Equal(BitConverter.DoubleToInt64Bits(unlabelled.Severity), BitConverter.DoubleToInt64Bits(labelled.Severity));
        Assert.Equal(unlabelled.Confidence, labelled.Confidence);
        Assert.Equal(unlabelled.RootFactValue, labelled.RootFactValue);
        Assert.Equal(unlabelled.IncidentId, labelled.IncidentId);
        Assert.Equal(unlabelled.StoryPathHash, labelled.StoryPathHash);
    }

    [Fact]
    public void ADifferentChainInTheSlot_DoesNotLabelThisOne()
    {
        var story = Story("SOS_SCHEDULER_YIELD", "h1");
        var before = story.StoryText;

        RecurrenceLabeler.Label([story], null, ReferenceUtc, Read(InSlot("other", 1), InSlot("other", 2)));

        Assert.Equal(before, story.StoryText);
    }

    [Fact]
    public void ThisWeeksEarlierPasses_AreNotAPriorWeek()
    {
        /* Rows from earlier passes today in this slot (weeks-ago 0) must not count: a chain fired an hour ago
           is not "recurring". With only week 0 and week 1 present, the run is two. */
        var story = Story("SOS_SCHEDULER_YIELD", "h1");
        var before = story.StoryText;

        RecurrenceLabeler.Label([story], null, ReferenceUtc, Read(InSlot("h1", 0), InSlot("h1", 1)));

        Assert.Equal(before, story.StoryText);
    }

    [Fact]
    public void Offset_PutsTheSlotOnTheServersClock_NotUtc()
    {
        /* 03:30 UTC Tuesday on a UTC-4 server is 23:30 MONDAY local: the slot is 23:00 Monday, and the prior
           rows the store returns are already local — a UTC-keyed rule would look for 03:00 Tuesday and miss. */
        var referenceUtc = new DateTime(2026, 9, 15, 3, 30, 0, DateTimeKind.Utc);
        var localSlot = new DateTime(2026, 9, 14, 23, 0, 0);
        var story = Story("SOS_SCHEDULER_YIELD", "h1");

        RecurrenceLabeler.Label([story], null, referenceUtc, new PriorOccurrenceRead(Offset, [
            new PriorOccurrence("h1", "X", localSlot.AddDays(-7), null),
            new PriorOccurrence("h1", "X", localSlot.AddDays(-14), null),
        ]));

        Assert.Contains("23:00 Monday, server local time", Investigation(story), StringComparison.Ordinal);
        Assert.Equal(23, story.RootFactMetadata![RecurrenceLabeler.RecurrenceHourMetadataKey]);
        Assert.Equal((int)DayOfWeek.Monday, story.RootFactMetadata[RecurrenceLabeler.RecurrenceDayOfWeekMetadataKey]);
    }

    [Fact]
    public void NoOffsetInTheStore_KeysOnUtc_AndSaysSo()
    {
        /* A PostgreSQL target has no server_properties row; a SQL Server target's on-load collector may not
           have run. The read says null, the slot is the UTC hour, and the sentence discloses it instead of
           presenting a UTC hour as the server's. */
        var story = Story("SOS_SCHEDULER_YIELD", "h1");
        var utcSlot = new DateTime(2026, 9, 15, 14, 0, 0);

        RecurrenceLabeler.Label([story], null, ReferenceUtc, new PriorOccurrenceRead(null, [
            new PriorOccurrence("h1", "X", utcSlot.AddDays(-7), null),
            new PriorOccurrence("h1", "X", utcSlot.AddDays(-14), null),
        ]));

        var inv = Investigation(story);
        Assert.Contains("14:00 Tuesday UTC", inv, StringComparison.Ordinal);
        Assert.Contains("the store carries no UTC offset for this server", inv, StringComparison.Ordinal);
        Assert.DoesNotContain("server local time", inv, StringComparison.Ordinal);
        Assert.Equal(3, RecurrenceLabeler.TryReadLabel(story.StoryText)!.RecurrenceWeeks);
    }

    [Fact]
    public void Idempotent_ASecondPassWritesNothing()
    {
        var story = Story("SOS_SCHEDULER_YIELD", "h1");
        var read = Read(InSlot("h1", 1), InSlot("h1", 2));

        RecurrenceLabeler.Label([story], null, ReferenceUtc, read);
        var once = story.StoryText;
        RecurrenceLabeler.Label([story], null, ReferenceUtc, read);

        Assert.Equal(once, story.StoryText);
        Assert.Equal(1, Investigation(story).Split(RecurrenceLabeler.RecurringMarker).Length - 1);
    }

    [Fact]
    public void AbsolutionAndZeroSeverityStories_AreSkipped_AndLegacyTextIsLeftAlone()
    {
        var absolution = Story("SOS_SCHEDULER_YIELD", "h1");
        absolution.IsAbsolution = true;
        var zero = Story("SOS_SCHEDULER_YIELD", "h1", severity: 0);
        var legacy = Story("SOS_SCHEDULER_YIELD", "h1");
        legacy.StoryText = "plain legacy prose, not the {h,i,r} blob";
        var read = Read(InSlot("h1", 1), InSlot("h1", 2));

        RecurrenceLabeler.Label([absolution, zero, legacy], null, ReferenceUtc, read);

        Assert.DoesNotContain(RecurrenceLabeler.RecurringMarker, absolution.StoryText, StringComparison.Ordinal);
        Assert.DoesNotContain(RecurrenceLabeler.RecurringMarker, zero.StoryText, StringComparison.Ordinal);
        Assert.Equal("plain legacy prose, not the {h,i,r} blob", legacy.StoryText);
        Assert.Null(legacy.RootFactMetadata);
    }

    [Fact]
    public void EmptyOrNullRead_IsANoOp()
    {
        var story = Story("SOS_SCHEDULER_YIELD", "h1");
        var before = story.StoryText;

        RecurrenceLabeler.Label([story], null, ReferenceUtc, null);
        RecurrenceLabeler.Label([story], null, ReferenceUtc, PriorOccurrenceRead.Empty);
        RecurrenceLabeler.Label([], null, ReferenceUtc, Read(InSlot("h1", 1), InSlot("h1", 2)));

        Assert.Equal(before, story.StoryText);
    }

    [Fact]
    public void Metadata_IsACopy_TheRootFactsDictionaryIsNotMutated()
    {
        /* InferenceEngine hands the story the root FACT's Metadata by reference. Labelling must not write the
           label's keys into the fact — the fact is the engine's, and a later reader of facts would find keys
           no collector wrote. */
        var factMetadata = new Dictionary<string, double> { ["wait_time_ms"] = 1234 };
        var story = Story("SOS_SCHEDULER_YIELD", "h1", metadata: factMetadata);

        RecurrenceLabeler.Label([story], null, ReferenceUtc, Read(InSlot("h1", 1), InSlot("h1", 2)));

        Assert.NotSame(factMetadata, story.RootFactMetadata);
        Assert.False(factMetadata.ContainsKey(RecurrenceLabeler.RecurringMetadataKey));
        Assert.Equal(1234, story.RootFactMetadata!["wait_time_ms"]); // the fact's own keys are carried
        Assert.Equal(1, story.RootFactMetadata[RecurrenceLabeler.RecurringMetadataKey]);
    }

    /* ---------------- the moved-window arm ---------------- */

    [Fact]
    public void JobInADifferentSlotLastWeek_LabelsEveryStoryTiedToTheJob_AndNothingElse()
    {
        var job = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        var schM = Story("SCH_M", "schm-hash", path: ["SCH_M", RecurrenceLabeler.JobKey]);
        /* A #3709-folded anomaly: its path is its own key, and the reconciler's sentence is what ties it. */
        var folded = Story("ANOMALY_WRITE_LATENCY", "anom-hash");
        var foldedAdvice = FactAdvice.TryReadStoryText(folded.StoryText)!;
        folded.StoryText = FactAdvice.SerializeForStoryText(foldedAdvice with { Investigation = foldedAdvice.Investigation + $" {RecurrenceLabeler.JobLinkMarker} — Agent job `Nightly Index Maintenance` was running well past its normal duration." });
        var cpu = Story("CPU_SQL_PERCENT", "cpu-hash");
        var cpuBefore = cpu.StoryText;

        /* Last week the job's card sat at 02:00 and 03:00 Thursday; this pass is 10:00 Tuesday. */
        var lastThursday02 = new DateTime(2026, 9, 10, 2, 0, 0);
        var read = Read(JobRow(lastThursday02, "Nightly Index Maintenance"), JobRow(lastThursday02.AddHours(1), "Nightly Index Maintenance"));

        RecurrenceLabeler.Label([job, schM, folded, cpu], [FiredJob("Nightly Index Maintenance")], ReferenceUtc, read);

        foreach (var labelled in new[] { job, schM, folded })
        {
            var inv = Investigation(labelled);
            Assert.Contains(RecurrenceLabeler.MovedWindowMarker, inv, StringComparison.Ordinal);
            Assert.Contains("Agent job `Nightly Index Maintenance` ran at 02:00 Thursday last week, 10:00 Tuesday this week (server local time", inv, StringComparison.Ordinal);
            Assert.Equal(1, labelled.RootFactMetadata![RecurrenceLabeler.MovedWindowMetadataKey]);
            Assert.Equal(2, labelled.RootFactMetadata[RecurrenceLabeler.MovedFromHourMetadataKey]);
            Assert.Equal((int)DayOfWeek.Thursday, labelled.RootFactMetadata[RecurrenceLabeler.MovedFromDayOfWeekMetadataKey]);

            var label = RecurrenceLabeler.TryReadLabel(labelled.StoryText);
            Assert.True(label!.MaintenanceWindowMoved);
            Assert.False(label.RecurringAtThisHour);
        }

        /* The CPU story shares the run but is not the job's card: #3632 declined that edge, and so does this. */
        Assert.Equal(cpuBefore, cpu.StoryText);
        Assert.Null(cpu.RootFactMetadata);
    }

    [Fact]
    public void JobInTheSameSlotLastWeek_IsRecurringNotMoved()
    {
        var job = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        /* Last week: 10:00 Tuesday (this slot) AND 11:00 Tuesday. In this slot -> not moved. */
        var read = Read(JobRow(SlotLocal.AddDays(-7), "Nightly Index Maintenance"), JobRow(SlotLocal.AddDays(-7).AddHours(1), "Nightly Index Maintenance"));

        RecurrenceLabeler.Label([job], [FiredJob("Nightly Index Maintenance")], ReferenceUtc, read);

        Assert.DoesNotContain(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);
    }

    [Fact]
    public void JobInTheSameSlotForThreeWeeks_IsLabelledRecurring_ByTheChainArm()
    {
        /* The job's own chain recurring is the recurrence arm's case — the job-hash rows are in-slot rows
           like any other chain's. Both arms read the same rows; neither needs the other. */
        var job = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        var read = Read(JobRow(SlotLocal.AddDays(-7), "Nightly Index Maintenance"), JobRow(SlotLocal.AddDays(-14), "Nightly Index Maintenance"));

        RecurrenceLabeler.Label([job], [FiredJob("Nightly Index Maintenance")], ReferenceUtc, read);

        var label = RecurrenceLabeler.TryReadLabel(job.StoryText)!;
        Assert.True(label.RecurringAtThisHour);
        Assert.Equal(3, label.RecurrenceWeeks);
        Assert.False(label.MaintenanceWindowMoved);
    }

    [Fact]
    public void ADifferentJobLastWeek_OrAnUnnamedOne_LabelsNothing()
    {
        var lastThursday = new DateTime(2026, 9, 10, 2, 0, 0);

        var job = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        RecurrenceLabeler.Label([job], [FiredJob("Nightly Index Maintenance")], ReferenceUtc, Read(JobRow(lastThursday, "Weekly CHECKDB")));
        Assert.DoesNotContain(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);

        /* A pre-#3693 card carries no name: the store cannot say WHICH job ran last week, so it says nothing. */
        RecurrenceLabeler.Label([job], [FiredJob("Nightly Index Maintenance")], ReferenceUtc, Read(JobRow(lastThursday, null)));
        Assert.DoesNotContain(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);

        /* This pass's job fact has no name (job_name NULL in the store): nothing to compare, nothing said. */
        RecurrenceLabeler.Label([job], [FiredJob(null)], ReferenceUtc, Read(JobRow(lastThursday, "Nightly Index Maintenance")));
        Assert.DoesNotContain(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);

        /* No facts at all (the one-argument shape): the arm is skipped. */
        RecurrenceLabeler.Label([job], null, ReferenceUtc, Read(JobRow(lastThursday, "Nightly Index Maintenance")));
        Assert.DoesNotContain(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);

        /* A quiet job (base severity 0 — present, not running long) is not "fired", the #3709 predicate. */
        var quiet = FiredJob("Nightly Index Maintenance");
        quiet.BaseSeverity = 0;
        RecurrenceLabeler.Label([job], [quiet], ReferenceUtc, Read(JobRow(lastThursday, "Nightly Index Maintenance")));
        Assert.DoesNotContain(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);
    }

    [Fact]
    public void JobTwoWeeksAgoOnly_IsNotLastWeek_NoMovedLabel()
    {
        var job = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        var twoThursdaysAgo = new DateTime(2026, 9, 3, 2, 0, 0);

        RecurrenceLabeler.Label([job], [FiredJob("Nightly Index Maintenance")], ReferenceUtc, Read(JobRow(twoThursdaysAgo, "Nightly Index Maintenance")));

        Assert.DoesNotContain(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);
    }

    [Fact]
    public void JobNameComparison_IsCaseInsensitive_LikeAgentsOwnCatalog()
    {
        var job = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);

        RecurrenceLabeler.Label([job], [FiredJob("nightly index maintenance")], ReferenceUtc, Read(JobRow(new DateTime(2026, 9, 10, 2, 0, 0), "Nightly Index Maintenance")));

        Assert.Contains(RecurrenceLabeler.MovedWindowMarker, Investigation(job), StringComparison.Ordinal);
    }

    /* ---------------- the cross-file contracts this class leans on ---------------- */

    [Fact]
    public void TryReadJobName_ReadsTheComposersOwnHeadline_AndNullWhenItCarriesNoName()
    {
        /* The prior week's name is recovered off the frozen RUNNING_JOBS card FactAdvice.ComposeRunningJobs
           writes (#3693). Composed live, not hand-written, so a change to the composer's headline that the
           reader cannot follow fails HERE rather than silently labelling nothing forever. */
        var named = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        FactAdvice.PopulateStoryText([named], [FiredJob("Nightly Index Maintenance")]);
        Assert.Equal("Nightly Index Maintenance", RecurrenceLabeler.TryReadJobName(named.StoryText));

        /* Two long jobs: the headline names the furthest-past one and counts the other. */
        var two = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        var twoFact = FiredJob("Nightly Index Maintenance");
        twoFact.Metadata["running_long_count"] = 2;
        FactAdvice.PopulateStoryText([two], [twoFact]);
        Assert.Equal("Nightly Index Maintenance", RecurrenceLabeler.TryReadJobName(two.StoryText));

        var unnamed = Story(RecurrenceLabeler.JobKey, "job-hash", severity: 0.5);
        FactAdvice.PopulateStoryText([unnamed], [FiredJob(null)]);
        Assert.Null(RecurrenceLabeler.TryReadJobName(unnamed.StoryText));

        Assert.Null(RecurrenceLabeler.TryReadJobName(null));
        Assert.Null(RecurrenceLabeler.TryReadJobName(string.Empty));
        Assert.Null(RecurrenceLabeler.TryReadJobName("legacy prose"));
    }

    [Fact]
    public void JobLinkMarker_IsTheSpellingTheReconcilerAndTheSymptomCardsWrite()
    {
        /* Two writers, one spelling, restated in the labeler so it references no other lane's constant: the
           #3709 fold sentence's marker (internal, visible to this assembly) ... */
        Assert.Equal(AnomalyIncidentReconciler.JobSentenceMarker, RecurrenceLabeler.JobLinkMarker);

        /* ... and FactAdvice.LinkedJobClause on a symptom card composed with the job fired this window. */
        var schM = Story("SCH_M", "schm-hash");
        var schMFact = new Fact { Key = "SCH_M", Source = "waits", Value = 0.2, BaseSeverity = 0.8, Severity = 0.8, Metadata = new() { ["wait_time_ms"] = 120000, ["waiting_tasks_count"] = 40 } };
        FactAdvice.PopulateStoryText([schM], [schMFact, FiredJob("Nightly Index Maintenance")]);
        Assert.Contains(RecurrenceLabeler.JobLinkMarker, Investigation(schM), StringComparison.Ordinal);
    }

    [Fact]
    public void TryReadLabel_IsNullForEmptyLegacyAndUnlabelledText()
    {
        Assert.Null(RecurrenceLabeler.TryReadLabel(null));
        Assert.Null(RecurrenceLabeler.TryReadLabel(string.Empty));
        Assert.Null(RecurrenceLabeler.TryReadLabel("plain legacy prose"));
        Assert.Null(RecurrenceLabeler.TryReadLabel(FactAdvice.SerializeForStoryText(new AdviceBlock("h", "i", "r"))));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(3, 0)]
    [InlineData(4, 1)]
    [InlineData(7, 1)]
    [InlineData(10, 1)]
    [InlineData(11, 2)]
    [InlineData(14, 2)]
    [InlineData(21, 3)]
    public void WeeksAgo_RoundsToTheNearestWeek(int daysAgo, int expectedWeeks)
    {
        var reference = new DateTime(2026, 9, 15);
        Assert.Equal(expectedWeeks, RecurrenceLabeler.WeeksAgo(reference, reference.AddDays(-daysAgo)));
    }

    [Fact]
    public void ReadLowerBound_IsTheLookbackPlusAnHourOfSlack()
    {
        Assert.Equal(21, RecurrenceLabeler.LookbackDays);
        Assert.Equal(3, RecurrenceLabeler.MinimumConsecutiveWeeks);
        Assert.Equal(ReferenceUtc.AddDays(-21).AddHours(-1), RecurrenceLabeler.ReadLowerBoundUtc(ReferenceUtc));
    }
}
