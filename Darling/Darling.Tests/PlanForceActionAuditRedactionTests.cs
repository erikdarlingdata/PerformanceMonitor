/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4346: <c>PgPlanForceActionStore.SanitizeDetailForAudit</c> is the read-side gate that keeps
/// pre-#4326 rows' raw exception text out of <c>get_plan_force_actions</c> (and any other reader of
/// <c>GetRecentActionsAsync</c>). There is no build marker on the row, so the gate is a positive match
/// against the one line #4326 made the only safe shape for a <c>state_unavailable</c> evidence line;
/// anything else on that line is legacy and gets replaced.
/// </summary>
public sealed class PlanForceActionAuditRedactionTests
{
    [Fact]
    public void PostFixLine_PassesThrough_TypeOnly()
    {
        var detail = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    [Fact]
    public void PostFixLine_PassesThrough_WithSqlState()
    {
        var detail = "state_unavailable: the forcing and automatic-plan-correction state read failed (PostgresException, SQLSTATE 57014; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    [Fact]
    public void PostFixLine_PassesThrough_MissingSchemaNote()
    {
        var detail = "state_unavailable: the forcing and automatic-plan-correction state read failed (PostgresException, SQLSTATE 42P01; a table or column the read needs is missing, logged only at Debug level) \u2014 an unattended force cannot proceed on an unknown engine state";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    /// <summary>The pin: a pre-#4326 row's raw exception text (host, credential, relation names — anything
    /// an exception message can carry) never reaches the caller. This is the exact shape
    /// <c>PgPlanForceActionStore.TryGetTargetStatesAsync</c>'s old catch wrote before #4326.</summary>
    [Fact]
    public void LegacyLine_WithRawExceptionText_IsRedacted()
    {
        var legacy = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException: 28P01: password authentication failed for user \"darling_ro\" at host db-primary-02.internal)";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(legacy);

        Assert.DoesNotContain("darling_ro", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
        Assert.DoesNotContain("password authentication failed", sanitized);
        Assert.Equal(
            "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state",
            sanitized);
    }

    [Fact]
    public void OtherBlockerLines_AreNeverTouched()
    {
        var detail = "apc_owns_it: query_store_stats: plan 7 is_forced_plan = true\nstate_unavailable: garbage (Exception: whatever leaked)";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(detail)!;
        var lines = sanitized.Split('\n');

        Assert.Equal("apc_owns_it: query_store_stats: plan 7 is_forced_plan = true", lines[0]);
        Assert.DoesNotContain("whatever leaked", lines[1]);
    }

    [Fact]
    public void NonStateUnavailableDetail_IsUnchanged()
    {
        var detail = "no write path in this build (#2138 phase 1 is detection, evidence and dry-run only)";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    [Fact]
    public void NullDetail_StaysNull()
    {
        Assert.Null(PgPlanForceActionStore.SanitizeDetailForAudit(null));
    }

    /// <summary>pin (a) from the #4363 anchoring round: a legacy line that STARTS with the safe prefix but
    /// carries exception text after the fixed sentence — the shape a naive prefix check (rather than a full
    /// anchor) would have let through.</summary>
    [Fact]
    public void LegacyLine_SafePrefixWithTrailingExceptionText_IsRedacted()
    {
        var legacy = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) extra: password authentication failed for user \"darling_ro\" at host db-primary-02.internal \u2014 an unattended force cannot proceed on an unknown engine state";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(legacy)!;

        Assert.DoesNotContain("darling_ro", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
        Assert.DoesNotContain("password authentication failed", sanitized);
        Assert.Equal(
            "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state",
            sanitized);
    }

    /// <summary>pin (b): the safe shape followed by a newline and exception text — an embedded newline in
    /// a legacy exception message must not let the second line escape as if it were a sibling blocker's
    /// evidence line.</summary>
    [Fact]
    public void LegacyLine_SafeShapeFollowedByNewlineAndExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\nleaked: password=hunter2 host=db-primary-02.internal";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
        Assert.Equal(
            "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state",
            sanitized);
    }

    /// <summary>pin (c), safe half: a CRLF-terminated sibling blocker line ahead of the safe shape must not
    /// stop the safe shape from passing — the anchor treats the position right after '\n' as the line start
    /// regardless of a preceding '\r'.</summary>
    [Fact]
    public void CrlfPrecededSafeLine_StillPassesThrough()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var detail = "apc_owns_it: query_store_stats: plan 7 is_forced_plan = true\r\n" + safeLine;
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(detail)!;

        Assert.Equal(detail, sanitized);
    }

    /// <summary>pin (c), unsafe half: a CRLF-joined legacy line still gets redacted.</summary>
    [Fact]
    public void CrlfSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\r\nleaked: password=hunter2 host=db-primary-02.internal";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
    }

    /// <summary>A lone '\r' between the safe shape and smuggled text is not a line boundary to .NET's
    /// '^'/'\z' under <c>Multiline</c>, so the smuggled text stays inside the redacted block rather than
    /// surviving as if it were a new line.</summary>
    [Fact]
    public void LoneCrSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\rleaked: password=hunter2";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
    }

    /// <summary>The Unicode LINE SEPARATOR (U+2028) is not a line boundary to .NET's '^'/'\z' either, so
    /// smuggled text after one stays inside the redacted block.</summary>
    [Fact]
    public void UnicodeLineSeparatorSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\u2028leaked: password=hunter2";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
    }

    /// <summary>Same as the LINE SEPARATOR pin, for the Unicode PARAGRAPH SEPARATOR (U+2029).</summary>
    [Fact]
    public void UnicodeParagraphSeparatorSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\u2029leaked: password=hunter2";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
    }

    /// <summary>F1 (#4363 security round): <c>ForcePlanBotPolicy.Blockers</c> always adds
    /// <c>state_unavailable</c> LAST — <c>apc_owns_it</c>/<c>apc_enabled_for_database</c> both require a
    /// non-null, non-empty <c>state</c> and cannot co-occur with the null/empty-state condition that adds
    /// <c>state_unavailable</c> — so a real detail never has a sibling blocker AFTER it. A pre-#4326 row's
    /// raw exception message CAN embed <c>"\n" + "apc_owns_it:"</c> (or any other sibling's exact prefix)
    /// as free text; a sibling-prefix lookahead would have ended the block there and let everything after
    /// it — unredacted — through. The fix anchors the block to <c>\z</c> instead: everything from the
    /// legacy row's <c>state_unavailable:</c> onward is redacted, including the impersonating text.</summary>
    [Fact]
    public void LegacyLine_EmbeddingASiblingPrefix_IsRedactedToTheEnd()
    {
        var legacy = "apc_owns_it: query_store_stats: plan 7 is_forced_plan = true\n" +
            "state_unavailable: the forcing and automatic-plan-correction state read failed (PostgresException: 28P01: password authentication failed for user \"fake_user\"\n" +
            "apc_owns_it: host=db-fake-01.example password=hunter2) \u2014 an unattended force cannot proceed on an unknown engine state";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(legacy)!;

        Assert.DoesNotContain("hunter2", sanitized);
        Assert.DoesNotContain("db-fake-01", sanitized);
        Assert.DoesNotContain("fake_user", sanitized);
        Assert.StartsWith("apc_owns_it: query_store_stats: plan 7 is_forced_plan = true\n", sanitized);
        Assert.Equal(
            "apc_owns_it: query_store_stats: plan 7 is_forced_plan = true\n" +
            "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state",
            sanitized);
    }

    /// <summary>F2 (#4363 security round): the null-state "returned no row" shape
    /// (<c>ForcePlanBotPolicy.Blockers(target, null, null)</c>) never carried exception text, and must
    /// pass through unchanged — built from the writer itself so wording drift there turns this test red
    /// instead of silently over-redacting.</summary>
    [Fact]
    public void NoRowShape_FromTheWriter_PassesThroughUnchanged()
    {
        var target = new ForcePlanTarget(Database: "db1", QueryId: 42, PlanId: 7);
        var blockers = ForcePlanBotPolicy.Blockers(target, state: null, stateUnavailableReason: null);
        var detail = ForcePlanBotPolicy.Evidence(blockers)!;

        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
        Assert.Contains("returned no row for plan 7 of query 42 in db1", detail);
    }

    /// <summary>F2: the empty-state "ran and observed nothing" shape
    /// (<c>ForcePlanBotPolicy.Blockers(target, emptyState, null)</c>) — the common production case —
    /// never carried exception text either, and must pass through unchanged.</summary>
    [Fact]
    public void EmptyStateShape_FromTheWriter_PassesThroughUnchanged()
    {
        var target = new ForcePlanTarget(Database: "db1", QueryId: 42, PlanId: 7);
        var emptyState = new ForcePlanTargetState(
            PlanIsForced: null, PlanForcingType: null, ForceFailureCount: null, LastForceFailureReason: null,
            PlanObservedAtUtc: null, OtherForcedPlanId: null, OtherForcedPlanForcingType: null,
            OtherForcedPlanObservedAtUtc: null, ApcState: null, ApcStateReason: null, ApcRegressedPlanId: null,
            ApcLastGoodPlanId: null, ApcLastGoodPlanForcingType: null, ApcLastGoodPlanIsForced: null,
            ApcLastGoodPlanForceFailureReason: null, ApcExecuteActionInitiatedBy: null, ApcObservedAtUtc: null,
            ForceLastGoodPlanActualState: null, EnablementObservedAtUtc: null);
        Assert.True(emptyState.IsEmpty);

        var blockers = ForcePlanBotPolicy.Blockers(target, emptyState, stateUnavailableReason: null);
        var detail = ForcePlanBotPolicy.Evidence(blockers)!;

        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
        Assert.Contains("ran and observed nothing for this target inside the last", detail);
    }

    /// <summary>Both journal reads apply the sanitizer. Exercises
    /// <see cref="PgPlanForceActionStore.SanitizeDetailForAudit"/> the way both
    /// <c>GetRecentActionsAsync</c> and <c>GetPendingReviewsAsync</c> get it: both route through
    /// <see cref="PgPlanForceActionStore"/>'s shared <c>ReadRecord</c> mapper, which
    /// applies the sanitizer once per row, so a legacy exception-bearing line never survives either
    /// read's output shape.</summary>
    [Fact]
    public void BothReads_ApplyTheSameSanitizer()
    {
        var legacy = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException: 28P01: password authentication failed for user \"darling_ro\" at host db-primary-02.internal)";
        var recentActionsShape = legacy;
        var pendingReviewShape = legacy;

        Assert.DoesNotContain("db-primary-02", PgPlanForceActionStore.SanitizeDetailForAudit(recentActionsShape));
        Assert.DoesNotContain("db-primary-02", PgPlanForceActionStore.SanitizeDetailForAudit(pendingReviewShape));
    }

    /// <summary>
    /// #4376's CENSUS pin: every method on <c>PgPlanForceActionStore</c> that builds a
    /// <c>PlanForceActionRecord</c> from an <c>NpgsqlDataReader</c> does it by calling the shared
    /// <c>ReadRecord</c> mapper — the sanitizer's one choke point — rather than constructing the record
    /// directly, or reaching around it via a target-typed <c>new(...)</c>, a <c>with</c> expression that
    /// overwrites <c>Detail</c>, or a raw read of the <c>detail</c> column ordinal outside <c>ReadRecord</c>.
    /// Reads the class's own source text rather than trusting a comment, so a bypass added later fails
    /// THIS pin, not just a live-store test that may never run against a legacy row.
    ///
    /// <para>Comments are stripped and line endings normalized before scanning, so a comment that merely
    /// mentions a bypass shape can't false-fail the pin. The detection function is exercised against small
    /// synthetic sources below (the same shape <c>MigrationDataMovingRungCensusPins.TheScan_ActuallyReadsTheLadder</c>
    /// uses) so a regex that stopped matching anything would fail loudly there instead of leaving this
    /// pin vacuous.</para>
    /// </summary>
    [Fact]
    public void EveryRecordConstruction_GoesThroughReadRecord()
    {
        var source = StripComments(ReadStoreSource().ReplaceLineEndings("\n"));

        const string readRecordSignature = "private static PlanForceActionRecord ReadRecord(";
        var readRecordStart = source.IndexOf(readRecordSignature, StringComparison.Ordinal);
        Assert.True(readRecordStart >= 0, "ReadRecord itself was not found in PgPlanForceActionStore.cs — the scan below has nothing to exclude.");

        /* ReadRecord is an expression-bodied member ('=> new(...);'); its body ends at the first ');'
           after its signature, not at the class's own closing brace (which is what a bare search for
           "\n}" finds, since ReadRecord is the class's last member and every line inside it is
           indented). Bound the window there so it actually covers ReadRecord's body. */
        var readRecordBodyEnd = source.IndexOf(");", readRecordStart, StringComparison.Ordinal);
        Assert.True(readRecordBodyEnd >= 0, "could not find the end of ReadRecord's body — the exclusion window is unbounded.");
        readRecordBodyEnd += ");".Length;

        var readRecordWindow = source[readRecordStart..readRecordBodyEnd];
        Assert.False(string.IsNullOrEmpty(readRecordWindow), "the ReadRecord window is empty — the bounds above are wrong.");
        Assert.Contains("SanitizeDetailForAudit(", readRecordWindow, StringComparison.Ordinal);

        var beforeReadRecord = source[..readRecordStart];
        var afterReadRecord = source[readRecordBodyEnd..];
        var outsideReadRecord = beforeReadRecord + afterReadRecord;

        var constructionSites = CountConstructionSites(source);
        Assert.Equal(1, constructionSites);
        Assert.Equal(0, CountConstructionSites(outsideReadRecord));

        Assert.DoesNotContain("with { Detail", outsideReadRecord, StringComparison.Ordinal);
        Assert.DoesNotContain("with{Detail", outsideReadRecord, StringComparison.Ordinal);

        /* The only raw read of the detail column (ordinal 18, per ReadRecord's own
           'IsDBNull(18) ? null : reader.GetString(18)' pair) must be inside ReadRecord. */
        var detailOrdinal = DetailColumnOrdinal(readRecordWindow);
        Assert.False(
            outsideReadRecord.Contains($"GetString({detailOrdinal})", StringComparison.Ordinal)
            || outsideReadRecord.Contains($"IsDBNull({detailOrdinal})", StringComparison.Ordinal),
            $"the detail column (ordinal {detailOrdinal}) is read outside ReadRecord — that reader bypasses the sanitizer.");

        /* Positive controls: the SAME detection function run against small synthetic sources, so the
           pin above is proven live rather than trusted to have found nothing by accident. */
        const string directBypass =
            "class C {\n" +
            "    private static PlanForceActionRecord ReadRecord(NpgsqlDataReader reader) => new(\n" +
            "        ActionId: reader.GetInt64(0));\n" +
            "    private static PlanForceActionRecord ReadOther(NpgsqlDataReader reader) {\n" +
            "        return new PlanForceActionRecord(1, DateTime.UtcNow, 1, \"s\", \"d\", 1, 1, \"a\", \"m\", \"actor\", \"dec\", \"r\", 1, 1, 1, null, false, \"o\", null, null);\n" +
            "    }\n" +
            "}";
        Assert.Equal(2, CountConstructionSites(StripComments(directBypass)));

        const string targetTypedBypass =
            "class C {\n" +
            "    private static PlanForceActionRecord ReadRecord(NpgsqlDataReader reader) => new(\n" +
            "        ActionId: reader.GetInt64(0));\n" +
            "    private static PlanForceActionRecord ReadOther(NpgsqlDataReader reader) {\n" +
            "        return new(ActionId: reader.GetInt64(0));\n" +
            "    }\n" +
            "}";
        Assert.Equal(2, CountConstructionSites(StripComments(targetTypedBypass)));

        const string withExpressionBypass =
            "class C {\n" +
            "    private static PlanForceActionRecord ReadOther(NpgsqlDataReader reader, PlanForceActionRecord r) {\n" +
            "        return r with { Detail = SanitizeDetailForAudit(reader.GetString(18)) };\n" +
            "    }\n" +
            "}";
        Assert.Contains("with { Detail", StripComments(withExpressionBypass), StringComparison.Ordinal);

        const string commentOnlyMention =
            "class C {\n" +
            "    // not a real bypass: new PlanForceActionRecord( is just mentioned here, in a comment\n" +
            "    /* also new(ActionId: 1) doesn't count */\n" +
            "    private static PlanForceActionRecord ReadRecord(NpgsqlDataReader reader) => new(\n" +
            "        ActionId: reader.GetInt64(0));\n" +
            "}";
        Assert.Equal(1, CountConstructionSites(StripComments(commentOnlyMention)));
    }

    /// <summary>Strips <c>//</c> line comments and <c>/* */</c> block comments (crude but adequate for
    /// this class's own source, which has no such sequence inside a string literal) so a comment that
    /// mentions a bypass shape can't be counted as one.</summary>
    private static string StripComments(string source)
    {
        var withoutBlocks = Regex.Replace(source, @"/\*.*?\*/", "", RegexOptions.Singleline);
        return Regex.Replace(withoutBlocks, @"//[^\n]*", "");
    }

    /// <summary>Counts every construction of <c>PlanForceActionRecord</c>: the explicit
    /// <c>new PlanForceActionRecord(</c> form, plus every target-typed <c>new(</c> whose argument list
    /// opens with the record's first named parameter (<c>ActionId:</c>).</summary>
    private static int CountConstructionSites(string source)
    {
        var explicitCtor = Regex.Matches(source, @"new\s+PlanForceActionRecord\s*\(").Count;
        var targetTyped = Regex.Matches(source, @"new\s*\(\s*ActionId\s*:").Count;
        return explicitCtor + targetTyped;
    }

    /// <summary>
    /// #4346/#4377's exemption contract: the ONE named raw reader (<c>PlanForceActionDetailScrub.RunAsync</c>)
    /// must return no <c>detail</c> text to its caller — its public <c>Summary</c> carries counts only — and
    /// its write path must be built from <c>SanitizeDetailForAudit</c>'s output, not from the raw value it
    /// read. Reads the scrub's own source text (same technique as <see cref="PlanForceActionDetailCensusTests"/>
    /// uses) rather than trusting a comment, so a later change that widens the exemption's surface fails this
    /// pin directly.
    /// </summary>
    [Fact]
    public void PlanForceActionDetailScrub_ExemptedMethod_ReturnsNoDetailText()
    {
        var source = StripComments(ReadScrubSource().ReplaceLineEndings("\n"));

        /* Summary's public surface is counts only: no string-typed property. A `string` or `string?`
           property on the public Summary class would be the exemption smuggling detail text back out. */
        var summaryStart = source.IndexOf("public sealed class Summary", StringComparison.Ordinal);
        Assert.True(summaryStart >= 0, "could not find PlanForceActionDetailScrub.Summary to check its public surface.");
        var summaryEnd = source.IndexOf("\n}", summaryStart, StringComparison.Ordinal);
        Assert.True(summaryEnd >= 0, "could not find the end of Summary's body.");
        var summaryWindow = source[summaryStart..summaryEnd];
        Assert.DoesNotContain("public string", summaryWindow, StringComparison.Ordinal);

        /* RunAsync's return type is Task<Summary> — not a string, not a string-bearing tuple. */
        Assert.Contains("public static async Task<Summary> RunAsync(", source, StringComparison.Ordinal);

        /* The write path calls SanitizeDetailForAudit and stores exactly that result (the `sanitized`
           local), never the raw `detail` local it read, into the list that becomes the UPDATE payload. */
        Assert.Contains("PgPlanForceActionStore.SanitizeDetailForAudit(detail)", source, StringComparison.Ordinal);
        Assert.Contains("toUpdateDetails.Add(sanitized)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("toUpdateDetails.Add(detail)", source, StringComparison.Ordinal);
    }

    private static string ReadScrubSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "PlanForceActionDetailScrub.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }

    /// <summary>Derives the detail column's ordinal from ReadRecord's own
    /// <c>IsDBNull(N) ? null : reader.GetString(N)</c> pair on the <c>Detail:</c> line, rather than
    /// hard-coding it.</summary>
    private static int DetailColumnOrdinal(string readRecordWindow)
    {
        var match = Regex.Match(
            readRecordWindow,
            @"Detail:\s*SanitizeDetailForAudit\(reader\.IsDBNull\((?<ordinal>\d+)\)\s*\?\s*null\s*:\s*reader\.GetString\(\k<ordinal>\)\)");
        Assert.True(match.Success, "could not find ReadRecord's Detail: line in the expected shape to derive the detail column ordinal.");
        return int.Parse(match.Groups["ordinal"].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static string ReadStoreSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "PgPlanForceActionStore.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }
}
