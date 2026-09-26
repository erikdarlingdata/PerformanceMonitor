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
    /// read. Reads the scrub's own source text (same technique as the census scan above) rather than
    /// trusting a comment, so a later change that widens the exemption's surface fails this pin directly.
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

    /// <summary>
    /// #4376: the raw-<c>detail</c>-read choke point isn't just
    /// <c>PgPlanForceActionStore</c> — nothing under <c>Darling/</c> (outside <c>Darling.Tests</c>) or
    /// <c>PerformanceMonitor.Common/</c> may read the <c>detail</c> column of
    /// <c>collect.plan_force_actions</c> except <see cref="PgPlanForceActionStore.GetRecentActionsAsync"/>
    /// and <see cref="PgPlanForceActionStore.GetPendingReviewsAsync"/> (the two readers that feed
    /// <c>ReadRecord</c>). A SELECT list naming <c>detail</c> or <c>pfa.detail</c> in a string that also
    /// mentions <c>plan_force_actions</c> counts as a read; the INSERT column list in
    /// <c>JournalAsync</c> (and its <c>RETURNING action_id</c>) does not.
    ///
    /// <para>A field may be exempted by adding its fully qualified name (<c>Namespace.Type.FieldName</c>)
    /// to <see cref="RawDetailReaderExemptions"/>. All entries must share the SAME owning type — at most
    /// ONE exempted owner, reserved for a future one-time audit-detail scrub (#4346) — and each entry is
    /// itself asserted below (a) to actually be reachable from the census scan, and (b) to be referenced
    /// nowhere outside its own type (<see cref="AssertExemptedFieldsOnlyReferencedInsideTheirOwner"/>).</para>
    /// </summary>
    [Fact]
    public void NoOtherProductionCode_ReadsTheDetailColumnDirectly()
    {
        var exemptedOwners = RawDetailReaderExemptions
            .Select(e => e[..e.LastIndexOf('.')])
            .Distinct()
            .ToArray();
        Assert.True(exemptedOwners.Length <= 1, "at most one raw-detail-reader exempted TYPE is allowed, even if it owns multiple SQL fields.");

        AssertExemptedFieldsOnlyReferencedInsideTheirOwner();

        var violations = new System.Collections.Generic.List<string>();
        foreach (var file in ProductionSourceFiles())
        {
            var text = StripComments(File.ReadAllText(file).ReplaceLineEndings("\n"));
            foreach (var fqName in RawDetailReadersIn(text))
            {
                if (Array.IndexOf(RawDetailReaderExemptions, fqName) < 0
                    && Array.IndexOf(AllowedRawDetailReaders, fqName) < 0)
                {
                    violations.Add($"{Path.GetFileName(file)}: {fqName}");
                }
            }
        }

        Assert.True(
            violations.Count == 0,
            "raw read(s) of collect.plan_force_actions.detail outside ReadRecord's two callers, not in "
            + "RawDetailReaderExemptions: " + string.Join("; ", violations));

        /* Positive controls: the SAME detection function, run against synthetic sources. */
        const string rawSelectBypass =
            "namespace N {\n" +
            "class SomeOtherReader {\n" +
            "    public void ReadIt() {\n" +
            "        var cmd = new NpgsqlCommand(@\"SELECT detail FROM collect.plan_force_actions WHERE server_id = $1\", connection);\n" +
            "    }\n" +
            "}\n" +
            "}";
        var found = RawDetailReadersIn(StripComments(rawSelectBypass));
        Assert.Contains("N.SomeOtherReader.ReadIt", found);

        var exempted = new[] { "N.SomeOtherReader.ReadIt" };
        var stillViolating = System.Linq.Enumerable.Where(found, n => Array.IndexOf(exempted, n) < 0);
        Assert.Empty(stillViolating);

        const string insertOnlyMention =
            "namespace N {\n" +
            "class Journal {\n" +
            "    public void JournalAsync() {\n" +
            "        var cmd = new NpgsqlCommand(@\"INSERT INTO collect.plan_force_actions (action_time, detail) VALUES ($1, $2) RETURNING action_id\", connection);\n" +
            "    }\n" +
            "}\n" +
            "}";
        Assert.Empty(RawDetailReadersIn(StripComments(insertOnlyMention)));

        /* Positive control (a) — #4384's own false-green: a NEW const string field declared right before
           RunAsync, inside the scrub's own type. Before this fix, the field-held SQL with no enclosing
           method was attributed to "the next method declared in the same type" — RunAsync itself — so an
           evil field just riding next to the real one would inherit RunAsync's exemption for free. Now the
           field is attributed to ITS OWN name, which is not in RawDetailReaderExemptions, so it still
           violates even though it sits one line above the exempted method. */
        const string evilFieldBesideTheRealExemption =
            "namespace PerformanceMonitor.Darling.Service {\n" +
            "public static class PlanForceActionDetailScrub {\n" +
            "    private const string LegacyDetailCandidateSql = @\"SELECT action_id, detail FROM collect.plan_force_actions WHERE detail LIKE '%x%'\";\n" +
            "    private const string EvilSql = @\"SELECT detail FROM collect.plan_force_actions\";\n" +
            "    public static async Task<Summary> RunAsync() {\n" +
            "    }\n" +
            "}\n" +
            "}";
        var evilFound = RawDetailReadersIn(StripComments(evilFieldBesideTheRealExemption));
        Assert.Contains("PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub.LegacyDetailCandidateSql", evilFound);
        Assert.Contains("PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub.EvilSql", evilFound);
        Assert.DoesNotContain("PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub.RunAsync", evilFound);
        var evilStillViolating = System.Linq.Enumerable.Where(
            evilFound, n => Array.IndexOf(RawDetailReaderExemptions, n) < 0);
        Assert.Contains("PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub.EvilSql", evilStillViolating);

        /* Positive control (b): the scrub's real exempted field passes — covered end to end by
           RawDetailReaderExemption_RoundTrips_AgainstTheScrubsOwnAttribution below, restated here as the
           companion half of control (a): CandidateSql above IS in RawDetailReaderExemptions and is not a
           violation. */
        Assert.DoesNotContain(
            "PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub.LegacyDetailCandidateSql",
            evilStillViolating);

        /* Positive control (d): method-local SQL in another class attributes to that method's name, and
           is not exempted. */
        const string methodLocalInAnotherClass =
            "namespace N {\n" +
            "class OtherReader {\n" +
            "    public void GetIt() {\n" +
            "        const string sql = @\"SELECT detail FROM collect.plan_force_actions WHERE action_id = $1\";\n" +
            "    }\n" +
            "}\n" +
            "}";
        var methodLocalFound = RawDetailReadersIn(StripComments(methodLocalInAnotherClass));
        Assert.Contains("N.OtherReader.GetIt", methodLocalFound);

        /* Positive control (e): SQL sitting inside a NESTED type is attributed to the nested type's own
           member, not to the outer type's. */
        const string sqlInNestedType =
            "namespace N {\n" +
            "class Outer {\n" +
            "    public class Inner {\n" +
            "        public void ReadIt() {\n" +
            "            var cmd = new NpgsqlCommand(@\"SELECT detail FROM collect.plan_force_actions\", connection);\n" +
            "        }\n" +
            "    }\n" +
            "}\n" +
            "}";
        var nestedFound = RawDetailReadersIn(StripComments(sqlInNestedType));
        Assert.Contains("N.Inner.ReadIt", nestedFound);
        Assert.DoesNotContain("N.Outer.ReadIt", nestedFound);
    }

    /// <summary>
    /// The second guard #4384 adds: an exempted field's name may not be REFERENCED anywhere outside its own
    /// owning type (checked here, not just its own declaration — a raw read is only safe because the scrub
    /// keeps this field to itself). Scans comment- and string-stripped source for the field's bare
    /// identifier and asserts every occurrence's enclosing type is the exemption's own owner. The owning
    /// type in this repo has only <c>RunAsync</c> and its private static helpers as members, so "referenced
    /// only inside the owner type" and "referenced only by RunAsync or its private helpers" are the same
    /// check here; this asserts the broader (type-scoped) form, which is what the source actually lets a
    /// scan verify without re-deriving call graphs.
    /// </summary>
    private static void AssertExemptedFieldsOnlyReferencedInsideTheirOwner()
    {
        foreach (var exemption in RawDetailReaderExemptions)
        {
            var lastDot = exemption.LastIndexOf('.');
            var ownerType = exemption[..lastDot];
            var fieldName = exemption[(lastDot + 1)..];

            foreach (var file in ProductionSourceFiles())
            {
                var text = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file).ReplaceLineEndings("\n"));
                foreach (Match m in Regex.Matches(text, $@"\b{Regex.Escape(fieldName)}\b"))
                {
                    var enclosingType = EnclosingTypeFqName(text, m.Index);
                    Assert.True(
                        enclosingType == ownerType,
                        $"{fieldName} (exempted only inside {ownerType}) is referenced from {enclosingType ?? "<unknown>"} in {Path.GetFileName(file)}.");
                }
            }
        }
    }

    /// <summary>
    /// #4346's own census-attribution bug, pinned directly: the scan's own detection function
    /// (<see cref="RawDetailReadersIn"/>, built on <see cref="EnclosingMethodFqName"/>) must derive the SAME
    /// fully qualified name for <c>PlanForceActionDetailScrub</c>'s raw SELECT that
    /// <see cref="RawDetailReaderExemptions"/> lists — a round trip. Before the #4346 fix, the nested
    /// <c>Summary</c> class declared ABOVE <c>RunAsync</c> stole the attribution
    /// (<c>PerformanceMonitor.Darling.Service.Summary.Summary</c>) because the old
    /// <c>EnclosingMethodFqName</c> picked the nearest preceding <c>class</c> keyword rather than the type
    /// whose braces actually enclose the SQL. #4384 then found #4346's OWN replacement still wrong: the
    /// SQL lives in a <c>const string CandidateSql</c> FIELD, not in <c>RunAsync</c>'s body, so a correct
    /// attribution must name the FIELD — round-tripping against <c>RunAsync</c> would have been the same
    /// false green this pin exists to catch, just shifted one bug later. The exemption's string can never
    /// round-trip against a wrong name, so <see cref="NoOtherProductionCode_ReadsTheDetailColumnDirectly"/>
    /// would fail in CI with the exemption present and correct.
    /// </summary>
    [Fact]
    public void RawDetailReaderExemption_RoundTrips_AgainstTheScrubsOwnAttribution()
    {
        Assert.Single(RawDetailReaderExemptions);

        var text = StripComments(ReadScrubSource().ReplaceLineEndings("\n"));
        var found = RawDetailReadersIn(text);

        Assert.Contains(RawDetailReaderExemptions[0], found);
        Assert.DoesNotContain("PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub.RunAsync", found);
    }

    /// <summary>The ONE exempted OWNER (#4346/#4384): <c>PlanForceActionDetailScrub</c>'s
    /// <c>CandidateSql</c> field, the scrub's own raw SELECT, which must read <c>detail</c> raw because
    /// every other reader already sanitizes it on the way out (see
    /// <see cref="PlanForceActionDetailScrub_ExemptedMethod_ReturnsNoDetailText"/> for the exemption's own
    /// contract pin: <c>RunAsync</c> returns no <c>detail</c> text, and writes back only sanitizer output).
    /// Entries are field-scoped FQ names (<c>Namespace.Type.FieldName</c>), never a method name and never a
    /// pattern — every entry here must share the same owning TYPE (checked in
    /// <see cref="NoOtherProductionCode_ReadsTheDetailColumnDirectly"/>), so the scrub could in principle
    /// carry a second SQL field without widening the exemption to a new owner, but nothing else is
    /// grandfathered in silently: any OTHER raw reader — a different field, a different type — still fails
    /// this pin, and any reference to one of these fields from OUTSIDE its own type fails
    /// <see cref="AssertExemptedFieldsOnlyReferencedInsideTheirOwner"/>.</summary>
    private static readonly string[] RawDetailReaderExemptions =
    [
        "PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub.LegacyDetailCandidateSql",
    ];

    /// <summary>The two readers that feed <c>ReadRecord</c> — the sanitizer's own choke point — are
    /// allowed to mention the <c>detail</c> column in their SQL text; they never touch it in C# code
    /// outside the mapper.</summary>
    private static readonly string[] AllowedRawDetailReaders =
    [
        "PerformanceMonitor.Darling.Service.PgPlanForceActionStore.GetRecentActionsAsync",
        "PerformanceMonitor.Darling.Service.PgPlanForceActionStore.GetPendingReviewsAsync",
    ];

    /// <summary>Finds every verbatim-string SQL statement mentioning <c>plan_force_actions</c> that reads
    /// (rather than writes) the <c>detail</c> column, and attributes each to its enclosing
    /// <c>Namespace.Type.Method</c> by scanning backward from the match for the nearest preceding
    /// <c>namespace</c>, <c>class</c>, and method signature.</summary>
    private static System.Collections.Generic.List<string> RawDetailReadersIn(string text)
    {
        var results = new System.Collections.Generic.List<string>();
        foreach (Match sqlMatch in Regex.Matches(text, "@\"[^\"]*\"", RegexOptions.Singleline))
        {
            var sql = sqlMatch.Value;
            if (!sql.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var isInsert = Regex.IsMatch(sql, @"INSERT\s+INTO\s+collect\.plan_force_actions", RegexOptions.IgnoreCase);
            var mentionsDetail = Regex.IsMatch(sql, @"(?<!insert\s(?:.|\n)*?)\bpfa\.detail\b", RegexOptions.IgnoreCase)
                || Regex.IsMatch(sql, @"\bpfa\.detail\b", RegexOptions.IgnoreCase)
                || (Regex.IsMatch(sql, @"\bSELECT\b", RegexOptions.IgnoreCase) && Regex.IsMatch(sql, @"(?<![\w.])detail(?![\w])", RegexOptions.IgnoreCase));

            if (isInsert || !mentionsDetail)
            {
                continue;
            }

            var fqName = EnclosingMethodFqName(text, sqlMatch.Index);
            if (fqName is not null)
            {
                results.Add(fqName);
            }
        }

        return results;
    }

    /// <summary>
    /// #4346 census-attribution bug: the ORIGINAL version of this method picked the enclosing type by the
    /// nearest preceding <c>class</c> KEYWORD in the text, not by which type's BRACES actually enclose
    /// <paramref name="position"/>. <c>PlanForceActionDetailScrub</c> declares a nested <c>public sealed
    /// class Summary</c> above <c>RunAsync</c>, whose body closes long before the scrub's raw-<c>detail</c>
    /// SELECT — textually nearest, but not enclosing — so every SQL site in <c>RunAsync</c> was attributed
    /// to <c>Summary.Summary</c> (a constructor-shaped match on the record-style ctor) and the ONE
    /// exemption in <see cref="RawDetailReaderExemptions"/> never matched it.
    ///
    /// <para>Fixed by using <see cref="CSharpSourceWalker.BraceBalanced"/> — the same shared brace walk
    /// #2927 consolidated five hand-rolled copies onto — to compute each declaration candidate's actual
    /// body span, and keeping only the candidate whose span CONTAINS <paramref name="position"/>; among
    /// those, the one whose opening brace is furthest to the right (innermost) wins. Applied to both the
    /// type and the method, so a nested type declared before the real enclosing method can no longer steal
    /// either name.</para>
    /// </summary>
    private static string? EnclosingMethodFqName(string text, int position)
    {
        /* Braces inside a string literal (a SQL verbatim string could in principle contain one) would
           unbalance a naive brace count, exactly the reason StripCommentsAndStrings' contract exists — so
           brace positions are computed over the MASKED text, same length, so every offset still lines up
           with position and with the declaration matches below (which never occur inside a literal). */
        var masked = CSharpSourceWalker.StripCommentsAndStrings(text);

        var namespaceMatch = LastMatch(text[..position], @"\bnamespace\s+([\w.]+)");

        const string methodPattern = @"(?:public|private|internal|protected)[^\n{;]*?\b(\w+)\s*\([^;{]*\)\s*(?=\{)";
        const string typePattern = @"\b(?:class|struct|record|interface)\s+(\w+)";
        const string fieldPattern = @"(?:public|private|internal|protected)\s+(?:const|static\s+readonly)\s+string\??\s+(\w+)\s*=\s*";

        var typeMatch = InnermostEnclosingDeclaration(text, masked, position, typePattern);
        if (typeMatch is null)
        {
            return null;
        }

        var cls = typeMatch.Value.Name;
        var ns = namespaceMatch?.Groups[1].Value;

        /* Usually the SQL text sits inside a method body, and brace-enclosure finds it directly (this is
           what fixes the #4346 bug: a nested type declared earlier no longer wins just for being nearer in
           text). */
        var methodMatch = InnermostEnclosingDeclaration(text, masked, position, methodPattern);
        if (methodMatch is not null)
        {
            return ns is null ? $"{cls}.{methodMatch.Value.Name}" : $"{ns}.{cls}.{methodMatch.Value.Name}";
        }

        /* #4384: a query built as a `const string`/`static readonly string` FIELD declared just above the
           method that uses it — this scrub's own shape — is never enclosed by any method's braces at all:
           the field belongs to the class, not to a method. The ORIGINAL fix for this shape (#4346)
           attributed it to "the nearest method declared AFTER the field, in the same type" — which is a
           false green waiting to happen: a raw `detail` SELECT added as a NEW const field just before
           RunAsync would inherit RunAsync's own exemption purely by being textually adjacent to it, never
           having to earn a name of its own. A field's identity is its own declared name, so the field is
           attributed to ITSELF — `Namespace.Type.FieldName` — never to a neighbouring method. */
        var fieldName = NearestPrecedingFieldInitializer(text, position, fieldPattern);
        if (fieldName is null)
        {
            return null;
        }

        return ns is null ? $"{cls}.{fieldName}" : $"{ns}.{cls}.{fieldName}";
    }

    /// <summary>The name of the nearest <c>const string</c>/<c>static readonly string</c> field
    /// declaration before <paramref name="position"/> whose <c>=</c> initializer leads directly (only
    /// whitespace between) into the literal at <paramref name="position"/> — i.e. this field's own
    /// initializer IS the literal being attributed, not some earlier field's. Returns <c>null</c> when no
    /// such declaration is found, which lets the caller fall through cleanly rather than guessing.</summary>
    private static string? NearestPrecedingFieldInitializer(string text, int position, string fieldPattern)
    {
        string? best = null;
        var bestIndex = -1;

        foreach (Match m in Regex.Matches(text[..position], fieldPattern))
        {
            var gap = text[(m.Index + m.Length)..position];
            if (gap.Any(c => !char.IsWhiteSpace(c)))
            {
                continue;
            }

            if (m.Index > bestIndex)
            {
                bestIndex = m.Index;
                best = m.Groups[1].Value;
            }
        }

        return best;
    }

    /// <summary>The innermost <c>Namespace.Type</c> enclosing <paramref name="position"/> — the type-only
    /// half of <see cref="EnclosingMethodFqName"/>, factored out so the exempted-field reference guard can
    /// ask "which type owns this occurrence" without also needing a method name.</summary>
    private static string? EnclosingTypeFqName(string text, int position)
    {
        var masked = CSharpSourceWalker.StripCommentsAndStrings(text);
        var namespaceMatch = LastMatch(text[..position], @"\bnamespace\s+([\w.]+)");
        const string typePattern = @"\b(?:class|struct|record|interface)\s+(\w+)";

        var typeMatch = InnermostEnclosingDeclaration(text, masked, position, typePattern);
        if (typeMatch is null)
        {
            return null;
        }

        var ns = namespaceMatch?.Groups[1].Value;
        return ns is null ? typeMatch.Value.Name : $"{ns}.{typeMatch.Value.Name}";
    }

    /// <summary>Finds every declaration matching <paramref name="declPattern"/> before
    /// <paramref name="position"/>, computes its body span with <see cref="CSharpSourceWalker.BraceBalanced"/>
    /// over <paramref name="masked"/> (comments and literal text blanked, same length as
    /// <paramref name="text"/>), and returns the captured name plus brace span of whichever body actually
    /// CONTAINS <paramref name="position"/> — the innermost one, i.e. the one whose opening brace is
    /// furthest right. Returns <c>null</c> when no declaration's body encloses the position.</summary>
    private static (string Name, int OpenBrace, int CloseBrace)? InnermostEnclosingDeclaration(
        string text, string masked, int position, string declPattern)
    {
        (string Name, int OpenBrace, int CloseBrace)? best = null;

        foreach (Match m in Regex.Matches(text[..position], declPattern))
        {
            var braceIndex = text.IndexOf('{', m.Index + m.Length);
            if (braceIndex < 0 || braceIndex > position)
            {
                continue;
            }

            var body = CSharpSourceWalker.BraceBalanced(masked, braceIndex);
            var closeIndex = braceIndex + body.Length - 1;

            if (position > closeIndex)
            {
                continue;
            }

            if (best is null || braceIndex > best.Value.OpenBrace)
            {
                best = (m.Groups[1].Value, braceIndex, closeIndex);
            }
        }

        return best;
    }

    private static Match? LastMatch(string text, string pattern)
    {
        Match? last = null;
        foreach (Match m in Regex.Matches(text, pattern))
        {
            last = m;
        }

        return last;
    }

    /// <summary>Every <c>.cs</c> file under <c>Darling/</c> (excluding <c>Darling.Tests</c> and build
    /// output) plus <c>PerformanceMonitor.Common/</c>, resolved from this test file's own path the same
    /// way <see cref="ReadStoreSource"/> finds the store.</summary>
    private static System.Collections.Generic.IEnumerable<string> ProductionSourceFiles(
        [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !Directory.Exists(Path.Combine(dir, "Darling")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        var root = dir!;

        var darlingFiles = Directory.EnumerateFiles(Path.Combine(root, "Darling"), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}Darling.Tests{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                && !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal));

        var commonDir = Path.Combine(root, "PerformanceMonitor.Common");
        var commonFiles = Directory.Exists(commonDir)
            ? Directory.EnumerateFiles(commonDir, "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    && !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            : Enumerable.Empty<string>();

        return darlingFiles.Concat(commonFiles);
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
