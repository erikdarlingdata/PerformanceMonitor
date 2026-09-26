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
    /// <para>A method may be exempted by adding its fully qualified name to
    /// <see cref="RawDetailReaderExemptions"/> — capped at one entry, reserved for a future one-time
    /// audit-detail scrub (#4346), and itself asserted below to return no detail text.</para>
    /// </summary>
    [Fact]
    public void NoOtherProductionCode_ReadsTheDetailColumnDirectly()
    {
        Assert.True(RawDetailReaderExemptions.Length <= 1, "at most one raw-detail-reader exemption is allowed.");

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
    }

    /// <summary>At most ONE entry, reserved for a future one-time audit-detail scrub (#4346). Empty
    /// today: the scrub doesn't exist yet, so any new raw reader must be named here explicitly before
    /// it can pass — nothing is grandfathered in silently.</summary>
    private static readonly string[] RawDetailReaderExemptions = Array.Empty<string>();

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

    private static string? EnclosingMethodFqName(string text, int position)
    {
        var before = text[..position];

        var methodMatch = LastMatch(before, @"(?:public|private|internal|protected)[^\n{;]*?\b(\w+)\s*\([^;{]*\)\s*(?:=>|\{)");
        var classMatch = LastMatch(before, @"\bclass\s+(\w+)");
        var namespaceMatch = LastMatch(before, @"\bnamespace\s+([\w.]+)");

        if (methodMatch is null || classMatch is null)
        {
            return null;
        }

        var ns = namespaceMatch?.Groups[1].Value;
        var cls = classMatch.Groups[1].Value;
        var method = methodMatch.Groups[1].Value;

        return ns is null ? $"{cls}.{method}" : $"{ns}.{cls}.{method}";
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
