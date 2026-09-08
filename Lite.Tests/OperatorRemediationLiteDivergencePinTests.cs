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
using PerformanceMonitor.Analysis;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2138 phase 1 lands in Darling only, and this pin exists so that stays a DECISION rather than
/// something the next lane inherits.
///
/// <para><b>Why it is not in Lite, and no longer provisionally.</b> Settled on #2138: remediation
/// execution is Darling-only, permanently. The rule was already stated in Lite's own code, but with two
/// different reasons, and only one of them survived — which is exactly the kind of thing this pin exists
/// to keep honest. The obsolete half named the deprecated Dashboard SKU as the owner, so it pointed at
/// nothing live. The half that decides it is Lite's STORE: a DuckDB file is per-workstation, so a
/// remediation journal kept there cannot be the shared audit trail such a journal exists to be — two
/// operators acting on one server would each hold half of it and neither could see the other's forces —
/// and it cannot carry the bot's own-forces-only invariant, which is a predicate plus a self-reference
/// within ONE shared store. No deprecation can invalidate that, which is why the wording now rests on it.</para>
///
/// <para>The shared pure seam (<see cref="OperatorRemediationGate"/> / <see cref="OperatorRemediationFlow"/>)
/// still lives in <c>PerformanceMonitor.Analysis</c>, which Lite already references — so the divergence is
/// about which SKU may ACT, never about duplicated logic.</para>
///
/// <para><b>What this pin does.</b> It asserts the divergence is COMPLETE and its stated reason still
/// exists. Both halves matter, and they fail in opposite directions: if Lite gains a remediation
/// credential or journal without the scope decision being revisited, the first assertions fail; if the
/// scope statements this divergence rests on are deleted or reworded away, the last one fails, and this
/// file's whole justification has to be rewritten rather than quietly outliving its premise.</para>
///
/// <para>It is deliberately NOT a claim that Lite should never act. It is a claim that today it does not,
/// on a stated basis, and that changing either fact is a visible edit here.</para>
/// </summary>
public sealed class OperatorRemediationLiteDivergencePinTests
{
    /// <summary>
    /// The shared seam is reachable from Lite (it is in an assembly Lite references) but unused by it —
    /// so the divergence is "Lite does not act", not "Lite could not". Asserted by resolving the types
    /// from the running Lite test assembly's reference graph, which is a stronger statement than a source
    /// grep: it proves the reference really exists rather than that a string appears.
    /// </summary>
    [Fact]
    public void TheSharedSeamIsReachableFromLite_SoNeitherSkuWouldNeedItsOwnCopy()
    {
        Assert.NotNull(typeof(OperatorRemediationGate));
        Assert.NotNull(typeof(OperatorRemediationFlow));

        /* And it is the SAME assembly the Darling side consults — one policy path, per the #2146
           contract. If these ever differ, "the bot executes exactly the verdict object agents inspect"
           has stopped being true at the assembly level. */
        Assert.Equal(
            typeof(FactRemediation).Assembly.GetName().Name,
            typeof(OperatorRemediationGate).Assembly.GetName().Name);
    }

    /// <summary>
    /// The tokens that can only appear under <c>Lite/</c> if Lite gained the phase-1 capability — a
    /// credential slot, the shared gate being USED rather than merely referenceable, the executor seam,
    /// the journal, or a force/evict statement.
    ///
    /// <para><b>Not a scan for the word "remediation".</b> That was the first version of this test and it
    /// found 12 files, every one of them correct: Lite renders <c>remediation_action_json</c> as
    /// copy-paste advice, which is a first-class Lite feature and the very thing the advise-only stance
    /// describes. A scan that fires on the feature working is one that gets an exclusion list bolted onto
    /// it until it means nothing. These tokens name the CAPABILITY instead, so the scan and the stance
    /// agree: rendering advice is expected, executing it is the thing that is absent.</para>
    /// </summary>
    [Fact]
    public void LiteHasNoRemediationCredential_NoExecutorSeam_AndNoJournal()
    {
        var capabilityTokens = new[]
        {
            /* the credential */
            "RemediationUsername", "RemediationEncryptedPassword",
            "remediation_username", "remediation_encrypted_password",
            /* the shared seam, used rather than referenceable */
            nameof(OperatorRemediationGate), nameof(OperatorRemediationFlow),
            /* the write seam, the journal, and the policy */
            "IPlanForceExecutor", "PlanForceActionRecord", "plan_force_actions", "ForcePlanBotPolicy",
            /* the statements themselves */
            "sp_query_store_force_plan", "sp_query_store_unforce_plan", "FREEPROCCACHE",
        };

        var files = LiteSourceFiles();
        var offenders =
            (from path in files
             let text = File.ReadAllText(path)
             from token in capabilityTokens
             where text.Contains(token, StringComparison.Ordinal)
             select $"{RelativeToRepo(path)} ({token})").ToList();

        Assert.True(
            offenders.Count == 0,
            "Lite gained a #2138 phase-1 capability. Phase 1 is Darling-only because Lite states it has "
            + "no privileged remediation path (see this class's remarks); if that scope decision has been "
            + "revisited, delete this pin in the same change rather than adding an exclusion:\n  "
            + string.Join("\n  ", offenders));

        /* Two controls, because the assertion above is an absence and an absence has two ways to be
           vacuous. First: the enumeration really read Lite — Lite is a 260-odd-file app, so a floor well
           below that catches a scan that returned nothing. */
        Assert.True(files.Count >= 100, $"the Lite source enumeration returned {files.Count} files");

        /* Second: the scan can actually fire. Run the same token list against a Darling file that DOES
           carry the capability — if this finds nothing, the matcher is broken and the absence above
           proves nothing about Lite. */
        var darlingJournal = ParitySource.ReadFile(
            "Darling/PerformanceMonitor.Darling.Service/PgPlanForceActionStore.cs");
        Assert.Contains(
            capabilityTokens,
            token => darlingJournal.Contains(token, StringComparison.Ordinal));
    }

    /// <summary>
    /// Lite's DuckDB schema carries no plan-force journal TABLE. This is the half that matters most: a
    /// journal added to Lite while Lite cannot act would be a permanently-empty table, which reads to
    /// every later consumer as "the feature is here and nothing has happened" rather than "the feature is
    /// not here".
    ///
    /// <para><b>Asserted on table NAMES, not on substrings of the DDL</b> — and this is the second time
    /// this file's first instinct was a scan too broad to survive its own subject. The DDL genuinely
    /// contains <c>plan_force</c>: <c>plan_correction</c> carries a generated
    /// <c>last_good_plan_force_failure_reason</c> column, which is the MONITORED SERVER'S own Query Store
    /// forcing-failure reason — the opposite of a bot's audit trail of its own writes, and a column Lite
    /// has read for a long time. A statement-substring scan reported that as a journal, which is a scan
    /// that has to be relaxed to ship. The claim is about a table existing, so the test asks about
    /// tables.</para>
    /// </summary>
    [Fact]
    public void LitesSchemaHasNoPlanForceJournalTable()
    {
        var tableNames = PerformanceMonitorLite.Database.Schema.GetAllTableStatements()
            .Select(TableNameOf)
            .Where(name => name.Length > 0)
            .ToList();

        /* Two positive controls, because the assertion below is an absence. The enumeration produced a
           real schema... */
        Assert.Contains("servers", tableNames);

        /* ...and the NAME EXTRACTION works, rather than silently returning empty strings that would make
           every absence assertion vacuous. plan_correction is the table whose COLUMN caused this test's
           first version to fail, so its presence here is also the proof that the new form discriminates
           the column from a table. */
        Assert.Contains("plan_correction", tableNames);
        Assert.True(tableNames.Count >= 40, $"only {tableNames.Count} table names parsed out of the schema");

        var journals = tableNames
            .Where(name =>
                name.Contains("plan_force", StringComparison.OrdinalIgnoreCase) ||
                name.Contains("remediation", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.True(
            journals.Count == 0,
            "Lite's schema gained a plan-force/remediation journal table: " + string.Join(", ", journals));
    }

    /// <summary>
    /// The table name out of a <c>CREATE TABLE [IF NOT EXISTS] name (…)</c>, or empty when the statement
    /// is not one. Empty rather than throwing so a future non-CREATE statement in the list does not fail
    /// this test for an unrelated reason — the count control above is what stops an all-empty parse
    /// reading as agreement.
    /// </summary>
    private static string TableNameOf(string statement)
    {
        var match = System.Text.RegularExpressions.Regex.Match(
            statement,
            @"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?<name>[A-Za-z0-9_]+)",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase
                | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

        return match.Success ? match.Groups["name"].Value : string.Empty;
    }

    /// <summary>
    /// The stated basis for the divergence still exists in Lite's own source. Without this, the pin above
    /// could outlive its reason: someone could delete the advise-only remarks, and the divergence would
    /// carry on being enforced by a test whose justification had evaporated. Matched on the load-bearing
    /// phrases rather than whole paragraphs so a rewording that keeps the decision keeps the pin.
    ///
    /// <para>The RULE and the REASON are asserted separately, because the wording fix on #2138 is the
    /// case that showed why: the rule never changed and one of its two reasons died, so a test that only
    /// checked the rule would have gone on passing over a justification that named a deprecated SKU as
    /// the owner of a capability. Asserting the reason too is what made this pin redden and get updated
    /// rather than quietly outlive its premise.</para>
    /// </summary>
    [Fact]
    public void TheAdviseOnlyStatementsAndTheirDurableReason_StillExist()
    {
        var item = ParitySource.ReadFile("Lite/Analysis/Recommendations/LiteRecommendationItem.cs");
        var card = ParitySource.ReadFile("Lite/Analysis/Recommendations/LiteRecommendationsViewModel.cs");
        var tab = ParitySource.ReadFile("Lite/Controls/RecommendationsTab.xaml.cs");

        /* The rule. Unchanged by the #2138 wording fix, and still the thing an operator relies on. */
        Assert.Contains("NO in-app Apply/execute path", item, StringComparison.Ordinal);
        Assert.Contains("Lite is ADVISE-ONLY", card, StringComparison.Ordinal);
        Assert.Contains("NO privileged remediation execution", card, StringComparison.Ordinal);
        Assert.Contains("NO Apply button", tab, StringComparison.Ordinal);

        /* The reason, in all three places that state the rule — the store argument, which a deprecation
           cannot reach. Matched on "per-workstation" because that is the load-bearing word: it is what
           makes a journal there unable to be a shared audit trail. */
        foreach (var (file, text) in new[]
                 {
                     ("LiteRecommendationItem.cs", item),
                     ("LiteRecommendationsViewModel.cs", card),
                     ("RecommendationsTab.xaml.cs", tab),
                 })
        {
            Assert.True(
                text.Contains("per-workstation", StringComparison.Ordinal),
                $"{file} states the advise-only rule without the store reason it now rests on. If the "
                + "reason has moved, move this assertion with it — do not drop it, or the rule outlives "
                + "its justification again (#2138).");
        }

        /* And the DEAD reason is gone from every one of them: it named the deprecated Dashboard SKU as
           the owner of remediation execution, which is a claim about a live division of responsibility
           that no longer exists. Reinstating it would be a regression to a justification that points
           nowhere. */
        foreach (var (file, text) in new[]
                 {
                     ("LiteRecommendationItem.cs", item),
                     ("LiteRecommendationsViewModel.cs", card),
                     ("RecommendationsTab.xaml.cs", tab),
                 })
        {
            Assert.DoesNotContain("Dashboard-only", text, StringComparison.Ordinal);
            Assert.DoesNotContain("per project scope", text, StringComparison.Ordinal);
        }

        /* Positive control for those two absences: the same read DOES find the Dashboard named in these
           files as a UI COMPARISON, which is deliberately left alone — its reason is the surface's tab
           shape, not a scope claim, so the deprecation does not touch it. Without this control the
           assertions above would pass just as well against a failed read. */
        Assert.Contains(
            "the Dashboard) no \"Open in Active Queries\" deep-link", card, StringComparison.Ordinal);
        Assert.Contains("Dashboard's \"Open in Active Queries\" deep-link is omitted", tab, StringComparison.Ordinal);
    }

    private static System.Collections.Generic.List<string> LiteSourceFiles() =>
        Directory.EnumerateFiles(Path.Combine(ParitySource.RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories)
            .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .ToList();

    private static string RelativeToRepo(string path) =>
        Path.GetRelativePath(ParitySource.RepoRoot(), path).Replace('\\', '/');
}
