/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4346/#4376/#4384: an exact per-file census of every place production C# source builds a string literal
/// naming BOTH <c>plan_force_actions</c> and the whole word <c>detail</c>. Replaces the #4346/#4376
/// enclosing-method-attribution scan (<c>PlanForceActionAuditRedactionTests.NoOtherProductionCode_
/// ReadsTheDetailColumnDirectly</c>, deleted by #4384) with a simpler, exact count: no attribution, no
/// per-caller allow-list, just "how many literals in this file say both words" against a hard-coded map
/// that must equal today's real count in every file. A new site anywhere fails the map; the exemption list
/// in (c) below documents WHY the scrub's own site is allowed to exist, not that it is invisible to the
/// census.
/// </summary>
public sealed class PlanForceActionDetailCensusTests
{
    private const string StoreFileName = "PgPlanForceActionStore.cs";
    private const string ScrubFileName = "PlanForceActionDetailScrub.cs";
    private const string MigrationsFileName = "PgMigrations.cs";

    /// <summary>The ONE exempted type (#4346/#4377/#4384): <c>PlanForceActionDetailScrub</c> reads
    /// <c>detail</c> raw because it is the one-time scrub that has to find pre-#4326 rows every other
    /// reader now sanitizes on the way out (see the type's own remarks). At most one entry — a second
    /// exempted type would need a fresh decision, not a silent second line here.</summary>
    private static readonly string[] RawDetailReaderExemptions =
    [
        "PerformanceMonitor.Darling.Service.PlanForceActionDetailScrub",
    ];

    [Fact]
    public void ExemptionList_HasAtMostOneEntry()
    {
        Assert.True(RawDetailReaderExemptions.Length <= 1, "at most one exempted type is allowed.");
    }

    /* ---------------------------------------------------------------------------------------------------
     * (a) Controls over synthetic sources.
     * --------------------------------------------------------------------------------------------------- */

    [Fact]
    public void Control_RegularString_Counts1()
    {
        const string source =
            "class C { void M() { var sql = \"SELECT detail FROM collect.plan_force_actions\"; } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_VerbatimString_Counts1()
    {
        const string source =
            "class C { void M() { var sql = @\"SELECT detail FROM collect.plan_force_actions\"; } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_RawString_Counts1()
    {
        const string source =
            "class C { void M() { var sql = \"\"\"\n" +
            "    SELECT detail FROM collect.plan_force_actions\n" +
            "    \"\"\"; } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_InterpolatedString_Counts1()
    {
        const string source =
            "class C { void M(string c) { var sql = $\"SELECT {c} FROM collect.plan_force_actions " +
            "WHERE detail IS NULL\"; } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_PlusSplitAcrossTwoLiterals_Counts1()
    {
        const string source =
            "class C { void M() { var sql = \"SELECT detail FROM \" + \"collect.plan_force_actions\"; } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_ExpressionBodiedProperty_Counts1()
    {
        const string source =
            "class C { string Sql => \"SELECT detail FROM collect.plan_force_actions\"; }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_BuildArgument_Counts1()
    {
        const string source =
            "class C { void M() { Build(@\"SELECT detail FROM collect.plan_force_actions\"); } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_InstanceReadonlyField_Counts1()
    {
        const string source =
            "class C { readonly string _sql = \"SELECT detail FROM collect.plan_force_actions\"; }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_FieldWithNoAccessModifier_Counts1()
    {
        const string source =
            "class C { string sql = \"SELECT detail FROM collect.plan_force_actions\"; }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_CommentOnlyMention_Counts0()
    {
        const string source =
            "class C {\n" +
            "    // SELECT detail FROM collect.plan_force_actions is what the OLD code used to do\n" +
            "    void M() { }\n" +
            "}";
        Assert.Equal(0, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    [Fact]
    public void Control_OnlyOneOfTheTwoWords_Counts0()
    {
        const string source =
            "class C { void M() { var sql = \"SELECT detail FROM collect.some_other_table\"; " +
            "var other = \"SELECT server_name FROM collect.plan_force_actions\"; } }";
        Assert.Equal(0, PlanForceActionDetailCensus.CountDetailSites(source));
    }

    /* ---------------------------------------------------------------------------------------------------
     * (b) The real-tree census: an exact hard-coded map, everything else must be zero.
     * --------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// Today's real per-file count, computed and hard-coded (#4384). Sites, one line each:
    ///
    /// <para><b><see cref="StoreFileName"/> = 3:</b>
    /// (1) <c>JournalAsync</c>'s INSERT — the column list names <c>detail</c>;
    /// (2) <c>GetPendingReviewsAsync</c>'s SELECT — the audit-review read, <c>pfa.detail</c> in the list;
    /// (3) <c>GetRecentActionsAsync</c>'s SELECT — the other <c>ReadRecord</c> caller, same column list.</para>
    ///
    /// <para><b><see cref="ScrubFileName"/> = 2:</b>
    /// (1) <c>LegacyDetailCandidateSql</c> — the coarse candidate-row filter, the one exempted raw read;
    /// (2) <c>BatchUpdateSql</c> — the write-back, <c>SET detail = ...</c> against the same table.</para>
    ///
    /// <para><b><see cref="MigrationsFileName"/> = 1:</b>
    /// V107's DDL literal — the <c>CREATE TABLE collect.plan_force_actions</c> body declares a <c>detail
    /// text</c> column, so the same literal names both words; this is schema DDL, never a read, and needs
    /// no exemption entry (only READS are exempted here), but the census counts literals, not readers.</para>
    /// </summary>
    private static readonly Dictionary<string, int> ExpectedCountsByFileName = new()
    {
        [StoreFileName] = 3,
        [ScrubFileName] = 2,
        [MigrationsFileName] = 1,
    };

    [Fact]
    public void RealTree_MatchesExactPerFileCounts()
    {
        var unexpected = new List<string>();
        var actualByFile = new Dictionary<string, int>();

        foreach (var file in ProductionSourceFiles())
        {
            var text = File.ReadAllText(file).ReplaceLineEndings("\n");
            var count = PlanForceActionDetailCensus.CountDetailSites(text);
            var fileName = Path.GetFileName(file);

            if (count == 0)
            {
                continue;
            }

            actualByFile[fileName] = actualByFile.GetValueOrDefault(fileName) + count;

            if (!ExpectedCountsByFileName.ContainsKey(fileName))
            {
                var sites = PlanForceActionDetailCensus.JoinedStringLiterals(text)
                    .Where(l => l.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase)
                        && Regex.IsMatch(l, @"\bdetail\b", RegexOptions.IgnoreCase))
                    .Select(Truncate);
                unexpected.Add($"{fileName}: {string.Join(" | ", sites)}");
            }
        }

        Assert.True(
            unexpected.Count == 0,
            "unexpected file(s) with a plan_force_actions+detail site not in the hard-coded map: "
            + string.Join("; ", unexpected));

        foreach (var (fileName, expected) in ExpectedCountsByFileName)
        {
            Assert.True(
                actualByFile.TryGetValue(fileName, out var actual) && actual == expected,
                $"{fileName}: expected {expected} site(s), found {actualByFile.GetValueOrDefault(fileName)}.");
        }
    }

    private static string Truncate(string text)
    {
        var oneLine = text.Replace("\n", "\\n", StringComparison.Ordinal);
        return oneLine.Length > 120 ? oneLine[..120] + "..." : oneLine;
    }

    /* ---------------------------------------------------------------------------------------------------
     * (c) The exemption list names only the store file plus the exempted type's own file.
     * --------------------------------------------------------------------------------------------------- */

    [Fact]
    public void ExemptionList_NamesOnlyTheStoreFileAndTheExemptedType()
    {
        var exemptedFileNames = RawDetailReaderExemptions
            .Select(t => t[(t.LastIndexOf('.') + 1)..] + ".cs")
            .ToArray();

        Assert.Equal(new[] { ScrubFileName }, exemptedFileNames);

        var mapFileNames = ExpectedCountsByFileName.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();
        var expectedFileNames = new[] { MigrationsFileName, ScrubFileName, StoreFileName }
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(expectedFileNames, mapFileNames);
    }

    /* ---------------------------------------------------------------------------------------------------
     * (d) M1: LegacyDetailCandidateSql is declared once and used exactly once.
     * --------------------------------------------------------------------------------------------------- */

    [Fact]
    public void M1_LegacyDetailCandidateSql_DeclaredOnceUsedOnce()
    {
        var scrubFile = ProductionSourceFiles()
            .Single(f => Path.GetFileName(f) == ScrubFileName);
        var text = File.ReadAllText(scrubFile).ReplaceLineEndings("\n");

        Assert.Equal(2, PlanForceActionDetailCensus.CountIdentifierUses(text, "LegacyDetailCandidateSql"));
    }

    [Fact]
    public void M1_NoProductionCode_ReferencesLegacyDetailCandidateSqlByReflectionOrNameof()
    {
        var violations = new List<string>();

        foreach (var file in ProductionSourceFiles())
        {
            var raw = File.ReadAllText(file).ReplaceLineEndings("\n");

            if (raw.Contains("GetField(\"LegacyDetailCandidateSql", StringComparison.Ordinal)
                || raw.Contains("nameof(LegacyDetailCandidateSql", StringComparison.Ordinal))
            {
                violations.Add(Path.GetFileName(file));
            }
        }

        Assert.True(
            violations.Count == 0,
            "production code reaches LegacyDetailCandidateSql by reflection/nameof, bypassing the "
            + "exemption's own owner: " + string.Join(", ", violations));
    }

    /// <summary>Every <c>.cs</c> file under <c>Darling/</c> (excluding <c>Darling.Tests</c>, <c>bin</c> and
    /// <c>obj</c>) plus <c>PerformanceMonitor.Common/</c>, resolved from <see cref="RepoFile.Root"/>.</summary>
    private static IEnumerable<string> ProductionSourceFiles()
    {
        var root = RepoFile.Root;

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
}
