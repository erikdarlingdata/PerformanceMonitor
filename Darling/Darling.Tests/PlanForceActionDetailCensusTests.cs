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
/// #4346: an exact per-file census of every place production C# source builds a string literal
/// naming BOTH <c>plan_force_actions</c> and the whole word <c>detail</c>. Replaces the #4346/#4376
/// enclosing-method-attribution scan (<c>PlanForceActionAuditRedactionTests.NoOtherProductionCode_
/// ReadsTheDetailColumnDirectly</c>, since deleted) with a simpler, exact count: no attribution, no
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

    /// <summary>The ONE exempted type (#4346): <c>PlanForceActionDetailScrub</c> reads
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

    /// <summary>Control: a <c>$$"""..."""</c> interpolated raw string's hole opens/closes with
    /// exactly two braces — both <see cref="PlanForceActionDetailCensus.CountDetailSites"/> and
    /// <see cref="PlanForceActionDetailCensus.CountTableMentions"/> must count the literal once, and the
    /// <c>{{x}}</c> hole must not be treated as literal text that could supply either word.</summary>
    [Fact]
    public void Control_DoubleDollarRawString_HoleIsNotText_Counts1()
    {
        const string source =
            "class C { void M(string x) { var sql = $$\"\"\"SELECT {{x}} FROM " +
            "collect.plan_force_actions WHERE detail IS NULL\"\"\"; } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
        Assert.Equal(1, PlanForceActionDetailCensus.CountTableMentions(source));
    }

    /// <summary>Control: in a <c>$$"""..."""</c> literal, a SINGLE brace is literal text, not a
    /// hole delimiter (a hole needs the full two-brace run) — the literal text "{ literal brace }" must
    /// still be read as ordinary characters, and the mention still counts once.</summary>
    [Fact]
    public void Control_DoubleDollarRawString_SingleBraceIsLiteralText_Counts1()
    {
        const string source =
            "class C { void M() { var sql = $$\"\"\"{ literal brace } plan_force_actions detail\"\"\"; } }";
        Assert.Equal(1, PlanForceActionDetailCensus.CountDetailSites(source));
        Assert.Equal(1, PlanForceActionDetailCensus.CountTableMentions(source));
    }

    /* ---------------------------------------------------------------------------------------------------
     * (b) The real-tree census: an exact hard-coded map, everything else must be zero.
     * --------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// Today's real per-file count, computed and hard-coded. Sites, one line each. Keyed by repo-relative
    /// path, the same shape as <see cref="ExpectedTableMentionCountsByPath"/> below — a bare file name is
    /// ambiguous once two files anywhere in the tree happen to share a name.
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
        ["Darling/PerformanceMonitor.Darling.Service/PgPlanForceActionStore.cs"] = 3,
        ["Darling/PerformanceMonitor.Darling.Service/PlanForceActionDetailScrub.cs"] = 2,
        ["Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs"] = 1,
    };

    [Fact]
    public void RealTree_MatchesExactPerFileCounts()
    {
        var root = RepoFile.Root;
        var unexpected = new List<string>();
        var actualByPath = new Dictionary<string, int>();

        foreach (var file in ProductionSourceFiles())
        {
            var text = File.ReadAllText(file).ReplaceLineEndings("\n");
            var count = PlanForceActionDetailCensus.CountDetailSites(text);
            var relativePath = Path.GetRelativePath(root, file).Replace('\\', '/');

            if (count == 0)
            {
                continue;
            }

            actualByPath[relativePath] = actualByPath.GetValueOrDefault(relativePath) + count;

            if (!ExpectedCountsByFileName.ContainsKey(relativePath))
            {
                var sites = PlanForceActionDetailCensus.JoinedStringLiterals(text)
                    .Where(l => l.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase)
                        && Regex.IsMatch(l, @"\bdetail\b", RegexOptions.IgnoreCase))
                    .Select(Truncate);
                unexpected.Add($"{relativePath}: {string.Join(" | ", sites)}");
            }
        }

        Assert.True(
            unexpected.Count == 0,
            "unexpected file(s) with a plan_force_actions+detail site not in the hard-coded map: "
            + string.Join("; ", unexpected));

        foreach (var (path, expected) in ExpectedCountsByFileName)
        {
            var actual = actualByPath.GetValueOrDefault(path);
            if (actual != expected)
            {
                var text = File.ReadAllText(Path.Combine(root, path)).ReplaceLineEndings("\n");
                var sites = PlanForceActionDetailCensus.JoinedStringLiterals(text)
                    .Where(l => l.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase)
                        && Regex.IsMatch(l, @"\bdetail\b", RegexOptions.IgnoreCase))
                    .Select(Truncate);
                Assert.Fail(
                    $"{path}: expected {expected} site(s), found {actual}. Literals: "
                    + string.Join(" | ", sites));
            }
        }
    }

    private static string Truncate(string text)
    {
        var oneLine = text.Replace("\n", "\\n", StringComparison.Ordinal);
        return oneLine.Length > 120 ? oneLine[..120] + "..." : oneLine;
    }

    /* ---------------------------------------------------------------------------------------------------
     * (b2) A second, exact per-file census : every literal naming the table AT ALL, with or
     * without "detail" alongside it. This is strictly wider than (b)'s co-occurrence count above, so it
     * also catches a table name held in its own const, a bare SELECT * against the table, or a "detail"
     * reference landing in a separate interpolation hole from the table name — none of which trip
     * CountDetailSites. Keyed by repo-relative path (a bare file name is ambiguous once two files anywhere
     * in the tree happen to share a name).
     * --------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// Today's real per-file table-mention count, computed and hard-coded. Sites, one line each:
    ///
    /// <para><b>Darling/PerformanceMonitor.Darling.Service/PgPlanForceActionStore.cs = 4:</b>
    /// the INSERT (<c>JournalAsync</c>), the last-action-time subquery, and the two SELECTs
    /// (<c>GetPendingReviewsAsync</c>, <c>GetRecentActionsAsync</c>) — the same three reads (b) counts,
    /// plus the subquery (b) does not (no "detail" word in that literal).</para>
    ///
    /// <para><b>Darling/PerformanceMonitor.Darling.Service/PlanForceActionDetailScrub.cs = 2:</b>
    /// same two sites as (b)'s <c>ScrubFileName</c> row: the candidate-row filter and the write-back.</para>
    ///
    /// <para><b>Darling/PerformanceMonitor.Darling.Service/DarlingRetention.cs = 2:</b>
    /// the retention table-name literal passed to the generic purge helper, and the
    /// <c>TimeSlicedDeleteSql("collect.plan_force_actions", "action_time")</c> call — neither mentions
    /// "detail", so (b) never counts this file at all.</para>
    ///
    /// <para><b>Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs = 1:</b>
    /// the huge migration-probe literal's V107 line, <c>table_name = 'plan_force_actions'</c> — a
    /// boot-time schema sentinel, not a data read, and never mentions "detail" either.</para>
    ///
    /// <para><b>Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs = 2:</b>
    /// V107's own <c>CREATE TABLE collect.plan_force_actions</c> DDL body (same literal (b) counts, since
    /// it also declares a <c>detail text</c> column), plus a SEPARATE later migration literal that starts
    /// with an unrelated <c>ALTER TABLE config.config_monitored_servers</c> but joins onward into more DDL
    /// naming <c>plan_force_actions</c> without the word "detail" appearing near it — a joined-literal
    /// artifact of how migrations concatenate adjacent rungs, not a second reader.</para>
    /// </summary>
    private static readonly Dictionary<string, int> ExpectedTableMentionCountsByPath = new()
    {
        ["Darling/PerformanceMonitor.Darling.Service/PgPlanForceActionStore.cs"] = 4,
        ["Darling/PerformanceMonitor.Darling.Service/PlanForceActionDetailScrub.cs"] = 2,
        ["Darling/PerformanceMonitor.Darling.Service/DarlingRetention.cs"] = 2,
        ["Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs"] = 1,
        ["Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs"] = 2,
    };

    [Fact]
    public void RealTree_MatchesExactTableMentionCountsPerFile()
    {
        var root = RepoFile.Root;
        var unexpected = new List<string>();
        var actualByPath = new Dictionary<string, int>();

        foreach (var file in ProductionSourceFiles())
        {
            var text = File.ReadAllText(file).ReplaceLineEndings("\n");
            var count = PlanForceActionDetailCensus.CountTableMentions(text);
            var relativePath = Path.GetRelativePath(root, file).Replace('\\', '/');

            if (count == 0)
            {
                continue;
            }

            actualByPath[relativePath] = actualByPath.GetValueOrDefault(relativePath) + count;

            if (!ExpectedTableMentionCountsByPath.ContainsKey(relativePath))
            {
                var sites = PlanForceActionDetailCensus.JoinedStringLiterals(text)
                    .Where(l => l.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase))
                    .Select(Truncate);
                unexpected.Add($"{relativePath}: {string.Join(" | ", sites)}");
            }
        }

        Assert.True(
            unexpected.Count == 0,
            "unexpected file(s) with a plan_force_actions mention not in the hard-coded map: "
            + string.Join("; ", unexpected));

        foreach (var (path, expected) in ExpectedTableMentionCountsByPath)
        {
            var actual = actualByPath.GetValueOrDefault(path);
            if (actual != expected)
            {
                var text = File.ReadAllText(Path.Combine(root, path)).ReplaceLineEndings("\n");
                var sites = PlanForceActionDetailCensus.JoinedStringLiterals(text)
                    .Where(l => l.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase))
                    .Select(Truncate);
                Assert.Fail(
                    $"{path}: expected {expected} mention(s), found {actual}. Literals: "
                    + string.Join(" | ", sites));
            }
        }
    }

    [Fact]
    public void CountTableMentions_ConstDeclarationOfTheTableLiteral_Counts()
    {
        const string source = "const string T = \"collect.plan_force_actions\";";
        Assert.Equal(1, PlanForceActionDetailCensus.CountTableMentions(source));
    }

    [Fact]
    public void CountTableMentions_BareSelectStarAgainstTheTable_Counts()
    {
        const string source = "\"SELECT * FROM collect.plan_force_actions\"";
        Assert.Equal(1, PlanForceActionDetailCensus.CountTableMentions(source));
    }

    [Fact]
    public void CountTableMentions_DetailInASeparateInterpolationHole_StillCounts()
    {
        const string source = "$\"SELECT {\"detail\"} FROM collect.plan_force_actions\"";
        Assert.Equal(1, PlanForceActionDetailCensus.CountTableMentions(source));
    }

    [Fact]
    public void CountTableMentions_CommentOnlyMention_DoesNotCount()
    {
        const string source = "// collect.plan_force_actions is the auto force-plan bot's journal";
        Assert.Equal(0, PlanForceActionDetailCensus.CountTableMentions(source));
    }

    /* ---------------------------------------------------------------------------------------------------
     * (c) The exemption list names exactly the files that back the map's non-store, non-migration
     * entries — i.e. every reader the co-occurrence map admits that isn't the store's own read or schema
     * DDL is accounted for by a documented exempted type, with nothing left over and nothing missing.
     * --------------------------------------------------------------------------------------------------- */

    /// <summary>Asserts that <see cref="ExpectedCountsByFileName"/>'s entries, MINUS the store file (its
    /// reads are the sanctioned, sanitizing path every OTHER reader goes through) and MINUS
    /// <see cref="MigrationsFileName"/> (schema DDL — a <c>CREATE TABLE ... detail text</c> literal is not
    /// a reader at all, so it needs no exemption entry), are EXACTLY the files of the types in
    /// <see cref="RawDetailReaderExemptions"/>. A file could appear in the map for reasons other than
    /// reading <c>detail</c> raw (the map counts literals, not readers) — this asserts that after removing
    /// the two known, non-exemption-worthy entries, what remains matches the exemption list one-for-one.
    /// </summary>
    [Fact]
    public void ExemptionList_NamesOnlyTheStoreFileAndTheExemptedType()
    {
        var exemptedFileNames = RawDetailReaderExemptions
            .Select(t => t[(t.LastIndexOf('.') + 1)..] + ".cs")
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(new[] { ScrubFileName }, exemptedFileNames);

        var nonStoreNonMigrationFileNames = ExpectedCountsByFileName.Keys
            .Select(Path.GetFileName)
            .Where(n => n != StoreFileName && n != MigrationsFileName)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(exemptedFileNames, nonStoreNonMigrationFileNames);
    }

    /* ---------------------------------------------------------------------------------------------------
     * (d) LegacyDetailCandidateSql is declared once and used exactly once.
     * --------------------------------------------------------------------------------------------------- */

    [Fact]
    public void LegacyDetailCandidateSql_DeclaredOnceUsedOnce()
    {
        var scrubFile = ProductionSourceFiles()
            .Single(f => Path.GetFileName(f) == ScrubFileName);
        var text = File.ReadAllText(scrubFile).ReplaceLineEndings("\n");

        Assert.Equal(2, PlanForceActionDetailCensus.CountIdentifierUses(text, "LegacyDetailCandidateSql"));
    }

    /// <summary>Control (#4346): a second use written as an interpolation HOLE
    /// (<c>$"{LegacyDetailCandidateSql}"</c>) must be counted — a hole is code, not literal text, so
    /// masking it away the way literal text is masked would make this use invisible.</summary>
    [Fact]
    public void Control_IdentifierUsedInsideAnInterpolationHole_Counts1()
    {
        const string source = "var s = $\"{LegacyDetailCandidateSql}\";";
        Assert.Equal(1, PlanForceActionDetailCensus.CountIdentifierUses(source, "LegacyDetailCandidateSql"));
    }

    [Fact]
    public void Control_IdentifierUsedInsideAnInterpolationHoleWithSurroundingText_Counts1()
    {
        const string source = "var s = $\"x {LegacyDetailCandidateSql} y\";";
        Assert.Equal(1, PlanForceActionDetailCensus.CountIdentifierUses(source, "LegacyDetailCandidateSql"));
    }

    /// <summary>#4346: no production code reaches <c>LegacyDetailCandidateSql</c> (or any private
    /// member of the exempted scrub type) via reflection instead of the compiler-checked reference the
    /// census counts above — a <c>GetField</c>/<c>GetFields</c> call sitting in the same file as
    /// <c>typeof(PlanForceActionDetailScrub)</c>, or anywhere in the scrub's own file, is exactly that
    /// bypass.</summary>
    [Fact]
    public void NoProductionCode_ReachesPlanForceActionDetailScrubMembersByReflection()
    {
        var violations = new List<string>();

        foreach (var file in ProductionSourceFiles())
        {
            var raw = File.ReadAllText(file).ReplaceLineEndings("\n");
            var hasGetField = raw.Contains("GetField(", StringComparison.Ordinal)
                || raw.Contains("GetFields(", StringComparison.Ordinal);

            if (!hasGetField)
            {
                continue;
            }

            var isScrubFile = Path.GetFileName(file) == ScrubFileName;
            var namesTheScrubType = raw.Contains("typeof(PlanForceActionDetailScrub)", StringComparison.Ordinal);

            if (isScrubFile || namesTheScrubType)
            {
                violations.Add(Path.GetFileName(file));
            }
        }

        Assert.True(
            violations.Count == 0,
            "production code reaches PlanForceActionDetailScrub members by reflection (GetField/GetFields "
            + "alongside typeof(PlanForceActionDetailScrub), or inside the scrub's own file): "
            + string.Join(", ", violations));
    }

    [Fact]
    public void NoProductionCode_ReferencesLegacyDetailCandidateSqlByReflectionOrNameof()
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
