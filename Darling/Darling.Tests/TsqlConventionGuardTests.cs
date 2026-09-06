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
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para><c>CONTRIBUTING.md</c>'s <b>T-SQL Style</b> list opens "All T-SQL code must follow", and until #3081
/// nothing enforced any of it. PR #3078 introduced <c>COUNT(*)</c> and <c>COUNT(DISTINCT …)</c> into a new
/// collector query — a violation of the bullet at <c>CONTRIBUTING.md:355</c> — and passed the Linux build, the
/// PostgreSQL tests, all four whole-tree guards, the 145-test command-deadline family, <c>review</c> and
/// <c>verify</c>. A review bot reading the diff was the only thing that objected.</para>
///
/// <para><b>This guard covers PART of that list and says which part.</b> The bullets are not equally checkable
/// against SQL held in C# string literals, and a scan that tried to enforce all of them would fire on prose in
/// a comment, on a column named <c>as_of</c>, and on identifiers that contain a keyword — noisy enough to stop
/// being read, which is the failure mode #3076 was weighing for a different guard. So
/// <see cref="RuleBullets"/> carries one entry per bullet with an explicit disposition, and
/// <see cref="TheDispositionMap_PartitionsTheBulletsContributingStates"/> compares its keys against the bullets
/// parsed out of <c>CONTRIBUTING.md</c> for EQUALITY: a tenth bullet, or a renamed one, reds here and forces a
/// scope decision rather than being silently unguarded.</para>
///
/// <para><b>The population is a string literal that OPENS a T-SQL statement.</b> Two factors, both required:
/// the literal must begin with a T-SQL statement keyword once its own SQL comments are blanked, and it must
/// carry a token that only T-SQL has (<see cref="TsqlMarkers"/>). Both halves earn their keep. The marker
/// alone admits prose — <c>ScheduleManager</c>'s collector descriptions name <c>sys.dm_os_wait_stats</c>
/// twenty-three times and the two MCP instruction blobs mention SQL Server throughout. The statement opener
/// alone admits the store's own PostgreSQL and DuckDB SQL, where <c>COUNT_BIG()</c> does not exist and
/// <c>--</c> is idiomatic; <c>DarlingServerConnector</c> is the clean demonstration, carrying the SAME
/// <c>-- #2228</c> comment twice, once in a T-SQL probe (fixed here) and once in a PostgreSQL probe (left
/// alone, correctly).</para>
///
/// <para><b>Stated bound: SQL assembled across several literals is judged per literal.</b>
/// <c>FactRemediation</c> renders a remediation script with one <c>sb.Append</c> per line, so
/// <c>DECLARE @plan_handle varbinary(64);</c> is in the population and the <c>-- STEP 2</c> line above it is
/// not — a literal that is only a comment opens no statement. That is a real gap and it is the price of a
/// population rule a reader can hold in their head. The collectors, where the #3078 violation landed, hold
/// whole statements in one literal each.</para>
///
/// <para><b>Stated bound: C# string literals only, not standalone <c>.sql</c> files.</b> Measured on the tree
/// at the time of writing: the deprecated Full edition's <c>install/</c> carries 6 <c>COUNT()</c>, 6 <c>--</c>
/// comments and 75 uppercase data types; <c>upgrades/</c> carries 3 <c>@@ROWCOUNT</c>; the rigs and CI scripts
/// under <c>tools/</c> and <c>.github/sql/</c> carry 9 <c>COUNT()</c> and 101 <c>--</c> comments. Bringing them
/// in would mean either ~200 waivers or a remediation of code belonging to a retired SKU, and a guard whose
/// waiver list dwarfs its findings has stopped being a guard. The two live SKUs and the shared libraries they
/// both build on are what this scans.</para>
///
/// <para><b>Both SKUs, and one class rather than two.</b> The scan reads the shared libraries, all of
/// <c>Darling/</c> and all of <c>Lite/</c> — the #3078 violation landed in <c>PerformanceMonitor.Collectors</c>,
/// which both products build on, and Lite's <c>RemoteCollectorService</c> and <c>LocalDataService.FinOps.*</c>
/// send T-SQL of their own. Each of the three trees is floored SEPARATELY in
/// <see cref="NoTsqlStatementViolatesACoveredConvention"/>, spelled identically, because an arm written as a
/// special case is how the next tree inherits the sibling bug #3067 was filed for. Duplicating the class into
/// <c>Lite.Tests</c> would buy nothing and drift: the <c>darling</c> path filter reaches
/// <c>Lite/**/*.cs</c> and the <c>core</c> filter reaches every shared library, so this suite runs on a change
/// to any tree it reads — <c>CrossAppGuardCiGateTests</c> is what holds that, and
/// <c>darling-tree-guards</c> runs the whole suite on the arm where the filter did not fire.</para>
/// </summary>
public sealed class TsqlConventionGuardTests
{
    /* Rule identifiers. Covered ones are what Findings can emit; uncovered ones exist so the disposition map
       can name them, so TheUncoveredRules_... can carry a fixture per rule, and so neither list can grow
       without the other noticing. */
    private const string CountBig = "COUNT_BIG-not-COUNT";
    private const string RowcountBig = "ROWCOUNT_BIG-not-@@ROWCOUNT";
    private const string BlockComments = "block-comments-not-double-dash";
    private const string NoTabs = "spaces-not-tabs";
    private const string LowercaseTypes = "data-types-lowercase";

    private const string UppercaseKeywords = "keywords-UPPERCASE";
    private const string UnabbreviatedTypes = "data-types-unabbreviated";
    private const string SysnameForIdentifiers = "sysname-for-identifiers";
    private const string FourSpaceIndent = "indent-four-spaces";
    private const string AliasWithAs = "table-aliases-use-AS";
    private const string ColumnAliasForm = "column-alias-is-name-equals-expression";
    private const string TrailingCommas = "commas-trail";

    /// <summary>
    /// One entry per bullet in <c>CONTRIBUTING.md</c>'s T-SQL Style list, keyed by the bullet's bold lead-in.
    /// <c>Covered</c> is what this file's detector enforces for that bullet, <c>Uncovered</c> is what it does
    /// not, and <c>Phrases</c> is the wording the checks were derived FROM — asserted still present, so an
    /// edit to the rule cannot leave a check enforcing something the document no longer says.
    ///
    /// <para>Three bullets are split rather than all-or-nothing, and saying so is the point of the shape.
    /// <b>Data types</b> states two independent properties and only one of them is checked here.
    /// <b>Indentation</b> bans tabs and prescribes a depth; the ban is a token, the depth is a shape.
    /// A bullet with entries on both sides is honest about covering half of itself; a bullet listed as covered
    /// when it is not is the shape this issue was filed about.</para>
    /// </summary>
    private static readonly IReadOnlyDictionary<string, RuleBullet> RuleBullets =
        new Dictionary<string, RuleBullet>(StringComparer.Ordinal)
        {
            ["Keywords"] = new(
                Covered: Array.Empty<string>(),
                Uncovered: new[] { UppercaseKeywords },
                Phrases: new[] { "UPPERCASE", "`SELECT`" },
                Note: "Needs enough parsing to tell a keyword from an identifier that contains one. "
                    + "`FROM sys.dm_os_waiting_tasks AS owt` and the prose in a block comment above it are the "
                    + "same words to a regex, and this codebase's SQL comments are long and discuss SQL."),

            ["Data types"] = new(
                Covered: new[] { LowercaseTypes },
                Uncovered: new[] { UnabbreviatedTypes },
                Phrases: new[] { "lowercase", "never abbreviated", "`integer`", "`nvarchar(max)`", "`nvarchar(MAX)`" },
                Note: "The CASE half is checked. The ABBREVIATION half is not, and the reason is remediation "
                    + "cost rather than detectability: `int` where `integer` is meant measured at 33 sites in "
                    + "7 files when this landed, all of them inside T-SQL that runs against monitored "
                    + "production servers, and one of them is an sp_executesql parameter declaration "
                    + "(`N'@h varbinary(64), @stmt_start int, @stmt_end int'`) that nothing in this repository "
                    + "executes. Rewriting 33 live query strings belongs in a change whose subject is that "
                    + "rewrite. Carrying them in a waiver list instead was considered and rejected: the key "
                    + "would have to collapse repeated spellings within one member, so a 34th `CONVERT(int, "
                    + "NULL)` beside the ten already there would satisfy it — a guard claiming the rule while "
                    + "under-covering it, which is exactly the shape #3081 was filed about."),

            ["Object names"] = new(
                Covered: Array.Empty<string>(),
                Uncovered: new[] { SysnameForIdentifiers },
                Phrases: new[] { "`sysname`" },
                Note: "Depends on what the column MEANS. `nvarchar(128)` is correct for a wait type and wrong "
                    + "for a database name, and nothing in the text says which one a given column is."),

            ["Indentation"] = new(
                Covered: new[] { NoTabs },
                Uncovered: new[] { FourSpaceIndent },
                Phrases: new[] { "4 spaces", "never tabs" },
                Note: "The tab BAN is a token and is checked. The four-space DEPTH is a shape: continuation "
                    + "lines, CASE arms and the two-space `ON` rule all indent by amounts the bullet does not "
                    + "enumerate, so a depth check would need the layout rules rather than the character."),

            ["Table aliases"] = new(
                Covered: Array.Empty<string>(),
                Uncovered: new[] { AliasWithAs },
                Phrases: new[] { "Always use `AS`", "`FROM dbo.table AS t`" },
                Note: "Finding the alias slot means knowing where the FROM list ends, which means parsing. "
                    + "A bare `FROM sys.dm_os_wait_stats w` and a table-valued function call with two "
                    + "arguments are the same token sequence to anything short of that."),

            ["Column aliases"] = new(
                Covered: Array.Empty<string>(),
                Uncovered: new[] { ColumnAliasForm },
                Phrases: new[] { "`column_name = expression`" },
                Note: "A style SHAPE rather than a token: telling `wait_type AS wait` from a comparison in a "
                    + "predicate needs the clause boundaries."),

            ["Commas"] = new(
                Covered: Array.Empty<string>(),
                Uncovered: new[] { TrailingCommas },
                Phrases: new[] { "Trailing commas" },
                Note: "A style SHAPE. A leading comma is legal SQL in a legal position, so the check is about "
                    + "line layout inside a list whose extent has to be found first."),

            ["Comments"] = new(
                Covered: new[] { BlockComments },
                Uncovered: Array.Empty<string>(),
                Phrases: new[] { "`/* ... */`", "never `--`" },
                Note: "Checked by tokenising, not by line prefix. This codebase does not put an asterisk on a "
                    + "block comment's continuation lines, so a prefix filter reads that prose as code and a "
                    + "comment mentioning the banned form becomes an offender — the #3052 defect "
                    + "CommentFilterAdoptionTests exists to track. Six files carry a `--` inside block-comment "
                    + "prose today and none of them is a violation."),

            ["Functions"] = new(
                Covered: new[] { CountBig, RowcountBig },
                Uncovered: Array.Empty<string>(),
                Phrases: new[] { "`COUNT_BIG()`", "not `COUNT()`", "`ROWCOUNT_BIG()`", "not `@@ROWCOUNT`" },
                Note: "The bullet #3078 violated. Both halves are single tokens with a single correct "
                    + "replacement, which is what makes them worth a guard."),
        };

    private sealed record RuleBullet(string[] Covered, string[] Uncovered, string[] Phrases, string Note);

    /// <summary>
    /// Catalog definitions whose source carries no SQL statement of any dialect, with the phrase the source
    /// itself has to keep saying about that. An enumeration compared for EQUALITY rather than a tolerated
    /// zero: "the walk found nothing in this file" is what a broken literal walk looks like as well as what a
    /// query-less collector looks like, and only naming the second lets the first stay loud. An entry whose
    /// collector grows a query is forced out by the same comparison.
    /// </summary>
    private static readonly (string Collector, string SourceMustSay, string Why)[] DefinitionsWithoutSql =
    {
        ("pg_cpu_utilization", "has no SQL route",
            "CPU for a PostgreSQL target is INGESTED, not queried: DarlingCollectorRunner.IngestPgCpuAsync "
            + "reads it from RDS/Performance Insights, so BuildQuery and ReadAsync both throw and exist only "
            + "to satisfy ICollectorDefinition<TRow> and PgSchemaGenerator's DDL generation."),
    };

    /* ───────────────────────── the corpus scan ───────────────────────── */

    /// <summary>
    /// The scan, and the reason the population floors are separate assertions rather than a comment: this
    /// guard's entire failure direction is silent non-detection. A glob that opened nothing, a literal walk
    /// that desynchronised, or a marker set that stopped recognising T-SQL all produce zero offenders, and
    /// zero offenders is also what success looks like.
    ///
    /// <para>The floors are DERIVED, not numbers. Each scanned tree must contribute at least one T-SQL
    /// statement and the tree loop must have run for every tree, so "the sweep read one tree and reported
    /// clean on the others" fails by name. A count floor would additionally go stale by exactly the mechanism
    /// this guard exists to stop, and
    /// <see cref="EverySqlServerCollectorDefinitionsFile_ContributesATsqlStatement"/> is the stronger version
    /// of the same idea: a per-SITE requirement derived from the catalog, which a total can never give.</para>
    ///
    /// <para><b>There is deliberately no waiver list.</b> The nine pre-existing violations of the covered
    /// subset — three <c>COUNT(*)</c>, one <c>--</c>, five uppercase data types — were fixed in the change that
    /// added this file, so the covered subset measures zero on the tree with no exceptions carried. A waiver
    /// mechanism nobody needs is a mechanism the next exception takes for granted.</para>
    /// </summary>
    [Fact]
    public void NoTsqlStatementViolatesACoveredConvention()
    {
        var treesScanned = 0;
        var filesScanned = 0;
        var literalsScanned = 0;
        var statementsScanned = 0;
        var offenders = new List<string>();

        foreach (var (tree, roots) in ScannedTrees)
        {
            treesScanned++;
            var statementsInTree = 0;

            foreach (var path in SourceFiles(roots))
            {
                filesScanned++;
                var text = File.ReadAllText(path);
                var name = Path.GetFileName(path);

                foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
                {
                    literalsScanned++;

                    if (!IsTsqlStatement(body))
                    {
                        continue;
                    }

                    statementsScanned++;
                    statementsInTree++;

                    foreach (var (rule, detail) in Findings(body))
                    {
                        offenders.Add($"{name}:{EnclosingMember(text, start)} [{rule}] {detail}");
                    }
                }
            }

            /* Per tree, spelled the same way for all three. A union floored only in total passes while one
               tree contributes nothing, and the tree that goes dark is the one nobody is looking at. */
            Assert.True(
                statementsInTree > 0,
                $"the {tree} tree contributed no T-SQL statement at all, so every check below is vacuous "
                + $"there. Roots read: {string.Join(", ", roots)}.");
        }

        Assert.Equal(ScannedTrees.Length, treesScanned);
        Assert.True(filesScanned > 0, "the sweep opened no source file");
        Assert.True(literalsScanned > 0, "the literal walk extracted nothing from the files it opened");
        Assert.True(statementsScanned > 0, "no literal in the whole corpus was read as a T-SQL statement");

        Assert.True(
            offenders.Count == 0,
            "T-SQL in this repository violates a CONTRIBUTING.md convention this guard covers."
            + Environment.NewLine + string.Join(Environment.NewLine, offenders) + Environment.NewLine
            + "Covered here: " + CoveredSubsetSentence() + "." + Environment.NewLine
            + "NOT covered, so still on trust: " + UncoveredSubsetSentence() + ". "
            + "Fix the SQL rather than this guard; if the rule itself is wrong, change CONTRIBUTING.md and "
            + "TheDispositionMap_PartitionsTheBulletsContributingStates will tell you what else to update.");
    }

    /// <summary>
    /// Per-SITE non-vacuity, derived from the collector catalog, which is the authority on which engine a
    /// definition speaks: <c>ICollectorSchemaInfo.TargetEngine</c> defaults to <c>SqlServer</c> and
    /// <c>PostgresCollectorDefinitionBase</c> SEALS it to <c>PostgreSQL</c>. So the catalog answers "is this
    /// file's SQL T-SQL" without this guard having to guess from a filename — and <c>PgPlanFetcher</c> is why
    /// guessing would be wrong: it is named for the Postgres store and its SQL is T-SQL, fetched from a
    /// monitored SQL Server.
    ///
    /// <para>Both directions, and both are per-site rather than a floor, because reverting one of several
    /// occurrences leaves the rest satisfying a floor. Every SQL Server definition's file must contribute a
    /// T-SQL statement — the marker set going blind on one collector is then named, not averaged away — and
    /// no PostgreSQL definition's file may contribute one, while still being required to contain a statement
    /// the walk could see. That second requirement is the load-bearing half of the negative: a file the sweep
    /// never opened would satisfy "contributed no T-SQL" perfectly.</para>
    ///
    /// <para>The definitions that carry no SQL at all are named in <see cref="DefinitionsWithoutSql"/> and
    /// compared for EQUALITY, so "the walk found nothing here" cannot be the quiet answer for a collector
    /// that has a query. Each entry has to be corroborated by the source saying so itself.</para>
    /// </summary>
    [Fact]
    public void EverySqlServerCollectorDefinitionsFile_ContributesATsqlStatement()
    {
        var collectors = Path.Combine(RepoRoot(), "PerformanceMonitor.Collectors");

        var sqlServer = CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
            .ToArray();
        var postgres = CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql)
            .ToArray();

        Assert.NotEmpty(sqlServer);
        Assert.NotEmpty(postgres);

        var blind = new List<string>();
        var misread = new List<string>();
        var withoutSql = new List<string>();

        foreach (var definition in CollectorCatalog.All)
        {
            var type = definition.GetType().Name;
            var file = Path.Combine(collectors, type + ".cs");

            Assert.True(
                File.Exists(file),
                $"{type} is in the catalog but {type}.cs is not in PerformanceMonitor.Collectors, so this "
                + "check cannot read its SQL. The file-per-definition naming is what makes the catalog usable "
                + "as an engine oracle here; if a definition legitimately moves, this check needs a real "
                + "type-to-source map rather than a relaxed assertion.");

            var text = File.ReadAllText(file);
            var bodies = CSharpSourceWalker.StringLiteralBodies(text).ToArray();
            var statements = bodies.Count(b => OpensAStatement(b.Text));
            var tsql = bodies.Count(b => IsTsqlStatement(b.Text));

            if (statements == 0)
            {
                withoutSql.Add(definition.Name);

                var entry = DefinitionsWithoutSql.FirstOrDefault(e => e.Collector == definition.Name);

                if (entry.Collector is not null)
                {
                    /* The entry's claim, checked against the source rather than believed. A collector that
                       grew a query would still be found statement-less by a walk that had broken, and this
                       is what tells the two apart. */
                    Assert.True(
                        text.Contains(entry.SourceMustSay, StringComparison.Ordinal),
                        $"{definition.Name} is listed as carrying no SQL, but {type}.cs no longer says "
                        + $"\"{entry.SourceMustSay}\" — so the reason recorded for it is not the reason it "
                        + "has no SQL. Recorded reason: " + entry.Why);
                }

                continue;
            }

            if (definition.TargetEngine == CollectorTargetEngine.SqlServer && tsql == 0)
            {
                blind.Add($"{definition.Name} ({type}.cs)");
            }
            else if (definition.TargetEngine == CollectorTargetEngine.PostgreSql && tsql > 0)
            {
                misread.Add($"{definition.Name} ({type}.cs)");
            }
        }

        /* Set equality, not a threshold: a collector whose SQL became invisible lands here and reds, and an
           entry whose collector grew SQL is forced OUT rather than lingering. */
        Assert.Equal(
            DefinitionsWithoutSql.Select(e => e.Collector).OrderBy(c => c, StringComparer.Ordinal).ToArray(),
            withoutSql.OrderBy(c => c, StringComparer.Ordinal).ToArray());

        Assert.True(
            blind.Count == 0,
            "these SQL Server collectors' queries are no longer recognised as T-SQL, so NoTsqlStatement… "
            + "passes on them vacuously. TsqlMarkers has gone blind to a form they use:" + Environment.NewLine
            + string.Join(Environment.NewLine, blind));

        Assert.True(
            misread.Count == 0,
            "these PostgreSQL collectors' queries are being read as T-SQL, which would judge them against "
            + "rules that do not apply — COUNT_BIG() does not exist in PostgreSQL and `--` is idiomatic there. "
            + "TsqlMarkers has widened into PostgreSQL vocabulary:" + Environment.NewLine
            + string.Join(Environment.NewLine, misread));
    }

    /* ───────────────────────── the disposition map against the document ───────────────────────── */

    /// <summary>
    /// The bullets in <c>CONTRIBUTING.md</c>'s T-SQL Style list, against <see cref="RuleBullets"/>, at set
    /// equality — so a bullet added, removed or renamed cannot leave this file silently claiming a partition
    /// of a list that no longer looks like that. And each bullet's own wording is checked to still contain
    /// the phrases the checks were derived from: without that, the document could be edited to say the
    /// opposite of what <see cref="Findings"/> enforces and every test here would stay green.
    /// </summary>
    [Fact]
    public void TheDispositionMap_PartitionsTheBulletsContributingStates()
    {
        var bullets = ContributingTsqlBullets();

        Assert.True(
            bullets.Count > 0,
            "no bullets were parsed out of CONTRIBUTING.md's T-SQL Style list, so the comparison below would "
            + "be vacuous. The section heading, the 'Key points:' lead-in, or the bullet shape has changed.");

        Assert.Equal(
            RuleBullets.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            bullets.Select(b => b.Heading).OrderBy(h => h, StringComparer.Ordinal).ToArray());

        Assert.Equal(bullets.Count, bullets.Select(b => b.Heading).Distinct(StringComparer.Ordinal).Count());

        VerifyBulletPhrases(bullets);

        /* Neither side of the split may hold a rule the other list has never heard of, and every covered rule
           must be one Findings can actually emit. A rule id present in the map and absent from the detector
           reads as guarded and is not. */
        Assert.Equal(
            CoveredRules.OrderBy(r => r, StringComparer.Ordinal).ToArray(),
            RuleBullets.Values.SelectMany(b => b.Covered).OrderBy(r => r, StringComparer.Ordinal).ToArray());

        Assert.Equal(
            UncoveredRules.OrderBy(r => r, StringComparer.Ordinal).ToArray(),
            RuleBullets.Values.SelectMany(b => b.Uncovered).OrderBy(r => r, StringComparer.Ordinal).ToArray());

        Assert.Empty(CoveredRules.Intersect(UncoveredRules, StringComparer.Ordinal));
    }

    /// <summary>
    /// Non-vacuity for every required phrase, one at a time, because a phrase check is a
    /// <c>Contains</c> — and a <c>Contains</c> is exactly the shape a longer token satisfies by accident.
    /// This test was written after a mutation survived: <c>Phrases</c> for the Functions bullet asked for
    /// <c>COUNT_BIG()</c>, and rewriting the document's <c>COUNT_BIG()</c> to <c>COUNTBIG()</c> stayed green
    /// because <c>ROWCOUNT_BIG()</c> two words later still contains <c>COUNT_BIG()</c> as a substring. The
    /// phrases now carry their backticks, and this holds each of them individually rather than trusting that
    /// they are all distinct enough.
    ///
    /// <para>Driven with the REAL bullet text and one phrase deleted from it, so a phrase that the document
    /// does not actually contain fails <see cref="TheDispositionMap_PartitionsTheBulletsContributingStates"/>
    /// first — the two together mean each phrase is both present and load-bearing.</para>
    /// </summary>
    [Fact]
    public void EveryRequiredPhrase_IsLoadBearingOnItsOwn()
    {
        var bullets = ContributingTsqlBullets();
        var exercised = 0;

        foreach (var (heading, body) in bullets)
        {
            foreach (var phrase in RuleBullets[heading].Phrases)
            {
                var without = bullets
                    .Select(b => b.Heading == heading
                        ? (b.Heading, Body: b.Body.Replace(phrase, "…", StringComparison.Ordinal))
                        : b)
                    .ToArray();

                Assert.NotEqual(body, without.Single(b => b.Heading == heading).Body);
                Assert.ThrowsAny<Exception>(() => VerifyBulletPhrases(without));
                exercised++;
            }
        }

        Assert.Equal(RuleBullets.Values.Sum(b => b.Phrases.Length), exercised);
        Assert.True(exercised > 0, "no phrase was exercised, so this pin says nothing");
    }

    /// <summary>
    /// Each bullet still contains the wording its checks were derived from. Separated from the map so
    /// <see cref="EveryRequiredPhrase_IsLoadBearingOnItsOwn"/> can drive it with a doctored bullet and show
    /// the check reds — which reading the real document could never show.
    /// </summary>
    private static void VerifyBulletPhrases(IReadOnlyList<(string Heading, string Body)> bullets)
    {
        foreach (var (heading, body) in bullets)
        {
            foreach (var phrase in RuleBullets[heading].Phrases)
            {
                Assert.True(
                    body.Contains(phrase, StringComparison.Ordinal),
                    $"CONTRIBUTING.md's \"{heading}\" bullet no longer contains \"{phrase}\", which is wording "
                    + $"a check here was derived from. Bullet text now: {body}");
            }
        }
    }

    /* ───────────────────────── the detector, pinned in both directions ───────────────────────── */

    /// <summary>
    /// Known answers for every covered rule, and for the benign forms this repository's own T-SQL contains
    /// that a first cut of each check flagged. Each hazard is a real spelling: the two <c>COUNT(*)</c> sites
    /// in <c>DatabaseSizeStatsCollector</c>, the one inside <c>sp_executesql</c> in Lite's FinOps
    /// recommendations, <c>DarlingServerConnector</c>'s <c>--</c>, and the four uppercase type spellings.
    ///
    /// <para>The rule COVERAGE of the hazard set is asserted at set equality, not counted: a covered rule with
    /// no hazard fixture is a check nothing exercises, and adding one to <see cref="Findings"/> without a
    /// fixture reds here.</para>
    /// </summary>
    [Fact]
    public void TheDetector_FlagsEveryCoveredRule_AndNoBenignForm()
    {
        (string Sql, string Rule)[] hazards =
        {
            /* #3078's own shape, and the two sites that were live on dev when this landed. */
            ("SELECT c = (SELECT COUNT(*) FROM sys.dm_db_log_info(DB_ID()) AS li) OPTION(RECOMPILE);", CountBig),
            ("SELECT n = COUNT(DISTINCT owt.wait_type) FROM sys.dm_os_waiting_tasks AS owt OPTION(RECOMPILE);", CountBig),
            /* Lowercase spelling is the same violation of the same bullet, so the check is case-insensitive
               even though the UPPERCASE-keywords bullet is out of scope. Relying on the case would make this
               check's reach depend on a rule nothing enforces. */
            ("SELECT n = count(*) FROM sys.dm_os_waiting_tasks AS owt OPTION(RECOMPILE);", CountBig),
            /* Inside dynamic SQL: a single-quoted span that is NOT an XQuery argument stays in the code view,
               because sp_executesql's body is T-SQL that runs. */
            ("EXEC sys.sp_executesql N'SELECT @c = COUNT(*) FROM sys.availability_groups;', N'@c integer OUTPUT';", CountBig),
            ("UPDATE t SET x = 1 FROM sys.databases AS d; SELECT rows_touched = @@ROWCOUNT;", RowcountBig),
            ("SELECT d.name FROM sys.databases AS d\n    /* a real comment */\n    -- and a banned one\nOPTION(RECOMPILE);", BlockComments),
            /* A `--` on the very first character of the literal still opens no statement, so the fixture puts
               the statement first — which is also the only shape the corpus contains. */
            ("SELECT d.name -- trailing\nFROM sys.databases AS d OPTION(RECOMPILE);", BlockComments),
            ("SELECT\n\td.name\nFROM sys.databases AS d OPTION(RECOMPILE);", NoTabs),
            ("DECLARE @sql NVARCHAR(500); SELECT @sql = N'x' FROM sys.databases AS d;", LowercaseTypes),
            ("DECLARE @plan_handle VARBINARY(64); SELECT @plan_handle = NULL FROM sys.dm_exec_query_stats AS qs;", LowercaseTypes),
            ("SELECT e = CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) OPTION(RECOMPILE);", LowercaseTypes),
            ("DECLARE @sql nvarchar(MAX); SELECT @sql = N'x' FROM sys.databases AS d;", LowercaseTypes),
            /* Mixed case is not lowercase either. */
            ("DECLARE @db SysName; SELECT @db = d.name FROM sys.databases AS d;", LowercaseTypes),
        };

        var flagged = new List<string>();
        var seen = 0;

        foreach (var (sql, rule) in hazards)
        {
            seen++;

            Assert.True(
                IsTsqlStatement(sql),
                $"a hazard fixture is not even in the population, so it proves nothing about the detector: {sql}");

            var rules = Findings(sql).Select(f => f.Rule).ToArray();

            Assert.Contains(rule, rules);
            flagged.Add(rule);
        }

        Assert.Equal(hazards.Length, seen);

        Assert.Equal(
            CoveredRules.OrderBy(r => r, StringComparer.Ordinal).ToArray(),
            flagged.Distinct(StringComparer.Ordinal).OrderBy(r => r, StringComparer.Ordinal).ToArray());

        string[] benign =
        {
            /* The fixed forms. */
            "SELECT c = CONVERT(integer, COUNT_BIG(*)) FROM sys.dm_os_waiting_tasks AS owt OPTION(RECOMPILE);",
            "SELECT n = COUNT_BIG(DISTINCT owt.wait_type) FROM sys.dm_os_waiting_tasks AS owt OPTION(RECOMPILE);",
            "UPDATE t SET x = 1 FROM sys.databases AS d; SELECT rows_touched = ROWCOUNT_BIG();",
            "SET NOCOUNT ON; SELECT d.name FROM sys.databases AS d OPTION(RECOMPILE);",
            /* XQuery, and it is why the code view blanks an XML method's FIRST argument only. XQuery's
               function is count() and there is no COUNT_BIG in it; both spellings ship in
               DeadlocksCollector and BlockedProcessReportCollector. The SECOND argument is a T-SQL type
               name and stays visible, which is what keeps 'NVARCHAR(MAX)' there checkable. */
            "SELECT o = fr.n.value('let $c := . return count(../frame[. << $c])', 'integer')\nFROM sys.fn_xe_file_target_read_file(N'x', NULL, NULL, NULL) AS f\nCROSS APPLY f.event_data.nodes('//frame') AS fr(n) OPTION(RECOMPILE);",
            /* The block-comment continuation line, which is the #3052 defect this check must not have: this
               codebase writes no asterisk on continuation lines, and six files carry a `--` inside one. */
            "SELECT d.name\n/* memory_pressure_events.sample_time is naive UTC -- Darling passes it to\n   the viewer unchanged, so the frame is the collector's. */\nFROM sys.databases AS d OPTION(RECOMPILE);",
            /* A `--` inside a nested block comment, because T-SQL nests them and a non-nesting walk would
               end the outer comment at the inner close and read the rest as code. */
            "SELECT d.name\n/* outer /* inner -- not a comment */ still outer */\nFROM sys.databases AS d OPTION(RECOMPILE);",
            /* A `--` inside a string VALUE is data, not a comment. */
            "SELECT d.name FROM sys.databases AS d WHERE d.name <> N'a--b' OPTION(RECOMPILE);",
            /* The same thing AFTER a '' escape, which is what makes this fixture worth its line: a walk that
               consumes one quote and then looks at the next character closes the span on the escape and
               reads the remainder as code, so the `--` becomes a line comment. Every piece of dynamic SQL
               in this repository doubles its quotes — LocalDataService.FinOps.Recommendations has four
               N''…'' literals inside one sp_executesql — so the walk has to survive the form. */
            "SELECT d.name FROM sys.databases AS d WHERE d.name <> N'a''--b' OPTION(RECOMPILE);",
            /* A tab inside a string value is data too — the rule is about indentation. */
            "SELECT s = REPLACE(d.name, N'\t', N' ') FROM sys.databases AS d OPTION(RECOMPILE);",
            /* FOR XML is a clause, not a data type, and IndexObjectStatsCollector writes XML on its own
               line — which is exactly where a naive vocabulary match reads it as an uppercase type. */
            "SELECT k = STUFF((SELECT N',' + c.name FROM sys.index_columns AS c\n    FOR\n        XML\n        PATH(''),\n        TYPE\n).value('text()[1]', 'nvarchar(max)'), 1, 1, N'') FROM sys.indexes AS i OPTION(RECOMPILE);",
            /* AT TIME ZONE, likewise: TIME is a type name and this is not it. */
            "SELECT t = SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' FROM sys.databases AS d OPTION(RECOMPILE);",
            /* TABLE is deliberately absent from the type vocabulary: QueryStoreCollector declares a table
               variable with it and CREATE TABLE is everywhere. */
            "DECLARE @probe_failures TABLE (name sysname, error_text nvarchar(4000)); SELECT 1 FROM sys.databases AS d;",
            "SET NOCOUNT ON; CREATE TABLE #file_space (database_id integer NOT NULL); SELECT 1 FROM sys.databases AS d;",
            /* A quoted identifier that happens to be a type name. */
            "SELECT [INT] = d.database_id FROM sys.databases AS d OPTION(RECOMPILE);",
            /* MAX the aggregate, not MAX the length spec. */
            "SELECT m = MAX(osi.runnable_tasks_count) FROM sys.dm_os_schedulers AS osi OPTION(RECOMPILE);",
            /* @@ROWCOUNT spelled inside a comment, which is where this codebase discusses it. */
            "SELECT d.name FROM sys.databases AS d /* ROWCOUNT_BIG(), never @@ROWCOUNT */ OPTION(RECOMPILE);",
        };

        foreach (var sql in benign)
        {
            Assert.True(
                IsTsqlStatement(sql),
                $"a benign fixture is outside the population, so it exercises no check and its emptiness "
                + $"proves nothing: {sql}");

            var found = Findings(sql);

            Assert.True(
                found.Count == 0,
                "the detector flagged a benign form: " + sql + " => "
                + string.Join("; ", found.Select(f => $"[{f.Rule}] {f.Detail}")));
        }
    }

    /// <summary>
    /// The discriminator itself, in both directions, because the population is what every other assertion in
    /// this file rests on. The positives are shapes the corpus contains; the negatives are the other two
    /// dialects this repository speaks plus the prose forms that a marker-only test admitted.
    /// </summary>
    [Fact]
    public void ThePopulation_IsTsqlStatements_AndNothingElse()
    {
        string[] tsql =
        {
            "\nSET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;\n\nSELECT\n    latch_class = ls.latch_class\nFROM sys.dm_os_latch_stats AS ls\nOPTION(RECOMPILE);",
            "SELECT sqlserver_start_time FROM sys.dm_os_sys_info",
            "/* a leading block comment must not hide the statement */\nSELECT d.name FROM sys.databases AS d OPTION(RECOMPILE);",
            ";WITH x AS (SELECT TOP (1) d.name FROM sys.databases AS d) SELECT name FROM x;",
            "IF NOT EXISTS (SELECT 1/0 FROM sys.dm_xe_database_sessions AS s) BEGIN SELECT 1; END;",
            "DECLARE @plan_handle varbinary(64);",
            "EXEC sys.sp_configure N'cost threshold for parallelism', 50;",
        };

        foreach (var sql in tsql)
        {
            Assert.True(IsTsqlStatement(sql), "the discriminator did not see T-SQL here: " + sql);
        }

        string[] notTsql =
        {
            /* PostgreSQL: the store's own dialect. DarlingServerConnector carries this and the T-SQL probe
               above it, with the SAME `-- #2228` comment in both — the T-SQL one was a violation and this
               one is not, which is the whole reason the population is engine-aware. */
            "SELECT\n    version() AS server_version_text,\n    current_setting('server_version_num')::int / 10000 AS major_version,\n    -- #2228: which database this connection actually landed in.\n    current_database() AS connected_database",
            "SELECT count(*) FROM collect.wait_stats WHERE collection_time > now() AT TIME ZONE 'UTC' - interval '1 hour'",
            "INSERT INTO config.servers (server_id, name) VALUES ($1, $2) ON CONFLICT (server_id) DO UPDATE SET name = $2",
            /* DuckDB: Lite's store dialect, and the same file that holds Lite's T-SQL holds these. */
            "SELECT\n    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_server_memory_mb) AS p95_mb,\n    COUNT(*) AS sample_count\nFROM v_memory_stats\nWHERE server_id = $1",
            /* Prose that names DMVs — ScheduleManager's collector descriptions, twenty-three of them. */
            "Wait statistics from sys.dm_os_wait_stats",
            "Server-wide session summary (idle/leak signal): total/running/sleeping counts from sys.dm_exec_sessions",
            /* A log message that interpolates a server name and mentions a DMV. */
            "[{Server}] [{Database}] {Capture} XE session is still not visible in sys.dm_xe_database_sessions",
            /* An instruction blob: markdown that mentions SQL Server throughout and happens to contain a
               line starting with a keyword. A statement-opener test anchored at any LINE start admitted
               40kb of this; anchored at the literal's start it does not. */
            "You are connected to a SQL Server performance monitoring tool.\n\n## Notes\n\nIF the store is empty, sys.dm_os_wait_stats has nothing to say.",
            /* A SQL FRAGMENT: real, T-SQL, and outside the population by the stated bound. */
            "FROM sys.dm_exec_query_stats AS qs",
            "-- STEP 2 (apply): free every currently-cached plan for those hashes.",
        };

        foreach (var sql in notTsql)
        {
            Assert.False(IsTsqlStatement(sql), "the discriminator read this as T-SQL: " + sql);
        }
    }

    /// <summary>
    /// The rules this guard does NOT check, one fixture each, with the expected answer EMPTY — the #3079
    /// shape. Folded into <see cref="TheDetector_FlagsEveryCoveredRule_AndNoBenignForm"/>'s benign list they
    /// would record nothing, because that list already asserts emptiness for forms that are not violations at
    /// all; the claim here is different and sharper: each of these IS a violation of a rule
    /// <c>CONTRIBUTING.md</c> states, it IS inside the population, and this guard says nothing about it.
    ///
    /// <para><b>Every case carries a positive control through the same detector.</b> An empty answer is the
    /// success condition, and the other thing that produces an empty answer is a detector that has stopped
    /// working. So each fixture is also fed with a covered violation spliced in, and that has to be found. A
    /// dead <see cref="Findings"/> fails the control while satisfying every emptiness assertion.</para>
    ///
    /// <para>The fixture set's rule coverage is compared to the disposition map's uncovered side at set
    /// equality, so a rule moved out of scope without a fixture reds, and a fixture for a rule that is now
    /// covered reds too.</para>
    /// </summary>
    [Fact]
    public void TheUncoveredRules_AreViolatedByFixturesThisGuardDoesNotFlag()
    {
        (string Rule, string Sql)[] unguarded =
        {
            /* Keywords: every keyword lowercase. */
            (UppercaseKeywords,
                "select w.wait_type from sys.dm_os_wait_stats as w where w.wait_time_ms > 0 option(recompile);"),

            /* Data types, abbreviation half: `int` where the bullet says `integer`. Lowercase, so the half
               that IS covered stays silent — which is precisely the split being recorded. */
            (UnabbreviatedTypes,
                "SELECT c = CONVERT(int, w.waiting_tasks_count) FROM sys.dm_os_wait_stats AS w OPTION(RECOMPILE);"),

            /* Object names: an identifier column typed nvarchar(128) rather than sysname. */
            (SysnameForIdentifiers,
                "SELECT database_name = CONVERT(nvarchar(128), d.name) FROM sys.databases AS d OPTION(RECOMPILE);"),

            /* Indentation, depth half: two spaces rather than four. No tab, so the covered half is silent. */
            (FourSpaceIndent,
                "SELECT\n  d.name\nFROM sys.databases AS d\nOPTION(RECOMPILE);"),

            /* Table aliases: no AS. */
            (AliasWithAs,
                "SELECT w.wait_type FROM sys.dm_os_wait_stats w OPTION(RECOMPILE);"),

            /* Column aliases: `expression AS name` rather than `name = expression`. */
            (ColumnAliasForm,
                "SELECT w.wait_type AS wait FROM sys.dm_os_wait_stats AS w OPTION(RECOMPILE);"),

            /* Commas: leading rather than trailing. */
            (TrailingCommas,
                "SELECT\n    w.wait_type\n  , w.waiting_tasks_count\nFROM sys.dm_os_wait_stats AS w\nOPTION(RECOMPILE);"),
        };

        var covered = 0;

        foreach (var (rule, sql) in unguarded)
        {
            covered++;

            /* Observable: the fixture reaches the detector. A case the population filters out first would
               record a limitation that is not this guard's — #3079 found three of exactly that shape. */
            Assert.True(
                IsTsqlStatement(sql),
                $"the fixture for {rule} is outside the population, so its empty answer records nothing "
                + $"about the detector: {sql}");

            Assert.True(
                Findings(sql).Count == 0,
                $"the fixture for the UNCOVERED rule {rule} was flagged, so either the scope note is stale "
                + $"or a covered check has widened into it: {sql} => "
                + string.Join("; ", Findings(sql).Select(f => $"[{f.Rule}] {f.Detail}")));

            /* The positive control, through the same detector and the same fixture. */
            var withCovered = sql.Replace("OPTION(RECOMPILE);", "AND 1 = (SELECT COUNT(*) FROM sys.databases AS x) OPTION(RECOMPILE);", StringComparison.Ordinal);
            withCovered = withCovered.Replace("option(recompile);", "and 1 = (select count(*) from sys.databases as x) option(recompile);", StringComparison.Ordinal);

            Assert.NotEqual(sql, withCovered);
            Assert.Contains(CountBig, Findings(withCovered).Select(f => f.Rule));
        }

        Assert.Equal(unguarded.Length, covered);

        Assert.Equal(
            UncoveredRules.OrderBy(r => r, StringComparer.Ordinal).ToArray(),
            unguarded.Select(u => u.Rule).Distinct(StringComparer.Ordinal).OrderBy(r => r, StringComparer.Ordinal).ToArray());
    }

    /* ───────────────────────── the checks ───────────────────────── */

    private static readonly string[] CoveredRules = { CountBig, RowcountBig, BlockComments, NoTabs, LowercaseTypes };

    private static readonly string[] UncoveredRules =
    {
        UppercaseKeywords, UnabbreviatedTypes, SysnameForIdentifiers, FourSpaceIndent,
        AliasWithAs, ColumnAliasForm, TrailingCommas,
    };

    private static string CoveredSubsetSentence() =>
        string.Join("; ", RuleBullets
            .Where(b => b.Value.Covered.Length > 0)
            .Select(b => $"{b.Key} ({string.Join(", ", b.Value.Covered)})"));

    private static string UncoveredSubsetSentence() =>
        string.Join("; ", RuleBullets
            .Where(b => b.Value.Uncovered.Length > 0)
            .Select(b => $"{b.Key} ({string.Join(", ", b.Value.Uncovered)})"));

    /// <summary>
    /// <c>COUNT(</c> where <c>COUNT_BIG(</c> is meant. The lookbehind is load-bearing in both directions:
    /// without it <c>@@ROWCOUNT</c> and <c>SET NOCOUNT ON</c> supply a <c>COUNT</c>, and it is what lets
    /// <c>COUNT_BIG(</c> pass — the <c>_</c> after <c>COUNT</c> means the <c>\s*\(</c> never matches. No
    /// capture group, deliberately: a group around the token is how a greedy match absorbs the thing under
    /// test.
    /// </summary>
    private static readonly Regex CountCall = new(
        @"(?<![A-Za-z0-9_@#$])COUNT\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex RowcountGlobal = new(
        @"@@ROWCOUNT(?![A-Za-z0-9_])",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// SQL Server's data type names.
    ///
    /// <para><c>table</c> is deliberately ABSENT. It is a keyword in <c>CREATE TABLE</c> and in
    /// <c>DECLARE @x TABLE (…)</c>, both of which this repository writes in uppercase, and neither is a data
    /// type in the sense the bullet means.</para>
    /// </summary>
    private static readonly string[] TypeNames =
    {
        "bit", "tinyint", "smallint", "int", "integer", "bigint", "decimal", "numeric", "money",
        "smallmoney", "float", "real", "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset",
        "time", "char", "varchar", "text", "nchar", "nvarchar", "ntext", "binary", "varbinary", "image",
        "sysname", "uniqueidentifier", "xml", "sql_variant", "hierarchyid", "geography", "geometry",
        "rowversion", "timestamp",
    };

    /// <summary>
    /// The type vocabulary as one alternation, ordered LONGEST SPELLING FIRST — sorted here rather than
    /// hand-ordered in <see cref="TypeNames"/>, because a hand order is a claim that goes stale the moment a
    /// name is added.
    ///
    /// <para>Honestly scoped: this is DEFENSIVE, not a fix for a live defect, and no fixture below can show
    /// it. Alternation is first-match-wins at a position, so <c>int</c> ahead of <c>integer</c> matches the
    /// first three characters of the longer name — but the trailing lookahead then fails on <c>e</c> and the
    /// engine backtracks into the longer alternative, so today's answer is the same either way. What the
    /// ordering removes is the DEPENDENCE on that backtrack: the day a lookahead is loosened, a shorter
    /// alternative in front is how the token under test gets partly absorbed and reported as something it is
    /// not.</para>
    /// </summary>
    private static readonly Regex TypeName = new(
        @"(?<![A-Za-z0-9_@#$.])(?:"
        + string.Join("|", TypeNames.OrderByDescending(n => n.Length).ThenBy(n => n, StringComparer.Ordinal))
        + @")(?![A-Za-z0-9_])",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A length spec of <c>max</c>, anchored on the type name in front of it so the <c>MAX</c> aggregate
    /// cannot be mistaken for one. <c>MAX(x)</c> has an argument; <c>nvarchar(max)</c> has a type.
    /// </summary>
    private static readonly Regex MaxLengthSpec = new(
        @"(?<![A-Za-z0-9_@#$.])(?<type>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(?<max>max)\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Clause keywords that collide with a data type name, blanked before the type check runs.
    /// <c>FOR XML</c> ships in <c>IndexObjectStatsCollector</c> with <c>XML</c> on its own line, which is
    /// exactly the position a vocabulary match reads as an uppercase type.
    /// </summary>
    private static readonly Regex TypeNameClauseCollisions = new(
        @"\bFOR\s+(?:XML|JSON)\b|\bAT\s+TIME\s+ZONE\b|\bWAITFOR\s+(?:TIME|DELAY)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private readonly record struct Finding(string Rule, string Detail);

    /// <summary>
    /// Every covered-convention violation in one T-SQL statement. Internal to nothing — the corpus scan and
    /// all three fixture tests call THIS, so a control cannot certify behaviour the corpus scan does not have.
    /// </summary>
    private static IReadOnlyList<Finding> Findings(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);

        var kinds = SpanKinds(sql);
        var code = CodeAndDynamicSql(sql, kinds);
        var findings = new List<Finding>();

        foreach (Match match in CountCall.Matches(code))
        {
            findings.Add(new(CountBig, Where(sql, match.Index, "COUNT( — use COUNT_BIG(), wrapping it in CONVERT(integer, …) where the destination column is integer")));
        }

        foreach (Match match in RowcountGlobal.Matches(code))
        {
            findings.Add(new(RowcountBig, Where(sql, match.Index, "@@ROWCOUNT — use ROWCOUNT_BIG()")));
        }

        /* Comments and tabs are read off the SPAN KINDS rather than a text view, which is what makes a `--`
           inside block-comment prose, inside a nested block comment, or inside a string VALUE not a finding,
           and a real line comment one. A line-prefix filter cannot make that distinction — see #3052. */
        for (var i = 0; i < sql.Length - 1; i++)
        {
            if (kinds[i] == SqlSpan.LineComment
                && sql[i] == '-' && sql[i + 1] == '-'
                && (i == 0 || kinds[i - 1] != SqlSpan.LineComment))
            {
                findings.Add(new(BlockComments, Where(sql, i, "a `--` line comment — use /* … */")));
            }
        }

        for (var i = 0; i < sql.Length; i++)
        {
            /* A tab inside a string is data rather than indentation, so only the other spans count. */
            if (sql[i] == '\t' && kinds[i] != SqlSpan.StringLiteral)
            {
                findings.Add(new(NoTabs, Where(sql, i, "a tab — indent with 4 spaces")));
            }
        }

        var forTypes = Blank(code, TypeNameClauseCollisions);

        foreach (Match match in TypeName.Matches(forTypes))
        {
            if (!IsLowercase(match.Value))
            {
                findings.Add(new(LowercaseTypes, Where(sql, match.Index, $"the data type `{match.Value}` — data types are lowercase")));
            }
        }

        /* The length spec, which no vocabulary match can see: the type name in nvarchar(MAX) is spelled
           correctly and only the spec is wrong. Anchored on a name FROM the vocabulary so the MAX aggregate
           cannot be mistaken for one — MAX(x) has an argument, nvarchar(max) has a type. */
        foreach (Match match in MaxLengthSpec.Matches(forTypes))
        {
            var type = match.Groups["type"].Value;
            var spec = match.Groups["max"];

            if (!IsLowercase(spec.Value) && TypeNames.Contains(type, StringComparer.OrdinalIgnoreCase))
            {
                findings.Add(new(LowercaseTypes, Where(sql, spec.Index, $"the length spec `{type}({spec.Value})` — data types are lowercase, `max` included")));
            }
        }

        return findings;
    }

    private static bool IsLowercase(string token) =>
        token.All(c => !char.IsLetter(c) || char.IsLower(c));

    private static string Where(string sql, int index, string what)
    {
        var line = sql.AsSpan(0, Math.Min(index, sql.Length)).Count('\n') + 1;
        var from = Math.Max(0, index - 45);
        var to = Math.Min(sql.Length, index + 45);

        return $"line {line}: {what} — …{Regex.Replace(sql[from..to], @"\s+", " ")}…";
    }

    /* ───────────────────────── the population ───────────────────────── */

    /// <summary>
    /// A statement keyword at the START of the literal, once the literal's own SQL comments are gone. Anchored
    /// at the start rather than at any line start, and the difference is measured: the two MCP instruction
    /// blobs are 40kb and 57kb of markdown about SQL Server, and one of them contains a line beginning
    /// <c>IF</c>. Anchoring at any line start admitted both.
    /// </summary>
    private static readonly Regex StatementOpener = new(
        @"\A\s*;?\s*(?:SELECT|INSERT|UPDATE|DELETE|MERGE|TRUNCATE|WITH|DECLARE|SET|IF|EXEC|EXECUTE|CREATE|ALTER|DROP|USE|GRANT|REVOKE|PRINT|RAISERROR|THROW|BEGIN|OPEN|FETCH|CLOSE|DEALLOCATE)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Tokens only T-SQL has. Generous, because a T-SQL statement carrying none of these falls outside the
    /// population silently — the dangerous direction — and
    /// <see cref="EverySqlServerCollectorDefinitionsFile_ContributesATsqlStatement"/> is the per-site check
    /// that the set has not gone blind on any collector.
    ///
    /// <para>The stated bound: this is a marker list, not a parser. T-SQL written with none of these tokens —
    /// a bare <c>SELECT a FROM b WHERE c = 1;</c> against a monitored server — is not judged. No such
    /// statement exists in the corpus, because every read of a monitored SQL Server goes through a DMV, a
    /// catalog view, or a built-in.</para>
    /// </summary>
    private static readonly Regex TsqlMarkers = new(
        @"\bsys\.[A-Za-z_]"
        + @"|\bdm_(?:os|exec|db|io|tran|hadr|resource|xe|server)_"
        + @"|OPTION\s*\(\s*RECOMPILE\s*\)"
        + @"|@@(?:SPID|ROWCOUNT|VERSION|SERVERNAME|TRANCOUNT|IDENTITY|ERROR)\b"
        + @"|\b(?:SERVERPROPERTY|DB_NAME|DB_ID|OBJECT_NAME|OBJECT_SCHEMA_NAME|OBJECT_ID|SCHEMA_NAME"
        + @"|SUSER_SNAME|HAS_DBACCESS|HAS_PERMS_BY_NAME|DATABASEPROPERTYEX|GETDATE|GETUTCDATE|SYSDATETIME"
        + @"|SYSUTCDATETIME|SYSDATETIMEOFFSET|ISNULL|DATEDIFF|DATEADD|DATEPART|CONVERT|STUFF|CHARINDEX)\s*\("
        + @"|\b(?:sysname|nvarchar|nchar|ntext|datetime2|smalldatetime|datetimeoffset|varbinary"
        + @"|uniqueidentifier|sql_variant)\b"
        + @"|\bWITH\s*\(\s*NOLOCK\s*\)"
        + @"|(?<![A-Za-z0-9_])N'"
        + @"|\bTOP\s*\("
        + @"|\bSET\s+TRANSACTION\s+ISOLATION\s+LEVEL"
        + @"|\bSET\s+NOCOUNT\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Whether the literal opens a SQL statement of ANY dialect — the half of the population test
    /// that is engine-neutral, so the PostgreSQL side of
    /// <see cref="EverySqlServerCollectorDefinitionsFile_ContributesATsqlStatement"/> can require that a file
    /// was actually read before concluding it holds no T-SQL.</summary>
    private static bool OpensAStatement(string literal) =>
        StatementOpener.IsMatch(CommentsBlanked(literal, SpanKinds(literal)));

    private static bool IsTsqlStatement(string literal)
    {
        ArgumentNullException.ThrowIfNull(literal);

        var view = CommentsBlanked(literal, SpanKinds(literal));

        return StatementOpener.IsMatch(view) && TsqlMarkers.IsMatch(view);
    }

    /* ───────────────────────── SQL span classification ───────────────────────── */

    private enum SqlSpan
    {
        Code,
        BlockComment,
        LineComment,
        StringLiteral,
        QuotedIdentifier,
    }

    /// <summary>
    /// What every character of a T-SQL statement IS. Block comments NEST in T-SQL, so the walk carries a
    /// depth: without it an inner <c>*&#47;</c> closes the outer comment and everything after it reads as
    /// code, which is how a comment ends up supplying a violation.
    ///
    /// <para>The literal text handed in is C# source, so a verbatim string's doubled <c>""</c> is still
    /// doubled here. That is deliberate — <c>"</c> is not treated as a delimiter at all, because the only
    /// place this corpus uses it is inside an XQuery expression that is already inside a single-quoted span,
    /// and treating it as a quoted identifier there would desynchronise the walk.</para>
    /// </summary>
    private static SqlSpan[] SpanKinds(string sql)
    {
        var kinds = new SqlSpan[sql.Length];
        var i = 0;

        while (i < sql.Length)
        {
            if (sql[i] == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                while (i < sql.Length && sql[i] != '\n')
                {
                    kinds[i++] = SqlSpan.LineComment;
                }

                continue;
            }

            if (sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                var depth = 0;

                while (i < sql.Length)
                {
                    if (sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                    {
                        depth++;
                        kinds[i++] = SqlSpan.BlockComment;
                        kinds[i++] = SqlSpan.BlockComment;
                        continue;
                    }

                    if (sql[i] == '*' && i + 1 < sql.Length && sql[i + 1] == '/')
                    {
                        depth--;
                        kinds[i++] = SqlSpan.BlockComment;
                        kinds[i++] = SqlSpan.BlockComment;

                        if (depth == 0)
                        {
                            break;
                        }

                        continue;
                    }

                    kinds[i++] = SqlSpan.BlockComment;
                }

                continue;
            }

            if (sql[i] == '\'')
            {
                kinds[i++] = SqlSpan.StringLiteral;

                while (i < sql.Length)
                {
                    if (sql[i] == '\'')
                    {
                        /* '' is an escaped quote: consume BOTH and stay inside the span. Consuming one and
                           then testing the next character instead reopens the span on the second quote and
                           closes it on the next one, so N'a''b' ends after the escape and `b'` reads as
                           code — a walk that desynchronises on the one form this repository's dynamic SQL
                           is full of. */
                        if (i + 1 < sql.Length && sql[i + 1] == '\'')
                        {
                            kinds[i++] = SqlSpan.StringLiteral;
                            kinds[i++] = SqlSpan.StringLiteral;
                            continue;
                        }

                        kinds[i++] = SqlSpan.StringLiteral;
                        break;
                    }

                    kinds[i++] = SqlSpan.StringLiteral;
                }

                continue;
            }

            if (sql[i] == '[')
            {
                while (i < sql.Length)
                {
                    var close = sql[i] == ']';
                    kinds[i++] = SqlSpan.QuotedIdentifier;

                    if (close)
                    {
                        break;
                    }
                }

                continue;
            }

            kinds[i] = SqlSpan.Code;
            i++;
        }

        return kinds;
    }

    /// <summary>
    /// An XML method call whose argument list is about to open. The first argument of <c>.value()</c>,
    /// <c>.query()</c>, <c>.nodes()</c>, <c>.exist()</c> and <c>.modify()</c> is XQuery, not T-SQL — XQuery's
    /// aggregate is <c>count()</c>, there is no <c>COUNT_BIG</c> in it, and both spellings ship here. The
    /// SECOND argument is a T-SQL type name and is left visible, which is what keeps
    /// <c>.value('…', 'NVARCHAR(MAX)')</c> checkable.
    /// </summary>
    private static readonly Regex XmlMethodArgument = new(
        @"\.\s*(?:value|query|nodes|exist|modify)\s*\(\s*N?\z",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Comments blanked; strings and quoted identifiers kept. What the population test reads, so that a
    /// marker spelled only inside a comment cannot admit prose, while <c>N'…'</c> and a bracketed name still
    /// count as the T-SQL they are.
    /// </summary>
    private static string CommentsBlanked(string sql, SqlSpan[] kinds) =>
        Project(sql, i => kinds[i] is not (SqlSpan.BlockComment or SqlSpan.LineComment));

    /// <summary>
    /// Code plus dynamic SQL: comments, quoted identifiers and XQuery arguments blanked, other single-quoted
    /// spans kept. What the token checks read.
    ///
    /// <para>Keeping non-XQuery strings is what let this guard see the <c>COUNT(*)</c> inside Lite's
    /// <c>EXEC sys.sp_executesql N'SELECT @c = COUNT(*) …'</c>: that text is T-SQL that runs. Blanking
    /// quoted identifiers is what keeps a column named <c>[INT]</c> from reading as a type.</para>
    /// </summary>
    private static string CodeAndDynamicSql(string sql, SqlSpan[] kinds)
    {
        var keep = new bool[sql.Length];

        for (var i = 0; i < sql.Length; i++)
        {
            keep[i] = kinds[i] is SqlSpan.Code or SqlSpan.StringLiteral;
        }

        for (var i = 0; i < sql.Length;)
        {
            if (kinds[i] != SqlSpan.StringLiteral)
            {
                i++;
                continue;
            }

            var end = i;

            while (end < sql.Length && kinds[end] == SqlSpan.StringLiteral)
            {
                end++;
            }

            /* The window ends BEFORE the opening quote. Including it left the pattern's \z past the `'` and
               nothing matched, so every XQuery expression in the corpus stayed visible and its count()
               read as a T-SQL COUNT( — a false positive on three shipped collectors. */
            if (XmlMethodArgument.IsMatch(sql[Math.Max(0, i - 48)..i]))
            {
                for (var k = i; k < end; k++)
                {
                    keep[k] = false;
                }
            }

            i = end;
        }

        return Project(sql, i => keep[i]);
    }

    /// <summary>Blanks to spaces while preserving length and newlines, so an offset still points where it
    /// did and a reported line number is the line the reader will find.</summary>
    private static string Project(string sql, Func<int, bool> keep)
    {
        var sb = new StringBuilder(sql.Length);

        for (var i = 0; i < sql.Length; i++)
        {
            sb.Append(keep(i) ? sql[i] : sql[i] == '\n' ? '\n' : ' ');
        }

        return sb.ToString();
    }

    /// <summary>Blanks every match of <paramref name="pattern"/>, keeping length AND newlines. Replacing a
    /// multi-line match with plain spaces would keep the offsets and lose the line breaks, so every line
    /// number reported after a blanked <c>FOR XML</c> — which this repository writes across three lines —
    /// would point above where the reader has to look.</summary>
    private static string Blank(string text, Regex pattern) =>
        pattern.Replace(text, m => new string(m.Value.Select(c => c == '\n' ? '\n' : ' ').ToArray()));

    /* ───────────────────────── the corpus ───────────────────────── */

    /// <summary>
    /// The trees whose T-SQL this guard judges, grouped so each group can be floored on its own. The shared
    /// libraries are where #3078's violation landed and both products build on them; the two app trees hold
    /// the rest — Darling's XE provisioning, server probe and plan fetch, Lite's remote collectors and FinOps
    /// reads.
    ///
    /// <para>Not here, each for a stated reason. <c>deprecated/</c> and <c>install/</c> belong to the retired
    /// Full edition. <c>upgrades/</c>, <c>tools/</c> and <c>.github/sql/</c> are standalone <c>.sql</c> —
    /// rigs, CI scripts and old migrations rather than shipped product SQL, and measured as carrying enough
    /// violations to need a waiver list larger than this guard. Both test projects are out because their
    /// T-SQL is COPIES: the <c>*CollectorDefinitionTests</c> parity constants are asserted equal to
    /// <c>BuildQuery(context).Text</c>, so a violation cannot exist in the copy without existing in the
    /// shipped query — and this file's own fixtures deliberately carry every banned shape.</para>
    /// </summary>
    private static readonly (string Tree, string[] Roots)[] ScannedTrees =
    {
        ("shared libraries", new[]
        {
            "PerformanceMonitor.Alerting",
            "PerformanceMonitor.Analysis",
            "PerformanceMonitor.Collectors",
            "PerformanceMonitor.Common",
            "PerformanceMonitor.Notifications",
            "PerformanceMonitor.PlanAnalysis",
            "PerformanceMonitor.Ui",
        }),
        ("Darling", new[] { "Darling" }),
        ("Lite", new[] { "Lite" }),
    };

    private static IEnumerable<string> SourceFiles(string[] roots, [CallerFilePath] string thisFile = "")
    {
        var repo = RepoRoot(thisFile);
        var tests = Path.Combine("Darling", "Darling.Tests") + Path.DirectorySeparatorChar;

        foreach (var root in roots)
        {
            var directory = Path.Combine(repo, root);

            if (!Directory.Exists(directory))
            {
                throw new DirectoryNotFoundException(
                    $"the scanned root '{root}' does not exist under {repo}, so this guard is reading nothing "
                    + "there. Fix the root list rather than letting the sweep report clean on a tree it never "
                    + "opened.");
            }

            foreach (var path in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
                .OrderBy(p => p, StringComparer.Ordinal))
            {
                var relative = Path.GetRelativePath(repo, path);

                if (relative.StartsWith(tests, StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return path;
            }
        }
    }

    /// <summary>The bullets under <c>CONTRIBUTING.md</c>'s <c>### T-SQL Style</c> heading, as
    /// (bold lead-in, rest of the bullet).</summary>
    private static IReadOnlyList<(string Heading, string Body)> ContributingTsqlBullets([CallerFilePath] string thisFile = "")
    {
        var text = File.ReadAllText(Path.Combine(RepoRoot(thisFile), "CONTRIBUTING.md"));

        var section = text.IndexOf("### T-SQL Style", StringComparison.Ordinal);
        Assert.True(section > 0, "CONTRIBUTING.md no longer has a '### T-SQL Style' heading.");

        var lead = text.IndexOf("Key points:", section, StringComparison.Ordinal);
        Assert.True(lead > section, "CONTRIBUTING.md's T-SQL Style section no longer opens with 'Key points:'.");

        var bullets = new List<(string, string)>();

        foreach (var raw in text[lead..].Split('\n').Skip(1))
        {
            var line = raw.Trim('\r', ' ', '\t');

            if (line.Length == 0)
            {
                if (bullets.Count > 0)
                {
                    break;
                }

                continue;
            }

            if (!line.StartsWith("- ", StringComparison.Ordinal))
            {
                break;
            }

            var match = Regex.Match(line, @"^-\s+\*\*(?<heading>[^*]+)\*\*:\s*(?<body>.+)$");

            Assert.True(
                match.Success,
                "a bullet in CONTRIBUTING.md's T-SQL Style list is no longer '- **Heading**: body', so this "
                + "guard's disposition map can no longer be compared against it: " + line);

            bullets.Add((match.Groups["heading"].Value.Trim(), match.Groups["body"].Value.Trim()));
        }

        return bullets;
    }

    /// <summary>The member a literal belongs to, for the failure message: the nearest declaration above
    /// it.</summary>
    private static string EnclosingMember(string text, int offset)
    {
        var head = text[..Math.Min(offset, text.Length)];

        var matches = Regex.Matches(
            head,
            @"(?:const\s+string|static\s+string|string)\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:=|=>)"
            + @"|(?<name2>[A-Za-z_][A-Za-z0-9_]*)\s*\([^)]*\)\s*(?:=>|\{)");

        for (var i = matches.Count - 1; i >= 0; i--)
        {
            var name = matches[i].Groups["name"].Success
                ? matches[i].Groups["name"].Value
                : matches[i].Groups["name2"].Value;

            if (!string.IsNullOrEmpty(name))
            {
                return name;
            }
        }

        return "<unknown>";
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);

        return dir!;
    }
}
