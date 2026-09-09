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
using static Darling.Tests.CSharpMemberMap;

namespace Darling.Tests;

/// <summary>
/// <para><c>CONTRIBUTING.md</c>'s <b>T-SQL Style</b> list opens "All T-SQL code must follow", and until #3081
/// nothing enforced any of it. PR #3078 introduced <c>COUNT(*)</c> and <c>COUNT(DISTINCT …)</c> into a new
/// collector query — a violation of the bullet at <c>CONTRIBUTING.md:355</c> — and passed the Linux build, the
/// PostgreSQL tests, every whole-tree guard, the command-deadline family, <c>review</c> and
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
/// alone admits prose — <c>ScheduleManager</c>'s collector descriptions name a DMV in nearly every line of
/// them, and the two MCP instruction blobs mention SQL Server throughout. The statement opener
/// alone admits the store's own PostgreSQL and DuckDB SQL, where <c>COUNT_BIG()</c> does not exist and
/// <c>--</c> is idiomatic; <c>DarlingServerConnector</c> is the clean demonstration, carrying the SAME
/// <c>-- #2228</c> comment twice, once in a T-SQL probe (fixed here) and once in a PostgreSQL probe (left
/// alone, correctly).</para>
///
/// <para><b>The marker set has a residue, and it is ENUMERATED rather than asserted away.</b> An earlier
/// draft of this comment said every read of a monitored SQL Server goes through a DMV, a catalog view or
/// a built-in, so nothing T-SQL could fall outside the population. Testing that instead of stating it
/// found two counter-examples — and one of them mattered: Darling's Extended Events provisioning DDL was
/// real T-SQL going to production servers with no marker in it at all. That is fixed (the markers now
/// reach <c>EVENT SESSION</c>) and what remains is pinned at set
/// equality by <see cref="TheTsqlTheMarkerSetCannotSee_IsTheseAndNoOthers"/>.</para>
///
/// <para><b>Stated bound: SQL assembled across several literals is judged per literal.</b>
/// <c>FactRemediation</c> renders a remediation script with one <c>sb.Append</c> per line, so
/// <c>DECLARE @plan_handle varbinary(64);</c> is in the population and the <c>-- STEP 2</c> line above it is
/// not — a literal that is only a comment opens no statement. That is a real gap and it is the price of a
/// population rule a reader can hold in their head. The collectors, where the #3078 violation landed, hold
/// whole statements in one literal each.</para>
///
/// <para><b>Stated bound: C# string literals only, not standalone <c>.sql</c> files.</b> The reason is a
/// measurement taken WHEN THIS LANDED and not re-taken since, so read it as the evidence for a decision
/// rather than as a live figure: the retired Full edition's <c>install/</c> carried 6 <c>COUNT()</c>, 6
/// <c>--</c> comments and 75 uppercase data types across 60 files; <c>upgrades/</c> carried 3
/// <c>@@ROWCOUNT</c>; the rigs and CI scripts under <c>tools/</c> and <c>.github/sql/</c> carried 9
/// <c>COUNT()</c> and 101 <c>--</c> comments. Two orders of magnitude more waivers than findings, all of
/// them against a retired SKU or a test rig, and a guard whose waiver list dwarfs its findings has stopped
/// being a guard. The two live SKUs and the shared libraries they both build on are what this scans, and
/// that population IS re-measured on every run by the floors below.</para>
///
/// <para><b>Both SKUs, and one class rather than two.</b> The scan reads the shared libraries, all of
/// <c>Darling/</c> and all of <c>Lite/</c> — the #3078 violation landed in <c>PerformanceMonitor.Collectors</c>,
/// which both products build on, and Lite's <c>RemoteCollectorService</c> and <c>LocalDataService.FinOps.*</c>
/// send T-SQL of their own. Each scanned tree is floored SEPARATELY in
/// <see cref="NoTsqlStatementViolatesACoveredConvention"/>, spelled identically, because an arm written as a
/// special case is how the next tree inherits the sibling bug #3067 was filed for. Duplicating the class into
/// <c>Lite.Tests</c> would buy nothing and drift: the <c>darling</c> path filter reaches
/// <c>Lite/**/*.cs</c> and the <c>core</c> filter reaches every shared library, so the "Run Darling tests"
/// step fires on a change to any tree this reads, and <c>darling-tree-guards</c> runs the whole suite on the
/// arm where it did not.</para>
///
/// <para><b>What holds that reachability, stated precisely rather than gestured at.</b>
/// <c>CrossAppGuardCiGateTests</c> requires every cross-app source read to be reachable by the filter that
/// gates its suite, and <c>CONTRIBUTING.md</c>'s "Writing a Test That Reads the Other SKU's Source" names
/// the three spellings it can see. A bare root iterated out of a <c>string[]</c> field is not one of them —
/// it is the "segment array declared in another member" case that section calls out as silent by
/// construction — and the bare <c>"Lite"</c> in <see cref="ScannedTrees"/> is exactly that. So the ANCHOR
/// paths carry the claim: each is a repo-rooted path literal at the site that reads it, which is the first
/// of the three, so <c>Lite/Services/LocalDataService.FinOps.Recommendations.cs</c> is in that guard's found
/// set and has to stay filter-reachable. Measured: with the anchors in place, pointing the Lite one at a
/// <c>Lite.Tests</c> path reds that guard by name; before they existed the same injection changed nothing,
/// which is how the gap was found rather than assumed. The injected path has to EXIST — that guard drops
/// references which do not resolve on disk, so a made-up filename is a red-proof that proves nothing.</para>
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
    private const string UnabbreviatedTypes = "data-types-unabbreviated";

    private const string UppercaseKeywords = "keywords-UPPERCASE";
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
    /// <para><b><c>Indentation</c> is the one split bullet</b> rather than all-or-nothing, and saying so
    /// is the point of the shape: it bans tabs and prescribes a depth, the ban is a token, the depth is a
    /// shape, and only the token is checked here. It is NAMED rather than counted, and
    /// <see cref="TheDispositionMap_PartitionsTheBulletsContributingStates"/> holds the naming, so a
    /// second split bullet cannot leave this prose quietly incomplete.
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
                Covered: new[] { LowercaseTypes, UnabbreviatedTypes },
                Uncovered: Array.Empty<string>(),
                Phrases: new[] { "lowercase", "never abbreviated", "`integer`", "`nvarchar(max)`", "`nvarchar(MAX)`" },
                Note: "Both halves are tokens with a single correct replacement, which is what makes them "
                    + "worth a guard. The CASE half reads the type vocabulary plus the `max` length spec, "
                    + "which no vocabulary match can see on its own. The ABBREVIATION half is `int` where "
                    + "`integer` is meant, and it is anchored on BOTH sides: without the trailing lookahead "
                    + "`integer` itself contains the token under test, and without the leading lookbehind so "
                    + "do `bigint`, `smallint` and `tinyint` — three type names this corpus writes and the "
                    + "bullet does not object to. `dec` for `decimal` and `double precision` for `float` are "
                    + "the other abbreviations T-SQL accepts and neither is in the detector: a stated bound "
                    + "on the CHECK rather than on the rule."),

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
    /// <para><b>There is deliberately no waiver list.</b> The covered subset measures zero on the tree
    /// with no exceptions carried, and every rule in it got there by having the tree's existing violations
    /// REWRITTEN rather than waived — the abbreviation rule's <c>int</c> spellings included. A rule whose
    /// cost is paid by a waiver is a rule this guard only appears to hold, and a waiver mechanism nobody
    /// needs is a mechanism the next exception takes for granted.</para>
    /// </summary>
    [Fact]
    public void NoTsqlStatementViolatesACoveredConvention()
    {
        var treesScanned = 0;
        var filesScanned = 0;
        var literalsScanned = 0;
        var statementsScanned = 0;
        var offenders = new List<string>();

        var repo = RepoRoot();

        foreach (var (tree, roots, anchor) in ScannedTrees)
        {
            treesScanned++;
            var statementsInTree = 0;
            var anchorStatements = 0;
            var anchorPath = Path.GetFullPath(
                Path.Combine(repo, anchor.Replace('/', Path.DirectorySeparatorChar)));

            foreach (var path in SourceFiles(roots))
            {
                filesScanned++;
                var text = File.ReadAllText(path);
                var name = Path.GetFileName(path);
                var isAnchor = string.Equals(path, anchorPath, StringComparison.Ordinal);

                /* Once per file at most, and only once a finding needs it: the map walks the whole
                   source, and most scanned files hold no T-SQL at all. */
                MemberMap? members = null;

                foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
                {
                    literalsScanned++;

                    if (!IsTsqlStatement(body))
                    {
                        continue;
                    }

                    statementsScanned++;
                    statementsInTree++;

                    if (isAnchor)
                    {
                        anchorStatements++;
                    }

                    foreach (var (rule, detail) in Findings(body))
                    {
                        members ??= Of(text);
                        offenders.Add(
                            $"{name}:{LineOf(text, start)} in {EnclosingMember(members, start)} "
                            + $"[{rule}] {detail}");
                    }
                }
            }

            /* Per tree, spelled the same way for every tree. A union floored only in total passes while one
               tree contributes nothing, and the tree that goes dark is the one nobody is looking at. */
            Assert.True(
                statementsInTree > 0,
                $"the {tree} tree contributed no T-SQL statement at all, so every check below is vacuous "
                + $"there. Roots read: {string.Join(", ", roots)}.");

            /* And the same requirement on a NAMED file, which a per-tree total cannot give: the tree still
               contributes when the glob reaches only part of it, or when the marker set goes blind on the
               one shape that file uses. */
            Assert.True(
                anchorStatements > 0,
                $"the {tree} tree's anchor file {anchor} contributed no T-SQL statement, so the tree total "
                + "above is being satisfied by other files. Either the sweep no longer reaches that path or "
                + "the discriminator no longer recognises the SQL in it.");
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

    /// <summary>
    /// The T-SQL this guard's marker set does NOT reach, ENUMERATED and compared for equality — because the
    /// first version of this file claimed there was none, and testing that claim found two.
    ///
    /// <para><b>Why the claim needed testing rather than stating.</b> "Every read of a monitored SQL Server
    /// goes through a DMV, a catalog view or a built-in" is a universal quantifier, and a universal
    /// quantifier names the one counter-example that would break it while a count only tells you to count
    /// again. Enumerating the complement of the population — literals that open a statement and carry no
    /// marker — and reading the members found two things the tally could never have shown: Darling's
    /// Extended Events provisioning DDL (<c>CREATE EVENT SESSION … ADD EVENT sqlserver.blocked_process_report
    /// …</c>) was real T-SQL sent to production servers and entirely outside the population, and
    /// <c>FactRemediation</c>'s cursor fetch still is. The first was fixed by adding
    /// <c>EVENT SESSION</c> to <see cref="TsqlMarkers"/>. The second
    /// is here.</para>
    ///
    /// <para><b>The discriminator, and why it is narrow on purpose.</b> The complement is over 500 literals,
    /// most of them prose that happens to start with a keyword and store SQL for the other two dialects. What
    /// separates T-SQL from both is the parameter spelling: <c>@name</c> is T-SQL's, <c>$1</c> is Npgsql's.
    /// So the candidate set is "opens a statement, no T-SQL marker, no positional parameter, has an
    /// <c>@name</c>" — which is a small enough set to READ, and reading it is the point. A T-SQL statement
    /// with no marker AND no parameter at all would still be outside this net; that is the residue, and it is
    /// a smaller residue than the one this test was written to measure.</para>
    /// </summary>
    [Fact]
    public void TheTsqlTheMarkerSetCannotSee_IsTheseAndNoOthers()
    {
        var unmarked = 0;
        var candidates = new List<string>();

        foreach (var (_, roots, _) in ScannedTrees)
        {
            foreach (var path in SourceFiles(roots))
            {
                var text = File.ReadAllText(path);

                foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
                {
                    var view = CommentsBlanked(body, SpanKinds(body));

                    if (!StatementOpener.IsMatch(view) || TsqlMarkers.IsMatch(view))
                    {
                        continue;
                    }

                    unmarked++;

                    if (PositionalParameter.IsMatch(view) || !NamedParameter.IsMatch(view))
                    {
                        continue;
                    }

                    candidates.Add(
                        $"{Path.GetFileName(path)}: {Regex.Replace(body.Trim(), @"\s+", " ")}");
                }
            }
        }

        /* The complement has to be non-empty, or the enumeration below is over nothing and its equality is
           satisfied by a broken upstream filter rather than by the tree. */
        Assert.True(
            unmarked > 0,
            "no literal in the corpus opens a statement without carrying a T-SQL marker, which cannot be "
            + "true while the stores' own PostgreSQL and DuckDB SQL is in the scanned trees. The statement "
            + "opener or the marker set has broken, not the tree.");

        Assert.Equal(
            new[]
            {
                /* FactRemediation renders its plan-cache clear script one sb.Append per line, and this line
                   is the one with no DMV, no catalog view and no built-in in it. Twice, because two of the
                   rendered scripts carry the same cursor loop. Both are already outside the population by
                   the per-literal bound as well — every other line of those scripts is judged — so closing
                   this would mean resolving concatenation rather than reading literals. */
                "FactRemediation.cs: FETCH NEXT FROM plan_cursor INTO @plan_handle;",
                "FactRemediation.cs: FETCH NEXT FROM plan_cursor INTO @plan_handle;",
            },
            candidates.OrderBy(c => c, StringComparer.Ordinal).ToArray());
    }

    /// <summary>Npgsql's positional parameter, which is how the store's own SQL announces itself.</summary>
    private static readonly Regex PositionalParameter = new(
        @"\$\d", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>T-SQL's parameter and variable spelling — the one thing neither PostgreSQL nor DuckDB
    /// writes, which is what makes it usable as a dialect tell where the marker set has gone quiet.</summary>
    private static readonly Regex NamedParameter = new(
        @"(?<![A-Za-z0-9_@])@[A-Za-z_]", RegexOptions.Compiled | RegexOptions.CultureInvariant);

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

        /* The abbreviation ratchet, at the BULLET rather than through the flattened lists. Both
           equalities above are over a UNION of every bullet's arrays, so neither can see which bullet a
           rule sits under: moving UnabbreviatedTypes to the Functions bullet's Covered array satisfies
           all of them, and the map then claims this guard enforces a sentence CONTRIBUTING.md does not
           have it enforcing. This is the direction nothing else holds.

           The OTHER direction — the rule drifting back to Uncovered while Findings still emits it — is
           already held, and stating where is what keeps a decorative assertion from being added here
           later. Putting it back on the Uncovered side makes the flattened Uncovered union disagree with
           UncoveredRules two assertions up; adding it to UncoveredRules as well instead trips the
           disjointness one line up, because Covered still holds it. A DoesNotContain here could
           therefore never be the assertion that reds, and an assertion that cannot fail is the shape
           #3081 was filed about. */
        Assert.Contains(UnabbreviatedTypes, RuleBullets["Data types"].Covered);

        /* The split bullets, NAMED in RuleBullets' own summary rather than counted there. A second one
           would leave that prose incomplete without contradicting it, which is the quiet direction. */
        Assert.Equal(
            new[] { "Indentation" },
            RuleBullets
                .Where(b => b.Value.Covered.Length > 0 && b.Value.Uncovered.Length > 0)
                .Select(b => b.Key)
                .OrderBy(k => k, StringComparer.Ordinal)
                .ToArray());
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
    /// recommendations, <c>DarlingServerConnector</c>'s <c>--</c>, and the data types not spelled in
    /// lowercase.
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
            /* The ABBREVIATION half of the same bullet, and every fixture for it is spelled LOWERCASE on
               purpose: the case half then stays silent, so what these record is the abbreviation check
               firing rather than a second rule covering for it. */
            ("SELECT c = CONVERT(int, w.waiting_tasks_count) FROM sys.dm_os_wait_stats AS w OPTION(RECOMPILE);", UnabbreviatedTypes),
            ("DECLARE @on_pos int; SELECT @on_pos = CHARINDEX(N' on ', @@VERSION);", UnabbreviatedTypes),
            /* A DDL type position rather than an expression one: a temp table's and a table variable's
               column types were both among the corpus's own spellings, and a check anchored only on
               CONVERT( would have missed every one of them. */
            ("SET NOCOUNT ON; CREATE TABLE #file_space (database_id int NOT NULL); SELECT 1 FROM sys.databases AS d;", UnabbreviatedTypes),
            /* An sp_executesql PARAMETER DECLARATION, which is where the corpus's remaining doubt sat: it
               is a type position like any other, `integer` is accepted in it, and the engine binds the two
               spellings to the same type. Verified against a live SQL Server rather than reasoned about,
               because nothing in this repository executes that path. */
            ("EXEC sys.sp_executesql N'SELECT probe = @a;', N'@a int', @a = 1;", UnabbreviatedTypes),
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
            /* An identifier that ENDS in Count and is then called, which is the only thing the COUNT(
               pattern's lookbehind actually decides — COUNT_BIG( and ROWCOUNT_BIG() are both rescued by the
               `_` that follows COUNT, not by what precedes it, and @@ROWCOUNT is never followed by `(`. So
               without a fixture of this shape the lookbehind changed no answer and read as load-bearing
               while nothing held it. Synthetic: ordinary SQL, absent from this corpus. */
            "SELECT n = dbo.RowCount(d.database_id) FROM sys.databases AS d OPTION(RECOMPILE);",
            /* XQuery, and it is why the code view blanks an XML method's FIRST argument only. XQuery's
               function is count() and there is no COUNT_BIG in it; both spellings ship in
               DeadlocksCollector and BlockedProcessReportCollector. The SECOND argument is a T-SQL type
               name and stays visible, which is what keeps 'NVARCHAR(MAX)' there checkable. */
            "SELECT o = fr.n.value('let $c := . return count(../frame[. << $c])', 'integer')\nFROM sys.fn_xe_file_target_read_file(N'x', NULL, NULL, NULL) AS f\nCROSS APPLY f.event_data.nodes('//frame') AS fr(n) OPTION(RECOMPILE);",
            /* The block-comment continuation line, which is the #3052 defect this check must not have: this
               codebase writes no asterisk on continuation lines, and collector SQL here
               carries a `--` inside one - MemoryPressureEventsCollector and DmvBlockingSnapshotCollector
               among them. */
            "SELECT d.name\n/* memory_pressure_events.sample_time is naive UTC -- Darling passes it to\n   the viewer unchanged, so the frame is the collector's. */\nFROM sys.databases AS d OPTION(RECOMPILE);",
            /* A `--` AFTER a nested block comment closes but still inside the outer one. T-SQL nests block
               comments; a non-nesting walk ends the outer comment at the inner `*​/` and reads everything
               after it as code, so this `--` becomes a line comment. The `--` has to sit after the inner
               close for the fixture to tell the two walks apart — with it BEFORE the close, both walks have
               it inside a comment and the case records nothing. It survived a de-nesting mutation written
               the first way. */
            "SELECT d.name\n/* outer /* inner */ -- still inside the outer comment */\nFROM sys.databases AS d OPTION(RECOMPILE);",
            /* A `--` inside a string VALUE is data, not a comment. */
            "SELECT d.name FROM sys.databases AS d WHERE d.name <> N'a--b' OPTION(RECOMPILE);",
            /* The same thing AFTER a '' escape, which is what makes this fixture worth its line: a walk that
               consumes one quote and then looks at the next character closes the span on the escape and
               reads the remainder as code, so the `--` becomes a line comment. Every piece of dynamic SQL
               in this repository doubles its quotes — LocalDataService.FinOps.Recommendations wraps a
               CASE expression full of N''…'' literals inside one sp_executesql — so the walk has to
               survive the form. */
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
            /* And one containing an ESCAPED bracket, which is what makes the ]] arm of the walk
               load-bearing: stopping at the first `]` leaves `]INT]` outside the identifier and in the
               code stream. Synthetic - no identifier in this corpus embeds a bracket - and added
               because the review of this change pointed out the asymmetry with the '' arm. */
            "SELECT [a]]INT] = d.database_id FROM sys.databases AS d OPTION(RECOMPILE);",
            /* MAX the aggregate, not MAX the length spec. */
            "SELECT m = MAX(osi.runnable_tasks_count) FROM sys.dm_os_schedulers AS osi OPTION(RECOMPILE);",
            /* And the adversarial version, which is what makes the length spec's type ANCHOR load-bearing:
               an identifier called with a single argument spelled `MAX`. `MAX` is not a reserved word in
               T-SQL, so a column may be named it, and `MAX(MAX)` is then `IDENT ( max )` — textually the
               same shape as `nvarchar(max)`. Synthetic rather than drawn from the corpus, deliberately: the
               shape does not occur here, and without it dropping the anchor changed no answer at all, so the
               anchor read as load-bearing while nothing held it. */
            "SELECT largest = MAX(MAX) FROM dbo.metrics AS mt OPTION(RECOMPILE);",
            /* @@ROWCOUNT spelled inside a comment, which is where this codebase discusses it. */
            "SELECT d.name FROM sys.databases AS d /* ROWCOUNT_BIG(), never @@ROWCOUNT */ OPTION(RECOMPILE);",
            /* The abbreviation check's near misses. Every token here CONTAINS `int` and none of them is
               the violation, so this is where an unanchored or greedy match reds. `integer` is the
               CORRECT spelling — a detector matching it would report every rewritten site as the
               violation the rewrite removed, which is every site in the corpus. `bigint`,
               `smallint` and `tinyint` are type names this corpus writes; the leading lookbehind is the
               only thing holding them. */
            "DECLARE @a integer, @b bigint, @c smallint, @d tinyint; SELECT @a = d.database_id FROM sys.databases AS d;",
            /* An identifier merely CONTAINING the token, at each end: `int_col` is held by the trailing
               lookahead's `_` and `sysint` by the lookbehind. Synthetic — ordinary SQL, absent from this
               corpus — and the two directions are separate cases because one anchor cannot hold both. */
            "SELECT int_col = d.database_id, sysint = d.source_database_id FROM sys.databases AS d OPTION(RECOMPILE);",
            /* A VARIABLE named @int, which is what the `@` in the lookbehind is for: after a sigil the
               token is a name, and this codebase's dynamic SQL is full of parameter names. */
            "DECLARE @int integer; SELECT @int = COUNT_BIG(*) FROM sys.databases AS d;",
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
            /* The Extended Events provisioning DDL, which carried no marker at all until an
               enumeration of the population's complement found it. It reaches a monitored server on
               every session repair, so it belongs in the population as much as any collector query. */
            "CREATE EVENT SESSION [darling_deadlocks] ON SERVER\n    ADD EVENT sqlserver.xml_deadlock_report\n    ADD TARGET package0.ring_buffer (SET max_memory = 4096);",
            "ALTER EVENT SESSION [darling_deadlocks] ON SERVER STATE = START;",
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
            /* Prose that names DMVs — the shape ScheduleManager's collector descriptions all have. */
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

        var exercised = 0;

        foreach (var (rule, sql) in unguarded)
        {
            exercised++;

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

        Assert.Equal(unguarded.Length, exercised);

        Assert.Equal(
            UncoveredRules.OrderBy(r => r, StringComparer.Ordinal).ToArray(),
            unguarded.Select(u => u.Rule).Distinct(StringComparer.Ordinal).OrderBy(r => r, StringComparer.Ordinal).ToArray());
    }

    /* ───────────────────────── the offender label, pinned against the real tree ───────────────────────── */

    /// <summary>
    /// One site the offender label has to name correctly: a file, a distinctive substring of the T-SQL
    /// literal that locates it, and the member that literal is written inside.
    ///
    /// <para><b>Located by anchor rather than by offset, because an offset is a frozen enumeration.</b>
    /// All five of these are live source files that change most weeks, and a literal's character offset
    /// moves when anything above it is edited. A pinned integer would go red for a reason unrelated to
    /// attribution, and the next reader would repair it by updating the number — which silently retargets
    /// the pin at whatever literal now sits there. The anchor is content, so it travels with the literal,
    /// and <see cref="TheOffenderLabel_NamesTheMemberAtEverySiteInTheFixtureTable"/> requires it to match
    /// exactly one literal in the file.</para>
    ///
    /// <para><c>UsedToReport</c> is what the predecessor said at that site, and <c>WasCorrect</c> says
    /// whether that was right. It is not decoration: the table asserts its own rows are consistent with it,
    /// so a row claiming to fix a misattribution whose expectation equals the old answer pins nothing and
    /// reds. The <c>ServerPropertiesCollector</c> row is the one that was already correct and is carried
    /// precisely so the change cannot be shown to work by breaking it.</para>
    /// </summary>
    private static readonly (string File, string Anchor, string Member, string UsedToReport, bool WasCorrect, string What)[]
        MemberAttributionSites =
    {
        ("Darling/PerformanceMonitor.Darling.Analysis/PgPlanFetcher.cs",
            "N'@h varbinary(64), @stmt_start integer, @stmt_end integer'",
            "FetchPlanBySqlHandleAsync", "SqlConnectionStringBuilder", false,
            "a TYPE being constructed. The nearest match above the literal was "
            + "new SqlConnectionStringBuilder(connectionString) { … }, which is a class name, not a member "
            + "of this file at all — so the label sent a reader to look for a member that does not exist "
            + "here."),

        ("Lite/Services/LocalDataService.FinOps.Recommendations.cs",
            "DECLARE @role nvarchar(20) = N'Standalone';",
            "GetAgReplicaRoleAsync", "sql", false,
            "a LOCAL, and right for the wrong reason. The literal is the initialiser of const string sql, "
            + "so the old pattern matched the variable it is assigned to — close enough to look correct and "
            + "wrong as soon as the same method holds two queries, which is the shape three of the other "
            + "sites in this file have."),

        ("Lite/Services/LocalDataService.QueryStore.cs",
            "FROM sys.query_store_plan AS qsp",
            "FetchQueryStorePlanAsync", "if", false,
            "a bare KEYWORD. if (quotedDbName == null) { matched the second alternative, which asked only "
            + "for an identifier, a parenthesised anything and an opening brace — a description of most C# "
            + "statements."),

        ("PerformanceMonitor.Collectors/IndexObjectStatsCollector.cs",
            "/* Size + row counts (one scan of dm_db_partition_stats) */",
            "BuildPerDatabaseStatsBody", "optimizeForSequentialKey", false,
            "a LOCAL again, and the one that reads most like a real answer: optimizeForSequentialKey is a "
            + "string variable holding a column expression, so the label named a fragment of the query "
            + "rather than the method that builds it."),

        ("PerformanceMonitor.Collectors/ServerPropertiesCollector.cs",
            "the deferred-object-ref pattern, NOT a version guard (#980)",
            "QueryText", "QueryText", true,
            "the case that was already RIGHT, because the literal is a member initialiser and the member's "
            + "own declaration was the nearest match above it. Carried so the rewrite cannot be shown to "
            + "work by breaking the shape that worked."),
    };

    /// <summary>
    /// The label names the enclosing member at every site in the table.
    ///
    /// <para><b>Why the message matters as much as the detection.</b> Detection is untouched by any of
    /// this — the same literals are read, the same rules fire, the offender COUNT is identical. But the
    /// offender list is the guard's entire output at the moment it reds, and it was sending readers to a
    /// type name, a local variable or the word <c>if</c>. A guard that detects correctly and then
    /// misdirects is worse than one that says nothing, because the reader has no reason to doubt it.</para>
    /// </summary>
    [Fact]
    public void TheOffenderLabel_NamesTheMemberAtEverySiteInTheFixtureTable()
    {
        var repo = RepoRoot();
        var exercised = 0;

        foreach (var site in MemberAttributionSites)
        {
            /* A row that expects what the predecessor already said, while claiming to correct it, pins
               nothing — and a row that claims the old answer was right while expecting a different one is
               a contradiction. Both directions, so the table cannot quietly stop being a fixture. */
            Assert.Equal(site.WasCorrect, string.Equals(site.Member, site.UsedToReport, StringComparison.Ordinal));

            var path = Path.GetFullPath(
                Path.Combine(repo, site.File.Replace('/', Path.DirectorySeparatorChar)));

            Assert.True(
                File.Exists(path),
                $"the fixture site {site.File} no longer exists, so this row checks nothing. Move the row "
                + "to wherever the literal went rather than deleting it — the shape it pins is the point, "
                + "not the file.");

            var text = File.ReadAllText(path);
            var map = Of(text);

            /* The offset is DERIVED from the anchor, through the same literal walk the scan uses, so the
               site cannot drift onto a different literal and cannot be pinned to a stale integer. */
            var located = CSharpSourceWalker.StringLiteralBodies(text)
                .Where(l => l.Text.Contains(site.Anchor, StringComparison.Ordinal))
                .ToList();

            Assert.True(
                located.Count == 1,
                $"the anchor for {site.File} matched {located.Count} string literals, so it no longer "
                + $"identifies one site. Anchor: {site.Anchor}");

            var (start, body) = located[0];

            /* And it has to be in the population, or the row records nothing about a guard that only ever
               labels T-SQL. #3079's shape: a fixture the population filters out first. */
            Assert.True(
                IsTsqlStatement(body),
                $"the literal anchored in {site.File} is no longer read as a T-SQL statement, so the guard "
                + "would never label it and this row is vacuous.");

            Assert.Equal(site.Member, EnclosingMember(map, start));

            /* The line half of the label, cross-checked by a DIFFERENT derivation: LineOf counts
               newlines in a span, this reads the file as lines and finds the anchor's own line. The
               literal starts at or above its anchor, so the reported line must not be past it.
               Asserting instead that the line falls inside the resolved member's range would be a
               tautology — the member was selected by containing this very offset and LineOf is
               monotonic in it, so the comparison could not disagree with what it validates (#3089's
               finding about the same shape). */
            var line = LineOf(text, start);
            var anchorLine = Array.FindIndex(
                File.ReadAllLines(path),
                l => l.Contains(site.Anchor, StringComparison.Ordinal)) + 1;

            Assert.True(
                anchorLine > 0 && line <= anchorLine,
                $"the label for {site.File} reports line {line}, but the anchor is on line {anchorLine} "
                + "of the file as read by lines. The reported line is what a reader opens the file at, so "
                + "it must not point past the literal it labels.");

            exercised++;
        }

        Assert.Equal(MemberAttributionSites.Length, exercised);
    }

    /// <summary>
    /// Every T-SQL literal in the whole corpus is attributed to something that can be a member name.
    ///
    /// <para><b>This is what turns five fixture rows into the population.</b> The table above pins the
    /// sites that were measured; this pins the ones nobody looked at, and on the tree as it shipped it is
    /// the assertion that carried the finding: 48 of the 160 T-SQL literals the scan reads were labelled
    /// with a bare C# keyword or with nothing at all. A per-site table can never say that, because the
    /// sites it does not name are exactly where the next one will be.</para>
    ///
    /// <para>The keyword list is the check that shares no code with the resolver: <c>if</c>, <c>catch</c>
    /// and <c>foreach</c> are not member names in any C# program, so a label that is one is wrong without
    /// anything having to agree about how members are found. <see cref="Unknown"/> is the other arm, and it
    /// is the resolver's own admission — it is what a shape the declaration regex cannot read resolves to,
    /// which is why that regex is allowed to be narrow.</para>
    ///
    /// <para><b>What these two arms bracket is keyword-or-nothing, NOT wrong-or-nothing, and the
    /// difference is the whole limitation of this assertion.</b> A label that is a plausible
    /// user-defined identifier but not the enclosing member passes both arms in silence. That is not a
    /// hypothetical shape: of the four misattributions
    /// <see cref="MemberAttributionSites"/> was built from, only <c>if</c> was a keyword —
    /// <c>SqlConnectionStringBuilder</c> (a BCL type), <c>optimizeForSequentialKey</c> and <c>sql</c>
    /// (locals) are all identifier-shaped, so THREE of the four are shapes this test cannot see. They are
    /// pinned by exact comparison at five sites; a regression to that shape anywhere in the rest of the
    /// corpus is undetected here and is caught only if it happens to land on one of those five.</para>
    ///
    /// <para><b>The consequence worth naming: widening <see cref="DeclarationHead"/>'s modifier list is
    /// fixture-guarded only.</b> That field's own note explains that admitting a bare <c>const</c> or
    /// <c>static</c> would re-admit the local-variable case — and a re-admitted local produces an
    /// identifier-shaped label, which is precisely what this assertion is blind to. So the guard against
    /// that widening is a comment on <see cref="DeclarationHead"/> plus five fixture rows, not a
    /// corpus-scale test. The two comments point at each other deliberately.</para>
    ///
    /// <para><b>And the obvious detector is a trap, so it is deliberately absent.</b> Asserting that the
    /// label appears among <c>map.Declarations</c>' names cannot fail: <see cref="EnclosingMember"/>
    /// returns <see cref="DeclaredRange.Name"/> taken from that very map, so the check and the thing it
    /// validates are the same value read twice. It is #3089's tautology one artifact over, and a pin that
    /// cannot fail is worse than a limitation that is written down — it converts this paragraph into
    /// false confidence. Closing the gap for real needs a second, independent derivation of "which member
    /// is this offset in", which is a bigger change than the message this PR fixes.</para>
    /// </summary>
    [Fact]
    public void EveryTsqlLiteralInTheCorpus_IsAttributedToADeclaredMember()
    {
        var repo = RepoRoot();
        var literals = 0;
        var offenders = new List<string>();

        foreach (var (tree, roots, _) in ScannedTrees)
        {
            var inTree = 0;

            foreach (var path in SourceFiles(roots))
            {
                var text = File.ReadAllText(path);
                MemberMap? map = null;

                foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
                {
                    if (!IsTsqlStatement(body))
                    {
                        continue;
                    }

                    literals++;
                    inTree++;
                    map ??= Of(text);
                    var member = EnclosingMember(map, start);

                    if (member == Unknown || CSharpKeywords.Contains(member))
                    {
                        offenders.Add(
                            $"{Path.GetRelativePath(repo, path).Replace('\\', '/')}:{LineOf(text, start)} "
                            + $"-> '{member}'");
                    }
                }
            }

            Assert.True(
                inTree > 0,
                $"the {tree} tree contributed no T-SQL literal, so nothing here was attributed at all and "
                + "this assertion is vacuous there.");
        }

        Assert.True(literals > 0, "the literal walk extracted no T-SQL from the corpus");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} of {literals} T-SQL literals are labelled with a C# statement keyword or "
            + "with nothing, so the offender list this guard prints when it reds would send a reader to the "
            + "wrong place — or to no place. Either the member is declared in a shape DeclarationHead does "
            + "not read (an accessor, an attribute argument, or a member with no access modifier — all "
            + "stated on that field), or the brace walk lost the body, which "
            + nameof(TheMemberScan_ReadsEveryDeclarationWhole) + " names separately."
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// Every C# reserved keyword, none of which can be an identifier and therefore none of which can be a
    /// member name.
    ///
    /// <para><b>The independent half of the assertion above.</b> It agrees with nothing in the resolver, so
    /// it cannot pass by the resolver and the check making the same mistake — which a second derivation of
    /// "what are this file's members" would be free to do. The whole reserved set rather than the statement
    /// keywords that were actually observed (<c>if</c>, <c>catch</c>, <c>foreach</c>): a resolver that
    /// starts returning a MODIFIER or a built-in type name is the same defect one token over, and
    /// <c>readonly</c> is the measured instance — dropping the "a parameter list directly follows the
    /// name" test in <see cref="DeclaredName"/> makes a tuple-typed field resolve to <c>readonly</c>, which
    /// a list of statement keywords would have let through.</para>
    /// </summary>
    private static readonly HashSet<string> CSharpKeywords = new(StringComparer.Ordinal)
    {
        "abstract", "as", "base", "bool", "break", "byte", "case", "catch", "char", "checked", "class",
        "const", "continue", "decimal", "default", "delegate", "do", "double", "else", "enum", "event",
        "explicit", "extern", "false", "finally", "fixed", "float", "for", "foreach", "goto", "if",
        "implicit", "in", "int", "interface", "internal", "is", "lock", "long", "namespace", "new",
        "null", "object", "operator", "out", "override", "params", "private", "protected", "public",
        "readonly", "ref", "return", "sbyte", "sealed", "short", "sizeof", "stackalloc", "static",
        "string", "struct", "switch", "this", "throw", "true", "try", "typeof", "uint", "ulong",
        "unchecked", "unsafe", "ushort", "using", "virtual", "void", "volatile", "while",

        /* Contextual, and every one of them can begin a statement or an accessor, so a label that is one
           is a scan that stopped at the wrong token rather than a member with an unusual name. */
        "add", "async", "await", "get", "init", "partial", "record", "remove", "required", "set", "var",
        "when", "where", "yield",
    };

    /// <summary>
    /// Every member declaration's range is one whole member.
    ///
    /// <para><b>Attribution is silent in both failing directions, which is why each is asserted
    /// separately.</b> An OVER-EXTENDED body keeps containing the literals below it AND the next member's,
    /// so it hands out a name that is confidently wrong. An UNDER-READ body stops containing the literals
    /// below the cut, and a literal contained by nothing is labelled <see cref="Unknown"/> — which is only
    /// loud if some census is looking for a site of that kind. Thirteen members strand a literal today and
    /// no census looks for those, so the whole class read as healthy until the ranges themselves were
    /// checked against a second derivation.</para>
    ///
    /// <para>The two arms are complementary rather than redundant, and both are pinned by
    /// <see cref="TheMemberScan_IsBoundedByTheNextDeclaration_AndByTheBraceWalkAtTheEndOfTheFile"/>:
    /// the over-run arm compares the brace walk against a regex offset, so it is independent of the walk
    /// but structurally blind at the end of the file, where there is no next declaration to run past. The
    /// unterminated arm is what covers that tail. #3089 measured the same pair on the same kind of
    /// scan.</para>
    ///
    /// <para>The floors are derived, not counted. An exact declaration total would restate the size of the
    /// tree and go stale on the next commit — the thing this guard's own fixture table is written to
    /// avoid — so what is required is that every scanned tree contributed members and that the anchor file
    /// did.</para>
    /// </summary>
    [Fact]
    public void TheMemberScan_ReadsEveryDeclarationWhole()
    {
        var repo = RepoRoot();
        var members = 0;
        var truncated = new List<string>();
        var overExtended = new List<string>();
        var overlapping = new List<string>();
        var stoppedShort = new List<string>();

        foreach (var (tree, roots, anchor) in ScannedTrees)
        {
            var inTree = 0;
            var inAnchor = 0;
            var anchorPath = Path.GetFullPath(
                Path.Combine(repo, anchor.Replace('/', Path.DirectorySeparatorChar)));

            foreach (var path in SourceFiles(roots))
            {
                var map = Of(File.ReadAllText(path));
                var relative = Path.GetRelativePath(repo, path).Replace('\\', '/');
                var previousEnd = -1;
                var previousName = string.Empty;

                foreach (var declaration in map.Declarations)
                {
                    if (declaration.Kind != DeclarationKind.Member)
                    {
                        continue;
                    }

                    members++;
                    inTree++;

                    if (string.Equals(path, anchorPath, StringComparison.Ordinal))
                    {
                        inAnchor++;
                    }

                    var where = $"{relative}:{LineOf(map.Code, declaration.Start)} {declaration.Name}";

                    switch (ShapeOf(declaration))
                    {
                        case RangeShape.Unterminated:
                            truncated.Add(where);
                            continue;

                        case RangeShape.OverExtended:
                            overExtended.Add(
                                $"{where} runs to line {LineOf(map.Code, declaration.End - 1)}, past the "
                                + $"declaration at line {LineOf(map.Code, declaration.NextStart)}");
                            continue;

                        case RangeShape.Truncated:
                            stoppedShort.Add($"{relative} {declaration.Name}");
                            continue;

                        case RangeShape.WholeMember:
                        default:
                            break;
                    }

                    /* Members do not nest, so two member ranges overlapping means one of them swallowed the
                       other — the same defect the bound above catches, asked over the ranges rather than
                       over the declaration offsets, and it stays meaningful for the last member in a file
                       where the bound has nothing to compare against. */
                    if (declaration.Start < previousEnd)
                    {
                        overlapping.Add($"{where} starts inside {previousName}");
                    }

                    previousEnd = declaration.End;
                    previousName = declaration.Name;
                }
            }

            Assert.True(inTree > 0, $"no member declaration was read anywhere in the {tree} tree");
            Assert.True(
                inAnchor > 0,
                $"the {tree} tree's anchor file {anchor} contributed no member declaration, so the tree "
                + "total above is being satisfied by other files.");
        }

        Assert.True(members > 0, "the declaration sweep read no member at all");

        Assert.True(
            truncated.Count == 0,
            "these member bodies opened a brace that never closed, so every T-SQL literal below the "
            + "opening brace is attributed to no member. Either the source's braces do not balance or the "
            + "walk stopped understanding a delimiter:"
            + Environment.NewLine + string.Join(Environment.NewLine, truncated));

        Assert.True(
            overExtended.Count == 0,
            "these member bodies were read past the next declaration, so the literals in the members "
            + "below them are labelled with THIS member's name — a confident wrong answer, which nothing "
            + "downstream can see:"
            + Environment.NewLine + string.Join(Environment.NewLine, overExtended));

        Assert.True(
            overlapping.Count == 0,
            "these member ranges overlap. Members do not nest, so one range has swallowed another and the "
            + "literals in the inner one are attributed by whichever starts later:"
            + Environment.NewLine + string.Join(Environment.NewLine, overlapping));

        /* Asserted as a SET, not a count. A count restates the size of the inventory and goes stale with
           no edit to any member — and it cannot tell an addition from a removal, which is the whole
           question here: one member leaving is progress, one arriving is a scan that has quietly stopped
           reading a member whole. Set equality fails on both, and names which. */
        Assert.Equal(
            KnownTruncatedRanges.OrderBy(s => s, StringComparer.Ordinal),
            stoppedShort.OrderBy(s => s, StringComparer.Ordinal));
    }

    /// <summary>
    /// Every member whose range stops short of its own content — carried, labelled, and deliberately NOT
    /// fixed.
    ///
    /// <para><b>Why an inventory rather than a repair.</b> No consumer of the map under-reads because of
    /// these. The four that exist — <c>DarlingPgReadSqlParsesLiveTests</c>,
    /// <c>StoreSqlClockDisciplineTests</c>, <c>TempDbReservedLabelProvenanceTests</c> and this file — either
    /// scan a corpus none of these members are in (the first two read only
    /// <c>PerformanceMonitor.Darling.Storage</c>) or look for a kind of site none of them strand: what
    /// falls outside these ranges is date formats and UI fallbacks — <c>"yyyy-MM-dd HH:mm:ss"</c>,
    /// <c>"Never"</c>, <c>"None scheduled"</c>, <c>"N0"</c> — never T-SQL and never a tempdb label. Editing
    /// 31 member bodies to satisfy a walker would be changing the subject to suit the instrument.</para>
    ///
    /// <para><b>What the inventory is for is the day that stops being true.</b> Thirteen of these strand a
    /// string literal, and <see cref="EnclosingMember"/> answers <c>&lt;unknown&gt;</c> for every one of
    /// them today. A census that starts looking for a site of that kind — a format string, a renderer name —
    /// would silently miss it here, and the miss reads as absence rather than as error. This list is what
    /// makes such a member visible before a census is written against it, which is the opposite order from
    /// how the current 31 were found.</para>
    ///
    /// <para>Scope: the trees <see cref="ScannedTrees"/> sweeps, so <c>Darling.Tests</c> and
    /// <c>Lite.Tests</c> are outside it. Three further truncated members live there and are not listed.</para>
    /// </summary>
    private static readonly string[] KnownTruncatedRanges =
    [
        "PerformanceMonitor.Collectors/CollectorRuntimePrecondition.cs DescribeObserved",
        "PerformanceMonitor.Collectors/PgColumnStatsCoverage.cs Figure",
        "PerformanceMonitor.Collectors/StallWaitProbe.cs TriggerElapsedFor",
        "PerformanceMonitor.Collectors/StallWaitProbe.cs FitsUnderBudget",
        "PerformanceMonitor.Common/SystemHealthParser.cs GbFromBytes",
        "PerformanceMonitor.Common/SystemHealthParser.cs GbFromKb",
        "PerformanceMonitor.Notifications/WebhookAlertService.cs DeriveResourceDatabase",
        "Darling/PerformanceMonitor.Darling.Analysis/PgBaselineProvider.cs IsCommandTimeout",
        "Darling/PerformanceMonitor.Darling.Service/DarlingConfig.cs ToSettings",
        "Darling/PerformanceMonitor.Darling.Service/DarlingConfig.cs IsConfigured",
        "Darling/PerformanceMonitor.Darling.Service/HypotheticalIndexRequest.cs IsComplete",
        "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingDataReader.cs OutputFinding",
        "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingStallProbeReader.cs TriggerMbPerSecond",
        "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingStallProbeReader.cs TerminalSilenceMs",
        "Darling/PerformanceMonitor.Darling.Service/Targets/PostgresTargetProvider.cs WithDatabase",
        "Darling/PerformanceMonitor.Darling.Service/Targets/SqlServerTargetProvider.cs WithDatabase",
        "Darling/PerformanceMonitor.Darling.Viewer/MainWindow.ServerManagement.cs SelectedTabCollectorScope",
        "Darling/PerformanceMonitor.Darling.Viewer/ManageServersWindow.xaml.cs LastCollectedDisplay",
        "Darling/PerformanceMonitor.Darling.Viewer/RecommendationsViewModel.cs HasStructuredFixAction",
        "Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs BuildViewerPreferences",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.Blocking.cs EventTimeLocal",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.Deadlock.cs DeadlockTimeLocal",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.Deadlock.cs LastTranStartedLocal",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.JobHistory.cs RunTimeLocal",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.JobHistory.cs LastSuccessfulRunLocal",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.JobHistory.cs NextScheduledRunLocal",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.LongQueries.cs EventTimeLocal",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.PlanCorrection.cs Local",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.SystemEvents.cs Local",
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerPostgresDisplay.cs Timestamp",
        "Lite/Services/LocalDataService.CollectionHealth.cs OutputFinding",
    ];

    /* ───────────────────────── the resolver, pinned on arranged source ───────────────────────── */

    /// <summary>
    /// The four shapes that misattributed, arranged so each one sits between the member declaration and
    /// the literal — and the DIFFERENCE that makes it a control rather than a restatement: the same
    /// arrangement is asked twice, once for a literal inside the first member and once for a literal
    /// inside the second, and both answers have to move with the enclosing declaration.
    ///
    /// <para>Arranged rather than measured, because the point is the shape and not the file. Each of the
    /// four is a real site in <see cref="MemberAttributionSites"/>; put together in one body they also
    /// cover the case no single real site does, which is all four preceding the same literal — the old
    /// resolver returned whichever was nearest, so which one it named depended on line order rather than
    /// on scope.</para>
    /// </summary>
    [Fact]
    public void TheResolver_AttributesByScope_NotByTheNearestNameAbove()
    {
        const string source = """
            namespace Probe;

            internal sealed class Subject
            {
                private const string Preceding = "SELECT 1 FROM sys.databases;";

                public string Auto { get; set; } = "SELECT 7 FROM sys.schemas;";

                private static readonly (string Name, string Sql)[] Table =
                {
                    ("first", "SELECT 5 FROM sys.tables;"),
                };

                public string Constrained<T>(T value)
                    where T : class
                {
                    return "SELECT 6 FROM sys.types;" + value?.ToString();
                }

                public string First(string database)
                {
                    const string sql = "SELECT 2 FROM sys.objects;";
                    return sql + database;
                }

                public string Second(string database)
                {
                    var builder = new SqlConnectionStringBuilder(database)
                    {
                        ConnectTimeout = 10,
                    };

                    if (builder is null)
                    {
                        return "SELECT 3 FROM sys.columns;";
                    }

                    foreach (var c in database)
                    {
                        _ = c;
                    }

                    const string inner = "SELECT 4 FROM sys.indexes;";
                    return inner;
                }
            }
            """;

        var map = Of(source);

        /* Both members were read, and read as members — a map that resolved neither would satisfy every
           equality below by returning <unknown> twice, which is not the same answer. */
        Assert.Equal(
            new[] { "Auto", "Constrained", "First", "Preceding", "Second", "Table" },
            map.Declarations.Where(d => d.Kind == DeclarationKind.Member)
                            .Select(d => d.Name)
                            .OrderBy(n => n, StringComparer.Ordinal)
                            .ToArray());

        Assert.Equal(
            new[] { "Subject" },
            map.Declarations.Where(d => d.Kind == DeclarationKind.Type).Select(d => d.Name).ToArray());

        /* The member initialiser, which is the shape that already worked. */
        Assert.Equal("Preceding", EnclosingMember(map, source.IndexOf("SELECT 1", StringComparison.Ordinal)));

        /* A local const string in the SAME member — the LocalDataService shape. The old resolver named
           the local, sql. */
        Assert.Equal("First", EnclosingMember(map, source.IndexOf("SELECT 2", StringComparison.Ordinal)));

        /* Inside an if, below a type construction with a brace — the PgPlanFetcher shape. The old
           resolver named the type, SqlConnectionStringBuilder, then if once the block opened. */
        Assert.Equal("Second", EnclosingMember(map, source.IndexOf("SELECT 3", StringComparison.Ordinal)));

        /* And below all four of them at once, which no single real site arranges. */
        Assert.Equal("Second", EnclosingMember(map, source.IndexOf("SELECT 4", StringComparison.Ordinal)));

        /* A TUPLE-TYPED field, whose first ( belongs to the type rather than to a parameter list. This is
           the shape that decides how the name is read at all: "the identifier before the first paren"
           answers readonly here, and readonly is a modifier, not a member. ScannedTrees and
           DefinitionsWithoutSql in this very file are both this shape. */
        Assert.Equal("Table", EnclosingMember(map, source.IndexOf("SELECT 5", StringComparison.Ordinal)));

        /* A GENERIC member with a constraint, where the parameter list follows the > that closed the type
           parameters rather than the name itself. Left unhandled the scan reads past the parameter list
           into the constraint and answers class. Nine members in the scanned trees carry a where-clause,
           so the shape is real; none of them holds T-SQL, which is why it is arranged here. */
        Assert.Equal(
            "Constrained",
            EnclosingMember(map, source.IndexOf("SELECT 6", StringComparison.Ordinal)));

        /* An AUTO-PROPERTY with an initialiser, where the braced group is the accessor list and the
           literal is after it. This is the shape whose truncation ShapeOf cannot report — the range
           would stop at the accessor list's } , still closed and still under NextStart — so it is the
           one case where a wrong range is silent everywhere and only this assertion is left. */
        Assert.Equal("Auto", EnclosingMember(map, source.IndexOf("SELECT 7", StringComparison.Ordinal)));

        /* The difference. Move the same literal text from the second member into the first and the answer
           has to follow it; an answer driven by the nearest name above would not move, because the names
           above it are unchanged. */
        var moved = source.Replace(
            "const string sql = \"SELECT 2 FROM sys.objects;\";",
            "const string sql = \"SELECT 4 FROM sys.indexes;\";",
            StringComparison.Ordinal);

        Assert.NotEqual(source, moved);
        Assert.Equal(
            "First",
            EnclosingMember(Of(moved), moved.IndexOf("SELECT 4", StringComparison.Ordinal)));
    }

    /// <summary>
    /// The scan reads a body whose literals hold unbalanced braces, and a raw brace count does not.
    ///
    /// <para>Arranged rather than measured, because the corpus balances its literal braces today — which
    /// is precisely the standing assumption worth refusing. Both directions are asserted, so
    /// <see cref="CSharpSourceWalker"/> is shown to be load-bearing rather than said to be: a <c>}</c> in
    /// a literal ends a raw count early and truncates the body, so every literal after it is attributed
    /// to nothing.</para>
    /// </summary>
    [Fact]
    public void TheMemberScan_ReadsABodyWhoseLiteralHoldsABrace_AndARawCountDoesNot()
    {
        const string source = """
            namespace Probe;

            internal sealed class Subject
            {
                public string Body()
                {
                    var brace = "} SELECT 1 FROM sys.databases;";
                    return brace + "SELECT 2 FROM sys.objects;";
                }

                public string After()
                {
                    return "SELECT 3 FROM sys.columns;";
                }
            }
            """;

        var offset = source.IndexOf("SELECT 2", StringComparison.Ordinal);

        Assert.Equal("Body", EnclosingMember(Of(source), offset));

        /* The same declaration walk over RAW source, which is what every copy of this idiom did before
           #3052 extracted the walker. The literal's } closes Body early, so the offset lands outside every
           member range and resolves to nothing at all. */
        var raw = OfRawSourceForControlTestsOnly(source);
        var body = raw.Declarations.Single(d => d.Name == "Body");

        Assert.True(
            offset >= body.End,
            "the raw brace count no longer truncates Body, so this control is not arranging the thing it "
            + "claims to. It needs a } inside a string literal in the body, ahead of the offset.");

        Assert.Equal(Unknown, EnclosingMember(raw, offset));
    }

    /// <summary>
    /// The two over-run arms fire on different source, so neither is redundant.
    ///
    /// <para><see cref="RangeShape.OverExtended"/> is the arm that can disagree with the brace walk,
    /// because it compares the walk's answer against a regex match offset. It is also structurally blind
    /// at the END of the file: the last declaration has no successor to run past, so a runaway there
    /// satisfies it in silence. <see cref="RangeShape.Unterminated"/> is what still covers that tail.
    /// Asserted on two arrangements rather than argued, and each names the arm the other one misses.</para>
    /// </summary>
    [Fact]
    public void TheMemberScan_IsBoundedByTheNextDeclaration_AndByTheBraceWalkAtTheEndOfTheFile()
    {
        /* Mid-file: First's closing brace is missing, so the brace walk closes on Second's and swallows
           it. The bound sees a declaration start inside the range. */
        const string midFile = """
            namespace Probe;

            internal sealed class Subject
            {
                public string First()
                {
                    return "SELECT 1 FROM sys.databases;";

                public string Second()
                {
                    return "SELECT 2 FROM sys.objects;";
                }
            }
            """;

        var mid = Of(midFile);
        var first = mid.Declarations.Single(d => d.Name == "First");

        Assert.Equal(RangeShape.OverExtended, ShapeOf(first));
        Assert.Equal(RangeShape.WholeMember, ShapeOf(mid.Declarations.Single(d => d.Name == "Second")));

        /* End of file: the LAST member's body never closes. There is no later declaration, so the bound
           is silent — NextStart is the end of the text and the range cannot exceed it — and only the
           brace walk's own failure to close can see it. */
        const string atEndOfFile = """
            namespace Probe;

            internal sealed class Subject
            {
                public string Only()
                {
                    return "SELECT 1 FROM sys.databases;";
            """;

        var tail = Of(atEndOfFile);
        var only = tail.Declarations.Single(d => d.Name == "Only");

        Assert.Equal(RangeShape.Unterminated, ShapeOf(only));

        /* And the bound really is blind here rather than merely quiet: the range end is the sentinel, so
           the comparison the mid-file case turns on cannot fire. */
        Assert.True(only.End < 0);
        Assert.False(only.End > only.NextStart);
    }

    /* ───────────────────────── the checks ───────────────────────── */

    private static readonly string[] CoveredRules =
    {
        CountBig, RowcountBig, BlockComments, NoTabs, LowercaseTypes, UnabbreviatedTypes,
    };

    private static readonly string[] UncoveredRules =
    {
        UppercaseKeywords, SysnameForIdentifiers, FourSpaceIndent,
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

        /* The abbreviation half of the same bullet, off the SAME view as the case check above — so a type
           name inside a blanked `FOR XML` or `AT TIME ZONE` cannot supply one either. */
        foreach (Match match in AbbreviatedTypeUse.Matches(forTypes))
        {
            findings.Add(new(UnabbreviatedTypes, Where(sql, match.Index, $"the data type `{match.Value}` — data types are never abbreviated, so `integer`")));
        }

        return findings;
    }

    /// <summary>
    /// <c>int</c> where <c>CONTRIBUTING.md</c> says <c>integer</c>.
    ///
    /// <para><b>Both anchors are load-bearing, and each is held by a benign fixture.</b> The trailing
    /// lookahead is what stops <c>integer</c> — the correct spelling — matching on its own first three
    /// characters, which would make every fix report itself as the violation it fixed. The leading
    /// lookbehind is what stops <c>bigint</c>, <c>smallint</c> and <c>tinyint</c>, three type names this
    /// corpus writes and the bullet does not object to, and it carries <c>.</c> so a column reference
    /// <c>t.int</c> is not one either. No capture group, deliberately: a group around the token is how a
    /// greedy match absorbs the thing under test.</para>
    ///
    /// <para><c>int</c> is the only abbreviation the bullet's own examples name; <c>dec</c> for
    /// <c>decimal</c> and <c>double precision</c> for <c>float</c> are the others T-SQL accepts and neither
    /// is checked here. That is a stated bound on the CHECK, not on the rule.</para>
    /// </summary>
    private static readonly Regex AbbreviatedTypeUse = new(
        @"(?<![A-Za-z0-9_@#$.])int(?![A-Za-z0-9_])",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

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
    /// <para><c>sqlserver.</c> and <c>package0.</c> went in beside <c>EVENT SESSION</c> and were then
    /// REMOVED: measured, every statement in the corpus carrying either also carries <c>EVENT SESSION</c>,
    /// so no fixture could show them mattering and a mutation deleting them stayed green. A marker nothing
    /// depends on is a claim nothing holds.</para>
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
        + @"|\bSET\s+NOCOUNT\b"
        + @"|\bEVENT\s+SESSION\b",
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
                kinds[i++] = SqlSpan.QuotedIdentifier;

                while (i < sql.Length)
                {
                    if (sql[i] == ']')
                    {
                        /* ]] is an escaped bracket inside a quoted identifier, exactly as '' is inside a
                           string. Stopping at the first ] ends the identifier early and hands the rest of
                           the name to the token checks as code, so [a]]INT] reports an uppercase data type
                           that is part of a column name. Nothing in the corpus writes one today; the
                           asymmetry with the '' arm right above is what made it worth closing. */
                        if (i + 1 < sql.Length && sql[i + 1] == ']')
                        {
                            kinds[i++] = SqlSpan.QuotedIdentifier;
                            kinds[i++] = SqlSpan.QuotedIdentifier;
                            continue;
                        }

                        kinds[i++] = SqlSpan.QuotedIdentifier;
                        break;
                    }

                    kinds[i++] = SqlSpan.QuotedIdentifier;
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
    /// <para><b>The cost of keeping them, since it is a real one.</b> A string VALUE that happens to contain
    /// a banned token reads as the token: <c>WHERE d.state_desc = N'DATETIME'</c> would be reported as an
    /// uppercase data type. There is no such value in the corpus — the scan measures zero on the covered
    /// subset — and the failure direction is a spurious red on correct SQL rather than a hidden violation,
    /// which is the direction to be wrong in. Blanking string values instead would trade that for missing
    /// every violation inside dynamic SQL, which is written all over the scanned trees — and one
    /// <c>sp_executesql</c> body held one of the nine violations this change fixed.</para>
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
    ///
    /// <para>Each group names an ANCHOR file whose T-SQL must stay visible, which is a per-site requirement
    /// that a per-tree total cannot give — and it is also what makes this file's cross-app read of Lite
    /// legible to <c>CrossAppGuardCiGateTests</c>. A bare root iterated out of a field is not one of the
    /// three spellings that guard can see — <c>CONTRIBUTING.md</c> names them, and calls this one silent by
    /// construction — and the bare <c>"Lite"</c> below is exactly it. A repo-rooted path literal at the site
    /// that reads it is the first of the three, so the anchor is what puts this read into that guard's found
    /// set instead of leaving it to arrive from another pin. The
    /// three anchors are the file #3078's violation landed in and the two non-shared files this change
    /// fixed.</para>
    /// </summary>
    private static readonly (string Tree, string[] Roots, string Anchor)[] ScannedTrees =
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
        }, "PerformanceMonitor.Collectors/StallWaitProbe.cs"),
        ("Darling", new[] { "Darling" },
            "Darling/PerformanceMonitor.Darling.Service/DarlingServerConnector.cs"),
        ("Lite", new[] { "Lite" },
            "Lite/Services/LocalDataService.FinOps.Recommendations.cs"),
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
