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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The two surfaces #3424 and #3425 changed, asserted at the level a pure-function test cannot reach: that
/// each one still ASKS <see cref="PerformanceMonitor.Collectors.PgCappedRead"/>, asks it with the ceiling
/// the surface actually enforces, and keeps the two SQL properties the fix rests on.
///
/// <para><b>Why the ceiling is pinned as a symbol.</b> <c>PgCappedRead</c> decides the difference between
/// "raise the limit" and "raising the limit cannot help" by comparing the leading block against the maximum
/// permitted limit. Passed a literal, that decision is made against a number nothing keeps equal to what
/// <c>McpHelpers.ValidateTop</c> refuses - so the classifier would answer confidently about a surface that
/// does not exist, and every arm would still look plausible. The pin is on the CALL SITE because that is
/// where the drift happens; <c>PgCappedReadTests</c> cannot see it.</para>
///
/// <para><b>And why the default is pinned.</b> #3278's answerless-first order is load-bearing - it is what
/// stops an unmeasured index reading as a clean one - so <c>answered_only</c> exists to give the answers a
/// route, not to demote that order. A default of true would silently invert the surface every prior reader
/// relies on.</para>
/// </summary>
public sealed class PgCappedReadSurfaceTests
{
    private const string IndexTool =
        "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs";

    private const string ExtensionTool =
        "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs";

    private const string WebEndpoints =
        "Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs";

    /// <summary>
    /// Both MCP reads classify their own reach, and both hand the classifier the SHARED ceiling constant
    /// rather than a literal.
    /// </summary>
    [Theory]
    [InlineData(IndexTool, "GetPgIndexBloat")]
    [InlineData(ExtensionTool, "GetPgExtensions")]
    public void EachSurfaceAsksTheClassifier_WithTheCeilingItEnforces(string file, string member)
    {
        var body = StripComments(MemberBody(file, member));

        Assert.Contains("PgCappedRead.Classify", body, StringComparison.Ordinal);
        Assert.Contains("maxLimit: McpHelpers.MaxTop", body, StringComparison.Ordinal);

        /* THE LITERAL, not merely the absence of the symbol. A call passing both would satisfy the
           assertion above and still decide against the wrong number on whichever argument won. */
        Assert.DoesNotContain("maxLimit: 1000", body, StringComparison.Ordinal);
        Assert.DoesNotContain("maxLimit: 1_000", body, StringComparison.Ordinal);

        /* The verdict has to REACH the payload. A classifier called and then discarded is the shape this
           whole pair of issues is about - a read that has the answer and does not say it. */
        Assert.Contains("reach.Reach.ToString()", body, StringComparison.Ordinal);
        Assert.Contains("reach.Message", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>answered_only</c> defaults to FALSE on the MCP signature and at the web dispatch, so a caller who
    /// has not asked still gets #3278's answerless-first order.
    /// </summary>
    [Fact]
    public void AnsweredOnlyDefaultsToFalseOnEverySurface()
    {
        Assert.Contains(
            "bool answered_only = false",
            StripComments(MemberSignature(IndexTool, "GetPgIndexBloat")),
            StringComparison.Ordinal);

        var web = StripComments(ReadSource(WebEndpoints));

        Assert.Contains("QueryBool(c, \"answered_only\", false)", web, StringComparison.Ordinal);
        Assert.Contains("PBool(\"answered_only\", false)", web, StringComparison.Ordinal);
        Assert.DoesNotContain("QueryBool(c, \"answered_only\", true)", web, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>database_name</c> reaches the extension read from both surfaces, so the filter that makes a host
    /// completable is not MCP-only.
    /// </summary>
    [Fact]
    public void TheDatabaseFilterReachesBothSurfaces()
    {
        Assert.Contains(
            "string? database_name = null",
            StripComments(MemberSignature(ExtensionTool, "GetPgExtensions")),
            StringComparison.Ordinal);

        Assert.Contains(
            "Str(c, \"database_name\")",
            StripComments(ReadSource(WebEndpoints)),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// NO caller-controlled parameter enters the <c>DISTINCT ON</c> scope, #3278's tie-break is still in it,
    /// and the answered-only gate sits outside it.
    ///
    /// <para><b>The claim is about COUPLING, not about which rows come back.</b> Mutation testing settled
    /// that: moving the predicate inside the scope left every assertion in this suite green, because the
    /// inner tie-break already prefers an answered row over a newer label, so both placements keep each
    /// index's newest answered row. What the placement protects is the inner scope's TEXT, which
    /// <c>PgIndexBloatCoverageTests</c> pins to match the coverage census so the two cannot disagree about
    /// which row is current. A predicate inside it makes the texts diverge while that pin - it compares the
    /// distinct key and the tie-break - keeps passing, and the equivalence becomes a dependency on the
    /// tie-break's preference that nothing defends.</para>
    ///
    /// <para>So the assertion is the parameter's ABSENCE from the inner scope, which is the property that
    /// fails under exactly that edit, rather than the gate's presence after it - which a second copy added
    /// inside would satisfy.</para>
    /// </summary>
    [Fact]
    public void NoCallerParameterEntersTheDistinctOnScope()
    {
        var sql = DarlingPgIndexBloatReader.PgIndexBloatSql;

        var distinct = sql.IndexOf(
            "DISTINCT ON (database_name, schema_name, table_name, index_name)", StringComparison.Ordinal);
        var tieBreak = sql.IndexOf(
            "(skipped_reason IS NULL) DESC, collection_time DESC", StringComparison.Ordinal);
        var closingAlias = sql.IndexOf(") AS latest", StringComparison.Ordinal);
        var gate = sql.IndexOf("WHERE (NOT $5 OR latest.skipped_reason IS NULL)", StringComparison.Ordinal);

        Assert.True(distinct > 0, "the row read's distinct key is gone or renamed");
        Assert.True(tieBreak > distinct, "#3278's measured-row-wins tie-break is gone from the inner scope");
        Assert.True(closingAlias > distinct, "the inner scope's closing alias is gone or renamed");
        Assert.True(gate > closingAlias, "the answered-only gate is not after the inner scope");

        /* THE INNER SCOPE, and it may mention only the three parameters the census's own scope mentions -
           server_id and the two window ends. $4 is the LIMIT and $5 is the caller's filter, and neither has
           any business deciding which row represents an index. */
        var innerScope = sql[distinct..closingAlias];

        Assert.DoesNotContain("$5", innerScope, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", innerScope, StringComparison.Ordinal);

        /* The gate is a no-op by default, so the shipped order is still the one #3278 established. */
        Assert.Contains(
            "ORDER BY (skipped_reason IS NOT NULL) DESC",
            sql,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The extension row read's database filter sits INSIDE the <c>DISTINCT ON</c> scope, and the install
    /// census groups by extension name ALONE.
    ///
    /// <para>The grouping is the whole reason the census fits inside the cap on any host: one row per
    /// extension is bounded by what the PostgreSQL build offers - 102 on the measured fleet - and does not
    /// grow as databases are added. Grouping by <c>(database_name, extension_name)</c> instead would
    /// reproduce the product this census exists to escape, so the census would outgrow the cap exactly where
    /// the row list already does.</para>
    /// </summary>
    [Fact]
    public void TheExtensionCensusIsBoundedByExtensionNamesAndNotByDatabases()
    {
        var rowSql = DarlingPgExtensionAvailabilityReader.PgExtensionAvailabilitySql;
        var filter = rowSql.IndexOf("($5::text IS NULL OR database_name = $5::text)", StringComparison.Ordinal);
        var closingAlias = rowSql.IndexOf(") AS latest", StringComparison.Ordinal);

        Assert.True(filter > 0, "the database filter is gone from the extension row read");
        Assert.True(
            filter < closingAlias,
            "the database filter moved OUTSIDE the DISTINCT ON scope, so every database's rows are still "
            + "materialised before it applies");

        var censusSql = DarlingPgExtensionAvailabilityReader.InstallCensusSql;

        Assert.Contains("GROUP BY latest.extension_name", censusSql, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "GROUP BY latest.database_name",
            censusSql,
            StringComparison.Ordinal);

        /* NO LIMIT, which is the census's defining property and the one a later edit would take away by
           reflex. A capped census is the defect one level up. */
        Assert.DoesNotContain("LIMIT", censusSql, StringComparison.Ordinal);

        /* The denominator travels with the counts, so no consumer can print installations without it. */
        Assert.Contains("AS databases_total", censusSql, StringComparison.Ordinal);
        Assert.Contains("AS databases_installed", censusSql, StringComparison.Ordinal);
        Assert.Contains("AS databases_outdated", censusSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>answered_only</c> over a server that HAS candidate indexes answers with its own status token, not
    /// with <c>empty</c>.
    ///
    /// <para>The <c>EXTENSION_MISSING</c> discipline (#3240): a refusal whose cause and remedy differ from
    /// the nearest existing label gets its own name. An <c>empty</c> here would carry the "widen the window,
    /// this collector runs daily" sentence over a server whose collector ran perfectly and whose every index
    /// is unmodellable - advice for a different situation, reading as "nothing to reclaim".</para>
    /// </summary>
    [Fact]
    public void TheAnsweredOnlyRefusalBandsApartFromEmpty()
    {
        var body = StripComments(MemberBody(IndexTool, "GetPgIndexBloat"));

        var refusal = body.IndexOf("\"no_answers\"", StringComparison.Ordinal);
        var empty = body.IndexOf("\"empty\"", StringComparison.Ordinal);

        Assert.True(refusal > 0, "the answered_only refusal lost its own status token");
        Assert.True(empty > 0, "the ordinary empty status is gone");
        Assert.True(
            refusal < empty,
            "the empty status is returned before the answered_only refusal is considered, so a filtered "
            + "read over a server with indexes answers empty and reads as nothing to reclaim");

        Assert.Contains(
            "answered_only && coverage.Candidates.IndexCount > 0",
            body,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The extension payload carries the census, the reach and the install census - by NAME, because
    /// automation keys on names.
    /// </summary>
    [Fact]
    public void TheExtensionPayloadCarriesItsCensusAndItsReach()
    {
        var body = StripComments(MemberBody(ExtensionTool, "GetPgExtensions"));

        foreach (var field in new[]
        {
            "census = new", "reach = new", "install_census = census",
            "databases_total = ", "extension_name_count = ", "rows_available = ",
            "databases_complete_in_page = ",
        })
        {
            Assert.Contains(field, body, StringComparison.Ordinal);
        }

        /* THE ECHO IS THE NORMALISED VALUE, not the caller's raw string. A whitespace-only database_name
           reaches this from the web surface, the query treats it as no filter, and echoing the raw string
           beside server-wide rows labels the response with a scope it does not have - the "one response,
           one scope" guarantee contradicted by its own label. Pinned as the assignment rather than as the
           field's presence, because `database_name,` is what the defect looked like. */
        Assert.Contains("database_name = scope,", body, StringComparison.Ordinal);
        Assert.DoesNotContain("\r\n                database_name,", body, StringComparison.Ordinal);

        /* ONE normalisation, reached through the reader's own helper rather than restated here. Three places
           have to agree - both reads and the echo - and the finding was the third one disagreeing. */
        Assert.Contains(
            "DarlingPgExtensionAvailabilityReader.NormalizeDatabaseFilter(database_name)",
            body,
            StringComparison.Ordinal);
        Assert.DoesNotContain("IsNullOrWhiteSpace(database_name)", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The index bloat payload carries the reach block AND echoes the filter, so a saved response says which
    /// population it describes.
    /// </summary>
    [Fact]
    public void TheIndexBloatPayloadEchoesTheFilterBesideItsReach()
    {
        var body = StripComments(MemberBody(IndexTool, "GetPgIndexBloat"));

        foreach (var field in new[]
        {
            "answered_only,", "reach = new", "answerless_rows_ahead = ",
            "answered_rows_on_server = ", "answered_rows_withheld = ",
            "a_raised_limit_would_help = ", "max_limit = McpHelpers.MaxTop",
        })
        {
            Assert.Contains(field, body, StringComparison.Ordinal);
        }

        /* THE COVERAGE CENSUS IS STILL ASKED, and unconditionally. #3278's denominator is what makes a
           filtered ranking readable at all, so a reach block that replaced it would be a regression
           dressed as a feature. */
        Assert.Contains(
            "DarlingPgIndexBloatReader.GetCoverageVerdictAsync", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Comments blanked, string LITERALS kept — assembled from the walker's own two mirror outputs and its
    /// code mask, never from a hand-rolled masker.
    ///
    /// <para><b>Both halves are needed and neither alone will do.</b> The stripped copy cannot see
    /// <c>"no_answers"</c> or <c>PBool("answered_only", false)</c>, which are the pins that matter most here:
    /// they are what the surface SAYS. The raw source cannot be used either, because a doc comment naming
    /// <c>"no_answers"</c> would satisfy the assertion while the code returned <c>"empty"</c> — a pin passing
    /// on its own documentation, which is exactly the vacuity these scans are prone to.
    /// <c>StripCommentsAndStrings</c> replaces rather than removes, so its output and
    /// <c>StringLiteralBodies</c>' offsets index the same text and the splice is exact.</para>
    ///
    /// <para><b>The delimiters need restoring too, and the rule is safe rather than approximate.</b>
    /// <c>StringLiteralBodies</c> reports the BODY, and a literal's own quotes are non-code, so splicing
    /// bodies alone yields <c>QueryBool(c, answered_only, false)</c> — an identifier where the source has a
    /// literal, and every literal pin fails. A literal opener in C# is built only from <c>"</c>, <c>@</c>
    /// and <c>$</c>, so restoring exactly those three wherever they are non-code restores every delimiter
    /// and can leak nothing else: those characters occurring in PROSE come back as themselves while the
    /// words around them stay blank, which is why a commented-out <c>"no_answers"</c> collapses to
    /// <c>"          "</c> and still does not satisfy the pin. Verified against that case rather than
    /// assumed.</para>
    /// </summary>
    private static string StripComments(string source)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source).ToCharArray();

        foreach (var (start, text) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            for (var i = 0; i < text.Length && start + i < stripped.Length; i++)
            {
                stripped[start + i] = text[i];
            }
        }

        var code = CSharpSourceWalker.CodeMask(source);

        for (var i = 0; i < source.Length; i++)
        {
            if (!code[i] && source[i] is '"' or '@' or '$')
            {
                stripped[i] = source[i];
            }
        }

        return new string(stripped);
    }

    /// <summary>
    /// <see cref="StripComments"/> keeps what the surface SAYS and drops what it merely documents.
    ///
    /// <para>The instrument's own control, and it is not ceremony: every literal pin in this class is a
    /// substring test, and a substring test over text that still held comments would pass on a doc comment
    /// quoting the very token the code stopped emitting. This asserts the helper discriminates the two
    /// cases, so the pins above are measuring the code.</para>
    /// </summary>
    [Fact]
    public void TheScanKeepsLiteralsAndStillDropsCommentsThatQuoteThem()
    {
        const string Source = """
            /* the literal "no_answers" in a comment */
            // and "database_name" in another
            var kept = "no_answers";
            """;

        var scanned = StripComments(Source);

        Assert.Contains("var kept = \"no_answers\";", scanned, StringComparison.Ordinal);
        Assert.DoesNotContain("the literal", scanned, StringComparison.Ordinal);
        Assert.DoesNotContain("\"database_name\"", scanned, StringComparison.Ordinal);

        /* ONE occurrence, not merely one presence: the comment's copy collapsing to quotes around blanks
           has to leave the token itself appearing exactly where the code emits it. */
        Assert.Equal(
            1,
            scanned.Split("no_answers", StringSplitOptions.None).Length - 1);
    }

    /// <summary>
    /// One member's brace-balanced body, verbatim — located on the stripped copy so a mention of the name in
    /// a doc comment cannot be mistaken for the declaration, and balanced by the WALKER's own helper rather
    /// than a local loop: braces inside an interpolation hole are code, so a hand-rolled counter
    /// unbalances on the first <c>$"...{x}..."</c> it meets.
    /// </summary>
    private static string MemberBody(string relativePath, string member)
    {
        var source = ReadSource(relativePath);
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);
        var declarations = Declarations(stripped, member).ToList();
        var at = Assert.Single(declarations);
        var open = stripped.IndexOf('{', at);

        Assert.True(open > at, member + " has no body");

        var extent = CSharpSourceWalker.BraceBalanced(stripped, open).Length;

        return source[open..(open + extent)];
    }

    private static string MemberSignature(string relativePath, string member)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(ReadSource(relativePath));
        var declarations = Declarations(stripped, member).ToList();
        var at = Assert.Single(declarations);
        var open = stripped.IndexOf('{', at);
        var arrow = stripped.IndexOf("=>", at, StringComparison.Ordinal);
        var stops = new[] { open, arrow }.Where(i => i > at).ToArray();

        Assert.NotEmpty(stops);

        /* The ORIGINAL text over the stripped offsets: a signature's parameter DEFAULTS include string
           literals, and the stripped copy has blanked them. The walker replaces rather than removes, so the
           offsets are the same in both. */
        return ReadSource(relativePath)[at..stops.Min()];
    }

    /// <summary>
    /// Every DECLARATION of a member, never the first occurrence: these files CALL the members they declare
    /// and a first-match scan brace-balances whichever one happens to appear first.
    /// </summary>
    private static IEnumerable<int> Declarations(string stripped, string member)
    {
        for (var at = stripped.IndexOf(member + "(", StringComparison.Ordinal);
             at >= 0;
             at = stripped.IndexOf(member + "(", at + 1, StringComparison.Ordinal))
        {
            var statementStart = stripped.LastIndexOfAny([';', '{', '}'], at) + 1;
            var head = stripped[statementStart..at];

            if (head.Contains(" private ", StringComparison.Ordinal)
                || head.Contains(" public ", StringComparison.Ordinal)
                || head.Contains(" internal ", StringComparison.Ordinal)
                || head.Contains(" protected ", StringComparison.Ordinal))
            {
                yield return at;
            }
        }
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#3424/#3425 scan target not found: {path}");

        return File.ReadAllText(path);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
