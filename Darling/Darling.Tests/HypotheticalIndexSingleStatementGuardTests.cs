/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3385: the deliberate single-statement guard under the one dynamic-SQL sink in this product that is
/// handed text from a monitored server.
///
/// <para>
/// <c>EXPLAIN (GENERIC_PLAN)</c> needs the <c>$1</c> placeholders in normalized text to stay placeholders,
/// so the statement cannot travel as a bound parameter of the EXPLAIN. It is staged into a GUC and
/// string-concatenated into a server-side <c>EXECUTE</c> instead. <c>EXPLAIN</c> without <c>ANALYZE</c>
/// only PLANS, which is why that is not exploitable; this guard is the layer beneath that one.
/// </para>
///
/// <para>
/// <b>The whole difficulty is that a semicolon scan is the wrong check.</b> Real normalized
/// <c>pg_stat_statements</c> text is full of comments, and ~5% of it carries a <c>;</c> inside one, so a
/// <c>;</c> scan refuses legitimate index candidates while buying nothing. Every fixture in
/// <see cref="EveryFormThatCanHideASemicolon_IsAccepted"/> therefore holds a <c>;</c> somewhere a
/// <c>;</c> scan would find it, and <see cref="TheFixtureSetWouldFailAPlainSemicolonScan"/> asserts that
/// rather than leaving a reader to take it on trust.
/// </para>
/// </summary>
public sealed class HypotheticalIndexSingleStatementGuardTests
{
    /// <summary>
    /// One statement each, every one carrying a <c>;</c> inside a construct PostgreSQL's lexer swallows:
    /// a line comment, a block comment, a NESTED block comment with the <c>;</c> on either side of the
    /// nesting, a block comment holding an unbalanced apostrophe, a plain string, a string with <c>''</c>
    /// doubling, an <c>E'…'</c> string with a backslash-escaped quote and another with an escaped
    /// backslash, a <c>$$</c> body, a <c>$tag$</c> body, a <c>$outer$</c> body holding both a <c>$$</c>
    /// and a different <c>$tag$</c> that do not close it, a quoted identifier, a quoted identifier with
    /// <c>""</c> doubling, and the shape real captured text arrives in.
    /// </summary>
    private static readonly string[] s_acceptedCarryingASemicolon =
    {
        /* -- to end of line. */
        "SELECT a FROM t WHERE x = $1 -- ; not a separator",

        /* A block comment: the case the whole issue turns on. */
        "SELECT /* ; */ a FROM t WHERE x = $1",

        /* Nested, with the ; in the INNER comment. PostgreSQL block comments nest, so closing at the
           first terminator would read the rest of the outer comment as code. */
        "SELECT /* outer /* ; */ still outer */ a FROM t WHERE x = $1",

        /* Nested, with the ; in the OUTER comment after the inner one closes. */
        "SELECT /* /* inner */ ; */ a FROM t WHERE x = $1",

        /* A block comment whose apostrophe is unbalanced: a string-aware but comment-BLIND scan opens a
           string here and then swallows a real separator later. */
        "/* it's fine ; here */ SELECT a FROM t WHERE x = $1",

        /* '…' with no backslash escape, which is what standard_conforming_strings means. */
        "SELECT a FROM t WHERE note = 'a; b' AND x = $1",

        /* '' is an embedded quote, not the end of the string. */
        "SELECT a FROM t WHERE note = 'it''s a; b' AND x = $1",

        /* E'…', where a backslash DOES escape, so the quote after it is data. */
        """SELECT a FROM t WHERE note = E'a\'; b' AND x = $1""",

        /* E'…' with an escaped backslash, so the ; after it is still inside the string. */
        """SELECT a FROM t WHERE note = E'a\\; b' AND x = $1""",

        /* $$ … $$, an empty tag. */
        "SELECT $$a; b$$ AS note, a FROM t WHERE x = $1",

        /* $tag$ … $tag$. */
        "SELECT $body$a; b$body$ AS note, a FROM t WHERE x = $1",

        /* Only the byte-identical tag closes a body: neither the $$ nor the $body$ inside this one
           does. */
        "SELECT $outer$ a; b $$ c $body$ d $outer$ AS note, a FROM t WHERE x = $1",

        /* "…" quoted identifier. */
        "SELECT a AS \"a; b\" FROM t WHERE x = $1",

        /* "" is an embedded double quote. */
        "SELECT a AS \"a\"\"b; c\" FROM t WHERE x = $1",

        /* The shape captured text arrives in: a tracing header, prose mid-statement, and a run of
           placeholders including multi-digit ones. */
        """
        /* service='agent' */ SELECT a, b, c
          FROM t
         WHERE x = $1 -- ; bounded above
           AND y = $2 /* ; also bounded */
           AND z = $12
         LIMIT $35
        """,
    };

    [Fact]
    public void EveryFormThatCanHideASemicolon_IsAccepted()
        => Assert.All(
            s_acceptedCarryingASemicolon,
            statement => Assert.True(
                HypotheticalIndexExperiment.IsSingleStatement(statement),
                $"refused a single statement: {statement}"));

    /// <summary>
    /// The discriminating assertion, and the reason this suite is not a semicolon scan wearing a better
    /// name: every fixture above contains a <c>;</c>, so a guard reduced to <c>IndexOf(';')</c> refuses
    /// all of them and <see cref="EveryFormThatCanHideASemicolon_IsAccepted"/> goes red on every case.
    /// </summary>
    [Fact]
    public void TheFixtureSetWouldFailAPlainSemicolonScan()
    {
        Assert.NotEmpty(s_acceptedCarryingASemicolon);

        Assert.All(
            s_acceptedCarryingASemicolon,
            statement => Assert.True(
                statement.Contains(';', StringComparison.Ordinal),
                $"fixture carries no ; so it discriminates nothing: {statement}"));
    }

    [Theory]
    /* Nothing hiding anything: the baseline, so "accept everything" is not what passes above. */
    [InlineData("SELECT a FROM t WHERE x = $1")]
    /* Placeholders stay placeholders. A $ before a digit does not open a dollar-quoted body, and reading
       one that way would swallow the rest of the statement and every ; in it. */
    [InlineData("SELECT a FROM t WHERE x = $1 AND y = $2 AND z = $12 AND w = $35")]
    /* A $ that opens nothing: it is legal inside an identifier, and a tag with no closing $ is not a
       delimiter. */
    [InlineData("SELECT a$b FROM t WHERE x = $1")]
    /* An e that ENDS an identifier is not an E'…' prefix, so this is a plain string. */
    [InlineData("SELECT ase'plain' FROM t WHERE x = $1")]
    public void AnOrdinaryStatement_IsAccepted(string statement)
        => Assert.True(HypotheticalIndexExperiment.IsSingleStatement(statement));

    [Theory]
    /* Two statements, plainly. The other half of the discriminating pair. */
    [InlineData("SELECT $1; SELECT $2")]
    /* Two statements where a block comment ALSO holds a ;. A comment-aware scan has to skip the decoy and
       still find the real separator; a comment-blind one stops at the decoy. */
    [InlineData("SELECT $1 /* ; */; SELECT $2")]
    [InlineData("SELECT $1 /* /* ; */ */; SELECT $2")]
    /* Same, with the decoy in a line comment, so the separator is on the next line. */
    [InlineData("SELECT $1 -- ;\n; SELECT $2")]
    /* Same, with the decoy in each string and quoting form. */
    [InlineData("SELECT 'a; b'; SELECT $1")]
    [InlineData("SELECT 'it''s a; b'; SELECT $1")]
    [InlineData("""SELECT E'a\'; b'; SELECT $1""")]
    [InlineData("SELECT $$a; b$$; SELECT $1")]
    [InlineData("SELECT $body$a; b$body$; SELECT $1")]
    [InlineData("SELECT a AS \"a; b\"; SELECT $2")]
    /* A comment-blind but string-aware scan opens a string at this apostrophe and never sees the
       separator. */
    [InlineData("/* it's fine */ SELECT $1; SELECT $2")]
    /* An e that ENDS an identifier is not an E'…' prefix, so the backslash is data, the quote closes the
       string, and the ; is a real separator. Reading a plain string as an escape string is the one
       direction that hides one, so the ambiguous case is scanned as plain. */
    [InlineData("""SELECT ase'\'; x'""")]
    /* Not stripped. A normalized pg_stat_statements entry carries no terminator, so a trailing ; is text
       from somewhere else, and trimming it is the quiet transformation this guard exists to refuse. */
    [InlineData("SELECT a FROM t WHERE x = $1;")]
    [InlineData("SELECT a FROM t WHERE x = $1; /* done */")]
    /* Unterminated, in every form. Refused rather than read to the end of the text: the tail of a
       construct nobody can close is not a statement. */
    [InlineData("SELECT $1 /* ;")]
    [InlineData("SELECT $1 /* /* ; */")]
    [InlineData("SELECT 'a; b")]
    [InlineData("""SELECT E'a\'; b""")]
    [InlineData("SELECT $tag$ a; b")]
    [InlineData("SELECT $$ a; b")]
    /* A tag closes only on itself: $aa$ is not $a$. */
    [InlineData("SELECT $a$ x $aa$ y")]
    [InlineData("SELECT a AS \"a; b")]
    /* Zero statements is not one statement. */
    [InlineData("/* just a comment ; */")]
    [InlineData("-- nothing here ;")]
    [InlineData("   \n\t ")]
    [InlineData("")]
    [InlineData(null)]
    public void AnythingThatIsNotExactlyOneStatement_IsRefused(string? statement)
        => Assert.False(HypotheticalIndexExperiment.IsSingleStatement(statement));

    /// <summary>
    /// The guard is a PREDICATE, and that is the property that keeps the feature working. It returns a
    /// verdict, never text, so there is no path by which it rewrites, escapes or trims the statement, and
    /// the <c>$1</c> placeholders <c>GENERIC_PLAN</c> needs reach the EXPLAIN exactly as captured. The
    /// staging bind is asserted alongside it, because that is the value the guard cleared.
    /// </summary>
    [Fact]
    public void TheGuardIsAPredicate_SoTheTextItClearsIsNeverRewritten()
    {
        var source = ExperimentSource();

        Assert.Contains("public static bool IsSingleStatement(string? statementText)", source, StringComparison.Ordinal);
        Assert.Contains("stage.Parameters.AddWithValue(statementText)", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The guard runs at BOTH ends: the entry point, so a refused candidate costs the monitored server no
    /// round trip at all, and the sink, so a second caller of the private EXPLAIN helper cannot reach the
    /// concatenation without it.
    /// </summary>
    [Fact]
    public void TheGuardRunsAtTheEntryPointAndAtTheSink()
    {
        var source = ExperimentSource();

        Assert.Contains("if (!IsSingleStatement(normalizedStatementText))", source, StringComparison.Ordinal);
        Assert.Contains("if (!IsSingleStatement(statementText))", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// And the barrier the guard sits UNDER rather than replaces: the EXPLAIN only PLANS. A refactor that
    /// added <c>ANALYZE</c> would execute monitored-server text as the monitoring credential on the
    /// monitored server, and no single-statement check makes that acceptable. Asserted against the SQL
    /// itself, not the file, because the surrounding prose says the word too.
    /// </summary>
    [Fact]
    public void TheExplainStillOnlyPlans()
    {
        var sql = ExplainSql();

        Assert.Contains("EXPLAIN (GENERIC_PLAN, FORMAT JSON) ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ANALYZE", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The body of the <c>ExplainThroughGucSql</c> raw string literal, which is the SQL that actually
    /// reaches the monitored server.
    /// </summary>
    private static string ExplainSql()
    {
        var source = ExperimentSource();
        const string opening = "ExplainThroughGucSql = \"\"\"";

        var start = source.IndexOf(opening, StringComparison.Ordinal);
        Assert.True(start >= 0, "ExplainThroughGucSql is gone or renamed — this pin needs re-anchoring.");

        start += opening.Length;
        var end = source.IndexOf("\"\"\";", start, StringComparison.Ordinal);
        Assert.True(end > start, "ExplainThroughGucSql is no longer a raw string literal.");

        return source[start..end];
    }

    private static string ExperimentSource()
        => File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "Targets", "HypotheticalIndexExperiment.cs"));

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
