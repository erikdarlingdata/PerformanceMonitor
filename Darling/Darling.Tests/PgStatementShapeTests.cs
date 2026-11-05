/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7681: the shape comparison that lets the text fetch recognize an entry <c>pg_stat_statements</c> created from the service's
/// OLD, unmarked statement. Every pair below is a statement as the C# source writes it and the text a real PostgreSQL 18.6
/// <c>pg_stat_statements</c> stored for it (taken from the view, not written by hand).
/// </summary>
public sealed class PgStatementShapeTests
{
    public static TheoryData<string, string> SourceAndStoredText => new()
    {
        /* Constants of every kind, a negative number written two ways, a dollar-quoted string, an E'' string, and IN lists
           (PostgreSQL 18 squashes a list to one placeholder and a comment). */
        {
            """SELECT 1, -2, - 3, 4.5, -6.5e3, 'a''b', E'x\ny', $$dol$$, $tag$d2$tag$, true, false, NULL, x'1F'::int FROM generate_series(1, 2) AS g(i) WHERE g.i IN (1, 2, 3) AND g.i <> -1 AND g.i > - 9 AND g.i NOT IN (-1, -2);""",
            """SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::int FROM generate_series($14, $15) AS g(i) WHERE g.i IN ($16 /*, ... */) AND g.i <> $17 AND g.i > $18 AND g.i NOT IN ($19 /*, ... */)"""
        },
        /* A mixed-case keyword and function, a minus that PostgreSQL keeps (binary) beside ones it folds into the constant, a
           VALUES list, and the semicolon and the line comment after it, which the server cuts. */
        {
            "select   Lower('X'),\n   2 *-3, 1-2, 5 - -4, (-7), f.x - 1 from (values (1,2),(3,4)) as f(x,y) where f.x = any(array[1,2]); -- tail",
            "select   Lower($1),\n   $2 *$3, $4-$5, $6 - $7, ($8), f.x - $9 from (values ($10,$11),($12,$13)) as f(x,y) where f.x = any(array[$14 /*, ... */])"
        },
        /* A comment inside the statement (the server keeps it), a quoted identifier with a space and capitals, typed literals. */
        {
            """SELECT /* in */ 1 AS "Mixed Case", INTERVAL '5 minutes', '2020-01-01'::date, date '2020-01-01', ARRAY[1,2,3] FROM pg_class LIMIT 5 OFFSET 3;""",
            """SELECT /* in */ $1 AS "Mixed Case", INTERVAL $2, $3::date, date $4, ARRAY[$5 /*, ... */] FROM pg_class LIMIT $6 OFFSET $7"""
        },
        /* The service's own $n parameter stays a parameter, a single-element IN list, ANY over an array, IS NULL and a bare TRUE. */
        {
            "SELECT pg_sleep(0.0) WHERE 1 = 1 AND -1 < 2 AND 'a' IN ('b') AND 10 = ANY (ARRAY[-1,2]) AND NULL IS NULL AND true;",
            "SELECT pg_sleep($1) WHERE $2 = $3 AND $4 < $5 AND $6 IN ($7) AND $8 = ANY (ARRAY[$9 /*, ... */]) AND $10 IS NULL AND $11"
        },
        { "SELECT pg_sleep($1)", "SELECT pg_sleep($1)" },
        { "SELECT pg_sleep(0.5)", "SELECT pg_sleep($1)" },
        /* The marker is a comment, and it sits before the semicolon. */
        { "SELECT 1 FROM t WHERE a = $1\n" + PgOwnStatement.Marker + ";", "SELECT 1 FROM t WHERE a = $1" },
        /* A nested block comment, and a comment line between two tokens. */
        { "SELECT /* a /* b */ c */ x -- note\nFROM t", "select x from t" },
        /* Measured on 18.6 (#7697): the server writes a select list, a function's arguments and a VALUES row list out in full,
           so the commas stay in the shape. */
        { "select 1, 2", "select $1, $2" },
        { "select coalesce(1, 2, 3), greatest(4, 5)", "select coalesce($1, $2, $3), greatest($4, $5)" },
        { "insert into t values (1,2,3),(4,5,6)", "insert into t values ($1,$2,$3),($4,$5,$6)" },
        /* An IN list with a column in it is not squashed. */
        { "select * from t where a in (1, b, 3)", "select * from t where a in ($1, b, $2)" },
        /* A squashed IN list and a squashed ANY (ARRAY[...]) beside literals that stay separate. */
        {
            "SELECT 1, 2 FROM t WHERE a IN (1, 2, 3) AND b = ANY (ARRAY[4, 5]) AND c NOT IN (-6, -7)",
            "SELECT $1, $2 FROM t WHERE a IN ($3 /*, ... */) AND b = ANY (ARRAY[$4 /*, ... */]) AND c NOT IN ($5 /*, ... */)"
        },
        /* A DO block is stored as written, without its semicolon. */
        { "DO $x$ BEGIN PERFORM 1; END $x$;", "DO $x$ BEGIN PERFORM 1; END $x$" },
    };

    [Theory]
    [MemberData(nameof(SourceAndStoredText))]
    public void TheSourceText_AndTheTextPostgreSqlStoredForIt_HaveOneShape(string source, string stored) =>
        Assert.Equal(PgStatementShape.Key(source), PgStatementShape.Key(stored));

    public static TheoryData<string, string> DifferentStatements => new()
    {
        /* Two statements that differ only in a table name. */
        { "SELECT a FROM orders WHERE id = 1", "SELECT $1 FROM orders_archive WHERE id = $2" },
        { "SELECT a FROM orders WHERE id = 1", "SELECT a FROM orders WHERE id = $1 AND status = $2" },
        /* A quoted identifier keeps its case; an unquoted one does not. */
        { "SELECT \"Abc\" FROM t", "SELECT \"abc\" FROM t" },
        { "SELECT x FROM t", "SELECT \"x\" FROM t" },
        /* A different operator, a different literal position, a different function. */
        { "SELECT a FROM t WHERE b = 1", "SELECT a FROM t WHERE b <> 1" },
        { "SELECT a FROM t WHERE b = 1", "SELECT a FROM t WHERE 1 = b" },
        { "SELECT lower(a) FROM t", "SELECT upper(a) FROM t" },
        /* Statements that differ only in how many literals they carry are different entries in pg_stat_statements (#7697). */
        { "SELECT 1", "SELECT $1, $2" },
        { "SELECT 1, 2", "SELECT 1, 2, 3" },
        { "SELECT f($1)", "SELECT f($1, $2)" },
        { "SELECT pg_catalog.current_setting('a')", "SELECT pg_catalog.current_setting($1, $2)" },
        { "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES ($1, $2)" },
        /* A squashed IN list is one placeholder, but the commas outside it still count, and so does a column inside it. */
        { "SELECT 1 FROM t WHERE a IN (1, 2, 3)", "SELECT 1, 2 FROM t WHERE a IN ($1 /*, ... */)" },
        { "SELECT 1 FROM t WHERE a IN (1, 2)", "SELECT 1 FROM t WHERE a IN ($1, b)" },
        /* A DO block is not normalized, so a different body is a different statement. */
        { "DO $a$ BEGIN PERFORM 1; END $a$", "DO $a$ BEGIN PERFORM 2; END $a$" },
    };

    [Theory]
    [MemberData(nameof(DifferentStatements))]
    public void StatementsThatDifferInAnythingButLiterals_HaveDifferentShapes(string first, string second) =>
        Assert.NotEqual(PgStatementShape.Key(first), PgStatementShape.Key(second));

    [Fact]
    public void ALiteralInsideAString_OrAComment_IsNotAToken()
    {
        Assert.Equal(PgStatementShape.Key("SELECT 'a -- b /* c' , x"), PgStatementShape.Key("SELECT $1, x"));
        Assert.Equal(PgStatementShape.Key("SELECT x /* 'unterminated */ FROM t"), PgStatementShape.Key("SELECT x FROM t"));
        Assert.Equal(new List<string> { "select", PgStatementShape.Wildcard, "from", "t" }, PgStatementShape.Tokenize("SELECT -5 FROM t;"));
    }

    [Fact]
    public void AnInSubquery_KeepsItsSelectListCommas_AndAConstantListInsideItStillCollapses()
    {
        var w = PgStatementShape.Wildcard;
        Assert.Equal(new List<string> { "x", "in", "(", "select", w, ",", w, ")" }, PgStatementShape.Tokenize("x IN (SELECT 1, 2)"));
        Assert.Equal(new List<string> { "x", "in", "(", w, ")" }, PgStatementShape.Tokenize("x IN (1, 2, 3)"));
        Assert.Equal(
            new List<string> { "x", "in", "(", "select", "a", "from", "t", "where", "b", "in", "(", w, ")", ")" },
            PgStatementShape.Tokenize("x IN (SELECT a FROM t WHERE b IN (1, 2, 3))"));
        Assert.Equal(
            new List<string> { "x", "in", "(", "with", "c", "as", "(", "select", w, ")", "select", "*", "from", "c", ")" },
            PgStatementShape.Tokenize("x IN (WITH c AS (SELECT 1) SELECT * FROM c)"));
        Assert.Equal(new List<string> { "x", "in", "(", "values", "(", w, ",", w, ")", ")" }, PgStatementShape.Tokenize("x IN (/* c */ VALUES (1, 2))"));
        Assert.Equal(new List<string> { "x", "in", "(", "table", "t", ")" }, PgStatementShape.Tokenize("x IN (TABLE t)"));
    }

    [Fact]
    public void TheRegistry_LearnsEachStatementTheServiceMarks_ByItsShape()
    {
        var sql = "SELECT pm7681_reg_a FROM pg_catalog.pg_class WHERE relkind IN ('r', 'm') AND relpages > 128";
        Assert.False(PgOwnStatement.IsServiceStatementText("SELECT pm7681_reg_a FROM pg_catalog.pg_class WHERE relkind IN ($1 /*, ... */) AND relpages > $2"));

        var marked = PgOwnStatement.Mark(sql + ";");
        Assert.EndsWith(PgOwnStatement.Marker + ";", marked, StringComparison.Ordinal);
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT pm7681_reg_a FROM pg_catalog.pg_class WHERE relkind IN ($1 /*, ... */) AND relpages > $2"));
        Assert.True(PgOwnStatement.IsServiceStatementText(sql.ToUpperInvariant().Replace("PM7681_REG_A", "pm7681_reg_a", StringComparison.Ordinal)));
        Assert.False(PgOwnStatement.IsServiceStatementText("SELECT pm7681_reg_a FROM pg_catalog.pg_namespace WHERE relkind IN ($1 /*, ... */) AND relpages > $2"));
        Assert.False(PgOwnStatement.IsServiceStatementText(null));
    }

    [Fact]
    public void TheRegistry_LearnsEveryStatementOfABatch_AndAStatementThatWasMarkedAlready()
    {
        PgOwnStatement.MarkBatch(["SELECT pm7681_batch_a FROM t1 WHERE id = 7", "SELECT pm7681_batch_b FROM t2"]);
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT pm7681_batch_a FROM t1 WHERE id = $1"));
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT pm7681_batch_b FROM t2"));

        PgOwnStatement.Mark("SELECT pm7681_premarked FROM t3\n" + PgOwnStatement.Marker + ";");
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT pm7681_premarked FROM t3"));
    }

    [Fact]
    public void ASemicolon_InALiteralOrADollarQuotedBody_IsNotAStatementBoundary()
    {
        Assert.Single(PgStatementShape.Keys("SELECT 'a;b' AS x, 1"));
        Assert.Single(PgStatementShape.Keys("SELECT $$a;b$$, 1"));
        Assert.Single(PgStatementShape.Keys("SELECT \"a;b\" FROM t"));
        Assert.Equal(PgStatementShape.Key("SELECT x FROM t"), Assert.Single(PgStatementShape.Keys("SELECT x FROM t;")));

        /* Three statements, an empty one between the last two. */
        var keys = PgStatementShape.Keys("SELECT 'a;b' FROM t; DO $q$ BEGIN PERFORM 1; PERFORM 2; END $q$;; SELECT 3;");
        Assert.Equal(new[] { PgStatementShape.Key("SELECT $1 FROM t"), PgStatementShape.Key("DO $q$ BEGIN PERFORM 1; PERFORM 2; END $q$"), PgStatementShape.Key("SELECT $1") }, keys);
    }

    [Fact]
    public void AStringWithSeveralStatements_IsRegisteredOneStatementAtATime()
    {
        /* pg_stat_statements stores each statement of the string on its own, so each one's stored text must be recognized. */
        PgOwnStatement.Mark("SELECT pm7697_ms_a FROM t1 WHERE id = 7; SELECT pm7697_ms_b, 2 FROM t2");
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT pm7697_ms_a FROM t1 WHERE id = $1"));
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT pm7697_ms_b, $1 FROM t2"));

        /* A semicolon in a string literal or in a dollar-quoted body does not split a statement. */
        PgOwnStatement.Mark("SELECT 'pm7697;lit' FROM pm7697_lit; SELECT $q$x;y$q$ FROM pm7697_dq");
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT $1 FROM pm7697_lit"));
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT $1 FROM pm7697_dq"));
        Assert.False(PgOwnStatement.IsServiceStatementText("SELECT $1"));

        /* The same text again is a no-op: still recognized, nothing else becomes so. */
        PgOwnStatement.Mark("SELECT pm7697_ms_a FROM t1 WHERE id = 7; SELECT pm7697_ms_b, 2 FROM t2");
        Assert.True(PgOwnStatement.IsServiceStatementText("SELECT pm7697_ms_a FROM t1 WHERE id = $1"));
        Assert.False(PgOwnStatement.IsServiceStatementText("SELECT pm7697_ms_b FROM t2"));
    }

    [Fact]
    public void TheAuroraProbe_IsLearnedStatementByStatement_ByTheTextStoredBeforeTheMarker()
    {
        var unmarked = DarlingServerConnector.PostgresAuroraProbeQueryText.Replace("{0}", "pm7697_aurora", StringComparison.Ordinal);
        var semicolon = unmarked.IndexOf("$aurora_probe$;", StringComparison.Ordinal);
        Assert.True(semicolon > 0);

        /* What an older build's probe left in pg_stat_statements: the DO block as written (the server cuts its semicolon), and the
           SELECT with its constants replaced. */
        var storedDo = unmarked[..(semicolon + "$aurora_probe$".Length)];
        const string StoredSelect = "SELECT pg_catalog.current_setting($1, $2)";
        Assert.False(PgOwnStatement.IsServiceStatementText(storedDo));

        var marked = DarlingServerConnector.MarkAuroraProbe(unmarked);
        Assert.EndsWith(PgOwnStatement.Marker, marked, StringComparison.Ordinal);
        Assert.True(PgOwnStatement.IsServiceStatementText(storedDo));
        Assert.True(PgOwnStatement.IsServiceStatementText(StoredSelect));
    }

    [Fact]
    public void ADoBlock_IsRecognizedByItsBody_NotByTheKeyword()
    {
        /* The server does not normalize a DO block, and each body is its own entry, so an application's DO block must not match
           the service's. */
        PgOwnStatement.Mark("DO $pm7697_do$ BEGIN PERFORM 1; END $pm7697_do$");
        Assert.True(PgOwnStatement.IsServiceStatementText("DO $pm7697_do$ BEGIN PERFORM 1; END $pm7697_do$"));
        Assert.False(PgOwnStatement.IsServiceStatementText("DO $pm7697_do$ BEGIN PERFORM 2; END $pm7697_do$"));
        Assert.False(PgOwnStatement.IsServiceStatementText("DO $$ BEGIN PERFORM 1; END $$"));
    }

    [Fact]
    public void AnEntryWhoseTextMatchesAServiceStatement_JoinsTheOwnEntries_ThoseAlreadyMarkedStay()
    {
        PgOwnStatement.Mark("SELECT pm7681_merge FROM t WHERE a = 1");
        var matching = "SELECT pm7681_merge FROM t WHERE a = $1";
        var candidates = Candidates((10, 5, matching), (20, 5, matching), (30, 5, "SELECT pm7681_merge FROM t_other WHERE a = $1"));

        /* No candidates: the marked list is returned as it is, NULL included. */
        Assert.Null(PgStatementText.WithMatchedEntries(null, null));
        Assert.Equal("[{\"u\":1,\"d\":2}]", PgStatementText.WithMatchedEntries("[{\"u\":1,\"d\":2}]", null));

        var learned = PgStatementText.WithMatchedEntries(null, candidates);
        Assert.Equal([(10L, 5L), (20L, 5L)], Pairs(learned));

        /* Added to the marked entries, and an entry both lists name is kept once. */
        var merged = PgStatementText.WithMatchedEntries("[{\"d\": 5, \"u\": 10}, {\"d\": 9, \"u\": 99}]", candidates);
        Assert.Equal([(10L, 5L), (20L, 5L), (99L, 9L)], Pairs(merged).OrderBy(p => p.Item1).ToArray());

        /* Nothing matches: NULL stays NULL. */
        Assert.Null(PgStatementText.WithMatchedEntries(null, Candidates((30, 5, "SELECT pm7681_merge FROM t_other WHERE a = $1"))));
    }

    private static string Candidates(params (long User, long Database, string Text)[] entries) =>
        JsonSerializer.Serialize(entries.Select(e => new Dictionary<string, object> { ["u"] = e.User, ["d"] = e.Database, ["t"] = e.Text }));

    private static (long, long)[] Pairs(string? json)
    {
        Assert.NotNull(json);
        using var document = JsonDocument.Parse(json);
        return document.RootElement.EnumerateArray().Select(e => (e.GetProperty("u").GetInt64(), e.GetProperty("d").GetInt64())).ToArray();
    }
}
