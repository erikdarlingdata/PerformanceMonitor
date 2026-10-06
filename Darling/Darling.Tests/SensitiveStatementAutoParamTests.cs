/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348: the auto-parameter scope of the XML walk. SQL Server stores a simple- or forced-parameterized statement
/// under its parameterized text (<c>set [password] = @1</c>) and moves the literal into the statement's
/// <c>ParameterList</c>, so the stored text alone names nothing. The walk puts each literal back (the probe), judges
/// that, and a named probe withholds the statement's parameter values while the statement text itself is kept.
///
/// The judge here is a STAND-IN for the .NET evaluation that lands with the pattern work (A1): it mirrors the two
/// T-SQL alternatives that matter here (a credential-shaped name, optionally bracketed or quoted, then <c>=</c> and a
/// string or binary literal; and <c>pwd=</c> as a connection-string key), with ASCII word boundaries. The real judge
/// runs over the captured fixtures in the wiring wave.
/// </summary>
public class SensitiveStatementAutoParamTests
{
    private const string P = "<<P>>";
    private const string PText = "&lt;&lt;P&gt;&gt;";
    private static readonly XNamespace Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    // Stand-in for the .NET evaluation of T4 and T9 (see the class comment).
    private static readonly Regex T4 = new(@"(password|pwd|secret)[\]""]?\s*=\s*(n?'|0x)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex T9 = new(@"(?<![A-Za-z0-9_])pwd\s*=",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static bool StandIn(string s) => T4.IsMatch(s) || T9.IsMatch(s);

    private readonly ITestOutputHelper _output;

    public SensitiveStatementAutoParamTests(ITestOutputHelper output) => _output = output;

    private static string Fixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "StatementScrub", name));

    private static string? Run(string xml, Func<string, bool>? judge = null, int max = int.MaxValue) =>
        SensitiveStatements.XmlCore(xml, judge ?? StandIn, () => false, P, null, max);

    private static XElement Col(string column, string? compiled, string? runtime = null)
    {
        var e = new XElement(Ns + "ColumnReference", new XAttribute("Column", column));
        if (compiled is not null)
            e.Add(new XAttribute("ParameterCompiledValue", compiled));
        if (runtime is not null)
            e.Add(new XAttribute("ParameterRuntimeValue", runtime));
        return e;
    }

    private static XElement Stmt(string element, string text, params XElement[] columns)
    {
        var plan = new XElement(Ns + "QueryPlan");
        if (columns.Length > 0)
            plan.Add(new XElement(Ns + "ParameterList", columns));
        return new XElement(Ns + element, new XAttribute("StatementText", text), new XAttribute("StatementId", "1"), plan);
    }

    private static string Plan(params XElement[] statements) =>
        new XElement(Ns + "ShowPlanXML",
            new XElement(Ns + "BatchSequence", new XElement(Ns + "Batch", new XElement(Ns + "Statements", statements))))
            .ToString(SaveOptions.DisableFormatting);

    private static List<string> Attrs(string xml, string name) =>
        XDocument.Parse(xml).Descendants().Select(e => (string?)e.Attribute(name)).Where(v => v is not null).Select(v => v!).ToList();

    private static string OnlyStatementText(string xml) => Attrs(xml, "StatementText").Single();

    // ── the RED on L2's code (no probe): the stored form names nothing, so nothing was withheld ──

    [Theory]
    [InlineData("(@1 nvarchar(4000),@2 tinyint)UPDATE [dbo].[t] set [password] = @1  WHERE [id]=@2")]
    [InlineData("(@1 nvarchar(4000),@2 tinyint)UPDATE [dbo].[t] set [pwd] = @1  WHERE [id]=@2")]
    [InlineData("(@1 nvarchar(4000),@2 tinyint)UPDATE [dbo].[t] set [secret] = @1  WHERE [id]=@2")]
    public void SetCredentialColumnToToken_WithholdsTheCompiledValues_AndKeepsTheStatementText(string text)
    {
        string xml = Plan(Stmt("StmtSimple", text, Col("@1", "N'S3cret-value'"), Col("@2", "(7)")));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.NotSame(xml, result);
        Assert.Equal(new[] { P, P }, Attrs(result!, "ParameterCompiledValue"));
        Assert.Equal(text, OnlyStatementText(result!));
        Assert.DoesNotContain("S3cret-value", result);
    }

    [Fact]
    public void WhereClauseOfTwoTokens_WithholdsBothValues_AndKeepsTheStatementText()
    {
        const string text = "(@1 nvarchar(4000),@2 nvarchar(4000))SELECT [id] FROM [dbo].[t] WHERE [username]=@1 AND [password]=@2";
        string xml = Plan(Stmt("StmtSimple", text, Col("@2", "N'S3cret-value'"), Col("@1", "N'app'")));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.Equal(new[] { P, P }, Attrs(result!, "ParameterCompiledValue"));
        Assert.Equal(text, OnlyStatementText(result!));
    }

    [Theory]
    [InlineData("SELECT [id] FROM [dbo].[t] WHERE [secret_id]=@1")]
    [InlineData("SELECT [id] FROM [dbo].[t] WHERE [PasswordHash]=@1")]
    [InlineData("SELECT @@ROWCOUNT WHERE [id]=@@SPID")]
    public void NameOnlyLooksSimilar_KeepsItsValues_AndReturnsTheSameInstance(string text)
    {
        string xml = Plan(Stmt("StmtSimple", text, Col("@1", "N'plain-value'")));

        Assert.Same(xml, Run(xml));
    }

    [Fact]
    public void TokensAreMatchedByOrdinal_AtTenNeverTakesTheValueOfOne()
    {
        // @10 is a different parameter from @1. A prefix replace would read [pwd]=N'a'0 and withhold; the right
        // probe is [pwd]=(5), a number, which names nothing.
        string xml = Plan(Stmt("StmtSimple", "SELECT [id] FROM [dbo].[t] WHERE [pwd]=@10", Col("@1", "N'a'"), Col("@10", "(5)")));

        Assert.Same(xml, Run(xml));
    }

    [Fact]
    public void TokenFollowedByWordCharacter_OrPrecededByOne_IsNotAToken()
    {
        string xml = Plan(Stmt("StmtSimple", "SELECT [id] FROM [dbo].[t] WHERE [pwd]=@1abc OR [pwd]=a@1",
            Col("@1", "N'S3cret-value'")));

        Assert.Same(xml, Run(xml));
    }

    [Fact]
    public void ForcedParameterization_TheZeroTokenIsCovered()
    {
        string xml = Plan(Stmt("StmtSimple", "(@0 varchar(8000))SELECT [id] FROM [dbo].[t] WHERE [password] = @0",
            Col("@0", "'S3cret-value'")));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.Equal(new[] { P }, Attrs(result!, "ParameterCompiledValue"));
    }

    [Fact]
    public void ApplicationNamedParameter_KeepsItsValue()
    {
        // sp_executesql: the statement text keeps the application's own parameter name, which is not a token.
        string xml = Plan(Stmt("StmtSimple", "(@pwd nvarchar(50))UPDATE [dbo].[t] SET [password] = @pwd",
            Col("@pwd", "N'S3cret-value'")));

        Assert.Same(xml, Run(xml));
    }

    [Fact]
    public void ValueRulesFireOnThePutBackLiteral_AndTheQuestionMarkFormWouldNotName()
    {
        const string text = "(@1 nvarchar(4000))INSERT [dbo].[cfg] VALUES (@1)";
        string withValue = Plan(Stmt("StmtSimple", text, Col("@1", "N'Server=h;PWD=x'")));
        string noValue = Plan(Stmt("StmtSimple", text));

        string? result = Run(withValue);

        Assert.NotNull(result);
        Assert.Equal(new[] { P }, Attrs(result!, "ParameterCompiledValue"));
        Assert.Equal(text, OnlyStatementText(result!));
        // with no value to put back the probe reads VALUES (N'?'), which names nothing
        Assert.Same(noValue, Run(noValue));
    }

    [Fact]
    public void NumericValue_NamesNothing()
    {
        string xml = Plan(Stmt("StmtSimple", "(@1 int)INSERT [dbo].[cfg] VALUES (@1)", Col("@1", "(7)")));

        Assert.Same(xml, Run(xml));
    }

    [Fact]
    public void RuntimeValue_IsUsedWhenThereIsNoCompiledValue()
    {
        string xml = Plan(Stmt("StmtSimple", "(@1 nvarchar(4000))UPDATE [dbo].[t] set [password] = @1",
            Col("@1", null, "N'S3cret-value'")));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.Equal(new[] { P }, Attrs(result!, "ParameterRuntimeValue"));
    }

    [Fact]
    public void TokenWithNoParameterListEntry_FallsBackToTheQuestionMark_AndACredentialNameStillNames()
    {
        const string text = "(@1 nvarchar(4000))UPDATE [dbo].[t] set [password] = @1";
        var stmt = Stmt("StmtSimple", text);
        stmt.Element(Ns + "QueryPlan")!.Add(new XElement(Ns + "ScalarOperator", new XAttribute("ScalarString", "[plain-text]")));
        string xml = Plan(stmt);

        string? result = Run(xml);

        // [password] = N'?' is still named, so the scope opens (the ScalarString inside is withheld); the text is kept
        Assert.NotNull(result);
        Assert.Equal(P, Attrs(result!, "ScalarString").Single());
        Assert.Equal(text, OnlyStatementText(result!));
    }

    [Fact]
    public void KeptStatementTextOfAProbeScope_SurvivesWhenItHoldsAQuote()
    {
        // P13: inside a scope any value holding a quote is withheld (L-J), except the probe scope's own text.
        const string text = "(@1 nvarchar(4000))UPDATE [dbo].[t] set [password] = @1, [note] = 'x'";
        string xml = Plan(Stmt("StmtSimple", text, Col("@1", "N'S3cret-value'")));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.Equal(text, OnlyStatementText(result!));
        Assert.Equal(new[] { P }, Attrs(result!, "ParameterCompiledValue"));
    }

    [Fact]
    public void NestedStatement_EachParameterListBelongsToItsOwnStatement()
    {
        // Outer StmtCond: [pwd]=@1 with a number (names nothing). Inner StmtSimple: [pwd]=@1 with a literal (named).
        // A ParameterList read for the wrong statement would withhold the outer one (N'?' fallback) or keep the inner.
        var inner = Stmt("StmtSimple", "SELECT [id] FROM [dbo].[t] WHERE [pwd]=@1", Col("@1", "N'S3cret-inner'"));
        var outer = new XElement(Ns + "StmtCond",
            new XAttribute("StatementText", "IF EXISTS (SELECT 1 FROM [dbo].[t] WHERE [pwd]=@1)"),
            new XElement(Ns + "Condition", new XElement(Ns + "QueryPlan",
                new XElement(Ns + "ParameterList", Col("@1", "(5)")))),
            new XElement(Ns + "Then", new XElement(Ns + "Statements", inner)));
        string xml = Plan(outer);

        string? result = Run(xml);

        Assert.NotNull(result);
        var values = Attrs(result!, "ParameterCompiledValue");
        Assert.Equal(new[] { "(5)", P }, values);
        Assert.DoesNotContain("S3cret-inner", result);
        Assert.Equal(2, Attrs(result!, "StatementText").Count);
    }

    [Fact]
    public void SiblingStatements_AreJudgedOnTheirOwn()
    {
        var named = Stmt("StmtSimple", "(@1 nvarchar(4000))UPDATE [dbo].[t] set [password] = @1", Col("@1", "N'S3cret-value'"));
        var plain = Stmt("StmtSimple", "(@1 nvarchar(4000))SELECT [id] FROM [dbo].[t] WHERE [name]=@1", Col("@1", "N'plain-value'"));
        string xml = Plan(named, plain);

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.Equal(new[] { P, "N'plain-value'" }, Attrs(result!, "ParameterCompiledValue"));
    }

    [Fact]
    public void ParameterListPastTheCut_StillWithholdsTheStatementByName()
    {
        var plan = new XElement(Ns + "QueryPlan",
            new XElement(Ns + "RelOp", new XAttribute("NodeId", "0"),
                new XElement(Ns + "ScalarOperator", new XAttribute("ScalarString", "[plain-text]"))),
            new XElement(Ns + "RelOp", new XAttribute("NodeId", "1"), new XAttribute("Filler", new string('x', 4000))),
            new XElement(Ns + "ParameterList", Col("@1", "N'S3cret-value'")));
        var stmt = new XElement(Ns + "StmtSimple",
            new XAttribute("StatementText", "(@1 nvarchar(4000))UPDATE [dbo].[t] set [password] = @1"), plan);
        string xml = Plan(stmt);
        Assert.True(xml.IndexOf("<ParameterList>", StringComparison.Ordinal) > 1500);

        // 1,000 characters in: the compiled value lies past the cut, so the probe reads [password] = N'?' (named),
        // and the scope withholds the ScalarString that sits inside the cut.
        string? result = Run(xml, max: 1000);

        Assert.NotNull(result);
        Assert.DoesNotContain("plain-text", result);
        Assert.Contains(PText, result);
    }

    // ── L2c review fixes (H1, M1, L3) ──

    private static bool CanaryOrStandIn(string s) => StandIn(s) || s.Contains("CANARY", StringComparison.Ordinal);

    [Fact]
    public void WithAMaxOutput_ValuesPastPassOnesStop_AreWithheldEvenWhenTheOutputIsShorter_LongNamedStatementFirst()
    {
        // statement 1 is named and long: its text becomes the short placeholder, so pass 2 reaches input that pass 1
        // (which stops by INPUT offset) never read
        string xml = Plan(
            Stmt("StmtSimple", "CANARY " + new string('x', 3000)),
            Stmt("StmtSimple", "(@1 nvarchar(4000),@2 tinyint)UPDATE [dbo].[u] set [password] = @1  WHERE [id]=@2",
                Col("@1", "N'S3cret-drift'"), Col("@2", "(7)")));
        const int max = 2000;

        string? cut = Run(xml, CanaryOrStandIn, max);
        string? whole = Run(xml, CanaryOrStandIn);

        Assert.NotNull(cut);
        Assert.NotNull(whole);
        Assert.DoesNotContain("S3cret-drift", whole);
        Assert.DoesNotContain("S3cret-drift", cut![..Math.Min(max, cut.Length)]);
    }

    [Fact]
    public void WithAMaxOutput_ValuesPastPassOnesStop_AreWithheldEvenWhenTheOutputIsShorter_LargeInList()
    {
        // &apos; is five characters in the input and one in the output, so pass 2 outruns pass 1 on a long IN-list
        var sb = new StringBuilder();
        sb.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>");
        sb.Append("<StmtSimple StatementId=\"1\" StatementText=\"(@0 nvarchar(50)");
        for (int i = 1; i <= 119; i++) sb.Append(",@").Append(i).Append(" nvarchar(50)");
        sb.Append(")INSERT [dbo].[cfg] VALUES (@0");
        for (int i = 1; i <= 119; i++) sb.Append(",@").Append(i);
        sb.Append(")\"><QueryPlan><ScalarOperator ScalarString=\"CANARY\"/><ParameterList>");
        for (int i = 0; i < 119; i++)
            sb.Append("<ColumnReference Column=\"@").Append(i).Append("\" ParameterCompiledValue=\"N&apos;aaaaaaaa&apos;\"/>");
        sb.Append("<ColumnReference Column=\"@119\" ParameterCompiledValue=\"N&apos;Server=h;PWD=drift2&apos;\"/>");
        sb.Append("</ParameterList></QueryPlan></StmtSimple>");
        sb.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        string xml = sb.ToString();
        int last = xml.IndexOf("PWD=drift2", StringComparison.Ordinal);
        int max = last - 200;
        Assert.True(max > 1000, "fixture shape");

        string? cut = Run(xml, CanaryOrStandIn, max);

        Assert.NotNull(cut);
        Assert.DoesNotContain("drift2", cut![..Math.Min(max, cut.Length)]);
    }

    [Fact]
    public void RuntimeValueThatDiffersFromTheCompiledValue_IsJudgedToo()
    {
        // an actual plan: the compiled value is whichever run compiled the cached plan, the runtime value is this run's
        const string text = "(@1 nvarchar(50))INSERT [dbo].[cfg] VALUES (@1)";
        string xml = Plan(Stmt("StmtSimple", text, Col("@1", "N'benign'", "N'Server=h;PWD=rt3'")));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.NotSame(xml, result);
        Assert.DoesNotContain("rt3", result);
        Assert.Equal(new[] { P }, Attrs(result!, "ParameterCompiledValue"));
        Assert.Equal(new[] { P }, Attrs(result!, "ParameterRuntimeValue"));
        Assert.Equal(text, OnlyStatementText(result!));

        // both benign (equal, then different): nothing is named and the same instance comes back
        string same = Plan(Stmt("StmtSimple", text, Col("@1", "N'benign'", "N'benign'")));
        string differ = Plan(Stmt("StmtSimple", text, Col("@1", "N'benign'", "N'other'")));
        Assert.Same(same, Run(same));
        Assert.Same(differ, Run(differ));
    }

    [Fact]
    public void AValueWhoseTokenIsNotInTheStatementText_IsJudgedOnItsOwn()
    {
        // the stored text is shorter than the statement, so no token puts the value back into the probe
        const string text = "INSERT [dbo].[cfg] VALUES (";
        string xml = Plan(Stmt("StmtSimple", text, Col("@1", "N'Server=h;PWD=s7'")));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.DoesNotContain("s7", result);
        Assert.Equal(new[] { P }, Attrs(result!, "ParameterCompiledValue"));
        Assert.Equal(text, OnlyStatementText(result!));

        // a plain value, and an application-named parameter, name nothing
        string plain = Plan(Stmt("StmtSimple", text, Col("@1", "N'plain'")));
        string appNamed = Plan(Stmt("StmtSimple", text, Col("@pwd", "N'Server=h;PWD=s7'")));
        Assert.Same(plain, Run(plain));
        Assert.Same(appNamed, Run(appNamed));
    }


    [Fact]
    public void ElementFormParameterizedText_NamedStatementIsWithheld_EvenAfterAnEarlierHit()
    {
        // Statement 1 is named by its own text (the first hit); statement 2 only by an element-form ParameterizedText,
        // which pass 2 reads after the start tag, so pass 1 has to record it.
        var first = Stmt("StmtSimple", "UPDATE [dbo].[t] SET password = N'S3cret-one'", Col("@1", "N'a'"));
        var second = Stmt("StmtSimple", "SELECT 1", Col("@1", "N'keep-or-withhold'"));
        second.Element(Ns + "QueryPlan")!.AddFirst(new XElement(Ns + "ParameterizedText",
            "(@1 nvarchar(50))UPDATE [dbo].[t] SET [password] = N'S3cret-two'"));
        string xml = Plan(first, second);

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.Equal(new[] { P, P }, Attrs(result!, "ParameterCompiledValue"));
        Assert.DoesNotContain("S3cret-two", result);
    }

    // ── the captured fixtures: what SQL Server stored on a real instance ──

    [Theory]
    [InlineData("autoparam_update_prepared.xml")]
    [InlineData("autoparam_select_prepared.xml")]
    public void CapturedPreparedPlan_WithholdsTheLiteral_AndKeepsTheStatementText(string file)
    {
        string xml = Fixture(file);
        string original = OnlyStatementText(xml);

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.NotSame(xml, result);
        Assert.DoesNotContain("S3cret-fixture", result);
        Assert.Equal(original, OnlyStatementText(result!));
        Assert.All(Attrs(result!, "ParameterCompiledValue"), v => Assert.Equal(P, v));
        Assert.NotEmpty(Attrs(result!, "ParameterCompiledValue"));
    }

    [Theory]
    [InlineData("autoparam_update_adhoc_shell.xml")]
    [InlineData("autoparam_select_adhoc_shell.xml")]
    public void CapturedAdhocShell_WithholdsItsLiteralStatementText_AndItsParameterizedTextAttribute(string file)
    {
        string xml = Fixture(file);
        Assert.Contains("S3cret-fixture", OnlyStatementText(xml));
        Assert.Single(Attrs(xml, "ParameterizedText"));

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.Equal(P, OnlyStatementText(result!));
        Assert.Equal(P, Attrs(result!, "ParameterizedText").Single());
        Assert.DoesNotContain("S3cret-fixture", result);
    }

    // ── a plan that does not parse: withheld whole when the probe could have mattered ──

    [Fact]
    public void CutPreparedPlan_InsideItsParameterList_IsWithheldWhole()
    {
        string xml = Fixture("autoparam_update_prepared.xml");
        int at = xml.IndexOf("ParameterCompiledValue=\"N&apos;S3", StringComparison.Ordinal) + 30;
        string cut = xml.Substring(0, at);

        // the judge alone names nothing in the cut text (the stored text is [password] = @1)
        Assert.False(StandIn(System.Net.WebUtility.HtmlDecode(cut)));
        Assert.Equal(P, Run(cut));
    }

    [Fact]
    public void CutPlan_WithNoToken_TakesTheWholeTextPath()
    {
        const string plain = "<ShowPlanXML><StmtSimple StatementText=\"SELECT 1 WHERE [x] = 1\" ParameterCompiledValue=\"(5)\"><QueryPlan";
        const string named = "<ShowPlanXML><StmtSimple StatementText=\"UPDATE t SET password = N'S3cret'\" ParameterCompiledValue=\"(5)\"><QueryPlan";

        Assert.Same(plain, Run(plain));
        Assert.Equal(P, Run(named));
    }

    [Fact]
    public void CutPlan_WithATokenButNoParameterValues_TakesTheWholeTextPath()
    {
        string xml = Fixture("autoparam_update_prepared.xml");
        string cut = xml.Substring(0, xml.IndexOf("<ParameterList>", StringComparison.Ordinal) + 8);
        Assert.Contains("@1", cut);
        Assert.DoesNotContain("ParameterCompiledValue", cut);
        Assert.DoesNotContain("ParameterRuntimeValue", cut);

        // a column the judge does not name: with every token put back as N'?' it still names nothing
        string neutral = cut.Replace("[password]", "[note]", StringComparison.Ordinal);
        Assert.Contains("[note] = @1", neutral);
        Assert.Same(neutral, Run(neutral));
    }

    [Fact]
    public void CutPlan_WithATokenAndNoParameterValues_IsJudgedWithTheTokensPutBackAsAPlaceholderValue()
    {
        // The stored text names nothing on its own (set [password] = @1); the parsed twin judges
        // set [password] = N'?', names it, and withholds the statement's residual literals. The cut text has no
        // parameter value to put back, but the same judgment applies.
        string xml = Fixture("autoparam_update_prepared.xml");
        string cut = xml.Substring(0, xml.IndexOf("<ParameterList>", StringComparison.Ordinal) + 8);
        Assert.Contains("[password] = @1", cut);
        Assert.False(StandIn(System.Net.WebUtility.HtmlDecode(cut)));
        Assert.Equal(P, Run(cut));

        const string issue = "<ShowPlanXML><StmtSimple StatementText=\"UPDATE [t] set [password] = @1 WHERE [note] LIKE N&apos;%abc%&apos;\"";
        Assert.Equal(P, Run(issue));
    }

    // ── measurements: the walk's cost with the probe (printed; the ceilings are loose) ──

    private const string FillerRelOp =
        "<RelOp NodeId=\"1\" PhysicalOp=\"Clustered Index Seek\" LogicalOp=\"Clustered Index Seek\" EstimateRows=\"1\" EstimatedTotalSubtreeCost=\"0.0032\" Parallel=\"0\"><OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"c1\"/></OutputList><IndexScan Ordered=\"1\"><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[pk]\"/></IndexScan></RelOp>";

    private static string BigPlan(int targetChars, string firstStatementText, string? firstCompiled, bool tokensInRest)
    {
        var sb = new StringBuilder(targetChars + 4096);
        sb.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>");
        int n = 0;
        while (sb.Length < targetChars)
        {
            string text = n == 0 ? firstStatementText
                : tokensInRest ? "(@1 nvarchar(50))SELECT [c] FROM [dbo].[t] WHERE [name]=@1" : "SELECT [c] FROM [dbo].[t] WHERE [name] = 1";
            string? compiled = n == 0 ? firstCompiled : tokensInRest ? "N'plain-value'" : null;
            sb.Append("<StmtSimple StatementText=\"").Append(text).Append("\" StatementId=\"").Append(++n).Append("\"><QueryPlan>");
            for (int i = 0; i < 20; i++)
                sb.Append(FillerRelOp);
            if (compiled is not null)
                sb.Append("<ParameterList><ColumnReference Column=\"@1\" ParameterCompiledValue=\"").Append(compiled).Append("\"/></ParameterList>");
            sb.Append("</QueryPlan></StmtSimple>\n");
        }
        sb.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        return sb.ToString();
    }

    private (long Ms, long Bytes, string? Result) Measure(string label, string xml, int max)
    {
        // the judge matches the earlier walk measurements (a plain substring test), so the numbers compare
        Func<string, bool> substring = s => s.Contains("CANARY", StringComparison.OrdinalIgnoreCase);
        GC.Collect();
        long before = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        string? result = SensitiveStatements.XmlCore(xml, substring, () => false, P, null, max);
        sw.Stop();
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        string line = string.Create(CultureInfo.InvariantCulture,
            $"SSF-L2B-MEASURE {label}: {xml.Length / 1_000_000.0:F1} M chars, {sw.ElapsedMilliseconds} ms, {bytes / 1_048_576.0:F1} MB allocated, {xml.Length / 1_000_000.0 / Math.Max(sw.Elapsed.TotalSeconds, 0.0001):F0} M chars/s");
        _output.WriteLine(line);
        TestContext.Current.SendDiagnosticMessage(line);
        return (sw.ElapsedMilliseconds, bytes, result);
    }

    [Fact]
    public void Measure_PassOne_27MbPlan_NoHit_AndWithAThousandAutoParameterizedStatements()
    {
        string plain = BigPlan(27_000_000, "SELECT [c] FROM [dbo].[t] WHERE [name] = 1", null, tokensInRest: false);
        var noToken = Measure("27 MB no hit, no token", plain, int.MaxValue);
        Assert.Same(plain, noToken.Result);

        // the same shape with a token and a ParameterList in every statement
        string tokens = BigPlan(27_000_000, "(@1 nvarchar(50))SELECT [c] FROM [dbo].[t] WHERE [name]=@1", "N'plain-value'", tokensInRest: true);
        var withTokens = Measure("27 MB no hit, token in every statement", tokens, int.MaxValue);
        Assert.Same(tokens, withTokens.Result);

        // probe cost per 1,000 statements: a plan of just 1,000 statements, with and without the tokens
        string thousandPlain = BigPlan(1000 * (FillerRelOp.Length * 20 + 130), "SELECT 1", null, tokensInRest: false);
        string thousandTokens = BigPlan(1000 * (FillerRelOp.Length * 20 + 230), "(@1 nvarchar(50))SELECT 1 WHERE [x]=@1", "N'plain-value'", tokensInRest: true);
        int statements = Regex.Matches(thousandTokens, "<StmtSimple ").Count;
        for (int warm = 0; warm < 2; warm++)
        {
            Measure("1,000 statements, no token", thousandPlain, int.MaxValue);
            Measure($"{statements} statements, a token each", thousandTokens, int.MaxValue);
        }

        Assert.True(noToken.Ms < 5_000 && withTokens.Ms < 5_000);
    }

    [Fact]
    public void Measure_20MbPlan_WithACut_ThreeShapes_StayUnderTheCeilings()
    {
        const int Max = 512_000;
        string hitFirst = BigPlan(20_000_000, "UPDATE [dbo].[t] SET [c] = 'CANARY'", null, tokensInRest: false);
        string noHit = BigPlan(20_000_000, "SELECT [c] FROM [dbo].[t] WHERE [name] = 1", null, tokensInRest: false);
        string probeFirst = BigPlan(20_000_000, "(@1 nvarchar(50))UPDATE [dbo].[t] SET [c]=@1", "N'CANARY'", tokensInRest: true);

        foreach (var (label, xml) in new[] { ("20 MB hit in the first statement", hitFirst), ("20 MB no hit", noHit), ("20 MB probe hit in the first statement", probeFirst) })
        {
            for (int pass = 0; pass < 2; pass++)
            {
                var m = Measure(label, xml, Max);
                if (pass == 1)
                {
                    Assert.True(m.Ms < 500, $"{label}: {m.Ms} ms");
                    Assert.True(m.Bytes < 32L * 1024 * 1024, $"{label}: {m.Bytes} bytes");
                }
            }
        }
        Assert.Contains(PText, Measure("check", probeFirst, Max).Result);
    }
}
