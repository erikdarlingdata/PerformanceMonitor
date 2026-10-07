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
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348: the XML walk of the statement filter (<c>SensitiveStatements.XmlCore</c>). The judge here is a plain
/// substring test on CANARY, so these pin the mechanics (what is judged, what is exempt, what a withheld
/// statement writes, the failure rules and the budget), not the pattern.
/// </summary>
public class SensitiveStatementXmlTests
{
    private const string P = "<<P>>";

    /// <summary>The placeholder as it sits in serialized text (it holds angle brackets, so it is escaped).</summary>
    private const string PText = "&lt;&lt;P&gt;&gt;";
    private static readonly XNamespace Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static bool Canary(string s) => s.Contains("CANARY", StringComparison.OrdinalIgnoreCase);

    private static string Fixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "StatementScrub", name));

    private static string? Run(string? xml, Func<string, bool>? named = null, Func<bool>? spent = null,
        Action<TimeSpan>? charge = null, int max = int.MaxValue) =>
        SensitiveStatements.XmlCore(xml, named ?? Canary, spent ?? (() => false), P, charge, max);

    /// <summary>Every element and attribute name, in document order: the shape a rewrite must keep.</summary>
    private static List<string> Shape(string xml)
    {
        var shape = new List<string>();
        foreach (var e in XDocument.Parse(xml).Descendants())
        {
            shape.Add(e.Name.LocalName + "[" + string.Join(",", e.Attributes().Select(a => a.Name.LocalName)) + "]");
        }
        return shape;
    }

    private static List<XElement> Stmts(string xml) => XDocument.Parse(xml).Descendants(Ns + "StmtSimple").ToList();

    private static string?[] Values(XElement stmt, string attribute) =>
        stmt.Descendants().Select(e => (string?)e.Attribute(attribute)).Where(v => v is not null).ToArray();

    // ── the RED: a named statement withholds its own values, and only its own ──

    [Fact]
    public void NamedStatementWithholdsItsParametersAndLiterals_AndTheNextStatementIsJudgedOnItsOwn()
    {
        string xml = Fixture("two_statements.xml");

        string? result = Run(xml);

        Assert.NotNull(result);
        Assert.NotSame(xml, result);
        Assert.Equal(Shape(xml), Shape(result!));

        var s = Stmts(result!);
        Assert.Equal(2, s.Count);

        // statement 1 (StatementText holds CANARY): the whole H2 list is the caller's placeholder
        Assert.All(Values(s[0], "ParameterCompiledValue"), v => Assert.Equal(P, v));
        Assert.All(Values(s[0], "ParameterRuntimeValue"), v => Assert.Equal(P, v));
        Assert.All(Values(s[0], "ScalarString"), v => Assert.Equal(P, v));
        Assert.All(Values(s[0], "ConstValue"), v => Assert.Equal(P, v));
        Assert.Equal(P, (string?)s[0].Attribute("ParameterizedText"));
        Assert.Equal(P, (string?)s[0].Attribute("StatementText"));
        // ... including a numeric parameter value, which names nothing on its own
        Assert.DoesNotContain("(7)", result);

        // statement 2: ParameterCompiledValue holding CANARY is exempt and KEPT; ScalarString and ConstValue holding
        // CANARY are judged on their own and become the placeholder; the plain runtime value stays
        Assert.Equal("N'CANARY-keep'", Values(s[1], "ParameterCompiledValue").Single());
        Assert.Equal("N'plain-runtime'", Values(s[1], "ParameterRuntimeValue").Single());
        Assert.Contains("CANARY-keep", result);
        Assert.DoesNotContain("CANARY-two", result);
        Assert.Contains(Values(s[1], "ScalarString"), v => v == P);
        Assert.Equal(P, Values(s[1], "ConstValue").Single());
        Assert.Equal("SELECT name FROM dbo.people WHERE name = @n", (string?)s[1].Attribute("StatementText"));

        // the statement-1 text itself never survives, and the document still parses (Parse above)
        Assert.DoesNotContain("CANARY-one", result);
        Assert.DoesNotContain("CANARY-compiled", result);
        Assert.DoesNotContain("CANARY-runtime", result);
    }

    [Fact]
    public void ThePlaceholderIsTheParameter_NotAConstantOfTheFile()
    {
        string? result = SensitiveStatements.XmlCore(Fixture("two_statements.xml"), Canary, () => false, "[[other]]");

        Assert.Contains("[[other]]", result);
        Assert.DoesNotContain(P, result);
    }

    [Fact]
    public void NoHit_ReturnsTheSameInstance()
    {
        string xml = Fixture("two_statements.xml").Replace("CANARY", "plain", StringComparison.Ordinal);

        Assert.Same(xml, Run(xml));
        Assert.Null(Run(null));
        Assert.Same(string.Empty, Run(string.Empty));
    }

    [Fact]
    public void ParameterValuesAreExemptOutsideAScope_EvenWhenTheyHoldTheName()
    {
        const string xml = "<a><ColumnReference ParameterCompiledValue=\"N'CANARY'\" ParameterRuntimeValue=\"N'CANARY'\"/></a>";

        Assert.Same(xml, Run(xml));
    }

    // ── scope opening and the L-J rule ──

    [Fact]
    public void AParameterizedTextAttributeHoldingTheName_OpensTheScope_AndInsideItAnyQuotedValueIsWithheld()
    {
        const string xml =
            "<StmtSimple StatementText=\"UPDATE t SET c = @1\" ParameterizedText=\"(@1 int)UPDATE t SET c = @1 -- CANARY\" StatementSubTreeCost=\"0.5\">" +
            "<QueryPlan><Warnings><PlanAffectingConvert Expression=\"x = 'lit-ssf'\" Note=\"n\"/></Warnings>" +
            "<ParameterList><ColumnReference Column=\"@1\" ParameterCompiledValue=\"(7)\"/></ParameterList></QueryPlan></StmtSimple>" +
            "<StmtSimple StatementText=\"SELECT 1\"><QueryPlan><Warnings><PlanAffectingConvert Expression=\"x = 'lit-ssf'\"/></Warnings>" +
            "<ParameterList><ColumnReference Column=\"@1\" ParameterCompiledValue=\"(8)\"/></ParameterList></QueryPlan></StmtSimple>";

        string result = Run("<r>" + xml + "</r>")!;
        var stmts = XDocument.Parse(result).Descendants("StmtSimple").ToList();

        Assert.Equal(P, (string?)stmts[0].Attribute("ParameterizedText"));
        Assert.Equal("UPDATE t SET c = @1", (string?)stmts[0].Attribute("StatementText"));
        Assert.Equal("0.5", (string?)stmts[0].Attribute("StatementSubTreeCost"));
        var convert0 = stmts[0].Descendants("PlanAffectingConvert").Single();
        Assert.Equal(P, (string?)convert0.Attribute("Expression"));
        Assert.Equal("n", (string?)convert0.Attribute("Note"));
        Assert.Equal(P, (string?)stmts[0].Descendants("ColumnReference").Single().Attribute("ParameterCompiledValue"));
        Assert.Equal("@1", (string?)stmts[0].Descendants("ColumnReference").Single().Attribute("Column"));

        // the same attribute outside any scope is kept
        Assert.Equal("x = 'lit-ssf'", (string?)stmts[1].Descendants("PlanAffectingConvert").Single().Attribute("Expression"));
        Assert.Equal("(8)", (string?)stmts[1].Descendants("ColumnReference").Single().Attribute("ParameterCompiledValue"));
    }

    [Fact]
    public void TheScopeEndsAtTheStatementsEndTag_NestedStatementsAreInsideIt()
    {
        const string xml =
            "<r><StmtCond StatementText=\"IF CANARY\"><StmtSimple StatementText=\"SELECT 1\" Lit=\"'a'\"/><Cond Lit=\"'b'\"/></StmtCond>" +
            "<Other Lit=\"'c'\" ParameterRuntimeValue=\"(1)\"/></r>";

        string result = Run(xml)!;
        var doc = XDocument.Parse(result);

        Assert.Equal(P, (string?)doc.Descendants("StmtSimple").Single().Attribute("Lit"));
        Assert.Equal(P, (string?)doc.Descendants("Cond").Single().Attribute("Lit"));
        Assert.Equal("'c'", (string?)doc.Descendants("Other").Single().Attribute("Lit"));
        Assert.Equal("(1)", (string?)doc.Descendants("Other").Single().Attribute("ParameterRuntimeValue"));
    }

    [Fact]
    public void AnEmptyStatementElementOpensTheScopeOnlyForItsOwnAttributes()
    {
        const string xml = "<r><StmtUseDb StatementText=\"USE CANARY\" Lit=\"'a'\" Cost=\"1\"/><Other Lit=\"'c'\"/></r>";

        var doc = XDocument.Parse(Run(xml)!);

        Assert.Equal(P, (string?)doc.Descendants("StmtUseDb").Single().Attribute("Lit"));
        Assert.Equal("1", (string?)doc.Descendants("StmtUseDb").Single().Attribute("Cost"));
        Assert.Equal("'c'", (string?)doc.Descendants("Other").Single().Attribute("Lit"));
    }

    [Fact]
    public void InsideAScope_ElementFormsOfTheListAndQuotedTextAreWithheld()
    {
        const string xml =
            "<r><StmtSimple StatementText=\"x CANARY\"><ParameterizedText>(@1 int)select 1</ParameterizedText>" +
            "<Note>it's here</Note><Plain>7 rows</Plain></StmtSimple><After>it's fine</After></r>";

        var doc = XDocument.Parse(Run(xml)!);

        Assert.Equal(P, doc.Descendants("ParameterizedText").Single().Value);
        Assert.Equal(P, doc.Descendants("Note").Single().Value);
        Assert.Equal("7 rows", doc.Descendants("Plain").Single().Value);
        Assert.Equal("it's fine", doc.Descendants("After").Single().Value);
    }

    [Fact]
    public void InsideAScope_AValueHoldingABinaryLiteralIsWithheld_ExceptTheHashAndHandleAttributes()
    {
        const string xml =
            "<r><StmtSimple StatementText=\"x CANARY\" QueryHash=\"0x0A1B2C3D4E5F6071\" QueryPlanHash=\"0x0A1B2C3D4E5F6072\" " +
            "StatementSqlHandle=\"0x09000000AB\" ParameterizedPlanHandle=\"0x06000100AB\">" +
            "<PlanAffectingConvert ConvertIssue=\"Seek Plan\" Expression=\"CONVERT_IMPLICIT(varbinary(64),[t].[h],0)=0x0200ABCD\"/>" +
            "<RemoteQuery>SELECT 1 WHERE h = 0xDEADBEEF</RemoteQuery><Plain Note=\"index 0x is empty\" Cost=\"7\"/>" +
            "<PlanHandle>0x05000100CD</PlanHandle></StmtSimple>" +
            "<After><PlanAffectingConvert Expression=\"CONVERT_IMPLICIT(varbinary(64),[t].[h],0)=0x0200ABCD\"/><RemoteQuery>h = 0xDEADBEEF</RemoteQuery></After></r>";

        var doc = XDocument.Parse(Run(xml)!);
        var stmt = doc.Descendants("StmtSimple").Single();

        Assert.Equal(P, (string?)doc.Descendants("PlanAffectingConvert").First().Attribute("Expression"));
        Assert.Equal("Seek Plan", (string?)doc.Descendants("PlanAffectingConvert").First().Attribute("ConvertIssue"));
        Assert.Equal(P, doc.Descendants("RemoteQuery").First().Value);
        Assert.Equal("index 0x is empty", (string?)doc.Descendants("Plain").Single().Attribute("Note"));
        Assert.Equal("0x0A1B2C3D4E5F6071", (string?)stmt.Attribute("QueryHash"));
        Assert.Equal("0x0A1B2C3D4E5F6072", (string?)stmt.Attribute("QueryPlanHash"));
        Assert.Equal("0x09000000AB", (string?)stmt.Attribute("StatementSqlHandle"));
        Assert.Equal("0x06000100AB", (string?)stmt.Attribute("ParameterizedPlanHandle"));
        Assert.Equal("0x05000100CD", doc.Descendants("PlanHandle").Single().Value);
        // outside a withheld scope the value is kept (the judge does not name it)
        Assert.Equal("CONVERT_IMPLICIT(varbinary(64),[t].[h],0)=0x0200ABCD",
            (string?)doc.Descendants("PlanAffectingConvert").Last().Attribute("Expression"));
        Assert.Equal("h = 0xDEADBEEF", doc.Descendants("RemoteQuery").Last().Value);
    }

    [Fact]
    public void AnElementThatIsNotNamedStmtButCarriesAStatementText_OpensTheScope()
    {
        // The showplan schema names every statement element Stmt*; an element of another name that carries the
        // statement attributes is read the same way, in both passes (the ordinals stay in step).
        const string xml =
            "<r><Wrapper StatementText=\"x CANARY\" Lit=\"'a'\"><Inner Lit=\"'b'\"/><ConstValue>7</ConstValue></Wrapper>" +
            "<StmtSimple StatementText=\"SELECT 1\" Lit=\"'c'\"/><Other Lit=\"'d'\"/></r>";

        var doc = XDocument.Parse(Run(xml)!);

        Assert.Equal(P, (string?)doc.Descendants("Wrapper").Single().Attribute("StatementText"));
        Assert.Equal(P, (string?)doc.Descendants("Wrapper").Single().Attribute("Lit"));
        Assert.Equal(P, (string?)doc.Descendants("Inner").Single().Attribute("Lit"));
        Assert.Equal(P, doc.Descendants("ConstValue").Single().Value);
        Assert.Equal("'c'", (string?)doc.Descendants("StmtSimple").Single().Attribute("Lit"));
        Assert.Equal("'d'", (string?)doc.Descendants("Other").Single().Attribute("Lit"));
    }

    // ── other XML: reports, graphs, events ──

    [Fact]
    public void ADecodedLineBreakInsideAnAttribute_IsJudgedDecoded()
    {
        const string xml = "<StmtSimple StatementText=\"CREATE&#xD;&#xA;LOGIN x\" Cost=\"1\"/>";

        string? result = Run(xml, s => s.Contains("CREATE\r\nLOGIN", StringComparison.Ordinal));

        Assert.Contains("StatementText=\"" + PText + "\"", result);
        Assert.Contains("Cost=\"1\"", result);
    }

    [Fact]
    public void AlineBreakCharacterInAValueSurvivesTheRewriteAsACharacterReference()
    {
        const string xml = "<r a=\"x&#xD;&#xA;y\" b=\"CANARY\"/>";

        string result = Run(xml)!;

        Assert.Equal("x\r\ny", (string?)XDocument.Parse(result).Root!.Attribute("a"));
        Assert.Equal(P, (string?)XDocument.Parse(result).Root!.Attribute("b"));
    }

    [Fact]
    public void ABlockedProcessReport_WithholdsOnlyTheNamedInputBuffer()
    {
        const string xml =
            "<blocked-process-report monitorLoop=\"1\"><blocked-process><process id=\"p1\" status=\"suspended\">" +
            "<executionStack><frame line=\"1\" stmtstart=\"0\" sqlhandle=\"0x01\">UPDATE t SET c = 1</frame></executionStack>" +
            "<inputbuf>UPDATE t SET c = 'CANARY'</inputbuf></process></blocked-process>" +
            "<blocking-process><process id=\"p2\"><inputbuf>SELECT 1</inputbuf></process></blocking-process></blocked-process-report>";

        var doc = XDocument.Parse(Run(xml)!);

        Assert.Equal(P, doc.Descendants("inputbuf").First().Value);
        Assert.Equal("SELECT 1", doc.Descendants("inputbuf").Last().Value);
        Assert.Equal("UPDATE t SET c = 1", doc.Descendants("frame").Single().Value);
        Assert.Equal("1", (string?)doc.Descendants("blocked-process-report").Single().Attribute("monitorLoop"));
    }

    [Fact]
    public void ADeadlockGraph_WithholdsTheNamedInputBufferAndFrameText()
    {
        const string xml =
            "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list>" +
            "<process id=\"process1\" taskpriority=\"0\"><executionStack><frame procname=\"adhoc\" line=\"1\">DECLARE @c nvarchar(10) = N'CANARY'</frame></executionStack>" +
            "<inputbuf>EXEC s 'CANARY'</inputbuf></process>" +
            "<process id=\"process2\"><executionStack><frame procname=\"adhoc\" line=\"1\">SELECT 2</frame></executionStack><inputbuf>SELECT 2</inputbuf></process>" +
            "</process-list></deadlock>";

        var doc = XDocument.Parse(Run(xml)!);
        var frames = doc.Descendants("frame").ToList();
        var bufs = doc.Descendants("inputbuf").ToList();

        Assert.Equal(P, frames[0].Value);
        Assert.Equal("SELECT 2", frames[1].Value);
        Assert.Equal(P, bufs[0].Value);
        Assert.Equal("SELECT 2", bufs[1].Value);
        Assert.Equal("adhoc", (string?)frames[0].Attribute("procname"));
    }

    [Fact]
    public void ASystemHealthSqlTextAction_IsWithheldWhenNamed()
    {
        const string xml =
            "<event name=\"error_reported\" package=\"sqlserver\"><data name=\"error_number\"><value>102</value></data>" +
            "<action name=\"sql_text\" package=\"sqlserver\"><type name=\"unicode_string\" package=\"package0\"/><value>SELECT 'CANARY'</value></action>" +
            "<action name=\"database_id\" package=\"sqlserver\"><type name=\"uint16\" package=\"package0\"/><value>5</value></action></event>";

        var doc = XDocument.Parse(Run(xml)!);

        Assert.Equal(P, doc.Descendants("action").First().Element("value")!.Value);
        Assert.Equal("5", doc.Descendants("action").Last().Element("value")!.Value);
        Assert.Equal("102", doc.Descendants("data").Single().Element("value")!.Value);
    }

    [Fact]
    public void ACdataSection_IsJudgedAndKeptAsCdata()
    {
        const string xml = "<r><a><![CDATA[plain <b>]]></a><c><![CDATA[CANARY <b>]]></c></r>";

        string result = Run(xml)!;

        Assert.Contains("<a><![CDATA[plain <b>]]></a>", result);
        Assert.Contains("<c><![CDATA[" + P + "]]></c>", result);
    }

    // ── preservation in the rewrite ──

    [Fact]
    public void TheRewriteKeepsTheDeclarationOnlyWhenOnePresent()
    {
        string withDecl = "<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<r a=\"CANARY\"/>";
        string without = "<r a=\"CANARY\"/>";

        string r1 = Run(withDecl)!;
        string r2 = Run(without)!;

        Assert.StartsWith("<?xml version=\"1.0\" encoding=\"utf-16\"?>", r1);
        Assert.DoesNotContain("<?xml", r2);
    }

    [Fact]
    public void TheRewriteKeepsPrefixesNamespacesEmptyVersusFullElementsCommentsAndInstructions()
    {
        const string xml =
            "<root xmlns=\"urn:one\" xmlns:p=\"urn:two\" CANARY=\"x\" v=\"CANARY\"><p:item p:k=\"1\"/><full></full><!--a comment--><?pi some data?><p:wrap><p:inner/></p:wrap></root>";

        string result = Run(xml)!;
        var doc = XDocument.Parse(result);

        Assert.Equal("urn:one", doc.Root!.Name.NamespaceName);
        Assert.Equal("urn:two", doc.Root.Element(XNamespace.Get("urn:two") + "item")!.Name.NamespaceName);
        Assert.Contains("xmlns:p=\"urn:two\"", result);
        Assert.Contains("<p:item p:k=\"1\" />", result);
        Assert.Contains("<full></full>", result);
        Assert.Contains("<!--a comment-->", result);
        Assert.Contains("<?pi some data?>", result);
        Assert.Contains("<p:inner />", result);
        Assert.Equal(P, (string?)doc.Root.Attribute("v"));
        Assert.Equal("x", (string?)doc.Root.Attribute("CANARY"));
    }

    [Fact]
    public void TheRewriteKeepsTheWhitespaceBetweenElements()
    {
        const string xml = "<r>\r\n  <a x=\"CANARY\">\r\n    <b/>\r\n  </a>\r\n</r>";

        string result = Run(xml)!;

        // the reader normalizes a literal CR LF between elements to LF (XML 1.0 section 2.11); that is the only
        // change besides the withheld value and the space the writer puts before an empty element's slash
        Assert.Equal("<r>\n  <a x=\"" + PText + "\">\n    <b />\n  </a>\n</r>", result);
    }

    [Theory]
    [InlineData("spill_plan.sqlplan")]
    [InlineData("key_lookup_plan.sqlplan")]
    [InlineData("udf_plan.sqlplan")]
    public void ARealPlan_ComesBackEqualToItself_ExceptTheWithheldValues(string file)
    {
        string xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", file));
        const string hitValue = "false";
        string? result = Run(xml, s => s == hitValue);
        Assert.NotSame(xml, result);   // every plan holds a false flag

        var expected = XDocument.Parse(xml);
        foreach (var a in expected.Descendants().Attributes().Where(a => a.Value == hitValue).ToList())
            a.Value = P;
        var actual = XDocument.Parse(result!);

        Assert.True(XNode.DeepEquals(expected, actual), "only the withheld values may differ");
        // multi-line statement text survives as the same characters (CR LF inside an attribute is a character reference)
        Assert.Equal(
            expected.Descendants().Select(e => (string?)e.Attribute("StatementText")).Where(v => v is not null),
            actual.Descendants().Select(e => (string?)e.Attribute("StatementText")).Where(v => v is not null));
    }

    // ── failure rules and the budget ──

    [Fact]
    public void CutXml_IsJudgedWhole_DecodedBeforeJudging()
    {
        const string cutNamed = "<r><a>CREATE&#xD;&#xA;LOGIN</a><b";
        const string cutClean = "<r><a>plain</a><b";

        Assert.Equal(P, Run(cutNamed, s => s.Contains("CREATE\r\nLOGIN", StringComparison.Ordinal)));
        Assert.Same(cutClean, Run(cutClean));
        Assert.Equal(P, Run("<r><a>CANARY</a><b", Canary));
    }

    [Fact]
    public void ADtd_IsJudgedWhole()
    {
        const string named = "<!DOCTYPE r [<!ENTITY e \"x\">]><r a=\"CANARY\">&e;</r>";
        const string clean = "<!DOCTYPE r [<!ENTITY e \"x\">]><r>&e;</r>";

        Assert.Equal(P, Run(named));
        Assert.Same(clean, Run(clean));
    }

    [Fact]
    public void ASpentBudgetAfterTheFirstNode_GivesTheWholePlaceholder()
    {
        string xml = Fixture("two_statements.xml").Replace("CANARY", "plain", StringComparison.Ordinal);
        int asked = 0;

        string? pass1 = Run(xml, spent: () => ++asked > 1);

        Assert.Equal(P, pass1);

        // spent only once pass 2 is running: still the whole placeholder, never a half-written document
        string named = Fixture("two_statements.xml");
        int asks = 0;
        int asksAtFirstHit = 0;
        Run(named, v =>
        {
            bool hit = Canary(v);
            if (hit && asksAtFirstHit == 0)
                asksAtFirstHit = asks;
            return hit;
        }, () => { asks++; return false; });
        Assert.True(asksAtFirstHit > 0 && asks > asksAtFirstHit + 10, "pass 2 must ask the budget after pass 1 stopped");
        int calls = 0;

        string? pass2 = Run(named, spent: () => ++calls > asksAtFirstHit + 10);

        Assert.Equal(P, pass2);
    }

    [Fact]
    public void AJudgeThatThrows_FailsClosedToTheWholePlaceholder()
    {
        string? result = Run("<r a=\"v\"/>", _ => throw new InvalidOperationException("boom"));

        Assert.Equal(P, result);
    }

    [Fact]
    public void TheParseTimeIsChargedToTheCaller()
    {
        var charged = new List<TimeSpan>();

        Run(Fixture("two_statements.xml"), charge: charged.Add);

        Assert.NotEmpty(charged);
        Assert.All(charged, t => Assert.True(t >= TimeSpan.Zero));
    }

    // ── maxOutputChars (P3, M5, L-I) ──

    private static string PrettyPlan(int statements, int bodyLinesPerStatement, int canaryStatement)
    {
        var sb = new StringBuilder();
        sb.Append("<ShowPlanXML>\r\n");
        for (int i = 0; i < statements; i++)
        {
            string text = i == canaryStatement ? "SELECT CANARY" : "SELECT " + i;
            sb.Append("  <StmtSimple StatementText=\"").Append(text).Append("\" StatementId=\"").Append(i).Append("\">\r\n");
            for (int l = 0; l < bodyLinesPerStatement; l++)
                sb.Append("    <RelOp NodeId=\"").Append(l).Append("\" PhysicalOp=\"Scan\" ParameterCompiledValue=\"N'x'\"/>\r\n");
            sb.Append("  </StmtSimple>\r\n");
        }
        sb.Append("</ShowPlanXML>");
        return sb.ToString();
    }

    [Fact]
    public void WithAMaxOutput_ANamedStatementTextThatStarts2KbBeforeTheCutIsAHit()
    {
        // each statement is ~2 KB of body lines; the named one starts well before the cut at ~8 KB
        string xml = PrettyPlan(statements: 20, bodyLinesPerStatement: 20, canaryStatement: 3);
        int named = xml.IndexOf("SELECT CANARY", StringComparison.Ordinal);
        int cutAt = named + 2048;
        Assert.True(named > 0 && cutAt < xml.Length - 4096, "fixture shape");

        string? result = Run(xml, max: cutAt);

        Assert.NotSame(xml, result);
        Assert.DoesNotContain("CANARY", result);
        Assert.Contains("StatementText=\"" + PText + "\"", result);
        // the rewrite goes at least to the cut, and stops short of the whole document
        Assert.True(result!.Length >= cutAt, "result must reach the cut: " + result.Length + " vs " + cutAt);
        Assert.True(result.Length < xml.Length, "work is bounded by the cut");
    }

    [Fact]
    public void WithAMaxOutput_AValueWhoseNodeStartsPastTheCutIsNotJudged()
    {
        string xml = PrettyPlan(statements: 20, bodyLinesPerStatement: 20, canaryStatement: 18);
        int named = xml.IndexOf("SELECT CANARY", StringComparison.Ordinal);

        Assert.Same(xml, Run(xml, max: named - 4096));
        Assert.NotSame(xml, Run(xml, max: named + 4096));
        Assert.NotSame(xml, Run(xml));
    }

    [Fact]
    public void WithAMaxOutput_ALongNamedAttributeThatStartsBeforeTheCutIsStillJudgedWhole()
    {
        string longValue = new string('x', 100_000) + " CANARY";
        string xml = "<r>\r\n<a v=\"" + longValue + "\"/>\r\n<b/></r>";

        string? result = Run(xml, max: 200);

        Assert.NotSame(xml, result);
        Assert.Contains(PText, result);
        Assert.DoesNotContain("CANARY", result);
    }

    [Fact]
    public void WithAMaxOutput_ACleanDocumentIsTheSameInstance()
    {
        string xml = PrettyPlan(statements: 20, bodyLinesPerStatement: 20, canaryStatement: -1);

        Assert.Same(xml, Run(xml, max: 1000));
    }

    // ── measurements (assert correctness only; the timings are printed for the part file) ──

    [Fact]
    public void Measurement_PlanPaddedTo512Kb()
    {
        string plan = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", "serially-parallel.sqlplan"));
        int start = plan.IndexOf("<StmtSimple", StringComparison.Ordinal);
        int end = plan.IndexOf("</StmtSimple>", start, StringComparison.Ordinal) + "</StmtSimple>".Length;
        Assert.True(start > 0 && end > start);
        string block = plan.Substring(start, end - start);
        Assert.DoesNotContain("<StmtSimple", block.Substring(1));

        var padded = new StringBuilder(plan, 600_000);
        int insertAt = padded.ToString().IndexOf("</Statements>", StringComparison.Ordinal);
        Assert.True(insertAt > 0);
        var clones = new StringBuilder();
        while (plan.Length + clones.Length < 512 * 1024)
            clones.Append(block);
        padded.Insert(insertAt, clones.ToString());
        string big = padded.ToString();

        // a hit in statement 1 only: the canary goes in the first statement's text
        string firstTag = "StatementText=\"";
        int at = big.IndexOf(firstTag, start, StringComparison.Ordinal) + firstTag.Length;
        string hit = big.Insert(at, "CANARY ");

        Assert.Same(big, Run(big));
        double best1 = double.MaxValue;
        for (int i = 0; i < 7; i++)
        {
            var sw = Stopwatch.StartNew();
            Run(big);
            best1 = Math.Min(best1, sw.Elapsed.TotalSeconds);
        }

        string? rewritten = Run(hit);
        Assert.NotSame(hit, rewritten);
        Assert.DoesNotContain("CANARY", rewritten);
        double best2 = double.MaxValue;
        for (int i = 0; i < 7; i++)
        {
            var sw = Stopwatch.StartNew();
            Run(hit);
            best2 = Math.Min(best2, sw.Elapsed.TotalMilliseconds);
        }

        string line = string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"SSF-L2-MEASURE chars={big.Length} pass1_no_hit_ms={best1 * 1000:F1} pass1_Mchars_per_s={big.Length / 1e6 / best1:F1} pass2_hit_ms={best2:F1} out_chars={rewritten!.Length}");
        TestContext.Current.SendDiagnosticMessage(line);
        Assert.True(best1 > 0);
    }

    // ── comments and processing instructions are judged like a text node ──

    [Fact]
    public void ANamedComment_IsWithheld_AndAPlainOneIsByteIdentical()
    {
        const string plain = "<r><!--a plain comment--><a v=\"1\"/></r>";
        Assert.Same(plain, Run(plain));

        string? named = Run("<r><!--CANARY in a comment--><a v=\"1\"/></r>");

        Assert.NotNull(named);
        Assert.DoesNotContain("CANARY", named);
        Assert.Contains("<!--" + P + "-->", named);
        Assert.Contains("<a v=\"1\" />", named);
    }

    [Fact]
    public void ANamedProcessingInstruction_IsWithheld_AndAPlainOneIsByteIdentical()
    {
        const string plain = "<r><?pi some data?><a v=\"1\"/></r>";
        Assert.Same(plain, Run(plain));

        string? named = Run("<r><?pi CANARY data?><a v=\"1\"/></r>");

        Assert.NotNull(named);
        Assert.DoesNotContain("CANARY", named);
        Assert.Contains("<?pi " + P + "?>", named);
    }

    [Fact]
    public void ANamedComment_IsAHitEvenWhenNothingElseIs_AfterTheFirstElement()
    {
        string? named = Run("<r><a v=\"1\"/><b/><!--CANARY--></r>");

        Assert.NotNull(named);
        Assert.DoesNotContain("CANARY", named);
    }
}
