/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5477: a document the linear-time pre-check clears as raw text skips the walk (two parses and a judge call per
/// value). The clear must be exact, so every document here is run through the walk itself
/// (<c>XmlCore</c> with the production judge, which never takes the shortcut) and the answers must match; the
/// documents that must NOT take the shortcut are the ones where raw text and decoded values differ.
/// </summary>
public class SensitiveStatementRawCleanTests
{
    private const string Secret = "CREATE LOGIN [canary_raw] WITH PASSWORD = N'S3cret-canary-raw'";
    private const string Plain = "SELECT canary_plain_raw FROM dbo.t WHERE c = 1";

    private static bool Named(string value) =>
        SensitiveStatements.Names(value)
        || (value.Contains('&', StringComparison.Ordinal) && SensitiveStatements.Names(WebUtility.HtmlDecode(value)));

    /// <summary>What the walk answers, with no shortcut in front of it.</summary>
    private static string? Walk(string xml) =>
        SensitiveStatements.XmlCore(xml, Named, static () => false, SensitiveStatements.PlaceholderText);

    private static string? Filtered(string xml, out bool tookTheShortcut)
    {
        var before = SensitiveStatements.RawCleanClears;
        var result = SensitiveStatements.Xml(xml, new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget));
        tookTheShortcut = SensitiveStatements.RawCleanClears != before;
        return result;
    }

    [Fact]
    public void ACleanDocument_IsClearedWithoutAWalk_AndComesBackAsTheSameInstance()
    {
        var xml = "<r a=\"1\"><n>" + Plain + "</n><n b=\"two\">more text</n></r>";

        var result = Filtered(xml, out var tookTheShortcut);

        Assert.True(tookTheShortcut);
        Assert.Same(xml, result);
        Assert.Same(xml, Walk(xml));
    }

    [Fact]
    public void TheClear_IsChargedToTheBudget()
    {
        var budget = new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget);

        SensitiveStatements.Xml("<r>" + new string('x', 400_000) + "</r>", budget);

        Assert.True(budget.Elapsed > TimeSpan.Zero);
    }

    public static TheoryData<string> DocumentsThatMustBeWalked() => new()
    {
        // A named value: the walk replaces it.
        "<r><n>" + Secret + "</n><n>" + Plain + "</n></r>",
        // A named value in an attribute.
        "<r a=\"" + Secret + "\"><n>" + Plain + "</n></r>",
        // A named value in a CDATA section.
        "<r><![CDATA[" + Secret + "]]></r>",
        // The secret spelled with a character reference: only decoding shows it, so it is no raw-text clear.
        "<r><n>CREATE LOGIN x WITH PASSWORD&#32;= N'S3cret'</n></r>",
        // Entity-encoded quote around the literal.
        "<r><n>password = &apos;hunter2&apos;</n></r>",
        // A line comment in an attribute value: the reader turns the line break into a space, so the comment runs
        // on to the literal in the value and not in the raw text.
        "<r v=\"password -- note\nfoo 'x'\"><n>" + Plain + "</n></r>",
        "<r v=\"password -- note\tfoo 'x'\"/>",
        // An XML comment holds a double hyphen: no shortcut, and the comment is judged by the walk.
        "<r><!-- password = 'x' --><n>" + Plain + "</n></r>",
        // An auto-parameter token: the walk judges the statement with its parameter value put back.
        "<ShowPlanXML><StmtSimple StatementText=\"update t set pwd = @1\"><QueryPlan><ParameterList>"
            + "<ColumnReference Column=\"@1\" ParameterCompiledValue=\"N'hunter2'\" /></ParameterList></QueryPlan></StmtSimple></ShowPlanXML>",
        // An element-form ParameterizedText.
        "<r><ParameterizedText>select 1</ParameterizedText></r>",
        // Not well formed, with and without a secret.
        "<r><n>" + Plain,
        "<r><n>" + Secret,
        "just some text, not xml",
        "pwd = 'x' and not xml",
    };

    [Theory]
    [MemberData(nameof(DocumentsThatMustBeWalked))]
    public void EveryDocument_GetsTheWalksAnswer(string xml)
    {
        var expected = Walk(xml);

        var actual = Filtered(xml, out _);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("<r><n>CREATE LOGIN x WITH PASSWORD&#32;= N'S3cret'</n></r>")]
    [InlineData("<r><n>password = &apos;hunter2&apos;</n></r>")]
    [InlineData("<r v=\"password -- note\nfoo 'x'\"><n>x</n></r>")]
    [InlineData("<r><!-- harmless --><n>x</n></r>")]
    [InlineData("<ShowPlanXML><StmtSimple StatementText=\"select @1\"/></ShowPlanXML>")]
    [InlineData("<r><ParameterizedText>select 1</ParameterizedText></r>")]
    public void ADocumentWhereRawTextAndValuesDiffer_IsNeverClearedAsRawText(string xml)
    {
        Filtered(xml, out var tookTheShortcut);

        Assert.False(tookTheShortcut);
    }

    [Fact]
    public void ADocumentThatNamesAValue_IsNotCleared()
    {
        var xml = "<r><n>" + Plain + "</n><n>" + Secret + "</n></r>";

        var result = Filtered(xml, out var tookTheShortcut);

        Assert.False(tookTheShortcut);
        Assert.DoesNotContain("S3cret", result, StringComparison.Ordinal);
    }

    [Fact]
    public void ABudgetWithAFakeJudge_IsNeverShortCircuited()
    {
        var calls = 0;
        var budget = new SensitiveStatements.JudgeBudget(
            SensitiveStatements.ReadBudget,
            _ =>
            {
                calls++;
                return SensitiveStatements.Verdict.Named;
            });
        var before = SensitiveStatements.RawCleanClears;

        var result = SensitiveStatements.Xml("<r><n>" + Plain + "</n></r>", budget);

        Assert.Equal(before, SensitiveStatements.RawCleanClears);
        Assert.True(calls > 0);
        Assert.NotEqual("<r><n>" + Plain + "</n></r>", result);
    }

    [Fact]
    public void ASpentBudget_WithholdsTheDocument_AsBefore()
    {
        var budget = new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget);
        budget.AddElapsed(TimeSpan.FromSeconds(60));

        var result = SensitiveStatements.Xml("<r><n>" + Plain + "</n></r>", budget);

        Assert.Equal(SensitiveStatements.PlaceholderText, result);
    }
}
