/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (side find): SQL Server records <c>ParameterizedText</c> as an ATTRIBUTE of the statement element. An ad hoc
/// shell statement has no <c>QueryPlan</c>, so the parser's element-only read left the plan viewer's "Parameterized"
/// row empty. The attribute is read with the other statement attributes; the element form stays as the fallback.
/// The attribute fixture is a real capture (<c>Fixtures/StatementScrub/autoparam_select_adhoc_shell.xml</c>).
/// </summary>
public sealed class ShowPlanParserParameterizedTextTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static string Fixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "StatementScrub", name));

    private static PlanStatement OnlyStatement(string xml) =>
        ShowPlanParser.Parse(xml).Batches.SelectMany(b => b.Statements).Single();

    [Fact]
    public void AdhocShellStatement_ReadsTheParameterizedTextAttribute()
    {
        var stmt = OnlyStatement(Fixture("autoparam_select_adhoc_shell.xml"));

        Assert.Equal(
            "(@1 nvarchar(4000),@2 nvarchar(4000))SELECT [id] FROM [tempdb].[dbo].[ssf_l2b] WHERE [username]=@1 AND [password]=@2",
            stmt.ParameterizedText);
    }

    [Fact]
    public void ElementFormParameterizedText_StillParses()
    {
        string xml = $"""
            <ShowPlanXML xmlns="{Ns}" Version="1.539" Build="15.0.4410.1"><BatchSequence><Batch><Statements>
            <StmtSimple StatementText="SELECT 1" StatementId="1" StatementType="SELECT"><QueryPlan CachedPlanSize="8" CompileTime="1" CompileCPU="1" CompileMemory="96"><ParameterizedText>(@1 int)SELECT @1</ParameterizedText></QueryPlan></StmtSimple>
            </Statements></Batch></BatchSequence></ShowPlanXML>
            """;

        Assert.Equal("(@1 int)SELECT @1", OnlyStatement(xml).ParameterizedText);
    }

    [Fact]
    public void AttributeWinsOverTheElementForm()
    {
        string xml = $"""
            <ShowPlanXML xmlns="{Ns}" Version="1.539" Build="15.0.4410.1"><BatchSequence><Batch><Statements>
            <StmtSimple StatementText="SELECT 1" StatementId="1" StatementType="SELECT" ParameterizedText="(@1 int)ATTR"><QueryPlan CachedPlanSize="8" CompileTime="1" CompileCPU="1" CompileMemory="96"><ParameterizedText>(@1 int)ELEMENT</ParameterizedText></QueryPlan></StmtSimple>
            </Statements></Batch></BatchSequence></ShowPlanXML>
            """;

        Assert.Equal("(@1 int)ATTR", OnlyStatement(xml).ParameterizedText);
    }
}
