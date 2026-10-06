/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Security;

namespace Darling.Tests;

/// <summary>
/// #4348: the planted statements every statement-filter test uses. Plain constants and string builders only, with
/// no Darling types, so Lite.Tests can compile this file by link (precedent: the linked files in
/// <c>Lite.Tests.csproj</c>).
/// <para>A secret needle is text that must be gone after the filter. A kept needle is text the filter must leave
/// alone, so a test also proves the filter did not withhold everything.</para>
/// </summary>
internal static class StatementScrubCanary
{
    /// <summary>A statement the filter must withhold.</summary>
    public const string CanaryStatement = "CREATE LOGIN [canary_ssf] WITH PASSWORD = N'S3cret-canary-ssf'";

    /// <summary>A statement the filter must keep.</summary>
    public const string PlainStatement = "SELECT canary_plain_ssf FROM dbo.t WHERE c = @c";

    /// <summary>An auto-parameterized statement whose text is kept but whose parameter values are not.</summary>
    public const string AutoParamStatement =
        "(@1 nvarchar(4000),@2 tinyint)UPDATE [dbo].[ssf_autoparam_canary] set [password] = @1  WHERE [id]=@2";

    /// <summary>Text that must not survive the filter anywhere in a result.</summary>
    public static readonly string[] SecretNeedles =
    {
        "S3cret-canary-ssf",
        "param-secret-ssf",
        "const-secret-ssf",
        "autoparam-secret-ssf",
    };

    /// <summary>Text that must come through the filter unchanged.</summary>
    public static readonly string[] KeptNeedles =
    {
        "canary_plain_ssf",
        "param-canary-ssf",
        "ssf_autoparam_canary",
    };

    /// <summary>
    /// A ShowPlanXML with three <c>StmtSimple</c> elements. Statement 1 names the canary statement and carries
    /// secret parameter and constant values. Statement 2 is the plain statement with its own parameter value.
    /// Statement 3 is auto-parameterized: its <c>StatementText</c> is kept text, its parameter values are secret.
    /// </summary>
    public static string CanaryPlan()
    {
        return
            "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4000.1\">"
            + "<BatchSequence><Batch><Statements>"

            // Statement 1: the canary statement.
            + "<StmtSimple StatementText=\"" + Esc(CanaryStatement) + "\" StatementId=\"1\" StatementType=\"CREATE LOGIN\""
            + " ParameterizedText=\"" + Esc("(@p nvarchar(40))CREATE LOGIN [canary_ssf] WITH PASSWORD = @p") + "\">"
            + "<QueryPlan CachedPlanSize=\"16\">"
            + "<RelOp NodeId=\"0\" PhysicalOp=\"Compute Scalar\" LogicalOp=\"Compute Scalar\" EstimateRows=\"1\">"
            + "<OutputList/>"
            + "<ComputeScalar><DefinedValues><DefinedValue>"
            + "<ColumnReference Column=\"Expr1000\"/>"
            + "<ScalarOperator ScalarString=\"" + Esc("N'const-secret-ssf'") + "\">"
            + "<Const ConstValue=\"" + Esc("N'const-secret-ssf'") + "\"/>"
            + "</ScalarOperator>"
            + "</DefinedValue></DefinedValues></ComputeScalar>"
            + "</RelOp>"
            + "<ParameterList>"
            + "<ColumnReference Column=\"@p\" ParameterDataType=\"nvarchar(40)\""
            + " ParameterCompiledValue=\"" + Esc("N'param-secret-ssf'") + "\""
            + " ParameterRuntimeValue=\"" + Esc("N'param-secret-ssf'") + "\"/>"
            + "</ParameterList>"
            + "</QueryPlan></StmtSimple>"

            // Statement 2: the plain statement.
            + "<StmtSimple StatementText=\"" + Esc(PlainStatement) + "\" StatementId=\"2\" StatementType=\"SELECT\">"
            + "<QueryPlan CachedPlanSize=\"16\">"
            + "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" EstimateRows=\"1\">"
            + "<OutputList/>"
            + "<IndexScan Ordered=\"0\" ForcedIndex=\"0\" NoExpandHint=\"0\">"
            + "<Predicate><ScalarOperator ScalarString=\"" + Esc("[dbo].[t].[c]=[@c]") + "\"/></Predicate>"
            + "</IndexScan>"
            + "</RelOp>"
            + "<ParameterList>"
            + "<ColumnReference Column=\"@c\" ParameterDataType=\"int\""
            + " ParameterCompiledValue=\"" + Esc("N'param-canary-ssf'") + "\""
            + " ParameterRuntimeValue=\"" + Esc("N'param-canary-ssf'") + "\"/>"
            + "</ParameterList>"
            + "</QueryPlan></StmtSimple>"

            // Statement 3: auto-parameterized; the statement text is kept, the parameter values are not.
            + "<StmtSimple StatementText=\"" + Esc(AutoParamStatement) + "\" StatementId=\"3\" StatementType=\"UPDATE\">"
            + "<QueryPlan CachedPlanSize=\"16\">"
            + "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Update\" LogicalOp=\"Update\" EstimateRows=\"1\">"
            + "<OutputList/>"
            + "</RelOp>"
            + "<ParameterList>"
            + "<ColumnReference Column=\"@1\" ParameterDataType=\"nvarchar(4000)\""
            + " ParameterCompiledValue=\"" + Esc("N'autoparam-secret-ssf'") + "\"/>"
            + "<ColumnReference Column=\"@2\" ParameterDataType=\"tinyint\""
            + " ParameterCompiledValue=\"(7)\"/>"
            + "</ParameterList>"
            + "</QueryPlan></StmtSimple>"

            + "</Statements></Batch></BatchSequence></ShowPlanXML>";
    }

    private static string Esc(string value) => SecurityElement.Escape(value) ?? string.Empty;
}
