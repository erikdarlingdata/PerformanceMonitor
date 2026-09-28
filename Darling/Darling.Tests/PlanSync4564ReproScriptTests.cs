using System;
using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Ports PerformanceStudio's <c>ReproScriptBuilder</c> hardening (erikdarlingdata/PerformanceStudio@719d861,
/// @04ae735, @32be789, @9488bf0, @37d2ec3, @38ea2b6) so PerformanceMonitor's copy generates the same script for
/// the same plan. #4564.
/// </summary>
public class PlanSync4564ReproScriptTests
{
    private const string DeclarationListPlan = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
          <BatchSequence><Batch><Statements>
            <StmtSimple>
              <QueryPlan>
                <ParameterList>
                  <ColumnReference Column="@id" ParameterDataType="decimal(18,2)" ParameterCompiledValue="(42.50)" />
                </ParameterList>
              </QueryPlan>
            </StmtSimple>
          </Statements></Batch></BatchSequence>
        </ShowPlanXML>
        """;

    /// <summary>
    /// @9488bf0: the sp_executesql declaration list is left out of the substituted text, using the
    /// shared list-end parser rather than the old "closing ) followed by non-comma" scan.
    /// </summary>
    [Fact]
    public void BuildReproScript_DeclarationList_IsLeftOutOfTheQueryText()
    {
        var sql = ReproScriptBuilder.BuildReproScript(
            "(@id decimal(18,2))SELECT * FROM dbo.T WHERE Id = @id", "db", DeclarationListPlan, null);

        Assert.Contains("SELECT * FROM dbo.T WHERE Id = @id", sql);
        Assert.DoesNotContain("(@id decimal(18,2))SELECT", sql);
        Assert.Contains("@id = 42.50", sql);
    }

    [Fact]
    public void BuildReproScript_DeclarationListCutOffByTruncation_IsKeptAsItIs()
    {
        /* A list that never closes reports -1, and -1 must never reach a slice. */
        var sql = ReproScriptBuilder.BuildReproScript("(@id decimal(18,2),@x nvarch", "db", DeclarationListPlan, null);

        Assert.Contains("(@id decimal(18,2),@x nvarch", sql);
    }

    private static string PlanWithParameter(string column, string dataType, string compiledValue) =>
        $"""
         <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
           <BatchSequence><Batch><Statements>
             <StmtSimple>
               <QueryPlan>
                 <ParameterList>
                   <ColumnReference Column="{column}" ParameterDataType="{dataType}" ParameterCompiledValue="{compiledValue}" />
                 </ParameterList>
               </QueryPlan>
             </StmtSimple>
           </Statements></Batch></BatchSequence>
         </ShowPlanXML>
         """;

    /// <summary>@04ae735: a value that closes its own literal and appends T-SQL becomes a placeholder.</summary>
    [Fact]
    public void BuildReproScript_CompiledValueBreakingOutOfLiteral_BecomesPlaceholder()
    {
        var plan = PlanWithParameter("@id", "int", "1; DROP TABLE dbo.Orders --");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.DoesNotContain("DROP TABLE", sql);
        Assert.Contains("@id = ?", sql);
    }

    [Fact]
    public void BuildReproScript_CompiledValueWithUnbalancedQuote_BecomesPlaceholder()
    {
        var plan = PlanWithParameter("@name", "nvarchar(50)", "N'x'; EXEC sp_who --'");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.DoesNotContain("sp_who", sql);
        Assert.Contains("@name = ?", sql);
    }

    /// <summary>@04ae735: a hostile parameter name is dropped entirely, not merely escaped.</summary>
    [Fact]
    public void BuildReproScript_HostileParameterName_IsDroppedEntirely()
    {
        var plan = PlanWithParameter("@id = 1, @x int = 1; SHUTDOWN --", "int", "(1)");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.DoesNotContain("SHUTDOWN", sql);
    }

    [Fact]
    public void BuildReproScript_HostileDataType_IsDroppedEntirely()
    {
        var plan = PlanWithParameter("@id", "int; DROP TABLE dbo.Orders --", "(1)");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.DoesNotContain("DROP TABLE", sql);
    }

    /// <summary>@32be789: the placeholder for an unsafe value says why, instead of reading as a bug.</summary>
    [Fact]
    public void BuildReproScript_UnsafeValue_ExplainsThePlaceholder()
    {
        var plan = PlanWithParameter("@id", "int", "1; DROP TABLE dbo.Orders --");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.Contains("not a simple literal", sql);
        Assert.Contains("@id", sql);
    }

    [Fact]
    public void BuildReproScript_DroppedParameter_IsReportedInWarnings()
    {
        var plan = PlanWithParameter("@id", "int; DROP TABLE dbo.Orders --", "(1)");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.Contains("1 parameter(s) omitted", sql);
    }

    /// <summary>@32be789: real-world compiled values must survive the filter unchanged.</summary>
    [Theory]
    [InlineData("int", "(42)", "42")]
    [InlineData("int", "(-7)", "-7")]
    [InlineData("bit", "(0)", "0")]
    [InlineData("decimal(18,2)", "(1.50)", "1.50")]
    [InlineData("float", "(1.0000000000000000e+000)", "1.0000000000000000e+000")]
    [InlineData("money", "($10.50)", "$10.50")]
    [InlineData("varbinary(8)", "(0x1234ABCD)", "0x1234ABCD")]
    [InlineData("datetime", "('2024-01-01 00:00:00.000')", "'2024-01-01 00:00:00.000'")]
    [InlineData("nvarchar(50)", "N'O''Brien'", "N'O''Brien'")]
    [InlineData("int", "NULL", "NULL")]
    public void BuildReproScript_RealWorldCompiledValues_SurviveTheFilter(
        string dataType, string compiledValue, string expected)
    {
        var plan = PlanWithParameter("@p", dataType, compiledValue.Replace("\"", "&quot;"));
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.Contains($"@p = {expected}", sql);
        Assert.DoesNotContain("@p = ?", sql);
    }

    /// <summary>
    /// @37d2ec3 / @38ea2b6: the header comment splits "*/" and "/*" and folds line breaks (including VT, FF,
    /// NEL, LS and PS), so a crafted database name cannot end the comment early, open a nested one, or put GO
    /// on a line of its own.
    /// </summary>
    [Theory]
    [InlineData("master*/\nGO\nPRINT 'INJECTED';\nGO\n/*")]
    [InlineData("master*/ PRINT 'INJECTED'; /*")]
    [InlineData("master\r\nGO\r\nPRINT 'INJECTED';\r\nGO")]
    [InlineData("master/*")]
    [InlineData("master/*/")]
    [InlineData("master*/*")]
    [InlineData("master\vGO\fPRINT 'INJECTED';\u0085GO\u2028x\u2029y")]
    public void BuildReproScript_HostileDatabaseName_StaysInTheHeaderComment(string databaseName)
    {
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", databaseName, null, null);

        var header = HeaderComment(sql);
        var databaseLine = Assert.Single(header.Split('\n'), line => line.StartsWith("Database: [", StringComparison.Ordinal));
        Assert.StartsWith("Database: [master", databaseLine);
        Assert.DoesNotMatch(@"[\p{Cc}\u2028\u2029]", databaseLine.TrimEnd('\r'));
        Assert.DoesNotContain(header.Split('\n'), line => line.Trim() == "GO");
    }

    [Fact]
    public void BuildReproScript_HostileSource_StaysInTheHeaderComment()
    {
        var sql = ReproScriptBuilder.BuildReproScript(
            "SELECT 1", "db", null, null, source: "x*/ PRINT 'INJECTED'; /*");

        Assert.Contains("Source: x* / PRINT 'INJECTED'; / *", HeaderComment(sql));
    }

    /// <summary>The filter lives in script generation, not extraction — raw parameters are unaffected.</summary>
    [Fact]
    public void ExtractParametersFromPlan_StillReturnsRawParameters()
    {
        var plan = PlanWithParameter("@id", "int", "(42)");
        var parameters = ReproScriptBuilder.ExtractParametersFromPlan(plan);

        Assert.Single(parameters);
        Assert.Equal("@id", parameters[0].Name);
        Assert.Equal("42", parameters[0].CompiledValue);
    }

    // Everything up to the first "*/", which must be the header's own closing line.
    private static string HeaderComment(string sql)
    {
        Assert.StartsWith("/*", sql);
        var end = sql.IndexOf("*/", StringComparison.Ordinal);
        Assert.Equal('\n', sql[end - 1]);
        Assert.DoesNotContain("/*", sql[2..end]);
        return sql[..end];
    }
}
