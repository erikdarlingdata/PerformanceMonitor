using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Ports PerformanceStudio's <c>ReproScriptBuilder</c> hardening
/// (erikdarlingdata/PerformanceStudio#592, merged 6e52102) so PerformanceMonitor's copy admits SQL Server 2025's
/// <c>vector(n,float16)</c> parameter type, declares each parameter once across a batch's statements, and picks
/// a compiled value the same way PerformanceStudio does. #4613.
/// </summary>
public class PlanSync4613ReproScriptTests
{
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

    [Fact]
    public void BuildReproScript_AutoParameterizedNames_AreDeclaredAndAssigned()
    {
        /* PerformanceStudio#590: simple and forced parameterization name their parameters @0, @1, ... .
           These were dropped, so the script ran the statement without declaring them and failed with
           "Must declare the scalar variable". The ParameterList is copied from a forced-parameterization
           plan on SQL Server 2025, in its order. */
        const string plan = """
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
              <BatchSequence><Batch><Statements>
                <StmtSimple>
                  <QueryPlan>
                    <ParameterList>
                      <ColumnReference Column="@1" ParameterDataType="int" ParameterCompiledValue="(0)" />
                      <ColumnReference Column="@0" ParameterDataType="nvarchar(4000)" ParameterCompiledValue="N'b'" />
                    </ParameterList>
                  </QueryPlan>
                </StmtSimple>
              </Statements></Batch></BatchSequence>
            </ShowPlanXML>
            """;
        var sql = ReproScriptBuilder.BuildReproScript(
            "(@0 nvarchar(4000),@1 int)select t . id from dbo . T as t where t . v = @0 and t . id > @1",
            "db", plan, null);

        Assert.Contains("N'@1 int, @0 nvarchar(4000)'", sql);
        Assert.Contains("@1 = 0", sql);
        Assert.Contains("@0 = N'b'", sql);
        Assert.DoesNotContain("omitted", sql);
    }

    [Fact]
    public void BuildReproScript_ParameterUsedByTwoStatements_IsDeclaredOnce()
    {
        /* A batch's plan lists each statement's parameters, so a parameter that two statements use appears
           twice. Declaring it twice fails with "The variable name '@id' has already been declared". The
           ParameterLists are copied from such a plan on SQL Server 2025. */
        const string plan = """
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
              <BatchSequence><Batch><Statements>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@id" ParameterDataType="int" ParameterCompiledValue="(1)" />
                </ParameterList></QueryPlan></StmtSimple>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@id" ParameterDataType="int" ParameterCompiledValue="(1)" />
                </ParameterList></QueryPlan></StmtSimple>
              </Statements></Batch></BatchSequence>
            </ShowPlanXML>
            """;
        var sql = ReproScriptBuilder.BuildReproScript(
            "(@id int)SELECT COUNT_BIG(*) AS a FROM dbo.T AS t WHERE t.id = @id\n; SELECT COUNT_BIG(*) AS b FROM dbo.T AS t WHERE t.id > @id",
            "db", plan, null);

        Assert.Contains("N'@id int',", sql);
        Assert.Equal(1, sql.Split("@id = 1").Length - 1);
    }

    [Fact]
    public void BuildReproScript_AutoParameterTypedDifferentlyByTwoStatements_IsLeftOut()
    {
        /* Each auto-parameterized statement numbers its own parameters and types them by its literal, so a
           batch's estimated plan can list @1 smallint and @1 tinyint. Its statement text is the literal text,
           which doesn't use @1, so the script runs the batch as it is. The ParameterLists are copied from such
           a plan on SQL Server 2025. */
        const string plan = """
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
              <BatchSequence><Batch><Statements>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@1" ParameterDataType="smallint" ParameterCompiledValue="(22656)" />
                </ParameterList></QueryPlan></StmtSimple>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@1" ParameterDataType="tinyint" ParameterCompiledValue="(11)" />
                </ParameterList></QueryPlan></StmtSimple>
              </Statements></Batch></BatchSequence>
            </ShowPlanXML>
            """;
        var sql = ReproScriptBuilder.BuildReproScript(
            "SELECT t.id FROM dbo.T AS t WHERE t.id = 22656\n; SELECT t.v FROM dbo.T AS t WHERE t.id = 11",
            "db", plan, null);

        Assert.DoesNotContain("EXECUTE sys.sp_executesql", sql);
        Assert.Contains("different data type in different statements (left out): @1.", sql);
        Assert.Contains("SELECT t.v FROM dbo.T AS t WHERE t.id = 11", sql);
        Assert.DoesNotContain("omitted", sql);
        Assert.DoesNotContain("No parameters found in plan cache", sql);
        Assert.Contains("/* No parameters declared: see the warnings above */", sql);
    }

    [Fact]
    public void BuildReproScript_ParameterWithAValueOnlyInTheSecondStatement_UsesThatValue()
    {
        /* A statement compiled without sniffing lists the parameter with no compiled value. Taking the first
           entry would set @id to ? although another statement has a value. */
        const string plan = """
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
              <BatchSequence><Batch><Statements>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@id" ParameterDataType="int" />
                </ParameterList></QueryPlan></StmtSimple>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@id" ParameterDataType="int" ParameterCompiledValue="(5)" />
                </ParameterList></QueryPlan></StmtSimple>
              </Statements></Batch></BatchSequence>
            </ShowPlanXML>
            """;
        var sql = ReproScriptBuilder.BuildReproScript(
            "(@id int)SELECT COUNT_BIG(*) AS a FROM dbo.T AS t WHERE t.id = @id\n; SELECT COUNT_BIG(*) AS b FROM dbo.T AS t WHERE t.id > @id",
            "db", plan, null);

        Assert.Contains("N'@id int',", sql);
        Assert.Contains("@id = 5", sql);
        Assert.DoesNotContain("missing values", sql);
        Assert.DoesNotContain("different compiled value", sql);
    }

    [Fact]
    public void BuildReproScript_ParameterWithDifferentValuesInTwoStatements_UsesTheFirstAndWarns()
    {
        /* Statements recompiled at different times sniff different values. The script can only set one, so
           it uses the first and says the statements disagree. */
        const string plan = """
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
              <BatchSequence><Batch><Statements>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@id" ParameterDataType="int" ParameterCompiledValue="(1)" />
                </ParameterList></QueryPlan></StmtSimple>
                <StmtSimple><QueryPlan><ParameterList>
                  <ColumnReference Column="@id" ParameterDataType="int" ParameterCompiledValue="(2)" />
                </ParameterList></QueryPlan></StmtSimple>
              </Statements></Batch></BatchSequence>
            </ShowPlanXML>
            """;
        var sql = ReproScriptBuilder.BuildReproScript(
            "(@id int)SELECT COUNT_BIG(*) AS a FROM dbo.T AS t WHERE t.id = @id\n; SELECT COUNT_BIG(*) AS b FROM dbo.T AS t WHERE t.id > @id",
            "db", plan, null);

        Assert.Contains("@id = 1", sql);
        Assert.DoesNotContain("@id = 2", sql);
        Assert.Contains("different compiled value in different statements (set to the first usable one): @id.", sql);
    }

    [Theory]
    [InlineData("tinyint")]
    [InlineData("decimal(18,2)")]
    [InlineData("nvarchar(max)")]
    [InlineData("nvarchar(4000)")]
    [InlineData("datetime2(7)")]
    [InlineData("datetimeoffset(3)")]
    [InlineData("time(0)")]
    [InlineData("sys.geography")]
    [InlineData("sys.hierarchyid")]
    [InlineData("sql_variant")]
    [InlineData("xml")]
    [InlineData("json")]
    [InlineData("vector(3)")]
    [InlineData("vector(3,float16)")]
    [InlineData("[dbo].[Amount]")]
    public void BuildReproScript_DataTypesFromRealPlans_AreKept(string dataType)
    {
        /* Every type here but the last is a ParameterDataType that SQL Server 2025 wrote into a plan. An
           alias type shows up as its base type, and a typed xml parameter as xml. No plan here used brackets,
           but the check before this fix allowed them, so this keeps them working. */
        var plan = PlanWithParameter("@p", dataType, "NULL");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT @p", "db", plan, null);

        Assert.Contains($"N'@p {dataType}'", sql);
        Assert.DoesNotContain("omitted", sql);
    }

    [Theory]
    [InlineData("int) SELECT 2 SELECT (1")] // text after the closing paren
    [InlineData("decimal(18,2")]            // no closing paren
    [InlineData("int&#10;")]                // ends in a line break
    [InlineData("a.b.c.d")]                 // four-part name
    [InlineData("vector(3,&#10;GO&#10;)")]  // GO on a line of its own
    [InlineData("nvarchar(٤)")]        // an Arabic-Indic digit
    [InlineData("nvarchar(４)")]        // a full-width digit
    [InlineData("varchar(max,2)")]          // max takes no second part
    public void BuildReproScript_MalformedDataType_IsDropped(string dataType)
    {
        /* The check before this fix was a list of characters, and it passed every one of these but the one
           with GO. */
        var plan = PlanWithParameter("@id", dataType, "(1)");
        var sql = ReproScriptBuilder.BuildReproScript("SELECT 1", "db", plan, null);

        Assert.Contains("1 parameter(s) omitted", sql);
        Assert.DoesNotContain("SELECT 2", sql);
    }
}
