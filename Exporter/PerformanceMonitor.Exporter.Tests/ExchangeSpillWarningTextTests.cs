using System.Linq;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// #8517: the exchange spill warning the Exporter stores counts its writes as a person says it. One write reads
/// "Exchange spill: 1 write to TempDB." and a larger count keeps its plural. No checked-in plan spills a single
/// write, so the plan is built here with the count under test.
/// </summary>
public class ExchangeSpillWarningTextTests
{
    private static string PlanXml(long writesToTempDb) => $"""
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT c FROM dbo.t" StatementId="1" StatementCompId="1" StatementType="SELECT" RetrievedFromCache="true" QueryHash="0x1111" QueryPlanHash="0x2222">
          <QueryPlan DegreeOfParallelism="4" CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Parallelism" LogicalOp="Gather Streams" EstimateRows="1" EstimateIO="0" EstimateCPU="0.0285" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0317" Parallel="true" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList><ColumnReference Database="[db]" Schema="[dbo]" Table="[t]" Column="c"/></OutputList>
              <Warnings><ExchangeSpillDetails WritesToTempDb="{writesToTempDb}"/></Warnings>
              <Parallelism>
                <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="1" Parallel="true" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList><ColumnReference Database="[db]" Schema="[dbo]" Table="[t]" Column="c"/></OutputList>
                  <TableScan Ordered="false" ForcedIndex="false" ForceScan="false" NoExpandHint="false" Storage="RowStore">
                    <DefinedValues><DefinedValue><ColumnReference Database="[db]" Schema="[dbo]" Table="[t]" Column="c"/></DefinedValue></DefinedValues>
                    <Object Database="[db]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/>
                  </TableScan>
                </RelOp>
              </Parallelism>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    [Theory]
    [InlineData(1, "Exchange spill: 1 write to TempDB.")]
    [InlineData(5786565, "Exchange spill: 5,786,565 writes to TempDB.")]
    public void Exchange_spill_warning_row_counts_its_writes_as_a_person_says(long writes, string expectedStart)
    {
        var result = PlanFeatureExtractor.Extract("digest-exchange-spill", PlanXml(writes));

        var row = Assert.Single(result.Warnings, w => w.WarningType == "Exchange Spill");
        Assert.StartsWith(expectedStart, row.Message);
    }
}
