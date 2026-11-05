using System.Text;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// A deeply nested plan used to end the exporter process with a stack overflow: <c>ShowPlanParser</c> recurses once per
/// RelOp level (about 12 KB a frame) and the exporter called it on a 1 MB stack. These tests build synthetic plans of
/// that shape and run the per-plan path on a 1 MB-stack thread, the stack size of the Windows main thread and of
/// thread-pool threads. Without the fix the test host process dies instead of the test failing.
/// </summary>
public class DeepPlanTests
{
    private const int OneMegabyte = 1024 * 1024;
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    /// <summary>
    /// A statement whose plan is a left-deep chain of <paramref name="relOpDepth"/> Nested Loops RelOps over a scan,
    /// with a predicate of <paramref name="predicateLevels"/> nested AND levels on the scan.
    /// </summary>
    internal static string BuildPlan(int relOpDepth, int predicateLevels)
    {
        var sb = new StringBuilder();
        sb.Append($"<ShowPlanXML xmlns=\"{Ns}\" Version=\"1.564\" Build=\"16.0.4215.2\"><BatchSequence><Batch><Statements>");
        sb.Append("<StmtSimple StatementType=\"DELETE\" QueryHash=\"0xAB0C6A6D2B775FC5\" QueryPlanHash=\"0xFAFAD7EBF4FA0B95\"><QueryPlan>");
        for (var i = 0; i < relOpDepth; i++)
            sb.Append($"<RelOp NodeId=\"{i + 1}\" PhysicalOp=\"Nested Loops\" LogicalOp=\"Inner Join\" EstimateRows=\"1\"><NestedLoops>");
        sb.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" EstimateRows=\"1\"><IndexScan><Predicate>");
        for (var i = 0; i < predicateLevels; i++)
            sb.Append("<ScalarOperator ScalarString=\"(a) AND (b)\"><Logical Operation=\"AND\">");
        sb.Append("<ScalarOperator ScalarString=\"a\"><Identifier><ColumnReference Column=\"c\"/></Identifier></ScalarOperator>");
        for (var i = 0; i < predicateLevels; i++)
            sb.Append("</Logical></ScalarOperator>");
        sb.Append("</Predicate></IndexScan></RelOp>");
        for (var i = 0; i < relOpDepth; i++)
            sb.Append("</NestedLoops></RelOp>");
        sb.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>");
        return sb.ToString();
    }

    private static T RunOnSmallStack<T>(Func<T> work)
    {
        T? result = default;
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            try { result = work(); }
            catch (Exception ex) { failure = ex; }
        }, OneMegabyte);
        thread.Start();
        thread.Join();
        if (failure is not null)
            throw new InvalidOperationException("work failed on the small-stack thread", failure);
        return result!;
    }

    [Theory]
    [InlineData(146, 210)] // the measured production shape: a 146-level join chain and a 210-level predicate
    [InlineData(300, 2000)]
    [InlineData(PlanFeatureExtractor.MaxRelOpDepth - 1, 210)] // the deepest plan that is still parsed
    public void A_deep_plan_is_extracted_on_a_one_megabyte_stack(int relOpDepth, int predicateLevels)
    {
        var xml = BuildPlan(relOpDepth, predicateLevels);

        var result = RunOnSmallStack(() => PlanFeatureExtractor.Extract("deep-ok", xml));

        var row = Assert.Single(result.Features);
        Assert.Equal("ok", row.ExtractionStatus);
        Assert.True(row.OperatorCount >= relOpDepth + 1, $"operator_count {row.OperatorCount} counts every RelOp");
        Assert.Equal("DELETE", row.StatementType);
    }

    [Theory]
    [InlineData(PlanFeatureExtractor.MaxRelOpDepth + 1, 0)]
    [InlineData(2000, 210)]
    [InlineData(1, PlanFeatureExtractor.MaxElementDepth)] // two elements per predicate level: far past the element limit
    [InlineData(1, 100_000)]
    public void A_plan_nested_past_the_limits_is_reported_too_deep_and_not_parsed(int relOpDepth, int predicateLevels)
    {
        var xml = BuildPlan(relOpDepth, predicateLevels);

        var result = RunOnSmallStack(() => PlanFeatureExtractor.Extract("deep-too-deep", xml));

        var row = Assert.Single(result.Features);
        Assert.Equal("too_deep", row.ExtractionStatus);
        Assert.Equal(0, row.StatementOrdinal);
        Assert.Equal("deep-too-deep", row.PlanDigest);
        Assert.Equal(Encoding.UTF8.GetByteCount(xml), row.PlanXmlBytes);
        Assert.Null(row.OperatorCount);
        Assert.Empty(result.Warnings);
        Assert.Empty(result.MissingIndexes);
    }

    [Fact]
    public void Extraction_does_not_depend_on_the_caller_stack()
    {
        // A plan that overflows a 1 MB stack when parsed on the caller's thread (80 RelOp levels overflow it).
        var xml = BuildPlan(200, 0);

        var small = RunOnSmallStack(() => PlanFeatureExtractor.Extract("d", xml));
        var normal = PlanFeatureExtractor.Extract("d", xml);

        Assert.Equal(normal.Features, small.Features);
    }

    [Fact]
    public void Depth_check_counts_RelOp_and_element_levels_exactly()
    {
        Assert.False(PlanFeatureExtractor.ExceedsDepthLimits(BuildPlan(PlanFeatureExtractor.MaxRelOpDepth - 1, 0)));
        Assert.True(PlanFeatureExtractor.ExceedsDepthLimits(BuildPlan(PlanFeatureExtractor.MaxRelOpDepth, 0)));

        // Sibling RelOps are not nested: a wide plan is never too deep.
        var siblings = new StringBuilder($"<ShowPlanXML xmlns=\"{Ns}\"><Batch><Statements><StmtSimple><QueryPlan><RelOp><Concat>");
        for (var i = 0; i < PlanFeatureExtractor.MaxRelOpDepth * 4; i++)
            siblings.Append("<RelOp/><RelOp></RelOp>");
        siblings.Append("</Concat></RelOp></QueryPlan></StmtSimple></Statements></Batch></ShowPlanXML>");
        Assert.False(PlanFeatureExtractor.ExceedsDepthLimits(siblings.ToString()));

        // Malformed XML is left to the parser, which reports parse_failed.
        Assert.False(PlanFeatureExtractor.ExceedsDepthLimits("<a><b></a>"));
    }

    [Fact]
    public void The_whole_per_plan_path_survives_a_deep_plan_on_a_one_megabyte_stack()
    {
        // The path DimensionTableExporter takes for one plan: plan_shape row with the XML, the shape hash, the feature
        // extraction, then the feature rows, all through DuckDB appenders into Parquet.
        var dir = Directory.CreateTempSubdirectory("darling-export-deep-plan-").FullName;
        try
        {
            var deepButParsed = BuildPlan(146, 210);
            var tooDeep = BuildPlan(PlanFeatureExtractor.MaxRelOpDepth + 100, 210);

            var statuses = RunOnSmallStack(() =>
            {
                var shape = new List<ColumnShape> { new("plan_xml", "VARCHAR"), new("plan_xml_bytes", "BIGINT") };
                var featuresPath = Path.Combine(dir, "features.parquet");
                var shapePath = Path.Combine(dir, "shape.parquet");
                using (var shapeWriter = new ChunkedParquetWriter(Path.Combine(dir, "shape.duckdb"), "plan_shape", shape))
                using (var featuresWriter = new ChunkedParquetWriter(Path.Combine(dir, "features.duckdb"), "plan_features", DimensionTableExporter.FeatureColumns()))
                {
                    foreach (var (digest, xml) in new[] { ("aa", deepButParsed), ("bb", tooDeep) })
                    {
                        var row = shapeWriter.Appender.CreateRow();
                        row.AppendValue(xml);
                        row.AppendValue((long)Encoding.UTF8.GetByteCount(xml));
                        row.EndRow();
                        shapeWriter.RowAppended(xml.Length);

                        Assert.Equal(2, PlanShapeHasher.Compute(xml).StatementPlanHashes.Count + 1);
                        foreach (var feature in PlanFeatureExtractor.Extract(digest, xml).Features)
                        {
                            DimensionTableExporter.AppendFeatureRow(featuresWriter.Appender, "store_a", new DateTime(2026, 10, 1), feature);
                            featuresWriter.RowAppended();
                        }
                    }

                    shapeWriter.Complete(shapePath);
                    featuresWriter.Complete(featuresPath);
                }

                using var connection = new DuckDBConnection("Data Source=:memory:");
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText =
                    $"SELECT string_agg(plan_digest || '=' || extraction_status, ',' ORDER BY plan_digest), " +
                    $"(SELECT count(*) FROM read_parquet('{shapePath.Replace("'", "''")}')) " +
                    $"FROM read_parquet('{featuresPath.Replace("'", "''")}')";
                using var reader = command.ExecuteReader();
                Assert.True(reader.Read());
                Assert.Equal(2L, reader.GetInt64(1)); // both plan_shape rows land, deep or not
                return reader.GetString(0);
            });

            Assert.Equal("aa=ok,bb=too_deep", statuses);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
