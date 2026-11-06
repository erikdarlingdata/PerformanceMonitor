using System;
using System.IO;
using System.Linq;
using System.Text;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class PlanFeatureExtractorTests
{
    private static string ReadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "Plans", name));

    [Fact]
    public void MissingIndex_fixture_extracts_the_real_table_and_impact()
    {
        var xml = ReadFixture("missing_index.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-missing-index", xml);

        Assert.Single(result.Features);
        var row = result.Features[0];
        Assert.Equal("ok", row.ExtractionStatus);
        Assert.Equal(1, row.StatementOrdinal);
        Assert.Equal("0x3777D0958877876E", row.QueryPlanHash);
        Assert.Equal(1, row.MissingIndexCount);

        Assert.Single(result.MissingIndexes);
        var idx = result.MissingIndexes[0];
        Assert.Equal("SampleDb3", idx.DatabaseName);
        Assert.Equal("dbo", idx.SchemaName);
        Assert.Equal("ProductNameMapping", idx.TableName);
        Assert.Equal(40.4627, idx.Impact, precision: 3);
        Assert.Equal("TenantId", idx.EqualityColumns);
        Assert.Null(idx.InequalityColumns);
        Assert.Contains("ExternalProductNameRaw", idx.IncludeColumns);
        Assert.Contains("MappedProductId", idx.IncludeColumns);
    }

    [Fact]
    public void ImplicitConversion_fixture_carries_a_PlanAffectingConvert_warning()
    {
        var xml = ReadFixture("implicit_conversion.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-implicit-conversion", xml);

        Assert.Single(result.Features);
        Assert.Equal("ok", result.Features[0].ExtractionStatus);
        Assert.Equal("0xA78AD4272579257B", result.Features[0].QueryPlanHash);

        Assert.True(result.Features[0].WarningCount >= 1);
        Assert.Contains(result.Warnings, w => w.WarningType == "Implicit Conversion");
    }

    [Fact]
    public void StmtCond_fixture_reads_the_condition_plan_and_carries_its_key_hash()
    {
        var xml = ReadFixture("stmt_cond.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-stmt-cond", xml);

        Assert.Single(result.Features);
        var row = result.Features[0];
        Assert.Equal("ok", row.ExtractionStatus);
        // Since PerformanceMonitor #4470 the parser reads the IF condition's own QueryPlan and takes the statement
        // facts (type, QueryHash, QueryPlanHash) from the StmtCond element. Before, it kept an empty placeholder
        // statement with no hash.
        Assert.Equal("0x50F69F40CDBB134A", row.QueryPlanHash);
        Assert.Equal("0xE451D88319CC70FF", row.QueryHash);
        Assert.Equal("COND WITH QUERY", row.StatementType);
        Assert.Equal(5, row.OperatorCount);
        // #7899: one table variable (@hits) read by one operator is one finding, not a statement warning plus an operator warning.
        Assert.Equal(1, row.WarningCount);
        Assert.All(result.Warnings, w => Assert.Equal("Table Variable", w.WarningType));
    }

    [Fact]
    public void FeatureVersion_is_3_and_every_row_carries_it()
    {
        Assert.Equal(3, PlanFeatureExtractor.FeatureVersion);

        var result = PlanFeatureExtractor.Extract("digest-version", ReadFixture("missing_index.sqlplan"));

        Assert.All(result.Features, f => Assert.Equal(3, f.FeatureVersion));
        Assert.All(result.Warnings, w => Assert.Equal(3, w.FeatureVersion));
        Assert.All(result.MissingIndexes, i => Assert.Equal(3, i.FeatureVersion));
    }

    [Fact]
    public void ModulePlan_second_statement_gains_a_Bare_Scan_warning()
    {
        var result = PlanFeatureExtractor.Extract("digest-module-warnings", ReadFixture("module_plan.sqlplan"));

        var second = result.Features.Single(f => f.StatementOrdinal == 2);
        // #7899: the table variable in this plan is one finding, no longer a statement warning plus an operator warning.
        Assert.Equal(2, second.WarningCount);
        Assert.Contains(result.Warnings, w => w.StatementOrdinal == 2 && w.WarningType == "Bare Scan");
    }

    [Fact]
    public void StmtCursor_fixture_parses_with_its_own_statement_type()
    {
        var xml = ReadFixture("stmt_cursor.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-stmt-cursor", xml);

        Assert.Single(result.Features);
        Assert.Equal("ok", result.Features[0].ExtractionStatus);
        Assert.Equal("DECLARE CURSOR", result.Features[0].StatementType);
        // #7899: the table variable in this plan is one finding, no longer a statement warning plus an operator warning.
        Assert.Equal(3, result.Features[0].WarningCount);
        Assert.Contains(result.Warnings, w => w.WarningType == "Cursor Missing LOCAL");
        Assert.Contains(result.Warnings, w => w.WarningType == "Bare Scan");
    }

    [Fact]
    public void MultiStatement_fixture_gives_two_rows_in_document_order()
    {
        var xml = ReadFixture("multi_statement.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-multi-statement", xml);

        Assert.Equal(2, result.Features.Count);
        Assert.Equal(1, result.Features[0].StatementOrdinal);
        Assert.Equal(2, result.Features[1].StatementOrdinal);
        Assert.All(result.Features, f => Assert.Equal("ok", f.ExtractionStatus));
    }

    [Fact]
    public void EmptyShell_fixture_gives_one_no_statements_row()
    {
        var xml = ReadFixture("empty_shell.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-empty-shell", xml);

        Assert.Single(result.Features);
        var row = result.Features[0];
        Assert.Equal("no_statements", row.ExtractionStatus);
        Assert.Equal(0, row.StatementOrdinal);
        Assert.Null(row.QueryPlanHash);
        Assert.Null(row.EstimatedSubtreeCost);
        Assert.Empty(result.Warnings);
        Assert.Empty(result.MissingIndexes);
    }

    [Fact]
    public void Malformed_fixture_never_throws_and_reports_parse_failed()
    {
        var xml = ReadFixture("malformed.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-malformed", xml);

        Assert.Single(result.Features);
        var row = result.Features[0];
        Assert.Equal("parse_failed", row.ExtractionStatus);
        Assert.Equal(0, row.StatementOrdinal);
        Assert.Null(row.EstimatedRows);
        Assert.Equal(System.Text.Encoding.UTF8.GetByteCount(xml), row.PlanXmlBytes);
    }

    [Fact]
    public void Well_formed_xml_that_is_not_a_showplan_reports_parse_failed()
    {
        var result = PlanFeatureExtractor.Extract("digest-not-a-plan", "<root><child/></root>");

        Assert.Equal("parse_failed", Assert.Single(result.Features).ExtractionStatus);
    }

    [Fact]
    public void A_plan_nested_past_the_parsers_own_depth_limit_reports_parse_failed_not_partial_rows()
    {
        // 1,100 nested IF/THEN blocks (more than ShowPlanParser.MaxParseDepth, 1000) after one ordinary statement. The
        // parser sets ParseError and keeps the partial plan; the extractor must not report that as ok or no_statements.
        const string ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
        var sb = new StringBuilder();
        sb.Append($"<ShowPlanXML xmlns=\"{ns}\" Version=\"1.564\" Build=\"16.0.4215.2\"><BatchSequence><Batch><Statements>");
        sb.Append("<StmtSimple StatementType=\"SELECT\" QueryHash=\"0xA\" QueryPlanHash=\"0xB\"/>");
        for (var i = 0; i < 1100; i++)
            sb.Append("<StmtCond StatementType=\"COND\"><Then><Statements>");
        sb.Append("<StmtSimple StatementType=\"SELECT\" QueryHash=\"0x1\" QueryPlanHash=\"0x2\"/>");
        for (var i = 0; i < 1100; i++)
            sb.Append("</Statements></Then></StmtCond>");
        sb.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");

        var result = PlanFeatureExtractor.Extract("digest-deep-cond", sb.ToString());

        var row = Assert.Single(result.Features);
        Assert.Equal("parse_failed", row.ExtractionStatus);
        Assert.Equal(0, row.StatementOrdinal);
        Assert.Empty(result.Warnings);
    }

    [Fact]
    public void Empty_shell_keeps_its_byte_count()
    {
        var xml = ReadFixture("empty_shell.sqlplan");
        var row = Assert.Single(PlanFeatureExtractor.Extract("digest-empty", xml).Features);

        Assert.Equal("no_statements", row.ExtractionStatus);
        Assert.Equal(198, row.PlanXmlBytes);
    }

    [Fact]
    public void Oversized_input_is_never_parsed()
    {
        var xml = new string('x', PlanFeatureExtractor.MaxParseChars + 1);
        var result = PlanFeatureExtractor.Extract("digest-oversized", xml);

        Assert.Single(result.Features);
        var row = result.Features[0];
        Assert.Equal("oversized", row.ExtractionStatus);
        Assert.Equal(0, row.StatementOrdinal);
        Assert.Equal(PlanFeatureExtractor.MaxParseChars + 1, row.PlanXmlBytes);
    }

    [Fact]
    public void ModulePlan_fixture_gives_ten_statements_two_with_query_plan_hash()
    {
        var xml = ReadFixture("module_plan.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-module-plan", xml);

        Assert.Equal(10, result.Features.Count);
        Assert.All(result.Features, f => Assert.Equal("ok", f.ExtractionStatus));

        var hashes = result.Features.Where(f => f.QueryPlanHash != null).Select(f => f.QueryPlanHash).ToList();
        Assert.Equal(2, hashes.Count);
        Assert.Contains("0xFD1ABB424011422E", hashes);
        Assert.Contains("0x726A6BF6619CB442", hashes);
    }

    [Fact]
    public void Every_row_of_the_same_plan_carries_the_same_byte_count()
    {
        var xml = ReadFixture("multi_statement.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-multi-statement-bytes", xml);

        var expectedBytes = Encoding.UTF8.GetByteCount(xml);
        Assert.All(result.Features, f => Assert.Equal(expectedBytes, f.PlanXmlBytes));
    }

    [Fact]
    public void PhysicalOps_are_distinct_and_sorted()
    {
        var xml = ReadFixture("missing_index.sqlplan");
        var result = PlanFeatureExtractor.Extract("digest-physical-ops", xml);

        var ops = result.Features[0].PhysicalOps;
        Assert.NotNull(ops);
        var parts = ops!.Split(',');
        Assert.Equal(parts.OrderBy(p => p, StringComparer.Ordinal), parts);
        Assert.Equal(parts.Distinct().Count(), parts.Length);
    }
}

public class PlanShapeHasherTests
{
    private static string ReadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "Plans", name));

    [Fact]
    public void ModulePlan_fixture_gives_the_two_documented_hashes_in_order()
    {
        var xml = ReadFixture("module_plan.sqlplan");
        var (shapeHash, statementHashes) = PlanShapeHasher.Compute(xml);

        Assert.Equal(new[] { "0xFD1ABB424011422E", "0x726A6BF6619CB442" }, statementHashes);
        Assert.False(string.IsNullOrEmpty(shapeHash));
        Assert.Equal(shapeHash, shapeHash.ToLowerInvariant());
    }

    [Fact]
    public void EmptyShell_fixture_gives_the_empty_list_hash()
    {
        var xml = ReadFixture("empty_shell.sqlplan");
        var (shapeHash, statementHashes) = PlanShapeHasher.Compute(xml);

        Assert.Empty(statementHashes);
        Assert.False(string.IsNullOrEmpty(shapeHash));
    }

    [Fact]
    public void Same_input_gives_the_same_hash()
    {
        var xml = ReadFixture("module_plan.sqlplan");
        var (hash1, _) = PlanShapeHasher.Compute(xml);
        var (hash2, _) = PlanShapeHasher.Compute(xml);

        Assert.Equal(hash1, hash2);
    }

    [Fact]
    public void Malformed_input_returns_whatever_was_collected_before_the_error()
    {
        var xml = ReadFixture("malformed.sqlplan");
        var (shapeHash, statementHashes) = PlanShapeHasher.Compute(xml);

        Assert.False(string.IsNullOrEmpty(shapeHash));
        Assert.NotNull(statementHashes);
    }
}
