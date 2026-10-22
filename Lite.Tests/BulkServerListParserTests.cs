/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The shared "Add Multiple Servers" paste-box grammar (<see cref="BulkServerListParser"/>). This test file
/// is MIRRORED byte-for-byte in Darling.Tests so the two bulk dialogs are pinned to the identical grammar —
/// separators, trailing-field tolerance, comment/blank skipping with correct line numbers, the field-count
/// rules, and the null-safe empty-input contract.
/// </summary>
public sealed class BulkServerListParserTests
{
    [Fact]
    public void BareHostnames_ParseAsServerNameOnly()
    {
        var result = BulkServerListParser.Parse("SQL2022\nSQL2019");

        Assert.Empty(result.Errors);
        Assert.Equal(2, result.Servers.Count);
        Assert.Equal("SQL2022", result.Servers[0].ServerName);
        Assert.Null(result.Servers[0].DisplayName);
        Assert.Null(result.Servers[0].DatabaseName);
        Assert.Equal(1, result.Servers[0].LineNumber);
        Assert.Equal("SQL2019", result.Servers[1].ServerName);
        Assert.Equal(2, result.Servers[1].LineNumber);
    }

    [Fact]
    public void CommaForm_TwoFields_IsServerAndDisplay()
    {
        var result = BulkServerListParser.Parse("SQL2022, Production");

        var line = Assert.Single(result.Servers);
        Assert.Equal("SQL2022", line.ServerName);
        Assert.Equal("Production", line.DisplayName);
        Assert.Null(line.DatabaseName);
    }

    [Fact]
    public void CommaForm_ThreeFields_IsServerDisplayDatabase()
    {
        var result = BulkServerListParser.Parse("myazure.database.windows.net, Azure Prod, AdventureWorks");

        var line = Assert.Single(result.Servers);
        Assert.Equal("myazure.database.windows.net", line.ServerName);
        Assert.Equal("Azure Prod", line.DisplayName);
        Assert.Equal("AdventureWorks", line.DatabaseName);
    }

    [Fact]
    public void TabForm_TwoAndThreeFields_SplitOnTab()
    {
        var two = BulkServerListParser.Parse("SQL2022\tProduction");
        var lineTwo = Assert.Single(two.Servers);
        Assert.Equal("SQL2022", lineTwo.ServerName);
        Assert.Equal("Production", lineTwo.DisplayName);
        Assert.Null(lineTwo.DatabaseName);

        var three = BulkServerListParser.Parse("host\tName\tdb1");
        var lineThree = Assert.Single(three.Servers);
        Assert.Equal("host", lineThree.ServerName);
        Assert.Equal("Name", lineThree.DisplayName);
        Assert.Equal("db1", lineThree.DatabaseName);
    }

    [Fact]
    public void TabTakesPrecedence_CommaStaysInsideTheField()
    {
        // A line containing BOTH a tab and a comma splits on the tab; the comma is part of the field.
        var result = BulkServerListParser.Parse("SQL2022\tContoso, Inc.");

        var line = Assert.Single(result.Servers);
        Assert.Equal("SQL2022", line.ServerName);
        Assert.Equal("Contoso, Inc.", line.DisplayName);
    }

    [Fact]
    public void InteriorEmptyField_IsNull_NotAnError()
    {
        var result = BulkServerListParser.Parse("host,,db1");

        Assert.Empty(result.Errors);
        var line = Assert.Single(result.Servers);
        Assert.Equal("host", line.ServerName);
        Assert.Null(line.DisplayName);
        Assert.Equal("db1", line.DatabaseName);
    }

    [Fact]
    public void TrailingSeparators_AreDropped_CommaAndTab()
    {
        var comma = BulkServerListParser.Parse("SQL2022, Prod,");
        var commaLine = Assert.Single(comma.Servers);
        Assert.Equal("SQL2022", commaLine.ServerName);
        Assert.Equal("Prod", commaLine.DisplayName);
        Assert.Null(commaLine.DatabaseName);
        Assert.Empty(comma.Errors);

        var tab = BulkServerListParser.Parse("SQL2022\tProd\t");
        var tabLine = Assert.Single(tab.Servers);
        Assert.Equal("SQL2022", tabLine.ServerName);
        Assert.Equal("Prod", tabLine.DisplayName);
        Assert.Null(tabLine.DatabaseName);
        Assert.Empty(tab.Errors);
    }

    [Fact]
    public void CommentAndBlankLines_AreSkipped_WithLineNumbersStillCorrect()
    {
        var result = BulkServerListParser.Parse("# header comment\n\nSQL2022\n   \n# another\nSQL2019");

        Assert.Empty(result.Errors);
        Assert.Equal(2, result.Servers.Count);
        Assert.Equal("SQL2022", result.Servers[0].ServerName);
        Assert.Equal(3, result.Servers[0].LineNumber);
        Assert.Equal("SQL2019", result.Servers[1].ServerName);
        Assert.Equal(6, result.Servers[1].LineNumber);
    }

    [Fact]
    public void MoreThanThreeFields_IsAnError_WithTheRightLineNumber()
    {
        var result = BulkServerListParser.Parse("SQL2022\nhost, a, b, c");

        Assert.Single(result.Servers);
        var error = Assert.Single(result.Errors);
        Assert.Equal(2, error.LineNumber);
        Assert.Contains("too many fields", error.Message, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EmptyServerToken_IsAnError()
    {
        var result = BulkServerListParser.Parse(", Production, db");

        Assert.Empty(result.Servers);
        var error = Assert.Single(result.Errors);
        Assert.Equal(1, error.LineNumber);
        Assert.Contains("server name is empty", error.Message, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CrlfAndLf_AreEquivalent()
    {
        var lf = BulkServerListParser.Parse("SQL2022\nSQL2019\nSQL2016");
        var crlf = BulkServerListParser.Parse("SQL2022\r\nSQL2019\r\nSQL2016");

        Assert.Equal(
            lf.Servers.Select(s => (s.LineNumber, s.ServerName)),
            crlf.Servers.Select(s => (s.LineNumber, s.ServerName)));
        Assert.Equal(3, crlf.Servers.Count);
        // A CRLF line leaves no stray carriage return on the trailing field.
        Assert.Equal("SQL2016", crlf.Servers[2].ServerName);
    }

    [Fact]
    public void ExcelPaste_TabsWithTrailingEmptyCells_ParseCleanly()
    {
        // Spreadsheet copy: tab-separated with a trailing empty cell (and a trailing tab on the row).
        var result = BulkServerListParser.Parse("SQL2022\tProduction\t\nSQL2019\tStaging\tReportServer\t");

        Assert.Empty(result.Errors);
        Assert.Equal(2, result.Servers.Count);
        Assert.Equal("SQL2022", result.Servers[0].ServerName);
        Assert.Equal("Production", result.Servers[0].DisplayName);
        Assert.Null(result.Servers[0].DatabaseName);
        Assert.Equal("SQL2019", result.Servers[1].ServerName);
        Assert.Equal("Staging", result.Servers[1].DisplayName);
        Assert.Equal("ReportServer", result.Servers[1].DatabaseName);
    }

    [Fact]
    public void RawText_IsPreservedPreTrim()
    {
        var result = BulkServerListParser.Parse("   SQL2022  ,  Production  ");

        var line = Assert.Single(result.Servers);
        Assert.Equal("   SQL2022  ,  Production  ", line.RawText);
        Assert.Equal("SQL2022", line.ServerName);
        Assert.Equal("Production", line.DisplayName);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\n\n")]
    [InlineData("# only a comment")]
    public void NullEmptyOrWhitespaceInput_YieldsEmptyResult_NoThrow(string? text)
    {
        var result = BulkServerListParser.Parse(text);

        Assert.Empty(result.Servers);
        Assert.Empty(result.Errors);
    }

    // ── The #2027 port rule: "host,port" is SQL Server's own syntax, not a display name ────────────────

    [Fact]
    public void CommaPort_FoldsIntoTheServerName_AloneAndWithDisplayAndDatabase()
    {
        var result = BulkServerListParser.Parse("sql01,2433\nsql02,14330, Prod POS\nsql03,50000, Prod POS, master");

        Assert.Empty(result.Errors);
        Assert.Equal(3, result.Servers.Count);

        Assert.Equal("sql01,2433", result.Servers[0].ServerName);
        Assert.Null(result.Servers[0].DisplayName);

        Assert.Equal("sql02,14330", result.Servers[1].ServerName);
        Assert.Equal("Prod POS", result.Servers[1].DisplayName);
        Assert.Null(result.Servers[1].DatabaseName);

        Assert.Equal("sql03,50000", result.Servers[2].ServerName);
        Assert.Equal("Prod POS", result.Servers[2].DisplayName);
        Assert.Equal("master", result.Servers[2].DatabaseName);
    }

    [Fact]
    public void CommaPort_WithDisplayAndDatabase_IsThreeLogicalFields_NotTooMany()
    {
        /* Four comma fields, but the port folds before the max-field check — the exact paste the
           #2027 report shows failing. A FIFTH field is still an error. */
        var ok = BulkServerListParser.Parse("sql01,2433,Prod,master");
        Assert.Empty(ok.Errors);
        Assert.Equal("sql01,2433", Assert.Single(ok.Servers).ServerName);

        var bad = BulkServerListParser.Parse("sql01,2433,Prod,master,extra");
        Assert.Empty(bad.Servers);
        Assert.Contains("too many fields", Assert.Single(bad.Errors).Message);
    }

    [Theory]
    [InlineData("sql01,0", "0")]           /* port 0 is not a valid port */
    [InlineData("sql01,65536", "65536")]   /* out of range */
    [InlineData("sql01,24x3", "24x3")]     /* not all digits */
    [InlineData("sql01,-1433", "-1433")]   /* signed */
    public void NonPortNumericLookalikes_StayDisplayNames(string line, string expectedDisplay)
    {
        var result = BulkServerListParser.Parse(line);

        var parsed = Assert.Single(result.Servers);
        Assert.Equal("sql01", parsed.ServerName);
        Assert.Equal(expectedDisplay, parsed.DisplayName);
    }

    [Fact]
    public void TabSeparatedLines_KeepCommaPortInsideTheFirstCell_Unchanged()
    {
        var result = BulkServerListParser.Parse("sql01,2433\t1234\tmaster");

        var parsed = Assert.Single(result.Servers);
        Assert.Equal("sql01,2433", parsed.ServerName);
        /* On a tab line a pure-numeric display name survives — the documented escape for the one
           thing the comma-line port rule costs. */
        Assert.Equal("1234", parsed.DisplayName);
        Assert.Equal("master", parsed.DatabaseName);
    }

    [Fact]
    public void CommaLine_PureNumericSecondField_IsTakenAsPort_TheDocumentedTradeoff()
    {
        /* Pinning the tradeoff by name: on a comma line "1433" cannot be a display name anymore. */
        var result = BulkServerListParser.Parse("sql01,1433");

        var parsed = Assert.Single(result.Servers);
        Assert.Equal("sql01,1433", parsed.ServerName);
        Assert.Null(parsed.DisplayName);
    }

    /// <summary>#6352: a pasted server name that SqlClient would read as a named pipe or an admin connection is a line
    /// error, as the single-server dialogs refuse it; the ordinary forms on the same paste still parse.</summary>
    [Fact]
    public void ANamedPipeOrAdminOrPathServerName_IsALineError_AndTheOtherLinesStillParse()
    {
        var result = BulkServerListParser.Parse("np:sql01\nadmin:sql02,Dac\n\\\\sql03\\pipe\\sql\\query\n//sql04/pipe\nsql05,2433\ntcp:sql06\n[fe80::1]\n");

        Assert.Equal(new[] { 1, 2, 3, 4 }, result.Errors.Select(e => e.LineNumber).ToArray());
        Assert.All(result.Errors, e => Assert.Contains("is not accepted", e.Message, StringComparison.Ordinal));
        Assert.Equal(new[] { "sql05,2433", "tcp:sql06", "[fe80::1]" }, result.Servers.Select(s => s.ServerName).ToArray());
    }
}
