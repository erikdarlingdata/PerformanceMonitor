/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4582: the plan viewer's "Copy Query Text" fallback to the captured full query on a
/// truncated single-statement plan (erikdarlingdata/PerformanceStudio@35249e2, corrected by
/// @fdd3d22 to count statements the way the grid counts). <see cref="PlanDisplayText.CopyQueryText"/>
/// is the pure part; the clipboard-write guard itself lives in <c>PerformanceMonitor.Ui.ClipboardText</c>
/// (net10.0-windows, cannot run here — see <c>Viewer4582WpfTests</c> below, which is read-against but
/// not run on macOS).
/// </summary>
public sealed class Viewer4582Tests
{
    private static PlanStatement Statement(string text) => new() { StatementText = text };

    private static string Truncated(string prefix) => prefix + new string('x', PlanStatement.TruncationLengthThreshold);

    [Fact]
    public void SingleStatement_Truncated_WithCapturedText_ReturnsTheCapturedText()
    {
        var statement = Statement(Truncated("SELECT "));
        var captured = "SELECT * FROM dbo.Orders WHERE OrderId = 1";

        var result = PlanDisplayText.CopyQueryText(statement, statementCount: 1, capturedQueryText: captured);

        Assert.Equal(captured, result);
    }

    [Fact]
    public void SingleStatement_NotTruncated_ReturnsThePlansOwnText_EvenWithCapturedTextAvailable()
    {
        var statement = Statement("SELECT 1");
        var captured = "SELECT 1 -- as captured";

        var result = PlanDisplayText.CopyQueryText(statement, statementCount: 1, capturedQueryText: captured);

        Assert.Equal(statement.StatementText, result);
    }

    [Fact]
    public void Truncated_WithNoCapturedText_ReturnsThePlansOwnTruncatedText()
    {
        var statement = Statement(Truncated("SELECT "));

        var result = PlanDisplayText.CopyQueryText(statement, statementCount: 1, capturedQueryText: null);

        Assert.Equal(statement.StatementText, result);
    }

    /// <summary>
    /// The case @fdd3d22 fixed: a plan captured around <c>EXEC dbo.SomeProc</c> has one BATCH
    /// statement but several rows once the procedure body is flattened in (the grid's count, via
    /// <c>PlanStatements.EnumerateAll</c> in the viewer) — so a caller must NOT count batches and
    /// must NOT hand back the captured text (the outer EXEC) for a truncated body statement.
    /// </summary>
    [Fact]
    public void MultiStatementByGridCount_Truncated_ReturnsThePlansOwnText_NotTheCapturedBatch()
    {
        var bodyStatement = Statement(Truncated("UPDATE dbo.Big SET Col = "));
        var captured = "EXEC dbo.SomeProc";

        // statementCount: 2 mirrors the grid's flattened count (the outer EXEC plus one body
        // statement), even though the plan was captured around a single batch statement.
        var result = PlanDisplayText.CopyQueryText(bodyStatement, statementCount: 2, capturedQueryText: captured);

        Assert.Equal(bodyStatement.StatementText, result);
    }
}
