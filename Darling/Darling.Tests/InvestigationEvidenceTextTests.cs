/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8555: the Viewer's Investigations detail pane draws an evidence item's <c>key_numbers</c> string, when it holds JSON, as the object it
/// is. Text that is not JSON stays as it was.
/// </summary>
public sealed class InvestigationEvidenceTextTests
{
    [Fact]
    public void AKeyNumbersStringThatHoldsJson_IsDrawnAsTheObject()
    {
        var stored = "[{\"tool\":\"get_cpu\",\"key_numbers\":\"{\\\"sql_cpu\\\":\\\"17.9% average\\\",\\\"path\\\":\\\"a-\\\\u003Eb\\\"}\"}]";

        var shown = InvestigationsWindow.EvidenceText(stored);

        Assert.Contains("\"sql_cpu\": \"17.9% average\"", shown);
        Assert.Contains("\"path\": \"a->b\"", shown);
        Assert.DoesNotContain("\\\"", shown);
        Assert.DoesNotContain("u003E", shown);
    }

    [Theory]
    [InlineData("[{\"tool\":\"get_blocking\",\"key_numbers\":\"head blocker 412 s\"}]", "head blocker 412 s")]
    [InlineData("[{\"tool\":\"x\",\"key_numbers\":\"412\"}]", "412")]
    public void AKeyNumbersStringThatIsNotAnObjectOrArray_StaysText(string stored, string text)
    {
        Assert.Contains("\"key_numbers\": \"" + text + "\"", InvestigationsWindow.EvidenceText(stored));
    }

    [Theory]
    [InlineData("")]
    [InlineData("not json")]
    [InlineData("{\"tool\":\"x\"}")]
    public void EvidenceThatIsNotAJsonArray_IsShownAsStored(string stored)
    {
        Assert.Equal(stored, InvestigationsWindow.EvidenceText(stored));
    }
}
