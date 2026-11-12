/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7228: the Viewer's three history windows save a plan through the shared <c>FileSaveHelper.SavePlanFile</c> and show
/// "Saved" only when it returns true. That return rests on <c>TryWriteText</c> reporting a failed write as false, and on
/// <c>SanitizeFileName</c> giving the suggested name only characters a file name can hold (the Viewer tab passes a
/// collected object's full name through it).
/// </summary>
public class ViewerHistoryWindowPlanSaveTests
{
    [Fact]
    public void TryWriteText_AFailedWrite_ReturnsFalse_SoNoWindowShowsSaved()
    {
        var folder = Path.Combine(Path.GetTempPath(), "pm7228v-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            // A directory is not a file: the write fails the way a full disk or a locked file does.
            Assert.False(FileSaveHelper.TryWriteText(folder, "<ShowPlanXML/>", out var error));
            Assert.False(string.IsNullOrWhiteSpace(error));
        }
        finally
        {
            Directory.Delete(folder);
        }
    }

    [Fact]
    public void SanitizeFileName_KeepsAViewerProcedureName_AndCleansASchemaQualifiedOneWithBadCharacters()
    {
        Assert.Equal("ProcedurePlan_dbo.GetOrders", FileSaveHelper.SanitizeFileName("ProcedurePlan_dbo.GetOrders"));
        var clean = FileSaveHelper.SanitizeFileName("ProcedurePlan_db:sales/dbo.\"Get|Orders\"");
        Assert.Equal(-1, clean.IndexOfAny(Path.GetInvalidFileNameChars()));
    }
}
