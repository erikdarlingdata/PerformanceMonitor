/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7564 and #7556: a size cell prints no digit its reply cannot back. An index of 456 KB (466,944 bytes) read "460.8 KB" in By Index and
/// "409.6 KB" in Index Bloat because the page turned a rounded MB back into KB, and Buffer Pool Residency read "0 MB" and "1 MB" for
/// 0.2 and 0.5. These run the shipped <c>util.js</c> and the Storage, I/O, Waits and Memory column lists through the shared grid under
/// Node. Node is skipped when it is not installed.
/// </summary>
public sealed class WebSizeCellsBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-size-cells-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped panel script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the size-cells harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the size-cells harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Texts(JsonElement r, string name) =>
        r.GetProperty(name).GetProperty("texts").EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void AMegabyteSizeUnderOneMegabyte_KeepsItsMegabytesAndTheDecimalsItWasGiven()
    {
        var f = Run().GetProperty("fmtMbFit");
        Assert.Equal("0.45 MB", f.GetProperty("0.45").GetString());
        Assert.Equal("0.2 MB", f.GetProperty("0.2").GetString());
        // A rounded MB of 0 is any size under half its last place, so it is not "0 B".
        Assert.Equal("0 MB", f.GetProperty("0").GetString());
        Assert.Equal("—", f.GetProperty("null").GetString());
        Assert.Equal("—", f.GetProperty("x").GetString());
        Assert.Equal("1.5 MB", f.GetProperty("1.5").GetString());
        Assert.Equal("2.0 GB", f.GetProperty("2048").GetString());
    }

    [Fact]
    public void ByIndexSize_ShowsTheIndexBytes_NotARoundedMegabyteTurnedBackIntoKilobytes()
    {
        var r = Run();
        // 466,944 bytes is exactly 456 KB (the What It Costs cell says 456 KB). The reply's index_mb of 0.45 used to print 460.8 KB.
        Assert.Equal(new[] { "456.0 KB", "16.0 KB", "—", "21.4 MB" }, Texts(r, "byIndexSize"));
    }

    [Fact]
    public void IndexBloatSizeAndReclaimable_ShowTheirBytes()
    {
        var r = Run();
        var size = Texts(r, "bloatSize");
        Assert.Equal("456.0 KB", size[0]);
        Assert.Equal("16.0 KB", size[1]);
        Assert.Equal("21.4 MB", size[3]);
        // estimated_reclaimable_mb is 0 for 45,600 bytes and 4 for 4 MB; the bytes are what the cell reads.
        Assert.Equal(new[] { "45.6 KB", "—", "0 B", "4.0 MB" }, Texts(r, "bloatReclaimable"));
    }

    [Fact]
    public void TableBloatHeapAndEstimate_ShowTheirBytes_AndAnUnmeasuredHeapIsADash()
    {
        var r = Run();
        Assert.Equal(new[] { "456.0 KB", "—" }, Texts(r, "tableHeap"));
        Assert.Equal(new[] { "8.0 KB", "—" }, Texts(r, "tableBloat"));
    }

    [Fact]
    public void BufferPoolResidencyAndDeviceIo_ShowSmallValuesInTheirOwnUnit_NotZeroOrOneMegabyte()
    {
        var r = Run();
        // 29 buffers of 8 KB: the reply's buffer_mb of 0.2 used to read "0 MB".
        Assert.Equal(new[] { "232.0 KB", "0 B" }, Texts(r, "resident"));
        Assert.Equal(new[] { "204.8 KB" }, Texts(r, "deviceRead"));
        Assert.Equal(new[] { "0 B" }, Texts(r, "deviceWrite"));
    }

    [Fact]
    public void AMegabyteColumnThatCanHoldSmallValues_KeepsThemAndStaysRightAligned()
    {
        var r = Run();
        // 0.5 used to round up to "1 MB" and 0 stayed "0 MB" beside a nonzero share.
        Assert.Equal(new[] { "0.5 MB", "0 MB", "2.5 MB", "—" }, Texts(r, "granted"));
        Assert.Equal(new[] { "0.75 MB" }, Texts(r, "clerk"));
        Assert.All(r.GetProperty("granted").GetProperty("aligned").EnumerateArray(), a => Assert.True(a.GetBoolean(), "a fitted MB cell lost its numeric alignment"));
    }

    [Fact]
    public void SizeColumnsOutsideTheServerTabs_KeepTheMegabytesTheirReplyCarries()
    {
        var r = Run().GetProperty("r2");
        string[] One(string name) => r.GetProperty(name).GetProperty("texts").EnumerateArray().Select(e => e.GetString()!).ToArray();
        // Whole-number MB used to round 0.5 up to "1 MB" and print 0.25 as "0 MB".
        Assert.Equal(new[] { "0.5 MB", "2.5 MB" }, One("semaphoreGranted"));
        Assert.Equal(new[] { "0.25 MB" }, One("semaphoreAvailable"));
        Assert.Equal(new[] { "0.5 MB" }, One("grantsUsed"));
        Assert.Equal(new[] { "0.25 MB" }, One("tempdbVersionStore"));
        Assert.Equal(new[] { "0.75 MB" }, One("tempdbReserved"));
        Assert.Equal(new[] { "0.5 MB" }, One("tempdbInternal"));
        Assert.Equal(new[] { "0.5 MB" }, One("tempdbUser"));
        Assert.Equal(new[] { "0.5 MB" }, One("dbTotal"));
        Assert.Equal(new[] { "2.5 MB" }, One("dbUsed"));
        Assert.Equal(new[] { "0.5 MB" }, One("fileSize"));
        Assert.Equal(new[] { "0.25 MB" }, One("pvsSize"));
        Assert.Equal(new[] { "8.5 MB" }, One("pvsData"));
        Assert.Equal(new[] { "0.5 MB" }, One("growth"));
        Assert.Equal(new[] { "0.5 MB" }, One("finopsTotal"));
        Assert.Equal(new[] { "0.25 MB" }, One("finopsUsed"));
        Assert.Equal(new[] { "0.25 MB" }, One("finopsFree"));
        Assert.Equal(new[] { "0.5 MB" }, One("finopsVolumeFree"));
        Assert.Equal(new[] { "4.5 MB" }, One("versionStoreData"));
    }

    [Fact]
    public void TheAvailableMemoryTiles_OfTheUtilizationPageAndTheMemoryPressureTemplate_KeepASmallValue()
    {
        var r = Run().GetProperty("r3");
        string Tile(string set, string label) => r.GetProperty(set).GetProperty(label).GetString()!;
        // 0.5 used to round up to "1 MB" and 0.25 to "0 MB". On an Azure SQL Database the same field carries the other caption.
        Assert.Equal("0.5 MB", Tile("utilizationBox", "Available"));
        Assert.Equal("0.25 MB", Tile("utilizationAzure", "Available under limit"));
        Assert.Equal("0.5 MB", Tile("templateBox", "Available"));
        Assert.Equal("0.25 MB", Tile("templateAzure", "Available under limit"));
    }
}
