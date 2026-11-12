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
/// #7936: in the notebook composer, adding, moving or removing a cell keeps the other cells' editors as they are, so no
/// unchanged panel sends its preview read again and a field the author is typing in keeps its focus. Only a scope change
/// (the range or the variables) runs every panel's preview again, once each. Run as the shipped <c>notebook.js</c> under
/// Node against a scripted <c>/api/compose/run</c> (<c>web-notebook-editor-cells-harness.mjs</c>). Node is skipped when it is
/// not installed.
/// </summary>
public sealed class WebNotebookEditorCellsBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-notebook-editor-cells-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the notebook editor cells harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the notebook editor cells harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();
    private static int Runs(JsonElement step) => step.GetProperty("runs").GetArrayLength();

    [Fact]
    public void OpeningTheNotebook_RunsEachPanelsPreviewOnce()
    {
        var run = Result.Value;
        Assert.Equal(new[] { "md", "panel", "panel", "panel", "panel", "md" }, Strings(run.GetProperty("openCells")));
        Assert.Equal(4, run.GetProperty("openRuns").GetArrayLength());
    }

    [Fact]
    public void AddingAMarkdownCell_KeepsTheOtherCells_SendsNoRead_AndKeepsTheAuthorsFocus()
    {
        var step = Result.Value.GetProperty("addMd");
        Assert.Equal(0, Runs(step));
        Assert.Equal(new[] { "md", "panel", "panel", "panel", "panel", "md", "md" }, Strings(step.GetProperty("cells")));
        Assert.True(step.GetProperty("keptNodes").GetBoolean(), "the existing cell editors were rebuilt");
        Assert.Equal(1, step.GetProperty("newNodes").GetInt32());
        Assert.True(step.GetProperty("focusKept").GetBoolean(), "the panel title the author was typing in lost its focus");
        Assert.Equal(Enumerable.Range(1, 7).Select(i => "Cell " + i).ToArray(), Strings(step.GetProperty("titles")));
    }

    [Fact]
    public void AddingAPanelCell_BuildsOnlyTheNewCell_WhosePreviewHasNoMetricYet()
    {
        var step = Result.Value.GetProperty("addPanel");
        Assert.Equal(0, Runs(step));
        Assert.True(step.GetProperty("keptNodes").GetBoolean(), "the existing cell editors were rebuilt");
        Assert.Equal(1, step.GetProperty("newNodes").GetInt32());
        Assert.Equal("panel", Strings(step.GetProperty("cells"))[^1]);
    }

    [Fact]
    public void MovingACellUp_SwapsTheTwoNodes_RenumbersTheCells_AndSendsNoRead()
    {
        var step = Result.Value.GetProperty("moveUp");
        Assert.Equal(0, Runs(step));
        Assert.True(step.GetProperty("keptNodes").GetBoolean(), "the cell editors were rebuilt");
        Assert.True(step.GetProperty("swapped").GetBoolean(), "the two cells did not change places as the same nodes");
        Assert.Equal(Enumerable.Range(1, 8).Select(i => "Cell " + i).ToArray(), Strings(step.GetProperty("titles")));
        // The moved cell was the last one; its markdown neighbour is now last, so its label and the buttons' ends follow the new order.
        Assert.Equal(new[] { "Markdown for cell 1", "Markdown for cell 6", "Markdown for cell 8" }, Strings(step.GetProperty("mdLabels")));
        Assert.True(step.GetProperty("upDisabledOnFirst").GetBoolean());
        Assert.True(step.GetProperty("downDisabledOnLast").GetBoolean());
        Assert.True(step.GetProperty("downEnabledOnMoved").GetBoolean(), "the moved cell is no longer last, so Move down must be enabled on it");
    }

    [Fact]
    public void RemovingACell_KeepsTheOtherNodes_RenumbersFromOne_AndSendsNoRead()
    {
        var step = Result.Value.GetProperty("remove");
        Assert.Equal(0, Runs(step));
        Assert.Equal(7, step.GetProperty("count").GetInt32());
        Assert.True(step.GetProperty("keptNodes").GetBoolean(), "the remaining cell editors were rebuilt");
        Assert.True(step.GetProperty("firstIsPanel").GetBoolean(), "the first panel cell did not move up into the removed cell's place as the same node");
        Assert.Equal("Cell 1", Strings(step.GetProperty("titles"))[0]);
        Assert.True(step.GetProperty("focusKept").GetBoolean(), "the panel title the author was typing in lost its focus");
    }

    [Fact]
    public void AScopeChange_RunsEveryPanelsPreviewAgain_OnceEach()
    {
        var step = Result.Value.GetProperty("scopeChange");
        // Five panel cells, one of them the new blank one that has no metric to preview: four previews, one per panel with a metric.
        Assert.Equal(5, step.GetProperty("panelCount").GetInt32());
        Assert.Equal(4, Runs(step));
    }
}
