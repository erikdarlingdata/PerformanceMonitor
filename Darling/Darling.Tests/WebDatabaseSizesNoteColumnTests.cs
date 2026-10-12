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
using System.Text.Json;
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web Database Sizes table for the other databases on an Azure SQL Database server. <c>get_database_sizes</c> puts
/// <see cref="AzureSiblingDatabaseSize.LogNote"/> under <c>size_note</c> on the database entry of such a database, and on
/// no other entry. The table lists that key as a Note column that is left out unless a row fills it, so every other
/// server shows no column of dashes. The option is opt-in: a table that does not ask for it renders as before.
/// </summary>
public sealed class WebDatabaseSizesNoteColumnTests
{
    private static string Slice(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor not found: {start}");
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, $"anchor not found after {start}: {end}");
        return source[from..to];
    }

    [Fact]
    public void TheDatabaseSizesColumns_ListTheNoteOnThePayloadKey_AndLeaveItOutWhenNoRowFillsIt()
    {
        Assert.Equal("size_note", AzureSiblingDatabaseSize.RowNoteKey);

        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "read-fields.js");
        var columns = Slice(js, "get_database_sizes: {", "  get_file_io_stats: {");
        Assert.Contains("{ key: \"size_note\", label: \"Note\", wrap: true, hideWhenEmpty: true }", columns, StringComparison.Ordinal);

        /* Only the Note column opts in: Database, Total and Used keep showing on every server. */
        Assert.Single(columns.Split("hideWhenEmpty", StringSplitOptions.None)[1..]);
    }

    [Fact]
    public void TheTableRenderer_DropsAnEmptyOptInColumn_AndKeepsEveryOtherColumn()
    {
        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js");
        Assert.Contains("const cols = visibleColumns(allCols, rows);", panels, StringComparison.Ordinal);

        var script = """
            const m = await import(process.argv[1]);
            const names = (cols, rows) => m.visibleColumns(cols, rows).map((c) => c.key).join(",");
            const plain = { key: "a" };
            const note = { key: "note", hideWhenEmpty: true };
            const zero = { key: "zero", hideWhenEmpty: true };
            console.log(JSON.stringify({
              noneFilled: names([plain, note], [{ a: 1, note: null }, { a: 2, note: "" }, { a: 3 }]),
              oneFilled: names([plain, note], [{ a: 1 }, { a: 2, note: "n/a" }]),
              zeroIsAValue: names([plain, zero], [{ a: 1, zero: 0 }]),
              plainKeptEmpty: names([plain], [{ a: null }]),
              allDropped: names([note, zero], [{ note: null, zero: null }]),
            }));
            """;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add("--input-type=module");
        psi.ArgumentList.Add("-e");
        psi.ArgumentList.Add(script);
        psi.ArgumentList.Add(new Uri(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js")).AbsoluteUri);

        Process proc;
        try { proc = Process.Start(psi)!; }
        catch (Win32Exception) { Assert.Skip("Node is not installed, so the shipped page script cannot be run."); throw; } // The source pins above still hold the change in place.

        using (proc)
        {
            var output = proc.StandardOutput.ReadToEnd().Trim();
            var error = proc.StandardError.ReadToEnd();
            Assert.True(proc.WaitForExit(20000) && proc.ExitCode == 0, "node failed: " + error);
            using var doc = JsonDocument.Parse(output);
            Assert.Equal("a", doc.RootElement.GetProperty("noneFilled").GetString());
            Assert.Equal("a,note", doc.RootElement.GetProperty("oneFilled").GetString());
            Assert.Equal("a,zero", doc.RootElement.GetProperty("zeroIsAValue").GetString());
            Assert.Equal("a", doc.RootElement.GetProperty("plainKeptEmpty").GetString());
            Assert.Equal("note,zero", doc.RootElement.GetProperty("allDropped").GetString());
        }
    }
}
