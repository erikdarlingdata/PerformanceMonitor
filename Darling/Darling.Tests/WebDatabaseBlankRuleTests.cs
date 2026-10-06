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
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5245: the page and the service use ONE blank-name rule for a database name. The service's rule is
/// <c>string.IsNullOrWhiteSpace</c> (<see cref="DatabaseFilter"/>); the page's is <c>isBlankDatabaseName</c> in
/// <c>viewer-local.js</c>, run here under Node. Both answer the SAME list: every UTF-16 code unit on its own (all 65,536,
/// lone surrogates included) and a set of mixed strings, so a character the two disagree on (U+0085 and U+FEFF are the two
/// JavaScript's <c>trim()</c> gets differently) fails here by name. Node is skipped when it is not installed.
/// </summary>
public sealed class WebDatabaseBlankRuleTests
{
    /// <summary>The shared list: every code unit alone, then mixed strings.</summary>
    private static string[] SharedList()
    {
        var list = Enumerable.Range(0, 0x10000).Select(c => ((char)c).ToString()).ToList();
        list.Add("");
        list.Add("\u0085 ");
        list.Add("﻿ ");
        list.Add(" a");
        list.Add("\u0085a");
        list.Add("﻿a");
        list.Add("  　\t\r\n");
        list.Add("\u0009\u000b\u000c\u001c\u001f");
        list.Add("᠎");
        list.Add("SalesDb");
        list.Add(" SalesDb");
        list.Add(new string('x', 128));
        list.Add(new string('x', 129));
        list.Add(new string('\u0085', 129));
        return list.ToArray();
    }

    private static JsonElement RunNode(string[] list)
    {
        var psi = new ProcessStartInfo("node")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "viewer-local-blank-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "viewer-local.js"));

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
            var output = proc.StandardOutput.ReadToEndAsync();
            var json = JsonSerializer.Serialize(list.Select(t => t.Select(c => (int)c).ToArray()).ToArray());
            proc.StandardInput.Write(json);
            proc.StandardInput.Close();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the blank-rule harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the blank-rule harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Result.Trim());
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ThePageAndTheService_AgreeOnWhichTextIsBlank_ForEveryCodeUnitAndTheMixedStrings()
    {
        var list = SharedList();
        var page = RunNode(list).GetProperty("blank").GetString()!;
        Assert.Equal(list.Length, page.Length);

        var disagreements = new StringBuilder();
        for (var i = 0; i < list.Length; i++)
        {
            var service = string.IsNullOrWhiteSpace(list[i]);
            Assert.Equal(service, DatabaseFilter.One(list[i]).IsAll);
            Assert.Equal(service, DatabaseFilter.Of([list[i]]).IsAll);
            if (service != (page[i] == '1'))
            {
                disagreements.Append(string.Join(" ", list[i].Take(4).Select(c => "U+" + ((int)c).ToString("X4")))).Append("; ");
            }
        }

        Assert.True(disagreements.Length == 0, "the page and the service disagree on: " + disagreements);
    }

    [Fact]
    public void TheNextLineAndByteOrderMarkCharacters_AreTheTwoJavaScriptTrimCountsDifferently()
    {
        /* Named so a failure reads without the full list: U+0085 is whitespace to .NET (blank), U+FEFF is not (a name). */
        var list = new[] { "\u0085", "﻿" };
        var page = RunNode(list);
        Assert.Equal("10", page.GetProperty("blank").GetString());
        Assert.Equal("01", page.GetProperty("name").GetString());
        Assert.True(string.IsNullOrWhiteSpace(list[0]));
        Assert.False(string.IsNullOrWhiteSpace(list[1]));
    }

    [Fact]
    public void ANameIsTextThatIsNotBlankAndNotOver128Characters_OnTheSharedList()
    {
        var list = SharedList();
        var page = RunNode(list).GetProperty("name").GetString()!;
        for (var i = 0; i < list.Length; i++)
        {
            var expected = !string.IsNullOrWhiteSpace(list[i]) && list[i].Length <= 128;
            Assert.True(expected == (page[i] == '1'), "name rule differs for text #" + i);
        }
    }
}
