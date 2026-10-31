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
/// The notebook's markdown renderer (<c>markdown.js</c>) run under Node. A fenced block shows its body as typed; a fence whose
/// info string is <c>text</c> (the alert notebook's finding story) gets the wrapping prose class, and any other fence is drawn as
/// before. Node is skipped when it is not installed.
/// </summary>
public sealed class WebMarkdownTests
{
    private static JsonElement Render(string markdown)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-markdown-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.Environment["HARNESS_INPUT"] = JsonSerializer.Serialize(new[] { markdown });

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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the markdown harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the markdown harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.GetProperty("out")[0].Clone();
        }
    }

    private static bool HasLink(JsonElement node) =>
        node.GetProperty("tag").GetString() == "a" || node.GetProperty("kids").EnumerateArray().Any(HasLink);

    [Fact]
    public void ATextFence_RendersAsWrappingProse_WithItsContentAsLiteralText()
    {
        const string story = "See [here](https://example.invalid/story) and **bold**.";
        var blocks = Render("```text\n" + story + "\n```");

        var pre = Assert.Single(blocks.EnumerateArray());
        Assert.Equal("pre", pre.GetProperty("tag").GetString());
        Assert.Equal("md-pre md-pre-text", pre.GetProperty("cls").GetString());
        Assert.Equal(story, pre.GetProperty("kids")[0].GetProperty("text").GetString());
        Assert.False(HasLink(pre), "a link written inside the block must stay text, not become an <a>");
    }

    [Fact]
    public void ALongerTextFence_WithAFenceClosingAttempt_StaysOneProseBlock()
    {
        const string story = "```\n[x](https://example.invalid/y)\n````\n[z](https://example.invalid/z)";
        var blocks = Render("`````text\n" + story + "\n`````");

        var pre = Assert.Single(blocks.EnumerateArray());
        Assert.Equal("md-pre md-pre-text", pre.GetProperty("cls").GetString());
        Assert.Equal(story, pre.GetProperty("kids")[0].GetProperty("text").GetString());
        Assert.False(HasLink(pre));
    }

    [Theory]
    [InlineData("```\nplain [x](https://example.invalid/y)\n```")]
    [InlineData("```sql\nplain [x](https://example.invalid/y)\n```")]
    [InlineData("```texture\nplain [x](https://example.invalid/y)\n```")]
    public void AnyOtherFence_RendersAsBefore(string markdown)
    {
        var pre = Assert.Single(Render(markdown).EnumerateArray());

        Assert.Equal("md-pre", pre.GetProperty("cls").GetString());
        Assert.Equal("plain [x](https://example.invalid/y)", pre.GetProperty("kids")[0].GetProperty("text").GetString());
        Assert.False(HasLink(pre));
    }
}
