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

    private static string PlainText(JsonElement node) =>
        node.GetProperty("tag").GetString() == "#text"
            ? node.GetProperty("text").GetString()!
            : string.Concat(node.GetProperty("kids").EnumerateArray().Select(PlainText));

    private static bool HasTag(JsonElement node, params string[] tags) =>
        tags.Contains(node.GetProperty("tag").GetString()) || node.GetProperty("kids").EnumerateArray().Any(k => HasTag(k, tags));

    [Theory]
    [InlineData("**Story:** SOS_SCHEDULER_YIELD -> CPU_SQL_PERCENT -> LCK_M_SCH_M", "Story: SOS_SCHEDULER_YIELD -> CPU_SQL_PERCENT -> LCK_M_SCH_M")]
    [InlineData("Read sys_dm_exec_requests and sys_dm_os_wait_stats.", "Read sys_dm_exec_requests and sys_dm_os_wait_stats.")]
    [InlineData("a__b__c and snake__case__name", "a__b__c and snake__case__name")]
    public void UnderscoresInsideWords_StayLiteralText(string markdown, string expectedText)
    {
        var block = Assert.Single(Render(markdown).EnumerateArray());

        Assert.Equal(expectedText, PlainText(block));
        Assert.False(HasTag(block, "em"), "an underscore inside a word must not start italics");
    }

    [Theory]
    [InlineData("a _slanted_ word", "em", "slanted")]
    [InlineData("a __heavy__ word", "strong", "heavy")]
    [InlineData("_start_ of line", "em", "start")]
    [InlineData("a _snake_case_ word", "em", "snake_case")]
    public void UnderscoresAtWordEdges_StillEmphasise(string markdown, string tag, string inner)
    {
        var block = Assert.Single(Render(markdown).EnumerateArray());

        var span = block.GetProperty("kids").EnumerateArray().Single(k => k.GetProperty("tag").GetString() == tag);
        Assert.Equal(inner, PlainText(span));
    }

    /// <summary>The text a block reads as, a code span's own text included.</summary>
    private static string ShownText(JsonElement node) =>
        node.GetProperty("text").ValueKind == JsonValueKind.String
            ? node.GetProperty("text").GetString()!
            : string.Concat(node.GetProperty("kids").EnumerateArray().Select(ShownText));

    /// <summary>
    /// #7944: a backtick fence's info string holds no backtick (CommonMark), so a line that opens and closes inline code with three
    /// backticks, with more text after it, is a paragraph holding one code span, and the lines after it are drawn as themselves rather
    /// than swallowed into a code block that runs to the end of the cell.
    /// </summary>
    [Fact]
    public void ALineOfInlineTripleBacktickCode_IsAParagraphWithOneCodeSpan_AndOpensNoFence()
    {
        var blocks = Render("```code``` and then some text\n\nA second paragraph.").EnumerateArray().ToArray();

        Assert.Equal(2, blocks.Length);
        Assert.Equal("p", blocks[0].GetProperty("tag").GetString());
        var code = Assert.Single(blocks[0].GetProperty("kids").EnumerateArray(), k => k.GetProperty("tag").GetString() == "code");
        Assert.Equal("code", code.GetProperty("text").GetString());
        Assert.Equal("code and then some text", ShownText(blocks[0]));
        Assert.Equal("p", blocks[1].GetProperty("tag").GetString());
        Assert.Equal("A second paragraph.", ShownText(blocks[1]));
        Assert.False(HasTag(blocks[0], "pre") || HasTag(blocks[1], "pre"), "no line of this cell is a fence");
    }

    /// <summary>#7944: the same line inside a paragraph run stays in that paragraph (soft-wrapped), since it starts no block.</summary>
    [Fact]
    public void ALineOfInlineTripleBacktickCode_AfterProse_StaysInTheSameParagraph()
    {
        var block = Assert.Single(Render("Intro line\n```code``` tail").EnumerateArray());

        Assert.Equal("p", block.GetProperty("tag").GetString());
        Assert.Equal("Intro line code tail", ShownText(block));
        Assert.Single(block.GetProperty("kids").EnumerateArray(), k => k.GetProperty("tag").GetString() == "code");
    }

    /// <summary>#7944: a real fence still opens: a backtick fence with a plain info string, and a tilde fence whose info string may hold backticks.</summary>
    [Theory]
    [InlineData("```\nx = `1`\n```", "md-pre", "x = `1`")]
    [InlineData("```sql\nselect `1`\n```", "md-pre", "select `1`")]
    [InlineData("~~~ `info`\nbody\n~~~", "md-pre", "body")]
    [InlineData("```text\n```code``` inside\n```", "md-pre md-pre-text", "```code``` inside")]
    public void ARealFence_StillOpens_AndHoldsItsBodyVerbatim(string markdown, string cls, string body)
    {
        var pre = Assert.Single(Render(markdown).EnumerateArray());

        Assert.Equal("pre", pre.GetProperty("tag").GetString());
        Assert.Equal(cls, pre.GetProperty("cls").GetString());
        Assert.Equal(body, pre.GetProperty("kids")[0].GetProperty("text").GetString());
    }

    /// <summary>The text of each code span in a block, in order.</summary>
    private static string[] CodeSpans(JsonElement block) =>
        block.GetProperty("kids").EnumerateArray()
            .Where(k => k.GetProperty("tag").GetString() == "code")
            .Select(k => k.GetProperty("text").GetString()!)
            .ToArray();

    /// <summary>
    /// #7944: a run of backticks opens a code span that the next run of exactly the same length closes (CommonMark). A longer or
    /// shorter run inside the span is part of its text, and a two-backtick span is one span rather than two empty ones.
    /// </summary>
    [Theory]
    [InlineData("`a``b`", "a``b", new[] { "a``b" })]
    [InlineData("``double`` and `single`", "double and single", new[] { "double", "single" })]
    public void ABacktickRun_IsClosedByTheNextRunOfTheSameLength(string markdown, string shown, string[] spans)
    {
        var block = Assert.Single(Render(markdown).EnumerateArray());

        Assert.Equal("p", block.GetProperty("tag").GetString());
        Assert.Equal(spans, CodeSpans(block));
        Assert.Equal(shown, ShownText(block));
    }

    /// <summary>
    /// #7944: a run of backticks with no run of the same length after it is literal text, kept whole, and the scan goes on after the
    /// run rather than inside it, so a later span still pairs on its own.
    /// </summary>
    [Theory]
    [InlineData("x ``` y", "x ``` y", new string[0])]
    [InlineData("``` a `b`", "``` a b", new[] { "b" })]
    public void ABacktickRunWithNoMatch_IsLiteralText(string markdown, string shown, string[] spans)
    {
        var block = Assert.Single(Render(markdown).EnumerateArray());

        Assert.Equal("p", block.GetProperty("tag").GetString());
        Assert.Equal(spans, CodeSpans(block));
        Assert.Equal(shown, ShownText(block));
    }
}
