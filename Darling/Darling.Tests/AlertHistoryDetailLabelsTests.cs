/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Alert History's expanded Detail row shows every label beside its value, and each section heading on its
/// own line. A blocking or incident alert's detail text is a heading line ("Blocking chain (x2)", "Incident 1
/// of 2") over indented "Label: value" lines. <c>parseDetailFields</c> folded a heading that followed a value
/// onto that value ("24.1s-34.9s Incident 1 of 2"), and turned a heading that came first into a value with no
/// label, which took the label column's grid cell and moved every later label and value over by one: the
/// labels landed at the far edge of the wide value column, out of sight, leaving bare values.
///
/// <para>The parser is a pure function, so the first test runs the shipped one under Node on a realistic
/// blocking and incident text. Node is skipped when it is not installed, the way
/// <see cref="WebRenderSettleTests"/> does; the source pins below hold the shape without it.</para>
/// </summary>
public sealed class AlertHistoryDetailLabelsTests
{
    private const string AlertsPath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/alerts.js";
    private const string CssPath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/css/app.css";

    /* Reads parseDetailFields out of the shipped alerts.js (no DOM is involved in it) and prints its result as
       JSON: an array of [label, value], with a null label for a section heading. */
    private const string NodeScript =
        "const fs = require('node:fs');" +
        "const src = fs.readFileSync(process.argv[1], 'utf8');" +
        "const start = src.indexOf('function parseDetailFields(');" +
        "const end = src.indexOf('\\n}', start) + 2;" +
        "const parse = new Function(src.slice(start, end) + '\\nreturn parseDetailFields;')();" +
        "console.log(JSON.stringify(parse(JSON.parse(process.argv[2]).join('\\n'))));";

    /// <summary>Runs the shipped parser over the lines, or returns false when Node is not installed.</summary>
    private static bool TryParse(string[] lines, out List<(string? Label, string Value)> fields)
    {
        fields = new List<(string?, string)>();

        var psi = new ProcessStartInfo("node")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-e");
        psi.ArgumentList.Add(NodeScript);
        psi.ArgumentList.Add(PathTo(AlertsPath));
        psi.ArgumentList.Add(JsonSerializer.Serialize(lines));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the parser script did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the parser script failed: " + error.Result);

            using var doc = JsonDocument.Parse(output);
            if (doc.RootElement.ValueKind == JsonValueKind.Null)
            {
                return true; // parseDetailFields returned null: the text renders raw, with no fields.
            }

            foreach (var pair in doc.RootElement.EnumerateArray())
            {
                var label = pair[0].ValueKind == JsonValueKind.Null ? null : pair[0].GetString();
                fields.Add((label, pair[1].GetString()!));
            }

            return true;
        }
    }

    [Fact]
    public void BlockingAndIncidentDetail_KeepsEveryLabelWithItsValue_AndEachHeadingOnItsOwnEntry()
    {
        var lines = new[]
        {
            "Blocking chain (x2)",
            "  Database: SalesDb",
            "  Blocked Query: SELECT 1",
            "  Blocking Query: UPDATE t",
            "  Wait Range: 24.1s-34.9s",
            "",
            "Incident 1 of 2",
            "  Dedup Key: abc",
            "  Involved Objects: dbo.t",
            "  Occurrences: 2",
            "  Total Occurrences: 3",
            "  Incident Since: 2026-01-01",
        };

        if (!TryParse(lines, out var fields))
        {
            return; // no Node here: the source pins below still hold the fix in place.
        }

        // Two headings and nine labelled fields.
        Assert.Equal(11, fields.Count);

        // A heading is its own entry with no label, first or not.
        Assert.Equal((null, "Blocking chain (x2)"), fields[0]);
        Assert.Equal((null, "Incident 1 of 2"), fields[5]);

        // It is not glued onto the value before it, and no label or value moved.
        Assert.Equal(("Wait Range", "24.1s-34.9s"), fields[4]);
        Assert.Equal(("Dedup Key", "abc"), fields[6]);
        Assert.Equal(("Occurrences", "2"), fields[8]);
        Assert.Equal(("Total Occurrences", "3"), fields[9]);
        Assert.Equal(("Incident Since", "2026-01-01"), fields[10]);
    }

    [Fact]
    public void AWrappedValueLine_StillFoldsIntoItsField_AndPlainTextStillRendersRaw()
    {
        if (!TryParse(new[] { "Story: a long story", "wraps here", "Severity: high" }, out var story))
        {
            return;
        }

        Assert.Equal(new (string?, string)[] { ("Story", "a long story wraps here"), ("Severity", "high") }, story);

        // Fewer than two labelled lines: not a field block, so the text stays a plain block (null, no fields).
        TryParse(new[] { "Just one sentence." }, out var plain);
        Assert.Empty(plain);
    }

    [Fact]
    public void FieldRow_AlwaysWritesTheLabel_AndGivesAHeadingItsOwnFullWidthRow()
    {
        var js = ReadRepoFileLf(AlertsPath);
        var start = js.IndexOf("function fieldRow(", StringComparison.Ordinal);
        Assert.True(start >= 0, "fieldRow was not found in alerts.js");
        var body = js.Substring(start, js.IndexOf("\n}", start, StringComparison.Ordinal) - start);

        // The label span is unconditional for a labelled row (it was `k ? el(...) : null`, which let a label-less
        // row into the two-column grid), and a heading is a separate element, not a value with no label.
        Assert.Contains("el(\"span\", { class: \"fk\", text: k })", body, StringComparison.Ordinal);
        Assert.DoesNotContain("k ? el(", body, StringComparison.Ordinal);
        Assert.Contains("el(\"div\", { class: \"detail-heading\", text: v })", body, StringComparison.Ordinal);

        // parseDetailFields keeps a heading as a label-less entry rather than folding it onto a value.
        Assert.Contains("fields.push([null, line]);", js, StringComparison.Ordinal);
        Assert.DoesNotContain("fields.push([\"\", line]);", js, StringComparison.Ordinal);

        // The label pattern DeadlockBodyLabelGrammarTests reads is untouched.
        Assert.Contains("const m = /^([A-Za-z][A-Za-z ]{0,28}):\\s*(.*)$/.exec(line);", js, StringComparison.Ordinal);
    }

    [Fact]
    public void DetailHeading_SpansBothGridColumns_SoItSitsOnItsOwnLine()
    {
        var css = ReadRepoFileLf(CssPath);

        // The grid is two columns (label, value); a heading that does not span them shares a row with a value.
        Assert.Matches(new Regex(@"\.detail-fields\s*\{[^}]*grid-template-columns:\s*max-content 1fr"), css);
        Assert.Matches(new Regex(@"\.detail-heading\s*\{[^}]*grid-column:\s*1\s*/\s*-1"), css);
    }
}
