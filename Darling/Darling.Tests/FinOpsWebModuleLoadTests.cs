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
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A FinOps page module that uses a name it does not import throws a ReferenceError while the browser loads it, and
/// because finops.js and app.js import every page, the whole web UI stays blank. <c>node --check</c> only parses,
/// so nothing else catches it. The first test loads the shipped modules under Node with a stubbed DOM (skipped when
/// Node is not installed); the second holds the same fact at source level so a runner without Node still guards it.
/// </summary>
public sealed class FinOpsWebModuleLoadTests
{
    private static readonly string[] JsRoot = { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" };
    private static readonly string[] SharedModules = { "util.js", "panels.js", "charts.js", "read-fields.js" };

    [Fact]
    public async System.Threading.Tasks.Task EveryFinOpsModuleLoadsUnderNodeWithoutAReferenceErrorOrAMissingExport()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "WebHarness", "load-finops-modules.mjs"));
        psi.ArgumentList.Add(PathTo(JsRoot));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped FinOps modules cannot be loaded.");
            return;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the module load script did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "a FinOps module failed to load: " + await error);
            Assert.Contains("loaded pages/finops/utilization.js", output);
        }
    }

    /// <summary>Names a shared module exports, read from its <c>export</c> lines.</summary>
    private static HashSet<string> Exports()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var module in SharedModules)
        {
            var src = ReadRepoFile(JsRoot.Append(module).ToArray()).ReplaceLineEndings("\n");
            foreach (Match m in Regex.Matches(src, @"(?m)^export\s+(?:async\s+)?(?:function\*?|const|let|class)\s+([A-Za-z_$][\w$]*)"))
                names.Add(m.Groups[1].Value);
        }
        return names;
    }

    private static string StripCommentsAndStrings(string body)
    {
        body = Regex.Replace(body, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        body = Regex.Replace(body, @"(?m)^\s*//.*$", " ");
        body = Regex.Replace(body, "\"(?:[^\"\\\\\\n]|\\\\.)*\"", "\"\"");
        return Regex.Replace(body, "'(?:[^'\\\\\\n]|\\\\.)*'", "''");
    }

    /// <summary>The identifiers a module uses that a shared module exports but its import lines do not name.</summary>
    internal static List<string> UnimportedSharedNames(string source, HashSet<string> exports)
    {
        source = source.ReplaceLineEndings("\n");
        var imported = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(source, @"(?m)^import\s*\{([^}]*)\}\s*from\s*""[^""]+"";"))
            foreach (var part in m.Groups[1].Value.Split(','))
                imported.Add(part.Trim().Split(" as ", StringSplitOptions.None).Last().Trim());
        var body = StripCommentsAndStrings(Regex.Replace(source, @"(?m)^import\s*\{[^}]*\}\s*from\s*""[^""]+"";", ""));
        // A name the module declares itself is its own, not the shared one.
        var declared = new HashSet<string>(
            Regex.Matches(body, @"\b(?:function|const|let|var|class)\s+([A-Za-z_$][\w$]*)").Select(m => m.Groups[1].Value),
            StringComparer.Ordinal);
        return exports
            .Where(n => !imported.Contains(n) && !declared.Contains(n)
                && Regex.IsMatch(body, @"(?<![\w$.])" + Regex.Escape(n) + @"(?![\w$])"))
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
    }

    [Fact]
    public void EveryFinOpsPageImportsEverySharedNameItUses()
    {
        var exports = Exports();
        Assert.Contains("renderPanel", exports);
        Assert.Contains("READ_FIELDS", exports);
        Assert.Contains("VIZ", exports);
        Assert.Contains("el", exports);

        var dir = PathTo(JsRoot.Concat(new[] { "pages", "finops" }).ToArray());
        var files = Directory.GetFiles(dir, "*.js");
        Assert.NotEmpty(files);
        foreach (var file in files)
        {
            var missing = UnimportedSharedNames(File.ReadAllText(file), exports);
            Assert.True(missing.Count == 0, Path.GetFileName(file) + " uses but does not import: " + string.Join(", ", missing));
        }
    }

    [Fact]
    public void ThePinFlagsAModuleThatDropsAnImportItStillUses()
    {
        var exports = Exports();
        const string broken = "import { VIZ } from \"../../panels.js\";\nimport { el } from \"../../util.js\";\nconst X = READ_FIELDS.a;\nrenderPanel({});\nVIZ.stat(el());\n";
        Assert.Equal(new[] { "READ_FIELDS", "renderPanel" }, UnimportedSharedNames(broken, exports));
        const string fixedSource = "import { renderPanel, VIZ } from \"../../panels.js\";\nimport { el } from \"../../util.js\";\nimport { READ_FIELDS } from \"../../read-fields.js\";\nconst X = READ_FIELDS.a;\nrenderPanel({});\nVIZ.stat(el());\n";
        Assert.Empty(UnimportedSharedNames(fixedSource, exports));
    }
}
