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
/// app.js imports every page module statically, so a module that throws while it loads blanks the whole web UI.
/// <c>node --check</c> only parses, so nothing else catches that. Two checks guard it, and they catch different things.
/// The Node load imports every file under wwwroot/js (each one separately, app.js included) with a stubbed DOM; it
/// catches a link-time missing export (<c>import { X }</c> from a module that does not export X, a SyntaxError) and an
/// unimported name used AT TOP LEVEL while the module loads. It does NOT catch an unimported name used only inside a
/// function body, because that throws only when the function runs. The source pin catches that case: it reads every
/// module's <c>export</c> lines and flags a module that uses an exported name it neither imports nor declares. The Node
/// load is skipped when Node is not installed; the source pin still guards a runner without Node.
/// </summary>
public sealed class WebModuleLoadTests
{
    private static readonly string[] JsRoot = { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" };

    [Fact]
    public async System.Threading.Tasks.Task EveryWebModuleLoadsUnderNodeWithoutATopLevelReferenceErrorOrAMissingExport()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "WebHarness", "load-web-modules.mjs"));
        psi.ArgumentList.Add(PathTo(JsRoot));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped web modules cannot be loaded.");
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

            Assert.True(proc.ExitCode == 0, "a web module failed to load: " + await error);
            Assert.Contains("loaded app.js", output);
            Assert.Contains("loaded pages/finops/utilization.js", output);
            Assert.Equal(AllModules().Length, Regex.Matches(output, @"(?m)^loaded (?!\d+ modules)").Count);
        }
    }

    private static string[] AllModules() => Directory.GetFiles(PathTo(JsRoot), "*.js", SearchOption.AllDirectories);

    /// <summary>Names any module under wwwroot/js exports, read from its <c>export</c> lines.</summary>
    private static HashSet<string> Exports()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in AllModules())
        {
            var src = File.ReadAllText(file).ReplaceLineEndings("\n");
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
        // A re-export (export { x } from "...") forwards a name without using it, so it is not a use.
        var body = StripCommentsAndStrings(Regex.Replace(source, @"(?m)^(?:import|export)\s*\{[^}]*\}\s*from\s*""[^""]+"";", ""));
        // An object-literal key (`{ tab: x }`) names a property, not the shared binding.
        body = Regex.Replace(body, @"(?<=[{,]\s*)[A-Za-z_$][\w$]*(?=\s*:)", "_key");
        // A name the module declares itself is its own, not the shared one: declarations, function parameters,
        // arrow parameters, catch bindings and destructuring all bind a local.
        var declared = new HashSet<string>(
            Regex.Matches(body, @"\b(?:function\*?|const|let|var|class)\s+([A-Za-z_$][\w$]*)").Select(m => m.Groups[1].Value),
            StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(body, @"\bfunction\*?\s*[\w$]*\s*\(([^)]*)\)|\(([^()]*)\)\s*=>|\bcatch\s*\(([^)]*)\)|\b(?:const|let|var)\s*[\{\[]([^}\]]*)[\}\]]|\b([A-Za-z_$][\w$]*)\s*=>"))
            for (var g = 1; g <= 5; g++)
                foreach (Match id in Regex.Matches(m.Groups[g].Value, @"[A-Za-z_$][\w$]*"))
                    declared.Add(id.Value);
        return exports
            .Where(n => !imported.Contains(n) && !declared.Contains(n)
                && Regex.IsMatch(body, @"(?<![\w$.])" + Regex.Escape(n) + @"(?![\w$])"))
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
    }

    [Fact]
    public void EveryWebModuleImportsEveryExportedNameItUses()
    {
        var exports = Exports();
        Assert.Contains("renderPanel", exports);
        Assert.Contains("READ_FIELDS", exports);
        Assert.Contains("VIZ", exports);
        Assert.Contains("el", exports);

        var root = PathTo(JsRoot);
        var files = AllModules();
        Assert.NotEmpty(files);
        foreach (var file in files)
        {
            var missing = UnimportedSharedNames(File.ReadAllText(file), exports);
            Assert.True(missing.Count == 0, Path.GetRelativePath(root, file) + " uses but does not import: " + string.Join(", ", missing));
        }
    }

    [Fact]
    public void ThePinFlagsAFinOpsModuleThatDropsAnImportItStillUses()
    {
        var exports = Exports();
        const string broken = "import { VIZ } from \"../../panels.js\";\nimport { el } from \"../../util.js\";\nconst X = READ_FIELDS.a;\nrenderPanel({});\nVIZ.stat(el());\n";
        Assert.Equal(new[] { "READ_FIELDS", "renderPanel" }, UnimportedSharedNames(broken, exports));
        const string fixedSource = "import { renderPanel, VIZ } from \"../../panels.js\";\nimport { el } from \"../../util.js\";\nimport { READ_FIELDS } from \"../../read-fields.js\";\nconst X = READ_FIELDS.a;\nrenderPanel({});\nVIZ.stat(el());\n";
        Assert.Empty(UnimportedSharedNames(fixedSource, exports));
    }

    [Fact]
    public void ThePinFlagsANonFinOpsModuleAndIgnoresLocalBindings()
    {
        var exports = Exports();
        var shared = exports.OrderBy(n => n, StringComparer.Ordinal).First(n => n != "el");
        var broken = "import { el } from \"../util.js\";\nexport function render() { return el(" + shared + "); }\n";
        Assert.Equal(new[] { shared }, UnimportedSharedNames(broken, exports));
        // The same name as a parameter, a destructured binding, an arrow parameter or a property is not the shared one.
        var local = "export function a(" + shared + ") { return " + shared + "; }\n"
            + "export function b(o) { const { " + shared + " } = o; return " + shared + "; }\n"
            + "export const c = (" + shared + ", x) => " + shared + " + x;\n"
            + "export const d = o => o." + shared + ";\n";
        Assert.Empty(UnimportedSharedNames(local, exports));
    }
}
