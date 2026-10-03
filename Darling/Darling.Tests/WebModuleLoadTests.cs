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
using System.Threading;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// app.js imports every page module statically, so a module that throws while it loads blanks the whole web UI.
/// <c>node --check</c> only parses, so nothing else catches that. Two checks guard it, and they catch different things.
/// The Node load imports every file under wwwroot/js (each one separately, app.js included) with a stubbed DOM; it
/// catches a link-time missing export (<c>import { X }</c> from a module that does not export X, a SyntaxError) and an
/// unimported name used AT TOP LEVEL while the module loads, and (after the imports, the harness drains the event loop)
/// an error thrown or rejected in async work a module starts, such as app.js's un-awaited start() calls. It does NOT catch an unimported name used only inside a
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
            var errorTask = proc.StandardError.ReadToEndAsync();
            var outputTask = proc.StandardOutput.ReadToEndAsync();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));
            try
            {
                await proc.WaitForExitAsync(timeout.Token);
            }
            catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the module load script did not finish in 30 s");
            }

            var output = await outputTask;
            Assert.True(proc.ExitCode == 0, "a web module failed to load or left an async error unhandled: " + await errorTask);
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
            foreach (var n in ExportListNames(src))
                names.Add(n);
        }
        return names;
    }

    /// <summary>Names exported by a local <c>export { a, b as c };</c> list (a re-export with <c>from</c> is not one).</summary>
    internal static List<string> ExportListNames(string source)
    {
        var names = new List<string>();
        foreach (Match m in Regex.Matches(source.ReplaceLineEndings("\n"), @"(?m)^export\s*\{([^}]*)\}\s*;?[ \t]*(?!\s*from\b)$"))
            foreach (var part in m.Groups[1].Value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                names.Add(part.Split(" as ", StringSplitOptions.None).Last().Trim());
        return names;
    }

    [Fact]
    public void TheExportReaderSeesAnExportListAndNotAReExport()
    {
        Assert.Equal(new[] { "a", "c" }, ExportListNames("const a = 1;\nexport { a, b as c };\n"));
        Assert.Empty(ExportListNames("export { x } from \"./y.js\";\n"));
    }

    private static string StripCommentsAndStrings(string body)
    {
        body = Regex.Replace(body, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        body = Regex.Replace(body, @"(?m)^\s*//.*$", " ");
        body = Regex.Replace(body, "\"(?:[^\"\\\\\\n]|\\\\.)*\"", "\"\"");
        return Regex.Replace(body, "'(?:[^'\\\\\\n]|\\\\.)*'", "''");
    }

    private static HashSet<string> ImportedNames(string source)
    {
        var imported = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(source, @"(?m)^import\s*\{([^}]*)\}\s*from\s*""[^""]+"";"))
            foreach (var part in m.Groups[1].Value.Split(','))
                imported.Add(part.Trim().Split(" as ", StringSplitOptions.None).Last().Trim());
        return imported;
    }

    /// <summary>The code of a module with its re-exports, comments, strings and object-literal keys neutralized.</summary>
    private static string CodeBody(string source)
    {
        // A re-export (export { x } from "...") forwards a name without using it, so it is not a use.
        var body = StripCommentsAndStrings(Regex.Replace(source, @"(?m)^(?:import|export)\s*\{[^}]*\}\s*from\s*""[^""]+"";", ""));
        // An object-literal key (`{ tab: x }`) names a property, not the shared binding.
        return Regex.Replace(body, @"(?<=[{,]\s*)[A-Za-z_$][\w$]*(?=\s*:)", "_key");
    }

    /// <summary>Names a module binds locally: declarations, function parameters, arrow parameters, catch bindings and
    /// destructuring all bind a local.</summary>
    private static HashSet<string> LocalBindings(string body)
    {
        var declared = new HashSet<string>(
            Regex.Matches(body, @"\b(?:function\*?|const|let|var|class)\s+([A-Za-z_$][\w$]*)").Select(m => m.Groups[1].Value),
            StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(body, @"\bfunction\*?\s*[\w$]*\s*\(([^)]*)\)|\(([^()]*)\)\s*=>|\bcatch\s*\(([^)]*)\)|\b(?:const|let|var)\s*[\{\[]([^}\]]*)[\}\]]|\b([A-Za-z_$][\w$]*)\s*=>"))
            for (var g = 1; g <= 5; g++)
                foreach (Match id in Regex.Matches(m.Groups[g].Value, @"[A-Za-z_$][\w$]*"))
                    declared.Add(id.Value);
        return declared;
    }

    /// <summary>Names a module both imports and binds locally. The pin treats a locally bound name as the module's own,
    /// so such a name would silently switch the pin off for that import; it has to be empty.</summary>
    internal static List<string> ImportedNamesAlsoBoundLocally(string source) =>
        ImportedNames(source.ReplaceLineEndings("\n"))
            .Where(n => LocalBindings(CodeBody(source.ReplaceLineEndings("\n"))).Contains(n))
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

    /// <summary>The identifiers a module uses that a shared module exports but its import lines do not name.</summary>
    internal static List<string> UnimportedSharedNames(string source, HashSet<string> exports)
    {
        source = source.ReplaceLineEndings("\n");
        var imported = ImportedNames(source);
        var body = CodeBody(source);
        var declared = LocalBindings(body);
        return exports
            .Where(n => !imported.Contains(n) && !declared.Contains(n)
                && Regex.IsMatch(body, @"(?<![\w$.])" + Regex.Escape(n) + @"(?![\w$])"))
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
    }

    [Fact]
    public void NoWebModuleBindsLocallyANameItImports()
    {
        var root = PathTo(JsRoot);
        foreach (var file in AllModules())
        {
            var both = ImportedNamesAlsoBoundLocally(File.ReadAllText(file));
            Assert.True(both.Count == 0, Path.GetRelativePath(root, file) + " imports and also binds locally: " + string.Join(", ", both)
                + " (the source pin would treat the name as the module's own and stop checking it)");
        }
    }

    [Fact]
    public void ThePinFailsWhenAModuleImportsANameAndAlsoBindsItAsAParameter()
    {
        const string collides = "import { field } from \"./util.js\";\nexport function f(field) { return field; }\n";
        Assert.Equal(new[] { "field" }, ImportedNamesAlsoBoundLocally(collides));
        const string clean = "import { field } from \"./util.js\";\nexport function f(x) { return field(x); }\n";
        Assert.Empty(ImportedNamesAlsoBoundLocally(clean));
        // A name that is NOT imported and is bound locally is the module's own, and stays out of the report.
        Assert.Empty(ImportedNamesAlsoBoundLocally("export function f(field) { return field; }\n"));
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
