/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2114 / #2181 / #2331: a <c>{StaticResource Key}</c> whose key is not resolvable from the file that
/// references it is not a style nit — inside a DataGrid cell template it throws
/// <c>XamlParseException</c> during measure, and WPF's re-attempted template realization stack-overflows
/// the process (<c>0xc00000fd</c>, uncatchable, no error dialog). The first version of this scan modeled
/// the failure as CROSS-APP (a key defined nowhere in the same app), because that was #2114's shape —
/// and #2331 proved that model too generous: the Darling Viewer's own Query Store grid referenced
/// <c>DarkButton</c>, a key that IS defined in the app… in <c>MainWindow.xaml</c>'s window resources,
/// which a UserControl's templates cannot see. StaticResource resolves LEXICALLY at load (own file, then
/// merged dictionaries, then App.xaml) — never through the runtime element tree, which is
/// DynamicResource's job. It shipped in 3.4.0 and stayed invisible on the dogfood box only because an
/// EMPTY grid never applies its cell template.
///
/// <para>So the model here is per-FILE, matching WPF's actual lookup: a reference in file F must resolve
/// from F's own definitions, dictionaries F merges (transitively, via
/// <c>&lt;ResourceDictionary Source="…"/&gt;</c>), or the app scope (App.xaml plus everything IT merges).
/// The scan proved exact before it was adopted: run over both apps it flagged exactly the one real crash
/// and zero false positives. A key that must be shared across files belongs in App.xaml or a merged
/// dictionary — moving it there is the fix this test demands, never widening the model back.</para>
/// </summary>
public sealed class XamlStaticResourceHygieneTests
{
    /* Each app scope: its XAML subtrees. Shared control libraries would join the scope of every
       app that references them; today neither app consumes XAML from outside its own tree. */
    private static readonly (string App, string[] Roots)[] Scopes =
    {
        ("Lite", new[] { "Lite" }),
        ("Darling.Viewer", new[] { Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer") }),
    };

    private static readonly Regex Reference = new(
        @"\{StaticResource\s+(?<key>[A-Za-z0-9_.]+)\s*\}", RegexOptions.Compiled);

    private static readonly Regex Definition = new(
        @"x:Key\s*=\s*""(?<key>[A-Za-z0-9_.]+)""", RegexOptions.Compiled);

    private static readonly Regex MergeSource = new(
        @"<ResourceDictionary\s+Source\s*=\s*""(?<src>[^""]+)""", RegexOptions.Compiled);

    [Fact]
    public void EveryStaticResourceKey_ResolvesFromTheFileThatReferencesIt()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for " +
            "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it — fix the " +
            "walk-up rather than skipping, or the rule stops being enforced without anyone noticing.");

        var offenders = new List<string>();
        foreach (var (app, roots) in Scopes)
        {
            var files = roots
                .Select(r => Path.Combine(root!, r))
                .Where(Directory.Exists)
                .SelectMany(r => Directory.EnumerateFiles(r, "*.xaml", SearchOption.AllDirectories))
                .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}"))
                .ToList();
            Assert.NotEmpty(files);

            /* App scope: App.xaml's own keys plus everything it merges — visible everywhere in the app,
               because Application resources are the last stop of every StaticResource lookup. */
            var appXaml = files.FirstOrDefault(f => Path.GetFileName(f) == "App.xaml");
            var appScope = appXaml is null
                ? new HashSet<string>(StringComparer.Ordinal)
                : TransitiveDefinitions(appXaml, new HashSet<string>(StringComparer.OrdinalIgnoreCase));

            /* System-supplied keys referenced by name, never defined in app XAML. */
            appScope.Add("SystemParameters.VerticalScrollBarWidthKey");

            foreach (var file in files)
            {
                var visible = TransitiveDefinitions(file, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                visible.UnionWith(appScope);

                foreach (Match m in Reference.Matches(File.ReadAllText(file)))
                {
                    var key = m.Groups["key"].Value;
                    if (!visible.Contains(key))
                        offenders.Add($"{Path.GetRelativePath(root!, file)}: StaticResource {key} ({app} scope)");
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "StaticResource keys that do not resolve from the file referencing them (own definitions + " +
            "merged dictionaries + App.xaml scope). StaticResource is LEXICAL — a key defined in another " +
            "window's or control's resources is invisible no matter who hosts whom at runtime, and inside a " +
            "cell template the miss is the #2114/#2331 uncatchable stack-overflow crash. Define the key in " +
            "the same file, move it to App.xaml / a merged dictionary, or drop the explicit Style:\n" +
            string.Join("\n", offenders));
    }

    /// <summary>The keys defined in a file plus, transitively, in every dictionary it merges via
    /// <c>Source=</c> (relative paths, and <c>;component/</c> pack paths with the prefix stripped and the
    /// remainder resolved against the referencing file — correct for same-assembly sources, the only kind
    /// this repo uses; a cross-assembly pack URI would not resolve here). A Source that cannot be resolved
    /// contributes nothing, which only ever makes the scan stricter.</summary>
    private static HashSet<string> TransitiveDefinitions(string file, HashSet<string> seenFiles)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        if (!seenFiles.Add(file) || !File.Exists(file))
        {
            return keys;
        }

        var text = File.ReadAllText(file);
        foreach (Match m in Definition.Matches(text))
        {
            keys.Add(m.Groups["key"].Value);
        }

        foreach (Match m in MergeSource.Matches(text))
        {
            var src = m.Groups["src"].Value;
            var componentIndex = src.IndexOf(";component/", StringComparison.OrdinalIgnoreCase);
            if (componentIndex >= 0)
            {
                src = src[(componentIndex + ";component/".Length)..];
            }

            var candidate = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(file)!, src.Replace('/', Path.DirectorySeparatorChar)));
            keys.UnionWith(TransitiveDefinitions(candidate, seenFiles));
        }

        return keys;
    }

    /// <summary>Same walk-up idiom as <c>DocCommentHygieneTests.FindRepoRoot</c>.</summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
