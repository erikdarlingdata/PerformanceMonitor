using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/*
    This repository is PUBLIC. Fixtures reach for realistic-looking instance names, and a real
    fleet's names carry tenant and customer identifiers that must not be published.

    Two scrubs missed the same string before this guard existed. The first substituted the old
    service prefix WITH its trailing hyphen, so the bare form survived as a peer's match pattern
    -- and that one was load-bearing, so the rename compiled and quietly changed what the fixture
    asserted. The second swept case-sensitively, so an upper-case host spelled with a real tenant
    slug survived in the case-insensitivity test, which is precisely the assertion that has to be
    spelled in capitals.

    So the guard is DERIVED, not a list: it matches the SHAPE of an instance name anywhere in
    tracked source, case-insensitively, and allows only slugs that are obviously invented.
    A new fixture file is covered the day it is added, without anyone remembering to enlist it.
*/
public sealed class FleetIdentifierScrubTests
{
    /* Greek letters and role words -- nothing here names a real customer. pgmonitor is a role
       word too: it names one of the maintainer's own monitoring hosts, the same as monitor. */
    private static readonly HashSet<string> SyntheticSlugs = new(StringComparer.OrdinalIgnoreCase)
    {
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "omega",
        "monitor", "pgmonitor", "multi", "primary", "replica", "secondary", "reporting",
        "test", "sample", "example", "demo", "fake", "dummy", "placeholder",
    };

    /*
        No leading \b. A name often follows an escape in a C# literal --
        "servers:\nprod-..." -- and in the SOURCE TEXT the n of \n is a word character, so a
        leading boundary fails at exactly the spot a hostname does occur. The scrub this guard
        backs missed a name that way. The second segment is loose for the same reason: the
        fleet has a pg-* store as well as use[0-9] ones.
    */
    private static readonly Regex InstanceName = new(
        @"(?:prod|stage|staging|dev|qa|uat)-[a-z]+-[a-z0-9]+-(?<slug>[a-z0-9]+)-[0-9]+\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    [Fact]
    public void NoTrackedSourceFileNamesARealFleetTenant()
    {
        var offenders = new List<string>();
        var scanned = 0;
        var root = RepoRoot();

        foreach (var file in TrackedSourceFiles())
        {
            scanned++;
            var text = File.ReadAllText(file);
            foreach (Match m in InstanceName.Matches(text))
            {
                var slug = m.Groups["slug"].Value;
                if (SyntheticSlugs.Contains(slug))
                {
                    continue;
                }

                var line = text.Take(m.Index).Count(c => c == '\n') + 1;
                var relative = Path.GetRelativePath(root, file);
                offenders.Add($"{relative}:{line}  {m.Value}  (slug '{slug}')");
            }
        }

        /* If the sweep stops reaching the tree, it passes for the wrong reason. */
        Assert.True(scanned > 200, $"only scanned {scanned} files -- the sweep lost the tree");

        Assert.True(
            offenders.Count == 0,
            "This repository is public. These instance names use a slug that is not on the "
          + "synthetic list, so they may name a real tenant. Rename them, or add the slug to "
          + "SyntheticSlugs if it is genuinely invented:"
          + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    private static IEnumerable<string> TrackedSourceFiles()
    {
        var extensions = new[] { ".cs", ".ps1", ".md", ".json", ".xaml", ".js", ".yml", ".yaml", ".sql" };

        return Directory
            .EnumerateFiles(RepoRoot(), "*", SearchOption.AllDirectories)
            .Where(f => extensions.Contains(Path.GetExtension(f), StringComparer.OrdinalIgnoreCase))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}.git{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}node_modules{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}pg-runtime{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            /*
                .claude/worktrees holds separate checkouts of other branches. They are not part of
                the tree this repo publishes, and scanning them makes the guard permanently red on
                a developer box while staying green in CI, where they do not exist -- which is the
                worst of both, because a local red that never goes away trains people to ignore it.
            */
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}.claude{Path.DirectorySeparatorChar}", StringComparison.Ordinal));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
