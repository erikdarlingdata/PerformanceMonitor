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

    TWO shapes are reachable that way -- a full hostname, and a short name plus its ordinal.
    Two more are NOT, and the sweeps below deliberately do not attempt them:

      - A bare name in prose: "measured on OMEGA". There is no shape to match, because the
        token is just a word. The nearest pattern available -- capitalised prose tokens --
        cannot tell a name from an acronym or from this codebase's own emphasis caps, so its
        allowlist would have to be the English language.

      - A name standing as the value of an identity-bearing fixture field. Also no shape: those
        fields hold free-form descriptive strings by design, which is GOOD test naming and worth
        keeping, and a name reads as one more of them.

    Hashing a denylist rescues neither case. A hostname slug is a short token drawn from a small
    alphabet, so its whole keyspace enumerates in microseconds: a hash committed to a public repo
    publishes what it stands for. Moving the salt to a CI secret trades that for a guard that
    silently no-ops on every fork and outside contribution, which is exactly the "green for the
    wrong reason" failure the scanned-count assert exists to catch. So those two categories stay a
    review matter, and what is left below is a tripwire rather than a proof.

    Deliberately not written down here: any inventory of how much ordinary prose the unreachable
    shapes would match, and any distinguishing property of the names this guard exists to keep out.
    Neither is needed to maintain the guard, both are useful only to someone working out where it
    does not look, and this file is public. Resist the urge to helpfully re-add them.
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

    /* Generic role words that legitimately carry an ordinal. Measured off the tree, not
       guessed: these are every non-slug word that precedes a zero-padded ordinal anywhere in
       it, and not one of them could be a customer -- each names a role (box, srv, web, target,
       bench), a product (sql, aurora), an environment (prod, dev, local), or a test state
       (gone). Deliberately SEPARATE from SyntheticSlugs: a word added here must not quietly
       widen the slug position of the full-hostname sweep, which is the stricter of the two. */
    private static readonly HashSet<string> OrdinalRoleWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "prod", "dev", "local", "sql", "aurora", "srv", "box", "web", "target", "bench", "gone",
    };

    /*
        A server gets named by its short name and ordinal too -- omega-01, not
        prod-sql-use1-omega-01 -- and InstanceName cannot see that form: it requires the
        service prefix and both middle segments. #2490 and #2900 swept the full-hostname shape
        only, #2903 closed this one by hand, and the sweep below is what makes it automatic.

        The ZERO PADDING is what makes the shape checkable at all. Prose writes a measurement
        as top-25, sev-10, pre-18, phase-2, and never pads one to two digits; a fleet pads
        every ordinal. Demanding a padded ordinal rather than any number at all collapses the
        false-positive vocabulary from 114 words (985 occurrences) to 11 (103),
        and demanding a purely alphabetic word drops the escape-sequence and version debris
        with it (n2026-08 out of a literal "\n2026-08", krb5-2, w1f-2).

        The ceiling follows from that padding, as a property of the shape rather than a bet on
        how a fleet numbers its boxes: padding is only observable below ten. A two-digit ordinal
        needs none, so it is shape-identical to the two-digit measurements prose already writes,
        and widening the ordinal does not extend the discriminator -- it spends it. Re-measured
        on this tree: allowing any two-digit ordinal doubles the false-positive vocabulary, 11
        words to 28 and 103 occurrences to 204, and every word it adds is a measurement or a
        version number rather than a name. So this reaches -00 through -09 by construction; a
        short name first published above that is out of reach, and stays a review matter like
        the two shapes at the top of the file. Widen the shape only against a measurement that
        says the trade has changed, and prefer tightening it over lengthening either allowlist.
    */
    private static readonly Regex ShortNameOrdinal = new(
        @"\b(?<slug>[a-z]{3,})-0[0-9]\b",
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

    [Fact]
    public void NoTrackedSourceFileNamesAServerByShortNameAndOrdinal()
    {
        var offenders = new List<string>();
        var scanned = 0;
        var root = RepoRoot();

        foreach (var file in TrackedSourceFiles())
        {
            scanned++;
            var text = File.ReadAllText(file);
            foreach (Match m in ShortNameOrdinal.Matches(text))
            {
                var slug = m.Groups["slug"].Value;
                if (SyntheticSlugs.Contains(slug) || OrdinalRoleWords.Contains(slug))
                {
                    continue;
                }

                var line = text.Take(m.Index).Count(c => c == '\n') + 1;
                var relative = Path.GetRelativePath(root, file);
                offenders.Add($"{relative}:{line}  {m.Value}  (short name '{slug}')");
            }
        }

        /* Same reason as above: a sweep that stops reaching the tree passes vacuously. */
        Assert.True(scanned > 200, $"only scanned {scanned} files -- the sweep lost the tree");

        Assert.True(
            offenders.Count == 0,
            "This repository is public. These read as a server's short name followed by its "
          + "ordinal, so they may name a real tenant even though they are not full hostnames. "
          + "Rename the short name to a synthetic slug, or -- if the word names a role rather "
          + "than a server -- add it to OrdinalRoleWords:"
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
