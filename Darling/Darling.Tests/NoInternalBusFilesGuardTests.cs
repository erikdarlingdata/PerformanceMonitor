using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5167's squash committed two files from the agent coordination bus into this repository, and
/// this repository is PUBLIC. Scrubcheck found no customer, tenant or host names in them, but they
/// carried internal vocabulary and the layout of a private tree, and once on <c>dev</c> they merge
/// into every branch and into every seat's working copy.
///
/// <para>The cause was a path, not a judgement: a lane brief named a bus path that was not
/// absolute, and a relative path resolves inside the CODE worktree, so a report written "to the
/// bus" landed here. <c>.gitignore</c> now stops that accident. This test is what catches a FORCED
/// add, which <c>.gitignore</c> cannot, and it fails loudly enough to explain itself to whoever
/// trips it.</para>
///
/// <para>Deliberately in Darling.Tests rather than Lite.Tests: Lite.Tests cannot build or run on a
/// Mac, so a guard placed there could not be verified by the seat that needs it most.</para>
/// </summary>
public class NoInternalBusFilesGuardTests
{
    /* Nothing from the coordination bus belongs in this tree: lane reports, design notes, seat
       state and inbox/outbox messages all live in the separate private repository. */
    private static readonly string[] BusDirectories = { "design", "inbox", "outbox" };

    [Fact]
    public void NoDirectoryFromTheAgentBus_IsPresentInThisRepository()
    {
        var root = RepoRoot();
        var found = BusDirectories
            .Select(d => Path.Combine(root, d))
            .Where(Directory.Exists)
            .SelectMany(d => Directory.EnumerateFiles(d, "*", SearchOption.AllDirectories))
            .Select(f => Path.GetRelativePath(root, f).Replace(Path.DirectorySeparatorChar, '/'))
            .OrderBy(f => f, System.StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            found.Length == 0,
            "This repository is PUBLIC, and these tracked files belong to the PRIVATE agent "
                + "coordination bus:\n  "
                + string.Join("\n  ", found)
                + "\n\nRemove them with `git rm -r` and keep bus writes out of the code tree. If a "
                + "lane brief told you to write a report to a bus path, that path MUST be absolute: "
                + "a relative path resolves inside the code worktree, which is exactly how #5167 "
                + "leaked two files into this public repository.");
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
