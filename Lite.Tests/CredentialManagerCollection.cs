/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The one collection for every Lite test class that reads or writes the real Windows Credential Manager (#5785).
/// Every such test already uses a target name of its own (a new GUID per test), so no two tests share an entry. That
/// is not enough: when one thread writes and deletes its own entries, Windows can answer another thread's read of a
/// DIFFERENT entry, written a moment before, with ERROR_NOT_FOUND (1168). Eight threads that each wrote, read back
/// and deleted 200 unique targets lost 3 to 6 of 1,600 reads to that error; one thread lost none of 400. So the
/// classes cannot be isolated by name, only by time: xunit runs a <see cref="CollectionDefinitionAttribute.DisableParallelization"/>
/// collection on its own, after the parallel classes have finished, one test at a time.
///
/// <para>Running alone is stricter than the <c>app-alert-statics</c> collection two members came from, so the App
/// statics those two write stay guarded. <see cref="CredentialManagerCollectionCensusTests"/> keeps new classes in.</para>
/// </summary>
[CollectionDefinition("credential-manager", DisableParallelization = true)]
public sealed class CredentialManagerCollection
{
    public const string Name = "credential-manager";
}

/// <summary>
/// #5785: a Lite test class that reaches the real Credential Manager outside <see cref="CredentialManagerCollection"/>
/// races every other one, because Windows can lose a read of one entry while another thread writes or deletes a
/// different one. The scan looks for the calls that reach the real store from a test.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CredentialManagerCollectionCensusTests
{
    /* The calls that reach the real Credential Manager from a test: a real CredentialService or CredentialStore, the one a
       ServerManager or ProfileManager owns, App's webhook and SMTP helpers, and the app's route-secret access. The last
       alternative covers C#'s target-typed new(...): a CredentialStore or CredentialService type mention followed, in the
       same declaration, by new (. Singleline is set for that alternative's multi-line declarations. */
    private static readonly Regex s_realStore = new(
        @"new CredentialService\(|new CredentialStore\(|\.CredentialService\b|App\.(SaveWebhookUrl|GetWebhookUrl|GetSmtpPassword|SaveSmtpPassword)\(|RouteSecretAccess\.CredentialManager\(|\b(CredentialStore|CredentialService)\b[^;{}]{0,160}?\bnew\s*\(",
        RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private const string Membership = "[Collection(\"credential-manager\")]";

    /* The seven classes the census found when it was written; fewer means the scan stopped seeing them. */
    private const int MemberFloor = 7;

    [Fact]
    public void EveryClassThatReachesTheRealCredentialManager_RunsInTheCredentialManagerCollection()
    {
        var thisFile = Path.GetFileName(ThisFile());
        var root = TestsRoot();
        /* Subfolders are scanned too (Helpers/ holds .cs files); build output under bin/ and obj/ is not source. */
        var reaching = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(f => !string.Equals(Path.GetFileName(f), thisFile, StringComparison.OrdinalIgnoreCase))
            .Where(f => !Path.GetRelativePath(root, f).Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .SkipLast(1).Any(seg => seg.Equals("bin", StringComparison.OrdinalIgnoreCase) || seg.Equals("obj", StringComparison.OrdinalIgnoreCase)))
            .Select(f => (File: Path.GetRelativePath(root, f), Text: File.ReadAllText(f)))
            .Where(f => s_realStore.IsMatch(f.Text))
            .ToList();

        var outside = reaching
            .Where(f => !f.Text.Contains(Membership, StringComparison.Ordinal))
            .Select(f => f.File)
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            outside.Count == 0,
            "A Lite.Tests class reaches the real Windows Credential Manager outside the credential-manager collection (#5785). "
          + "Windows can lose a read of one entry while another thread writes or deletes a different one, so a unique target "
          + "name does not isolate it. Add " + Membership + " above the class:\n  " + string.Join("\n  ", outside));

        Assert.True(
            reaching.Count >= MemberFloor,
            $"only {reaching.Count} Lite.Tests files reach the real Credential Manager; the scan has stopped seeing them");
    }

    [Fact]
    public void TheCredentialManagerCollection_IsNotParallel_AndIsTheOnlyOneOfItsName()
    {
        var definitions = typeof(CredentialManagerCollectionCensusTests).Assembly.GetTypes()
            .Select(t => t.GetCustomAttribute<CollectionDefinitionAttribute>())
            .Where(a => a is not null && string.Equals(a.Name, CredentialManagerCollection.Name, StringComparison.Ordinal))
            .ToList();

        Assert.Single(definitions);
        Assert.True(definitions[0]!.DisableParallelization, "the credential-manager collection must run alone, or it serializes nothing");
    }

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;

    private static string TestsRoot() => Path.GetDirectoryName(ThisFile())!;
}
