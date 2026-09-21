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
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every application that can set an Entra authentication mode on a connection string carries the package
/// that implements it. Derived from the tree — the projects, the auth modes their sources assign, and the
/// <c>ProjectReference</c> edges between them — rather than listed, so a third SKU or a new Entra call site
/// inherits the requirement instead of remembering it (#3838).
///
/// <para><b>The defect this is the census for.</b> <c>Microsoft.Data.SqlClient</c> 7.x split the Active
/// Directory authentication providers OUT of the core package into
/// <c>Microsoft.Data.SqlClient.Extensions.Azure</c>. Lite picked that package up when the 7.0 bump landed,
/// because Lite is where interactive Entra auth lived. Darling then grew non-interactive Entra auth
/// (service principal and managed identity, #3484) and the GUI for it (#3485) WITHOUT the package, so every
/// one of those connections opened with <c>Cannot find an authentication provider for
/// 'ActiveDirectoryServicePrincipal'</c> — a runtime failure, on a code path that compiles perfectly,
/// reported by an operator rather than by CI.</para>
///
/// <para><b>Why a package census and not a connection test.</b> The driver resolves the provider by the
/// assembly being PRESENT in the running application's dependency closure; there is no registration call to
/// assert was made and no startup wiring to pin. Measured against 7.0.3 rather than assumed:
/// <c>SqlAuthenticationProvider.GetProvider</c> returns null for all four AAD methods with the core package
/// alone and <c>ActiveDirectoryAuthenticationProvider</c> for all four with the extension package added, no
/// other change. So the only thing that decides whether these connections can work is a line in a project
/// file, and a project file is text this suite can read.</para>
///
/// <para><b>Two arms, because the fix and the requirement are at different levels.</b> The DIRECT arm holds
/// the SHIPPED projects that assign an Entra mode themselves: those declare the package directly, the way
/// the house declares any direct dependency rather than inheriting it. Test projects are subtracted there
/// and only there — they name the modes in assertions about the mapping and open nothing — and the
/// subtraction is stated at the call site rather than here, where it would be prose about a filter. The CLOSURE arm holds what actually governs
/// runtime — a connection opens inside whichever application is running, so the requirement belongs to every
/// project whose reference closure REACHES an Entra-assigning project, satisfied by the package being
/// anywhere in its own closure. The closure arm is what covers the Viewer and this test project without
/// demanding a reference neither of them uses: the Viewer opens no <c>SqlConnection</c> at all (its Test
/// Connection button enqueues a <c>test_connect</c> command the service executes), and it still ships the
/// service's assemblies beside it.</para>
///
/// <para><b><c>deprecated/</c> is out of scope.</b> Dashboard and Installer.Core carry the package already,
/// Installer does not, and nothing there is built or shipped. Including that tree would mean either a
/// standing exemption list or a demand on code nobody runs; the scope is what ships, and the exclusion is
/// asserted to be load-bearing below rather than left as a silent <c>Where</c>.</para>
///
/// <para><b>What this cannot see.</b> Whether the package RESOLVES — that is NuGet's business, and
/// <see cref="LockedModeRestoreCoverageTests"/> is what holds the lock files that pin it. Whether a mode is
/// reachable at runtime, as opposed to spelled in source: a mode assigned behind a feature gate nobody can
/// turn on still counts here, which is the safe direction. And an Entra mode reaching the driver as literal
/// connection-string TEXT (<c>Authentication=Active Directory Service Principal</c>) rather than through
/// <c>SqlConnectionStringBuilder.Authentication</c> — no call site spells it that way today, and the
/// detector's own ability to fire is exercised against synthetic text so a pattern that matched nothing
/// could not report the whole repository as safe.</para>
/// </summary>
public sealed class EntraProviderPackageCensusTests
{
    private const string CorePackage = "Microsoft.Data.SqlClient";

    private const string ProviderPackage = "Microsoft.Data.SqlClient.Extensions.Azure";

    /// <summary>
    /// An assignment of an Entra authentication mode. Keyed on the enum member rather than on the property
    /// name, because the member is what names a mode the split package implements: <c>SqlPassword</c> and
    /// <c>NotSpecified</c> are served by the core package and must not pull this requirement in.
    /// </summary>
    private static readonly Regex EntraModeAssignment = new(
        @"SqlAuthenticationMethod\.ActiveDirectory\w*", RegexOptions.Compiled);

    /// <summary>One <c>&lt;PackageReference Include="..." /&gt;</c>, versionless or not — central package
    /// management means the version is in <c>Directory.Packages.props</c> and the csproj carries the name
    /// alone, so the name is all this needs to match.</summary>
    private static readonly Regex PackageReference = new(
        @"<PackageReference\s+Include=""(?<id>[^""]+)""", RegexOptions.Compiled);

    /// <summary>One <c>&lt;ProjectReference Include="..." /&gt;</c>. MSBuild spells these with backslashes
    /// on every platform, so the path is normalised where it is resolved.</summary>
    private static readonly Regex ProjectReference = new(
        @"<ProjectReference\s+Include=""(?<path>[^""]+)""", RegexOptions.Compiled);

    /// <summary>
    /// The projects that assign an Entra mode in their OWN sources and therefore declare the provider
    /// package directly. Asserted as an exact set rather than a floor: a project joining this population is
    /// a new Entra call site, and it should have to say so here.
    /// </summary>
    [Fact]
    public void EveryProjectAssigningAnEntraMode_DeclaresTheProviderPackage()
    {
        var projects = Survey();

        /* TEST projects are subtracted, and the distinction is the point rather than a convenience: a test
           project names the enum member in ASSERTIONS about the mapping (Darling.Tests pins the
           service-principal mode DarlingConfig produces, Lite.Tests pins five of them), and it opens no
           connection to a monitored server at all. Requiring the package of them would be requiring it of
           code that cannot hit the failure. They are not thereby unexamined: both sit in the closure arm
           below, which is where a test project that DOES open such a connection would be answered. */
        var assigners = projects.Values
            .Where(p => p.AssignsEntraMode && !p.IsTestProject)
            .Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        /* The floor that makes the equality below mean something: an empty census satisfies an empty
           expectation, and this suite's whole subject is a population of two. */
        Assert.True(
            projects.Count >= 10,
            $"the census found only {projects.Count} projects under {Root} — it is not reading the "
          + "repository, so every assertion here would pass for a reason unrelated to the defect");

        Assert.Equal(
            new[] { "PerformanceMonitor.Darling.Service", "PerformanceMonitorLite" },
            assigners);

        foreach (var name in assigners)
        {
            var project = projects[name];

            /* Stated as part of the failure rather than asserted separately: a project that assigns an Entra
               mode without referencing the core driver at all would be a shape nobody has built, and the
               message should say which half is missing. */
            Assert.True(
                project.Packages.Contains(CorePackage, StringComparer.Ordinal),
                $"{name} assigns an Entra authentication mode but does not reference {CorePackage}");

            Assert.True(
                project.Packages.Contains(ProviderPackage, StringComparer.Ordinal),
                $"{name} assigns SqlAuthenticationMethod.ActiveDirectory* but does not reference "
              + $"{ProviderPackage}. The Entra providers left the core package in Microsoft.Data.SqlClient "
              + "7.x and the driver finds them by the assembly being present in the closure, so every "
              + "connection on that path opens with \"Cannot find an authentication provider\" — which is "
              + "#3838, reported from a service-principal target against a managed instance. The version is "
              + "pinned centrally already; the fix is one versionless PackageReference line");
        }
    }

    /// <summary>
    /// The runtime requirement: any project whose reference closure reaches an Entra-assigning project can
    /// host one of those connections, so the provider package has to be somewhere in its own closure. This
    /// is the arm that would have caught #3838 from the Viewer's and this test project's side too, and the
    /// arm that stays true if the direct reference ever moves to a shared library.
    /// </summary>
    [Fact]
    public void EveryProjectThatCanHostAnEntraConnection_ReachesTheProviderPackage()
    {
        var projects = Survey();
        var hosts = new List<string>();

        foreach (var project in projects.Values.OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            var closure = Closure(projects, project.Name);

            /* Resolved through the census rather than indexed into it: Closure carries the unresolvable
               MSBuild-property reference through as a NAME (see its header), so a lookup here has to be a
               TryGetValue. Indexing threw on generate-ladder-fixture, which is the one project in the tree
               whose reference is a property. */
            var reached = closure
                .Select(name => projects.TryGetValue(name, out var p) ? p : null)
                .Where(p => p is not null)
                .Select(p => p!)
                .ToArray();

            if (!reached.Any(p => p.AssignsEntraMode))
            {
                continue;
            }

            hosts.Add(project.Name);

            Assert.True(
                reached.Any(p => p.Packages.Contains(ProviderPackage, StringComparer.Ordinal)),
                $"{project.Name}'s project closure reaches a project that assigns an Entra authentication "
              + $"mode, but no project in that closure references {ProviderPackage} — so an Entra "
              + "connection opened while this application is running cannot find its provider (#3838)");
        }

        /* A floor, not an equality: this population grows every time anything references the service or
           Lite, and pinning it exactly would turn an ordinary reference edge into an edit here. What must
           not happen is the set emptying out, which is what a broken assigner detector looks like. */
        Assert.True(
            hosts.Count >= 4,
            $"only {hosts.Count} projects were found able to host an Entra connection. Both applications "
          + "and both test projects are in that closure, so a number this small means the assigner "
          + "detector matched nothing and every closure assertion above was skipped");
    }

    /// <summary>
    /// The two detectors fire on input that should trip them and stay quiet on input that should not — the
    /// assertion this suite rests on, since a pattern that matched nothing would report every project as
    /// compliant and do it in green.
    /// </summary>
    [Fact]
    public void TheDetectors_CanActuallyFire()
    {
        Assert.Matches(
            EntraModeAssignment,
            "builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryServicePrincipal;");
        Assert.Matches(
            EntraModeAssignment,
            "Authentication = SqlAuthenticationMethod.ActiveDirectoryManagedIdentity,");

        /* The modes the core package still serves on its own must NOT pull the requirement in, or the census
           would demand the provider package of every project that opens a SQL-auth connection. */
        Assert.DoesNotMatch(
            EntraModeAssignment, "builder.Authentication = SqlAuthenticationMethod.SqlPassword;");
        Assert.DoesNotMatch(
            EntraModeAssignment, "builder.Authentication = SqlAuthenticationMethod.NotSpecified;");

        Assert.Equal(
            ProviderPackage,
            PackageReference
                .Match($@"    <PackageReference Include=""{ProviderPackage}"" />")
                .Groups["id"].Value);

        /* Versioned too: the deprecated tree and any future VersionOverride spell it with attributes after
           the Include, and a detector that only knew the versionless shape would read those as absent. */
        Assert.Equal(
            CorePackage,
            PackageReference
                .Match($@"<PackageReference Include=""{CorePackage}"" Version=""7.0.3"" />")
                .Groups["id"].Value);
    }

    /// <summary>
    /// The <c>deprecated/</c> exclusion is doing work: that tree contains projects this census would
    /// otherwise judge. Asserted rather than left implicit, because an exclusion that excludes nothing reads
    /// as a decision while being a no-op — and if the tree is ever deleted, this says so instead of leaving
    /// a filter behind that nobody can evaluate.
    /// </summary>
    [Fact]
    public void TheDeprecatedTree_IsExcludedAndThatExclusionMatters()
    {
        var deprecated = Directory
            .EnumerateFiles(Path.Combine(Root, "deprecated"), "*.csproj", SearchOption.AllDirectories)
            .Where(NotUnderBuildOutput)
            .ToArray();

        Assert.NotEmpty(deprecated);

        Assert.DoesNotContain(
            "deprecated",
            Survey().Values.SelectMany(p => new[] { p.RelativePath }),
            StringComparer.Ordinal);

        /* The reason the exclusion is safe rather than convenient: at least one project in there references
           the core driver, so the tree is genuinely in this census's subject matter and is being left out on
           purpose. */
        Assert.Contains(
            deprecated,
            path => File.ReadAllText(path).Contains($@"Include=""{CorePackage}""", StringComparison.Ordinal));
    }

    /// <summary>Every shipped project, keyed by assembly-ish project name.</summary>
    private static Dictionary<string, ProjectFacts> Survey()
    {
        var projects = new Dictionary<string, ProjectFacts>(StringComparer.Ordinal);

        foreach (var path in Directory
            .EnumerateFiles(Root, "*.csproj", SearchOption.AllDirectories)
            .Where(NotUnderBuildOutput)
            .Where(p => !Relative(p).StartsWith("deprecated/", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal))
        {
            var text = File.ReadAllText(path);
            var directory = Path.GetDirectoryName(path)!;

            var packages = PackageReference
                .Matches(text)
                .Select(m => m.Groups["id"].Value)
                .ToHashSet(StringComparer.Ordinal);

            var references = ProjectReference
                .Matches(text)
                .Select(m => Path.GetFullPath(Path.Combine(
                    directory, m.Groups["path"].Value.Replace('\\', Path.DirectorySeparatorChar))))
                .Select(Path.GetFileNameWithoutExtension)
                .Select(n => n!)
                .ToArray();

            projects[Path.GetFileNameWithoutExtension(path)] = new ProjectFacts(
                Path.GetFileNameWithoutExtension(path),
                Relative(path),
                packages,
                references,
                AssignsEntraMode(directory),
                text.Contains("<IsTestProject>true</IsTestProject>", StringComparison.Ordinal));
        }

        return projects;
    }

    /// <summary>
    /// Whether any source file owned by this project assigns an Entra mode. Comments and strings are
    /// stripped first, through the shared walker: this very file quotes the enum member repeatedly in prose,
    /// and a raw read would make every project holding a discussion of the defect look like a call site of
    /// it.
    /// </summary>
    private static bool AssignsEntraMode(string projectDirectory) => Directory
        .EnumerateFiles(projectDirectory, "*.cs", SearchOption.AllDirectories)
        .Where(NotUnderBuildOutput)
        .Any(file => EntraModeAssignment.IsMatch(
            CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file))));

    /// <summary>The project itself plus every project reachable through <c>ProjectReference</c>. Iterative
    /// and visited-guarded rather than recursive: the graph is a DAG today, and a cycle should be a hang
    /// nobody debugs rather than a stack overflow.</summary>
    private static HashSet<string> Closure(Dictionary<string, ProjectFacts> projects, string start)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Stack<string>();
        pending.Push(start);

        while (pending.Count > 0)
        {
            var name = pending.Pop();

            /* A name absent from the census is skipped rather than dereferenced. One reference in the tree
               is an MSBuild PROPERTY (generate-ladder-fixture takes -p:StorageProject=... so it can build
               against an unpacked release), so its target is unknowable from text and there is nothing to
               resolve. Skipping is the safe direction: it can only make a closure SMALLER, and a smaller
               closure cannot manufacture a passing assertion — it can only fail to demand the package,
               which the direct arm demands of the assigning project anyway. */
            if (!seen.Add(name) || !projects.TryGetValue(name, out var project))
            {
                continue;
            }

            foreach (var reference in project.References)
            {
                pending.Push(reference);
            }
        }

        return seen;
    }

    /// <summary>bin/ and obj/ excluded the way the rest of this family excludes them: obj/ carries generated
    /// project files and sources, and a staged verification harness under bin/ would otherwise be swept as if
    /// it were shipped code.</summary>
    private static bool NotUnderBuildOutput(string path) =>
        !path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) &&
        !path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal);

    private static string Relative(string absolute) =>
        Path.GetRelativePath(Root, absolute).Replace(Path.DirectorySeparatorChar, '/');

    private sealed record ProjectFacts(
        string Name,
        string RelativePath,
        HashSet<string> Packages,
        string[] References,
        bool AssignsEntraMode,
        bool IsTestProject);
}
