using System;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5804: a release publishes nothing before every blocking check has passed. The Windows build job's release steps
/// assert that every published executable is signed; before #5804 the darling-linux job uploaded the Linux tarball and
/// pushed the ghcr <c>:latest</c> image in parallel with them, so a failed guard left a half-published release. These
/// pins hold the split: darling-linux only packages (read-only, no publish step), and the release-only
/// darling-linux-publish job needs both jobs and their success before it uploads, pushes, signs or attests.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class LinuxReleasePublishOrderTests
{
    private static readonly string[] PublishSteps =
    {
        "Upload linux artifact to the release",
        "Build and push container image (ghcr, versioned + latest)",
        "Install cosign",
        "Sign container image (keyless, Sigstore)",
        "Attest the linux tarball (GitHub provenance)",
    };

    private static string Yaml() => RepoFile.ReadRepoFileLf(".github", "workflows", "build.yml");

    private static string JobBlock(string yaml, string key)
    {
        var at = yaml.IndexOf($"\n  {key}:\n", StringComparison.Ordinal);
        Assert.True(at >= 0, $"build.yml has no job `{key}`");

        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest[(key.Length + 4)..], "\n  [A-Za-z0-9_-]+:\n");
        return next.Success ? "\n" + rest[..(key.Length + 4 + next.Index)] : "\n" + rest;
    }

    [Fact]
    public void TheLinuxBuildJob_PublishesNothing_AndHasOnlyReadAccess()
    {
        var job = JobBlock(Yaml(), "darling-linux");

        foreach (var step in PublishSteps)
        {
            Assert.DoesNotContain($"- name: {step}\n", job, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("gh release upload", job, StringComparison.Ordinal);
        Assert.DoesNotContain("docker push", job, StringComparison.Ordinal);
        Assert.Contains("\n    permissions:\n      contents: read\n\n", job, StringComparison.Ordinal);
        Assert.Contains("name: darling-linux-release", job, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLinuxPublishJob_WaitsForTheWindowsBuildAndTheLinuxBuild()
    {
        var job = JobBlock(Yaml(), "darling-linux-publish");

        Assert.Contains("\n    needs: [build, darling-linux]\n", job, StringComparison.Ordinal);
        Assert.Contains(
            "\n    if: ${{ !cancelled() && github.event_name == 'release' && needs.build.result == 'success' "
            + "&& needs.darling-linux.result == 'success' }}\n",
            job,
            StringComparison.Ordinal);

        var at = PublishSteps.Select(step => job.IndexOf($"- name: {step}\n", StringComparison.Ordinal)).ToArray();
        Assert.All(at, index => Assert.True(index >= 0, "a Linux publish step is missing from darling-linux-publish"));
        Assert.Equal(at.OrderBy(index => index), at);

        var download = job.IndexOf("- name: Download the linux artifact\n", StringComparison.Ordinal);
        Assert.InRange(download, 0, at[0]);
    }

    [Fact]
    public void NoOtherJob_PublishesTheLinuxRelease()
    {
        var yaml = Yaml();
        var jobsAt = yaml.IndexOf("\njobs:\n", StringComparison.Ordinal);
        Assert.True(jobsAt >= 0, "build.yml has no jobs: block");
        var jobs = Regex.Matches(yaml[jobsAt..], "\n  ([a-z][a-z0-9-]*):\n").Select(m => m.Groups[1].Value).ToArray();
        Assert.Contains("darling-linux-publish", jobs);

        foreach (var key in jobs.Where(k => k != "darling-linux-publish"))
        {
            Assert.DoesNotContain("PerformanceMonitorDarling-linux-x64-*.tar.gz releases/SHA256SUMS-linux.txt", JobBlock(yaml, key), StringComparison.Ordinal);
            Assert.DoesNotContain("performancemonitor-darling\"\n", JobBlock(yaml, key), StringComparison.Ordinal);
        }
    }
}
