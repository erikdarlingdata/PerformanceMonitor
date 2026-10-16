/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The CI quarantine (#5942): <c>ci-select.py quarantine</c> decides which entries of .github/ci-quarantine.txt a pull
/// request honors, and <c>quarantine-args</c> turns the decision into the runner arguments that leave them out.
///
/// <para><b>Why.</b> A test that dev's own push broke fails every unrelated pull request until a fix merges. The list
/// sets such a test aside, but a wrong entry hides a real regression, so every doubt must fail open: the test runs. These
/// cases pin that rule on fixture files and a fixed clock: an honored entry leaves its test out, and an expired entry,
/// an entry the latest dev run did not fail, an entry the pull request reaches, an unreadable dev result and an unusable
/// selection each leave it in. One case chains the real <c>map-select</c> into <c>quarantine</c>, so a rename of a
/// selection reason cannot quietly turn the reach check off.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CiSelectQuarantineGuardTests : IDisposable
{
    private const string Test = "Lite.Tests.OwnTests.Runs";
    private static readonly DateTime Now = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ci-quarantine-" + Guid.NewGuid().ToString("N"));

    public CiSelectQuarantineGuardTests()
    {
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private static string Script() => Path.Combine(ParitySource.RepoRoot(), ".github", "scripts", "ci-select.py");

    private static string Line(string name, DateTime at, string issue = "#5999") =>
        $"{name} | 38903116863 | {at.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture)} | {issue}";

    private string Write(string name, params string[] lines)
    {
        var path = Path.Combine(_dir, name);
        File.WriteAllLines(path, lines);
        return path;
    }

    private string Selection(string? reason, bool full = false)
    {
        var why = reason is null ? "{}" : "{\"lite\": {\"OwnTests\": \"" + reason + "\", \"OtherTests\": \"covers a changed file\"}}";
        return Write("selection-" + Guid.NewGuid().ToString("N") + ".json",
            "{\"full\": " + (full ? "true" : "false") + ", \"why\": " + why + "}");
    }

    private (int ExitCode, string Output) Python(params string[] args)
    {
        var psi = new ProcessStartInfo("python")
        {
            WorkingDirectory = ParitySource.RepoRoot(),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add(Script());
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi) ?? throw new InvalidOperationException("python did not start");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        Assert.True(p.WaitForExit(120_000), "ci-select.py did not finish in two minutes");
        return (p.ExitCode, stdout.Result + stderr.Result);
    }

    /// <summary>Runs <c>quarantine</c> at the fixed clock and returns the honored test names and the ignored ones with their reasons.</summary>
    private (List<string> Honored, Dictionary<string, string> Ignored, string Out) Decide(
        string file, string? devFailures, string? selection)
    {
        var outPath = Path.Combine(_dir, "decided-" + Guid.NewGuid().ToString("N") + ".json");
        var args = new List<string> { "quarantine", "--file", file, "--now", Now.ToString("o", CultureInfo.InvariantCulture), "--out", outPath };
        if (devFailures is not null)
        {
            args.Add("--dev-failures-file");
            args.Add(devFailures);
        }

        if (selection is not null)
        {
            args.Add("--selection");
            args.Add(selection);
        }

        var (exit, output) = Python([.. args]);
        Assert.True(exit == 0, "quarantine failed:\n" + output);
        using var doc = JsonDocument.Parse(File.ReadAllText(outPath));
        var honored = doc.RootElement.GetProperty("honored").EnumerateArray().Select(h => h.GetProperty("name").GetString() ?? "").ToList();
        var ignored = doc.RootElement.GetProperty("ignored").EnumerateArray()
            .ToDictionary(i => i.GetProperty("name").GetString() ?? "", i => i.GetProperty("reason").GetString() ?? "", StringComparer.Ordinal);
        return (honored, ignored, outPath);
    }

    private string DevFailed(params string[] names) => Write("dev-" + Guid.NewGuid().ToString("N") + ".txt", names);

    [Fact]
    public void HonoredEntry_LeavesItsTestOut_OfTheRunnerArguments()
    {
        var file = Write("q.txt", "# a comment", "", Line(Test, Now.AddHours(-5)));
        var (honored, ignored, outPath) = Decide(file, DevFailed(Test, "Lite.Tests.Other.Failing"), Selection(null));

        Assert.Equal([Test], honored);
        Assert.Empty(ignored);
        var (exit, args) = Python("quarantine-args", "--quarantine", outPath, "--suite", "lite");
        Assert.Equal(0, exit);
        Assert.Equal(["-method-", Test], args.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToArray());
        Assert.Equal(string.Empty, Python("quarantine-args", "--quarantine", outPath, "--suite", "darling").Output.Trim());
    }

    [Fact]
    public void AnEntryRunsWhenItIsOlderThanSeventyTwoHours_AndNotBefore()
    {
        var file = Write("q.txt", Line(Test, Now.AddHours(-73)), Line("Lite.Tests.OtherTests.Runs", Now.AddHours(-71)));
        var (honored, ignored, _) = Decide(file, DevFailed(Test, "Lite.Tests.OtherTests.Runs"), Selection(null));

        Assert.Equal(["Lite.Tests.OtherTests.Runs"], honored);
        Assert.Contains("expired", ignored[Test], StringComparison.Ordinal);
    }

    [Fact]
    public void AnEntryTheLatestDevRunDidNotFail_IsIgnored()
    {
        var file = Write("q.txt", Line(Test, Now.AddHours(-1)));
        var (honored, ignored, _) = Decide(file, DevFailed("Lite.Tests.SomethingElse.Fails"), Selection(null));

        Assert.Empty(honored);
        Assert.Contains("did not fail this test", ignored[Test], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("own file changed")]
    [InlineData("covers a changed file")]
    [InlineData("text pattern")]
    [InlineData("names a changed symbol")]
    [InlineData("shares a collection with a changed class")]
    public void APullRequestThatReachesTheTest_RunsIt(string reason)
    {
        var file = Write("q.txt", Line(Test, Now.AddHours(-1)));
        var (honored, ignored, _) = Decide(file, DevFailed(Test), Selection(reason));

        Assert.Empty(honored);
        Assert.Contains("reaches it", ignored[Test], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("reads the tree")]
    [InlineData("guard stage")]
    public void ATestSelectedForEveryChange_IsStillQuarantinedOnAnUnrelatedPullRequest(string reason)
    {
        var file = Write("q.txt", Line(Test, Now.AddHours(-1)));
        var (honored, _, _) = Decide(file, DevFailed(Test), Selection(reason));

        Assert.Equal([Test], honored);
    }

    [Fact]
    public void WhenAnythingIsUnknown_TheTestRuns()
    {
        var file = Write("q.txt", Line(Test, Now.AddHours(-1)), "this line is not an entry");

        // The dev run's failures could not be read (the Actions API failed): fail open.
        var (honored, ignored, _) = Decide(file, Path.Combine(_dir, "no-such-file.txt"), Selection(null));
        Assert.Empty(honored);
        Assert.Contains("could not be read", ignored[Test], StringComparison.Ordinal);
        Assert.Contains(ignored.Keys, k => k.StartsWith("this line", StringComparison.Ordinal));

        // No selection, an unreadable one, and a FULL one say nothing about what the pull request reaches.
        foreach (var selection in new string?[] { null, Write("bad.json", "not json"), Selection(null, full: true) })
        {
            var (h, i, _) = Decide(file, DevFailed(Test), selection);
            Assert.Empty(h);
            Assert.Contains("no usable test map selection", i[Test], StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AnEmptyOrMissingFile_HonorsNothing_AndNeverFails()
    {
        Assert.Empty(Decide(Write("empty.txt"), null, null).Honored);
        Assert.Empty(Decide(Path.Combine(_dir, "missing.txt"), null, null).Honored);
    }

    [Fact]
    public void TheRealMapSelection_NamesTheReason_ThatTheReachCheckReads()
    {
        // A tree of two test classes and a map: Other.cs covers Lite/B.cs, Own.cs covers Lite/A.cs.
        var tests = Path.Combine(_dir, "tree", "Lite.Tests");
        Directory.CreateDirectory(tests);
        const string fact = "\n{\n    [Fact]\n    public void Runs() { }\n}\n";
        File.WriteAllText(Path.Combine(tests, "Own.cs"), "public class OwnTests" + fact);
        File.WriteAllText(Path.Combine(tests, "Other.cs"), "public class OtherTests" + fact);
        var map = Write("map.json", "{\"schema\": 1, \"sha\": \"0000000\", \"built_at\": \"" + DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture) + "\", \"tool\": \"synthetic\",",
            " \"files\": [\"Lite.Tests/Own.cs\", \"Lite/A.cs\", \"Lite.Tests/Other.cs\", \"Lite/B.cs\"],",
            " \"classes\": {\"lite\": {\"OwnTests\": {\"own\": 0, \"files\": [1], \"seconds\": 1.0},",
            "                       \"OtherTests\": {\"own\": 2, \"files\": [3], \"seconds\": 1.0}}},",
            " \"text_patterns\": {}}");
        var file = Write("q.txt", Line(Test, Now.AddHours(-1)));

        string SelectionFor(string changed)
        {
            var path = Path.Combine(_dir, "sel-" + Guid.NewGuid().ToString("N") + ".json");
            var (exit, output) = Python("map-select", "--map", map, "--root", Path.Combine(_dir, "tree"), "--drift-file", "-", "--out", path, changed);
            Assert.True(exit == 0, output);
            using var doc = JsonDocument.Parse(File.ReadAllText(path));
            Assert.False(doc.RootElement.GetProperty("full").GetBoolean(), "map-select answered FULL:\n" + output);
            return path;
        }

        // Lite/B.cs reaches OtherTests only: the unrelated pull request leaves the quarantined test out.
        Assert.Equal([Test], Decide(file, DevFailed(Test), SelectionFor("Lite/B.cs")).Honored);
        // Lite/A.cs is covered by OwnTests, and the test's own file is OwnTests: both pull requests run it.
        Assert.Empty(Decide(file, DevFailed(Test), SelectionFor("Lite/A.cs")).Honored);
        Assert.Empty(Decide(file, DevFailed(Test), SelectionFor("Lite.Tests/Own.cs")).Honored);
    }

    [Fact]
    public void TheDevLogReader_TakesTheFailedTestsAndLeavesTheArgumentsOff()
    {
        // The runner prints `<time>Z     Namespace.Class.Method [FAIL]`; a theory case adds its arguments in parentheses.
        var log = Write("job.log",
            "2026-10-16T13:00:00.3819926Z     Lite.Tests.OwnTests.Runs [FAIL]",
            "2026-10-16T13:00:00.3819927Z     Lite.Tests.OwnTests.Theory(a: 1, b: \"x [FAIL]\") [FAIL]",
            "2026-10-16T13:00:00.3819928Z       Assert.Equal() Failure [FAIL] in the middle of a message",
            "2026-10-16T13:00:00.3819929Z     Lite.Tests.OwnTests.Passes [PASS]");
        var psi = new ProcessStartInfo("python")
        {
            WorkingDirectory = ParitySource.RepoRoot(),
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add("-c");
        psi.ArgumentList.Add("import importlib.util, sys; s = importlib.util.spec_from_file_location('cs', sys.argv[1]); m = importlib.util.module_from_spec(s); "
            + "s.loader.exec_module(m); print('\\n'.join(sorted(m.failed_tests_of_log(open(sys.argv[2], encoding='utf-8').read()))))");
        psi.ArgumentList.Add(Script());
        psi.ArgumentList.Add(log);
        using var p = Process.Start(psi) ?? throw new InvalidOperationException("python did not start");
        var stdout = p.StandardOutput.ReadToEnd();
        Assert.True(p.WaitForExit(120_000));
        Assert.Equal(["Lite.Tests.OwnTests.Runs", "Lite.Tests.OwnTests.Theory"],
            stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToArray());
    }
}
