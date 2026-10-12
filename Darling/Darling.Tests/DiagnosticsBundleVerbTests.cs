/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the <c>--diagnostics-bundle</c> verb's recognition, usage and argument errors.</summary>
public sealed class DiagnosticsBundleVerbTests
{
    [Theory]
    [InlineData("--diagnostics-bundle")]
    [InlineData("--DIAGNOSTICS-BUNDLE")]
    [InlineData("--diag-bundle")]
    [InlineData("--Diag-Bundle")]
    public void IsDiagnosticsBundleVerb_AcceptsBothSpellingsInAnyCase(string arg)
    {
        Assert.True(DarlingCliCommands.IsDiagnosticsBundleVerb(arg));
        Assert.True(DarlingCliCommands.IsKnownVerb(arg));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { arg, "x.json" }));
    }

    [Theory]
    [InlineData("--diagnostics")]
    [InlineData("--diag")]
    [InlineData("diagnostics-bundle")]
    [InlineData("--diagnostics-bundles")]
    [InlineData("")]
    public void IsDiagnosticsBundleVerb_RejectsNearMisses(string arg)
    {
        Assert.False(DarlingCliCommands.IsDiagnosticsBundleVerb(arg));
    }

    [Fact]
    public void UsageText_NamesTheVerb()
    {
        Assert.Contains("--diagnostics-bundle <path>", DarlingCliCommands.UsageText(), StringComparison.Ordinal);
    }

    [Fact]
    public void ProgramDispatch_HasABlockForTheVerb()
    {
        var program = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Program.cs").ReplaceLineEndings("\n");
        Assert.Contains("DarlingCliCommands.IsDiagnosticsBundleVerb(args[0])", program, StringComparison.Ordinal);
        Assert.Contains("DarlingCliCommands.DiagnosticsBundleAsync(args.Skip(1).ToArray()", program, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseArgs_ReadsEveryOption_AndAppendsJson()
    {
        var (options, error) = DiagnosticsBundle.ParseArgs(new[] { "out", "--hours", "48", "--server", "alpha-01", "--log-dir", "/x", "--config", "/c.json", "--alias-map", "/m.txt", "--force" });
        Assert.Null(error);
        Assert.NotNull(options);
        Assert.Equal("out.json", options!.OutputPath);
        Assert.Equal(48, options.Hours);
        Assert.Equal("alpha-01", options.ServerName);
        Assert.Equal("/x", options.LogDirectory);
        Assert.Equal("/c.json", options.ConfigPath);
        Assert.Equal("/m.txt", options.AliasMapPath);
        Assert.True(options.Force);
        Assert.Equal(DiagnosticsBundle.DefaultHours, DiagnosticsBundle.ParseArgs(new[] { "o.txt" }).Options!.Hours);
        Assert.Equal("o.txt", DiagnosticsBundle.ParseArgs(new[] { "o.txt" }).Options!.OutputPath);
    }

    public static TheoryData<string> BadArgumentLines => new()
    {
        "",
        "a.json --hours 0",
        "a.json --hours 169",
        "a.json --hours abc",
        "a.json --hours",
        "a.json b.json",
        "a.json --nope",
        "a.json --server",
    };

    [Theory]
    [MemberData(nameof(BadArgumentLines))]
    public async Task ArgumentErrors_ExitOne_AndWriteNothing(string line)
    {
        var args = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var root = Directory.CreateTempSubdirectory("darling-bundle-args-");
        try
        {
            var rooted = args.Select(a => a.EndsWith(".json", StringComparison.Ordinal) ? Path.Combine(root.FullName, a) : a).ToArray();
            var exit = await DarlingCliCommands.DiagnosticsBundleAsync(rooted, new StringWriter(), new StringWriter(), CancellationToken.None);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.ConfigError, exit);
            Assert.Empty(root.GetFiles());
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void ExitCodes_AreTheDocumentedSix()
    {
        Assert.Equal(new[] { 0, 1, 2, 3, 4, 5 }, new[]
        {
            DarlingCliCommands.DiagnosticsBundleExitCode.Ok, DarlingCliCommands.DiagnosticsBundleExitCode.ConfigError,
            DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, DarlingCliCommands.DiagnosticsBundleExitCode.PartialBundle,
            DarlingCliCommands.DiagnosticsBundleExitCode.OutputError, DarlingCliCommands.DiagnosticsBundleExitCode.LeakGuard,
        });
    }
}
