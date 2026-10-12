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
/// Every test that mints a scratch database drops it before the test ends.
///
/// <para><b>The defect this closes.</b> Seven live test classes (28 tests) minted a scratch database with
/// <c>ScratchPostgres.CreateAsync</c> and disposed only their connection, so each database lived until the process-exit
/// drain. Since #5480 each drop unschedules the database's TimescaleDB jobs and waits for their workers, about 0.4
/// seconds a database on a CI runner, so the drain of those 29 databases took 11 seconds. The test runner (xunit.v3
/// under Microsoft.Testing.Platform) forces the process out with exit code 1 ("Foreground threads were left running,
/// forcing process exit") 10 seconds after the run returns, so the nightly's Darling PostgreSQL step failed with 28,763
/// tests passed, and its publish job was skipped. The drain was not a thread: it was the process-exit handler holding
/// the exit past the runner's wait.</para>
///
/// <para><b>What this pins.</b> A method that mints a scratch database, directly or through a helper that returns it,
/// disposes it in the same method (<c>await using</c> or <c>DisposeAsync</c>). A helper that returns the store and can
/// skip or throw after minting it disposes the store before it lets the skip or the failure out, since the caller
/// never owns a store the helper did not return. Source-level on purpose: a live check needs a PostgreSQL server, and
/// the databases a leak leaves are only counted after every test has finished.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ScratchDatabaseDisposalCensusTests
{
    /* Spelled in two parts so this file's own samples are not mistaken for a test that mints a database. */
    private const string CreateCall = "ScratchPostgres" + ".CreateAsync(";

    private static readonly Regex MemberStart = new(
        @"^    (?:\[[^\]]*\]\s*)*(?:public|private|internal|protected|static|async)\b",
        RegexOptions.Compiled);

    private static readonly Regex DisposeCall = new(@"\b\w*[sS]cratch\w*\??\.DisposeAsync\(", RegexOptions.Compiled);

    [Fact]
    public void NoTestLeavesAScratchDatabaseForTheExitDrain()
    {
        var directory = FindTestProjectDirectory();
        Assert.NotNull(directory);

        var leaks = new List<string>();
        var scanned = 0;
        foreach (var file in Directory.EnumerateFiles(directory!, "*.cs", SearchOption.TopDirectoryOnly).OrderBy(f => f, StringComparer.Ordinal))
        {
            if (Path.GetFileName(file) is "ScratchDatabaseDisposalCensusTests.cs" or "ScratchPostgres.cs")
            {
                continue;
            }

            var source = File.ReadAllText(file);
            if (!source.Contains(CreateCall, StringComparison.Ordinal))
            {
                continue;
            }

            scanned++;
            leaks.AddRange(FindLeaks(source).Select(method => $"{Path.GetFileName(file)}: {method}"));
        }

        Assert.True(scanned > 100, $"the census read {scanned} files that mint a scratch database; the walk to the test sources is broken");
        Assert.True(
            leaks.Count == 0,
            "These methods mint a scratch database and never drop it, so it waits for the process-exit drain, which the test runner "
            + "cuts off after 10 seconds (exit code 1 with every test passed). Put `await using var scratch = ...` on it, or call "
            + "`await scratch.DisposeAsync()` in the finally:" + Environment.NewLine + string.Join(Environment.NewLine, leaks));
    }

    [Fact]
    public void TheDetector_FlagsAMethodThatMintsAndNeverDisposes()
    {
        var leak = $@"
public sealed class Sample
{{
    [Fact]
    public async Task Leaks()
    {{
        var scratch = await {CreateCall}cs, default);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
    }}
}}";
        Assert.Equal(new[] { "Leaks" }, FindLeaks(leak));
    }

    [Fact]
    public void TheDetector_FlagsAHelperTestThatOnlyDisposesItsConnection()
    {
        /* The shape the seven classes had: the opener returns the store, and the test's finally closed the connection. */
        var leak = $@"
public sealed class Sample
{{
    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {{
        var scratch = await {CreateCall}cs, default);
        return (new NpgsqlConnection(scratch.ConnectionString), scratch);
    }}

    [Fact]
    public async Task Leaks()
    {{
        var (connection, scratch) = await OpenAsync();
        try
        {{
        }}
        finally
        {{
            await connection.DisposeAsync();
        }}
    }}
}}";
        Assert.Equal(new[] { "Leaks" }, FindLeaks(leak));
    }

    [Fact]
    public void TheDetector_FlagsAnOpenerThatCanSkipAfterMintingAndDoesNotDispose()
    {
        var leak = $@"
public sealed class Sample
{{
    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {{
        var scratch = await {CreateCall}cs, default);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        Assert.SkipWhen(true, ""needs TimescaleDB"");
        return (connection, scratch);
    }}
}}";
        Assert.Equal(new[] { "OpenAsync" }, FindLeaks(leak));
    }

    [Fact]
    public void TheDetector_AcceptsEveryWayTheRepoDisposesAStore()
    {
        var clean = $@"
public sealed class Sample
{{
    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {{
        var scratch = await {CreateCall}cs, default);
        NpgsqlConnection? connection = null;
        try
        {{
            connection = new NpgsqlConnection(scratch.ConnectionString);
            Assert.SkipWhen(true, ""needs TimescaleDB"");
            return (connection, scratch);
        }}
        catch
        {{
            await scratch.DisposeAsync();
            throw;
        }}
    }}

    private static async Task<ScratchPostgres> SeedAsync(string cs) => await {CreateCall}cs, default);

    [Fact]
    public async Task DisposesInFinally()
    {{
        var (connection, scratch) = await OpenAsync();
        try {{ }}
        finally
        {{
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
        }}
    }}

    [Fact]
    public async Task UsingDeclaration()
    {{
        await using var scratch = await {CreateCall}cs, default);
    }}

    [Fact]
    public async Task UsingDeclarationThroughAHelper()
    {{
        await using var store = await SeedAsync(""cs"");
    }}

    [Fact]
    public async Task UsingAnAlias()
    {{
        var scratch = await SeedAsync(""cs"");
        await using var _ = scratch;
    }}
}}";
        Assert.Empty(FindLeaks(clean));
    }

    /// <summary>The methods in <paramref name="source"/> that mint a scratch database and do not drop it, by name.</summary>
    internal static IReadOnlyList<string> FindLeaks(string source)
    {
        source = Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        source = Regex.Replace(source, @"//[^\r\n]*", string.Empty);

        var chunks = SplitIntoMembers(source);

        /* A helper whose return type carries the store, or a small handle type whose own DisposeAsync drops it, hands
           the store to its caller, who then owns the drop. */
        var owners = new List<string> { "ScratchPostgres" };
        owners.AddRange(FindHandleTypes(source));
        var ownerType = new Regex(@"\b(?:Task|ValueTask)<[^;{]*\b(?:" + string.Join("|", owners.Select(Regex.Escape)) + @")\b");
        var creators = chunks
            .Where(c => ownerType.IsMatch(ReturnType(c)))
            .Select(c => c.Name)
            .ToHashSet(StringComparer.Ordinal);

        var lifetimeDropsTheStore = chunks.Any(c => c.Name == "DisposeAsync" && DisposeCall.IsMatch(c.Text));
        var creatorCall = creators.Count == 0 ? null : new Regex(@"(?<![\w.])(?:" + string.Join("|", creators.Select(Regex.Escape)) + @")\s*\(");

        var leaks = new List<string>();
        foreach (var chunk in chunks)
        {
            var body = chunk.Text[FirstLine(chunk.Text).Length..];
            var minted = body.Contains(CreateCall, StringComparison.Ordinal);
            var calledAHelper = creatorCall is not null && creatorCall.IsMatch(body);
            if (!minted && !calledAHelper)
            {
                continue;
            }

            if (creators.Contains(chunk.Name))
            {
                /* The helper itself: only a skip or a throw after minting is a leak, because the caller never got the store. */
                var afterMint = minted ? body[body.IndexOf(CreateCall, StringComparison.Ordinal)..] : body;
                if (afterMint.Contains("Skip", StringComparison.Ordinal) && !DisposeCall.IsMatch(afterMint))
                {
                    leaks.Add(chunk.Name);
                }

                continue;
            }

            if (chunk.Name == "InitializeAsync" && lifetimeDropsTheStore)
            {
                continue;
            }

            if (!Disposes(body, creators))
            {
                leaks.Add(chunk.Name);
            }
        }

        return leaks;
    }

    /// <summary>The nested types whose <c>DisposeAsync</c> drops a scratch database: a test hands one out and disposes it as a unit.</summary>
    private static IEnumerable<string> FindHandleTypes(string source)
    {
        var typeDeclaration = new Regex(@"^\s+(?:(?:public|private|internal|protected|sealed|static|readonly|file|partial)\s+)*(?:class|record\s+struct|record|struct)\s+(\w+)");
        var lines = source.Replace("\r\n", "\n").Split('\n');
        string? lastType = null;
        var disposeWindow = 0;
        var found = new HashSet<string>(StringComparer.Ordinal);
        foreach (var line in lines)
        {
            var declaration = typeDeclaration.Match(line);
            if (declaration.Success)
            {
                lastType = declaration.Groups[1].Value;
            }

            if (Regex.IsMatch(line, @"\bValueTask\s+DisposeAsync\(\)"))
            {
                disposeWindow = 12;
            }
            else if (disposeWindow > 0)
            {
                disposeWindow--;
                if (lastType is not null && DisposeCall.IsMatch(line))
                {
                    found.Add(lastType);
                }
            }
        }

        return found;
    }

    private static bool Disposes(string body, HashSet<string> creators)
    {
        /* `await (await Mint(...)).DisposeAsync()` mints and drops in one statement. */
        if (DisposeCall.IsMatch(body)
            || body.Contains("DropRememberedAsync(", StringComparison.Ordinal)
            || Regex.IsMatch(body, @"CreateAsync\([^;]*\)\)\s*\.DisposeAsync\("))
        {
            return true;
        }

        /* `await using var x = ...` on a variable named for the store, or on the result of the mint or a helper. */
        if (Regex.IsMatch(body, @"await\s+using\s*\(?\s*(?:var\s+)?\w*[sS]cratch\w*\s*="))
        {
            return true;
        }

        if (Regex.IsMatch(body, @"await\s+using\s*\(?\s*(?:var\s+)?\w+\s*=\s*[\w.]*[sS]cratch\w*\s*[;)]"))
        {
            return true;
        }

        var sources = new List<string> { Regex.Escape("ScratchPostgres" + ".CreateAsync") };
        sources.AddRange(creators.Select(Regex.Escape));
        return Regex.IsMatch(body, @"await\s+using\s*\(?\s*(?:var\s+)?\w+\s*=\s*\(?\s*await\s+(?:" + string.Join("|", sources) + @")\b");
    }

    /// <summary>The part of a member's first line before its name: its return type, so a parameter of the store's type does not count.</summary>
    private static string ReturnType(Member member)
    {
        var line = FirstLine(member.Text);
        var at = line.IndexOf(member.Name + "(", StringComparison.Ordinal);
        return at < 0 ? line : line[..at];
    }

    private static string FirstLine(string text)
    {
        var end = text.IndexOf('\n');
        return end < 0 ? text : text[..(end + 1)];
    }

    private readonly record struct Member(string Name, string Text);

    /// <summary>Cuts a class into its members: each starts on a line at the class-member indent (4 spaces) that opens with a modifier.</summary>
    private static List<Member> SplitIntoMembers(string source)
    {
        var members = new List<Member>();
        var lines = source.Replace("\r\n", "\n").Split('\n');
        var start = -1;
        for (var i = 0; i <= lines.Length; i++)
        {
            var atEnd = i == lines.Length;
            if (!atEnd && !MemberStart.IsMatch(lines[i]))
            {
                continue;
            }

            if (start >= 0)
            {
                var text = string.Join("\n", lines[start..i]) + "\n";
                var name = Regex.Match(lines[start], @"(\w+)\s*(?:<[^>(]*>)?\s*\(");
                members.Add(new Member(name.Success ? name.Groups[1].Value : lines[start].Trim(), text));
            }

            start = i;
        }

        return members;
    }

    private static string? FindTestProjectDirectory()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                var source = Path.Combine(directory.FullName, "Darling", "Darling.Tests");
                return Directory.Exists(source) ? source : null;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
