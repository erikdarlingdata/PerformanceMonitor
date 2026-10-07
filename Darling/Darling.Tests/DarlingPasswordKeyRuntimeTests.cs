/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store-free half of the password key runtime (#5366): the self-test that judges a kept key file, the reset verb's
/// text and grammar, the ring a web write sees before the start finishes, and the source pins that keep the start where
/// it must be (after migrations and provisioning, before any collection) and the per-sweep check in the loop.
/// </summary>
public sealed class DarlingPasswordKeyRuntimeTests
{
    [Fact]
    public void TheSelfTest_PassesForTheKeysOwnPrivateHalf_AndFailsForAnyOtherKey()
    {
        using var key = PasswordPrivateKey.Generate();
        using var other = PasswordPrivateKey.Generate();
        var published = new PublishedKey(key.PublicKey.KeyId, key.PublicKey.Spki);

        Assert.True(DarlingPasswordKeyRuntime.SelfTest(key, published));
        Assert.False(DarlingPasswordKeyRuntime.SelfTest(other, published));
    }

    [Fact]
    public void BeforeTheStartFinishes_TheRingRefusesWithTheStillLoadingReason()
    {
        // Production code sets the static once, at start; no test sets it, so a fresh process still holds the first ring.
        var ring = DarlingPasswordKey.Current;
        Assert.False(ring.Status.CanSeal);
        Assert.Equal(DarlingPasswordKey.NotReadyReason, ring.Status.Reason);
        var thrown = Assert.Throws<InvalidOperationException>(
            () => ring.Seal("p@ss-not-real", PasswordBinding.ForSmtp("smtp.example.com", 25, false, "user")));
        Assert.Equal(DarlingPasswordKey.NotReadyReason, thrown.Message);
    }

    [Theory]
    [InlineData(0, "these 0 saved passwords")]
    [InlineData(1, "this 1 saved password must")]
    [InlineData(7, "these 7 saved passwords must be entered again")]
    public void TheResetSentence_SaysHowManySavedPasswordsMustBeEnteredAgain(int count, string fragment)
    {
        var text = DarlingCliCommands.ResetPasswordKeyRestartSentence(count);
        Assert.StartsWith("Restart the service. It will make a new password key, and ", text, StringComparison.Ordinal);
        Assert.Contains(fragment, text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheResetVerb_IsRecognizedCaseInsensitively_AndNothingElseIs()
    {
        Assert.True(DarlingCliCommands.IsResetPasswordKeyVerb("--reset-password-key"));
        Assert.True(DarlingCliCommands.IsResetPasswordKeyVerb("--RESET-PASSWORD-KEY"));
        Assert.False(DarlingCliCommands.IsResetPasswordKeyVerb("--reset-password"));
        Assert.False(DarlingCliCommands.IsResetPasswordKeyVerb("--add-server"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--reset-password-key"), "The start-up classifier must let the verb through to its dispatch.");
    }

    [Fact]
    public async System.Threading.Tasks.Task TheResetVerb_RefusesAnUnknownOption_WithoutOpeningAnyStore()
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingCliCommands.ResetPasswordKeyAsync(
            ["--password", "x"], output, error, TestContext.Current.CancellationToken);

        Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, exit);
        Assert.Contains("--reset-password-key", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheWorker_StartsTheKeyBeforeTheLogHashKey_AndChecksItOnEverySweep()
    {
        var worker = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");
        var start = worker.IndexOf("DarlingPasswordKeyRuntime.StartForServiceAsync(", StringComparison.Ordinal);
        var logHash = worker.IndexOf("DarlingLogHashKeyFile.LoadForService(", StringComparison.Ordinal);
        var firstCollector = worker.IndexOf("new DarlingCollectorRunner(postgres", StringComparison.Ordinal);
        var sweepCheck = worker.IndexOf("_passwordKeyRuntime.SweepCheckAsync(", StringComparison.Ordinal);
        var loop = worker.IndexOf("while (!stoppingToken.IsCancellationRequested)\r\n        {\r\n            /* #4732", StringComparison.Ordinal);
        if (loop < 0)
        {
            loop = worker.IndexOf("while (!stoppingToken.IsCancellationRequested)\n        {\n            /* #4732", StringComparison.Ordinal);
        }

        var provisioning = worker.IndexOf("DarlingManagedRoles.EnsureProvisionedAsync(postgres", StringComparison.Ordinal);

        Assert.True(provisioning >= 0 && start > provisioning, "The key starts after role provisioning.");
        Assert.True(start > 0 && start < logHash, "The key starts beside the log-hash key, before it.");
        Assert.True(start < firstCollector, "The key starts before the collector runner exists.");
        Assert.True(loop > 0 && sweepCheck > loop, "The sweep check is the first thing in the sweep loop's body.");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(worker, @"DarlingPasswordKeyRuntime\.StartForServiceAsync\("));
    }

    [Fact]
    public void TheVerb_IsDispatchedFromProgram()
    {
        var program = ReadSource("Darling/PerformanceMonitor.Darling.Service/Program.cs");
        Assert.Contains("DarlingCliCommands.IsResetPasswordKeyVerb(args[0])", program, StringComparison.Ordinal);
        Assert.Contains("DarlingCliCommands.ResetPasswordKeyAsync(args[1..]", program, StringComparison.Ordinal);
    }

    [Fact]
    public void TheKeyStore_NamesEveryTriggerTheTablesScriptCreates()
    {
        foreach (var (table, trigger) in PasswordKeyTables.OwnerOnlyTriggers)
        {
            Assert.Contains($"CREATE TRIGGER {trigger}", PasswordKeyTables.CreateSql, StringComparison.Ordinal);
            Assert.Contains($"ENABLE ALWAYS TRIGGER {trigger};", PasswordKeyTables.CreateSql, StringComparison.Ordinal);
            Assert.Contains($"config.{table};", PasswordKeyTables.CreateSql, StringComparison.Ordinal);
        }

        Assert.Equal(8, PasswordKeyTables.OwnerOnlyTriggers.Count);
        Assert.Equal(8, System.Text.RegularExpressions.Regex.Matches(PasswordKeyTables.CreateSql, "CREATE TRIGGER ").Count);
    }

    private static string ReadSource(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
