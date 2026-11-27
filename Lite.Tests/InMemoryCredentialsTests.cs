/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8754: the in-memory credential backend behind <c>[InMemoryCredentials]</c>. Classes that moved onto it run in parallel, so what
/// they write must stay inside their own async flow, and the store must answer the way the real one does where the code under test
/// can tell (a secret over the limit is refused with the reason, a missing key reads as not found).
/// </summary>
[InMemoryCredentials]
public sealed class InMemoryCredentialsTests
{
    [Fact]
    public async Task TwoFlowsWithTheirOwnStore_NeverSeeEachOthersEntries()
    {
        using var bothSaved = new Barrier(2);
        var key = "shared-key";

        async Task<bool> FlowAsync(string password)
        {
            await Task.Yield();
            using var scope = CredentialService.UseStoreForCurrentTest(new InMemoryCredentialStore());
            var credentials = new CredentialService();
            Assert.True(credentials.SaveCredential(key, "user", password));
            bothSaved.SignalAndWait(TimeSpan.FromSeconds(30));
            await Task.Yield();
            return new CredentialService().GetCredential(key)?.Password == password;
        }

        var results = await Task.WhenAll(Task.Run(() => FlowAsync("one")), Task.Run(() => FlowAsync("two")));
        Assert.All(results, r => Assert.True(r));
    }

    [Fact]
    public void TheStore_RefusesASecretOverTheLimit_WithTheReason_AndReadsAMissingKeyAsNotFound()
    {
        var store = new InMemoryCredentialStore();
        var tooLong = new string('x', CredentialStore.MaxSecretLength + 1);

        var refused = store.TrySaveCredential("a", "user", tooLong);
        Assert.False(refused.Succeeded);
        Assert.Contains(CredentialStore.MaxSecretLength.ToString("N0"), refused.Failure);
        Assert.DoesNotContain(tooLong, refused.Failure);
        Assert.Equal(CredentialReadOutcome.NotFound, store.TryReadCredential("a", out var read, out _));
        Assert.Null(read);

        Assert.True(store.SaveCredential("a", "user", new string('x', CredentialStore.MaxSecretLength)));
        Assert.Equal(CredentialReadOutcome.Found, store.TryReadCredential("a", out read, out _));
        Assert.Equal("user", read!.Value.Username);
        Assert.Equal(new[] { "a" }, store.ListKeys("a"));
        Assert.True(store.TryDeleteCredential("a").Succeeded);
        Assert.False(store.CredentialExists("a"));
    }
}
