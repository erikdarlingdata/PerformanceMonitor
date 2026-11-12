/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8754: <c>[InMemoryCredentials]</c> must reach a plain <c>new CredentialService()</c> in the test body and in a body run on the
/// shared STA test thread. If it did not, the classes that run in parallel on that attribute would write the developer's real
/// Windows Credential Manager (#5785) and still pass. Each fact saves through a plain <c>new CredentialService()</c>, reads it back
/// through another, and then asks the real store (a service made with the override cleared) for the same key: it must not be there.
/// This class takes the attribute and no serial collection, because it is what the attribute's parallel classes rely on.
/// </summary>
[InMemoryCredentials]
public sealed class InMemoryCredentialsReachTests
{
    [Fact]
    public void APlainCredentialService_InTheTestBody_UsesTheInMemoryStore_AndNeverTheRealOne()
    {
        var key = "pm8760-body-" + Guid.NewGuid().ToString("N");
        try
        {
            SaveReadAndCheckTheRealStoreIsUntouched(key);
        }
        finally
        {
            DeleteFromTheRealStore(key);
        }
    }

    [Fact]
    public void APlainCredentialService_InsideStaTestThreadRun_UsesTheInMemoryStore_AndNeverTheRealOne()
    {
        var key = "pm8760-sta-" + Guid.NewGuid().ToString("N");
        try
        {
            StaTestThread.Run(() => SaveReadAndCheckTheRealStoreIsUntouched(key));
        }
        finally
        {
            DeleteFromTheRealStore(key);
        }
    }

    private static void SaveReadAndCheckTheRealStoreIsUntouched(string key)
    {
        Assert.True(new CredentialService().SaveCredential(key, "user", "secret"));
        Assert.Equal(("user", "secret"), new CredentialService().GetCredential(key));

        using (RealStore())
        {
            Assert.Null(new CredentialService().GetCredential(key));
        }
    }

    private static void DeleteFromTheRealStore(string key)
    {
        using (RealStore())
        {
            var real = new CredentialService();
            if (real.GetCredential(key) is not null)
            {
                real.DeleteCredential(key);
            }
        }
    }

    /* A null store clears the override for this flow, so a plain new CredentialService() builds the real Credential Manager one. */
    private static IDisposable RealStore() => CredentialService.UseStoreForCurrentTest(null!);
}
