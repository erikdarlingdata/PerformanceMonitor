/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5775: <see cref="CredentialStore.ListKeys"/> against the real Windows Credential Manager, under a key prefix made of a
/// fresh GUID so nothing of the user's is read or touched, and cleaned up in a finally. Skips when this session cannot write a
/// credential (a service account or a locked-down runner), because then there is nothing to enumerate.
/// </summary>
public sealed class CredentialStoreListKeysTests
{
    [Fact]
    public void ListKeys_ReturnsOnlyThisStoresKeysThatStartWithThePrefix_WithoutTheStorePrefix()
    {
        var unique = "ListKeysTest" + Guid.NewGuid().ToString("N") + "_";
        var mine = new CredentialStore("PerformanceMonitorLiteTest_");
        var other = new CredentialStore("PerformanceMonitorLiteTestOther_");
        string[] keys = [unique + "a", unique + "b"];
        try
        {
            Assert.SkipWhen(!mine.SaveCredential(keys[0], "webhook", "https://value.invalid/a"), "This session cannot write to Windows Credential Manager.");
            Assert.True(mine.SaveCredential(keys[1], "webhook", "https://value.invalid/b"));
            Assert.True(other.SaveCredential(unique + "c", "webhook", "https://value.invalid/c"));
            Assert.True(mine.SaveCredential("Unrelated_" + unique, "webhook", "https://value.invalid/d"));

            var listed = mine.ListKeys(unique);

            Assert.Equal(keys.OrderBy(k => k, StringComparer.Ordinal), listed.OrderBy(k => k, StringComparer.Ordinal));
            Assert.Empty(mine.ListKeys("NoSuchKey" + Guid.NewGuid().ToString("N")));
            Assert.Throws<ArgumentException>(() => mine.ListKeys(""));
        }
        finally
        {
            foreach (var key in keys)
            {
                mine.DeleteCredential(key);
            }

            mine.DeleteCredential("Unrelated_" + unique);
            other.DeleteCredential(unique + "c");
        }
    }
}
