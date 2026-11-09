/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using PerformanceMonitorLite;
using Xunit.Sdk;
using Xunit.v3;

/* #8434: every test in the assembly is judged after it runs; one that leaves App's store paths changed fails, and the paths are put back. */
[assembly: AppPathsGuard]

/// <summary>
/// Fails a test that leaves the app's static store paths changed (#8434).
///
/// <para><see cref="App.DatabasePath"/>, <see cref="App.ConfigDirectory"/> and <see cref="App.SharedConfigDirectory"/> are process-wide, and
/// a <see cref="MainWindow"/> reads all three when it is built. A test that pointed them at a scratch folder of its own, deleted the
/// folder and did not put the paths back left every later window in the process writing servers.json into a folder that no longer
/// existed, and which test was hurt depended only on the order the classes ran in. This attribute, applied to the whole assembly,
/// records the three paths before each test and compares them after it; a test that changed one fails with a message that names its
/// class and the property, and the paths are put back first, so the test named is the only one that fails. The judgement runs after
/// the test body and its own cleanup, so a test that restores the paths through <see cref="AppPathsScope"/> in a <c>using</c> or a
/// <c>finally</c> has done so before it is judged.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class AppPathsGuardAttribute : BeforeAfterTestAttribute
{
    private readonly ConcurrentDictionary<string, AppPaths> _before = new(StringComparer.Ordinal);

    public override void Before(MethodInfo methodUnderTest, IXunitTest test) => _before[test.UniqueID] = AppPaths.Current;

    public override void After(MethodInfo methodUnderTest, IXunitTest test)
    {
        if (!_before.TryRemove(test.UniqueID, out var before))
        {
            return;
        }

        if (AppPathsGuard.Verdict(test.TestCase.TestClassName ?? test.TestDisplayName, before, AppPaths.Current) is { } failure)
        {
            before.Apply();
            throw new XunitException(failure);
        }
    }
}

/// <summary>The judgement behind <see cref="AppPathsGuardAttribute"/>.</summary>
internal static class AppPathsGuard
{
    /// <summary>
    /// The failure text for a test of <paramref name="testClassName"/> that found the paths at <paramref name="before"/> and left them at
    /// <paramref name="after"/>, naming each property that differs with both values, or null when the two are the same.
    /// </summary>
    internal static string? Verdict(string testClassName, AppPaths before, AppPaths after)
    {
        if (before == after)
        {
            return null;
        }

        var changes = new List<string>(3);
        Describe(changes, nameof(App.DatabasePath), before.DatabasePath, after.DatabasePath);
        Describe(changes, nameof(App.ConfigDirectory), before.ConfigDirectory, after.ConfigDirectory);
        Describe(changes, nameof(App.SharedConfigDirectory), before.SharedConfigDirectory, after.SharedConfigDirectory);
        return $"{testClassName} left {string.Join(" and ", changes)}. The app's store paths are process-wide and a MainWindow reads them "
            + "when it is built, so a test that points them at a folder of its own puts them back before it finishes, or a later test's "
            + "window reads and writes a folder this one has deleted (#8434). Set them through AppPathsScope, and dispose it before the folder is deleted.";
    }

    private static void Describe(List<string> changes, string property, string was, string now)
    {
        if (!string.Equals(was, now, StringComparison.Ordinal))
        {
            changes.Add($"App.{property} changed (was \"{was}\", now \"{now}\")");
        }
    }
}
