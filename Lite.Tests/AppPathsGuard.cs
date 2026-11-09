/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using PerformanceMonitorLite;
using Xunit.Sdk;
using Xunit.v3;

/* #8434: every test in the assembly is judged before and after its body; one that finds or leaves App's store paths changed fails, and the paths are put back. */
[assembly: AppPathsGuard]

/// <summary>
/// Fails a test that finds or leaves the app's static store paths changed (#8434).
///
/// <para><see cref="App.DatabasePath"/>, <see cref="App.ConfigDirectory"/> and <see cref="App.SharedConfigDirectory"/> are process-wide, and
/// a <see cref="MainWindow"/> reads all three when it is built. A test that pointed them at a scratch folder of its own, deleted the
/// folder and did not put the paths back left every later window in the process writing servers.json into a folder that no longer
/// existed, and which test was hurt depended only on the order the classes ran in. This attribute, applied to the whole assembly,
/// compares the three paths with what they held when the assembly loaded (<see cref="AppPathsGuard.Resting"/>), before each test's
/// body and again after it. A test that fails the comparison fails with a message that names its class and the property, and the
/// paths are put back first, so the test named is the only one that fails.</para>
///
/// <para>The contract is that a test opens <see cref="AppPathsScope"/> inside its test method and disposes it before the method returns.
/// The before check runs after the class's constructor, so a scope opened in a constructor, or a path set by a class or collection
/// fixture, is caught before the body runs. The after check runs after the method and before the class is disposed, so a restore in
/// <c>Dispose</c> is too late and the test is failed; a <c>using</c> or a <c>finally</c> inside the method has restored the paths before
/// they are judged.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class AppPathsGuardAttribute : BeforeAfterTestAttribute
{
    public override void Before(MethodInfo methodUnderTest, IXunitTest test) => Judge(test, AppPathsGuard.VerdictBeforeBody);

    public override void After(MethodInfo methodUnderTest, IXunitTest test) => Judge(test, AppPathsGuard.VerdictAfterBody);

    private static void Judge(IXunitTest test, Func<string, AppPaths, AppPaths, string?> verdict)
    {
        if (verdict(test.TestCase.TestClassName ?? test.TestDisplayName, AppPathsGuard.Resting, AppPaths.Current) is { } failure)
        {
            AppPathsGuard.Resting.Apply();
            throw new XunitException(failure);
        }
    }
}

/// <summary>The judgement behind <see cref="AppPathsGuardAttribute"/>.</summary>
internal static class AppPathsGuard
{
    /// <summary>
    /// The three paths as the assembly found them when it loaded, before any test class was constructed: what every test starts from
    /// and must leave behind. On a test process that is the empty string for all three, since <see cref="App"/>'s startup does not run.
    /// </summary>
    internal static AppPaths Resting { get; private set; }

    [ModuleInitializer]
    internal static void CaptureResting() => Resting = AppPaths.Current;

    /// <summary>
    /// The failure text for a test of <paramref name="testClassName"/> whose body is about to run with the paths at <paramref name="now"/>
    /// rather than <paramref name="resting"/>, or null when they are the same. The paths were changed after the previous test was judged
    /// and before this body: by this class's constructor, a class or collection fixture, or an earlier test's Dispose.
    /// </summary>
    internal static string? VerdictBeforeBody(string testClassName, AppPaths resting, AppPaths now)
    {
        if (resting == now)
        {
            return null;
        }

        return $"{testClassName}'s test body was about to run with {Changes(resting, now)}. The paths were changed outside a test method, "
            + "by this class's constructor, a class or collection fixture, or an earlier test's Dispose, where the guard cannot see who did "
            + "it. The app's store paths are process-wide and a MainWindow reads them when it is built (#8434). Open AppPathsScope inside the "
            + "test method and dispose it before the method returns, before the folder is deleted.";
    }

    /// <summary>
    /// The failure text for a test of <paramref name="testClassName"/> that left the paths at <paramref name="after"/> rather than
    /// <paramref name="resting"/>, naming each property that differs with both values, or null when the two are the same.
    /// </summary>
    internal static string? VerdictAfterBody(string testClassName, AppPaths resting, AppPaths after)
    {
        if (resting == after)
        {
            return null;
        }

        return $"{testClassName} left {Changes(resting, after)}. The app's store paths are process-wide and a MainWindow reads them when it is "
            + "built, so a test that points them at a folder of its own puts them back before its method returns, or a later test's window "
            + "reads and writes a folder this one has deleted (#8434). This judgement runs before the class is disposed, so a restore in "
            + "Dispose is too late: open AppPathsScope inside the test method and dispose it before the method returns, before the folder is deleted.";
    }

    private static string Changes(AppPaths from, AppPaths to)
    {
        var changes = new List<string>(3);
        Describe(changes, nameof(App.DatabasePath), from.DatabasePath, to.DatabasePath);
        Describe(changes, nameof(App.ConfigDirectory), from.ConfigDirectory, to.ConfigDirectory);
        Describe(changes, nameof(App.SharedConfigDirectory), from.SharedConfigDirectory, to.SharedConfigDirectory);
        return string.Join(" and ", changes);
    }

    private static void Describe(List<string> changes, string property, string was, string now)
    {
        if (!string.Equals(was, now, StringComparison.Ordinal))
        {
            changes.Add($"App.{property} changed (was \"{was}\", now \"{now}\")");
        }
    }
}
