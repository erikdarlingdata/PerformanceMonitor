/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using PerformanceMonitorLite;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Lite.Tests;

/// <summary>
/// #8434: <see cref="AppPathsScope"/> points the app's three store paths at a folder and puts them back when it is disposed, and
/// <see cref="AppPathsGuardAttribute"/> fails a test that finds them changed before its body or leaves them changed after it, naming
/// the class and the property, and puts them back itself. The guard is driven directly here, with this test as the test under
/// judgement, so a failing verdict is observed rather than suffered. The facts write the process-wide statics, so the class runs in
/// the collection that runs alone: a parallel test's own before or after check must never read a folder of this class's.
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
public sealed class AppPathsGuardTests
{
    [Fact]
    public void AScope_PointsTheThreePathsAtItsFolder_AndPutsThemBackWhenDisposed()
    {
        var before = AppPaths.Current;
        var folder = Folder();

        using (var scope = new AppPathsScope(folder))
        {
            Assert.Equal(folder, scope.Folder);
            Assert.Equal(Path.Combine(folder, "monitor.duckdb"), App.DatabasePath);
            Assert.Equal(folder, App.ConfigDirectory);
            Assert.Equal(folder, App.SharedConfigDirectory);
        }

        Assert.Equal(before, AppPaths.Current);
    }

    [Fact]
    public void ATestThatSetsThePathsAndDoesNotPutThemBack_IsFailed_NamingItsClassAndEachProperty_AndThePathsArePutBack()
    {
        var guard = new AppPathsGuardAttribute();
        var (method, test) = ThisTest();
        var before = AppPaths.Current;
        var folder = Folder();

        guard.Before(method, test);
        AppPaths.InFolder(folder).Apply();
        var failure = Assert.Throws<XunitException>(() => guard.After(method, test));

        Assert.Contains(nameof(AppPathsGuardTests) + " left", failure.Message, StringComparison.Ordinal);
        Assert.Contains("App.DatabasePath changed", failure.Message, StringComparison.Ordinal);
        Assert.Contains("App.ConfigDirectory changed", failure.Message, StringComparison.Ordinal);
        Assert.Contains("App.SharedConfigDirectory changed", failure.Message, StringComparison.Ordinal);
        Assert.Contains(folder, failure.Message, StringComparison.Ordinal);
        Assert.Contains("a restore in Dispose is too late", failure.Message, StringComparison.Ordinal);
        Assert.Equal(before, AppPaths.Current);
    }

    [Fact]
    public void ATestWhosePathsWereChangedBeforeItsBody_IsFailedBeforeTheBodyRuns_AndThePathsArePutBack()
    {
        var guard = new AppPathsGuardAttribute();
        var (method, test) = ThisTest();
        var before = AppPaths.Current;
        var folder = Folder();

        /* What a scope opened in a constructor, or a path set by a fixture, leaves for the before check to find. */
        AppPaths.InFolder(folder).Apply();
        try
        {
            var failure = Assert.Throws<XunitException>(() => guard.Before(method, test));

            Assert.Contains(nameof(AppPathsGuardTests) + "'s test body was about to run with", failure.Message, StringComparison.Ordinal);
            Assert.Contains("App.DatabasePath changed", failure.Message, StringComparison.Ordinal);
            Assert.Contains(folder, failure.Message, StringComparison.Ordinal);
            Assert.Contains("this class's constructor, a class or collection fixture, or an earlier test's Dispose", failure.Message, StringComparison.Ordinal);
            Assert.Equal(before, AppPaths.Current);
        }
        finally
        {
            before.Apply();
        }
    }

    [Fact]
    public void ATestThatPutsThePathsBackThroughAScope_IsNotFailed()
    {
        var guard = new AppPathsGuardAttribute();
        var (method, test) = ThisTest();

        guard.Before(method, test);
        using (new AppPathsScope(Folder()))
        {
        }

        Assert.Null(Record.Exception(() => guard.After(method, test)));
    }

    [Fact]
    public void TheVerdicts_NameOnlyThePropertyThatChanged()
    {
        var resting = new AppPaths(@"C:\store\monitor.duckdb", @"C:\config", @"C:\shared");
        var changed = resting with { ConfigDirectory = @"C:\gone" };

        var after = AppPathsGuard.VerdictAfterBody("SomeTests", resting, changed);
        var before = AppPathsGuard.VerdictBeforeBody("SomeTests", resting, changed);

        Assert.NotNull(after);
        Assert.StartsWith("SomeTests left App.ConfigDirectory changed (was \"C:\\config\", now \"C:\\gone\")", after, StringComparison.Ordinal);
        Assert.NotNull(before);
        Assert.StartsWith("SomeTests's test body was about to run with App.ConfigDirectory changed (was \"C:\\config\", now \"C:\\gone\")", before, StringComparison.Ordinal);
        foreach (var verdict in new[] { after, before })
        {
            Assert.DoesNotContain("App.DatabasePath", verdict, StringComparison.Ordinal);
            Assert.DoesNotContain("App.SharedConfigDirectory", verdict, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void UnchangedPaths_AreNoFailure()
    {
        var paths = new AppPaths(@"C:\store\monitor.duckdb", @"C:\config", @"C:\shared");

        Assert.Null(AppPathsGuard.VerdictBeforeBody("SomeTests", paths, paths));
        Assert.Null(AppPathsGuard.VerdictAfterBody("SomeTests", paths, paths));
    }

    private static string Folder() => Path.Combine(Path.GetTempPath(), "AppPathsGuard_" + Guid.NewGuid().ToString("N")[..8]);

    /// <summary>The running test, as the guard sees it: its method and the xunit test the runner is executing.</summary>
    private static (MethodInfo Method, IXunitTest Test) ThisTest()
    {
        var test = Assert.IsAssignableFrom<IXunitTest>(TestContext.Current.Test);
        var method = typeof(AppPathsGuardTests).GetMethod(test.TestMethod.MethodName, BindingFlags.Instance | BindingFlags.Public)!;
        return (method, test);
    }
}
