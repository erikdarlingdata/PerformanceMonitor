/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Reflection;
using System.Windows;
using PerformanceMonitor.Ui;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

/// <summary>
/// Fails a test that changes the process-wide theme from a test collection that runs in parallel with other collections (#8395).
///
/// <para>A card's band brush is resolved at bind time from process-wide state: <see cref="ThemeManager.CurrentTheme"/> decides whether
/// <c>ThemedBandBrush</c> darkens a band for a light surface, and <c>Application.Current.TryFindResource</c> reads whatever dictionaries
/// are merged into the application's resources at that moment. A test that reads such a brush on its own thread, with no window of
/// its own, therefore binds the theme that is current when it reads. xUnit runs test collections in parallel, so a class in another
/// collection that applies a theme, or merges one into the application resources, changes what that reader sees for as long as it holds
/// the change. The readers and the writers share one collection, <c>application-theme</c>, whose definition disables parallelization, so
/// nothing else runs while a theme is swapped.</para>
///
/// <para>This attribute, applied to the whole assembly, keeps new writers in. <see cref="ApplicationThemeGuard"/> records every theme
/// change against the test that made it, through <see cref="ThemeManager.ThemeChanged"/> and the application's merged-dictionary
/// collection, whichever thread the change ran on (the STA host runs a body under the calling test's context). After each test the
/// recorded changes are judged against the test's collection: inside a collection whose definition sets <c>DisableParallelization = true</c>,
/// as <c>application-theme</c>'s does, a change is allowed; anywhere else the test fails and the message names the class and the change.
/// A change is attributed to the test current on the thread that makes it, so one made where no test is current, in a class or collection
/// fixture, a module initializer, a <c>DispatcherTimer</c> tick between bodies or the theme overrides-file watcher, is not recorded. The judgement runs after the test body and its own cleanup, so a writer that restores the theme in a <c>finally</c> has done so
/// before it is failed.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class ApplicationThemeGuardAttribute : BeforeAfterTestAttribute
{
    public override void Before(MethodInfo methodUnderTest, IXunitTest test) => ApplicationThemeGuard.Observe();

    public override void After(MethodInfo methodUnderTest, IXunitTest test)
    {
        ApplicationThemeGuard.Observe();
        ApplicationThemeGuard.AssertAllowed(test);
    }
}

/// <summary>The recorder behind <see cref="ApplicationThemeGuardAttribute"/>: which test changed the process-wide theme, and how.</summary>
internal static class ApplicationThemeGuard
{
    /// <summary>The collection every class that changes the application-scope theme, or reads a card brush off the STA thread, carries.</summary>
    internal const string CollectionName = "application-theme";

    private static readonly object s_lock = new();
    private static readonly Dictionary<string, List<string>> s_changes = new(StringComparer.Ordinal);
    private static INotifyCollectionChanged? s_hooked;

    static ApplicationThemeGuard()
    {
        ThemeManager.ThemeChanged += theme => Record($"ThemeManager.Apply(\"{theme}\")");
    }

    /// <summary>
    /// Watches the application's merged-dictionary collection, once per collection instance. The shared STA host creates the
    /// <see cref="Application"/> before its first body and calls this then (#8433), so the hook exists before any body can merge; the calls
    /// before and after every STA body and around every test re-hook if the collection instance changes, and do nothing while it is the same.
    /// </summary>
    internal static void Observe()
    {
        if (Application.Current is not { } app)
        {
            return;
        }

        if (app.Resources.MergedDictionaries is not INotifyCollectionChanged merged)
        {
            return;
        }

        lock (s_lock)
        {
            if (ReferenceEquals(merged, s_hooked))
            {
                return;
            }

            if (s_hooked is { } prior)
            {
                prior.CollectionChanged -= OnMergedDictionariesChanged;
            }

            merged.CollectionChanged += OnMergedDictionariesChanged;
            s_hooked = merged;
        }
    }

    /// <summary>
    /// After an STA body: when the body itself created the <see cref="Application"/>, every dictionary merged into it so far was merged by
    /// that body before the collection could be watched, so the merge is recorded here against the running test. The shared host creates the
    /// <see cref="Application"/> ahead of its first body (#8433), so for it the body never creates one; this covers a host that does not.
    /// </summary>
    internal static void ObserveAfterBody(bool applicationExistedBefore)
    {
        if (Application.Current is not { } app)
        {
            return;
        }

        var mergedWhileCreating = applicationExistedBefore ? 0 : app.Resources.MergedDictionaries.Count;
        Observe();
        if (mergedWhileCreating > 0)
        {
            Record($"{mergedWhileCreating} dictionar{(mergedWhileCreating == 1 ? "y" : "ies")} merged into Application.Current.Resources by the body that created the Application");
        }
    }

    /// <summary>The changes recorded against <paramref name="test"/> so far, in order; empty when it changed nothing.</summary>
    internal static IReadOnlyList<string> ChangesRecordedFor(ITest test)
    {
        lock (s_lock)
        {
            return s_changes.TryGetValue(test.UniqueID, out var changes) ? changes.ToArray() : Array.Empty<string>();
        }
    }

    /// <summary>Takes the changes recorded against <paramref name="test"/> and fails it when its collection does not run alone.</summary>
    internal static void AssertAllowed(IXunitTest test)
    {
        List<string>? changes;
        lock (s_lock)
        {
            if (!s_changes.Remove(test.UniqueID, out changes))
            {
                return;
            }
        }

        var collection = test.TestCase.TestCollection;
        if (Verdict(collection.TestCollectionDisplayName, collection.DisableParallelization, test.TestCase.TestClassName ?? test.TestDisplayName, changes) is { } failure)
        {
            throw new XunitException(failure);
        }
    }

    /// <summary>
    /// The failure text for a test that made <paramref name="changes"/> from the collection named, or null when the changes are allowed:
    /// none were made, or the collection's definition disables parallelization so it runs alone. The name is not consulted, so a
    /// collection called <see cref="CollectionName"/> whose definition lost that flag fails its writers like any other.
    /// </summary>
    internal static string? Verdict(string collectionDisplayName, bool collectionRunsAlone, string testClassName, IReadOnlyList<string> changes)
    {
        if (changes.Count == 0 || collectionRunsAlone)
        {
            return null;
        }

        return $"{testClassName} changed the process-wide theme ({string.Join("; ", changes.Distinct(StringComparer.Ordinal))}) from the test collection "
            + $"\"{collectionDisplayName}\", which runs in parallel with other collections. A card brush read in another collection binds the theme "
            + $"that is current when it reads, so a class that calls ThemeManager.Apply or merges into Application.Current.Resources carries "
            + $"[Collection(\"{CollectionName}\")], or sits in a collection whose [CollectionDefinition] sets DisableParallelization = true (#8395).";
    }

    private static void OnMergedDictionariesChanged(object? sender, NotifyCollectionChangedEventArgs e) =>
        Record($"Application.Current.Resources.MergedDictionaries.{e.Action}");

    private static void Record(string change)
    {
        if (TestContext.Current.Test is not { } test)
        {
            return;
        }

        lock (s_lock)
        {
            if (!s_changes.TryGetValue(test.UniqueID, out var changes))
            {
                changes = new List<string>();
                s_changes[test.UniqueID] = changes;
            }

            changes.Add(change);
        }
    }
}
