/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using Xunit.v3;

namespace Darling.Tests;

/// <summary>
/// #8395: a Viewer card's band brush is resolved from the process-wide theme at bind time, so a test that reads one off the STA thread
/// binds whatever theme another test is holding at that instant. The first test here holds a theme swap open from a second thread while a
/// card binds on the test's own thread, with the two threads handing off through gates rather than waiting out a clock, and shows both
/// halves of the mechanism: the swap reaches the bind, and <see cref="ApplicationThemeGuard"/> records the swap against the test that
/// held it even though the swap ran on the STA thread. The rest pins the recorder and the judgement that fails a writer outside a
/// collection that runs alone.
/// </summary>
[Collection("application-theme")]
public sealed class ApplicationThemeGuardTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The Dark amber every band and border takes for a Warning card when the theme is Dark.</summary>
    private const string DarkAmber = "#FFFFD54F";

    /// <summary>An online PostgreSQL card with nothing banded, the shape <c>UnmeasuredMetricsAreNotHealthyTests</c> reads: its border is the amber band.</summary>
    private static ServerSummaryItem CardMeasuringNothing()
    {
        var card = new ServerSummaryItem
        {
            ServerName = "t",
            ServerId = 1,
            HealthyCollectorCount = 0,
            CollectorCount = 0,
            DeadlockWindow = TimeSpan.FromHours(1),
            BlockingWindow = TimeSpan.FromHours(1),
            IsPostgres = true,
            LastCollectionTime = Now.AddSeconds(-30),
        };
        card.ApplyFreshness(Now);
        return card;
    }

    private static Application EnsureApplication() =>
        Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };

    /// <summary>The theme state a test takes over and hands back: the theme name, the merged dictionaries and the theme URI provider.</summary>
    private sealed record ThemeState(string Theme, List<ResourceDictionary> Merged, Func<string, Uri> UriProvider);

    /// <summary>Points the theme loader at the Viewer's theme files and applies Dark, so the card reads its Dark amber; returns what to hand back.</summary>
    private static ThemeState TakeOverTheThemeAsDark() => StaTestThread.Run(() =>
    {
        var app = EnsureApplication();
        var state = new ThemeState(ThemeManager.CurrentTheme, app.Resources.MergedDictionaries.ToList(), ThemeManager.ThemeUriProvider);
        ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");
        ThemeManager.Apply("Dark");
        return state;
    });

    private static void HandBack(ThemeState state) => StaTestThread.Run(() =>
    {
        var app = EnsureApplication();
        ThemeManager.Apply(state.Theme);
        app.Resources.MergedDictionaries.Clear();
        foreach (var dictionary in state.Merged)
        {
            app.Resources.MergedDictionaries.Add(dictionary);
        }

        ThemeManager.ThemeUriProvider = state.UriProvider;
    });

    [Fact]
    public async Task ASwapHeldOpenFromAnotherThread_ReachesACardBoundMeanwhile_AndIsRecordedAgainstTheTestThatHeldIt()
    {
        var test = TestContext.Current.Test;
        Assert.NotNull(test);

        var state = TakeOverTheThemeAsDark();
        string boundWhileHeld;
        string boundAfterwards;
        try
        {
            Assert.Equal(DarkAmber, CardMeasuringNothing().CardBorderBrush.Color.ToString());

            using var swapped = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();

            /* The holder swaps the theme on the STA thread, says so, and keeps the swap open until this thread has bound a card. */
            var holder = Task.Run(() =>
            {
                StaTestThread.Run(() => ThemeManager.Apply("Light"));
                swapped.Set();
                release.Wait();
                StaTestThread.Run(() => ThemeManager.Apply("Dark"));
            });

            try
            {
                swapped.Wait();
                boundWhileHeld = CardMeasuringNothing().CardBorderBrush.Color.ToString();
            }
            finally
            {
                release.Set();
            }

            await holder;
            boundAfterwards = CardMeasuringNothing().CardBorderBrush.Color.ToString();
        }
        finally
        {
            HandBack(state);
        }

        /* The card bound on this thread, with no window of its own, took the Light theme's darkened amber: the brush is process-wide state. */
        Assert.NotEqual(DarkAmber, boundWhileHeld);
        Assert.Equal(DarkAmber, boundAfterwards);

        /* And the swap is on this test's account, though it ran on the STA thread from a thread this test started. */
        var changes = ApplicationThemeGuard.ChangesRecordedFor(test);
        Assert.Contains("ThemeManager.Apply(\"Light\")", changes);
        Assert.Contains("ThemeManager.Apply(\"Dark\")", changes);
    }

    [Fact]
    public void AMergeIntoTheApplicationResources_IsRecordedAgainstTheTestThatMerged_AndTakenWhenItIsJudged()
    {
        var test = Assert.IsAssignableFrom<IXunitTest>(TestContext.Current.Test);

        StaTestThread.Run(() => EnsureApplication());
        StaTestThread.Run(() =>
        {
            var extra = new ResourceDictionary();
            Application.Current.Resources.MergedDictionaries.Add(extra);
            Application.Current.Resources.MergedDictionaries.Remove(extra);
        });

        var changes = ApplicationThemeGuard.ChangesRecordedFor(test);
        Assert.Contains("Application.Current.Resources.MergedDictionaries.Add", changes);
        Assert.Contains("Application.Current.Resources.MergedDictionaries.Remove", changes);

        /* This class is in the collection, so the judgement allows the changes and takes them off the account. */
        ApplicationThemeGuard.AssertAllowed(test);
        Assert.Empty(ApplicationThemeGuard.ChangesRecordedFor(test));
    }

    [Fact]
    public void ATestThatChangesNothing_HasNothingOnItsAccount()
    {
        var test = TestContext.Current.Test;
        Assert.NotNull(test);
        Assert.Empty(ApplicationThemeGuard.ChangesRecordedFor(test));
    }

    [Theory]
    [InlineData("Test collection for Darling.Tests.SomeRenderTests", false, false)]
    [InlineData("live-postgres", false, false)]
    [InlineData("application-theme", false, true)]
    [InlineData("viewer-time-statics", true, true)]
    [InlineData("credential-manager", true, true)]
    public void TheVerdict_FailsAWriterOnlyFromACollectionThatRunsBesideOthers(string collection, bool runsAlone, bool allowed)
    {
        var changes = new[] { "ThemeManager.Apply(\"Light\")", "Application.Current.Resources.MergedDictionaries.Reset" };
        var verdict = ApplicationThemeGuard.Verdict(collection, runsAlone, "Darling.Tests.SomeRenderTests", changes);

        if (allowed)
        {
            Assert.Null(verdict);
            return;
        }

        Assert.NotNull(verdict);
        Assert.StartsWith("Darling.Tests.SomeRenderTests changed the process-wide theme", verdict, StringComparison.Ordinal);
        Assert.Contains("ThemeManager.Apply(\"Light\")", verdict, StringComparison.Ordinal);
        Assert.Contains($"\"{collection}\"", verdict, StringComparison.Ordinal);
        Assert.Contains("[Collection(\"application-theme\")]", verdict, StringComparison.Ordinal);
    }

    [Fact]
    public void TheVerdict_AllowsATestThatChangedNothing_WhereverItRuns() =>
        Assert.Null(ApplicationThemeGuard.Verdict("Test collection for Darling.Tests.SomeRenderTests", false, "Darling.Tests.SomeRenderTests", Array.Empty<string>()));

    [Fact]
    public void TheCollection_RunsAlone_AndTheAssemblyCarriesTheGuard()
    {
        var definition = typeof(ApplicationThemeCollection).GetCustomAttribute<CollectionDefinitionAttribute>();
        Assert.NotNull(definition);
        Assert.Equal("application-theme", definition.Name);
        Assert.True(definition.DisableParallelization, "the application-theme collection runs alone, or a reader in another collection can bind a swapped theme");

        Assert.NotNull(typeof(ApplicationThemeGuardTests).Assembly.GetCustomAttribute<ApplicationThemeGuardAttribute>());
    }
}
