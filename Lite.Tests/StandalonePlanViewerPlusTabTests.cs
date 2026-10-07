/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Automation.Provider;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Markup;
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4684: a UI Automation select on the plan viewer's "+" tab added TWO empty sub-tabs instead of one. It is
/// the screen-reader path (Narrator, NVDA and JAWS select a tab through <c>SelectionItemPattern.Select()</c>),
/// so it is an accessibility bug. Lite and the Darling Viewer both build their sub-tabs through
/// <see cref="StandalonePlanViewerController"/>, so one test class covers both apps (the last test pins that
/// both hosts still do).
///
/// <para><b>Root cause, measured with a real UIA client round trip against a shown window</b> (not guessed):
/// the UIA core does not call <c>ISelectionItemProvider.Select()</c> alone. It first sets focus on the element,
/// then selects it, as two separate <c>Send</c>-priority dispatcher operations about 0.3 ms apart:
/// (1) <c>AutomationPeer.SetFocus</c> -&gt; <c>UIElement.Focus</c> -&gt; <c>TabItem.OnPreviewGotKeyboardFocus</c>,
/// and a TabItem SELECTS ITSELF when it gets focus (it calls
/// <c>SetCurrentValueInternal(IsSelectedProperty, true)</c>); (2) <c>SelectorItemAutomationPeer.Select</c> -&gt;
/// <c>SelectionChanger.SelectJustThisItem</c>. Each is a genuine selection change of "+", because the first
/// add already moved the selection to the new sub-tab. The controller adds a tab on every "+" selection, so
/// one gesture added two. Calling the peer's <c>Select()</c> alone adds only one, which is why the plain
/// direct-provider test below passes both before and after the fix; the focus-then-select test is the one
/// that fails on the old code.</para>
///
/// <para>No <c>Window.Show()</c>, like the other STA tests in this project: the window is laid out by hand so
/// the automation peers can enumerate the tab items, and the dispatcher is pumped explicitly. The focus step
/// is reproduced as the exact call <c>TabItem.OnPreviewGotKeyboardFocus</c> makes, not as a real keyboard
/// focus change, which needs a shown, activated window.</para>
/// </summary>
[Trait("Reads", "Darling")]
public sealed class StandalonePlanViewerPlusTabTests
{
    private const string PlusTag = "__PLAN_ADD_TAB__";

    /// <summary>What the UIA core does for one <c>SelectionItemPattern.Select()</c> on "+": a focus-driven
    /// selection (a TabItem selects itself on focus), then the explicit <c>Select()</c>, in one gesture with the
    /// dispatcher not getting a turn in between. Old code: two sub-tabs added.</summary>
    [Fact]
    public void UiaSelectOnPlus_FocusThenSelect_AddsExactlyOneSubTab_AndSelectsIt()
    {
        OnStaThread(rig =>
        {
            var before = rig.SubTabCount;

            rig.SelectByFocus(rig.PlusTab);
            rig.UiaSelect(rig.PlusTab);
            rig.Pump();

            Assert.Equal(before + 1, rig.SubTabCount);
            Assert.Same(rig.Tabs.Items[before], rig.Tabs.SelectedItem);
            Assert.NotSame(rig.PlusTab, rig.Tabs.SelectedItem);
        });
    }

    /// <summary>The order the other way round (select, then a focus-driven re-selection of "+") is the same
    /// gesture to a different client; it must not add either.</summary>
    [Fact]
    public void UiaSelectOnPlus_SelectThenFocus_AddsExactlyOneSubTab()
    {
        OnStaThread(rig =>
        {
            var before = rig.SubTabCount;

            rig.UiaSelect(rig.PlusTab);
            rig.SelectByFocus(rig.PlusTab);
            rig.Pump();

            Assert.Equal(before + 1, rig.SubTabCount);
            Assert.Same(rig.Tabs.Items[before], rig.Tabs.SelectedItem);
        });
    }

    /// <summary>The peer's <c>Select()</c> on its own is one selection, so one add. This passes before and
    /// after the fix; it is the guard that the absorb does not swallow a lone select.</summary>
    [Fact]
    public void UiaSelectAlone_AddsExactlyOneSubTab_AndSelectsIt()
    {
        OnStaThread(rig =>
        {
            var before = rig.SubTabCount;

            rig.UiaSelect(rig.PlusTab);
            rig.Pump();

            Assert.Equal(before + 1, rig.SubTabCount);
            Assert.Same(rig.Tabs.Items[before], rig.Tabs.SelectedItem);
        });
    }

    /// <summary>A mouse click reaches the sentinel by setting <c>IsSelected</c> on the item (through focus) or
    /// by setting the control's <c>SelectedItem</c>; either is one selection, so one add.</summary>
    [Theory]
    [InlineData("IsSelected")]
    [InlineData("SelectedItem")]
    public void MouseStyleSelectionOfPlus_AddsExactlyOneSubTab(string how)
    {
        OnStaThread(rig =>
        {
            var before = rig.SubTabCount;

            if (how == "IsSelected")
            {
                rig.PlusTab.IsSelected = true;
            }
            else
            {
                rig.Tabs.SelectedItem = rig.PlusTab;
            }

            rig.Pump();

            Assert.Equal(before + 1, rig.SubTabCount);
            Assert.Same(rig.Tabs.Items[before], rig.Tabs.SelectedItem);
        });
    }

    /// <summary>Two selections of "+" that the dispatcher separates (the first add is fully handled before the
    /// second click) are two gestures, so two adds. The absorb must not outlive its gesture.</summary>
    [Theory]
    [InlineData("mouse")]
    [InlineData("uia")]
    public void TwoSeparateSelectionsOfPlus_AddTwoSubTabs(string how)
    {
        OnStaThread(rig =>
        {
            var before = rig.SubTabCount;

            for (var i = 0; i < 2; i++)
            {
                if (how == "uia")
                {
                    rig.SelectByFocus(rig.PlusTab);
                    rig.UiaSelect(rig.PlusTab);
                }
                else
                {
                    rig.Tabs.SelectedItem = rig.PlusTab;
                }

                rig.Pump();
            }

            Assert.Equal(before + 2, rig.SubTabCount);
            Assert.Same(rig.Tabs.Items[before + 1], rig.Tabs.SelectedItem);
        });
    }

    /// <summary>The absorb only ever re-selects a tab that still exists. If the tab a gesture just added is closed
    /// before a further "+" selection lands, that selection ends on a live sub-tab, never on the closed one (WPF
    /// itself moves the selection onto "+" when the selected last sub-tab is removed, which adds a live tab).</summary>
    [Fact]
    public void SelectingPlusAfterTheJustAddedTabWasClosed_NeverSelectsTheClosedTab()
    {
        OnStaThread(rig =>
        {
            var before = rig.SubTabCount;

            rig.Tabs.SelectedItem = rig.PlusTab;
            var added = (TabItem)rig.Tabs.Items[before];
            rig.Tabs.Items.Remove(added);
            rig.Tabs.SelectedItem = rig.PlusTab;
            rig.Pump();

            Assert.False(rig.Tabs.Items.Contains(added));
            Assert.True(rig.Tabs.Items.Contains(rig.Tabs.SelectedItem));
            Assert.NotSame(rig.PlusTab, rig.Tabs.SelectedItem);
            Assert.Equal(before + 1, rig.SubTabCount);
        });
    }

    /// <summary>
    /// Lite and the Darling Viewer are covered by the tests above only because both hosts build their sub-tabs
    /// through the shared controller. Pin that neither grows a private "+" tab of its own (the deprecated
    /// Dashboard still has one, <c>deprecated/Dashboard/MainWindow.PlanViewer.cs</c>, and is deliberately left).
    /// </summary>
    [Theory]
    [InlineData("Lite/MainWindow.PlanViewer.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/MainWindow.PlanViewer.cs")]
    public void PlanViewerHost_BuildsItsSubTabsThroughTheSharedController(string hostFile)
    {
        var source = ParitySource.ReadFile(hostFile);

        Assert.Contains("new StandalonePlanViewerController(MainWindowPlanTabControl)", source, StringComparison.Ordinal);
        Assert.Contains("PlanViewerController.AddNewEmptyPlanSubTab()", source, StringComparison.Ordinal);

        // Nothing here may build a sub-tab, own the sentinel, or watch the tab control's selection itself.
        Assert.DoesNotContain("new TabItem", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PLAN_ADD_TAB", source, StringComparison.Ordinal);
        Assert.DoesNotContain("SelectionChanged", source, StringComparison.Ordinal);
        Assert.DoesNotContain("MainWindowPlanTabControl.Items.Add", source, StringComparison.Ordinal);
        Assert.DoesNotContain("MainWindowPlanTabControl.Items.Insert", source, StringComparison.Ordinal);
    }

    /* ── harness ── */

    private sealed class Rig
    {
        public Window Window { get; }
        public TabControl Tabs { get; }

        private Rig(Window window, TabControl tabs)
        {
            Window = window;
            Tabs = tabs;
        }

        /// <summary>The trailing "+" sentinel tab.</summary>
        public TabItem PlusTab => (TabItem)Tabs.Items[Tabs.Items.Count - 1];

        public int SubTabCount => Tabs.Items.Count - 1;

        /// <summary>A real Window, a real TabControl under Lite's real Light theme (which carries
        /// <c>ForegroundMutedBrush</c>, <c>TabCloseButton</c> and the implicit TabItem style), the controller,
        /// <c>EnsureInitialized</c> and one real sub-tab: the steady state a "+" click finds.</summary>
        public static Rig Create()
        {
            var window = new Window { Width = 900, Height = 600, ShowInTaskbar = false };
            window.Resources.MergedDictionaries.Add(
                (ResourceDictionary)XamlReader.Parse(ParitySource.ReadFile("Lite/Themes/LightTheme.xaml")));
            var tabs = new TabControl();
            window.Content = tabs;

            var controller = new StandalonePlanViewerController(tabs);
            controller.EnsureInitialized();
            controller.AddNewEmptyPlanSubTab();

            var rig = new Rig(window, tabs);
            rig.Pump();
            Assert.Equal(1, rig.SubTabCount);
            Assert.Equal(PlusTag, rig.PlusTab.Tag);
            return rig;
        }

        /// <summary>Lays the window out (so the automation peers can enumerate the tab items) and runs the
        /// dispatcher until everything queued at any priority, the Loaded-priority deferral and the gesture's
        /// own Background follow-up included, has run.</summary>
        public void Pump()
        {
            /* The TabControl is laid out directly: an unshown Window never applies its own template, so it never
               puts its content in the visual tree, but the TabControl measured as a root builds its tab strip. */
            Tabs.Measure(new Size(900, 600));
            Tabs.Arrange(new Rect(0, 0, 900, 600));
            Tabs.UpdateLayout();

            var frame = new DispatcherFrame();
            Dispatcher.CurrentDispatcher.BeginInvoke(DispatcherPriority.SystemIdle, new Action(() => frame.Continue = false));
            Dispatcher.PushFrame(frame);
        }

        /// <summary>The focus half of a UIA select: <c>TabItem.OnPreviewGotKeyboardFocus</c> selects a tab
        /// that receives focus by calling exactly this.</summary>
        public void SelectByFocus(TabItem tab) => tab.SetCurrentValue(Selector.IsSelectedProperty, true);

        /// <summary>The pattern half of a UIA select: the item peer's <c>ISelectionItemProvider.Select()</c>,
        /// found through the tab control's own automation peer the way a UIA client reaches it.</summary>
        public void UiaSelect(TabItem tab)
        {
            var tabsPeer = UIElementAutomationPeer.CreatePeerForElement(Tabs);
            var children = tabsPeer.GetChildren();
            Assert.NotNull(children);
            var itemPeer = children!.Single(p => p is ItemAutomationPeer { Item: var item } && ReferenceEquals(item, tab));
            var provider = (ISelectionItemProvider)itemPeer.GetPattern(PatternInterface.SelectionItem)!;
            provider.Select();
        }
    }

    /// <summary>WPF objects need STA and a dispatcher. Bounded, so a wedged frame fails the test instead of
    /// hanging the suite; the dispatcher is shut down so nothing leaks between tests.</summary>
    private static void OnStaThread(Action<Rig> body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try
            {
                body(Rig.Create());
            }
            catch (Exception ex)
            {
                error = ex;
            }
            finally
            {
                Dispatcher.CurrentDispatcher.InvokeShutdown();
            }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.IsBackground = true;
        thread.Start();

        Assert.True(thread.Join(TimeSpan.FromSeconds(60)), "the STA thread did not finish within 60 s.");

        if (error is not null)
        {
            throw error;
        }
    }
}
