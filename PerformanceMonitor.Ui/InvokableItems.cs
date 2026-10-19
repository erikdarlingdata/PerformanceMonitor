/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows.Automation;
using System.Windows.Automation.Peers;
using System.Windows.Automation.Provider;
using System.Windows.Controls;
using System.Windows.Threading;

namespace PerformanceMonitor.Ui;

/// <summary>The item an automation client invoked (#6138).</summary>
public sealed class ItemInvokedEventArgs : EventArgs
{
    public ItemInvokedEventArgs(object item) => Item = item;

    /// <summary>The data item whose row or card was invoked.</summary>
    public object Item { get; }
}

/// <summary>
/// A server list whose rows answer the UI Automation Invoke (default action) pattern (#6138). Both apps' sidebars open a
/// server's tab on a double-click or Enter, which a screen reader or a test tool cannot send; the rows exposed only
/// SelectionItem, so no automation route opened a tab. Invoking a row raises <see cref="ItemInvoked"/>, and the window
/// runs the same open method its double-click and Enter run. Everything else is the plain <see cref="ListBox"/>: give it
/// <c>Style="{DynamicResource {x:Type ListBox}}"</c> to keep the app's ListBox theme, which a subclass does not pick up.
/// </summary>
public class InvokableListBox : ListBox
{
    /// <summary>Raised on the UI thread, after the automation call has returned, for the invoked row's data item.</summary>
    public event EventHandler<ItemInvokedEventArgs>? ItemInvoked;

    internal void RaiseItemInvoked(object item) => ItemInvoked?.Invoke(this, new ItemInvokedEventArgs(item));

    protected override AutomationPeer OnCreateAutomationPeer() => new InvokableListBoxAutomationPeer(this);

    private sealed class InvokableListBoxAutomationPeer : ListBoxAutomationPeer
    {
        public InvokableListBoxAutomationPeer(InvokableListBox owner) : base(owner)
        {
        }

        protected override ItemAutomationPeer CreateItemAutomationPeer(object item) =>
            new InvokableListBoxItemAutomationPeer(item, this);
    }

    private sealed class InvokableListBoxItemAutomationPeer : ListBoxItemAutomationPeer, IInvokeProvider
    {
        public InvokableListBoxItemAutomationPeer(object item, SelectorAutomationPeer selectorPeer) : base(item, selectorPeer)
        {
        }

        public override object? GetPattern(PatternInterface patternInterface) =>
            patternInterface == PatternInterface.Invoke ? this : base.GetPattern(patternInterface);

        public void Invoke()
        {
            if (ItemsControlAutomationPeer?.Owner is InvokableListBox list && Item is { } item)
            {
                /* Deferred, as WPF's own Button peer does: a handler may open a dialog, which must not hold the caller. */
                list.Dispatcher.BeginInvoke(DispatcherPriority.Input, new Action(() => list.RaiseItemInvoked(item)));
            }
        }
    }
}

/// <summary>
/// The Overview card list with the UI Automation Invoke (default action) pattern on each card (#6138): a card opened its
/// server's tab on a double-click only. Invoking a card raises <see cref="ItemInvoked"/> for its data item; the window
/// runs the same open method its double-click runs.
/// </summary>
public class InvokableItemsControl : ItemsControl
{
    /// <summary>Raised on the UI thread, after the automation call has returned, for the invoked card's data item.</summary>
    public event EventHandler<ItemInvokedEventArgs>? ItemInvoked;

    internal void RaiseItemInvoked(object item) => ItemInvoked?.Invoke(this, new ItemInvokedEventArgs(item));

    protected override AutomationPeer OnCreateAutomationPeer() => new InvokableItemsControlAutomationPeer(this);

    private sealed class InvokableItemsControlAutomationPeer : ItemsControlAutomationPeer
    {
        public InvokableItemsControlAutomationPeer(InvokableItemsControl owner) : base(owner)
        {
        }

        protected override ItemAutomationPeer CreateItemAutomationPeer(object item) =>
            new InvokableItemAutomationPeer(item, this);
    }

    private sealed class InvokableItemAutomationPeer : ItemAutomationPeer, IInvokeProvider
    {
        public InvokableItemAutomationPeer(object item, ItemsControlAutomationPeer itemsControlPeer) : base(item, itemsControlPeer)
        {
        }

        protected override AutomationControlType GetAutomationControlTypeCore() => AutomationControlType.ListItem;

        protected override string GetClassNameCore() => "InvokableItem";

        public override object? GetPattern(PatternInterface patternInterface) =>
            patternInterface == PatternInterface.Invoke ? this : base.GetPattern(patternInterface);

        public void Invoke()
        {
            if (ItemsControlAutomationPeer?.Owner is InvokableItemsControl list && Item is { } item)
            {
                list.Dispatcher.BeginInvoke(DispatcherPriority.Input, new Action(() => list.RaiseItemInvoked(item)));
            }
        }
    }
}

/// <summary>The screen-reader name of an alert count badge (#6138): the bare "1" the badge paints says nothing on its own.</summary>
public static class UnreadAlertsName
{
    /// <summary>"1 unread alert", "3 unread alerts"; an empty name for no alerts, since the badge is hidden then.</summary>
    public static string For(int count) => count switch
    {
        <= 0 => string.Empty,
        1 => "1 unread alert",
        _ => count.ToString(System.Globalization.CultureInfo.InvariantCulture) + " unread alerts"
    };
}
