/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Input;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The keyboard rules both apps' server lists share (#5763): which key opens the selected server's tab, how a
/// server is found again after the list's items were replaced, and how a list is rebound without losing the
/// keyboard. Lite's sidebar and the Darling viewer's sidebar are two separate ListBoxes over two separate
/// <c>FleetView</c> projections, so the rules live here and each window only supplies its own rows.
/// </summary>
public static class ServerListKeyboard
{
    /// <summary>True for a plain Enter (the main key and the keypad one both arrive as <see cref="Key.Return"/>).
    /// Enter with Ctrl, Alt, Shift or Windows held is some other gesture and does not open a tab.</summary>
    public static bool IsOpenGesture(Key key, ModifierKeys modifiers) =>
        key == Key.Return && modifiers == ModifierKeys.None;

    /// <summary>The key-down handler body both lists run: on the open gesture, marks the key handled (so the list does
    /// not also beep or move the selection) and runs <paramref name="openSelected"/>, the same method the list's
    /// double-click handler runs.</summary>
    public static void HandleKeyDown(KeyEventArgs e, Action openSelected)
    {
        if (e.Handled || !IsOpenGesture(e.Key, Keyboard.Modifiers))
        {
            return;
        }

        e.Handled = true;
        openSelected();
    }

    /// <summary>
    /// The Invoke (default action) body both lists run (#6138): selects the invoked row, then runs
    /// <paramref name="openSelected"/>, the same method the double-click and Enter run, so an automation client opens a
    /// tab by the one open path. A group header's selection toggles the group and moves the selection on to a server
    /// row, so the open runs only when the invoked row is still the selected one.
    /// </summary>
    public static void HandleInvoke(ListBox list, object item, Action openSelected)
    {
        list.SelectedItem = item;
        if (ReferenceEquals(list.SelectedItem, item))
        {
            openSelected();
        }
    }

    /// <summary>
    /// The row for the server <paramref name="previousId"/> names, or null when there is none (no previous server,
    /// or it is no longer in <paramref name="rows"/>). The match is by the server's identity, never by its index:
    /// after the list's items are replaced the same server sits at a different position, and a group header may be
    /// row 0. The caller decides what a null means (the first server, or no selection).
    /// </summary>
    public static TRow? FindByIdentity<TRow, TId>(IEnumerable<TRow> rows, TId? previousId, Func<TRow, TId> idOf)
        where TRow : class
        where TId : struct, IEquatable<TId>
    {
        if (previousId is not TId wanted)
        {
            return null;
        }

        return rows.FirstOrDefault(r => idOf(r).Equals(wanted));
    }

    /// <summary>
    /// Replaces <paramref name="list"/>'s items and keeps the keyboard on the list if it had it. Replacing
    /// <see cref="System.Windows.Controls.ItemsControl.ItemsSource"/> discards every row container, and the focused
    /// one goes with it, so without this a user who selected a server and then waited for a refresh would find Enter
    /// going nowhere. The list itself takes the focus (it handles the Enter key for its rows).
    /// </summary>
    public static void Rebind(ListBox list, IEnumerable items)
    {
        var hadKeyboard = list.IsKeyboardFocusWithin;
        list.ItemsSource = items;
        if (hadKeyboard)
        {
            list.Focus();
        }
    }
}
