/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// What the store holds for one generic webhook destination's signing key (#5998): the sealed current key, the sealed previous
/// key kept for the rotation window, when that window ends, and the stored generic URL and proxy the key is bound to. Every
/// text is as stored: the keys are sealed and are never opened here.
/// </summary>
public sealed record ViewerSigningKeyState(string Key, string PreviousKey, DateTime? PreviousUntilUtc, string StoredUrl, string Proxy)
{
    /// <summary>Whether a key is stored. Any text counts, so a malformed value is not hidden from a save that would orphan it.</summary>
    public bool HasKey => !string.IsNullOrWhiteSpace(Key);

    /// <summary>Whether the previous key is still sent at <paramref name="nowUtc"/>.</summary>
    public bool Rotating(DateTime nowUtc) => !string.IsNullOrWhiteSpace(PreviousKey) && PreviousUntilUtc is { } until && until > nowUtc;

    /// <summary>Names no stored text: the keys are sealed, but a log line has no use for them either.</summary>
    public override string ToString() => "ViewerSigningKeyState { HasKey = " + HasKey + " }";
}

/// <summary>
/// The words and the checks of the signing key block in Settings and in the route dialog (#5998). The block mirrors the bearer
/// token's: a saved key reads as saved (<see cref="ViewerWebhookSealing.IsSaved"/>), and the value is never shown back. A new
/// key is shown once, in a box the person copies from; nothing here logs it, puts it in a label after that box, or copies it.
/// </summary>
public static class ViewerWebhookSigningKeys
{
    /// <summary>What the block says next to a new key's box.</summary>
    public const string ShownOnceText = "Copy this key now and give it to the receiver. It will not be shown again.";

    /// <summary>What a destination without a key says.</summary>
    public const string NoKeyText = "No key. Posts to this URL are not signed.";

    /// <summary>Beside the Send Test button of a destination: a test sent from this app is not signed.</summary>
    public const string UnsignedTestText =
        "The service signs its posts. A test sent from this app is not signed, and a receiver that checks signatures will refuse it.";

    /// <summary>What a key action says when the destination has no saved, sealed generic URL to bind the key to.</summary>
    public const string SaveUrlFirstText =
        "Save the generic webhook URL first. A key is tied to the saved URL and proxy, so the URL has to be saved before a key is made.";

    /// <summary>What a key action says when the URL or proxy changed between reading the destination and writing the key.</summary>
    public const string ChangedMeanwhileText =
        "The generic webhook URL or proxy changed while the key was made. Close this window, open it again and retry.";

    /// <summary>What a save says when it would change the URL or proxy a key is tied to.</summary>
    public const string RemoveKeyFirstText =
        "A signing key is saved for this destination, and it is tied to the generic webhook URL and proxy. Remove the key first, "
        + "save the change, then generate a new key.";

    /// <summary>What a Settings save says when the generic proxy changes while a route holds a key.</summary>
    public const string RouteKeysTieToProxyText =
        "A route holds a signing key, and it is tied to the generic webhook proxy. Remove the key on each route first, "
        + "save the change, then generate the keys again.";

    /// <summary>The line the key block shows for a destination.</summary>
    public static string StatusText(ViewerSigningKeyState? state, DateTime nowUtc)
    {
        if (state is null || !state.HasKey)
        {
            return NoKeyText;
        }

        if (!ViewerWebhookSealing.IsSaved(state.Key))
        {
            return "A key is stored but is not sealed. Rotate it to replace it, or remove it.";
        }

        return state.Rotating(nowUtc)
            ? "Saved. The previous key is also sent until " + state.PreviousUntilUtc!.Value.ToUniversalTime().ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture) + "."
            : "Saved. Rotate to replace it, or remove it.";
    }

    /// <summary>The button the key block offers: Generate when there is no key, Rotate when there is one.</summary>
    public static string ActionText(ViewerSigningKeyState? state) => state is { HasKey: true } ? "Rotate key" : "Generate key";

    /// <summary>
    /// The sentence a save is refused with because it would break a key, or null when it is safe. A key is sealed for the stored
    /// generic URL text and the settings row's generic proxy, so a changed URL text (a retyped URL is sealed again to new text),
    /// a cleared URL or a changed proxy leaves the key unreadable to the service.
    /// </summary>
    public static string? SaveRefusal(ViewerSigningKeyState? stored, string? newUrlText, string? newProxy, bool checkProxy)
    {
        if (stored is null || !stored.HasKey)
        {
            return null;
        }

        if (!string.Equals(newUrlText ?? "", stored.StoredUrl, StringComparison.Ordinal))
        {
            return RemoveKeyFirstText;
        }

        return checkProxy && !string.Equals(newProxy ?? "", stored.Proxy, StringComparison.Ordinal) ? RemoveKeyFirstText : null;
    }

    /// <summary>The sentence a Settings save is refused with because the generic proxy changes while a route holds a key, or null.</summary>
    public static string? RouteKeyRefusal(IReadOnlyCollection<int> routeIdsWithKeys, string? newProxy, string? storedProxy) =>
        routeIdsWithKeys.Count > 0 && !string.Equals(newProxy ?? "", storedProxy ?? "", StringComparison.Ordinal)
            ? RouteKeysTieToProxyText + " Routes: " + string.Join(", ", routeIdsWithKeys.OrderBy(i => i)) + "."
            : null;

    /// <summary>The binding a destination's key is sealed under: the signing slot, the row, the settings row's generic proxy and
    /// the stored generic URL text. The same binding the service opens it with.</summary>
    public static PasswordBinding BindingFor(string row, ViewerSigningKeyState state) =>
        PasswordBinding.ForWebhook(PasswordBinding.SigningKeySlot, row, state.Proxy, state.StoredUrl);

    /// <summary>A new key, sealed for <paramref name="state"/>'s destination. Returns the plain key (to show once) and the sealed text.</summary>
    public static (string Plain, string Sealed) MakeKey(string row, ViewerSigningKeyState state, ViewerPasswordSealer sealer)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(sealer);
        if (!ViewerWebhookSealing.IsSaved(state.StoredUrl))
        {
            throw new ViewerPasswordRefusedException(SaveUrlFirstText);
        }

        var plain = WebhookSigning.NewKey();
        return (plain, sealer.SealWebhook(plain, BindingFor(row, state)));
    }
}
