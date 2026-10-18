/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What Lite keeps about one generic webhook destination's signing key besides the key itself (#6012): whether a key is
/// saved and when the previous key stops signing. Both are plain facts: the settings row keeps them in settings.json, a
/// route in its table row. The keys are in Windows Credential Manager.
/// </summary>
public readonly record struct LiteSigningState(bool HasKey, DateTime? PreviousUntilUtc)
{
    /// <summary>No key.</summary>
    public static readonly LiteSigningState None = new(false, null);
}

/// <summary>What the route row says about a key and whether it opens: for the route dialog, which cannot learn it from the loaded
/// route (a key that will not open turns the generic channel off and loads the route with no key).</summary>
public readonly record struct LiteRouteSigning(LiteSigningState State, bool Unreadable);

/// <summary>What a Generate or Rotate gave: the new key (shown once, never stored in a place that shows it back), the state to save,
/// or the sentence for why nothing changed. The error never contains a key.</summary>
public sealed record LiteSigningKeyResult(string? NewKey, LiteSigningState State, string? Error)
{
    /// <summary>Names no key.</summary>
    public override string ToString() => "LiteSigningKeyResult { Generated = " + (NewKey is not null) + ", Error = " + (Error ?? "none") + " }";
}

/// <summary>
/// The signing keys of Lite's generic webhooks (#6012, Darling's #5998 ported). Lite is the sender, so it holds the keys:
/// the settings row's generic URL has one key and each route with its own generic URL has one (decision D2). A key lives in
/// Credential Manager (through <see cref="RouteSecretAccess"/>, as Lite's bearer token headers and route destinations do),
/// the current key and, for the 7 days of <see cref="WebhookSigning.RotationOverlap"/> after a rotation, the previous one
/// (D3). A saved flag with a key that will not open is reported as unreadable, and the caller turns the destination off, so a
/// signed destination is never sent unsigned. Nothing here logs, returns in an error, or prints a key.
/// </summary>
public static class LiteWebhookSigningKeys
{
    /// <summary>What the key block says next to a new key's box (Darling's words).</summary>
    public const string ShownOnceText = "Copy this key now and give it to the receiver. It will not be shown again.";

    /// <summary>What a destination without a key says.</summary>
    public const string NoKeyText = "No key. Posts to this URL are not signed.";

    /// <summary>What a route's key block says before the route is saved.</summary>
    public const string SaveRouteFirstText = "Save the route with a generic webhook URL first, then open it again to make a key.";

    /// <summary>What a test or a send says when a saved key will not open.</summary>
    public const string UnreadableText = "Not sent: a generic webhook signing key could not be opened, and a signed destination is never sent unsigned.";

    /// <summary>What Remove says when the keys are gone but the flag could not be saved (#6012): the flag still says a key is saved,
    /// so the destination reads as unreadable and is held back, never sent unsigned.</summary>
    public const string RemoveNotSavedText = "The keys were removed, but the settings could not be saved, so settings still say a key is saved. Posts to this URL are held back until you try Remove again.";

    /// <summary>The Credential Manager key of the settings row's generic signing key (current or previous).</summary>
    public static string SettingsName(bool previous) => previous ? "GenericWebhookSigningKeyPrevious" : "GenericWebhookSigningKey";

    /// <summary>Every route signing key starts with this. It is not <c>NotificationRoute_</c>, so the destination sweep never reads it.</summary>
    public const string RouteKeyPrefix = "NotificationRouteSigning_";

    /// <summary>The Credential Manager key of a route's generic signing key (current or previous).</summary>
    public static string RouteName(int routeId, bool previous) =>
        string.Create(CultureInfo.InvariantCulture, $"{RouteKeyPrefix}{routeId}_{(previous ? "Previous" : "Current")}");

    /// <summary>The line the key block shows: never the key, only that one is saved and when a rotation ends.</summary>
    public static string StatusText(LiteSigningState state, DateTime nowUtc, bool unreadable = false)
    {
        if (!state.HasKey)
        {
            return NoKeyText;
        }

        if (unreadable)
        {
            return "A key is saved but could not be opened, so posts to this URL are held back. Rotate it to replace it, or remove it.";
        }

        return state.PreviousUntilUtc is { } until && until > nowUtc
            ? "Saved. The previous key is also sent until " + until.ToUniversalTime().ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture) + "."
            : "Saved. Rotate to replace it, or remove it.";
    }

    /// <summary>The button the key block offers: Generate when there is no key, Rotate when there is one.</summary>
    public static string ActionText(LiteSigningState state) => state.HasKey ? "Rotate key" : "Generate key";

    /// <summary>
    /// Makes a new key and saves it as the current one; a current key already saved becomes the previous one for
    /// <see cref="WebhookSigning.RotationOverlap"/>. The previous key is written first and the new one read back, so a refused
    /// write leaves the saved state as it was. The caller saves the returned <see cref="LiteSigningKeyResult.State"/>.
    /// </summary>
    public static LiteSigningKeyResult Generate(RouteSecretAccess secrets, string currentName, string previousName, LiteSigningState state, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        var newKey = WebhookSigning.NewKey();
        DateTime? until = null;
        try
        {
            if (state.HasKey)
            {
                var old = secrets.Read(currentName) ?? "";
                if (!string.IsNullOrWhiteSpace(old))
                {
                    if (secrets.Write(previousName, old) is { } previousFailure)
                    {
                        return new LiteSigningKeyResult(null, state, "The previous key could not be kept in Windows Credential Manager because " + previousFailure + ", so the key was not changed.");
                    }

                    until = nowUtc + WebhookSigning.RotationOverlap;
                }
            }

            if (secrets.Write(currentName, newKey) is { } failure)
            {
                return new LiteSigningKeyResult(null, state, "The key could not be saved to Windows Credential Manager because " + failure + ", so the key was not changed.");
            }

            if (!string.Equals(secrets.Read(currentName), newKey, StringComparison.Ordinal))
            {
                return new LiteSigningKeyResult(null, state, "Windows Credential Manager did not keep the key, so the key was not changed.");
            }
        }
        catch (Exception ex)
        {
            return new LiteSigningKeyResult(null, state, "The key could not be saved to Windows Credential Manager (" + ex.GetType().Name + "), so the key was not changed.");
        }

        return new LiteSigningKeyResult(newKey, new LiteSigningState(true, until), null);
    }

    /// <summary>
    /// <see cref="Generate(RouteSecretAccess, string, string, LiteSigningState, DateTime)"/> plus the save of the flag's home
    /// (#6012, round 2): a new key is returned only after Credential Manager and <paramref name="saveState"/> both took it.
    /// <paramref name="saveState"/> writes the new state (settings.json for the settings row, the route's row for a route) and
    /// returns null when it worked, else why not. When it fails or throws, both Credential Manager names are put back as they
    /// were before the call, no key is returned, and the error says so; if the put-back fails too, the error says that.
    /// The error never contains a key.
    /// </summary>
    public static LiteSigningKeyResult Generate(
        RouteSecretAccess secrets, string currentName, string previousName, LiteSigningState state, DateTime nowUtc,
        Func<LiteSigningState, string?> saveState)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        ArgumentNullException.ThrowIfNull(saveState);
        string oldCurrent;
        string oldPrevious;
        try
        {
            oldCurrent = secrets.Read(currentName) ?? "";
            oldPrevious = secrets.Read(previousName) ?? "";
        }
        catch (Exception ex)
        {
            return new LiteSigningKeyResult(null, state, "The saved key could not be read from Windows Credential Manager (" + ex.GetType().Name + "), so the key was not changed.");
        }

        var made = Generate(secrets, currentName, previousName, state, nowUtc);
        if (made.Error is not null || made.NewKey is null)
        {
            /* Generate may have written the previous key before it stopped; put both back as they were. */
            if (made.Error is not null)
            {
                _ = Restore(secrets, currentName, previousName, oldCurrent, oldPrevious);
            }

            return made;
        }

        string? saveFailure;
        try
        {
            saveFailure = saveState(made.State);
        }
        catch (Exception ex)
        {
            saveFailure = "it failed (" + ex.GetType().Name + ")";
        }

        if (saveFailure is null)
        {
            return made;
        }

        var restoreFailure = Restore(secrets, currentName, previousName, oldCurrent, oldPrevious);
        var message = "The new key was not kept: the signing setting could not be saved because " + saveFailure + ".";
        message += restoreFailure is null
            ? " Windows Credential Manager was put back as it was, so the key was not changed."
            : " Windows Credential Manager could not be put back (" + restoreFailure + "), so it may hold a key that was never shown. Rotate the key again or remove it.";
        return new LiteSigningKeyResult(null, state, message);
    }

    /// <summary>Puts the two Credential Manager names back to what they held. A name that held nothing is deleted. Null when both
    /// are as they were, else a short reason. Never contains a key.</summary>
    private static string? Restore(RouteSecretAccess secrets, string currentName, string previousName, string oldCurrent, string oldPrevious)
    {
        try
        {
            var ok = RestoreOne(secrets, currentName, oldCurrent);
            ok &= RestoreOne(secrets, previousName, oldPrevious);
            return ok ? null : "Windows Credential Manager refused the change";
        }
        catch (Exception ex)
        {
            return ex.GetType().Name;
        }
    }

    private static bool RestoreOne(RouteSecretAccess secrets, string name, string old)
    {
        if (string.Equals(secrets.Read(name) ?? "", old, StringComparison.Ordinal))
        {
            return true;
        }

        return old.Length == 0 ? secrets.Delete(name) : secrets.Write(name, old) is null;
    }

    /// <summary>Removes both keys. Returns the new state (none), or the same state and an error when a key would not go.</summary>
    public static LiteSigningState Remove(RouteSecretAccess secrets, string currentName, string previousName, LiteSigningState state, out string? error)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        error = null;
        try
        {
            if (!secrets.Delete(currentName) || !secrets.Delete(previousName))
            {
                error = "Windows Credential Manager would not remove the key, so it is still saved.";
                return state;
            }
        }
        catch (Exception ex)
        {
            error = "Windows Credential Manager would not remove the key (" + ex.GetType().Name + "), so it is still saved.";
            return state;
        }

        return LiteSigningState.None;
    }

    /// <summary>
    /// Opens the keys of a destination. No saved flag is <see cref="WebhookSigningKeys.None"/>, unsigned. A saved flag whose
    /// current key will not open sets <paramref name="unreadable"/> and gives none: the caller turns the destination off. The
    /// previous key is read only while its window is open, and one that will not open is left out (the current key still signs).
    /// </summary>
    public static WebhookSigningKeys Open(RouteSecretAccess secrets, string currentName, string previousName, LiteSigningState state, DateTime nowUtc, out bool unreadable)
    {
        unreadable = false;
        if (!state.HasKey)
        {
            return WebhookSigningKeys.None;
        }

        string key;
        try
        {
            key = secrets.Read(currentName) ?? "";
        }
        catch (Exception)
        {
            key = "";
        }

        if (!WebhookSigning.TryDecodeKey(key, out _))
        {
            unreadable = true;
            return WebhookSigningKeys.None;
        }

        var previous = "";
        if (state.PreviousUntilUtc is { } until && until > nowUtc)
        {
            try
            {
                previous = secrets.Read(previousName) ?? "";
            }
            catch (Exception)
            {
                previous = "";
            }

            if (!WebhookSigning.TryDecodeKey(previous, out _))
            {
                previous = "";
            }
        }

        return new WebhookSigningKeys(key.Trim(), previous.Trim(), previous.Length == 0 ? null : state.PreviousUntilUtc);
    }
}
