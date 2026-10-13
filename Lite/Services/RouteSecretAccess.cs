/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The four things <see cref="DuckDbNotificationRouteStore"/> needs from the place a route's destinations are kept (#5775):
/// read one, write one, delete one and list the keys that exist. The app passes <see cref="CredentialManager"/>; a test
/// passes an in-memory fake with fault injection, so no test touches the real Credential Manager. A key is the part after
/// the <c>PerformanceMonitorLite_</c> prefix, which is what <see cref="CredentialService"/> takes.
/// </summary>
/// <param name="Read">The value under a key; empty when absent or unreadable.</param>
/// <param name="Write">Stores a value (never empty) and says whether it worked.</param>
/// <param name="Delete">Removes a key and says whether it is gone afterwards; a key that was never there counts as gone.</param>
/// <param name="ListKeys">Every key that starts with the given prefix. Empty when none exist or the list cannot be read.</param>
public sealed record RouteSecretAccess(
    Func<string, string> Read,
    Func<string, string, bool> Write,
    Func<string, bool> Delete,
    Func<string, IReadOnlyList<string>> ListKeys)
{
    /// <summary>Windows Credential Manager through <see cref="CredentialService"/>, straight (not through
    /// <c>App.SaveWebhookUrl</c>, which swallows the result), so a refused save is reported (#5750).</summary>
    public static RouteSecretAccess CredentialManager()
    {
        var credentials = new CredentialService();
        return new RouteSecretAccess(
            key => credentials.GetCredential(key)?.Password ?? "",
            (key, value) => credentials.SaveCredential(key, "webhook", value),
            key => credentials.DeleteCredential(key) || !credentials.CredentialExists(key),
            credentials.ListKeys);
    }
}
