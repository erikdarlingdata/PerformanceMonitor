/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net.Http;
using System.Security.Cryptography;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// The HTTP handler every front-door call to a store goes through (#6840). It trusts a store's certificate one of two ways and
/// never a third: the system's own validation, or, when the store's <c>certificateSha256</c> is set, that exact certificate and
/// nothing else. It does not follow redirects (a store that redirects is answering for somebody else) and it keeps no cookies
/// (the web proxy carries each store's session itself, so one store's cookie is never sent to another).
/// </summary>
internal static class DarlingFrontDoorHttp
{
    /// <summary>A handler for one store. The caller owns it and the <see cref="HttpClient"/> built on it.</summary>
    internal static SocketsHttpHandler CreateHandler(FrontDoorStore store, TimeSpan connectTimeout)
    {
        var handler = new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseCookies = false,
            ConnectTimeout = connectTimeout,
            /* A store whose address moves is found again within minutes instead of holding a dead connection for the process's life. */
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        };

        if (store.CertificateSha256 is { } pin)
        {
            var expected = Convert.FromHexString(pin);

            /* A pin replaces chain validation: a self-signed certificate has no chain, and the pin is the operator's own statement
               of which certificate this store is. A different certificate fails whatever the chain says. The first argument is
               the certificate the server presented. */
            handler.SslOptions.RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                certificate is not null
                && CryptographicOperations.FixedTimeEquals(SHA256.HashData(certificate.GetRawCertData()), expected);
        }

        return handler;
    }
}
