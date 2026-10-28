/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.FrontDoor;
using Xunit;

namespace Darling.Tests;

/// <summary>#6840: the handler every front-door call goes through trusts a store's certificate by the system's rules, or by its
/// SHA-256 pin alone, and never follows a redirect or keeps a cookie. The listener is an in-process Kestrel HTTPS endpoint on
/// 127.0.0.1 with a throwaway self-signed certificate.</summary>
public sealed class FrontDoorHttpTests
{
    private static readonly CancellationToken Ct = TestContext.Current.CancellationToken;

    private static X509Certificate2 MakeCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=front-door-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        using var selfSigned = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));

        /* A round trip through a PFX gives the key a persisted form that Windows' TLS stack can use. */
        return X509CertificateLoader.LoadPkcs12(selfSigned.Export(X509ContentType.Pkcs12, "pw"), "pw");
    }

    private static string Pin(X509Certificate2 certificate) => Convert.ToHexString(SHA256.HashData(certificate.RawData));

    private static async Task<(WebApplication App, Uri Address)> StartAsync(X509Certificate2 certificate)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o => o.Listen(IPAddress.Loopback, 0, listen => listen.UseHttps(certificate)));
        var app = builder.Build();
        app.MapGet("/", () => "ok");
        app.MapGet("/redirect", (HttpContext c) =>
        {
            c.Response.Cookies.Append("peer-session", "abc");
            return Results.Redirect("/");
        });
        app.MapGet("/cookie-echo", (HttpContext c) => c.Request.Headers.ContainsKey("Cookie") ? "cookie" : "none");
        await app.StartAsync(Ct);

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses;
        return (app, new Uri(System.Linq.Enumerable.Single(addresses)));
    }

    private static FrontDoorStore StoreFor(Uri address, string? pin)
        => new("use1", address, null, pin);

    private static HttpClient ClientFor(FrontDoorStore store)
        => new(DarlingFrontDoorHttp.CreateHandler(store, TimeSpan.FromSeconds(10)), disposeHandler: true) { Timeout = TimeSpan.FromSeconds(30) };

    [Fact]
    public async Task APin_ThatMatchesTheCertificate_IsAccepted_WithNoChainTrust()
    {
        using var certificate = MakeCertificate();
        var (app, address) = await StartAsync(certificate);
        try
        {
            using var client = ClientFor(StoreFor(address, Pin(certificate)));
            Assert.Equal("ok", await client.GetStringAsync(address, Ct));
        }
        finally
        {
            await app.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task APin_ThatDoesNotMatch_IsRefused()
    {
        using var served = MakeCertificate();
        using var other = MakeCertificate();
        var (app, address) = await StartAsync(served);
        try
        {
            using var client = ClientFor(StoreFor(address, Pin(other)));
            await Assert.ThrowsAsync<HttpRequestException>(() => client.GetStringAsync(address, Ct));
        }
        finally
        {
            await app.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task WithNoPin_ASelfSignedCertificate_IsRefused_BySystemValidation()
    {
        using var certificate = MakeCertificate();
        var (app, address) = await StartAsync(certificate);
        try
        {
            using var client = ClientFor(StoreFor(address, null));
            await Assert.ThrowsAsync<HttpRequestException>(() => client.GetStringAsync(address, Ct));
        }
        finally
        {
            await app.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task ARedirect_IsNotFollowed_AndNoCookieIsKept()
    {
        using var certificate = MakeCertificate();
        var (app, address) = await StartAsync(certificate);
        try
        {
            using var client = ClientFor(StoreFor(address, Pin(certificate)));

            using var redirect = await client.GetAsync(new Uri(address, "/redirect"), Ct);
            Assert.Equal(HttpStatusCode.Redirect, redirect.StatusCode);

            /* The peer's Set-Cookie from the first answer is not sent back on the second call. */
            Assert.Equal("none", await client.GetStringAsync(new Uri(address, "/cookie-echo"), Ct));
        }
        finally
        {
            await app.StopAsync(Ct);
        }
    }

    [Fact]
    public void TheHandler_CarriesTheConnectTimeout_AndTheRedirectAndCookieSettings()
    {
        using var handler = DarlingFrontDoorHttp.CreateHandler(StoreFor(new Uri("https://use1.example.test/"), null), TimeSpan.FromSeconds(7));
        Assert.False(handler.AllowAutoRedirect);
        Assert.False(handler.UseCookies);
        Assert.Equal(TimeSpan.FromSeconds(7), handler.ConnectTimeout);
        Assert.Null(handler.SslOptions.RemoteCertificateValidationCallback);
    }
}
