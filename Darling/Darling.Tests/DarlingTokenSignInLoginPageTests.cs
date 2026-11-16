/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>The SSO-only login page and the token-sign-in-disabled page (#8735).</summary>
public sealed class DarlingTokenSignInLoginPageTests
{
    /* SHA-256 of the pages and policies as they were before #8735, so the existing two renderings stay byte-identical. */
    private const string PlainPageHash = "B9E5662FC6CC2AABE5BC3AD754A5EA6A93B68D73D02E412BB73CA149FA6BF338";
    private const string SsoPageHash = "4A0DEF115D95CA937443CB0022B0FC2C04B82CC90438163038FA9D76885112C8";
    private const string PlainPolicyHash = "15E41D424D109267AFAE10B1569618484EAB47814033A05A800F570F98C6B3E4";
    private const string SsoPolicyHash = "FD6070467496CD88CFB293C326A54B6C256E226DC82F1E74C4627DA8935F3DD6";

    private static string Sha(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    [Fact]
    public void ExistingVariants_AreByteIdentical()
    {
        Assert.Equal(PlainPageHash, Sha(DarlingWebHostService.BuildLoginPageHtml(false)));
        Assert.Equal(SsoPageHash, Sha(DarlingWebHostService.BuildLoginPageHtml(true)));
        Assert.Equal(PlainPolicyHash, Sha(DarlingWebHostService.LoginPagePolicy(false)));
        Assert.Equal(SsoPolicyHash, Sha(DarlingWebHostService.LoginPagePolicy(true)));
    }

    [Fact]
    public void SsoOnly_HasNoTokenInput_ButKeepsReturnAndSso()
    {
        var html = DarlingWebHostService.BuildLoginPageHtml(true, tokenField: false);
        Assert.DoesNotContain("name='token'", html, StringComparison.Ordinal);
        Assert.DoesNotContain("type='submit'", html, StringComparison.Ordinal);
        Assert.Contains("id='return'", html, StringComparison.Ordinal);
        Assert.Contains("id='sso'", html, StringComparison.Ordinal);
    }

    [Fact]
    public void SsoOnly_PolicyIsComputedFromItsOwnText()
    {
        var policy = DarlingWebHostService.LoginPagePolicy(true, false);
        var hashed = DarlingWebSecurityHeaders.PolicyForInlinePage(DarlingWebHostService.BuildLoginPageHtml(true, false));
        Assert.Equal(hashed.Replace("form-action 'self'", "form-action 'none'", StringComparison.Ordinal), policy);
        Assert.Contains("form-action 'none'", policy, StringComparison.Ordinal);
        Assert.DoesNotContain("form-action 'self'", policy, StringComparison.Ordinal);
        Assert.Contains("frame-ancestors 'none'", policy, StringComparison.Ordinal);
        Assert.Contains("form-action 'self'", DarlingWebHostService.LoginPagePolicy(true), StringComparison.Ordinal);
        Assert.Contains("form-action 'self'", DarlingWebHostService.LoginPagePolicy(false), StringComparison.Ordinal);
    }

    [Fact]
    public void NoTokenField_WithoutOidc_IsTheLocalOnlyPage_WithNeitherTokenFieldNorSsoLink()
    {
        var html = DarlingWebHostService.BuildLoginPageHtml(false, tokenField: false);

        Assert.DoesNotContain("name='token'", html, StringComparison.Ordinal);
        Assert.DoesNotContain("type='submit'", html, StringComparison.Ordinal);
        Assert.DoesNotContain("id='sso'", html, StringComparison.Ordinal);
        Assert.DoesNotContain("/auth/oidc/login", html, StringComparison.Ordinal);
        Assert.Contains("id='return'", html, StringComparison.Ordinal);
        Assert.Contains("only from the machine that runs this service", html, StringComparison.Ordinal);
        Assert.NotEqual(DarlingWebHostService.BuildLoginPageHtml(true, tokenField: false), html);
    }

    [Fact]
    public void NoTokenField_WithoutOidc_PolicyIsComputedFromItsOwnTextAndAllowsNoFormAction()
    {
        var policy = DarlingWebHostService.LoginPagePolicy(false, false);
        var hashed = DarlingWebSecurityHeaders.PolicyForInlinePage(DarlingWebHostService.BuildLoginPageHtml(false, false));

        Assert.Equal(hashed.Replace("form-action 'self'", "form-action 'none'", StringComparison.Ordinal), policy);
        Assert.Contains("form-action 'none'", policy, StringComparison.Ordinal);
        Assert.DoesNotContain("form-action 'self'", policy, StringComparison.Ordinal);
        Assert.NotEqual(DarlingWebHostService.LoginPagePolicy(true, false), policy);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DisabledPage_CarriesTheCode_AndIsFixed(bool oidcEnabled)
    {
        var first = await RenderDisabledAsync(oidcEnabled);
        var second = await RenderDisabledAsync(oidcEnabled);

        Assert.Equal(StatusCodes.Status403Forbidden, first.Status);
        Assert.Contains(DarlingTokenSignIn.RefusalMarkerHtml, first.Body, StringComparison.Ordinal);
        Assert.Equal(oidcEnabled, first.Body.Contains("href='/auth/oidc/login'", StringComparison.Ordinal));
        Assert.Equal(first.Body, second.Body);
        Assert.False(first.HasCookie);
    }

    [Fact]
    public async Task DisabledPage_WithoutOidc_SaysTokenSignInWorksOnlyOnThisMachine()
    {
        var page = await RenderDisabledAsync(oidcEnabled: false);

        Assert.Contains("only from the machine that runs this service", page.Body, StringComparison.Ordinal);
        Assert.DoesNotContain("<a ", page.Body, StringComparison.Ordinal);
    }

    private static async Task<(int Status, string Body, bool HasCookie)> RenderDisabledAsync(bool oidcEnabled)
    {
        var context = new DefaultHttpContext();
        using var body = new MemoryStream();
        context.Response.Body = body;
        await DarlingWebHostService.WriteTokenSignInDisabledAsync(context, StatusCodes.Status403Forbidden, oidcEnabled);
        var text = Encoding.UTF8.GetString(body.ToArray());
        return (context.Response.StatusCode, text, context.Response.Headers.ContainsKey("Set-Cookie"));
    }
}
