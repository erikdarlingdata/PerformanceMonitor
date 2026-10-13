/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Keeps the address of the connection itself in <see cref="HttpContext.Items"/>, ahead of the
/// forwarded-headers middleware that replaces <c>RemoteIpAddress</c> with the forwarded client. Only that
/// connection address can be loopback for the <c>allowFrom</c> exemption
/// (<see cref="DarlingWebHostService.IsLoopbackConnection"/>). It decides nothing about a request.
/// </summary>
internal sealed class DarlingConnectionAddressStamp
{
    private readonly RequestDelegate _next;

    public DarlingConnectionAddressStamp(RequestDelegate next)
    {
        _next = next;
    }

    public Task InvokeAsync(HttpContext context)
    {
        context.Items[DarlingWebHostService.ConnectionIpItemKey] = context.Connection.RemoteIpAddress;
        return _next(context);
    }
}
