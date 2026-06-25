/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// Constants for server authentication types.
/// </summary>
public static class AuthenticationTypes
{
    /// <summary>
    /// Windows integrated authentication.
    /// </summary>
    public const string Windows = "Windows";

    /// <summary>
    /// SQL Server username/password authentication.
    /// </summary>
    public const string SqlServer = "SqlServer";

    /// <summary>
    /// Microsoft Entra MFA (Azure AD) interactive authentication.
    /// </summary>
    public const string EntraMFA = "EntraMFA";

    /// <summary>
    /// Microsoft Entra service principal (client id + secret) — non-interactive.
    /// Maps to SqlAuthenticationMethod.ActiveDirectoryServicePrincipal.
    /// </summary>
    public const string ServicePrincipal = "ServicePrincipal";

    /// <summary>
    /// Azure managed identity (system- or user-assigned) — non-interactive.
    /// Maps to SqlAuthenticationMethod.ActiveDirectoryManagedIdentity.
    /// Only works when the app runs on an Azure resource with a managed identity.
    /// </summary>
    public const string ManagedIdentity = "ManagedIdentity";
}
