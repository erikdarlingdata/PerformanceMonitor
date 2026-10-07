/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The pair that identifies one assumed AWS role (#5452): the role ARN and the external ID sent with it, if any. Two
/// servers with the same pair share one assumed-role session. A record struct, so equality is by value and ordinal:
/// an ARN and an external ID are case-sensitive.
/// </summary>
public readonly record struct AwsRoleKey(string RoleArn, string? ExternalId);
