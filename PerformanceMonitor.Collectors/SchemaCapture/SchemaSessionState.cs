/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What an ensure of the schema-changes session left (#7643). <see cref="Running"/> is whether the session runs now.
/// <see cref="CreateTimeUtc"/> is when it last started, which moves on every start: a pass that stored the last value sees a
/// restart, and with it a gap in what the session saw. <see cref="FaultMessage"/> is set when the ensure failed, and
/// <see cref="FaultIsPermission"/> says the failure was a permission denial.
/// </summary>
public sealed record SchemaSessionState(bool Running, DateTime? CreateTimeUtc, string? FaultMessage, bool FaultIsPermission);
