/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorLite.Tests;

/// <summary>SqlException has no public constructor; this builds one through the driver's internals.</summary>
internal static class SqlExceptionFactory
{
    public static SqlException Create(int number, byte errorClass = 20, string message = "message")
    {
        const BindingFlags all = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static;
        var errorCtor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length).First();
        var ints = new Queue<object>(new object[] { number, 0, 0, 0 });
        var bytes = new Queue<object>(new object[] { (byte)0, errorClass, (byte)0, (byte)0 });
        var strings = new Queue<object>(new object[] { "server", message, "procedure", "extra", "extra" });
        var args = errorCtor.GetParameters().Select(p =>
            p.ParameterType == typeof(int) ? ints.Dequeue()
            : p.ParameterType == typeof(byte) ? bytes.Dequeue()
            : p.ParameterType == typeof(string) ? strings.Dequeue()
            : p.ParameterType == typeof(uint) ? (object)0u
            : p.ParameterType == typeof(Exception) ? null!
            : p.HasDefaultValue ? p.DefaultValue! : throw new InvalidOperationException($"unmapped SqlError ctor parameter {p.ParameterType}")).ToArray();
        var error = errorCtor.Invoke(args);

        var collection = Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
        typeof(SqlErrorCollection).GetMethod("Add", all, null, new[] { typeof(SqlError) }, null)!.Invoke(collection, new[] { error });

        var create = typeof(SqlException).GetMethods(all)
            .Where(m => m.Name == "CreateException" && m.GetParameters().Length >= 2
                && m.GetParameters()[0].ParameterType == typeof(SqlErrorCollection)
                && m.GetParameters()[1].ParameterType == typeof(string))
            .OrderBy(m => m.GetParameters().Length).First();
        var createArgs = create.GetParameters().Select((p, i) =>
            i == 0 ? collection
            : i == 1 ? "11.0"
            : p.ParameterType == typeof(Guid) ? Guid.Empty
            : p.HasDefaultValue ? p.DefaultValue! : null!).ToArray();
        return (SqlException)create.Invoke(null, createArgs)!;
    }
}
