/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Windows.Data;

namespace PerformanceMonitorDashboard.Helpers
{
    /// <summary>
    /// Converts server-time DateTime values for display based on the current TimeDisplayMode setting.
    /// Used by DataGrid DateTime columns to show timestamps in Server Time, Local Time, or UTC.
    /// </summary>
    public class ServerTimeConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is DateTime dt)
            {
                return ServerTimeHelper.ConvertForDisplay(dt, ServerTimeHelper.CurrentDisplayMode)
                    .ToString("yyyy-MM-dd HH:mm:ss");
            }

            return value;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotSupportedException();
        }
    }
}
