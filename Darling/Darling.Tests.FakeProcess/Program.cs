/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

/* The stand-in live process the Darling tests start on Windows under the image name they need.

   One argument that is a whole number of seconds: stay alive that long, then exit 0. A test that needs a
   live postgres.exe or sleeper.exe starts it this way and kills it when it is done.

   Anything else: write the arguments to stderr and exit 1 at once. A test that needs a pg_ctl.exe whose
   every command fails gets that by passing pg_ctl's own arguments. */
if (args.Length == 1
    && int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out var seconds))
{
    Thread.Sleep(TimeSpan.FromSeconds(seconds));
    return 0;
}

Console.Error.WriteLine("fake-process: nothing to do for: " + string.Join(' ', args));
return 1;
