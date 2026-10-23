@echo off
rem Install-Darling.cmd - double-click launcher for install-darling.ps1 (#6469).
rem
rem Asks Windows for administrator rights (the UAC prompt), then runs install-darling.ps1 from this folder with
rem the PowerShell execution policy set to Bypass for THAT ONE PROCESS. The machine's execution policy is never
rem changed, and the script file is not unblocked or altered. Any arguments are passed through to the script, for
rem example:  Install-Darling.cmd -Network
rem
rem The window stays open when the installer stops with an error, so the message can be read. The elevated window
rem this launcher opens for itself, or that Explorer opens for Run as administrator, stays open at the end either
rem way, because it would otherwise close with the output in it.
setlocal
set "DARLING_SCRIPT=%~dp0install-darling.ps1"
set "DARLING_LAUNCHER=%~f0"
set "DARLING_OWN_WINDOW="
set "DARLING_ARGS="

if /i "%~1"=="/ownwindow" set "DARLING_OWN_WINDOW=1"
if /i "%~1"=="/ownwindow" shift
rem Explorer's right-click > Run as administrator starts this file as  cmd.exe /c ""<file>""  in a window of its own, so the
rem window would close on success with the installer's last lines in it. A typed run from a console has no /c.
if not defined DARLING_OWN_WINDOW echo %cmdcmdline%| find /i " /c " >nul
if not defined DARLING_OWN_WINDOW if not errorlevel 1 set "DARLING_OWN_WINDOW=1"

:collect
if "%~1"=="" goto :collected
set "DARLING_ARGS=%DARLING_ARGS% %1"
shift
goto :collect
:collected

if exist "%DARLING_SCRIPT%" goto :found
echo install-darling.ps1 was not found beside this file. Extract the whole Darling zip and run this from the extracted folder.
pause
exit /b 1
:found

rem (shift moves %0 too, so the launcher's own path was captured above.)
rem fltmc succeeds only in an elevated session.
fltmc >nul 2>&1
if errorlevel 1 goto :elevate

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%DARLING_SCRIPT%"%DARLING_ARGS%
set "DARLING_RC=%errorlevel%"
if "%DARLING_RC%"=="0" goto :succeeded
echo.
echo The installer stopped with exit code %DARLING_RC%. The message above says why.
pause
exit /b %DARLING_RC%
:succeeded
if defined DARLING_OWN_WINDOW pause
exit /b 0

:elevate
echo Asking Windows for administrator rights to install PerformanceMonitor Darling...
rem The elevated window runs this same file again. cmd /c drops the outer quotes of  ""<file>" /ownwindow <args>".
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "try { Start-Process -FilePath $env:ComSpec -Verb RunAs -ArgumentList ('/c \"\"' + $env:DARLING_LAUNCHER + '\" /ownwindow' + $env:DARLING_ARGS + '\"') } catch { Write-Host ('Could not start the installer elevated: ' + $_.Exception.Message); exit 1 }"
if not errorlevel 1 exit /b 0
echo.
echo The installer was not started. Approve the administrator prompt, or open PowerShell as administrator and run install-darling.ps1.
pause
exit /b 1
