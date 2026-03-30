@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo.
echo ========================================
echo  Building Full Edition (Dashboard + Installers)
echo ========================================
echo.

:: Get version from csproj
for /f %%a in ('powershell -Command "([xml](Get-Content Dashboard\Dashboard.csproj)).Project.PropertyGroup.Version | Where-Object { $_ }"') do set VERSION=%%a
echo Version: %VERSION%
echo.

if not exist "releases" mkdir releases

:: ----------------------------------------
:: Dashboard
:: ----------------------------------------
echo [1/3] Publishing Dashboard...
dotnet publish Dashboard\Dashboard.csproj -c Release -o publish\Dashboard

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Dashboard build failed!
    exit /b 1
)

echo Creating Dashboard ZIP...
set DASH_ZIP=PerformanceMonitorDashboard-%VERSION%.zip
if exist "releases\%DASH_ZIP%" del "releases\%DASH_ZIP%"
powershell -Command "Compress-Archive -Path 'publish\Dashboard\*' -DestinationPath 'releases\%DASH_ZIP%' -Force"
echo.

:: ----------------------------------------
:: CLI Installer
:: ----------------------------------------
echo [2/2] Publishing CLI Installer...
dotnet publish Installer\PerformanceMonitorInstaller.csproj -c Release

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: CLI Installer build failed!
    exit /b 1
)
echo.

:: ----------------------------------------
:: Package Installer + SQL into ZIP
:: ----------------------------------------
echo Packaging Installer + SQL scripts...
set INST_DIR=publish\Installer
if exist "%INST_DIR%" rmdir /S /Q "%INST_DIR%"
mkdir "%INST_DIR%"
mkdir "%INST_DIR%\install"
mkdir "%INST_DIR%\upgrades"

copy "Installer\bin\Release\net8.0\win-x64\publish\PerformanceMonitorInstaller.exe" "%INST_DIR%\" >nul
copy "install\*.sql" "%INST_DIR%\install\" >nul
xcopy "upgrades" "%INST_DIR%\upgrades\" /E /I /Q >nul 2>&1
if exist README.md copy README.md "%INST_DIR%\" >nul
if exist LICENSE copy LICENSE "%INST_DIR%\" >nul
if exist THIRD_PARTY_NOTICES.md copy THIRD_PARTY_NOTICES.md "%INST_DIR%\" >nul

set INST_ZIP=PerformanceMonitorInstaller-%VERSION%.zip
if exist "releases\%INST_ZIP%" del "releases\%INST_ZIP%"
powershell -Command "Compress-Archive -Path 'publish\Installer\*' -DestinationPath 'releases\%INST_ZIP%' -Force"

echo.
echo ========================================
echo  Build Complete!
echo ========================================
echo.
echo Output:
echo   releases\%DASH_ZIP%
echo   releases\%INST_ZIP%
echo.

for %%A in ("releases\%DASH_ZIP%") do echo Dashboard size:  %%~zA bytes
for %%A in ("releases\%INST_ZIP%") do echo Installer size:  %%~zA bytes

endlocal
