@echo off
rem ============================================================
rem  ConvAI Cloud Gateway - one-click build script
rem
rem  Usage:  build.bat [clean] [debug^|release] [test]
rem    clean    wipe the build directory before configuring
rem    debug    build with CMAKE_BUILD_TYPE=Debug
rem    release  build with CMAKE_BUILD_TYPE=Release (default)
rem    test     run the full ctest suite after building
rem ============================================================
setlocal EnableDelayedExpansion
cd /d "%~dp0"

set BUILD_DIR=build
set BUILD_TYPE=Release
set CLEAN=0
set RUN_TESTS=0

:parse
if "%~1"=="" goto :done_parse
if /i "%~1"=="clean"   (set CLEAN=1& shift& goto :parse)
if /i "%~1"=="debug"   (set BUILD_TYPE=Debug& shift& goto :parse)
if /i "%~1"=="release" (set BUILD_TYPE=Release& shift& goto :parse)
if /i "%~1"=="test"    (set RUN_TESTS=1& shift& goto :parse)
echo [ERROR] Unknown option: %~1
echo Usage: build.bat [clean] [debug^|release] [test]
exit /b 1
:done_parse

rem ---- toolchain detection ----
where cmake >nul 2>nul
if errorlevel 1 (
    echo [ERROR] cmake not found in PATH. Please install CMake first.
    exit /b 1
)

where g++ >nul 2>nul
if errorlevel 1 (
    rem Fallback: WinLibs MinGW installed via winget
    set "MINGW_BIN=%LOCALAPPDATA%\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe\mingw64\bin"
    if exist "!MINGW_BIN!\g++.exe" (
        set "PATH=!MINGW_BIN!;!PATH!"
        echo [INFO] Using MinGW from !MINGW_BIN!
    ) else (
        echo [ERROR] g++ not found. Install WinLibs: winget install BrechtSanders.WinLibs.POSIX.UCRT
        exit /b 1
    )
)

if "%CLEAN%"=="1" (
    echo [INFO] Cleaning %BUILD_DIR% ...
    if exist "%BUILD_DIR%" rmdir /s /q "%BUILD_DIR%"
)

echo [INFO] Configuring (CMAKE_BUILD_TYPE=%BUILD_TYPE%) ...
cmake -B "%BUILD_DIR%" -S . -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER=g++ -DCMAKE_BUILD_TYPE=%BUILD_TYPE%
if errorlevel 1 goto :fail

echo [INFO] Building ...
cmake --build "%BUILD_DIR%" -j %NUMBER_OF_PROCESSORS%
if errorlevel 1 goto :fail

if "%RUN_TESTS%"=="1" (
    echo [INFO] Running tests ...
    ctest --test-dir "%BUILD_DIR%" --output-on-failure
    if errorlevel 1 goto :fail
)

echo.
echo [OK] Build succeeded: %BUILD_DIR%\cloud_gateway.exe
exit /b 0

:fail
echo.
echo [FAIL] Build failed.
exit /b 1
