@echo off
rem Idempotent :8092 auto-start for the NBA Hub backend.
netstat -ano | findstr ":8092" | findstr "LISTENING" >nul
if %errorlevel%==0 (
  echo NBA backend already running on :8092
  exit /b 0
)
start "NBA-Backend" /min node server.js
echo Started NBA backend on http://localhost:8092
