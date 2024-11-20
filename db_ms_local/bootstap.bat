@echo off
setlocal

rem Check if the mssql container is running
docker ps | findstr /I "mssql" >nul
if %ERRORLEVEL% equ 0 (
    rem If mssql container is running, stop the services
    docker compose -f docker-compose.yml down
) else (
    rem If mssql container is not running, start the services
    docker compose -f docker-compose.yml up -d
)

endlocal