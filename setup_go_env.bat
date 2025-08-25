@echo off
REM Go环境配置批处理脚本
REM 设置Go 1.23.6环境变量

echo 正在配置Go环境...

REM 设置GOROOT指向你的Go安装目录
set GOROOT=D:\env\go_sdk\go1.23.6

REM 设置GOPATH（Go工作空间）
set GOPATH=D:\code\go

REM 将Go的bin目录添加到PATH
set PATH=%GOROOT%\bin;%PATH%

REM 设置Go模块代理（加速下载）
set GOPROXY=https://goproxy.cn,direct

REM 设置Go模块校验和数据库
set GOSUMDB=sum.golang.google.cn

REM 启用Go模块
set GO111MODULE=on

echo Go环境配置完成！
echo GOROOT: %GOROOT%
echo GOPATH: %GOPATH%
echo PATH已更新，包含Go bin目录

echo.
echo 验证Go安装...
"%GOROOT%\bin\go.exe" version

echo.
echo 当前Go环境信息:
"%GOROOT%\bin\go.exe" env GOROOT
"%GOROOT%\bin\go.exe" env GOPATH
"%GOROOT%\bin\go.exe" env GOPROXY

pause
