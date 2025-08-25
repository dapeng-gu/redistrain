# Go环境配置脚本
# 设置Go 1.23.6环境变量

# 设置GOROOT指向你的Go安装目录
$env:GOROOT = "D:\env\go_sdk\go1.23.6"

# 设置GOPATH（Go工作空间）
$env:GOPATH = "D:\code\go"

# 将Go的bin目录添加到PATH
$env:PATH = "$env:GOROOT\bin;$env:PATH"

# 设置Go模块代理（加速下载）
$env:GOPROXY = "https://goproxy.cn,direct"

# 设置Go模块校验和数据库
$env:GOSUMDB = "sum.golang.google.cn"

# 启用Go模块
$env:GO111MODULE = "on"

Write-Host "Go环境配置完成！" -ForegroundColor Green
Write-Host "GOROOT: $env:GOROOT" -ForegroundColor Yellow
Write-Host "GOPATH: $env:GOPATH" -ForegroundColor Yellow
Write-Host "PATH已更新，包含Go bin目录" -ForegroundColor Yellow

# 验证Go安装
Write-Host "`n验证Go安装..." -ForegroundColor Cyan
& "$env:GOROOT\bin\go.exe" version

Write-Host "`n当前Go环境信息:" -ForegroundColor Cyan
& "$env:GOROOT\bin\go.exe" env GOROOT
& "$env:GOROOT\bin\go.exe" env GOPATH
& "$env:GOROOT\bin\go.exe" env GOPROXY
