# Go环境配置说明

## 环境配置文件

我为你创建了以下配置文件：

### 1. PowerShell配置脚本 (`setup_go_env.ps1`)
在PowerShell中运行：
```powershell
.\setup_go_env.ps1
```

### 2. 批处理配置脚本 (`setup_go_env.bat`)
在命令提示符中运行：
```cmd
setup_go_env.bat
```

### 3. VS Code配置 (`.vscode/settings.json`)
自动配置VS Code的Go环境设置

## 环境变量配置

- **GOROOT**: `D:\env\go_sdk\go1.23.6`
- **GOPATH**: `D:\code\go`
- **GOPROXY**: `https://goproxy.cn,direct` (国内加速)
- **GOSUMDB**: `sum.golang.google.cn`
- **GO111MODULE**: `on`

## 使用方法

1. 运行配置脚本设置环境变量
2. 重启VS Code或IDE以加载新的环境配置
3. 在项目目录中运行Go命令：
   ```bash
   go version
   go mod tidy
   go run main.go
   ```

## 项目模块说明

当前项目包含多个Go模块：
- `01_redis_basics/` - Redis基础练习 (Go 1.21)
- `02_advanced_redis/step1_task_storage/` - 高级Redis应用
- `asynq/` - Asynq任务队列库 (Go 1.22)

每个模块都有独立的`go.mod`文件，可以单独构建和运行。

## 验证安装

运行以下命令验证Go环境：
```bash
go version
go env GOROOT
go env GOPATH
```
