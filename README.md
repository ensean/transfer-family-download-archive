

# AWS Transfer Family 文件归档器

本项目包含一个 Lambda 函数，可自动将通过 AWS Transfer Family 下载的文件归档到同一 S3 存储桶中的归档文件夹。

## 工作原理

1. Lambda 函数订阅来自 AWS Transfer Family 的 CloudWatch 日志
2. 当文件上传完成时（通过日志中的 `CLOSE` 活动类型检测）
3. 函数将文件复制到同一 S3 存储桶中的 `archive/` 前缀下

## CloudWatch 日志过滤模式

Lambda 由具有以下过滤模式的 CloudWatch 日志触发：

```
{$.activity-type="CLOSE" && $.bytes-out =*}
```

此模式检测通过 AWS Transfer Family 完成的文件下载。

## 部署

1. 创建 Lambda 函数，内容复制`transfer_archiver.py`
2. 设置 Lambda 函数权限，允许其操作 S3 及其中的文件
3. 创建 Transfer family log group 订阅

