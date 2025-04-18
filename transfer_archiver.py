import json
import boto3
import gzip
import base64
import os
import logging
from urllib.parse import unquote_plus

# 配置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 初始化 AWS 客户端
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    处理来自 AWS Transfer Family 的 CloudWatch 日志的 Lambda 函数，
    并根据日志事件将文件归档到 S3。
    """
    logger.info("收到事件: %s", json.dumps(event))
    
    # CloudWatch 日志事件是经过压缩并以 base64 编码的
    cw_data = event.get('awslogs', {}).get('data', '')
    if not cw_data:
        logger.error("在事件中未找到 CloudWatch 日志数据")
        return {
            'statusCode': 400,
            'body': '未找到 CloudWatch 日志数据'
        }
    
    # 解码并解压日志数据
    compressed_payload = base64.b64decode(cw_data)
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_data = json.loads(uncompressed_payload)
    
    logger.info("解码后的日志数据: %s", json.dumps(log_data))
    
    # 处理每个日志事件
    for log_event in log_data.get('logEvents', []):
        try:
            process_log_event(log_event)
        except Exception as e:
            logger.error(f"处理日志事件时出错: {str(e)}")
            logger.error(f"日志事件: {json.dumps(log_event)}")
    
    return {
        'statusCode': 200,
        'body': f"已处理 {len(log_data.get('logEvents', []))} 个日志事件"
    }

def process_log_event(log_event):
    """
    处理来自 Transfer Family 的单个 CloudWatch 日志事件。
    查找文件下载事件并将下载的文件移动到归档文件夹。
    """
    message = log_event.get('message', '')
    logger.info(f"处理日志消息: {message}")
    
    # 将日志消息解析为 JSON
    message = json.loads(message)

    # 检查这是否是文件下载事件
    # 示例日志消息格式（可能需要根据实际日志进行调整）：
    # "Uploaded file /bucket-name/path/to/file.txt"
    if "bytes-out" in message.keys():
        try:
            # 从日志消息中提取文件信息
            # 此解析逻辑可能需要根据实际日志格式进行调整
            file_info = extract_file_info(message)
            
            if file_info:
                source_bucket = file_info['bucket']
                source_key = file_info['key']
                
                # 定义归档目标
                archive_key = f"archive/{source_key}"
                
                # 将文件复制到归档位置
                logger.info(f"正在将 {source_bucket}/{source_key} 复制到 {source_bucket}/{archive_key}")
                s3_client.copy_object(
                    Bucket=source_bucket,
                    Key=archive_key,
                    CopySource={'Bucket': source_bucket, 'Key': source_key}
                )
                
                logger.info(f"已成功将文件归档到 {archive_key}")
                s3_client.delete_object(
                    Bucket=source_bucket,
                    Key=source_key,
                )
                logger.info(f"已成功删除源文件 {source_key}")

        except Exception as e:
            logger.error(f"处理文件时出错: {str(e)}")
            raise

def extract_file_info(message):
    """
    从日志消息中提取存储桶和键信息。
    此函数需要根据 Transfer Family 日志的实际格式进行自定义。
    """
    # 示例实现 - 根据实际日志格式进行调整
    try:
        # 在日志消息中查找模式
        if "bytes-out" in message.keys():
            # 示例："path /bucket-name/path/to/file.txt"
            parts = message.get('path','')
            if parts.startswith('/'):
                parts = parts[1:]  # 删除前导斜杠
            
            # 拆分为存储桶和键
            bucket_and_key = parts.split('/', 1)
            if len(bucket_and_key) == 2:
                return {
                    'bucket': bucket_and_key[0],
                    'key': bucket_and_key[1]
                }
    except Exception as e:
        logger.error(f"从消息中提取文件信息时出错: {str(e)}")
        logger.error(f"消息: {message}")
    
    return None
