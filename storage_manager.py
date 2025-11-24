import boto3
import requests
import os
import logging
from io import BytesIO
from urllib.parse import urlparse
from botocore.config import Config

logger = logging.getLogger(__name__)

class StorageManager:
    def __init__(self):
        self.endpoint_url = os.getenv('R2_ENDPOINT_URL')
        self.access_key = os.getenv('R2_ACCESS_KEY_ID')
        self.secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
        self.bucket_name = os.getenv('R2_BUCKET_NAME')
        self.public_domain = os.getenv('R2_PUBLIC_DOMAIN')

        if self.access_key and self.secret_key:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(signature_version='s3v4'),
                region_name='auto' # R2 không yêu cầu region cụ thể, nhưng boto3 cần placeholder
            )
        else:
            self.s3_client = None
            logger.warning("R2 Credentials chưa được cấu hình.")

    def process_and_upload_image(self, meta_url, post_id):
        """
        1. Tải ảnh từ Meta URL (có token).
        2. Upload lên R2.
        3. Trả về Public URL vĩnh viễn.
        """
        if not self.s3_client or not meta_url:
            return meta_url # Fallback về link gốc nếu chưa cấu hình

        try:
            # B1: Tải ảnh về RAM
            response = requests.get(meta_url, stream=True, timeout=15)
            if response.status_code != 200:
                logger.error(f"Không thể tải ảnh từ Meta: {meta_url}")
                return meta_url

            # B2: Tạo tên file và metadata
            # Lấy đuôi file (.jpg, .png) từ url gốc, mặc định là .jpg
            path = urlparse(meta_url).path
            ext = os.path.splitext(path)[1]
            if not ext:
                ext = '.jpg'
            
            # Đặt tên file theo Post ID để dễ quản lý và tránh trùng lặp
            file_key = f"posts/{post_id}{ext}"
            
            # ContentType rất quan trọng để trình duyệt hiển thị ảnh thay vì tải xuống
            content_type = response.headers.get('content-type', 'image/jpeg')
            file_obj = BytesIO(response.content)

            # B3: Upload lên R2
            self.s3_client.upload_fileobj(
                file_obj,
                self.bucket_name,
                file_key,
                ExtraArgs={
                    'ContentType': content_type,
                    # 'ACL': 'public-read' # R2 thường quản lý public qua Bucket Policy, dòng này có thể bỏ nếu lỗi
                }
            )

            # B4: Tạo URL vĩnh viễn
            # Xử lý chuẩn hóa domain (bỏ dấu / ở cuối nếu có)
            domain = self.public_domain.rstrip('/')
            if not domain.startswith('http'):
                domain = f'https://{domain}'
            
            permanent_url = f"{domain}/{file_key}"
            
            logger.info(f"Đã upload R2 thành công: {permanent_url}")
            return permanent_url

        except Exception as e:
            logger.error(f"Lỗi khi upload ảnh R2 cho post {post_id}: {e}")
            return meta_url # Fallback về link gốc để không mất dữ liệu