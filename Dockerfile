# ใช้ Official Image ของ Playwright เพื่อให้มี Browser สำเร็จรูป
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# สั่งรันสคริปต์หลักเพื่อให้ Prefect สแตนด์บายรอเวลา
CMD ["python", "js100_pipeline.py"]