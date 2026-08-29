FROM python:3.11-slim

WORKDIR /app

# نصب وابستگی‌ها
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# کپی کردن تمام فایل‌های پروژه
COPY . .

# اجرای ربات
CMD ["python", "bot.py"]
