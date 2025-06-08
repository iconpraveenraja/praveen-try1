import pandas as pd
from pymongo import MongoClient
import logging
import schedule
import time
import smtplib
from email.message import EmailMessage


EMAIL_ADDRESS = "praveenrajaoncamera@gmail.com"
EMAIL_PASSWORD = "kjcrwolyagzyxisg"
RECEIVER_EMAIL = "praveenrajazp5164@gmail.com"

# Set up logging
logging.basicConfig(level=logging.INFO)

def send_email(subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = RECEIVER_EMAIL

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
        logging.info("✅ Email sent successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to send email: {e}")

def load_csv_to_mongodb():
    try:
        csv_file_path ="C:/Users/prave/Desktop/car_prices.csv"
        db_name = "car_database"
        collection_name = "car_prices"

        df = pd.read_csv(csv_file_path, encoding="utf-8")

        if df.empty:
            raise ValueError("CSV file is empty.")

        data = df.to_dict(orient="records")
        client = MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        collection = db[collection_name]

        collection.delete_many({})
        collection.insert_many(data)

        message = f"✅ car booking Successfully done {len(data)} records into {db_name}.{collection_name}"
        logging.info(message)
        send_email("MongoDB Booking Success", message)

    except Exception as e:
        error_msg = f"❌ Error occurred: {str(e)}"
        logging.error(error_msg)
        send_email("MongoDB Import Failed", error_msg)


schedule.every(1).minutes.do(load_csv_to_mongodb)


if __name__ == "__main__":
    logging.info("Scheduler started. Waiting for scheduled time...")
    while True:
        schedule.run_pending()
        time.sleep(60)
