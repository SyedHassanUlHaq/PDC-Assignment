import pandas as pd
from collections import Counter
import time

def process_payments_serial(students, payments):
    merged = pd.merge(students, payments, on="Student ID", how="left")
    results = {}

    for student_id, group in merged.groupby("Student ID"):
        payment_dates = group["Payment Date"].dropna().tolist()
        if payment_dates:
            date_counts = Counter(payment_dates)
            most_frequent_date = max(date_counts, key=date_counts.get)
            results[student_id] = {
                "Student Name": group.iloc[0]["Student Name"],
                "Payment Dates": payment_dates,
                "Most Frequent Date": most_frequent_date,
            }
    return results


students = pd.read_csv("students.csv")
payments = pd.read_csv("student_fees.csv")


start_time = time.time()
serial_result = process_payments_serial(students, payments)
end_time = time.time()

print("Serial Processing Result:")
for student_id, details in serial_result.items():
    print(f"{details['Student Name']} ({student_id}):")
    print(f"  Payment Dates: {details['Payment Dates']}")
    print(f"  Most Frequent Payment Date: {details['Most Frequent Date']}")

print(f"\nTime Taken (Serial): {end_time - start_time:.2f} seconds")
