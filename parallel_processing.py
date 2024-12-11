import pandas as pd
from collections import Counter
from multiprocessing import Pool
import time

def process_student_payments(student_group):
    student_id, group = student_group
    payment_dates = group["Payment Date"].dropna().tolist()
    if payment_dates:
        date_counts = Counter(payment_dates)
        most_frequent_date = max(date_counts, key=date_counts.get)
        return {
            "Student ID": student_id,
            "Student Name": group.iloc[0]["Student Name"],
            "Payment Dates": payment_dates,
            "Most Frequent Date": most_frequent_date,
        }
    return None

def process_payments_multiprocessing(students, payments):
    merged = pd.merge(students, payments, on="Student ID", how="left")
    student_groups = list(merged.groupby("Student ID"))

    with Pool() as pool:
        results = pool.map(process_student_payments, student_groups)

    return {res["Student ID"]: res for res in results if res is not None}

students = pd.read_csv("students.csv")
payments = pd.read_csv("student_fees.csv")

start_time = time.time()
multiprocessing_result = process_payments_multiprocessing(students, payments)
end_time = time.time()

print("Multiprocessing Processing Result:")
for student_id, details in multiprocessing_result.items():
    print(f"{details['Student Name']} ({student_id}):")
    print(f"  Payment Dates: {details['Payment Dates']}")
    print(f"  Most Frequent Payment Date: {details['Most Frequent Date']}")

print(f"\nTime Taken (Multiprocessing): {end_time - start_time:.2f} seconds")
