from faker import Faker
import pandas as pd
import random
import string
from google.cloud import storage

# Initialize Faker
fake = Faker()

# Function to generate a random password
def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for i in range(length))

def generate_random_us_phone_number() -> str:
    # Generate a random phone number
    area_code = fake.numerify("###")
    central_office_code = fake.numerify("###")
    subscriber_number = fake.numerify("####")

    # List of formats
    formats = [
        f"({area_code}) {central_office_code}-{subscriber_number}",  # (123) 456-7890
        f"{area_code}-{central_office_code}-{subscriber_number}",    # 123-456-7890
        f"{area_code}.{central_office_code}.{subscriber_number}",    # 123.456.7890
        f"+1 {area_code}-{central_office_code}-{subscriber_number}"   # +1 123-456-7890
    ]

    # Randomly choose a format
    return random.choice(formats)

# Function to generate dummy employee data
def generate_employee_data(num_employees):
    employee_data = []
    
    for _ in range(num_employees):
        employee = {
            'Employee ID': fake.unique.random_int(min=1000, max=9999),
            'First Name': fake.first_name(),
            'Last Name': fake.last_name(),
            'Email': fake.email(),
            'Phone Number': generate_random_us_phone_number(),
            'Address': fake.address().replace('\n', ', '),  # Replace newline with comma
            'Date of Birth': fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d'),
            'Position': fake.job(),
            'SSN': fake.ssn(),
            'Salary': fake.random_int(min=40000, max=120000),
            'Password': generate_password(),
            'Start Date': fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
        }
        employee_data.append(employee)
    
    return employee_data

# Function to upload file to GCS bucket
def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    # Initialize a storage client
    storage_client = storage.Client()
    
    # Retrieve the bucket
    bucket = storage_client.bucket(bucket_name)
    
    # Create a blob object in the bucket
    blob = bucket.blob(destination_blob_name)
    
    # Upload the file
    blob.upload_from_filename(source_file_path)
    
    print(f"File {source_file_path} uploaded to {bucket_name}/{destination_blob_name}")


if __name__=="__main__":
    # Generate data
    num_records = 999
    employee_data = generate_employee_data(num_records)
    df = pd.DataFrame(employee_data)

    # Export data to CSV
    csv_file_path = "employee_data.csv"
    df.to_csv(csv_file_path, index=False)
    print(f"Data exported to {csv_file_path}")

    # Google Cloud Storage configurations
    bucket_name = "bkt-etl-employee-data"
    destination_blob_name = "employee_data.csv"

    # Upload CSV to GCS bucket
    upload_to_gcs(bucket_name, csv_file_path, destination_blob_name)