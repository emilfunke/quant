from data_preparation import read_databento

def main():
    job_id = 'your_job_id_here'  # Replace with your actual job ID
    df = read_databento(job_id)
    print(df.head())  # Display the first few rows of the DataFrame

if __name__ == "__main__":
    main()