import os
import pandas as pd
import re

def read_database(country_code):
    database_path = f"data/{country_code}/{country_code}.csv"
    return pd.read_csv(database_path)

def process_leads(country_code):
    leads_folder = f"leads/{country_code}/"
    output_folder = f"results/{country_code}/"

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Read database for the specific country
    try:
        database_df = read_database(country_code)

        # Check if 'email' column exists in the database
        if 'email' not in database_df.columns:
            print(f"No 'email' column found in the database for {country_code}.")
            return

        # Check if leads folder is empty
        if not os.listdir(leads_folder):
            print(f"No leads found for {country_code}.")
            return

        # Iterate through lead files in the leads folder
        for lead_file in os.listdir(leads_folder):
            lead_path = os.path.join(leads_folder, lead_file)

            # Read leads file into a DataFrame
            leads_df = pd.read_csv(lead_path)

            # Initialize an empty DataFrame for matching rows
            matching_rows = pd.DataFrame()

            # Check if 'email' column exists in the leads file
            if 'email' not in leads_df.columns:
                print(f"No 'email' column found in {lead_file}.")
                continue

            # Iterate through each lead and check if it matches any row in the database
            for index, lead_row in leads_df.iterrows():
                email_pattern = re.compile(re.escape(str(lead_row['email'])), flags=re.IGNORECASE)
                phone_pattern = re.compile(re.sub(r'\D', '', str(lead_row['phone'])))
                
                email_matches = database_df['email'].str.contains(email_pattern)
                phone_matches = database_df.apply(lambda x: any(re.search(phone_pattern, str(entry)) for entry in x), axis=1)
                
                lead_match = database_df[email_matches | phone_matches]
                matching_rows = pd.concat([matching_rows, lead_match])

            if not matching_rows.empty:
                # Leads found, write to output folder
                output_path = os.path.join(output_folder, f"{os.path.splitext(lead_file)[0]}_output.csv")
                matching_rows.to_csv(output_path, index=False)
                total_leads = len(leads_df)
                matched_leads = len(matching_rows)
                print(f"Matching leads found for {lead_file} in {country_code} database: {matched_leads}/{total_leads} leads matched.")
            else:
                print(f"No matching leads found for {lead_file} in {country_code} database.")

    except FileNotFoundError:
        print(f"No database found for {country_code}.")

# Function to add '+' to phone numbers in the output files
def add_plus_to_phone_numbers(output_folder):
    # Iterate through output files in the specified folder
    for country_code in os.listdir(output_folder):
        country_folder = os.path.join(output_folder, country_code)
        if os.path.isdir(country_folder):
            for output_file in os.listdir(country_folder):
                if output_file.endswith("_output.csv"):
                    output_path = os.path.join(country_folder, output_file)

                    # Read the output CSV file
                    output_df = pd.read_csv(output_path)

                    # Add '+' to the phone numbers in the 'telefonInternational' column
                    output_df['telefonInternational'] = '+' + output_df['telefonInternational'].astype(str)

                    # Save the updated DataFrame back to the CSV file
                    output_df.to_csv(output_path, index=False)

                    print(f"Added '+' to phone numbers in {output_file}")

# Process leads for each country
countries = ['ES', 'UK', 'BENL', 'BEFR', 'DE', 'PL', 'SE']

for country_code in countries:
    process_leads(country_code)

# Add '+' to phone numbers in the output files
output_folder = "results/"
add_plus_to_phone_numbers(output_folder)
