import os
import csv
from pypdf import PdfReader
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Function to extract text from a PDF file with error handling
def extract_text_from_pdf(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        return text
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        return ""

# Function to get the first 300 words of text
def get_first_300_words(text):
    words = text.split()
    return " ".join(words[:300])

# Function to get a random selection of sections, including the eCTD_section
def get_random_ectd_sections(eCTD_section, full_structure):
    # Ensure the eCTD_section is in the list
    if eCTD_section not in full_structure:
        print(f"Error: {eCTD_section} not found in eCTD structure")
        return None
    
    # Copy the structure to avoid mutating the original list
    sections = full_structure.copy()

    # Remove the current eCTD section temporarily for random selection
    sections.remove(eCTD_section)

    # Randomly select sections and add the current section back
    num_sections = random.randint(10, len(full_structure))  # Between 10 and full length
    random_sections = random.sample(sections, num_sections - 1)
    random_sections.append(eCTD_section)

    # Sort the final list to maintain order
    random_sections.sort(key=lambda x: full_structure.index(x))

    return random_sections

# Function to process a single PDF
def process_pdf(pdf_path, full_ectd_structure):
    # Extract the eCTD section from the filename
    eCTD_section = os.path.basename(pdf_path).replace('.pdf', '')

    # Extract the text and first 300 words
    text = extract_text_from_pdf(pdf_path)
    if not text:
        return None  # Skip if no text could be extracted

    first_300_words = get_first_300_words(text)
    first_300_words = first_300_words.replace(eCTD_section, '')

    # Get a random selection of sections, including the current eCTD_section
    selected_sections = get_random_ectd_sections(eCTD_section, full_ectd_structure)
    if not selected_sections:
        print(f"Error: {eCTD_section} not found in eCTD structure")
        return None
    
    # Convert selected_sections to a string without quotes
    selected_sections_str = ", ".join(selected_sections)

    # Format the output string
    output_text = (
        f"human: Given the user's current eCTD structure: [{selected_sections_str}] "
        f"and the provided text chunk: {first_300_words} bot: {eCTD_section}"
    )

    return output_text

# Function to process all PDFs in a directory and its subdirectories concurrently
def process_pdfs_concurrently(directory, output_csv, full_ectd_structure, max_workers=8):
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["text"])  # Single column labeled "text"

        pdf_paths = []
        for root, _, files in os.walk(directory):
            for filename in files:
                if filename.endswith('.pdf'):
                    pdf_paths.append(os.path.join(root, filename))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_pdf, pdf_path, full_ectd_structure) for pdf_path in pdf_paths]
            for future in as_completed(futures):
                output_text = future.result()
                if output_text:
                    writer.writerow([output_text])

# Specify the directory containing PDFs and the output CSV file
pdf_directory = "dataset-raw"
output_csv_file = "dataset-processed.csv"

full_ectd_structure = [
    "1.1", "1.2", "1.3", "1.3.1", "1.3.1.1", "1.3.1.2", "1.3.1.3", "1.3.1.4", 
    "1.3.1.5", "1.3.2", "1.3.3", "1.3.4", "1.3.5", "1.3.5.1", "1.3.5.2", 
    "1.3.5.3", "1.3.6", "1.4", "1.4.1", "1.4.2", "1.4.3", "1.4.4", "1.5", 
    "1.5.1", "1.5.2", "1.5.3", "1.5.4", "1.5.5", "1.5.6", "1.5.7", "1.6", 
    "1.6.1", "1.6.2", "1.6.3", "1.7", "1.7.1", "1.7.2", "1.7.3", "1.7.4", 
    "1.8", "1.8.1", "1.8.2", "1.8.3", "1.8.4", "1.9", "1.9.1", "1.9.2", 
    "1.9.3", "1.9.4", "1.9.5", "1.9.6", "1.10", "1.10.1", "1.10.2", "1.11", 
    "1.11.1", "1.11.2", "1.11.3", "1.11.4", "1.12", "1.12.1", "1.12.2", 
    "1.12.3", "1.12.4", "1.12.5", "1.12.6", "1.12.7", "1.12.8", "1.12.9", 
    "1.12.10", "1.12.11", "1.12.12", "1.12.13", "1.12.14", "1.12.15", 
    "1.12.16", "1.12.17", "1.13", "1.13.1", "1.13.2", "1.13.3", "1.13.4", 
    "1.13.5", "1.13.6", "1.13.7", "1.13.8", "1.13.9", "1.13.10", "1.13.11", 
    "1.13.12", "1.13.13", "1.13.14", "1.13.15", "1.14", "1.14.1", "1.14.1.1", 
    "1.14.1.2", "1.14.1.3", "1.14.1.4", "1.14.1.5", "1.14.2", "1.14.2.1", 
    "1.14.2.2", "1.14.2.3", "1.14.3", "1.14.3.1", "1.14.3.2", "1.14.3.3", 
    "1.14.4", "1.14.4.1", "1.14.4.2", "1.14.5", "1.14.6", "1.15", "1.15.1", 
    "1.15.1.1", "1.15.1.2", "1.15.1.3", "1.15.1.4", "1.15.1.5", "1.15.1.6", 
    "1.15.1.7", "1.15.1.8", "1.15.1.9", "1.15.1.10", "1.15.1.11", "1.15.2", 
    "1.15.2.1", "1.15.2.1.1", "1.15.2.1.2", "1.15.2.1.3", "1.15.2.1.4", 
    "1.16", "1.16.1", "1.16.2", "1.16.2.1", "1.16.2.2", "1.16.2.3", 
    "1.16.2.4", "1.16.2.5", "1.16.2.6", "1.17", "1.17.1", "1.17.2", "1.18", 
    "1.19", "1.20", "2.2", "2.3", "2.4", "2.5", "2.6", "2.6.1", "2.6.2", 
    "2.6.3", "2.6.4", "2.6.5", "2.6.6", "2.6.7", "2.7", "2.7.1", "2.7.2", 
    "2.7.3", "2.7.4", "2.7.5", "2.7.6", "3.2.P", "3.2.R", "3.2", "3.2.A", 
    "3.2.S", "3.2.S.1", "3.2.P.1", "3.2.A.1", "3.2.S.2", "3.2.P.2", "3.2.A.2", 
    "3.2.A.3", "3.2.S.3", "3.2.P.3", "3.2.S.4", "3.2.P.4", "3.2.S.4.1", 
    "3.2.S.4.2", "3.2.S.4.3", "3.2.S.4.4", "3.2.S.4.5", "3.2.S.5", "3.2.P.5", 
    "3.2.P.5.1", "3.2.P.5.2", "3.2.P.5.3", "3.2.P.5.4", "3.2.P.5.5", 
    "3.2.P.5.6", "3.2.S.6", "3.2.P.6", "3.2.P.7", "3.2.S.7", "3.2.P.8", "3.3", 
    "4.2", "4.2.1", "4.2.1.1", "4.2.1.2", "4.2.1.3", "4.2.1.4", "4.2.2", 
    "4.2.2.1", "4.2.2.2", "4.2.2.3", "4.2.2.4", "4.2.2.5", "4.2.2.6", 
    "4.2.2.7", "4.2.3", "4.2.3.1", "4.2.3.2", "4.2.3.3", "4.2.3.4", 
    "4.2.3.5", "4.2.3.6", "4.2.3.7", "4.2.3.8", "4.2.3.9", "4.2.3.10", 
    "4.2.3.11", "4.2.3.12", "4.2.3.13", "4.2.3.14", "4.2.3.15", "4.2.3.16", 
    "4.2.3.17", "4.2.3.18", "4.2.3.19", "4.2.3.20", "4.2.3.21", "4.2.3.22", 
    "4.2.3.23", "4.3", "5.2", "5.3", "5.3.1", "5.3.1.1", "5.3.1.2", "5.3.1.3", 
    "5.3.1.4", "5.3.2", "5.3.2.1", "5.3.2.2", "5.3.2.3", "5.3.3", "5.3.3.1", 
    "5.3.3.2", "5.3.3.3", "5.3.3.4", "5.3.3.5", "5.3.4", "5.3.4.1", "5.3.4.2", 
    "5.3.3.3", "5.3.3.4", "5.3.3.5", "5.3.4", "5.3.4.1", "5.3.4.2", "5.3.5", 
    "5.3.5.1", "5.3.5.2", "5.3.5.3", "5.3.5.4", "5.3.6", "5.4"
]

process_pdfs_concurrently(pdf_directory, output_csv_file, full_ectd_structure)
