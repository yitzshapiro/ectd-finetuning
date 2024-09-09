import pandas as pd
import re

# Define file paths
input_csv_path = 'restructured_train.csv'
output_csv_path = 'train.csv'

# Load the CSV into a DataFrame
df = pd.read_csv(input_csv_path, quoting=1)  # Use quoting=1 to handle quotes in the CSV

# Function to extract and reformat eCTD sections
def extract_sections(text):
    # Extract the eCTD structure part from the text
    structure_match = re.search(r"Given the user's current eCTD structure:(.*?)and the provided text chunk:", text, re.DOTALL)
    if structure_match:
        structure_text = structure_match.group(1)
        # Extract all section numbers using regex from the structure text
        sections = re.findall(r'\d+(?:\.\d+|\.[A-Z])+', structure_text)
        
        # Extract only the first item from each match and remove duplicates
        unique_sections = sorted(set(sections), key=lambda x: [int(i) for i in x.split('.') if i.isdigit()])
        
        # Reformat the list as {1.1, 1.2, 1.3, ...}
        formatted_structure = '{' + ', '.join(unique_sections) + '}'

        # Reconstruct the text with the new format
        updated_text = f"Given the user's current eCTD structure: {formatted_structure} and the provided text chunk:"
        
        # Extract the content after "and the provided text chunk:"
        content_match = re.search(r"and the provided text chunk:(.*?)$", text, re.DOTALL)
        if content_match:
            updated_text += content_match.group(1)
        else:
            # If we can't find the marker, append the entire original text
            updated_text += text
    else:
        # If we can't find the structure part, return the original text
        updated_text = text

    return updated_text

# Apply the function to the 'content' column and prepend 'human: '
df['content'] = 'human: ' + df['content'].apply(extract_sections)

# Save only the 'content' column as train.csv
df['content'].to_csv(output_csv_path, index=False, header=False, quoting=1)  # Use quoting=1 to preserve quotes in the output

print("Processed file saved as train.csv")
