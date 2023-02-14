import os
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import json
from flask import Flask


# Get the credentials for the service account
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds_json = os.environ.get('CREDS_JSON')
creds_dict = json.loads(creds_json)
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

# Authenticate to Google Sheets API
gc = gspread.authorize(credentials)

# Open the Google Sheets spreadsheet
sh = gc.open_by_key(os.environ.get('SPREADSHEET_KEY'))

# Select the worksheet
worksheet = sh.sheet1

# Load the Excel file into a pandas DataFrame
# Get the data from the worksheet
df = pd.DataFrame(worksheet.get_all_records())

df.replace("", np.nan, inplace=True)
df.fillna(method='ffill', inplace=True)

# Create the root element for the XML file
root = ET.Element("conversations")

# Group the data by Conversation ID and Prompt
grouped = df.groupby(["Conversation ID", "Prompt"])

# Loop through the groups and create a conversation element for each group
for (conversation_id, prompt), group in grouped:
    conversation = ET.SubElement(root, "conversation", id=str(conversation_id))
    prompt_element = ET.SubElement(conversation, "prompt")
    prompt_element.text = prompt
    
    response_options = ET.SubElement(conversation, "response_options")
    
    # Group the data by Classification
    classification_grouped = group.groupby("Classification")
    
    # Loop through the classification groups and create a response element for each group
    for classification, classification_group in classification_grouped:
        response = ET.SubElement(response_options, "response", classification=classification)
        
        # Get the feedback and next_conversation_id values for this classification group
        feedback = classification_group["Feedback"].iloc[0]
        next_conversation_id = classification_group["Next Conversation ID"].iloc[0] if not pd.isna(classification_group["Next Conversation ID"].iloc[0]) else None
        
        # Loop through each row in the classification group and add a text element
        for index, row in classification_group.iterrows():
            text = ET.SubElement(response, "text")
            text.text = row["Text"]
        
        # Add the feedback element for this classification group
        feedback_element = ET.SubElement(response, "feedback")
        feedback_element.text = feedback
        
        # Check if there is a Next Conversation ID for this classification group
        if next_conversation_id is not None:
            next_conversation_id_element = ET.SubElement(response, "next_conversation_id")
            next_conversation_id_element.text = str(next_conversation_id)

# Write the XML file
tree = ET.ElementTree(root)
fname = "output.xml"
tree.write(fname, encoding="utf-8", xml_declaration=True)

# Create a Flask application instance
app = Flask(__name__)

# Define a route that will handle the HTTP GET request and return the XML file
@app.route('/xml')
def get_xml():

    with open(fname, 'r') as file:
        xml = file.read()
    return Response(xml, mimetype='text/xml')