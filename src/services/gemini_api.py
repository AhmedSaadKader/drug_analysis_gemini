"""
Gemini API Service Module
Provides interface to Google Gemini AI for content generation
"""
import google.generativeai as genai
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

model = None  # Define global model

def initialize_gemini():
    """Initializes the Gemini API."""
    global model  # Make model accessible
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        raise ValueError("GOOGLE_API_KEY environment variable not set")
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash')

def generate_content(prompt):
    """Generates content using the Gemini API.
    
    Args:
        prompt (str): The prompt to send to Gemini
        
    Returns:
        tuple: (generated_text, usage_metadata)
            - generated_text (str): The generated response text
            - usage_metadata (object): Metadata about token usage
    """
    if model is None:
        raise RuntimeError("Gemini API not initialized. Call initialize_gemini() first.")
    
    try:
        # Generate the response
        response = model.generate_content(prompt)
        
        # Extract the text from the response
        generated_text = response.text
        
        # Get usage metadata if available
        usage_metadata = getattr(response, 'usage_metadata', None)
        
        return generated_text, usage_metadata
    
    except Exception as e:
        print(f"Error generating content: {e}")
        return None, None