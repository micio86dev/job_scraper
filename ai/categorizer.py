import json
import logging
from typing import Dict, Optional
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

class JobCategorizer:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model

    async def categorize_job(self, title: str, description: str) -> Optional[Dict]:
        """Categorize job details using AI"""
        prompt = f"""
        Analyze the following job posting and extract the required information in JSON format.
        
        Job Title: {title}
        Job Description: {description[:3000]} # Limit description to avoid token limits
        
        Extract the following fields:
        - language: The primary language of the job posting (e.g., "en", "it", "es", "fr", "de")
        - technical_skills: A list of mandatory technical skills (languages, frameworks, tools)
        - seniority: The required seniority level: "Junior", "Mid", "Senior", "Lead", or "Unknown"
        - is_remote: Boolean, true if the job is remote or hybrid
        - formatted_address: The full address of the job location if available, otherwise null
        - city: The city of the job location if available, otherwise null
        - country: The country of the job location if available, otherwise null
        
        Return ONLY valid JSON.
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a professional technical recruiter and data analyst."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            return json.loads(content)
            
        except Exception as e:
            logger.error(f"Error categorizing job with AI: {e}")
            return None
