import asyncio
from typing import Annotated, List, Dict, Optional
import os
import json
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.auth.providers.bearer import BearerAuthProvider, RSAKeyPair
from mcp import ErrorData, McpError
from mcp.server.auth.provider import AccessToken
from mcp.types import TextContent, ImageContent, INVALID_PARAMS, INTERNAL_ERROR
from pydantic import BaseModel, Field

# Load environment variables
load_dotenv()

TOKEN = os.environ.get("AUTH_TOKEN")
MY_NUMBER = os.environ.get("MY_NUMBER")

assert TOKEN is not None, "Please set AUTH_TOKEN in your .env file"
assert MY_NUMBER is not None, "Please set MY_NUMBER in your .env file"

# Auth Provider
class SimpleBearerAuthProvider(BearerAuthProvider):
    def __init__(self, token: str):
        k = RSAKeyPair.generate()
        super().__init__(public_key=k.public_key, jwks_uri=None, issuer=None, audience=None)
        self.token = token

    async def load_access_token(self, token: str) -> AccessToken | None:
        if token == self.token:
            return AccessToken(
                token=token,
                client_id="puch-client",
                scopes=["*"],
                expires_at=None,
            )
        return None

# Rich Tool Description model
class RichToolDescription(BaseModel):
    description: str
    use_when: str
    side_effects: str | None = None

# Initialize Simple Database
def setup_simple_database():
    conn = sqlite3.connect('job_marketplace_simple.db')
    cursor = conn.cursor()
    
    # Drop existing tables
    cursor.execute('DROP TABLE IF EXISTS job_providers')
    cursor.execute('DROP TABLE IF EXISTS job_seekers')
    cursor.execute('DROP TABLE IF EXISTS job_requests')
    
    # Job Providers (Workers) Table
    cursor.execute('''
        CREATE TABLE job_providers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE,
            name TEXT,
            phone TEXT,
            skills TEXT,
            location TEXT,
            city TEXT,
            experience TEXT,
            rate TEXT,
            available INTEGER DEFAULT 1,
            created_at TEXT
        )
    ''')
    
    # Job Seekers (Customers) Table
    cursor.execute('''
        CREATE TABLE job_seekers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE,
            name TEXT,
            phone TEXT,
            location TEXT,
            city TEXT,
            created_at TEXT
        )
    ''')
    
    # Job Requests Table
    cursor.execute('''
        CREATE TABLE job_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seeker_id TEXT,
            job_type TEXT,
            description TEXT,
            location TEXT,
            status TEXT DEFAULT 'open',
            created_at TEXT
        )
    ''')
    
    # Insert sample job providers
    sample_providers = [
        ('provider_001', 'Rajesh Kumar', '9876543210', 'plumber bathroom repair pipe fixing', 'Andheri West', 'Mumbai', '5 years', '500 per day', 1),
        ('provider_002', 'Suresh Patel', '9876543211', 'electrician wiring ac repair', 'Bandra East', 'Mumbai', '3 years', '400 per day', 1),
        ('provider_003', 'Amit Sharma', '9876543212', 'painter wall painting interior', 'Powai', 'Mumbai', '7 years', '600 per day', 1),
        ('provider_004', 'Ramesh Yadav', '9876543213', 'plumber pipeline bathroom fitting', 'Hitech City', 'Hyderabad', '6 years', '550 per day', 1),
        ('provider_005', 'Krishna Reddy', '9876543214', 'electrician home wiring electrical', 'Gachibowli', 'Hyderabad', '8 years', '700 per day', 1)
    ]
    
    for provider in sample_providers:
        cursor.execute('''
            INSERT INTO job_providers (user_id, name, phone, skills, location, city, experience, rate, available, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', provider + (datetime.now().isoformat(),))
    
    conn.commit()
    conn.close()

# Initialize database
setup_simple_database()

# MCP Server Setup
mcp = FastMCP(
    "Basic Job Marketplace - Two Channels",
    auth=SimpleBearerAuthProvider(TOKEN),
)

# Tool: validate
@mcp.tool
async def validate() -> str:
    return MY_NUMBER

# Job Provider Registration
JobProviderRegDescription = RichToolDescription(
    description="Register as job provider or worker to offer services in job marketplace",
    use_when="Use ONLY when someone explicitly wants to register as worker, job provider, or offer services for jobs",
    side_effects="Creates job provider profile in marketplace database",
)

@mcp.tool(description=JobProviderRegDescription.model_dump_json())
async def register_job_provider(
    puch_user_id: Annotated[str, Field(description="User unique ID")],
    provider_name: Annotated[str, Field(description="Full name")],
    phone: Annotated[str, Field(description="Phone number")], 
    services: Annotated[str, Field(description="Services offered")],
    work_location: Annotated[str, Field(description="Work location")],
    city: Annotated[str, Field(description="City")],
    experience: Annotated[str, Field(description="Experience details")] = "1 year",
    daily_rate: Annotated[str, Field(description="Daily rate")] = "400 per day"
) -> str:
    """Register as job provider in marketplace"""
    
    try:
        conn = sqlite3.connect('job_marketplace_simple.db')
        cursor = conn.cursor()
        
        # Check if already registered
        cursor.execute('SELECT name FROM job_providers WHERE user_id=?', (puch_user_id,))
        existing = cursor.fetchone()
        if existing:
            conn.close()
            return f"You are already registered as job provider: {existing[0]}"
        
        # Insert new provider
        cursor.execute('''
            INSERT INTO job_providers (user_id, name, phone, skills, location, city, experience, rate, available, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (puch_user_id, provider_name, phone, services, work_location, city, experience, daily_rate, 1, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return f"""Job Provider Registration Successful!

Name: {provider_name}
Phone: {phone}
Services: {services}
Location: {work_location}, {city}
Experience: {experience}
Rate: {daily_rate}
Status: Active

You are now registered as job provider. Job seekers can find you when searching for your services."""
        
    except Exception as e:
        return f"Registration failed: {str(e)}"

# Job Seeker Registration  
JobSeekerRegDescription = RichToolDescription(
    description="Register as job seeker or customer to find workers and services in job marketplace",
    use_when="Use ONLY when someone explicitly wants to register as job seeker, customer, or needs to find workers/services",
    side_effects="Creates job seeker profile in marketplace database",
)

@mcp.tool(description=JobSeekerRegDescription.model_dump_json())
async def register_job_seeker(
    puch_user_id: Annotated[str, Field(description="User unique ID")],
    seeker_name: Annotated[str, Field(description="Full name")],
    phone: Annotated[str, Field(description="Phone number")],
    location: Annotated[str, Field(description="Location")],
    city: Annotated[str, Field(description="City")]
) -> str:
    """Register as job seeker in marketplace"""
    
    try:
        conn = sqlite3.connect('job_marketplace_simple.db')
        cursor = conn.cursor()
        
        # Check if already registered
        cursor.execute('SELECT name FROM job_seekers WHERE user_id=?', (puch_user_id,))
        existing = cursor.fetchone()
        if existing:
            conn.close()
            return f"You are already registered as job seeker: {existing[0]}"
        
        # Insert new seeker
        cursor.execute('''
            INSERT INTO job_seekers (user_id, name, phone, location, city, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (puch_user_id, seeker_name, phone, location, city, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return f"""Job Seeker Registration Successful!

Name: {seeker_name}
Phone: {phone}
Location: {location}, {city}
Status: Active

You can now search for job providers and post job requests."""
        
    except Exception as e:
        return f"Registration failed: {str(e)}"

# Find Job Providers
FindProvidersDescription = RichToolDescription(
    description="Search and find job providers or workers for specific services in job marketplace",
    use_when="Use ONLY when someone explicitly asks to find workers, job providers, or search for specific services like plumber, electrician etc",
    side_effects="Returns list of matching job providers from marketplace database",
)

@mcp.tool(description=FindProvidersDescription.model_dump_json())
async def find_job_providers(
    puch_user_id: Annotated[str, Field(description="User unique ID")],
    service_needed: Annotated[str, Field(description="Service or job type needed")],
    preferred_city: Annotated[str, Field(description="Preferred city")] = ""
) -> str:
    """Find job providers for specific service"""
    
    try:
        conn = sqlite3.connect('job_marketplace_simple.db')
        cursor = conn.cursor()
        
        # Auto-register as seeker if not registered
        cursor.execute('SELECT id FROM job_seekers WHERE user_id=?', (puch_user_id,))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO job_seekers (user_id, name, phone, location, city, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (puch_user_id, "User", "Not provided", "Not specified", preferred_city or "Not specified", datetime.now().isoformat()))
            conn.commit()
        
        # Search providers
        if preferred_city:
            cursor.execute('''
                SELECT * FROM job_providers 
                WHERE skills LIKE ? AND city LIKE ? AND available=1
                ORDER BY id DESC LIMIT 5
            ''', (f'%{service_needed}%', f'%{preferred_city}%'))
        else:
            cursor.execute('''
                SELECT * FROM job_providers 
                WHERE skills LIKE ? AND available=1
                ORDER BY id DESC LIMIT 5
            ''', (f'%{service_needed}%',))
        
        providers = cursor.fetchall()
        conn.close()
        
        if not providers:
            return f"No job providers found for {service_needed}" + (f" in {preferred_city}" if preferred_city else "")
        
        result = f"Found {len(providers)} job providers for {service_needed}:\n\n"
        
        for i, provider in enumerate(providers, 1):
            result += f"{i}. {provider[2]}\n"  # name
            result += f"   Phone: {provider[1]}\n"  # phone
            result += f"   Services: {provider[2]}\n"  # skills
            result += f"   Location: {provider[3]}, {provider[6]}\n"  # location, city
            result += f"   Experience: {provider[7]}\n"  # experience
            result += f"   Rate: {provider[4]}\n\n"  # rate
        
        result += "Contact them directly for your job requirements."
        return result
        
    except Exception as e:
        return f"Search failed: {str(e)}"

# Post Job Request
PostJobDescription = RichToolDescription(
    description="Post job request or requirement in marketplace for job providers to respond",
    use_when="Use ONLY when someone wants to post job requirement, job request, or needs workers to contact them for work",
    side_effects="Creates job request in marketplace that providers can see",
)

@mcp.tool(description=PostJobDescription.model_dump_json())
async def post_job_request(
    puch_user_id: Annotated[str, Field(description="User unique ID")],
    job_type: Annotated[str, Field(description="Type of job or service needed")],
    job_description: Annotated[str, Field(description="Detailed job description")],
    job_location: Annotated[str, Field(description="Job location")]
) -> str:
    """Post job request for providers to see"""
    
    try:
        conn = sqlite3.connect('job_marketplace_simple.db')
        cursor = conn.cursor()
        
        # Check if seeker is registered
        cursor.execute('SELECT name FROM job_seekers WHERE user_id=?', (puch_user_id,))
        seeker = cursor.fetchone()
        if not seeker:
            conn.close()
            return "Please register as job seeker first using register_job_seeker"
        
        # Insert job request
        cursor.execute('''
            INSERT INTO job_requests (seeker_id, job_type, description, location, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (puch_user_id, job_type, job_description, job_location, 'open', datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return f"""Job Request Posted Successfully!

Job Type: {job_type}
Description: {job_description}
Location: {job_location}
Status: Open

Job providers can now see your request and contact you directly."""
        
    except Exception as e:
        return f"Failed to post job: {str(e)}"

# View My Profile
ProfileViewDescription = RichToolDescription(
    description="View user profile status in job marketplace - shows if registered as provider or seeker",
    use_when="Use ONLY when user asks about their profile, registration status, or account information in job marketplace context",
    side_effects="Returns user profile information from job marketplace database",
)

@mcp.tool(description=ProfileViewDescription.model_dump_json())
async def view_job_profile(
    puch_user_id: Annotated[str, Field(description="User unique ID")]
) -> str:
    """View user profile in job marketplace"""
    
    try:
        conn = sqlite3.connect('job_marketplace_simple.db')
        cursor = conn.cursor()
        
        # Check provider profile
        cursor.execute('SELECT * FROM job_providers WHERE user_id=?', (puch_user_id,))
        provider = cursor.fetchone()
        
        # Check seeker profile
        cursor.execute('SELECT * FROM job_seekers WHERE user_id=?', (puch_user_id,))
        seeker = cursor.fetchone()
        
        # Check job requests if seeker
        job_requests = 0
        if seeker:
            cursor.execute('SELECT COUNT(*) FROM job_requests WHERE seeker_id=?', (puch_user_id,))
            job_requests = cursor.fetchone()[0]
        
        conn.close()
        
        if provider:
            return f"""Job Provider Profile:

Name: {provider[2]}
Phone: {provider[1]}
Services: {provider[2]}
Location: {provider[3]}, {provider[5]}
Experience: {provider[6]}
Rate: {provider[4]}
Status: {'Available' if provider[7] else 'Not Available'}

You are registered as job provider. Job seekers can find you for your services."""

        elif seeker:
            return f"""Job Seeker Profile:

Name: {seeker[2]}
Phone: {seeker[1]}
Location: {seeker[2]}, {seeker[3]}
Job Requests Posted: {job_requests}

You are registered as job seeker. You can search for job providers and post job requests."""

        else:
            return """No Profile Found

You can register as:
1. Job Provider - to offer services and get customers
2. Job Seeker - to find workers and post job requests

Use the appropriate registration tool to get started."""
        
    except Exception as e:
        return f"Profile access failed: {str(e)}"

# Browse All Providers
BrowseProvidersDescription = RichToolDescription(
    description="Browse all available job providers by service type or location in marketplace",
    use_when="Use ONLY when user wants to browse job providers, see available workers, or explore services in job marketplace",
    side_effects="Returns list of all available job providers from marketplace database",
)

@mcp.tool(description=BrowseProvidersDescription.model_dump_json())
async def browse_job_providers(
    service_filter: Annotated[str, Field(description="Filter by service type")] = "",
    city_filter: Annotated[str, Field(description="Filter by city")] = "",
    limit: Annotated[int, Field(description="Max results")] = 10
) -> str:
    """Browse all available job providers"""
    
    try:
        conn = sqlite3.connect('job_marketplace_simple.db')
        cursor = conn.cursor()
        
        # Build query
        query = "SELECT * FROM job_providers WHERE available=1"
        params = []
        
        if service_filter:
            query += " AND skills LIKE ?"
            params.append(f'%{service_filter}%')
        
        if city_filter:
            query += " AND city LIKE ?"
            params.append(f'%{city_filter}%')
        
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        providers = cursor.fetchall()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM job_providers WHERE available=1")
        total = cursor.fetchone()[0]
        
        conn.close()
        
        if not providers:
            return f"No job providers found with current filters. Total available: {total}"
        
        result = f"Available Job Providers ({len(providers)} of {total}):\n\n"
        
        for i, provider in enumerate(providers, 1):
            result += f"{i}. {provider[2]}\n"
            result += f"   Phone: {provider[1]}\n"
            result += f"   Services: {provider[2]}\n"
            result += f"   Location: {provider[3]}, {provider[5]}\n"
            result += f"   Experience: {provider[6]} | Rate: {provider[4]}\n\n"
        
        result += "Contact any provider directly for your job requirements."
        return result
        
    except Exception as e:
        return f"Browse failed: {str(e)}"

# Platform Statistics
StatsDescription = RichToolDescription(
    description="Show job marketplace statistics including total providers, seekers, and platform metrics",
    use_when="Use ONLY when user asks about platform statistics, marketplace data, or overall numbers in job marketplace context",
    side_effects="Returns comprehensive job marketplace statistics",
)

@mcp.tool(description=StatsDescription.model_dump_json())
async def job_marketplace_stats() -> str:
    """Show job marketplace statistics"""
    
    try:
        conn = sqlite3.connect('job_marketplace_simple.db')
        cursor = conn.cursor()
        
        # Get counts
        cursor.execute('SELECT COUNT(*) FROM job_providers')
        total_providers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM job_providers WHERE available=1')
        available_providers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM job_seekers')
        total_seekers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM job_requests')
        total_requests = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM job_requests WHERE status="open"')
        open_requests = cursor.fetchone()[0]
        
        # Get city distribution
        cursor.execute('SELECT city, COUNT(*) FROM job_providers GROUP BY city ORDER BY COUNT(*) DESC')
        cities = cursor.fetchall()
        
        # Get service distribution  
        cursor.execute('SELECT skills FROM job_providers')
        all_skills = cursor.fetchall()
        
        conn.close()
        
        # Count services
        service_count = {}
        for skill_row in all_skills:
            skills = skill_row[0].split()
            for skill in skills:
                service_count[skill] = service_count.get(skill, 0) + 1
        
        top_services = sorted(service_count.items(), key=lambda x: x[1], reverse=True)[:5]
        
        result = f"""Job Marketplace Statistics:

PROVIDERS:
Total Job Providers: {total_providers}
Available Now: {available_providers}

SEEKERS:
Total Job Seekers: {total_seekers}
Job Requests Posted: {total_requests}
Open Requests: {open_requests}

CITIES COVERED:
"""
        
        for city, count in cities:
            result += f"{city}: {count} providers\n"
        
        result += f"\nTOP SERVICES:\n"
        for service, count in top_services:
            result += f"{service}: {count} providers\n"
        
        result += f"\nThe marketplace connects job seekers with job providers directly."
        
        return result
        
    except Exception as e:
        return f"Stats failed: {str(e)}"

# Run Server
async def main():
    print("Starting Basic Job Marketplace - Two Channels")
    print("Channel 1: Job Providers (Workers offering services)")
    print("Channel 2: Job Seekers (Customers needing services)")
    print("Server running on http://0.0.0.0:8086")
    await mcp.run_async("streamable-http", host="0.0.0.0", port=8086)

if __name__ == "__main__":
    await mcp.run_async("streamable-http", host="0.0.0.0", port=PORT)

