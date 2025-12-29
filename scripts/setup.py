"""Setup script to initialize the project."""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
import shutil
from config.settings import Settings


def setup_project():
    """Initialize project directories and configuration."""
    print("Setting up NBA Prediction Model project...")
    
    # Get settings and create directories
    settings = Settings()
    settings.create_directories()
    
    # Create .env file from example if it doesn't exist
    project_root = Path(__file__).parent.parent
    env_example = project_root / "env.example"
    env_file = project_root / ".env"
    
    if not env_file.exists() and env_example.exists():
        shutil.copy(env_example, env_file)
        print(f"Created .env file from env.example")
        print("Please edit .env and add your API keys.")
    elif env_file.exists():
        print(".env file already exists.")
    else:
        print("Warning: env.example not found. Please create .env manually.")
    
    print("\nProject setup complete!")
    print("\nNext steps:")
    print("1. Edit .env file and add your API keys")
    print("2. Install dependencies: pip install -r requirements.txt")
    print("3. Run data collection scripts to populate database")


if __name__ == "__main__":
    setup_project()

