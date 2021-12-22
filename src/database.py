from sqlalchemy import create_engine

# Database connection string.
DATABASE_URL = "sqlite:///./data/covid.db"

# Create the engine.
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
