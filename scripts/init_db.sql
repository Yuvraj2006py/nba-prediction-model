-- Initialize database with extensions and basic setup
-- This script runs automatically when the PostgreSQL container is first created

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- The tables will be created by SQLAlchemy models
-- This file can be used for any custom initialization

