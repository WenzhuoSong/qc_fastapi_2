#!/bin/bash
# QC FastAPI 2 Quick Setup Script

set -e

echo "🚀 QC FastAPI 2 Setup Script"
echo "======================="
echo ""

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.11+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo "✓ Python version: $PYTHON_VERSION"

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📚 Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "✓ Dependencies installed"

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "⚙️  Creating .env from template..."
    cp .env.example .env
    echo "✓ .env created - PLEASE EDIT IT with your credentials!"
    echo ""
    echo "Required variables:"
    echo "  - DATABASE_URL"
    echo "  - ANTHROPIC_API_KEY"
    echo "  - QC_API_URL, QC_USER_ID, QC_API_TOKEN, QC_PROJECT_ID"
    echo "  - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID"
    echo "  - WEBHOOK_USER, WEBHOOK_SECRET"
else
    echo "✓ .env already exists"
fi

# Check if Docker is running
if command -v docker &> /dev/null; then
    if docker ps &> /dev/null; then
        echo "✓ Docker is running"

        # Check if PostgreSQL container exists
        if docker ps -a --format '{{.Names}}' | grep -q "^qc-fastapi-2-pg$"; then
            echo "✓ PostgreSQL container exists"

            # Check if it's running
            if docker ps --format '{{.Names}}' | grep -q "^qc-fastapi-2-pg$"; then
                echo "✓ PostgreSQL is running"
            else
                echo "🔄 Starting PostgreSQL container..."
                docker start qc-fastapi-2-pg
                echo "✓ PostgreSQL started"
            fi
        else
            echo "🗄️  Creating PostgreSQL container..."
            docker run -d --name qc-fastapi-2-pg \
              -e POSTGRES_DB=qc_fastapi_2 \
              -e POSTGRES_USER=qc_fastapi_2 \
              -e POSTGRES_PASSWORD=password \
              -p 5432:5432 \
              postgres:16
            echo "✓ PostgreSQL container created and started"
            echo "  Connection: postgresql+asyncpg://qc_fastapi_2:password@localhost:5432/qc_fastapi_2"
        fi
    else
        echo "⚠️  Docker is installed but not running"
        echo "   Please start Docker Desktop and run this script again"
    fi
else
    echo "⚠️  Docker not found - you'll need to setup PostgreSQL manually"
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Edit .env with your credentials"
echo "  2. Start the application:"
echo "     source venv/bin/activate"
echo "     uvicorn main:app --reload"
echo "  3. Visit http://localhost:8000/docs for API documentation"
echo ""
echo "For deployment to Railway, see DEPLOYMENT.md"
