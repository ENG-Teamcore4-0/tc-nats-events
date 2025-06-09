#!/bin/bash

# GitHub Repository Initialization Script for TC NATS Events
# This script automates the GitHub repository setup process

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REPO_NAME="tc-nats-events"
REPO_OWNER="teamcore"
REPO_DESCRIPTION="Event Sourcing and Durable Consumers for NATS JetStream - TeamCore Platform"
REPO_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}"

# Helper functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

check_dependencies() {
    log_info "Checking dependencies..."
    
    # Check if git is installed
    if ! command -v git &> /dev/null; then
        log_error "Git is not installed. Please install Git first."
        exit 1
    fi
    
    # Check if gh CLI is installed
    if ! command -v gh &> /dev/null; then
        log_warning "GitHub CLI (gh) is not installed. Manual setup will be required."
        MANUAL_SETUP=true
    else
        MANUAL_SETUP=false
    fi
    
    log_success "Dependencies checked"
}

create_repository() {
    if [ "$MANUAL_SETUP" = false ]; then
        log_info "Creating GitHub repository using GitHub CLI..."
        
        # Check if user is authenticated
        if ! gh auth status &> /dev/null; then
            log_error "Not authenticated with GitHub CLI. Run 'gh auth login' first."
            exit 1
        fi
        
        # Create repository
        gh repo create "${REPO_OWNER}/${REPO_NAME}" \
            --public \
            --description "$REPO_DESCRIPTION" \
            --clone=false
        
        log_success "Repository created: ${REPO_URL}"
    else
        log_warning "Manual repository creation required."
        echo "Please create the repository manually at: https://github.com/new"
        echo "Repository name: ${REPO_NAME}"
        echo "Description: ${REPO_DESCRIPTION}"
        echo "Make it public and don't initialize with README, .gitignore, or license"
        echo ""
        read -p "Press Enter when repository is created..."
    fi
}

setup_git_repository() {
    log_info "Setting up local Git repository..."
    
    # Initialize git if not already initialized
    if [ ! -d ".git" ]; then
        git init
        log_success "Git repository initialized"
    fi
    
    # Add remote if not exists
    if ! git remote get-url origin &> /dev/null; then
        git remote add origin "${REPO_URL}.git"
        log_success "Remote origin added"
    fi
    
    # Check if there are any files to commit
    if [ -n "$(git status --porcelain)" ]; then
        # Add all files
        git add .
        
        # Create initial commit
        git commit -m "feat: initial release of TC NATS Events package

- Event Sourcing implementation with immutable events
- Durable Consumer pattern with automatic synchronization  
- Event Publisher with delivery confirmation and retry logic
- Complete flexibility for custom event types
- Idempotent event processing
- Comprehensive metrics collection
- Structured logging with correlation tracking
- Full type safety and comprehensive test suite

🤖 Generated with Claude Code

Co-Authored-By: Claude <noreply@anthropic.com>"
        
        log_success "Initial commit created"
    fi
    
    # Set main branch
    git branch -M main
    
    # Push to GitHub
    log_info "Pushing to GitHub..."
    git push -u origin main
    log_success "Code pushed to GitHub"
}

configure_repository() {
    if [ "$MANUAL_SETUP" = false ]; then
        log_info "Configuring repository settings..."
        
        # Enable vulnerability alerts
        gh api repos/${REPO_OWNER}/${REPO_NAME} \
            --method PATCH \
            --field has_vulnerability_alerts=true \
            --silent
        
        # Set topics
        gh api repos/${REPO_OWNER}/${REPO_NAME}/topics \
            --method PUT \
            --field names='["nats","jetstream","event-sourcing","event-driven","microservices","python","durable-consumer","messaging","teamcore","at-least-once-delivery","horizontal-scaling","idempotency","metrics","structured-logging"]' \
            --silent
        
        log_success "Repository configured"
    else
        log_warning "Manual repository configuration required."
        echo "Please configure the following in GitHub repository settings:"
        echo "1. Enable vulnerability alerts"
        echo "2. Add topics: nats, jetstream, event-sourcing, event-driven, microservices, python"
        echo "3. Set up branch protection rules for main branch"
        echo "4. Enable Dependabot"
    fi
}

create_first_release() {
    log_info "Creating first release tag..."
    
    # Create and push tag
    git tag -a v0.1.0 -m "Release v0.1.0

Initial release of TC NATS Events package with:
- Complete Event Sourcing implementation
- Durable Consumer pattern with automatic synchronization
- Flexible custom event types
- Idempotency and comprehensive metrics
- Production-ready features with full test coverage"

    git push origin v0.1.0
    log_success "Release tag v0.1.0 created and pushed"
    
    if [ "$MANUAL_SETUP" = false ]; then
        # Wait a moment for the tag to be processed
        sleep 2
        
        # Check if release workflow completed
        log_info "Release workflow will create the GitHub release automatically"
        log_info "Check the Actions tab: ${REPO_URL}/actions"
    fi
}

setup_development_environment() {
    log_info "Setting up development environment..."
    
    # Check if Python virtual environment exists
    if [ ! -d "venv" ]; then
        python3 -m venv venv
        log_success "Python virtual environment created"
    fi
    
    # Activate virtual environment and install dependencies
    source venv/bin/activate 2>/dev/null || . venv/Scripts/activate 2>/dev/null || {
        log_warning "Could not activate virtual environment automatically"
        echo "Please run: source venv/bin/activate (or venv\\Scripts\\activate on Windows)"
    }
    
    # Install in development mode
    if command -v make &> /dev/null; then
        make install-dev
        log_success "Development dependencies installed"
    else
        pip install -e ".[dev,test,docs]"
        log_success "Development dependencies installed (pip)"
    fi
    
    # Set up pre-commit hooks
    if command -v pre-commit &> /dev/null; then
        pre-commit install
        log_success "Pre-commit hooks installed"
    else
        log_warning "Pre-commit not available. Install with: pip install pre-commit"
    fi
}

show_next_steps() {
    log_success "GitHub repository setup complete! 🎉"
    echo ""
    echo "Repository URL: ${REPO_URL}"
    echo ""
    echo "Next steps:"
    echo "1. 📖 Review the repository at ${REPO_URL}"
    echo "2. 🔧 Configure branch protection rules (see GITHUB_SETUP.md)"
    echo "3. 🔑 Add secrets for CI/CD (CODECOV_TOKEN, etc.)"
    echo "4. 👥 Add team members as collaborators"
    echo "5. 🚀 Start development with: make dev-nats && python examples/basic_pubsub.py"
    echo ""
    echo "Development commands:"
    echo "  make test          # Run tests"
    echo "  make lint          # Run linting"
    echo "  make dev-nats      # Start NATS for development"
    echo "  make dev-example   # Run example"
    echo ""
    echo "Documentation:"
    echo "  📋 Setup guide: GITHUB_SETUP.md"
    echo "  🎯 Custom events: docs/CUSTOM_EVENTS.md"
    echo "  🤝 Contributing: CONTRIBUTING.md"
    echo ""
    if [ "$MANUAL_SETUP" = true ]; then
        echo "⚠️  Manual setup required - see GITHUB_SETUP.md for details"
    fi
}

main() {
    echo "🚀 TC NATS Events - GitHub Repository Setup"
    echo "==========================================="
    echo ""
    
    # Check if we're in the right directory
    if [ ! -f "pyproject.toml" ] || [ ! -f "README.md" ]; then
        log_error "Please run this script from the tc-nats-events project root directory"
        exit 1
    fi
    
    check_dependencies
    create_repository
    setup_git_repository
    configure_repository
    create_first_release
    setup_development_environment
    show_next_steps
}

# Run main function
main "$@"