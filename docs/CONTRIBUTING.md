# Contributing to DealLedger

Thank you for your interest in contributing. This document explains how to help.

---

## Ways to Contribute

### 1. Add a Broker Scraper

The highest-impact contribution. See [adding-a-broker.md](adding-a-broker.md) for a step-by-step guide.

### 2. Report Data Errors

Found incorrect data? Open an issue with:
- The listing ID or URL
- What's wrong
- Evidence (screenshot, source link)

### 3. Challenge the Methodology

Think our classification or verification logic is flawed? Open an issue explaining:
- What's wrong with the current approach
- Your proposed alternative
- Evidence or reasoning

### 4. Improve Documentation

Docs can always be clearer. PRs welcome for:
- Typo fixes
- Clarifications
- Additional examples
- Translations

### 5. Build on Top

Use the data to build something useful. Let us know what you create.

---

## Development Setup

```bash
# Clone the repo
git clone https://github.com/dealledger/dealledger
cd dealledger

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest
```

---

## Code Style

- Python: Follow PEP 8, use Black for formatting
- Run `black .` before committing
- Run `flake8` to check for issues

---

## Pull Request Process

1. **Fork the repo** and create a feature branch
2. **Make your changes** with clear commit messages
3. **Add tests** if applicable
4. **Update docs** if you changed behavior
5. **Run tests** to make sure nothing broke
6. **Submit PR** with a clear description

### PR Title Format

- `Add broker: [Broker Name]` — for new scrapers
- `Fix: [description]` — for bug fixes
- `Docs: [description]` — for documentation
- `Improve: [description]` — for enhancements

---

## Issue Guidelines

### Bug Reports

Include:
- What you expected
- What actually happened
- Steps to reproduce
- Environment (Python version, OS)

### Feature Requests

Include:
- The problem you're trying to solve
- Your proposed solution
- Alternatives you considered

---

## Code of Conduct

- Be respectful
- Assume good intent
- Focus on the work, not the person
- No harassment, discrimination, or personal attacks

---

## Questions?

- Open a Discussion for general questions
- Open an Issue for bugs or specific problems
- Check existing issues before creating new ones

---

## Recognition

Contributors are credited in:
- CONTRIBUTORS.md
- Release notes
- Broker records (for scraper contributions)

Thank you for helping build the ledger.
