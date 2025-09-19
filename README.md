
---

### **godiff**
```markdown
# Godiff

A Go tool to compare domain datasets across days.  
Originally built as a commissioned project to process WHOIS domain lists and detect changes.

## Features
- Download global domain data from WHOIS
- Compare with previous day’s dataset
- Generate reports of new, deleted, and changed domains
- Filtering options for domain analysis

## Tech Stack
- Go (built in 2 weeks, as a self-learning project)

## Usage
```bash
go run main.go --today data/20250901.txt --yesterday data/20250831.txt
