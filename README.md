![Python](https://img.shields.io/badge/Python-3.10-blue)
![Tableau](https://img.shields.io/badge/Tableau-Visualization-orange)
![Database](https://img.shields.io/badge/PostgreSQL-Supabase-green)


# Clinical Pharmacy Monitoring Dashboard

A clinical pharmacy analytics project built to monitor medication consumption, patient refill behavior, and clinic visit patterns in a free hypertension clinic in Nigeria.

## Problem statement

We identified that patients were coming before their refill dates to collect medication leading to over-dispensing. Some patients come back one week after collecting a pack of their medication that should last a month to collect another pack. Most times they are not identified because they were not attended to by the same pharmacist. This is because the medications are given for free and they were exploiting the system. 

## Solution

I built a solution that identifies early refills by showing in real time the last visit date, medications collected, and quantities dispensed. The solution also tracks medication consumption pattern and predicts comsumption for procurement decisions. It also shows clinic pharmacy visit pattern with time. It can also be tailored to track medication adherence. 

## Project Overview

This project was developed in two phases:

### Phase 1: Research & Analysis
- Data collection and cleaning  
- Exploratory data analysis (EDA) using Python  
- Identification of medication refill patterns and system loopholes  
- Insights presented to stakeholders  

Files:
- Notebook: `research/notebook/data_analysis.ipynb`
- Presentation: `research/presentation/research findings.pdf`

### Phase 2: System Development
- ETL pipeline (Python + Supabase)  
- Data modelling and transformation  
- Interactive dashboard development using Tableau  

## Data

- `clinic_data_sample.csv`: sample dataset for demonstration  
- Full dataset is not included for privacy reasons  

## Tech Stack
- Python
- Pandas
- SQLAlchemy
- Supabase / PostgreSQL
- Tableau
- GitHub

## Workflow
1. Raw CSV data is ingested using Python
2. ETL reshapes medication columns from wide to long format
3. Cleaned tables are loaded into Supabase
4. Tableau connects to Supabase and powers the dashboard

## Data Model
### clinic_visits
One row per patient visit

### dispensed_medications_long
One row per medication dispensed per visit

## Key Features
- Patient lookup for latest dispensed medications
- Drug-level dispensing analysis
- Monthly and weekday clinic visit trends
- Medication consumption trend and forecast
- Refill monitoring and adherence-related insights

## Key Insights

- There were 456 cases of early refills for just Amlodipine 10mg
- 157 packs equivalent to 157,000 naira has been lost due to early refill  
- Mondays recorded the highest patient visit volume  
- Amlodipine 10mg was the most dispensed medication 
- The average monthly consumption of amlodipine is estimated at 270 packs 
- Early refill patterns suggest potential medication misuse in a free-drug system 

## Dashboard Preview

See the exported dashboard in the `dashboard/Clinical Pharmacy Monitoring Dashboard.pdf` file.

## Live Dashboard
View the interactive Tableau Public dashboard here: [Tableau Public Link](https://public.tableau.com/app/profile/caleb.chijindu.ugorji)

## Repository Structure

```text
clinical-pharmacy-monitoring-dashboard/
├── data/                 # Sample or raw data files
├── scripts/              # ETL and connection scripts
├── dashboard/            # Exported dashboard (PDF)
├── .env.example          # Environment variables template
├── .gitignore            # Ignored files
├── requirements.txt      # Python dependencies
└── README.md             # Project documentation

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name

2. Create environment:
    conda create -n clinic_env python=3.10
conda activate clinic_env

3.  Install dependencies:
    pip install -r requirements.txt

4. Create a .env file based on .env.example

5. Run the ETL script:
    python scripts/etl_clinic_data.py

```md
### Notes

- A Supabase (PostgreSQL) database is required to run this project  
- Update your `.env` file with your database credentials  
- The sample dataset is included for demonstration purposes
