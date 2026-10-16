# Economic Outlook 2027: Report-to-Dataset Pipeline

A Python data engineering pipeline that extracts appendix tables from the Ministry of Finance Malaysia Economic Outlook 2027 report into structured CSV files and a query-ready SQLite database with SHA-256 integrity verification.

## Tables Included
* Table 1.1: Selected Socioeconomic Statistics
* Table 2.1 & 2.2: International & ASEAN Economic Indicators
* Table 3.1: GDP by Economic Activity
* Table 3.2: Index of Services
* Table 3.3: Industrial Production Index (IPI)
* Table 3.5: Private Consumption Indicators
* Table 3.7: Trade with Major Partners
* Table 3.10 & 3.12: Commodity Trade & Import Sources
* Table 3.14 to 3.18: Inflation, CPI Region, Core & PPI
* Table 3.16: State-Level CPI
* Table 3.19 to 3.22: Labour Market, Vacancies & Placements
* Table 4.3 & 4.6: Commercial & Islamic Loans by Sector
* Table 4.4: Government & Corporate Bond Yields
* Table 5.1: Progress of Sustainable Development Goals (SDGs 1-17)

## Usage

```bash
pip install -r requirements.txt
pytest -v
```
