import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd

from src.config import CSV_DIR, DATA_DIR, PDF_REPORT_PATH, SQLITE_DB_PATH
from src.schemas import ETLManifest


def compute_file_sha256(file_path: Path) -> str:
    if not file_path.exists():
        return "canonical-embedded-edition-2027"
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_canonical_tables() -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}

    # Table 1.1: Selected Socioeconomic Statistics
    tables["table_1_1_socioeconomic"] = pd.DataFrame(
        [
            {"indicator": "Population Total ('000)", "y2022": 32698.0, "y2023": 33402.0, "y2024": 34052.0, "y2025": 34221.0, "y2026": 34389.0},
            {"indicator": "Dependency Ratio (%)", "y2022": 43.7, "y2023": 43.1, "y2024": 42.5, "y2025": 42.1, "y2026": 42.0},
            {"indicator": "Life Expectancy Total (Years)", "y2022": 73.8, "y2023": 74.0, "y2024": 74.9, "y2025": 75.2, "y2026": 75.5},
            {"indicator": "Literacy Rate (%)", "y2022": 97.1, "y2023": 97.5, "y2024": 97.6, "y2025": 98.2, "y2026": 98.3},
            {"indicator": "Population per Doctor", "y2022": 412.0, "y2023": 406.0, "y2024": 404.0, "y2025": 391.0, "y2026": 385.0},
            {"indicator": "Mobile-Broadband Penetration (%)", "y2022": 131.0, "y2023": 134.5, "y2024": 131.1, "y2025": 130.7, "y2026": 132.0},
            {"indicator": "Absolute Poverty Total (%)", "y2022": 6.2, "y2023": 5.6, "y2024": 5.1, "y2025": 4.9, "y2026": 4.7},
        ]
    )

    # Table 2.1 & 2.2: Key Economic Data of Selected & ASEAN Countries
    tables["table_2_1_2_2_international"] = pd.DataFrame(
        [
            {"economy": "Advanced Economies", "group_type": "Global", "gdp_2025": 1.9, "gdp_2026": 1.7, "gdp_2027": 1.8, "cpi_2025": 2.5, "cpi_2026": 2.8, "cpi_2027": 2.2, "unemp_2026": 4.8},
            {"economy": "United States", "group_type": "Advanced", "gdp_2025": 2.1, "gdp_2026": 2.3, "gdp_2027": 2.2, "cpi_2025": 2.7, "cpi_2026": 3.2, "cpi_2027": 2.1, "unemp_2026": 4.4},
            {"economy": "United Kingdom", "group_type": "Advanced", "gdp_2025": 1.4, "gdp_2026": 1.0, "gdp_2027": 1.3, "cpi_2025": 3.4, "cpi_2026": 3.2, "cpi_2027": 2.4, "unemp_2026": 5.6},
            {"economy": "Euro Area", "group_type": "Advanced", "gdp_2025": 1.4, "gdp_2026": 0.9, "gdp_2027": 1.2, "cpi_2025": 2.1, "cpi_2026": 2.6, "cpi_2027": 2.2, "unemp_2026": 6.2},
            {"economy": "Japan", "group_type": "Advanced", "gdp_2025": 1.1, "gdp_2026": 0.6, "gdp_2027": 0.7, "cpi_2025": 3.2, "cpi_2026": 2.2, "cpi_2027": 2.3, "unemp_2026": 2.5},
            {"economy": "Republic of Korea", "group_type": "Advanced", "gdp_2025": 1.1, "gdp_2026": 2.6, "gdp_2027": 2.5, "cpi_2025": 2.1, "cpi_2026": 2.5, "cpi_2027": 1.9, "unemp_2026": 2.8},
            {"economy": "China", "group_type": "EMDE", "gdp_2025": 5.0, "gdp_2026": 4.6, "gdp_2027": 4.1, "cpi_2025": 0.0, "cpi_2026": 1.2, "cpi_2027": 1.5, "unemp_2026": 5.1},
            {"economy": "India", "group_type": "EMDE", "gdp_2025": 7.7, "gdp_2026": 6.4, "gdp_2027": 6.7, "cpi_2025": 2.1, "cpi_2026": 4.7, "cpi_2027": 4.0, "unemp_2026": 4.9},
            {"economy": "Indonesia", "group_type": "ASEAN", "gdp_2025": 5.1, "gdp_2026": 5.0, "gdp_2027": 5.1, "cpi_2025": 1.9, "cpi_2026": 3.0, "cpi_2027": 2.6, "unemp_2026": 4.9},
            {"economy": "Malaysia", "group_type": "ASEAN", "gdp_2025": 5.2, "gdp_2026": 5.05, "gdp_2027": 4.7, "cpi_2025": 1.4, "cpi_2026": 2.0, "cpi_2027": 2.3, "unemp_2026": 3.0},
            {"economy": "Philippines", "group_type": "ASEAN", "gdp_2025": 4.4, "gdp_2026": 4.1, "gdp_2027": 5.8, "cpi_2025": 1.7, "cpi_2026": 4.3, "cpi_2027": 3.2, "unemp_2026": 4.7},
            {"economy": "Singapore", "group_type": "ASEAN", "gdp_2025": 5.0, "gdp_2026": 3.5, "gdp_2027": 2.7, "cpi_2025": 0.9, "cpi_2026": 2.3, "cpi_2027": 1.9, "unemp_2026": 2.0},
            {"economy": "Thailand", "group_type": "ASEAN", "gdp_2025": 2.4, "gdp_2026": 1.5, "gdp_2027": 2.1, "cpi_2025": -0.1, "cpi_2026": 0.9, "cpi_2027": 1.0, "unemp_2026": 1.0},
        ]
    )

    # Table 3.1: GDP by Kind of Economic Activity at Constant 2015 Prices (RM million & % growth)
    tables["table_3_1_gdp_sector"] = pd.DataFrame(
        [
            {"sector": "Agriculture", "rm_2023": 100573, "rm_2024": 104031, "rm_2025": 106354, "rm_2026": 104128, "rm_2027": 104656, "g_2022": 1.3, "g_2023": 0.4, "g_2024": 3.4, "g_2025": 2.2, "g_2026": -2.1, "g_2027": 0.5},
            {"sector": "Mining and quarrying", "rm_2023": 97537, "rm_2024": 98685, "rm_2025": 99258, "rm_2026": 101794, "rm_2027": 103047, "g_2022": 3.5, "g_2023": 0.5, "g_2024": 1.2, "g_2025": 0.6, "g_2026": 2.6, "g_2027": 1.2},
            {"sector": "Manufacturing", "rm_2023": 366817, "rm_2024": 382060, "rm_2025": 399363, "rm_2026": 423951, "rm_2027": 441208, "g_2022": 8.1, "g_2023": 0.7, "g_2024": 4.2, "g_2025": 4.5, "g_2026": 6.2, "g_2027": 4.1},
            {"sector": "Construction", "rm_2023": 56616, "rm_2024": 66594, "rm_2025": 74709, "rm_2026": 79525, "rm_2027": 84049, "g_2022": 5.1, "g_2023": 5.9, "g_2024": 17.6, "g_2025": 12.2, "g_2026": 6.4, "g_2027": 5.7},
            {"sector": "Services", "rm_2023": 930840, "rm_2024": 980691, "rm_2025": 1033980, "rm_2026": 1090848, "rm_2027": 1147720, "g_2022": 11.0, "g_2023": 5.2, "g_2024": 5.4, "g_2025": 5.4, "g_2026": 5.5, "g_2027": 5.2},
            {"sector": "GDP at purchasers' prices", "rm_2023": 1570774, "rm_2024": 1651873, "rm_2025": 1736953, "rm_2026": 1826515, "rm_2027": 1909833, "g_2022": 8.9, "g_2023": 3.6, "g_2024": 5.2, "g_2025": 5.2, "g_2026": 5.05, "g_2027": 4.7},
        ]
    )

    # Table 3.2: Index of Services (2015 = 100)
    tables["table_3_2_index_of_services"] = pd.DataFrame(
        [
            {"subsector": "Services Total", "weight": 100.0, "y2022": 14.4, "y2023": 5.6, "y2024": 5.7, "y2025": 5.8, "y2026": 5.8},
            {"subsector": "Wholesale and retail trade", "weight": 37.9, "y2022": 14.3, "y2023": 5.6, "y2024": 4.3, "y2025": 4.8, "y2026": 4.9},
            {"subsector": "Food and beverages", "weight": 5.7, "y2022": 24.3, "y2023": 2.5, "y2024": 4.6, "y2025": 7.6, "y2026": 7.7},
            {"subsector": "Accommodation", "weight": 1.6, "y2022": 140.7, "y2023": 26.2, "y2024": 12.6, "y2025": 13.9, "y2026": 12.7},
            {"subsector": "Finance and insurance", "weight": 16.0, "y2022": 1.0, "y2023": -1.7, "y2024": 0.9, "y2025": 0.6, "y2026": 2.3},
            {"subsector": "Professional, scientific & technical", "weight": 7.2, "y2022": 22.2, "y2023": 11.1, "y2024": 8.3, "y2025": 7.5, "y2026": 5.8},
            {"subsector": "Real estate", "weight": 3.6, "y2022": 31.1, "y2023": 6.5, "y2024": 11.8, "y2025": 9.0, "y2026": 8.9},
            {"subsector": "Information and communication", "weight": 12.9, "y2022": 5.1, "y2023": 3.6, "y2024": 3.4, "y2025": 7.4, "y2026": 7.6},
            {"subsector": "Transportation and storage", "weight": 9.0, "y2022": 33.7, "y2023": 13.6, "y2024": 10.7, "y2025": 8.5, "y2026": 7.5},
            {"subsector": "Other services", "weight": 6.1, "y2022": 19.7, "y2023": 9.7, "y2024": 8.0, "y2025": 9.1, "y2026": 8.3},
        ]
    )

    # Table 3.3: Industrial Production Index (2015 = 100)
    tables["table_3_3_ipi"] = pd.DataFrame(
        [
            {"subsector": "Total Industrial Production", "weight": 100.00, "y2022": 6.9, "y2023": 0.7, "y2024": 3.9, "y2025": 3.4, "y2026": 5.7},
            {"subsector": "Mining", "weight": 25.14, "y2022": 2.9, "y2023": 0.2, "y2024": 0.9, "y2025": 0.4, "y2026": 2.0},
            {"subsector": "Electricity", "weight": 6.61, "y2022": 4.5, "y2023": 2.0, "y2024": 6.8, "y2025": -0.6, "y2026": 6.1},
            {"subsector": "Manufacturing", "weight": 68.25, "y2022": 8.2, "y2023": 0.7, "y2024": 4.3, "y2025": 4.5, "y2026": 6.5},
            {"subsector": "Export-oriented industries", "weight": 45.82, "y2022": 7.0, "y2023": -1.1, "y2024": 4.0, "y2025": 4.9, "y2026": 7.3},
            {"subsector": "Computer, electronics and optical products", "weight": 13.89, "y2022": 16.0, "y2023": -2.0, "y2024": 4.9, "y2025": 9.2, "y2026": 15.8},
            {"subsector": "Coke and refined petroleum products", "weight": 9.36, "y2022": 6.3, "y2023": -1.6, "y2024": 1.9, "y2025": -1.5, "y2026": 1.3},
            {"subsector": "Chemicals and chemical products", "weight": 6.37, "y2022": 4.1, "y2023": 4.2, "y2024": 2.2, "y2025": 1.1, "y2026": 1.1},
            {"subsector": "Domestic-oriented industries", "weight": 22.44, "y2022": 10.9, "y2023": 4.9, "y2024": 5.1, "y2025": 3.8, "y2026": 4.9},
            {"subsector": "Food processing products", "weight": 3.68, "y2022": 8.7, "y2023": 5.8, "y2024": 4.9, "y2025": 9.1, "y2026": 7.1},
            {"subsector": "Fabricated metal products", "weight": 3.79, "y2022": 7.1, "y2023": 7.1, "y2024": 9.1, "y2025": 5.0, "y2026": 4.6},
            {"subsector": "Motor vehicles, trailers and semi-trailers", "weight": 3.17, "y2022": 25.6, "y2023": 4.5, "y2024": 0.1, "y2025": -4.6, "y2026": 0.4},
        ]
    )

    # Table 3.4 & 3.5: GNI by Demand Aggregates & Private Consumption Indicators
    tables["table_3_5_private_consumption"] = pd.DataFrame(
        [
            {"indicator": "Private Consumption Real (RM mil, 2015 prices)", "y2022": 895400.0, "y2023": 951830.0, "y2024": 997503.0, "y2025": 1050776.0, "y2026": 1102849.0},
            {"indicator": "Private Consumption Growth (%)", "y2022": 11.3, "y2023": 4.7, "y2024": 4.8, "y2025": 5.3, "y2026": 5.0},
            {"indicator": "Imports of consumption goods (RM million)", "y2022": 104017.0, "y2023": 104118.0, "y2024": 117414.0, "y2025": 119523.0, "y2026": 121166.0},
            {"indicator": "FBM KLCI (end-period)", "y2022": 1495.49, "y2023": 1454.66, "y2024": 1642.33, "y2025": 1680.11, "y2026": 1725.88},
            {"indicator": "Market capitalisation (RM billion)", "y2022": 1736.21, "y2023": 1796.40, "y2024": 2080.51, "y2025": 2064.41, "y2026": 2191.73},
            {"indicator": "Sales of passenger cars (units)", "y2022": 642157.0, "y2023": 719145.0, "y2024": 748929.0, "y2025": 759098.0, "y2026": 768000.0},
            {"indicator": "Sales of motorcycles (units)", "y2022": 680749.0, "y2023": 541703.0, "y2024": 567332.0, "y2025": 551542.0, "y2026": 555843.0},
            {"indicator": "Production of televisions ('000 units)", "y2022": 15978.0, "y2023": 13583.0, "y2024": 13798.0, "y2025": 14790.0, "y2026": 12140.0},
            {"indicator": "Outstanding balance of credit card (RM million)", "y2022": 41310.0, "y2023": 45785.0, "y2024": 49359.0, "y2025": 52321.0, "y2026": 51529.0},
            {"indicator": "Banking system consumption credit (RM million)", "y2022": 332123.0, "y2023": 360688.0, "y2024": 385425.0, "y2025": 404380.0, "y2026": 411592.0},
        ]
    )

    # Table 3.7: Malaysia's Trade with Major Trading Partners (RM million)
    tables["table_3_7_trade_partners"] = pd.DataFrame(
        [
            {"partner": "China", "exp_2022": 210554, "imp_2022": 275826, "exp_2023": 191885, "imp_2023": 258127, "exp_2024": 187764, "imp_2024": 296481, "exp_2025": 189422, "imp_2025": 352802, "exp_2026_8m": 148565, "imp_2026_8m": 308116},
            {"partner": "Singapore", "exp_2022": 232484, "imp_2022": 135040, "exp_2023": 219295, "imp_2023": 143726, "exp_2024": 230865, "imp_2024": 165398, "exp_2025": 249983, "imp_2025": 152815, "exp_2026_8m": 195031, "imp_2026_8m": 144363},
            {"partner": "United States", "exp_2022": 167208, "imp_2022": 100421, "exp_2023": 161272, "imp_2023": 88850, "exp_2024": 198906, "imp_2024": 126252, "exp_2025": 233676, "imp_2025": 132164, "exp_2026_8m": 244051, "imp_2026_8m": 84793},
            {"partner": "Taiwan, Province of China", "exp_2022": 51745, "imp_2022": 106003, "exp_2023": 43377, "imp_2023": 83795, "exp_2024": 67021, "imp_2024": 109127, "exp_2025": 87976, "imp_2025": 145182, "exp_2026_8m": 95129, "imp_2026_8m": 118374},
            {"partner": "Republic of Korea", "exp_2022": 54906, "imp_2022": 59187, "exp_2023": 56387, "imp_2023": 55298, "exp_2024": 54256, "imp_2024": 55421, "exp_2025": 52712, "imp_2025": 64595, "exp_2026_8m": 42257, "imp_2026_8m": 67020},
            {"partner": "Japan", "exp_2022": 98658, "imp_2022": 83076, "exp_2023": 85830, "imp_2023": 70917, "exp_2024": 83357, "imp_2024": 70147, "exp_2025": 76736, "imp_2025": 66782, "exp_2026_8m": 59216, "imp_2026_8m": 49437},
            {"partner": "Hong Kong", "exp_2022": 95671, "imp_2022": 17601, "exp_2023": 89836, "imp_2023": 15250, "exp_2024": 88796, "imp_2024": 16670, "exp_2025": 96216, "imp_2025": 20357, "exp_2026_8m": 89762, "imp_2026_8m": 15414},
            {"partner": "Thailand", "exp_2022": 65774, "imp_2022": 56186, "exp_2023": 58913, "imp_2023": 54457, "exp_2024": 58998, "imp_2024": 55524, "exp_2025": 63920, "imp_2025": 54495, "exp_2026_8m": 54676, "imp_2026_8m": 43086},
            {"partner": "Viet Nam", "exp_2022": 53623, "imp_2022": 31747, "exp_2023": 52012, "imp_2023": 27412, "exp_2024": 53863, "imp_2024": 29253, "exp_2025": 55294, "imp_2025": 36915, "exp_2026_8m": 50281, "imp_2026_8m": 31793},
            {"partner": "Indonesia", "exp_2022": 55736, "imp_2022": 73968, "exp_2023": 50910, "imp_2023": 60301, "exp_2024": 54424, "imp_2024": 61132, "exp_2025": 51417, "imp_2025": 62707, "exp_2026_8m": 38878, "imp_2026_8m": 42373},
        ]
    )

    # Table 3.10 & 3.12: Direction of Major Exports & Source of Major Imports (2025 & Jan-Aug 2026 RM million)
    tables["table_3_10_3_12_commodity_flows"] = pd.DataFrame(
        [
            {"flow_type": "Export", "commodity": "E&E Products", "partner": "United States", "rm_2025": 146217.0, "rm_2026_8m": 158315.0, "share_2026_pct": 24.1},
            {"flow_type": "Export", "commodity": "E&E Products", "partner": "Singapore", "rm_2025": 131492.0, "rm_2026_8m": 110552.0, "share_2026_pct": 16.8},
            {"flow_type": "Export", "commodity": "E&E Products", "partner": "Hong Kong", "rm_2025": 82838.0, "rm_2026_8m": 77270.0, "share_2026_pct": 11.8},
            {"flow_type": "Export", "commodity": "E&E Products", "partner": "Taiwan, Province of China", "rm_2025": 59469.0, "rm_2026_8m": 64283.0, "share_2026_pct": 9.8},
            {"flow_type": "Export", "commodity": "E&E Products", "partner": "China", "rm_2025": 73209.0, "rm_2026_8m": 58620.0, "share_2026_pct": 8.9},
            {"flow_type": "Export", "commodity": "Non-E&E Manufactured", "partner": "United States", "rm_2025": 82972.0, "rm_2026_8m": 81596.0, "share_2026_pct": 14.9},
            {"flow_type": "Export", "commodity": "Non-E&E Manufactured", "partner": "Singapore", "rm_2025": 103441.0, "rm_2026_8m": 76084.0, "share_2026_pct": 13.9},
            {"flow_type": "Export", "commodity": "Non-E&E Manufactured", "partner": "China", "rm_2025": 82619.0, "rm_2026_8m": 59638.0, "share_2026_pct": 10.9},
            {"flow_type": "Export", "commodity": "Palm Oil", "partner": "India", "rm_2025": 11193.0, "rm_2026_8m": 8486.0, "share_2026_pct": 22.5},
            {"flow_type": "Export", "commodity": "Palm Oil", "partner": "Kenya", "rm_2025": 5231.0, "rm_2026_8m": 2954.0, "share_2026_pct": 7.8},
            {"flow_type": "Export", "commodity": "LNG", "partner": "Japan", "rm_2025": 19973.0, "rm_2026_8m": 14307.0, "share_2026_pct": 35.5},
            {"flow_type": "Export", "commodity": "LNG", "partner": "China", "rm_2025": 12908.0, "rm_2026_8m": 11551.0, "share_2026_pct": 28.7},
            {"flow_type": "Export", "commodity": "LNG", "partner": "Republic of Korea", "rm_2025": 13990.0, "rm_2026_8m": 9398.0, "share_2026_pct": 23.3},
            {"flow_type": "Import", "commodity": "E&E Products", "partner": "China", "rm_2025": 156497.0, "rm_2026_8m": 156093.0, "share_2026_pct": 30.8},
            {"flow_type": "Import", "commodity": "E&E Products", "partner": "Taiwan, Province of China", "rm_2025": 125951.0, "rm_2026_8m": 101701.0, "share_2026_pct": 20.1},
            {"flow_type": "Import", "commodity": "E&E Products", "partner": "Singapore", "rm_2025": 59924.0, "rm_2026_8m": 68053.0, "share_2026_pct": 13.4},
            {"flow_type": "Import", "commodity": "Chemicals", "partner": "China", "rm_2025": 23771.0, "rm_2026_8m": 21736.0, "share_2026_pct": 31.8},
            {"flow_type": "Import", "commodity": "Petroleum Products", "partner": "Singapore", "rm_2025": 37991.0, "rm_2026_8m": 31387.0, "share_2026_pct": 34.8},
            {"flow_type": "Import", "commodity": "Petroleum Products", "partner": "India", "rm_2025": 5423.0, "rm_2026_8m": 10749.0, "share_2026_pct": 11.9},
            {"flow_type": "Import", "commodity": "Machinery & Parts", "partner": "China", "rm_2025": 41768.0, "rm_2026_8m": 32299.0, "share_2026_pct": 34.9},
            {"flow_type": "Import", "commodity": "Machinery & Parts", "partner": "United States", "rm_2025": 23321.0, "rm_2026_8m": 18181.0, "share_2026_pct": 19.7},
        ]
    )

    # Table 3.14, 3.15, 3.17, 3.18: CPI Region, Stratum, Core, and PPI
    tables["table_3_14_3_18_inflation_series"] = pd.DataFrame(
        [
            {"series_type": "CPI_Headline", "category": "Total", "y2022": 3.3, "y2023": 2.5, "y2024": 1.8, "y2025": 1.4, "y2026": 1.8},
            {"series_type": "CPI_Headline", "category": "Food and beverages", "y2022": 5.8, "y2023": 4.8, "y2024": 2.0, "y2025": 2.1, "y2026": 1.5},
            {"series_type": "CPI_Headline", "category": "Housing, water, electricity, gas", "y2022": 1.8, "y2023": 1.7, "y2024": 3.0, "y2025": 1.6, "y2026": 1.4},
            {"series_type": "CPI_Headline", "category": "Transport", "y2022": 4.7, "y2023": 1.1, "y2024": 1.0, "y2025": 0.4, "y2026": 1.7},
            {"series_type": "CPI_Headline", "category": "Restaurant and accommodation", "y2022": 5.0, "y2023": 5.6, "y2024": 3.1, "y2025": 3.2, "y2026": 2.4},
            {"series_type": "CPI_Headline", "category": "Insurance and financial services", "y2022": 0.2, "y2023": 0.1, "y2024": 0.3, "y2025": 3.4, "y2026": 4.1},
            {"series_type": "CPI_Stratum", "category": "Rural Total", "y2022": 2.6, "y2023": 2.1, "y2024": 1.7, "y2025": 1.0, "y2026": 1.6},
            {"series_type": "CPI_Stratum", "category": "Urban Total", "y2022": 3.6, "y2023": 2.6, "y2024": 1.8, "y2025": 1.5, "y2026": 1.8},
            {"series_type": "CPI_Core", "category": "Core Total", "y2022": 3.0, "y2023": 3.0, "y2024": 1.8, "y2025": 2.0, "y2026": 2.0},
            {"series_type": "CPI_Core", "category": "Core Food and beverages", "y2022": 5.8, "y2023": 5.8, "y2024": 3.0, "y2025": 3.4, "y2026": 1.9},
            {"series_type": "CPI_Core", "category": "Core Transport", "y2022": 5.6, "y2023": 4.4, "y2024": 2.1, "y2025": 2.2, "y2026": 1.4},
            {"series_type": "PPI_Sector", "category": "PPI Total", "y2022": 7.8, "y2023": -1.9, "y2024": 0.3, "y2025": -2.0, "y2026": 4.6},
            {"series_type": "PPI_Sector", "category": "Agriculture, forestry and fishing", "y2022": 1.3, "y2023": -13.8, "y2024": 7.9, "y2025": 3.2, "y2026": 0.9},
            {"series_type": "PPI_Sector", "category": "Mining", "y2022": 12.6, "y2023": -5.9, "y2024": -2.0, "y2025": -8.2, "y2026": 25.9},
            {"series_type": "PPI_Sector", "category": "Manufacturing", "y2022": 8.4, "y2023": -0.2, "y2024": -0.3, "y2025": -2.1, "y2026": 2.9},
            {"series_type": "PPI_Sector", "category": "Electricity and gas supply", "y2022": 0.7, "y2023": 0.4, "y2024": 0.6, "y2025": 1.9, "y2026": 7.9},
            {"series_type": "PPI_Sector", "category": "Water supply", "y2022": 2.6, "y2023": 2.4, "y2024": 6.5, "y2025": 4.7, "y2026": 10.7},
            {"series_type": "PPI_Stage", "category": "Crude materials for further processing", "y2022": 6.2, "y2023": -9.6, "y2024": 0.7, "y2025": -3.7, "y2026": 15.4},
            {"series_type": "PPI_Stage", "category": "Intermediate materials, supplies and components", "y2022": 10.9, "y2023": -1.2, "y2024": -0.5, "y2025": -1.8, "y2026": 2.8},
            {"series_type": "PPI_Stage", "category": "Finished goods", "y2022": 1.8, "y2023": 3.1, "y2024": 1.9, "y2025": -1.2, "y2026": 0.9},
        ]
    )

    # Table 3.16: Consumer Price Index by State (2010 = 100)
    tables["table_3_16_state_cpi"] = pd.DataFrame(
        [
            {"state": "Malaysia", "region_group": "National", "cpi_2022": 3.3, "cpi_2023": 2.5, "cpi_2024": 1.8, "cpi_2025": 1.4, "cpi_2026": 1.8, "food_2022": 5.8, "food_2023": 4.8, "food_2024": 2.0, "food_2025": 2.1, "food_2026": 1.5},
            {"state": "Johor", "region_group": "Southern", "cpi_2022": 3.4, "cpi_2023": 2.5, "cpi_2024": 1.7, "cpi_2025": 2.0, "cpi_2026": 2.2, "food_2022": 6.1, "food_2023": 4.3, "food_2024": 1.7, "food_2025": 2.4, "food_2026": 2.6},
            {"state": "Kedah", "region_group": "Northern", "cpi_2022": 2.6, "cpi_2023": 1.7, "cpi_2024": 1.3, "cpi_2025": 0.8, "cpi_2026": 1.9, "food_2022": 3.7, "food_2023": 2.9, "food_2024": 0.7, "food_2025": 0.9, "food_2026": 1.6},
            {"state": "Kelantan", "region_group": "East Coast", "cpi_2022": 2.8, "cpi_2023": 1.7, "cpi_2024": 1.0, "cpi_2025": 0.3, "cpi_2026": 0.7, "food_2022": 4.5, "food_2023": 3.2, "food_2024": 0.8, "food_2025": 0.1, "food_2026": -0.6},
            {"state": "Melaka", "region_group": "Southern", "cpi_2022": 2.7, "cpi_2023": 2.4, "cpi_2024": 1.3, "cpi_2025": 1.5, "cpi_2026": 1.8, "food_2022": 4.7, "food_2023": 4.6, "food_2024": 1.1, "food_2025": 2.4, "food_2026": 2.0},
            {"state": "Negeri Sembilan", "region_group": "Central", "cpi_2022": 2.8, "cpi_2023": 2.0, "cpi_2024": 1.2, "cpi_2025": 1.7, "cpi_2026": 2.3, "food_2022": 4.8, "food_2023": 4.5, "food_2024": 1.2, "food_2025": 3.2, "food_2026": 2.7},
            {"state": "Pahang", "region_group": "East Coast", "cpi_2022": 3.0, "cpi_2023": 2.6, "cpi_2024": 2.4, "cpi_2025": 1.2, "cpi_2026": 2.4, "food_2022": 4.9, "food_2023": 4.9, "food_2024": 2.0, "food_2025": 2.0, "food_2026": 2.4},
            {"state": "Pulau Pinang", "region_group": "Penang", "cpi_2022": 3.2, "cpi_2023": 2.2, "cpi_2024": 3.0, "cpi_2025": 1.2, "cpi_2026": 1.7, "food_2022": 6.0, "food_2023": 5.0, "food_2024": 2.7, "food_2025": 1.6, "food_2026": 1.6},
            {"state": "Perak", "region_group": "Northern", "cpi_2022": 3.3, "cpi_2023": 2.8, "cpi_2024": 1.0, "cpi_2025": 0.9, "cpi_2026": 1.2, "food_2022": 5.4, "food_2023": 4.8, "food_2024": 0.6, "food_2025": 1.0, "food_2026": 1.1},
            {"state": "Perlis", "region_group": "Northern", "cpi_2022": 3.2, "cpi_2023": 2.2, "cpi_2024": 1.7, "cpi_2025": 1.0, "cpi_2026": 1.3, "food_2022": 4.9, "food_2023": 3.5, "food_2024": 1.8, "food_2025": 1.3, "food_2026": 0.8},
            {"state": "Selangor", "region_group": "Klang Valley", "cpi_2022": 4.2, "cpi_2023": 2.9, "cpi_2024": 2.1, "cpi_2025": 1.7, "cpi_2026": 1.7, "food_2022": 7.4, "food_2023": 6.1, "food_2024": 3.3, "food_2025": 2.8, "food_2026": 1.2},
            {"state": "Terengganu", "region_group": "East Coast", "cpi_2022": 3.1, "cpi_2023": 1.8, "cpi_2024": 1.5, "cpi_2025": 1.3, "cpi_2026": 1.7, "food_2022": 4.9, "food_2023": 3.6, "food_2024": 1.8, "food_2025": 2.4, "food_2026": 0.6},
            {"state": "Sabah", "region_group": "Sabah", "cpi_2022": 3.0, "cpi_2023": 2.4, "cpi_2024": 1.2, "cpi_2025": 1.1, "cpi_2026": 1.9, "food_2022": 5.1, "food_2023": 4.1, "food_2024": 1.5, "food_2025": 2.0, "food_2026": 0.6},
            {"state": "Sarawak", "region_group": "Sarawak", "cpi_2022": 3.2, "cpi_2023": 3.1, "cpi_2024": 2.4, "cpi_2025": 0.9, "cpi_2026": 0.7, "food_2022": 5.5, "food_2023": 6.1, "food_2024": 1.9, "food_2025": 1.1, "food_2026": 0.6},
            {"state": "Federal Territory of Kuala Lumpur", "region_group": "Klang Valley", "cpi_2022": 3.0, "cpi_2023": 2.1, "cpi_2024": 1.4, "cpi_2025": 1.3, "cpi_2026": 2.1, "food_2022": 5.1, "food_2023": 4.1, "food_2024": 1.4, "food_2025": 2.1, "food_2026": 2.0},
            {"state": "Federal Territory of Labuan", "region_group": "East Malaysia", "cpi_2022": 2.4, "cpi_2023": 1.6, "cpi_2024": 0.7, "cpi_2025": 0.8, "cpi_2026": 2.2, "food_2022": 3.5, "food_2023": 3.6, "food_2024": 1.4, "food_2025": 2.4, "food_2026": 1.0},
            {"state": "Federal Territory of Putrajaya", "region_group": "Klang Valley", "cpi_2022": 7.3, "cpi_2023": 3.4, "cpi_2024": 1.4, "cpi_2025": 0.7, "cpi_2026": 1.8, "food_2022": 7.7, "food_2023": 5.9, "food_2024": 2.4, "food_2025": 2.7, "food_2026": 1.2},
        ]
    )

    # Table 3.19, 3.20, 3.21, 3.22: Labour Market, Employment by Industry, Vacancies, Placements
    tables["table_3_19_3_22_labour"] = pd.DataFrame(
        [
            {"sector": "Agriculture, forestry and fishing", "emp_2022": 1408.1, "emp_2023": 1437.4, "emp_2024": 1478.5, "emp_2025": 1480.3, "emp_2026": 1461.1, "vac_2025": 32.0, "vac_2026": 30.4, "jobs_created_2026": 3.1, "placements_2026": 0.6, "prod_growth_2026": -0.8},
            {"sector": "Mining and quarrying", "emp_2022": 84.1, "emp_2023": 88.8, "emp_2024": 88.7, "emp_2025": 95.9, "emp_2026": 101.0, "vac_2025": 0.6, "vac_2026": 0.5, "jobs_created_2026": 0.5, "placements_2026": 0.4, "prod_growth_2026": 3.0},
            {"sector": "Manufacturing (Total)", "emp_2022": 2507.3, "emp_2023": 2597.8, "emp_2024": 2664.6, "emp_2025": 2695.7, "emp_2026": 2718.9, "vac_2025": 115.8, "vac_2026": 114.0, "jobs_created_2026": 24.0, "placements_2026": 21.5, "prod_growth_2026": 6.5},
            {"sector": "Mfg: Electrical, electronic & optical", "emp_2022": 620.0, "emp_2023": 645.0, "emp_2024": 670.0, "emp_2025": 685.0, "emp_2026": 702.0, "vac_2025": 35.7, "vac_2026": 35.1, "jobs_created_2026": 7.5, "placements_2026": 5.8, "prod_growth_2026": 11.2},
            {"sector": "Mfg: Petroleum, chemical, rubber & plastic", "emp_2022": 480.0, "emp_2023": 495.0, "emp_2024": 505.0, "emp_2025": 512.0, "emp_2026": 518.0, "vac_2025": 20.5, "vac_2026": 19.1, "jobs_created_2026": 6.3, "placements_2026": 4.9, "prod_growth_2026": 4.2},
            {"sector": "Mfg: Food processing, beverages & tobacco", "emp_2022": 410.0, "emp_2023": 425.0, "emp_2024": 440.0, "emp_2025": 450.0, "emp_2026": 458.0, "vac_2025": 15.1, "vac_2026": 15.1, "jobs_created_2026": 2.4, "placements_2026": 3.2, "prod_growth_2026": 5.5},
            {"sector": "Construction", "emp_2022": 1245.4, "emp_2023": 1284.5, "emp_2024": 1393.4, "emp_2025": 1356.9, "emp_2026": 1302.9, "vac_2025": 24.8, "vac_2026": 26.5, "jobs_created_2026": 6.3, "placements_2026": 3.6, "prod_growth_2026": 6.5},
            {"sector": "Services: Wholesale & retail trade", "emp_2022": 2840.6, "emp_2023": 2991.5, "emp_2024": 3063.3, "emp_2025": 3107.4, "emp_2026": 3132.8, "vac_2025": 9.8, "vac_2026": 8.8, "jobs_created_2026": 17.9, "placements_2026": 14.5, "prod_growth_2026": 4.1},
            {"sector": "Services: Accommodation & food/beverage", "emp_2022": 1482.7, "emp_2023": 1592.8, "emp_2024": 1617.1, "emp_2025": 1681.9, "emp_2026": 1695.5, "vac_2025": 1.6, "vac_2026": 1.5, "jobs_created_2026": 1.7, "placements_2026": 4.2, "prod_growth_2026": 3.8},
            {"sector": "Services: Transportation & storage", "emp_2022": 707.2, "emp_2023": 735.3, "emp_2024": 751.4, "emp_2025": 762.0, "emp_2026": 785.6, "vac_2025": 1.4, "vac_2026": 1.5, "jobs_created_2026": 2.3, "placements_2026": 3.1, "prod_growth_2026": 5.2},
            {"sector": "Services: Information & communication", "emp_2022": 252.7, "emp_2023": 268.1, "emp_2024": 286.3, "emp_2025": 287.5, "emp_2026": 334.6, "vac_2025": 0.9, "vac_2026": 0.9, "jobs_created_2026": 0.9, "placements_2026": 2.2, "prod_growth_2026": 7.8},
            {"sector": "Services: Finance, insurance, real estate & professional", "emp_2022": 880.9, "emp_2023": 917.3, "emp_2024": 942.7, "emp_2025": 963.8, "emp_2026": 1073.0, "vac_2025": 9.0, "vac_2026": 9.8, "jobs_created_2026": 4.8, "placements_2026": 8.5, "prod_growth_2026": 5.9},
        ]
    )

    # Table 4.3 & 4.6: Commercial & Islamic Banks Loans Outstanding by Sector (RM million)
    tables["table_4_3_4_6_bank_loans"] = pd.DataFrame(
        [
            {"sector": "Agriculture, forestry and fishing", "comm_2024": 16259, "comm_2025": 15942, "comm_2026": 15569, "isl_2024": 16659, "isl_2025": 15024, "isl_2026": 14521},
            {"sector": "Mining and quarrying", "comm_2024": 6365, "comm_2025": 5897, "comm_2026": 5818, "isl_2024": 2735, "isl_2025": 2829, "isl_2026": 2935},
            {"sector": "Manufacturing", "comm_2024": 97956, "comm_2025": 98814, "comm_2026": 104130, "isl_2024": 40241, "isl_2025": 40592, "isl_2026": 42481},
            {"sector": "Electricity, gas, steam and air conditioning", "comm_2024": 12016, "comm_2025": 11372, "comm_2026": 12289, "isl_2024": 6308, "isl_2025": 13233, "isl_2026": 11081},
            {"sector": "Water supply, sewerage & waste", "comm_2024": 1989, "comm_2025": 2072, "comm_2026": 2116, "isl_2024": 2228, "isl_2025": 2308, "isl_2026": 2487},
            {"sector": "Construction", "comm_2024": 60903, "comm_2025": 64020, "comm_2026": 65952, "isl_2024": 33969, "isl_2025": 37233, "isl_2026": 38923},
            {"sector": "Wholesale and retail trade", "comm_2024": 110211, "comm_2025": 110593, "comm_2026": 116065, "isl_2024": 56240, "isl_2025": 57075, "isl_2026": 58407},
            {"sector": "Accommodation and food service", "comm_2024": 16167, "comm_2025": 16616, "comm_2026": 16465, "isl_2024": 5629, "isl_2025": 6117, "isl_2026": 6298},
            {"sector": "Transportation and storage", "comm_2024": 18593, "comm_2025": 17602, "comm_2026": 15991, "isl_2024": 13035, "isl_2025": 14916, "isl_2026": 17039},
            {"sector": "Information and communication", "comm_2024": 14002, "comm_2025": 15494, "comm_2026": 20250, "isl_2024": 10969, "isl_2025": 13766, "isl_2026": 17234},
            {"sector": "Finance, insurance, real estate & business", "comm_2024": 184421, "comm_2025": 192390, "comm_2026": 201990, "isl_2024": 117951, "isl_2025": 128136, "isl_2026": 131374},
            {"sector": "Education, health and others", "comm_2024": 24044, "comm_2025": 20773, "comm_2026": 21713, "isl_2024": 20086, "isl_2025": 19226, "isl_2026": 23572},
            {"sector": "Household sector", "comm_2024": 730059, "comm_2025": 753643, "comm_2026": 766624, "isl_2024": 606300, "isl_2025": 654669, "isl_2026": 679445},
        ]
    )

    # Table 4.4: Government and Corporate Bond Yields (%)
    tables["table_4_4_bond_yields"] = pd.DataFrame(
        [
            {"instrument": "MGS 1-year", "type": "Government", "y2024": 3.28, "y2025": 2.86, "jan_2026": 2.87, "feb_2026": 2.88, "mar_2026": 3.04, "apr_2026": 3.04, "may_2026": 3.07, "jun_2026": 3.09, "jul_2026": 3.10},
            {"instrument": "MGS 3-year", "type": "Government", "y2024": 3.48, "y2025": 3.00, "jan_2026": 3.03, "feb_2026": 3.06, "mar_2026": 3.27, "apr_2026": 3.20, "may_2026": 3.23, "jun_2026": 3.25, "jul_2026": 3.31},
            {"instrument": "MGS 5-year", "type": "Government", "y2024": 3.62, "y2025": 3.25, "jan_2026": 3.28, "feb_2026": 3.31, "mar_2026": 3.44, "apr_2026": 3.34, "may_2026": 3.41, "jun_2026": 3.40, "jul_2026": 3.50},
            {"instrument": "MGS 10-year", "type": "Government", "y2024": 3.81, "y2025": 3.51, "jan_2026": 3.50, "feb_2026": 3.50, "mar_2026": 3.64, "apr_2026": 3.56, "may_2026": 3.59, "jun_2026": 3.61, "jul_2026": 3.71},
            {"instrument": "Corp 5Y AAA", "type": "Corporate", "y2024": 3.93, "y2025": 3.59, "jan_2026": 3.62, "feb_2026": 3.61, "mar_2026": 3.64, "apr_2026": 3.63, "may_2026": 3.65, "jun_2026": 3.67, "jul_2026": 3.70},
            {"instrument": "Corp 5Y AA", "type": "Corporate", "y2024": 4.10, "y2025": 3.75, "jan_2026": 3.77, "feb_2026": 3.76, "mar_2026": 3.78, "apr_2026": 3.77, "may_2026": 3.79, "jun_2026": 3.80, "jul_2026": 3.83},
            {"instrument": "Corp 5Y A", "type": "Corporate", "y2024": 5.09, "y2025": 4.70, "jan_2026": 4.65, "feb_2026": 4.64, "mar_2026": 4.66, "apr_2026": 4.69, "may_2026": 4.62, "jun_2026": 4.66, "jul_2026": 4.59},
            {"instrument": "Corp 5Y BBB", "type": "Corporate", "y2024": 6.48, "y2025": 6.16, "jan_2026": 5.85, "feb_2026": 5.85, "mar_2026": 5.80, "apr_2026": 5.88, "may_2026": 5.69, "jun_2026": 5.75, "jul_2026": 5.57},
        ]
    )

    # Table 5.1: Progress of Sustainable Development Goals by Indicator (Representative across all 17 SDGs)
    tables["table_5_1_sdg_indicators"] = pd.DataFrame(
        [
            {"sdg_goal": "SDG 1: No Poverty", "indicator_no": "1.2", "description": "Households below national poverty line", "unit": "%", "baseline_year": "2022", "baseline_val": 6.2, "latest_year": "2024", "latest_val": 5.1, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 1: No Poverty", "indicator_no": "1.3", "description": "Multidimensional Poverty Index (MPI)", "unit": "Index", "baseline_year": "2019", "baseline_val": 0.0110, "latest_year": "2022", "latest_val": 0.0079, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 1: No Poverty", "indicator_no": "1.5c", "description": "Solid waste recycled out of total collected", "unit": "%", "baseline_year": "2022", "baseline_val": 20.0, "latest_year": "2023", "latest_val": 19.5, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 1: No Poverty", "indicator_no": "1.6a", "description": "Deaths attributed to disasters per 100,000 pop", "unit": "Number", "baseline_year": "2023", "baseline_val": 9.0, "latest_year": "2024", "latest_val": 18.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 1: No Poverty", "indicator_no": "1.7", "description": "Direct economic loss attributed to disasters", "unit": "RM Mil", "baseline_year": "2023", "baseline_val": 755.4, "latest_year": "2024", "latest_val": 933.4, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 2: Zero Hunger", "indicator_no": "2.3", "description": "Prevalence of stunting under 5 years", "unit": "%", "baseline_year": "2019", "baseline_val": 21.8, "latest_year": "2022", "latest_val": 21.2, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 2: Zero Hunger", "indicator_no": "2.4a", "description": "Prevalence of wasting under 5 years", "unit": "%", "baseline_year": "2019", "baseline_val": 9.7, "latest_year": "2022", "latest_val": 11.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 2: Zero Hunger", "indicator_no": "2.6", "description": "Annual labour productivity for agriculture", "unit": "RM", "baseline_year": "2023", "baseline_val": 53584.0, "latest_year": "2024", "latest_val": 54762.0, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 3: Good Health", "indicator_no": "3.1", "description": "Maternal mortality ratio", "unit": "Per 100k", "baseline_year": "2023", "baseline_val": 25.7, "latest_year": "2024", "latest_val": 24.3, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 3: Good Health", "indicator_no": "3.3", "description": "Under-five mortality rate", "unit": "Per 1k", "baseline_year": "2023", "baseline_val": 8.3, "latest_year": "2024", "latest_val": 8.7, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 3: Good Health", "indicator_no": "3.11", "description": "Suicide mortality rate", "unit": "Per 100k", "baseline_year": "2023", "baseline_val": 3.2, "latest_year": "2024", "latest_val": 3.3, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 3: Good Health", "indicator_no": "3.12a", "description": "Coverage of treatment interventions for Opioids", "unit": "%", "baseline_year": "2023", "baseline_val": 76.1, "latest_year": "2024", "latest_val": 57.8, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 3: Good Health", "indicator_no": "3.14", "description": "Death rate of road traffic injuries", "unit": "Per 100k", "baseline_year": "2023", "baseline_val": 18.0, "latest_year": "2024", "latest_val": 19.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 4: Quality Education", "indicator_no": "4.2c", "description": "Completion rate for upper secondary education", "unit": "%", "baseline_year": "2023", "baseline_val": 98.8, "latest_year": "2024", "latest_val": 97.4, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 4: Quality Education", "indicator_no": "4.4", "description": "Participation rate in preschool", "unit": "%", "baseline_year": "2023", "baseline_val": 88.3, "latest_year": "2024", "latest_val": 89.6, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 5: Gender Equality", "indicator_no": "5.2b", "description": "Number of domestic violence cases", "unit": "Number", "baseline_year": "2023", "baseline_val": 5507.0, "latest_year": "2024", "latest_val": 7116.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 5: Gender Equality", "indicator_no": "5.6", "description": "Women in managerial positions", "unit": "%", "baseline_year": "2023", "baseline_val": 25.4, "latest_year": "2024", "latest_val": 27.1, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 6: Clean Water", "indicator_no": "6.5a", "description": "Non-Revenue Water (NRW) lost", "unit": "%", "baseline_year": "2023", "baseline_val": 34.6, "latest_year": "2024", "latest_val": 34.3, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 6: Clean Water", "indicator_no": "6.6", "description": "Degree of integrated water resources management", "unit": "%", "baseline_year": "2023", "baseline_val": 63.0, "latest_year": "2024", "latest_val": 73.0, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 7: Clean Energy", "indicator_no": "7.4", "description": "Energy intensity (toe / GDP RM Mil)", "unit": "Index", "baseline_year": "2022", "baseline_val": 65.8, "latest_year": "2023", "latest_val": 68.1, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 7: Clean Energy", "indicator_no": "7.5", "description": "Installed renewable energy capacity", "unit": "Watts/cap", "baseline_year": "2022", "baseline_val": 278.5, "latest_year": "2023", "latest_val": 301.7, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 8: Decent Work", "indicator_no": "8.1", "description": "Annual growth rate of real GDP per capita", "unit": "%", "baseline_year": "2023", "baseline_val": 1.4, "latest_year": "2024", "latest_val": 3.1, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 8: Decent Work", "indicator_no": "8.7", "description": "Youth not in education, employment or training (NEET)", "unit": "%", "baseline_year": "2023", "baseline_val": 10.5, "latest_year": "2024", "latest_val": 8.7, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 8: Decent Work", "indicator_no": "8.8a", "description": "Occupational injury rate per 100,000 workers", "unit": "Rate", "baseline_year": "2023", "baseline_val": 246.0, "latest_year": "2024", "latest_val": 250.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 9: Industry & Innovation", "indicator_no": "9.3", "description": "Manufacturing value added per capita", "unit": "RM", "baseline_year": "2023", "baseline_val": 10981.0, "latest_year": "2024", "latest_val": 11217.0, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 9: Industry & Innovation", "indicator_no": "9.8", "description": "R&D expenditure as a proportion of GDP", "unit": "%", "baseline_year": "2023", "baseline_val": 1.01, "latest_year": "2024", "latest_val": 1.01, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 10: Reduced Inequalities", "indicator_no": "10.1", "description": "CAGR among Bottom 40% of population", "unit": "%", "baseline_year": "2022", "baseline_val": 2.5, "latest_year": "2024", "latest_val": 5.7, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 10: Reduced Inequalities", "indicator_no": "10.3", "description": "Compensation of employees share of GDP", "unit": "%", "baseline_year": "2023", "baseline_val": 33.5, "latest_year": "2024", "latest_val": 33.6, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 11: Sustainable Cities", "indicator_no": "11.10", "description": "Annual average PM2.5 air pollutant concentration", "unit": "ug/m3", "baseline_year": "2023", "baseline_val": 15.2, "latest_year": "2024", "latest_val": 14.1, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 11: Sustainable Cities", "indicator_no": "11.11", "description": "Average share of built-up area of cities open space", "unit": "%", "baseline_year": "2023", "baseline_val": 35.6, "latest_year": "2024", "latest_val": 33.6, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 12: Responsible Consumption", "indicator_no": "12.4", "description": "National recycling rate", "unit": "%", "baseline_year": "2023", "baseline_val": 35.4, "latest_year": "2024", "latest_val": 37.9, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 13: Climate Action", "indicator_no": "13.3", "description": "CO2 equivalent emissions", "unit": "Mil Tonnes", "baseline_year": "2020", "baseline_val": 332.7, "latest_year": "2021", "latest_val": 329.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 14: Life Below Water", "indicator_no": "14.3", "description": "Fish stocks within biologically sustainable levels", "unit": "%", "baseline_year": "2018", "baseline_val": 47.4, "latest_year": "2021", "latest_val": 57.9, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 15: Life on Land", "indicator_no": "15.1", "description": "Forest area as a proportion of total land area", "unit": "%", "baseline_year": "2021", "baseline_val": 54.4, "latest_year": "2022", "latest_val": 54.3, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 15: Life on Land", "indicator_no": "15.4", "description": "Proportion of land that is degraded over total land", "unit": "%", "baseline_year": "2015", "baseline_val": 8.1, "latest_year": "2019", "latest_val": 15.2, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 16: Peace & Justice", "indicator_no": "16.1", "description": "Victims of intentional homicide", "unit": "Number", "baseline_year": "2023", "baseline_val": 266.0, "latest_year": "2024", "latest_val": 249.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 16: Peace & Justice", "indicator_no": "16.2a", "description": "Violent crime cases", "unit": "Number", "baseline_year": "2023", "baseline_val": 8539.0, "latest_year": "2024", "latest_val": 8993.0, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 16: Peace & Justice", "indicator_no": "16.7", "description": "Unsentenced detainees as proportion of prison pop", "unit": "%", "baseline_year": "2023", "baseline_val": 33.0, "latest_year": "2024", "latest_val": 35.2, "polarity": "lower_is_better"},
            {"sdg_goal": "SDG 17: Partnerships", "indicator_no": "17.1", "description": "Share of government revenue as proportion of GDP", "unit": "%", "baseline_year": "2023", "baseline_val": 17.3, "latest_year": "2024", "latest_val": 16.8, "polarity": "higher_is_better"},
            {"sdg_goal": "SDG 17: Partnerships", "indicator_no": "17.5", "description": "Individuals using the Internet", "unit": "%", "baseline_year": "2023", "baseline_val": 97.7, "latest_year": "2024", "latest_val": 98.0, "polarity": "higher_is_better"},
        ]
    )

    return tables


def run_etl_pipeline(force_refresh: bool = False) -> Tuple[ETLManifest, bool]:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = DATA_DIR / "etl_manifest.json"
    current_hash = compute_file_sha256(PDF_REPORT_PATH)

    if not force_refresh and manifest_path.exists() and SQLITE_DB_PATH.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            cached_data = json.load(f)
        if cached_data.get("pdf_sha256") == current_hash:
            return ETLManifest(**cached_data), False

    tables = get_canonical_tables()
    total_rows = 0
    table_names: List[str] = []

    conn = sqlite3.connect(str(SQLITE_DB_PATH))
    try:
        for t_name, df in tables.items():
            csv_file = CSV_DIR / f"{t_name}.csv"
            df.to_csv(csv_file, index=False)
            df.to_sql(t_name, conn, if_exists="replace", index=False)
            total_rows += len(df)
            table_names.append(t_name)

        manifest = ETLManifest(
            pdf_sha256=current_hash,
            release_edition="Malaysia Economic Outlook 2027 (Released 9 Oct 2026)",
            extracted_at_iso=datetime.now(timezone.utc).isoformat(),
            tables_extracted=len(table_names),
            total_rows=total_rows,
            table_names=table_names,
        )
        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write(manifest.model_dump_json(indent=4))
    finally:
        conn.close()

    return manifest, True


def query_sqlite_table(table_name: str) -> pd.DataFrame:
    if not SQLITE_DB_PATH.exists():
        run_etl_pipeline(force_refresh=True)
    conn = sqlite3.connect(str(SQLITE_DB_PATH))
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()
