# Vendor Performance Analysis

## 📌 Project Overview
This project analyzes vendor, brand, and inventory performance for a retail/wholesale business to identify profitability drivers, inefficiencies, and operational risks.  
The goal is to support data-driven decisions around pricing, inventory management, vendor diversification, and cost optimization.

The analysis combines **SQL, Python-based EDA, statistical validation, and a Power BI dashboard** to deliver both analytical depth and clear business insights.

---

## 🎯 Business Objectives
- Identify top and low-performing vendors and brands
- Analyze sales, purchases, profit margins, and inventory turnover
- Detect unsold and slow-moving inventory tying up capital
- Understand the impact of bulk purchasing on unit cost
- Measure dependency on a small set of vendors
- Validate insights using statistical techniques

---

## 🧩 Dataset Description
The dataset contains vendor-level and brand-level transactional data, including:
- Purchase and sales quantities
- Purchase price and actual sales price
- Total purchase and sales values
- Gross profit and profit margin
- Freight cost and excise tax
- Inventory and stock turnover metrics

Data was stored in **MySQL** and accessed using **SQLAlchemy**.

---

## 🛠️ Tools & Technologies Used
- **Python** – Data analysis and statistical validation  
- **SQL** – Data extraction, aggregation, and transformations  
- **MySQL** – Database for storing transactional data  
- **SQLAlchemy** – Engine creation and database connectivity  
- **Jupyter Notebook** – Exploratory Data Analysis (EDA)  
- **Power BI** – Interactive dashboard and business reporting  
- **Pandas, NumPy, Matplotlib, Seaborn** – Data manipulation and visualization  

---

## 🔍 Analysis Approach

### 1. Data Extraction
- Connected to MySQL using SQLAlchemy
- Pulled vendor, brand, sales, and purchase data using SQL queries
- Cleaned and transformed data in Python

### 2. Exploratory Data Analysis (EDA)
- Analyzed distributions of sales, profit margins, prices, and inventory
- Identified:
  - Zero and negative profit cases
  - Unsold inventory and slow-moving stock
  - Pricing and freight cost outliers
- Filtered non-meaningful records (e.g., zero sales, negative margins)

### 3. Key Metrics Computed
- Total Sales and Total Purchases
- Gross Profit and Profit Margin
- Stock Turnover
- Sales-to-Purchase Ratio
- Unsold Inventory Capital
- Vendor and Brand Contribution %

### 4. Vendor & Brand Performance Analysis
- Ranked top vendors by sales and purchase contribution
- Identified low-performing vendors with poor stock turnover
- Found brands with low sales but high margins for pricing or promotion opportunities
- Highlighted over-dependence on top vendors

### 5. Statistical Validation
- Correlation analysis between pricing, sales, profit, and turnover
- Confidence interval comparison for profit margins
- Hypothesis testing to compare top vs. low-performing vendors
- Confirmed that different vendor groups follow different profitability models

---

## 📊 Dashboard Highlights (Power BI)
The interactive dashboard includes:
- Total Sales, Gross Profit, Avg Profit Margin
- Unsold Inventory Capital
- Vendor Purchase Contribution (%)
- Top Vendors by Sales
- Top Brands by Sales
- Low Performing Vendors (Stock Turnover)
- Low Performing Brands (Sales vs Profit Margin)

The dashboard allows business users to quickly identify risks, opportunities, and action areas.

---

## 💡 Key Insights
- A small group of vendors contributes the majority of purchases and sales, increasing supply-chain risk
- Bulk purchasing reduces unit cost significantly and improves profitability
- Large unsold inventory indicates inefficient stock planning
- Some vendors maintain high margins but struggle with sales volume
- High sales do not always translate to high profit margins

---

## 📌 Business Recommendations
- Diversify vendor base to reduce dependency risk
- Optimize pricing for low-sales, high-margin brands
- Reduce slow-moving inventory through better demand planning
- Use bulk purchasing strategically for cost savings
- Improve marketing and distribution for underperforming vendors

---

## 🚀 Project Outcome
This project delivers a complete **end-to-end data analysis solution**, combining backend data extraction, statistical analysis, and executive-level visualization to support smarter vendor and inventory decisions.
