## About
Coffee Scrub simulates the process of cleaning messy transaction data. It standardizes inconsistent columns, handles missing values, and prepares data for analysis such as customer segmentation or sales trends.

**Update** This project now has been updated with an end-to-end pipeline which uses Airflow to orchestrate and monitor transaction data. See SRC folder for DAG.

Key goals:  
- Demonstrate best practices in data preprocessing by moving from raw “dirty” inputs to typed, analytics-ready tables (staging → production).
- Provide reusable, pipeline-friendly cleaning/loading steps orchestrated in an end-to-end Airflow DAG (connection check → stage → production → dimension → fact).
- Improve reliability for visualization and modeling by publishing a stable production schema plus a simple star design ( `item_dimension`  +  `coffee_sales_fact` ) for analysis.
  
## Dataset
This project uses the [Dirty Cafe Sales Dataset] dataset from Kaggle.

**Source:** [Kaggle Dataset Link](https://www.kaggle.com/datasets/ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training?resource=download)  
**Author:** [Ahmed Mohamed]  
**License:** [CC BY-SA 4.0]  
**Downloaded:** [October 2025]

## Quick Start
1. Clone repo `git clone http://github.com/petrolmonkey/coffeescrub.git`
2. Go to ➡️ `cd coffee-scrub`
3. Jupyter `jupyter notebook notebooks/cafe-data-cleaning.ipynb`

## Technologies Used  
- Pandas 2.3.3, Python3.13.9, Jupyter, Airflow 2.7.3, mySQL Workbench 8.0
