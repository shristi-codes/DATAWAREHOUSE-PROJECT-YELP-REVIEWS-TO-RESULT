# 📊 The Yelp Effect: From Reviews to Results

A data warehouse and sentiment analysis project focused on extracting actionable insights from Yelp's open dataset. This end-to-end pipeline leverages ETL, dimensional modeling, and text mining to uncover patterns across industries — with a special focus on customer satisfaction and review sentiment.

## 🧠 Motivation

Reviews drive decision-making in modern digital economies. By analyzing Yelp reviews and check-ins, we aim to:
- Identify factors that influence positive customer experiences.
- Detect industry-specific pain points.
- Help businesses enhance services through data-driven decisions.

## 🛠️ Tech Stack

- **ETL & Orchestration:** Python, Alteryx, Apache Airflow
- **Databases:** MongoDB Atlas, Snowflake, GCP Cloud Storage
- **Data Modeling:** Star Schema, Fact Constellation, Kimball methodology
- **Text Mining & Sentiment Analysis:** Orange, VADER, topic modeling
- **Visualization:** Python (Matplotlib, Seaborn)
- **Project Management:** Agile, Scrum, Zoom, Kanban (Trello)

## 🗃️ Data Sources

We used three major JSON files from the [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/):
- `business.json`
- `review.json` (sampled 500K)
- `checkin.json`

## 🧹 ETL Pipeline

1. **Data Cleaning & Transformation**
   - Created industry dataset
   - Mapped businesses to 22 unique industries
   - Combined each unique review to the business dataset. (Mapping of review to the industry)
   - Transformed check-in data to numeric metrics

2. **Sentiment Scoring**
   - Preprocessed text using Orange and VADER and analyzed patterns
   - Labeled reviews as Positive, Negative, or Neutral
   - Used 'Text Blob' library in Apache Airflow to add sentiment score in the review dataset

3. **ETL Automation**
   - Loaded transformed data into MongoDB (Apache Airflow)
   - Exported to GCP (manually due to tier limitations)
   - Loaded into Snowflake from GCP staging
   - Snowflake to Python (Matplotlibrary) for visualization

## 📐 Dimensional Modeling

Designed a **fact constellation schema** in Snowflake:

- 📁 **Fact Tables:** `fact_reviews`, `fact_industry`
- 🧱 **Dimension Tables:** `dim_date`, `dim_business`, `dim_industry`, `dim_sentiment`

![ER Diagram](ER%20diagram.png)

## 📈 Key Insights

- **Restaurants & Food** captured ~75% of all reviews.
- **Check-ins** peaked mid-week, not weekends.
- **Sentiment:** Most industries had high positive sentiment (>80%).
- **COVID Impact:** 2020–22 showed drastic shifts in ratings and volume.

## 💡 Recommendations

- Customer-facing industries should automate review analysis pipelines.
- Negative feedback should be leveraged for service optimization.
- Dimensional models should be customized for sentiment-enriched metrics.

## 🔍 Notebooks & Scripts


## 🔍 Notebooks & Scripts

```
/data/                  -> Raw + transformed datasets (linked via Google Drive)
/notebooks/            -> Jupyter notebooks for EDA and transformation
/scripts/              -> Airflow DAGs and utility scripts
/Snowflake_queries/    -> SQL scripts and schemas
/docs/                 -> ER diagrams, architecture, documentation
```

## 🚀 Future Work

- Extend analysis to user behavior trends
- Implement schema-automation using NoSE
- Fully automate MongoDB → GCP pipeline

---

> **Note**: All data files are too large for direct upload. Shared via [Google Drive](https://drive.google.com/drive/folders/1AGB1XQ3UW9r0diXq9BvX3L_DBLu0a3iG)

