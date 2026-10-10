# Linguistic Analysis of News Articles

## Phase 2: PySpark Lakehouse

The project now contains a production-oriented Bronze and Silver pipeline for
Databricks or any Spark environment with Delta Lake enabled. It uses explicit
`StructType` schemas when reading CSV files—Spark schema inference is never
used—and stores the Lakehouse layers as Delta tables.

### Pipeline capabilities

- Idempotent Delta `MERGE INTO` upserts for both Bronze and Silver tables.
- Full, incremental, and historical backfill modes controlled by parameters.
- Per-record `load_timestamp` fields in Bronze, Silver, quarantine, and audit
  tables.
- Invalid dates, timestamps, and mandatory fields are written to a quarantine
  Delta table while valid records continue processing.
- Added source headers are detected, recorded in `source_extra_columns`, and
  quarantined for review without stopping the batch.
- `pipeline_execution_logs` records the layer, input parameter, start/end
  times, outcome, and inserted/updated row counts for every execution.

### Run in Databricks Free Edition

Databricks Community Edition was retired in 2025; use the current free,
serverless Databricks Free Edition. Upload the raw CSV files to a Unity Catalog
volume, clone this repository as a Databricks Git folder, then run
`Notebooks/phase2_databricks_run.py`. Set its three parameters to your volume
paths. For example:

```bash
/Volumes/<catalog>/<schema>/<volume>/raw/incremental/*.csv
/Volumes/<catalog>/<schema>/<volume>/lakehouse
```

For a historical backfill, change the input path and load type; no source path
is hard-coded in the pipeline:

Set `raw_input_path` to `/Volumes/<catalog>/<schema>/<volume>/raw/2024-10/*.csv`
and `load_type` to `backfill`.

Repeat either command safely. Identical records are no-ops and changed versions
of an existing article update the Silver record instead of creating duplicates.
See [the Phase 2 data dictionary](docs/phase2_data_dictionary.md) for every
Bronze, Silver, quarantine, and audit field.

This project aims to analyze linguistic patterns, sentiment biases, and keyword trends in news articles from mainstream international news sources on selected global topics. It combines web scraping, data analysis, machine learning, and visualization to uncover insights about media coverage and biases.

---

## Project Overview

This project investigates sentiment bias and keyword usage across news articles from five prominent sources (BBC, CNN, Al Jazeera, Fox, TRT) on global topics such as:
- Israel War
- Ukraine War
- Islamophobia
- US Elections
- China News

The analysis spans over 5,000 scraped articles, assessing sentiment biases and extracting key insights using advanced machine learning models and visualization techniques.

---

## Dataset

### Sources
- **News Outlets:** BBC, CNN, Al Jazeera, Fox, TRT  
- **Topics:** Israel War, Ukraine War, Islamophobia, US Elections, China News  

### Features
The dataset contains the following fields:
- `Source`: The news outlet publishing the article (e.g., BBC, CNN).  
- `Link`: The URL to the original article.  
- `Headline`: The headline of the article.  
- `Description`: A brief summary of the article.  
- `Timestamp`: Publication date and time of the article.  
- `Region`: Geographic region related to the news.  
- `Author`: The author(s) of the article.  
- `Article_Content`: Full text content of the article.  
- `Sentiment_Bias`: Sentiment classification of the article, derived using fine-tuned transformer models.  
- `Keywords`: Key terms extracted using SHAP for interpretability.

---

## Methodology

1. **Web Scraping**  
   Articles were scraped from the selected sources using **Selenium** and **BeautifulSoup**.  

2. **Data Cleaning**  
   The scraped data was cleaned and formatted to ensure consistency and accuracy.  

3. **Sentiment Bias Classification**  
   - Fine-tuned transformer models (based on Twitter data) were used for sentiment analysis.  
   - Sentiments were categorized as Positive, Neutral, or Negative.  

4. **Keyword Extraction**  
   - Keywords were extracted using **SHAP** (SHapley Additive exPlanations) to understand important terms contributing to sentiment predictions.  

5. **Visualization**  
   - Various visualizations were created to analyze and present trends and biases effectively.

---

## Visualizations

The following visualizations were used to explore and present the data:
- **Bar Charts**: Displaying sentiment bias distribution by source, topic, and region.  
- **Pie Charts**: Representing proportions of sentiment categories (Positive, Neutral, Negative).  
- **Line Charts**: Analyzing temporal trends in sentiment across topics.  
- **Map Charts**: Visualizing regional coverage of news articles.  
- **Word Clouds**: Highlighting frequent keywords for each topic and source.  
- **Bivariate Analysis**: Exploring correlations between different dataset features.

---

## Dependencies

The project requires the following libraries and tools:
- **Web Scraping**: `Selenium`, `BeautifulSoup4`  
- **Data Manipulation**: `pandas`, `numpy`  
- **Machine Learning**: `transformers`, `scikit-learn`, `SHAP`  
- **Visualization**: `matplotlib`, `seaborn`, `wordcloud`, `plotly`  
- **Other**: `nltk`


