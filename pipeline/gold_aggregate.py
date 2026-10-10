from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SILVER_PATH = ROOT / 'data' / 'silver' / 'news_silver.csv'
GOLD_DIR = ROOT / 'data' / 'gold'


def build_gold() -> Path:
    df = pd.read_csv(SILVER_PATH)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    topic_summary = (
        df.groupby(['topic', 'source', 'sentiment_bias'], dropna=False)
        .size()
        .reset_index(name='article_count')
    )
    topic_summary_path = GOLD_DIR / 'gold_topic_sentiment.csv'
    topic_summary.to_csv(topic_summary_path, index=False)

    daily_summary = (
        df.groupby(['date', 'source'], dropna=False)
        .size()
        .reset_index(name='article_count')
    )
    daily_summary_path = GOLD_DIR / 'gold_daily_volume.csv'
    daily_summary.to_csv(daily_summary_path, index=False)

    return GOLD_DIR


if __name__ == '__main__':
    output = build_gold()
    print(f'Gold layer created: {output}')
