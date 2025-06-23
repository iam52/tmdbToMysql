import asyncio
import os

import pandas as pd
from dotenv import load_dotenv

from src.movie_collector import AsyncTMDBCollector
from src.movie_collector_pipeline import collect_all_movies, collect_movie_details

load_dotenv()
API_KEY = os.getenv("api_key")

async def main():
    async with AsyncTMDBCollector(API_KEY) as collector:
        movie_ids, basic_info = await collect_all_movies(collector)

        detailed_movies = await collect_movie_details(collector, list(movie_ids))

        df = pd.DataFrame(detailed_movies)

        db_manager = DatabaseManager()
        db_manager.save_dadta(df)

        print("\n수집 완료 통계:")
        print(f"총 영화 수: {len(df)}")
        print("\n장르별 영화 수:")
        genre_counts = pd.Series([genre for genres in df['genres'] for genre in genres]).value_counts()
        print(genre_counts)

        print("\n데이터 검증:")
        print(df.info())
        print("\n결측치 확인:")
        print(df.isnull().sum())

if __name__ == "__main__":
    asyncio.run(main())