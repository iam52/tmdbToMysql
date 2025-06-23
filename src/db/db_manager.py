import os

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(
            f"mysql+pymysql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}"
            f"@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}?charset=utf8mb4"
        )

    def save_data(self, df):
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                self._save_movies(conn, df)
                self._save_genres(conn, df)
                self._save_cast(conn, df)
                self._save_keywords(conn, df)
                self._save_similar_movies(conn, df)
                trans.commit()
            except SQLAlchemyError as e:
                trans.rollback()
                raise RuntimeError(f"데이터베이스 저장 실패: {e}")

    def _save_movies(self, conn, df):
        df['release_date'] = df['release_date'].replace('', np.nan)
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        (df[['id', 'title', 'original_title', 'release_date', 'runtime', 'vote_average', 'vote_count',
             'popularity', 'poster_path']].to_sql('movies', conn, if_exists='append', index=False))
