import os
import json
import asyncio
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

async def collect_all_movies(collector, start_year=2020, end_year=2025):
    all_movie_ids = set()
    movie_basic_info = {}
    SAVE_DIR = "data"

    progress_file = f"{SAVE_DIR}/collection_progress.json"
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
            start_year = progress['current_year']
            all_movie_ids = set(progress['collected_ids'])
            print(f"이어서 수집 시작: {start_year}년부터")

    for year in range(start_year, end_year + 1):
        print(f"\n{year}년 영화 수집 중")
        page = 1
        while True:
            response = await collector.discover_movies(year, page)
            if not response or not response.get('results'):
                break

            for movie in response['results']:
                all_movie_ids.add(movie['id'])
                movie_basic_info[movie['id']] = {
                    'title': movie['title'],
                    'release_date': movie['release_date'],
                    'popularity': movie['popularity']
                }

            with open(progress_file, 'w') as f:
                json.dump({
                    'current_year': year,
                    'collected_ids': list(all_movie_ids)
                }, f)

            if page >= response['totla_pages']:
                break
            page += 1

        print(f"{year}년 완료: {len(all_movie_ids)}개 영화 ID 수집")

        if year % 5 == 0:
            save_path = f"{SAVE_DIR}/movie_ids_{year}.json"
            with open(save_path, 'w') as f:
                json.dump(list(all_movie_ids), f)

    return all_movie_ids, movie_basic_info

async def collect_movie_details(collector, movie_ids, concurrency=40):
    detailed_movies = []
    SAVE_DIR = "data"
    progress_file = f'{SAVE_DIR}/details_progress.json'
    semaphore = asyncio.Semaphore(concurrency)

    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
            collected_ids = set(progress['collected_ids'])
            movie_ids = [id for id in movie_ids if id not in collected_ids]
            detailed_movies = progress['collected_movies']
            print(f"이어서 수집: {len(movie_ids)}개 남음")

    async def process_movie(movie_id):
        async with semaphore:
            details = await collector.get_movie_details(movie_id)
            if details:
                return {
                    'id': details['id'],
                    'title': details['title'],
                    'original_title': details['original_title'],
                    'genres': [g['name'] for g in details['genres']],
                    'keywords': [k['name'] for k in details.get('keywords', {}).get('keywords', [])],
                    'cast': [{'id': c['id'], 'name': c['name']} for c in details['credits']['cast'][:10]],
                    'crew': [{'id': c['id'], 'name': c['name'], 'job': c['job']}
                             for c in details['credits']['crew'] if c['job'] in ['Director', 'Writer']],
                    'release_date': details['release_date'],
                    'runtime': details['runtime'],
                    'vote_average': details['vote_average'],
                    'vote_count': details['vote_count'],
                    'popularity': details['popularity'],
                    'poster_path': details['poster_path'],
                    'similar_movies': [m['id'] for m in details.get('similar', {}).get('results', [])]
                }
            return None

    results = await tqdm_asyncio.gather(*[process_movie(mid) for mid in movie_ids], desc="상세 정보 수집")
    detailed_movies.extend([r for r in results if r])

    with open(progress_file, 'w') as f:
        json.dump({
            'collected_ids': [m['id'] for m in detailed_movies],
            'collected_movies': detailed_movies
        }, f)

    df = pd.DataFrame(detailed_movies)
    df.to_csv(f'{SAVE_DIR}/movies_detailed_final.csv', index=False)
    return detailed_movies
