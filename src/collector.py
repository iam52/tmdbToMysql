import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv()

class AsyncTMDBCollector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def _make_request(self, url, params, max_retries=3):
        for attempt in range(max_retries):
            try:
                async with self.session.get(url, params=params) as response:
                    response.raise_for_status()

                    return await response.json()
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Error after {max_retries} attempts: {e}")

                    return None
                await asyncio.sleep(1)

        return None

    async def discover_movies(self, year, page=1):
        url = f"{self.base_url}/discover/movie"
        params = {
            'api_key': self.api_key,
            'language': 'ko-KR',
            'sort_by': 'popularity.desc',
            'page': page,
            'primary_release_year': year,
            'include-adult': 'false'
        }

        return await self._make_request(url, params)

    async def get_movie_details(self, movie_id):
        url = f"{self.base_url}/movie/{movie_id}"
        params = {
            'api_key': self.api_key,
            'language': 'ko-KR',
            'append_to_response': 'credits,keywords,similar'
        }

        return await self._make_request(url, params)