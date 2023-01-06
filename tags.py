from typing import List

from aiohttp import ClientTimeout
from aiohttp import request as async_request
from kh_common.caching import ArgsCache
from kh_common.config.constants import tags_host
from kh_common.hashing import Hashable
from kh_common.utilities import flatten


class Tags(Hashable) :

	Timeout: int = 30

	@ArgsCache(5)
	async def postTags(self, post_id: str) -> List[str] :
		async with async_request(
			'GET',
			f'{tags_host}/v1/fetch_tags/{post_id}',
			timeout=ClientTimeout(Tags.Timeout),
		) as response :
			data = await response.json()

			if not data :
				return []

			return list(flatten(data))
