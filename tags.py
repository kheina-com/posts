from aiohttp import ClientTimeout, request as async_request
from kh_common.config.constants import tags_host
from kh_common.utilities import flatten
from kh_common.caching import ArgsCache
from kh_common.hashing import Hashable


class Tags(Hashable) :

	Timeout: int = 30

	@ArgsCache(10)
	async def postTags(self, post_id: str) :
		async with async_request(
			'GET',
			f'{tags_host}/v1/fetch_tags/{post_id}',
			timeout=ClientTimeout(Tags.Timeout),
		) as response :
			data = await response.json()

			if not data :
				return set()

			return set(flatten(data))
