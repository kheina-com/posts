from kh_common.exceptions.http_error import BadRequest, Forbidden, NotFound, InternalServerError
from kh_common.caching import ArgsCache, SimpleCache
from typing import Optional, Dict, List, Tuple
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from uuid import uuid4


class Posts(SqlInterface) :

	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	def _validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest('the given page number is invalid.', logdata={ 'page_number': page_number })


	def _validateCount(self, count: int) :
		if count < 1 :
			raise BadRequest('the given count is invalid.', logdata={ 'count': count })


	def fetchPosts(self, user_id: int, tags: Tuple[str], count:int=64, page:int=1) :
		self._validatePageNumber(page)
		self._validateCount(count)

		try :
			data = self.query("""
				SELECT kheina.public.fetch_posts_by_tag(%s, %s, %s, %s);
				""",
				(tags, user_id, count, count * (page - 1)),
				fetch_all=True,
			)

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'page': page,
				'user_id': user_id,
				'tags': tags,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while fetching posts.', logdata=logdata)

		return {
			'posts': [i[0] for i in data],
		}
