from kh_common.exceptions.http_error import BadRequest, Forbidden, NotFound, InternalServerError
from kh_common.caching import ArgsCache, SimpleCache
from typing import Any, Dict, List, Tuple, Union
from kh_common.config.calculated import z_score
from kh_common.config.constants import epoch
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from kh_common.hashing import Hashable
from math import log10, sqrt
from uuid import uuid4


"""
resources:
	https://github.com/reddit-archive/reddit/blob/master/r2/r2/lib/db/_sorts.pyx
	https://steamdb.info/blog/steamdb-rating
	https://www.evanmiller.org/how-not-to-sort-by-average-rating.html
	https://www.reddit.com/r/TheoryOfReddit/comments/bpmd3x/how_does_hot_vs_best_vscontroversial_vs_rising/envijlj
"""


class Posts(SqlInterface, Hashable) :

	def __init__(self, z:float=0.8) :
		Hashable.__init__(self)
		SqlInterface.__init__(self)
		self.z = z_score[z]


	def _sign(x: Union[int, float]) -> int :
		return (x > 0) - (x < 0)


	def _hot(self, up: int, down: int, time: float) -> float :
		s: int = up - down
		return Posts._sign(s) * log10(max(abs(s), 1)) + (time - epoch) / 45000


	def _controversial(self, up: int, down: int) -> float :
		return (up + down)**(min(up, down)/max(up, down))


	def _best(self, up: int, total: int) -> float :
		if not total :
			return 0
		s: float = up / total
		return s - (s - 0.5) * 2**(-log10(total + 1))


	def _confidence(self, up: int, total: int) -> float :
		if not total :
			return 0
		phat = up / total
		return (
			(phat + self.z * self.z / (2 * total)
			- self.z * sqrt((phat * (1 - phat)
			+ self.z * self.z / (4 * total)) / total)) / (1 + self.z * self.z / total)
		)


	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	def _validateVote(self, vote: bool) :
		if not isinstance(vote, bool) :
			raise BadRequest('the given vote is invalid (vote value must be boolean. true = up, false = down)')


	def _validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest('the given page number is invalid.', logdata={ 'page_number': page_number })


	def _validateCount(self, count: int) :
		if count < 1 :
			raise BadRequest('the given count is invalid.', logdata={ 'count': count })


	def vote(self, user_id: int, post_id: str, upvote: bool) :
		self._validatePostId(post_id)
		self._validateVote(upvote)

		try :
			with self.transaction() as transaction :
				data = transaction.query("""
					INSERT INTO kheina.public.post_votes
					(user_id, post_id, upvote)
					VALUES
					(%s, %s, %s);

					SELECT COUNT(1), SUM(post_votes.upvote::int), posts.created_on
					FROM kheina.public.post_votes
						INNER JOIN kheina.public.posts
							ON posts.post_id = post_votes.post_id
					WHERE post_votes.post_id = %s
					GROUP BY posts.post_id;
					""",
					(
						user_id, post_id, upvote,
						post_id,
					),
					fetch_one=True,
				)

				transaction.commit()

				up: int = data[1]
				total: int = data[0]
				down: int = total - up
				created: float = data[2].timestamp()

				top: int = up - down
				hot: float = self._hot(up, down, created)
				best: float = self._confidence(up, total)
				controversial: float = self._controversial(up, down)

				data = transaction.query("""
					INSERT INTO kheina.public.post_scores
					(post_id, upvotes, downvotes, top, hot, best, controversial)
					VALUES
					(%s, %s, %s, %s, %s, %s, %s)
					ON CONFLICT ON CONSTRAINT post_scores_pkey DO 
						UPDATE kheina.public.post_scores
						SET upvotes = %s,
							downvotes = %s,
							top = %s,
							hot = %s,
							best = %s,
							controversial = %s
						WHERE post_id = %s;
					""",
					(
						post_id, up, down, top, hot, best, controversial,
						up, down, top, hot, best, controversial, post_id,
					),
					fetch_all=True,
				)

				transaction.commit()

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'post_id': post_id,
				'user_id': user_id,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while processing upvote.', logdata=logdata)


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
