from kh_common.exceptions.http_error import BadRequest, Forbidden, NotFound, InternalServerError
from kh_common.scoring import confidence, controversial as calc_cont, hot as calc_hot
from kh_common.caching import ArgsCache, SimpleCache
from typing import Any, Dict, List, Tuple, Union
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from kh_common.hashing import Hashable
from models import PostSort
from uuid import uuid4


class Posts(SqlInterface, Hashable) :

	single_post_keys = (
		'title',
		'description',
		'filename',
		'handle',
		'display_name',
		'created',
		'updated',
		'tags',
	)

	multiple_post_keys = (
		'post_id',
		'title',
		'description',
		'handle',
		'display_name',
	)

	def __init__(self) :
		Hashable.__init__(self)
		SqlInterface.__init__(self)


	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	def _validateVote(self, vote: Union[bool, type(None)]) :
		if not isinstance(vote, (bool, type(None))) :
			raise BadRequest('the given vote is invalid (vote value must be boolean. true = up, false = down)')


	def _validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest('the given page number is invalid.', logdata={ 'page_number': page_number })


	def _validateCount(self, count: int) :
		if count < 1 :
			raise BadRequest('the given count is invalid.', logdata={ 'count': count })


	def vote(self, user_id: int, post_id: str, upvote: Union[bool, type(None)]) :
		self._validatePostId(post_id)
		self._validateVote(upvote)

		try :
			with self.transaction() as transaction :
				data = transaction.query("""
					INSERT INTO kheina.public.post_votes
					(user_id, post_id, upvote)
					VALUES
					(%s, %s, %s)
					ON CONFLICT ON CONSTRAINT post_votes_pkey DO 
						UPDATE SET
							upvote = %s
						WHERE post_votes.user_id = %s
							AND post_votes.post_id = %s;

					SELECT COUNT(post_votes.upvote), SUM(post_votes.upvote::int), posts.created_on
					FROM kheina.public.posts
						LEFT JOIN kheina.public.post_votes
							ON post_votes.post_id = posts.post_id
								AND post_votes.upvote IS NOT NULL
					WHERE posts.post_id = %s
					GROUP BY posts.post_id;
					""",
					(
						user_id, post_id, upvote,
						upvote, user_id, post_id,
						post_id,
					),
					fetch_one=True,
				)

				up: int = data[1] or 0
				total: int = data[0] or 0
				down: int = total - up
				created: float = data[2].timestamp()

				top: int = up - down
				hot: float = calc_hot(up, down, created)
				best: float = confidence(up, total)
				controversial: float = calc_cont(up, down)

				transaction.query("""
					INSERT INTO kheina.public.post_scores
					(post_id, upvotes, downvotes, top, hot, best, controversial)
					VALUES
					(%s, %s, %s, %s, %s, %s, %s)
					ON CONFLICT ON CONSTRAINT post_scores_pkey DO 
						UPDATE SET
							upvotes = %s,
							downvotes = %s,
							top = %s,
							hot = %s,
							best = %s,
							controversial = %s
						WHERE post_scores.post_id = %s;
					""",
					(
						post_id, up, down, top, hot, best, controversial,
						up, down, top, hot, best, controversial, post_id,
					),
				)

				transaction.commit()

			return {
				post_id: {
					'up': up,
					'down': down,
					'total': total,
					'top': top,
					'hot': hot,
					'best': best,
					'controversial': controversial,
				}
			}

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'post_id': post_id,
				'user_id': user_id,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while processing vote.', logdata=logdata)


	def fetchPosts(self, user_id: int, sort: PostSort, tags: Tuple[str], count:int=64, page:int=1) :
		self._validatePageNumber(page)
		self._validateCount(count)

		try :
			if tags :
				data = self.query(f"""
					SELECT posts.post_id, posts.title, posts.description, users.handle, users.display_name
					FROM kheina.public.tags
						INNER JOIN kheina.public.tag_post
							ON tag_post.tag_id = tags.tag_id
						INNER JOIN kheina.public.posts
							ON posts.post_id = tag_post.post_id
								AND posts.privacy_id = privacy_to_id('public')
						INNER JOIN kheina.public.post_scores
							ON post_scores.post_id = tag_post.post_id
						INNER JOIN kheina.public.users
							ON posts.uploader = users.user_id
					WHERE tags.tag = any(%s)
						AND tags.deprecated = false
					GROUP BY posts.post_id, post_scores.{sort.name}, users.user_id
					HAVING count(1) >= %s
					ORDER BY post_scores.{sort.name} DESC NULLS LAST
					LIMIT %s
					OFFSET %s;
					""",
					(tags, len(tags), count, count * (page - 1)),
					fetch_all=True,
				)

			else :
				data = self.query(f"""
					SELECT posts.post_id, posts.title, posts.description, users.handle, users.display_name
					FROM kheina.public.posts
						INNER JOIN kheina.public.post_scores
							ON post_scores.post_id = posts.post_id
						INNER JOIN kheina.public.users
							ON users.user_id = posts.uploader
					WHERE posts.privacy_id = privacy_to_id('public')
					ORDER BY post_scores.{sort.name} DESC NULLS LAST
					LIMIT %s
					OFFSET %s;
					""",
					(count, count * (page - 1)),
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
			'posts': [
				dict(zip(Posts.multiple_post_keys, i))
				for i in data
			],
		}


	def getPost(self, user_id: int, post_id: str) :
		self._validatePostId(post_id)

		try :
			data = self.query("""
				SELECT posts.title, posts.description, posts.filename, users.handle, users.display_name, posts.created_on, posts.updated_on, array_agg(tags.tag)
				FROM kheina.public.posts
					INNER JOIN kheina.public.users
						ON posts.uploader = users.user_id
					INNER JOIN kheina.public.users
						ON posts.uploader = users.user_id
					LEFT JOIN kheina.public.tag_post
						ON tag_post.post_id = posts.post_id
					LEFT JOIN kheina.public.tags
						ON tags.tag_id = tag_post.tag_id
				WHERE post_id = %s
					AND (
						posts.privacy_id = privacy_to_id('public')
						OR posts.privacy_id = privacy_to_id('unlisted')
					)
				GROUP BY posts.post_id, users.user_id
				LIMIT 1;
				""",
				(post_id,),
				fetch_one=True,
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
			raise InternalServerError('an error occurred while fetch post.', logdata=logdata)

		return {
			post_id: dict(zip(Posts.single_post_keys, data)),
		}
