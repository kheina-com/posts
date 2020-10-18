from kh_common.exceptions.http_error import BadRequest, Forbidden, HttpErrorHandler, NotFound
from kh_common.scoring import confidence, controversial as calc_cont, hot as calc_hot
from typing import Any, Dict, List, Tuple, Union
from kh_common.utilities.json import json_stream
from kh_common.blocking import UserBlocking
from kh_common.caching import ArgsCache
from kh_common.logging import getLogger
from models import PostSort
from uuid import uuid4


class Posts(UserBlocking) :

	user_post_keys = (
		'post_id',
		'title',
		'description',
		'privacy',
	)


	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	def _validateVote(self, vote: Union[bool, type(None)]) :
		if not isinstance(vote, (bool, type(None))) :
			raise BadRequest('the given vote is invalid (vote value must be integer. 1 = up, -1 = down, 0 or null to remove vote)')


	def _validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest('the given page number is invalid.', logdata={ 'page_number': page_number })


	def _validateCount(self, count: int) :
		if count < 1 :
			raise BadRequest('the given count is invalid.', logdata={ 'count': count })


	@HttpErrorHandler('processing vote')
	def vote(self, user_id: int, post_id: str, upvote: Union[bool, type(None)]) :
		self._validatePostId(post_id)
		self._validateVote(upvote)

		with self.transaction() as transaction :
			data = transaction.query("""
				INSERT INTO kheina.public.post_votes
				(user_id, post_id, upvote)
				VALUES
				(%s, %s, %s)
				ON CONFLICT ON CONSTRAINT post_votes_pkey DO 
					ON CONFLICT ON CONSTRAINT post_votes_pkey DO 
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
					ON CONFLICT ON CONSTRAINT post_scores_pkey DO 
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


	@ArgsCache(60)
	def _fetch_posts(self, sort: PostSort, tags: Tuple[str], count: int, page: int) :
		if tags :
			data = self.query(f"""
				SELECT posts.post_id, posts.title, posts.description, users.handle, users.display_name, array_agg(t2.tag)
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
					INNER JOIN kheina.public.tag_post AS tp2
						ON tp2.post_id = posts.post_id
					INNER JOIN kheina.public.tags AS t2
						ON t2.tag_id = tp2.tag_id
							AND tags.deprecated = false
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
				SELECT posts.post_id, posts.title, posts.description, users.handle, users.display_name, array_agg(tags.tag)
				FROM kheina.public.posts
					INNER JOIN kheina.public.post_scores
						ON post_scores.post_id = posts.post_id
					INNER JOIN kheina.public.users
						ON users.user_id = posts.uploader
					LEFT JOIN (
						kheina.public.tag_post
							INNER JOIN kheina.public.tags
								ON tags.tag_id = tag_post.tag_id
									AND tags.deprecated = false
						) ON tag_post.post_id = posts.post_id
				WHERE posts.privacy_id = privacy_to_id('public')
				GROUP BY posts.post_id
				ORDER BY post_scores.{sort.name} DESC NULLS LAST
				LIMIT %s
				OFFSET %s;
				""",
				(count, count * (page - 1)),
				fetch_all=True,
			)

		return [
			{
				'post_id': row[0],
				'title': row[1],
				'description': row[2],
				'user': {
					'handle': row[3],
					'name': row[4],
				},
				'tags': set(row[5]),
			}
			for row in data
		]


	@HttpErrorHandler('fetching posts')
	def fetchPosts(self, user_id: int, sort: PostSort, tags: List[str], count:int=64, page:int=1) :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts = self._fetch_posts(sort, tuple(tags), count, page)
		blocked_tags = self.userBlockedTags(user_id)

		return {
			'posts': [
				post for post in posts
				if not post.pop('tags') & blocked_tags
			],
		}


	@ArgsCache(60)
	def _get_post(self, post_id: str) :

		data = self.query("""
			SELECT posts.title, posts.description, posts.filename, users.handle, users.display_name, posts.created_on, posts.updated_on, tag_classes.class, array_agg(tags.tag)
			FROM kheina.public.posts
				INNER JOIN kheina.public.users
					ON posts.uploader = users.user_id
				LEFT JOIN (
					kheina.public.tag_post
						INNER JOIN kheina.public.tags
							ON tags.tag_id = tag_post.tag_id
								AND tags.deprecated = false
						INNER JOIN kheina.public.tag_classes
							ON tag_classes.class_id = tags.class_id
					) ON tag_post.post_id = posts.post_id
			WHERE posts.post_id = %s
				AND (
					posts.privacy_id = privacy_to_id('public')
					OR posts.privacy_id = privacy_to_id('unlisted')
				)
			GROUP BY posts.post_id, users.user_id, tag_classes.class_id;
			""",
			(post_id,),
			fetch_all=True,
		)

		if not data :
			raise NotFound('no data was found for the provided post id.')

		return {
			'title': data[0][0],
			'description': data[0][1],
			'filename': data[0][2],
			'created': data[0][5].timestamp(),
			'updated': data[0][6].timestamp(),
			'user': {
				'handle': data[0][3],
				'name': data[0][4],
			},
			'tags': {
				row[7]: row[8]
				for row in data
			},
			'tags_flattened': set(row[8] for row in data),
		}


	@HttpErrorHandler('retrieving post')
	def getPost(self, user_id: int, post_id: str) :
		self._validatePostId(post_id)

		post = self._get_post(post_id)
		blocked_tags = self.userBlockedTags(user_id)

		if post.pop('tags_flattened') & blocked_tags :
			raise NotFound('no data was found for the provided post id.')

		return {
			post_id: post,
		}


	@HttpErrorHandler('retrieving user posts')
	@ArgsCache(60)
	def fetchUserPosts(self, user_id: int, sort: PostSort, count: int, page: int) :
		data = self.query(f"""
			SELECT posts.post_id, posts.title, posts.description, privacy.type
			FROM kheina.public.posts
				INNER JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
				INNER JOIN kheina.public.privacy
					ON privacy.privacy_id = posts.privacy_id
			WHERE posts.uploader = %s
			ORDER BY post_scores.{sort.name} DESC NULLS LAST
			LIMIT %s
			OFFSET %s;
			""",
			(user_id, count, count * (page - 1)),
			fetch_all=True,
		)

		return [
			dict(zip(Posts.user_post_keys, row))
			for row in data
		]
