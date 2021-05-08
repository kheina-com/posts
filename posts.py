from kh_common.exceptions.http_error import BadRequest, Forbidden, HttpErrorHandler, NotFound
from kh_common.scoring import confidence, controversial as calc_cont, hot as calc_hot
from typing import Any, Dict, List, Tuple, Union
from kh_common.blocking import UserBlocking
from kh_common.caching import ArgsCache
from collections import defaultdict
from kh_common.auth import KhUser
from asyncio import ensure_future
from models import PostSort
from tags import Tags


tagService = Tags()


class Posts(UserBlocking) :

	user_post_keys = (
		'post_id',
		'title',
		'description',
		'privacy',
	)


	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', post_id=post_id)


	def _validateVote(self, vote: Union[bool, type(None)]) :
		if not isinstance(vote, (bool, type(None))) :
			raise BadRequest('the given vote is invalid (vote value must be integer. 1 = up, -1 = down, 0 or null to remove vote)')


	def _validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest('the given page number is invalid.', page_number=page_number)


	def _validateCount(self, count: int) :
		if count < 1 :
			raise BadRequest('the given count is invalid.', count=count)


	@HttpErrorHandler('processing vote')
	def vote(self, user: KhUser, post_id: str, upvote: Union[bool, type(None)]) :
		self._validatePostId(post_id)
		self._validateVote(upvote)

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
					user.user_id, post_id, upvote,
					upvote, user.user_id, post_id,
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


	@ArgsCache(60)
	async def _fetch_posts(self, sort: PostSort, tags: Tuple[str], count: int, page: int, logged_in: bool) :
		offset: int = count * (page - 1)
		if tags :
			data = self.query(f"""
				SELECT
					posts.post_id,
					posts.title,
					posts.description,
					users.handle,
					users.display_name,
					post_scores.upvotes,
					post_scores.downvotes,
					users.icon,
					posts.rating,
					posts.parent,
					posts.created_on,
					posts.updated_on,
					posts.filename
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
				GROUP BY posts.post_id, post_scores.post_id, users.user_id
				HAVING count(1) >= %s
				ORDER BY post_scores.{sort.name} DESC NULLS LAST
				LIMIT %s
				OFFSET %s;
				""",
				(tags, len(tags), count, offset),
				fetch_all=True,
			)

		else :
			data = self.query(f"""
				SELECT
					posts.post_id,
					posts.title,
					posts.description,
					users.handle,
					users.display_name,
					post_scores.upvotes,
					post_scores.downvotes,
					users.icon,
					posts.rating,
					posts.parent,
					posts.created_on,
					posts.updated_on,
					posts.filename
				FROM kheina.public.posts
					INNER JOIN kheina.public.post_scores
						ON post_scores.post_id = posts.post_id
					INNER JOIN kheina.public.users
						ON users.user_id = posts.uploader
				WHERE posts.privacy_id = privacy_to_id('public')
				{'' if logged_in else "AND posts.rating = rating_to_id('general')"}
				GROUP BY posts.post_id, post_scores.post_id, users.user_id
				ORDER BY post_scores.{sort.name} DESC NULLS LAST
				LIMIT %s
				OFFSET %s;
				""",
				(count, offset),
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
					'icon': row[7],
				},
				'tags': await tagService.postTags(row[0]),
				'score': {
					'up': row[5],
					'down': row[6],
				},
				'rating': self._get_rating_map()[row[8]],
				'parent': row[9],
				'created': str(row[10]),
				'updated': str(row[11]),
				'media': bool(row[12]),
			}
			for row in data
		]


	@HttpErrorHandler('fetching posts')
	async def fetchPosts(self, user: KhUser, sort: PostSort, tags: Union[List[str], None], count:int=64, page:int=1) :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts = ensure_future(self._fetch_posts(sort, tuple(sorted(map(str.lower, filter(None, map(str.strip, filter(None, tags)))))) if tags else None, count, page, user.authenticated(raise_error=False)))
		blocked_tags = self.user_blocked_tags(user.user_id)

		return {
			'posts': [
				{
					**post,
					'blocked': bool(post['tags'] & blocked_tags),
					'tags': list(post['tags']),
				}
				for post in await posts
				if not post['tags'] & blocked_tags
			],
		}


	@ArgsCache(600)
	def _get_rating_map(self) :
		data = self.query("""
			SELECT rating_id, rating
			FROM kheina.public.ratings;
			""",
			fetch_all=True,
		)
		return dict(data)


	@ArgsCache(600)
	def _get_privacy_map(self) :
		data = self.query("""
			SELECT privacy_id, type
			FROM kheina.public.privacy;
			""",
			fetch_all=True,
		)
		return dict(data)


	@ArgsCache(600)
	def _get_media_type_map(self) :
		data = self.query("""
			SELECT media_type_id, file_type, mime_type
			FROM kheina.public.media_type;
			""",
			fetch_all=True,
		)
		return defaultdict(lambda : None, {
			row[0]: { 'file_type': row[1], 'mime_type': row[2] }
			for row in data
		})


	@ArgsCache(60)
	def _get_post(self, post_id: str) :
		data = self.query("""
			SELECT
				posts.title,
				posts.description,
				posts.filename,
				users.handle,
				users.display_name,
				posts.created_on,
				posts.updated_on,
				posts.privacy_id,
				posts.media_type_id,
				users.user_id,
				post_scores.upvotes,
				post_scores.downvotes,
				users.icon,
				posts.rating,
				posts.parent
			FROM kheina.public.posts
				INNER JOIN kheina.public.users
					ON posts.uploader = users.user_id
				LEFT JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
			WHERE posts.post_id = %s
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
			'created': str(data[0][5]),
			'updated': str(data[0][6]),
			'user': {
				'handle': data[0][3],
				'name': data[0][4],
				'icon': data[0][12],
			},
			'privacy': self._get_privacy_map()[data[0][7]],
			'media_type': self._get_media_type_map()[data[0][8]],
			'user_id': data[0][9],
			'score': {
				'up': data[0][10],
				'down': data[0][11],
			},
			'rating': self._get_rating_map()[data[0][13]],
			'parent': data[0][14],
		}


	@HttpErrorHandler('retrieving post')
	def getPost(self, user: KhUser, post_id: str) :
		self._validatePostId(post_id)

		post = self._get_post(post_id)
		uploader = post.pop('user_id')

		if post['privacy'] == 'unpublished' :
			post['created'] = post['updated'] = None

		user_is_uploader = uploader == user.user_id and user.authenticated(raise_error=False)

		if post['privacy'] in { 'public', 'unlisted' } or user_is_uploader :
			return post

		raise NotFound('no data was found for the provided post id.')


	@ArgsCache(60)
	async def _getComments(self, post_id: str, sort: PostSort, count: int, page: int) :
		data = self.query(f"""
			SELECT
				posts.post_id,
				posts.title,
				posts.description,
				users.handle,
				users.display_name,
				users.icon,
				post_scores.upvotes,
				post_scores.downvotes,
				posts.rating,
				posts.created_on,
				posts.updated_on,
				posts.filename
			FROM kheina.public.posts
				INNER JOIN kheina.public.users
					ON posts.uploader = users.user_id
				LEFT JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
			WHERE posts.parent = %s
				AND posts.privacy_id = privacy_to_id('public')
			ORDER BY post_scores.{sort.name} DESC NULLS LAST
			LIMIT %s
			OFFSET %s;
			""",
			(
				post_id,
				count,
				count * (page - 1),
			),
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
					'icon': row[5],
				},
				'tags': await tagService.postTags(row[0]),
				'score': {
					'up': row[6],
					'down': row[7],
				},
				'rating': self._get_rating_map()[row[8]],
				'created': str(row[9]),
				'updated': str(row[10]),
				'media': bool(row[11]),
			}
			for row in data
		]


	@HttpErrorHandler('retrieving comments')
	async def fetchComments(self, user: KhUser, post_id: str, sort: PostSort, count: int, page: int) :
		self._validatePostId(post_id)
		self._validatePageNumber(page)
		self._validateCount(count)

		posts = ensure_future(self._getComments(post_id, sort, count, page))
		blocked_tags = self.user_blocked_tags(user.user_id)

		return [
			{
				**post,
				'blocked': bool(post['tags'] & blocked_tags),
				'tags': list(post['tags']),
			}
			for post in await posts
		]


	@ArgsCache(60)
	async def _fetch_user_posts(self, handle: str, count: int, page: int) :
		data = self.query(f"""
			SELECT DISTINCT
				posts.post_id,
				posts.title,
				posts.description,
				u2.handle,
				u2.display_name,
				u2.icon,
				post_scores.upvotes,
				post_scores.downvotes,
				posts.rating,
				posts.parent,
				posts.created_on,
				posts.updated_on,
				posts.filename
			FROM kheina.public.users u
				INNER JOIN kheina.public.tags
					ON tags.owner = u.user_id
				INNER JOIN kheina.public.tag_post
					ON tag_post.tag_id = tags.tag_id
				INNER JOIN kheina.public.posts
					ON posts.post_id = tag_post.post_id
				INNER JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
				INNER JOIN kheina.public.users u2
					ON posts.uploader = u2.user_id
			WHERE u.handle = %s
				AND posts.privacy_id = privacy_to_id('public')
			ORDER BY posts.created_on DESC
			LIMIT %s
			OFFSET %s;
			""",
			(handle, count, count * (page - 1)),
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
					'icon': row[5],
				},
				'tags': await tagService.postTags(row[0]),
				'score': {
					'up': row[6],
					'down': row[7],
				},
				'rating': self._get_rating_map()[row[8]],
				'parent': row[9],
				'created': str(row[10]),
				'updated': str(row[11]),
				'media': bool(row[12]),
			}
			for row in data
		]


	@HttpErrorHandler('retrieving user posts')
	async def fetchUserPosts(self, user: KhUser, handle: str, count: int, page: int) :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts = ensure_future(self._fetch_user_posts(handle, count, page))
		blocked_tags = self.user_blocked_tags(user.user_id)

		return [
			{
				**post,
				'blocked': bool(post['tags'] & blocked_tags),
				'tags': list(post['tags']),
			}
			for post in await posts
		]


	@HttpErrorHandler("retrieving user's own posts")
	@ArgsCache(60)
	def fetchOwnPosts(self, user: KhUser, sort: PostSort, count: int, page: int) :
		data = self.query(f"""
			SELECT posts.post_id, posts.title, posts.description, privacy.type
			FROM kheina.public.posts
				LEFT JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
				LEFT JOIN kheina.public.privacy
					ON privacy.privacy_id = posts.privacy_id
			WHERE posts.uploader = %s
			ORDER BY post_scores.{sort.name} DESC NULLS LAST
			LIMIT %s
			OFFSET %s;
			""",
			(user.user_id, count, count * (page - 1)),
			fetch_all=True,
		)

		return [
			dict(zip(Posts.user_post_keys, row))
			for row in data
		]
