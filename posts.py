from kh_common.sql.query import Field, Join, JoinType, Operator, Order, Query, Table, Value, Where
from kh_common.scoring import confidence, controversial as calc_cont, hot as calc_hot
from kh_common.exceptions.http_error import BadRequest, HttpErrorHandler, NotFound
from models import MediaType, Post, PostSize, PostSort, Score
from kh_common.caching import ArgsCache, SimpleCache
from typing import List, Set, Optional, Tuple, Union
from kh_common.config.constants import users_host
from kh_common.models.user import UserPortable
from asyncio import ensure_future, Task, wait
from kh_common.models.privacy import Privacy
from kh_common.blocking import UserBlocking
from kh_common.models.rating import Rating
from kh_common.datetime import datetime
from kh_common.gateway import Gateway
from collections import defaultdict
from kh_common.auth import KhUser
from datetime import timedelta
from copy import copy
from tags import Tags


tagService = Tags()
UsersService = Gateway(users_host + '/v1/fetch_user/{handle}', UserPortable)


class Posts(UserBlocking) :

	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest(f'the given post id is invalid: {post_id}.')


	def _validateVote(self, vote: Union[bool, None]) :
		if not isinstance(vote, (bool, type(None))) :
			raise BadRequest('the given vote is invalid (vote value must be integer. 1 = up, -1 = down, 0 or null to remove vote)')


	def _validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest(f'the given page number is invalid: {page_number}. page number must be greater than or equal to 1.', page_number=page_number)


	def _validateCount(self, count: int) :
		if not 1 <= count <= 1000 :
			raise BadRequest(f'the given count is invalid: {count}. count must be between 1 and 1000.', count=count)


	async def _dict_to_post(self, post: dict, user: KhUser) -> Post :
		post = copy(post)
		uploader = post.pop('user')

		if post['privacy'] == Privacy.unpublished :
			post['created'] = post['updated'] = None

		return Post(
			**post,
			user = await UsersService(handle=uploader, auth=user.token.token_string if user.token else None),
			blocked = (
				bool(post['tags'] & self.user_blocked_tags(user.user_id))
				if 'tags' in post else
				bool(await tagService.postTags(post['post_id']) & self.user_blocked_tags(user.user_id))
			),
		)


	@HttpErrorHandler('processing vote')
	def vote(self, user: KhUser, post_id: str, upvote: Union[bool, None]) :
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

		return Score(
			up = up,
			down = down,
			total = total,
			user_vote = 0 if upvote is None else (1 if upvote else -1),
		)


	@ArgsCache(60)
	async def _count_posts_by_tag(self, tag: Optional[str]) :
		if tag :
			return await self.query_async("""
				SELECT COUNT(1)
				FROM kheina.public.tags
					INNER JOIN kheina.public.tag_post
						ON tags.tag_id = tag_post.tag_id
					INNER JOIN kheina.public.posts
						ON tag_post.post_id = posts.post_id
							AND posts.privacy_id = privacy_to_id('public')
				WHERE tags.tag = %s;
				""",
				(tag,),
				fetch_one=True,
			)

		else :
			return await self.query_async("""
				SELECT COUNT(1)
				FROM kheina.public.posts
				WHERE posts.privacy_id = privacy_to_id('public');
				""",
				fetch_one=True,
			)


	@ArgsCache(60)
	async def _fetch_posts(self, sort: PostSort, tags: Tuple[str], count: int, page: int) :
		idk = { }

		if tags :
			include_tags = []
			exclude_tags = []

			include_users = []
			exclude_users = []

			include_rating = []
			exclude_rating = []

			for tag in tags :
				exclude = tag.startswith('-')

				if exclude :
					tag = tag[1:]

				if tag.startswith('@') :
					tag = tag[1:]
					(exclude_users if exclude else include_users).append(tag)
					continue

				if tag in { 'general', 'mature', 'explicit' } :
					(exclude_rating if exclude else include_rating).append(tag)
					continue

				(exclude_tags if exclude else include_tags).append(tag)

			if len(include_users) > 1 :
				raise BadRequest('can only search for posts from, at most, one user at a time.')

			if len(include_rating) > 1 :
				raise BadRequest('can only search for posts from, at most, one rating at a time.')

			if include_tags or exclude_tags :
				query = Query(
					Table('kheina.public.tags')
				).join(
					Join(
						JoinType.inner,
						Table('kheina.public.tag_post'),
					).where(
						Where(
							Field('tag_post', 'tag_id'),
							Operator.equal,
							Field('tags', 'tag_id'),
						),
					),
					Join(
						JoinType.inner,
						Table('kheina.public.posts'),
					).where(
						Where(
							Field('posts', 'post_id'),
							Operator.equal,
							Field('tag_post', 'post_id'),
						),
						Where(
							Field('posts', 'privacy_id'),
							Operator.equal,
							"privacy_to_id('public')",
						),
					),
				).having(
					Where(
						Value(1, 'count'),
						Operator.equal,
						Value(len(include_tags)),
					),
				)

			else :
				query = Query(
					Table('kheina.public.posts')
				).where(
					Where(
						Field('posts', 'privacy_id'),
						Operator.equal,
						"privacy_to_id('public')",
					),
				)

			if include_tags :
				query.where(
					Where(
						Field('tags', 'deprecated'),
						Operator.equal,
						False,
					),
					Where(
						Field('tags', 'tag'),
						Operator.equal,
						Value(include_tags, 'any'),
					),
				)

			if exclude_tags :
				query.where(
					Where(
						Field('posts', 'post_id'),
						Operator.not_in,
						Query(
							Table('kheina.public.tags')
						).select(
							Field('tag_post', 'post_id'),
						).join(
							Join(
								JoinType.inner,
								Table('kheina.public.tag_post'),
							).where(
								Where(
									Field('tag_post', 'tag_id'),
									Operator.equal,
									Field('tags', 'tag_id'),
								),
							),
						).where(
							Where(
								Field('tags', 'tag'),
								Operator.equal,
								Value(exclude_tags, 'any'),
							),
						),
					),
				)

			if include_users :
				query.where(
					Where(
						Field('users', 'handle'),
						Operator.equal,
						Value(include_users[0]),
					),
				)

			if exclude_users :
				query.where(
					Where(
						Field('users', 'handle'),
						Operator.not_equal,
						Value(exclude_users, 'any'),
					),
				)

			if include_rating :
				query.where(
					Where(
						Field('posts', 'rating'),
						Operator.equal,
						Value(self._rating_to_id()[include_rating[0]]),
					),
				)

			if exclude_rating :
				query.where(
					Where(
						Field('posts', 'rating'),
						Operator.not_equal,
						Value(list(map(lambda x : self._rating_to_id()[x], exclude_rating)), 'all'),
					),
				)

			idk = {
				'tags': tags,
				'include_tags': include_tags,
				'exclude_tags': exclude_tags,
				'include_users': include_users,
				'exclude_users': exclude_users,
				'include_rating': include_rating,
				'exclude_rating': exclude_rating,
			}

		else :
			query = Query(
				Table('kheina.public.posts')
			).where(
				Where(
					Field('posts', 'privacy_id'),
					Operator.equal,
					"privacy_to_id('public')",
				),
			)

		if sort in { PostSort.new, PostSort.old } :
			query.order(
				Field('posts', 'created_on'),
				Order.descending_nulls_first if sort == PostSort.new else Order.ascending_nulls_last,
			)

		else :
			query.order(
				Field('post_scores', sort.name),
				Order.descending_nulls_first,
			).order(
				Field('posts', 'created_on'),
				Order.descending_nulls_first,
			)

		query.select(
			Field('posts', 'post_id'),
			Field('posts', 'title'),
			Field('posts', 'description'),
			Field('users', 'handle'),
			Field('post_scores', 'upvotes'),
			Field('post_scores', 'downvotes'),
			Field('posts', 'rating'),
			Field('posts', 'parent'),
			Field('posts', 'created_on'),
			Field('posts', 'updated_on'),
			Field('posts', 'filename'),
			Field('posts', 'media_type_id'),
			Field('posts', 'width'),
			Field('posts', 'height'),
		).join(
			Join(
				JoinType.inner,
				Table('kheina.public.post_scores'),
			).where(
				Where(
					Field('post_scores', 'post_id'),
					Operator.equal,
					Field('posts', 'post_id'),
				),
			),
			Join(
				JoinType.inner,
				Table('kheina.public.users'),
			).where(
				Where(
					Field('users', 'user_id'),
					Operator.equal,
					Field('posts', 'uploader'),
				),
			),
		).group(
			Field('posts', 'post_id'),
			Field('post_scores', 'post_id'),
			Field('users', 'user_id'),
		).limit(
			count,
		).page(
			page,
		)


		sql, params = query.build()

		self.logger.info({
			'query': sql,
			'params': params,
			**idk,
		})

		data = self.query(query, fetch_all=True)

		return [
			{
				'post_id': row[0],
				'title': row[1],
				'description': row[2],
				'user': row[3],
				'score': Score(
					up = row[4],
					down = row[5],
					total = row[4] + row[5],
				) if row[4] is not None else None,
				'rating': self._get_rating_map()[row[6]],
				'parent': row[7],
				'created': row[8],
				'updated': row[9],
				'filename': row[10],
				'media_type': self._get_media_type_map()[row[11]],
				'privacy': Privacy.public,
				'tags': await tagService.postTags(row[0]),
				'size': PostSize(width=row[12], height=row[13]) if row[12] and row[13] else None,
			}
			for row in data
		]

	@HttpErrorHandler('fetching posts')
	async def fetchPosts(self, user: KhUser, sort: PostSort, tags: Union[List[str], None], count:int=64, page:int=1) :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts = self._fetch_posts(sort, tuple(sorted(map(str.lower, filter(None, map(str.strip, filter(None, tags)))))) if tags else None, count, page)
		posts = [ensure_future(self._dict_to_post(post, user)) for post in await posts]

		if posts :
			await wait(posts)

		return list(map(Task.result, posts))


	@SimpleCache(600)
	def _get_rating_map(self) :
		data = self.query("""
			SELECT rating_id, rating
			FROM kheina.public.ratings;
			""",
			fetch_all=True,
		)
		return { x[0]: Rating[x[1]] for x in data if x[1] in Rating.__members__ }


	@SimpleCache(600)
	def _rating_to_id(self) :
		return { v.name: k for k, v in self._get_rating_map().items() }

	@SimpleCache(600)
	def _get_privacy_map(self) :
		data = self.query("""
			SELECT privacy_id, type
			FROM kheina.public.privacy;
			""",
			fetch_all=True,
		)
		return { x[0]: Privacy[x[1]] for x in data if x[1] in Privacy.__members__ }


	@SimpleCache(600)
	def _get_media_type_map(self) :
		data = self.query("""
			SELECT media_type_id, file_type, mime_type
			FROM kheina.public.media_type;
			""",
			fetch_all=True,
		)
		return defaultdict(lambda : None, {
			row[0]: MediaType(
				file_type = row[1],
				mime_type = row[2],
			)
			for row in data
		})


	@ArgsCache(10)
	def _get_followers(self, user_id) -> Set[str] :
		if not user_id :
			return set()

		data = self.query("""
			SELECT
				users.handle
			FROM kheina.public.following
				INNER JOIN kheina.public.users
					ON users.user_id = following.follows
			WHERE following.user_id = %s;
			""",
			(user_id,),
			fetch_all=True,
		)
		return set(map(lambda x : x[0].lower(), data))


	@ArgsCache(5)
	async def _get_post(self, post_id: str) :
		data = self.query("""
			SELECT
				posts.title,
				posts.description,
				posts.filename,
				users.handle,
				users.user_id,
				posts.created_on,
				posts.updated_on,
				posts.privacy_id,
				posts.media_type_id,
				post_scores.upvotes,
				post_scores.downvotes,
				posts.rating,
				posts.parent,
				posts.post_id,
				posts.width,
				posts.height
			FROM kheina.public.posts
				INNER JOIN kheina.public.users
					ON posts.uploader = users.user_id
				LEFT JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
			WHERE posts.post_id = %s
			""",
			(post_id,),
			fetch_one=True,
		)

		if not data :
			raise NotFound(f'no data was found for the provided post id: {post_id}.')

		return {
			'post_id': data[13],
			'title': data[0],
			'description': data[1],
			'user': data[3],
			'score': Score(
				up = data[9],
				down = data[10],
				total = data[9] + data[10],
			) if data[9] is not None else None,
			'rating': self._get_rating_map()[data[11]],
			'parent': data[12],
			'privacy': self._get_privacy_map()[data[7]],
			'created': data[5],
			'updated': data[6],
			'filename': data[2],
			'media_type': self._get_media_type_map()[data[8]],
			'user_id': data[4],
			'tags': await tagService.postTags(data[13]),
			'size': PostSize(width=data[14], height=data[15]) if data[14] and data[15] else None,
		}


	@HttpErrorHandler('retrieving post')
	async def getPost(self, user: KhUser, post_id: str) :
		self._validatePostId(post_id)

		post = ensure_future(self._get_post(post_id))
		post = await post
		uploader = post.pop('user_id')

		user_is_uploader = uploader == user.user_id and await user.authenticated(raise_error=False)

		if post['privacy'] in { Privacy.public, Privacy.unlisted } or user_is_uploader :
			return await self._dict_to_post(post, user)

		raise NotFound(f'no data was found for the provided post id: {post_id}.')


	@ArgsCache(5)
	async def _getComments(self, post_id: str, sort: PostSort, count: int, page: int) :
		data = self.query(f"""
			SELECT
				posts.post_id,
				posts.title,
				posts.description,
				users.handle,
				post_scores.upvotes,
				post_scores.downvotes,
				posts.rating,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				posts.media_type_id,
				posts.width,
				posts.height
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
				'user': row[3],
				'score': Score(
					up = row[4],
					down = row[5],
					total = row[4] + row[5],
				),
				'rating': self._get_rating_map()[row[6]],
				'created': row[7],
				'updated': row[8],
				'filename': row[9],
				'media_type': self._get_media_type_map()[row[10]],
				'privacy': Privacy.public,
				'tags': await tagService.postTags(row[0]),
				'size': PostSize(width=row[11], height=row[12]) if row[11] and row[12] else None,
			}
			for row in data
		]


	@HttpErrorHandler('retrieving comments')
	async def fetchComments(self, user: KhUser, post_id: str, sort: PostSort, count: int, page: int) :
		self._validatePostId(post_id)
		self._validatePageNumber(page)
		self._validateCount(count)

		posts = [ensure_future(self._dict_to_post(post, user)) for post in await self._getComments(post_id, sort, count, page)]

		if posts :
			await wait(posts)

		return list(map(Task.result, posts))


	@ArgsCache(10)
	@HttpErrorHandler('retrieving timeline posts')
	async def timelinePosts(self, user: KhUser, count: int, page: int) -> List[Post] :
		self._validatePageNumber(page)
		self._validateCount(count)

		query = Query(
			Table('kheina.public.posts')
		).select(
			Field('posts', 'post_id'),
			Field('posts', 'title'),
			Field('posts', 'description'),
			Field('users', 'handle'),
			Field('post_scores', 'upvotes'),
			Field('post_scores', 'downvotes'),
			Field('posts', 'rating'),
			Field('posts', 'parent'),
			Field('posts', 'created_on'),
			Field('posts', 'updated_on'),
			Field('posts', 'filename'),
			Field('posts', 'media_type_id'),
			Field('post_votes', 'upvote'),
			Field('posts', 'width'),
			Field('posts', 'height'),
		).join(
			Join(
				JoinType.inner,
				Table('kheina.public.following'),
			).where(
				Where(
					Field('following', 'user_id'),
					Operator.equal,
					Value(user.user_id),
				),
				Where(
					Field('following', 'follows'),
					Operator.equal,
					Field('posts', 'uploader'),
				),
			),
			Join(
				JoinType.inner,
				Table('kheina.public.post_scores'),
			).where(
				Where(
					Field('post_scores', 'post_id'),
					Operator.equal,
					Field('posts', 'post_id'),
				),
			),
			Join(
				JoinType.inner,
				Table('kheina.public.users'),
			).where(
				Where(
					Field('users', 'user_id'),
					Operator.equal,
					Field('posts', 'uploader'),
				),
			),
			Join(
				JoinType.left,
				Table('kheina.public.post_votes'),
			).where(
				Where(
					Field('post_votes', 'user_id'),
					Operator.equal,
					Field('following', 'user_id'),
				),
				Where(
					Field('post_votes', 'post_id'),
					Operator.equal,
					Field('posts', 'post_id'),
				),
			),
		).where(
			Where(
				Field('posts', 'privacy_id'),
				Operator.equal,
				"privacy_to_id('public')"
			),
		).group(
			Field('posts', 'post_id'),
			Field('post_scores', 'post_id'),
			Field('users', 'user_id'),
			Field('post_votes', 'post_id'),
			Field('post_votes', 'user_id'),
		).order(
			Field('posts', 'created_on'),
			Order.descending_nulls_first,
		).limit(
			count,
		).page(
			page,
		)

		blocked_tags = self.user_blocked_tags(user.user_id)
		data = self.query(query, fetch_all=True)

		return [
			Post(
				post_id = row[0],
				title = row[1],
				description = row[2],
				user = await UsersService(handle=row[3], auth=user.token.token_string if user.token else None),
				score = Score(
					up = row[4],
					down = row[5],
					total = row[4] + row[5],
					user_vote = 0 if row[12] is None else (1 if row[12] else -1)
				) if row[4] is not None else None,
				rating = self._get_rating_map()[row[6]],
				parent = row[7],
				created = row[8],
				updated = row[9],
				filename = row[10],
				media_type = self._get_media_type_map()[row[11]],
				privacy = Privacy.public,
				blocked = bool(await tagService.postTags(row[0]) & blocked_tags),
				size = PostSize(width=row[13], height=row[14]) if row[13] and row[14] else None,
			)
			for row in data
		]


	@ArgsCache(10)
	@HttpErrorHandler('generating RSS feed')
	async def RssFeedPosts(self, user: KhUser) :
		now = datetime.now()

		query = Query(
			Table('kheina.public.posts')
		).select(
			Field('posts', 'post_id'),
			Field('posts', 'title'),
			Field('posts', 'description'),
			Field('users', 'handle'),
			Field('post_scores', 'upvotes'),
			Field('post_scores', 'downvotes'),
			Field('posts', 'rating'),
			Field('posts', 'parent'),
			Field('posts', 'created_on'),
			Field('posts', 'updated_on'),
			Field('posts', 'filename'),
			Field('posts', 'media_type_id'),
			Field('post_votes', 'upvote'),
			Field('posts', 'width'),
			Field('posts', 'height'),
		).join(
			Join(
				JoinType.inner,
				Table('kheina.public.following'),
			).where(
				Where(
					Field('following', 'user_id'),
					Operator.equal,
					Value(user.user_id),
				),
				Where(
					Field('following', 'follows'),
					Operator.equal,
					Field('posts', 'uploader'),
				),
			),
			Join(
				JoinType.inner,
				Table('kheina.public.post_scores'),
			).where(
				Where(
					Field('post_scores', 'post_id'),
					Operator.equal,
					Field('posts', 'post_id'),
				),
			),
			Join(
				JoinType.inner,
				Table('kheina.public.users'),
			).where(
				Where(
					Field('users', 'user_id'),
					Operator.equal,
					Field('posts', 'uploader'),
				),
			),
			Join(
				JoinType.left,
				Table('kheina.public.post_votes'),
			).where(
				Where(
					Field('post_votes', 'user_id'),
					Operator.equal,
					Field('following', 'user_id'),
				),
				Where(
					Field('post_votes', 'post_id'),
					Operator.equal,
					Field('posts', 'post_id'),
				),
			),
		).where(
			Where(
				Field('posts', 'privacy_id'),
				Operator.equal,
				"privacy_to_id('public')"
			),
			Where(
				Field('posts', 'created_on'),
				Operator.greater_than_equal_to,
				Value(now - timedelta(days=1)),
			),
		).group(
			Field('posts', 'post_id'),
			Field('post_scores', 'post_id'),
			Field('users', 'user_id'),
			Field('post_votes', 'post_id'),
			Field('post_votes', 'user_id'),
		).order(
			Field('posts', 'created_on'),
			Order.descending_nulls_first,
		)

		data = ensure_future(self.query_async(query, fetch_all=True))
		blocked_tags = self.user_blocked_tags(user.user_id)
		data = await data

		return now, [
			Post(
				post_id = row[0],
				title = row[1],
				description = row[2],
				user = await UsersService(handle=row[3], auth=user.token.token_string if user.token else None),
				score = Score(
					up = row[4],
					down = row[5],
					total = row[4] + row[5],
					user_vote = 0 if row[12] is None else (1 if row[12] else -1)
				) if row[4] is not None else None,
				rating = self._get_rating_map()[row[6]],
				parent = row[7],
				created = datetime.fromtimestamp(row[8].timestamp()),
				updated = datetime.fromtimestamp(row[9].timestamp()),
				filename = row[10],
				media_type = self._get_media_type_map()[row[11]],
				privacy = Privacy.public,
				blocked = bool(await tagService.postTags(row[0]) & blocked_tags),
				size = PostSize(width=row[13], height=row[14]) if row[13] and row[14] else None,
			)
			for row in data
		]


	@ArgsCache(5)
	async def _fetch_user_posts(self, handle: str, count: int, page: int) :
		data = self.query(f"""
			SELECT DISTINCT
				posts.post_id,
				posts.title,
				posts.description,
				u2.handle,
				post_scores.upvotes,
				post_scores.downvotes,
				posts.rating,
				posts.parent,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				posts.media_type_id,
				posts.width,
				posts.height
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
				'user': row[3],
				'score': Score(
					up = row[4],
					down = row[5],
					total = row[4] + row[5],
				),
				'rating': self._get_rating_map()[row[6]],
				'parent': row[7],
				'privacy': Privacy.public,
				'created': row[8],
				'updated': row[9],
				'filename': row[10],
				'media_type': self._get_media_type_map()[row[11]],
				'tags': await tagService.postTags(row[0]),
				'size': PostSize(width=row[12], height=row[13]) if row[12] and row[13] else None,
			}
			for row in data
		]


	@HttpErrorHandler('retrieving user posts')
	async def fetchUserPosts(self, user: KhUser, handle: str, count: int, page: int) :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts = [ensure_future(self._dict_to_post(post, user)) for post in await self._fetch_user_posts(handle, count, page)]

		if posts :
			await wait(posts)

		return list(map(Task.result, posts))


	@HttpErrorHandler("retrieving user's own posts")
	@ArgsCache(5)
	async def fetchOwnPosts(self, user: KhUser, sort: PostSort, count: int, page: int) :
		self._validatePageNumber(page)
		self._validateCount(count)

		query = Query(
			Table('kheina.public.posts')
		).select(
			Field('posts', 'post_id'),
			Field('posts', 'title'),
			Field('posts', 'description'),
			Field('users', 'handle'),
			Field('post_scores', 'upvotes'),
			Field('post_scores', 'downvotes'),
			Field('posts', 'rating'),
			Field('posts', 'parent'),
			Field('posts', 'created_on'),
			Field('posts', 'updated_on'),
			Field('posts', 'filename'),
			Field('posts', 'privacy_id'),
			Field('posts', 'media_type_id'),
			Field('posts', 'width'),
			Field('posts', 'height'),
		).join(
			Join(
				JoinType.inner,
				Table('kheina.public.users'),
			).where(
				Where(
					Field('posts', 'uploader'),
					Operator.equal,
					Field('users', 'user_id'),
				),
			),
			Join(
				JoinType.left,
				Table('kheina.public.post_scores'),
			).where(
				Where(
					Field('post_scores', 'post_id'),
					Operator.equal,
					Field('posts', 'post_id'),
				),
			),
		).where(
			Where(
				Field('posts', 'uploader'),
				Operator.equal,
				Value(user.user_id),				
			),
		).limit(
			count,
		).page(
			page,
		)

		if sort in { PostSort.new, PostSort.old } :
			query.order(
				Field('posts', 'created_on'),
				Order.descending_nulls_first if sort == PostSort.new else Order.ascending_nulls_last,
			)

		else :
			query.order(
				Field('post_scores', sort.name),
				Order.descending_nulls_first,
			).order(
				Field('posts', 'created_on'),
				Order.descending_nulls_first,
			)

		data = self.query(query, fetch_all=True)

		return [
			Post(
				post_id = row[0],
				title = row[1],
				description = row[2],
				user = await UsersService(handle=row[3], auth=user.token.token_string if user.token else None),
				score = Score(
					up = row[4],
					down = row[5],
					total = row[4] + row[5],
				) if row[4] is not None else None,
				rating = self._get_rating_map()[row[6]],
				parent = row[7],
				privacy = self._get_privacy_map()[row[11]],
				created = row[8],
				updated = row[9],
				filename = row[10],
				media_type = self._get_media_type_map()[row[12]],
				blocked = False,
				size = PostSize(width=row[13], height=row[14]) if row[13] and row[14] else None,
			)
			for row in data
		]
