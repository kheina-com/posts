from asyncio import Task, ensure_future, wait
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional, Set, Tuple

from kh_common.auth import KhUser
from kh_common.caching import AerospikeCache, ArgsCache, SimpleCache
from kh_common.caching.key_value_store import KeyValueStore
from kh_common.config.constants import users_host
from kh_common.datetime import datetime
from kh_common.exceptions.http_error import BadRequest, HttpErrorHandler, NotFound
from kh_common.gateway import Gateway
from kh_common.models.privacy import Privacy
from kh_common.models.rating import Rating
from kh_common.models.user import UserPortable
from kh_common.sql import SqlInterface
from kh_common.sql.query import Field, Join, JoinType, Operator, Order, Query, Table, Value, Where

from fuzzly_posts.blocking import Blocking
from fuzzly_posts.models import MediaType, Post, PostId, PostSize, PostSort, Score
from fuzzly_posts.models.internal import InternalPost
from fuzzly_posts.scoring import Scoring
from tags import Tags


TagService: Tags = Tags()
UsersService: Gateway = Gateway(users_host + '/v1/fetch_user/{handle}', UserPortable)
KVS: KeyValueStore = KeyValueStore('kheina', 'posts-v2')
Scores: Scoring = Scoring()
BlockCheck: Blocking = Blocking()


class Posts(SqlInterface) :

	def _validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest(f'the given page number is invalid: {page_number}. page number must be greater than or equal to 1.', page_number=page_number)


	def _validateCount(self, count: int) :
		if not 1 <= count <= 1000 :
			raise BadRequest(f'the given count is invalid: {count}. count must be between 1 and 1000.', count=count)


	@HttpErrorHandler('processing vote')
	def vote(self, user: KhUser, post_id: str, upvote: Optional[bool]) -> Score :
		return Scores.vote(user, post_id, upvote)


	@ArgsCache(60)
	async def _fetch_posts(self, sort: PostSort, tags: Tuple[str], count: int, page: int) -> List[InternalPost] :
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

				if tag.startswith('sort:') :
					try :
						sort = PostSort[tag[5:]]

					except KeyError :
						raise BadRequest(f'{tag[5:]} is not a valid sort method. valid methods: {list(PostSort.__members__.keys())}')

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
				).having(
					Where(
						Value(1, 'count'),
						Operator.equal,
						Value(len(include_tags)),
					),
				)

			elif include_users :
				query = Query(
					Table('kheina.public.users')
				).join(
					Join(
						JoinType.inner,
						Table('kheina.public.posts'),
					).where(
						Where(
							Field('posts', 'uploader'),
							Operator.equal,
							Field('users', 'user_id'),
						),
						Where(
							Field('posts', 'privacy_id'),
							Operator.equal,
							"privacy_to_id('public')",
						),
					),
				)

			else :
				query = Query(
					Table('kheina.public.posts')
				).join(
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
						Field('lower(users', 'handle)'),
						Operator.equal,
						Value(include_users[0], 'lower'),
					),
				)

			if exclude_users :
				query.where(
					Where(
						Field('lower(users', 'handle)'),
						Operator.not_equal,
						Value(exclude_users, 'any'),  # TODO: add lower + any
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
			).join(
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
			).group(
				Field('posts', 'post_id'),
				Field('users', 'user_id'),
			)

		else :
			query.order(
				Field('post_scores', sort.name),
				Order.descending_nulls_first,
			).order(
				Field('posts', 'created_on'),
				Order.descending_nulls_first,
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
			).group(
				Field('posts', 'post_id'),
				Field('post_scores', 'post_id'),
				Field('users', 'user_id'),
			)

		query.select(
			Field('posts', 'post_id'),
			Field('posts', 'title'),
			Field('posts', 'description'),
			Field('users', 'handle'),
			Field('posts', 'rating'),
			Field('posts', 'parent'),
			Field('posts', 'created_on'),
			Field('posts', 'updated_on'),
			Field('posts', 'filename'),
			Field('posts', 'media_type_id'),
			Field('posts', 'width'),
			Field('posts', 'height'),
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

		data = await self.query_async(query, fetch_all=True)
		posts: List[InternalPost] = []

		for row in data :
			post = InternalPost(
				post_id=row[0],
				title=row[1],
				description=row[2],
				user=row[3],
				rating=self._get_rating_map()[row[4]],
				parent=row[5],
				privacy=Privacy.public,
				created=row[6],
				updated=row[7],
				filename=row[8],
				media_type=self._get_media_type_map()[row[9]],
				user_id=row[12],
				size=PostSize(width=row[10], height=row[11]) if row[10] and row[11] else None,
			)
			posts.append(post)
			KVS.put(post.post_id, post)

		return posts


	@HttpErrorHandler('fetching posts')
	async def fetchPosts(self, user: KhUser, sort: PostSort, tags: Optional[List[str]], count:int=64, page:int=1) -> List[Post] :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts: Task[List[InternalPost]] = self._fetch_posts(sort, tuple(sorted(map(str.lower, filter(None, map(str.strip, filter(None, tags)))))) if tags else None, count, page)
		posts: Task[List[Post]] = [ensure_future(post.post(user)) for post in await posts]

		if posts :
			await wait(posts)

		return list(map(Task.result, posts))


	@SimpleCache(float('inf'))
	def _get_rating_map(self) :
		data = self.query("""
			SELECT rating_id, rating
			FROM kheina.public.ratings;
			""",
			fetch_all=True,
		)
		return { x[0]: Rating[x[1]] for x in data if x[1] in Rating.__members__ }


	@SimpleCache(float('inf'))
	def _rating_to_id(self) :
		return { v.name: k for k, v in self._get_rating_map().items() }


	@SimpleCache(float('inf'))
	def _get_privacy_map(self) :
		data = self.query("""
			SELECT privacy_id, type
			FROM kheina.public.privacy;
			""",
			fetch_all=True,
		)
		return { x[0]: Privacy[x[1]] for x in data if x[1] in Privacy.__members__ }


	@SimpleCache(float('inf'))
	def _privacy_to_id(self) :
		return { v: k for k, v in self._get_privacy_map().items() }


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


	@AerospikeCache('kheina', 'posts-v2', '{post_id}', _kvs=KVS)
	async def _get_post(self, post_id: PostId) -> InternalPost :
		data = self.query("""
			SELECT
				posts.post_id,
				posts.title,
				posts.description,
				users.handle,
				users.user_id,
				posts.created_on,
				posts.updated_on,
				posts.privacy_id,
				posts.media_type_id,
				posts.filename,
				posts.rating,
				posts.parent,
				posts.width,
				posts.height
			FROM kheina.public.posts
				INNER JOIN kheina.public.users
					ON posts.uploader = users.user_id
				LEFT JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
			WHERE posts.post_id = %s
			""",
			(post_id.int(),),
			fetch_one=True,
		)

		if not data :
			raise NotFound(f'no data was found for the provided post id: {post_id}.')

		return InternalPost(
			post_id=data[0],
			title=data[1],
			description=data[2],
			user=data[3],
			user_id=data[4],
			created=data[5],
			updated=data[6],
			rating=self._get_rating_map()[data[10]],
			parent=data[11],
			privacy=self._get_privacy_map()[data[7]],
			filename=data[9],
			media_type=self._get_media_type_map()[data[8]],
			size=PostSize(width=data[12], height=data[13]) if data[12] and data[13] else None,
		)


	@HttpErrorHandler('retrieving post')
	async def getPost(self, user: KhUser, post_id: PostId) -> Post :
		post: InternalPost = await self._get_post(post_id)

		if (
			post.privacy in { Privacy.public, Privacy.unlisted } or
			(post.user_id == user.user_id and await user.authenticated(raise_error=False))
			# add additional check here to see if post is private, but user was given permission to view
		) :
			return await post.post(user)

		raise NotFound(f'no data was found for the provided post id: {post_id}.')


	@ArgsCache(5)
	async def _getComments(self, post_id: PostId, sort: PostSort, count: int, page: int) -> List[InternalPost] :
		# TODO: fix new and old sorts
		data = await self.query_async(f"""
			SELECT
				posts.post_id,
				posts.title,
				posts.description,
				users.handle,
				posts.rating,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				posts.media_type_id,
				posts.width,
				posts.height,
				users.user_id
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
				post_id.int(),
				count,
				count * (page - 1),
			),
			fetch_all=True,
		)

		posts: List[InternalPost] = []

		for row in data :
			post = InternalPost(
				post_id=row[0],
				title=row[1],
				description=row[2],
				user=row[3],
				rating=self._get_rating_map()[row[4]],
				privacy=Privacy.public,
				parent=post_id,
				created=row[5],
				updated=row[6],
				filename=row[7],
				media_type=self._get_media_type_map()[row[8]],
				user_id=row[11],
				size=PostSize(width=row[9], height=row[10]) if row[9] and row[10] else None,
			)
			posts.append(post)
			KVS.put(post.post_id, post)

		return posts


	@HttpErrorHandler('retrieving comments')
	async def fetchComments(self, user: KhUser, post_id: PostId, sort: PostSort, count: int, page: int) -> List[Post] :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts: Task[List[Post]] = [ensure_future(post.post(user)) for post in await self._getComments(post_id, sort, count, page)]

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
			Field('users', 'user_id'),
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

		data = await self.query_async(query, fetch_all=True)
		meta: Dict[str, Task[List[str]]] = { }
		token_string: Optional[str] = user.token.token_string if user.token else None

		for row in data :
			meta[row[0]] = {
				'tags': ensure_future(TagService.postTags(row[0])),
				'user': ensure_future(UsersService(handle=row[3], auth=token_string)),
			}

		return [
			Post(
				post_id = row[0],
				title = row[1],
				description = row[2],
				user = await meta[row[0]]['user'],
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
				blocked = await BlockCheck.isPostBlocked(user, row[3], row[15], await meta[row[0]]['tags']),
				size = PostSize(width=row[13], height=row[14]) if row[13] and row[14] else None,
			)
			for row in data
		]


	@ArgsCache(10)
	@HttpErrorHandler('generating RSS feed')
	async def RssFeedPosts(self, user: KhUser) -> Tuple[datetime, List[Post]]:
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
			Field('users', 'user_id'),
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

		data = await self.query_async(query, fetch_all=True)
		meta: Dict[str, Task[List[str]]] = { }
		token_string: Optional[str] = user.token.token_string if user.token else None

		for row in data :
			meta[row[0]] = {
				'tags': ensure_future(TagService.postTags(row[0])),
				'user': ensure_future(UsersService(handle=row[3], auth=token_string)),
			}

		return now, [
			Post(
				post_id = row[0],
				title = row[1],
				description = row[2],
				user = await meta[row[0]]['user'],
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
				blocked = await BlockCheck.isPostBlocked(user, row[3], row[15], await meta[row[0]]['tags']),
				size = PostSize(width=row[13], height=row[14]) if row[13] and row[14] else None,
			)
			for row in data
		]


	@ArgsCache(5)
	async def _fetch_user_posts(self, handle: str, count: int, page: int) -> List[InternalPost] :
		data = await self.query_async(f"""
			SELECT DISTINCT
				posts.post_id,
				posts.title,
				posts.description,
				u2.handle,
				posts.rating,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				posts.media_type_id,
				posts.width,
				posts.height,
				users.user_id,
				posts.parent
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

		posts: List[InternalPost] = []

		for row in data :
			post = InternalPost(
				post_id=row[0],
				title=row[1],
				description=row[2],
				user=row[3],
				rating=self._get_rating_map()[row[4]],
				privacy=Privacy.public,
				created=row[5],
				updated=row[6],
				filename=row[7],
				media_type=self._get_media_type_map()[row[8]],
				size=PostSize(width=row[9], height=row[10]) if row[9] and row[10] else None,
				user_id=row[11],
				parent=row[12],
			)
			posts.append(post)
			KVS.put(post.post_id, post)

		return posts


	@HttpErrorHandler('retrieving user posts')
	async def fetchUserPosts(self, user: KhUser, handle: str, count: int, page: int) -> List[Post] :
		self._validatePageNumber(page)
		self._validateCount(count)

		posts: List[Task[Post]] = [ensure_future(post.post(user)) for post in await self._fetch_user_posts(handle, count, page)]

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

		data = await self.query_async(query, fetch_all=True)

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


	@HttpErrorHandler("retrieving user's own posts")
	@ArgsCache(5)
	async def fetchDrafts(self, user: KhUser) -> List[Post] :
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
			Where(
				Field('posts', 'privacy_id'),
				Operator.equal,
				Value(self._privacy_to_id()[Privacy.draft]),				
			),
		).order(
			Field('posts', 'created_on'),
			Order.descending_nulls_first,
		)

		data = await self.query_async(query, fetch_all=True)

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
