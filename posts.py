from kh_common.sql.query import Field, Join, JoinType, Operator, Order, Query, Table, Value, Where
from kh_common.scoring import confidence, controversial as calc_cont, hot as calc_hot
from kh_common.exceptions.http_error import BadRequest, HttpErrorHandler, NotFound
from kh_common.models.privacy import Privacy, UserPrivacy
from kh_common.caching import ArgsCache, SimpleCache
from models import MediaType, Post, PostSort, Score
from kh_common.models.verified import Verified
from kh_common.models.user import UserPortable
from asyncio import ensure_future, Task, wait
from kh_common.blocking import UserBlocking
from kh_common.models.rating import Rating
from typing import List, Set, Tuple, Union
from collections import defaultdict
from kh_common.auth import KhUser
from copy import copy
from tags import Tags


tagService = Tags()


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
			user = UserPortable(
				following = uploader['handle'].lower() in self._get_followers(user.user_id),
				**uploader,
			),
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
			).where(
				Where(
					Field('tags', 'deprecated'),
					Operator.equal,
					False,					
				),
			).having(
				Where(
					Value(1, 'count'),
					Operator.equal,
					Value(
						len(include_tags) + len(include_users) + int(bool(include_rating))
					),
				),
			)

			if include_tags :
				query.where(
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
				Order.descending_nulls_last if sort == PostSort.new else Order.ascending_nulls_last,
			)

		else :
			query.order(
				Field('post_scores', sort.name),
				Order.descending_nulls_last,
			)

		query.select(
			Field('posts', 'post_id'),
			Field('posts', 'title'),
			Field('posts', 'description'),
			Field('users', 'handle'),
			Field('users', 'display_name'),
			Field('post_scores', 'upvotes'),
			Field('post_scores', 'downvotes'),
			Field('users', 'icon'),
			Field('posts', 'rating'),
			Field('posts', 'parent'),
			Field('posts', 'created_on'),
			Field('posts', 'updated_on'),
			Field('posts', 'filename'),
			Field('users', 'admin'),
			Field('users', 'mod'),
			Field('users', 'verified'),
			Field('posts', 'media_type_id'),
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
				'user': {
					'handle': row[3],
					'name': row[4],
					'privacy': UserPrivacy.public,
					'icon': row[7],
					'verified': Verified.admin if row[13] else (
						Verified.mod if row[14] else (
							Verified.artist if row[15] else None
						)
					)
				},
				'score': Score(
					up = row[5],
					down = row[6],
					total = row[5] + row[6],
				) if row[5] is not None else None,
				'rating': self._get_rating_map()[row[8]],
				'parent': row[9],
				'created': row[10],
				'updated': row[11],
				'filename': row[12],
				'media_type': self._get_media_type_map()[row[16]],
				'privacy': Privacy.public,
				'tags': await tagService.postTags(row[0]),
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
				posts.parent,
				users.admin,
				users.mod,
				users.verified,
				posts.post_id
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
			raise NotFound(f'no data was found for the provided post id: {post_id}.')

		return {
			'post_id': data[0][18],
			'title': data[0][0],
			'description': data[0][1],
			'user': {
				'handle': data[0][3],
				'name': data[0][4],
				'privacy': UserPrivacy.public,
				'icon': data[0][12],
				'verified': Verified.admin if data[0][15] else (
					Verified.mod if data[0][16] else (
						Verified.artist if data[0][17] else None
					)
				)
			},
			'score': Score(
				up = data[0][10],
				down = data[0][11],
				total = data[0][10] + data[0][11],
			) if data[0][10] is not None else None,
			'rating': self._get_rating_map()[data[0][13]],
			'parent': data[0][14],
			'privacy': self._get_privacy_map()[data[0][7]],
			'created': data[0][5],
			'updated': data[0][6],
			'filename': data[0][2],
			'media_type': self._get_media_type_map()[data[0][8]],
			'user_id': data[0][9],
			'tags': await tagService.postTags(data[0][18]),
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
				users.display_name,
				users.icon,
				post_scores.upvotes,
				post_scores.downvotes,
				posts.rating,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				users.admin,
				users.mod,
				users.verified,
				posts.media_type_id
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
					'privacy': UserPrivacy.public,
					'icon': row[5],
					'verified': Verified.admin if row[12] else (
						Verified.mod if row[13] else (
							Verified.artist if row[14] else None
						)
					)
				},
				'score': Score(
					up = row[6],
					down = row[7],
					total = row[6] + row[7],
				),
				'rating': self._get_rating_map()[row[8]],
				'created': row[9],
				'updated': row[10],
				'filename': row[11],
				'media_type': self._get_media_type_map()[row[15]],
				'privacy': Privacy.public,
				'tags': await tagService.postTags(row[0]),
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
			Field('users', 'display_name'),
			Field('post_scores', 'upvotes'),
			Field('post_scores', 'downvotes'),
			Field('users', 'icon'),
			Field('posts', 'rating'),
			Field('posts', 'parent'),
			Field('posts', 'created_on'),
			Field('posts', 'updated_on'),
			Field('posts', 'filename'),
			Field('users', 'admin'),
			Field('users', 'mod'),
			Field('users', 'verified'),
			Field('posts', 'media_type_id'),
			Field('post_votes', 'upvote'),
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
			Order.descending_nulls_last,
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
				user = UserPortable(
					handle = row[3],
					name = row[4],
					privacy = UserPrivacy.public,
					icon = row[7],
					following = True,
					verified = Verified.admin if row[13] else (
						Verified.mod if row[14] else (
							Verified.artist if row[15] else None
						)
					)
				),
				score = Score(
					up = row[5],
					down = row[6],
					total = row[5] + row[6],
					user_vote = 0 if row[17] is None else (1 if row[17] else -1)
				) if row[5] is not None else None,
				rating = self._get_rating_map()[row[8]],
				parent = row[9],
				created = row[10],
				updated = row[11],
				filename = row[12],
				media_type = self._get_media_type_map()[row[16]],
				privacy = Privacy.public,
				blocked = bool(await tagService.postTags(row[0]) & blocked_tags),
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
				u2.display_name,
				u2.icon,
				post_scores.upvotes,
				post_scores.downvotes,
				posts.rating,
				posts.parent,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				u2.admin,
				u2.mod,
				u2.verified,
				posts.media_type_id
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
					'privacy': UserPrivacy.public,
					'icon': row[5],
					'verified': Verified.admin if row[13] else (
						Verified.mod if row[14] else (
							Verified.artist if row[15] else None
						)
					)
				},
				'score': Score(
					up = row[6],
					down = row[7],
					total = row[6] + row[7],
				),
				'rating': self._get_rating_map()[row[8]],
				'parent': row[9],
				'privacy': Privacy.public,
				'created': row[10],
				'updated': row[11],
				'filename': row[12],
				'media_type': self._get_media_type_map()[row[16]],
				'tags': await tagService.postTags(row[0]),
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
				posts.parent,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				users.admin,
				users.mod,
				users.verified,
				posts.privacy_id,
				posts.media_type_id
			FROM kheina.public.posts
				INNER JOIN kheina.public.users
					ON posts.uploader = users.user_id
				LEFT JOIN kheina.public.post_scores
					ON post_scores.post_id = posts.post_id
			WHERE posts.uploader = %s
			ORDER BY post_scores.{sort.name} DESC NULLS LAST
			LIMIT %s
			OFFSET %s;
			""",
			(user.user_id, count, count * (page - 1)),
			fetch_all=True,
		)

		return [
			Post(
				post_id = row[0],
				title = row[1],
				description = row[2],
				user = UserPortable(
					handle = row[3],
					name = row[4],
					privacy = UserPrivacy.public,
					icon = row[5],
					following = False,
					verified = Verified.admin if row[13] else (
						Verified.mod if row[14] else (
							Verified.artist if row[15] else None
						)
					)
				),
				score = Score(
					up = row[6],
					down = row[7],
					total = row[6] + row[7],
				) if row[6] is not None else None,
				rating = self._get_rating_map()[row[8]],
				parent = row[9],
				privacy = self._get_privacy_map()[row[16]],
				created = row[10],
				updated = row[11],
				filename = row[12],
				media_type = self._get_media_type_map()[row[17]],
				blocked = False,
			)
			for row in data
		]
