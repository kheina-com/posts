from asyncio import Task, ensure_future
from datetime import datetime
from enum import Enum, unique
from typing import Dict, Iterable, List, Optional

from aiohttp import ClientResponseError
from fuzzly_users.api import UserClient
from fuzzly_users.internal import InternalUser
from fuzzly_users.models import UserPortable
from kh_common.auth import KhUser
from kh_common.config.constants import tags_host
from kh_common.config.credentials import fuzzly_client_token
from kh_common.gateway import Gateway
from kh_common.models.privacy import Privacy
from kh_common.models.rating import Rating
from kh_common.utilities import flatten
from pydantic import BaseModel, validator

from fuzzly_posts.blocking import is_post_blocked
from fuzzly_posts.models import MediaType, PostId, PostIdValidator, PostSize, Score
from fuzzly_posts.scoring import Scoring


Scores: Scoring = Scoring()
user_client: UserClient = UserClient(fuzzly_client_token, internal=True)


@unique
class TagGroupPortable(Enum) :
	artist: str = 'artist'
	subject: str = 'subject'
	sponsor: str = 'sponsor'
	species: str = 'species'
	gender: str = 'gender'
	misc: str = 'misc'


class TagPortable(str) :
	pass


class TagGroups(Dict[TagGroupPortable, List[TagPortable]]) :
	pass


TagsGateway: Gateway = Gateway(tags_host + '/v1/fetch_tags/{post_id}', TagGroups)


async def _get_tags(post_id: PostId) -> Iterable[str] :
	try :
		return flatten(await TagsGateway(post_id=post_id))

	except ClientResponseError as e :
		if e.status != 404 :
			raise

		return []


class Post(BaseModel) :
	_post_id_validator = PostIdValidator

	post_id: PostId
	title: Optional[str]
	description: Optional[str]
	user: UserPortable
	score: Optional[Score]
	rating: Rating
	parent: Optional[PostId]
	privacy: Privacy
	created: Optional[datetime]
	updated: Optional[datetime]
	filename: Optional[str]
	media_type: Optional[MediaType]
	size: Optional[PostSize]
	blocked: bool

	@validator('parent', pre=True, always=True)
	def _parent_validator(value) :
		if value :
			return PostId(value)


class InternalPost(BaseModel) :
	post_id: int
	title: Optional[str]
	description: Optional[str]
	user_id: int
	user: str
	rating: Rating
	parent: Optional[int]
	privacy: Privacy
	created: Optional[datetime]
	updated: Optional[datetime]
	filename: Optional[str]
	media_type: Optional[MediaType]
	size: Optional[PostSize]


	async def user_portable(self: 'InternalPost', user: KhUser) -> UserPortable :
		iuser: InternalUser = await user_client._user(user_id=self.user_id)
		return await iuser.portable(user)


	async def post(self: 'InternalPost', user: KhUser) -> Post :
		post_id: PostId = PostId(self.post_id)
		uploader_task: Task[UserPortable] = ensure_future(self.user_portable(user))
		score: Task[Score] = ensure_future(Scores.getScore(user, post_id))
		uploader: UserPortable
		blocked: bool = False

		if user :
			tags: TagGroups = ensure_future(_get_tags(post_id))
			uploader = await uploader_task
			blocked = await is_post_blocked(user, uploader.handle, self.user_id, await tags)

		else :
			uploader = await uploader_task

		return Post(
			post_id=post_id,
			title=self.title,
			description=self.description,
			user=uploader,
			score=await score,
			rating=self.rating,
			parent=self.parent,
			privacy=self.privacy,
			created=self.created,
			updated=self.updated,
			filename=self.filename,
			media_type=self.media_type,
			size=self.size,
			blocked=blocked,
		)
