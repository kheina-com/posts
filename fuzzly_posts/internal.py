from asyncio import Task, ensure_future
from datetime import datetime
from enum import Enum, unique
from typing import Dict, Iterable, List, Optional

from aiohttp import ClientResponseError
from kh_common.auth import KhUser
from kh_common.config.constants import tags_host, users_host
from kh_common.gateway import Gateway
from kh_common.models.privacy import Privacy
from kh_common.models.rating import Rating
from kh_common.models.user import UserPortable
from kh_common.utilities import flatten
from pydantic import BaseModel

from fuzzly_posts.blocking import is_post_blocked
from fuzzly_posts.scoring import Scoring
from fuzzly_posts.models import MediaType, Post, PostId, PostSize, Score


UserGateway: Gateway = Gateway(users_host + '/v1/fetch_user/{handle}', UserPortable)
Scores: Scoring = Scoring()


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

	async def post(self: 'InternalPost', user: KhUser) -> Post :
		post_id: PostId = PostId(self.post_id)
		uploader_task: Task[UserPortable] = ensure_future(UserGateway(handle=self.user))
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
