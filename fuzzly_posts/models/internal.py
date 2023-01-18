from asyncio import Task, ensure_future
from datetime import datetime
from enum import Enum, unique
from typing import Dict, List, Optional

from kh_common.auth import KhUser
from kh_common.config.constants import tags_host, users_host
from kh_common.gateway import Gateway
from kh_common.models.privacy import Privacy
from kh_common.models.rating import Rating
from kh_common.models.user import UserPortable
from kh_common.utilities import flatten
from pydantic import BaseModel

from fuzzly_posts.blocking import Blocking
from fuzzly_posts.models import MediaType, Post, PostSize, Score
from fuzzly_posts.scoring import Scoring


UserGateway: Gateway = Gateway(users_host + '/v1/fetch_user/{handle}', UserPortable)
Scores: Scoring = Scoring()
BlockCheck: Blocking = Blocking()


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
		uploader_task: Task[UserPortable] = ensure_future(UserGateway(handle=self.user))
		score: Task[Score] = ensure_future(Scores._get_score(self.post_id))
		vote: Task[int] = ensure_future(Scores._get_vote(self.user_id, self.post_id))
		uploader: UserPortable
		blocked: bool = False

		if user :
			tags: TagGroups = ensure_future(TagsGateway(post_id=self.post_id))
			uploader = await uploader_task
			blocked = await BlockCheck.isPostBlocked(user, uploader.handle, self.user_id, flatten(await tags))

		else :
			uploader = await uploader_task

		score: Score = await score
		score.user_vote = await vote

		return Post(
			post_id=self.post_id,
			title=self.title,
			description=self.description,
			user=uploader,
			score=score,
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
