from datetime import datetime
from enum import Enum, unique
from re import Pattern
from re import compile as re_compile
from typing import List, Optional, Union

from kh_common.base64 import b64decode, b64encode
from kh_common.caching import ArgsCache
from kh_common.config.constants import Environment, environment
from kh_common.config.repo import short_hash
from kh_common.models.privacy import Privacy
from kh_common.models.rating import Rating
from kh_common.models.user import UserPortable
from pydantic import BaseModel


class PostId(str) :
	"""
	automatically converts post ids in int, byte, or string format to their user-friendly str format.
	also checks for valid values.
	"""

	__str_format__: Pattern = re_compile(r'^[a-zA-Z0-9_-]{8}$')

	def __new__(cls, value: Union[str, bytes, int]) :
		# technically, the only thing needed to be done here to utilize the full 64 bit range is update the 6 bytes encoding to 8 and the allowed range in the int subtype
		# secret code to map uint to int, preserving positive values. this is only needed if bumping to 64 bit postids
		# int.from_bytes(int.to_bytes(int_value, 8, 'big'), 'big', signed=True)
		value_type: type = type(value)

		if value_type == str :
			if not PostId.__str_format__.match(value) :
				raise ValueError('str values must be in the format of /^[a-zA-Z0-9_-]{8}$/')

			return super(PostId, cls).__new__(cls, value)

		elif value_type == int :
			# the range of a 48 bit int stored in a 64 bit int (both starting at min values)
			if not 0 <= value <= 281474976710655 :
				raise ValueError('int values must be between 0 and 281474976710655.')

			return super(PostId, cls).__new__(cls, b64encode(int.to_bytes(value, 6, 'big')).decode())

		elif value_type == bytes :
			if len(value) != 6 :
				raise ValueError('bytes values must be exactly 6 bytes.')

			return super(PostId, cls).__new__(cls, b64encode(value).decode())

		else :
			raise NotImplementedError('value must be of type str, bytes, or int.')


	@ArgsCache(60)
	def int(self: 'PostId') -> int :
		return int.from_bytes(b64decode(self), 'big')


@unique
class PostSort(Enum) :
	new: str = 'new'
	old: str = 'old'
	top: str = 'top'
	hot: str = 'hot'
	best: str = 'best'
	controversial: str = 'controversial'


class VoteRequest(BaseModel) :
	post_id: PostId
	vote: Union[int, None]


class TimelineRequest(BaseModel) :
	count: Optional[int] = 64
	page: Optional[int] = 1


class BaseFetchRequest(TimelineRequest) :
	sort: PostSort


class FetchPostsRequest(BaseFetchRequest) :
	tags: Optional[List[str]]


class FetchCommentsRequest(BaseFetchRequest) :
	post_id: PostId


class GetUserPostsRequest(BaseModel) :
	handle: str
	count: Optional[int] = 64
	page: Optional[int] = 1


class Score(BaseModel) :
	up: int
	down: int
	total: int
	user_vote: int


class MediaType(BaseModel) :
	file_type: str
	mime_type: str


class PostSize(BaseModel) :
	width: int
	height: int


class Post(BaseModel) :
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


RssFeed = f"""<rss version="2.0">
<channel>
<title>Timeline | fuzz.ly</title>
<link>{'https://dev.fuzz.ly/timeline' if environment != Environment.prod else 'https://fuzz.ly/timeline'}</link>
<description>{{description}}</description>
<language>en-us</language>
<pubDate>{{pub_date}}</pubDate>
<lastBuildDate>{{last_build_date}}</lastBuildDate>
<docs>https://www.rssboard.org/rss-specification</docs>
<generator>fuzz.ly - posts v.{short_hash}</generator>
<image>
<url>https://cdn.fuzz.ly/favicon.png</url>
<title>Timeline | fuzz.ly</title>
<link>{'https://dev.fuzz.ly/timeline' if environment != Environment.prod else 'https://fuzz.ly/timeline'}</link>
</image>
<ttl>1440</ttl>
{{items}}
</channel>
</rss>"""


RssItem = """<item>{title}
<link>{link}</link>{description}
<author>{user}</author>
<pubDate>{created}</pubDate>{media}
<guid>{post_id}</guid>
</item>"""


RssTitle = '\n<title>{}</title>'


RssDescription = '\n<description>{}</description>'


RssMedia = '\n<enclosure url="{url}" length="{length}" type="{mime_type}"/>'


RssDateFormat = '%a, %d %b %Y %H:%M:%S.%f %Z'