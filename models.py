from typing import List, Optional, Union
from pydantic import BaseModel
from enum import Enum, unique


@unique
class PostSort(Enum) :
	top: str = 'top'
	hot: str = 'hot'
	best: str = 'best'
	controversial: str = 'controversial'
	# new: str = 'new'
	# old: str = 'old'


class VoteRequest(BaseModel) :
	post_id: str
	vote: Union[int, type(None)]


class BaseFetchRequest(BaseModel) :
	sort: PostSort
	count: Optional[int] = 64
	page: Optional[int] = 1


class FetchPostsRequest(BaseFetchRequest) :
	tags: Optional[List[str]]


class FetchCommentsRequest(BaseFetchRequest) :
	post_id: str


class GetUserPostsRequest(BaseModel) :
	handle: str
	count: Optional[int] = 64
	page: Optional[int] = 1
