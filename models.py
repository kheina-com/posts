from typing import List, Optional, Union
from pydantic import BaseModel
from enum import Enum, unique


@unique
class PostSort(Enum) :
	top: str = 'top'
	hot: str = 'hot'
	best: str = 'best'
	controversial: str = 'controversial'
	new: str = 'new'
	old: str = 'old'


class VoteRequest(BaseModel) :
	post_id: str
	vote: Union[int, type(None)]


class BaseFetchRequest(BaseModel) :
	sort: PostSort
	tags: List[str]
	count: int = 64
	page: int = 1


class FetchPostsRequest(BaseFetchRequest) :
	tags: List[str]


class GetPostRequest(BaseModel) :
	post_id: str
