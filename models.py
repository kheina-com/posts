from typing import List, Optional, Union
from pydantic import BaseModel
from enum import Enum, unique


@unique
class PostSort(Enum) :
	top: str = 'top'
	hot: str = 'hot'
	best: str = 'best'
	controversial: str = 'controversial'


class VoteRequest(BaseModel) :
	post_id: str
	vote: Union[int, type(None)]


class FetchPostsRequest(BaseModel) :
	sort: PostSort
	tags: List[str]
	count: int = 64
	page: int = 1


class GetPostRequest(BaseModel) :
	post_id: str
