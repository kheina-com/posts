from typing import List

from kh_common.caching import AerospikeCache
from kh_common.client import Client
from kh_common.gateway import Gateway

from fuzzly_posts.constants import Host
from fuzzly_posts.internal import InternalPost, Post


PostGateway: Gateway = Gateway(Host + '/v1/post/{post}', Post, method='GET')
MyPostsGateway: Gateway = Gateway(Host + '/v1/fetch_my_posts', List[Post], method='POST')


class PostClient(Client) :

	def __init__(self: 'PostClient', *a, internal=False, **kv) :
		super().__init__(*a, **kv)
		self._post: Gateway = None
		self.post: Gateway = self.authenticated(Gateway(Host + '/v1/post/{post_id}', Post, method='GET'))
		self.my_posts: Gateway = self.authenticated(Gateway(Host + '/v1/my_posts', Post, method='POST'))

		if internal :
			self._post = AerospikeCache('kheina', 'posts', '{post_id}', read_only=True)(self.authenticated(Gateway(Host + '/i1/post/{post_id}', InternalPost, method='GET')))
