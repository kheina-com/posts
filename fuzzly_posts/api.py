from kh_common.caching import AerospikeCache
from kh_common.caching.key_value_store import KeyValueStore
from kh_common.client import Client
from kh_common.gateway import Gateway

from fuzzly_posts.constants import Host
from fuzzly_posts.internal import InternalPost, Post


class PostClient(Client) :

	def __init__(self: 'PostClient', *a, internal=False, **kv) :
		super().__init__(*a, **kv)
		self._post: Gateway = None
		self._kvs: KeyValueStore = None
		self.post: Gateway = self.authenticated(Gateway(Host + '/v1/post/{post_id}', Post, method='GET'))
		self.my_posts: Gateway = self.authenticated(Gateway(Host + '/v1/my_posts', Post, method='POST'))

		if internal :
			self._kvs = KeyValueStore('kheina', 'posts')
			self._post = AerospikeCache('kheina', 'posts', '{post_id}', read_only=True, _kvs=self._kvs)(self.authenticated(Gateway(Host + '/i1/post/{post_id}', InternalPost, method='GET')))
