from typing import List

from kh_common.config.constants import posts_host
from kh_common.gateway import Gateway

from fuzzly_posts.models import Post


__version__: str = '0.0.3'


PostGateway: Gateway = Gateway(posts_host + '/v1/post/{post}', Post, 'GET')
MyPostsGateway: Gateway = Gateway(posts_host + '/v1/fetch_my_posts', List[Post], method='POST')
