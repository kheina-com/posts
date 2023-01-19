from typing import List

from kh_common.config.constants import posts_host
from kh_common.gateway import Gateway

from fuzzly_posts.models import Post


PostGateway: Gateway = Gateway(posts_host + '/v1/post/{post}', Post, method='GET')
MyPostsGateway: Gateway = Gateway(posts_host + '/v1/fetch_my_posts', List[Post], method='POST')
