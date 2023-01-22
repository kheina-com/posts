from typing import List

from fuzzly_posts.constants import Host
from kh_common.gateway import Gateway

from fuzzly_posts.internal import Post


PostGateway: Gateway = Gateway(Host + '/v1/post/{post}', Post, method='GET')
MyPostsGateway: Gateway = Gateway(Host + '/v1/fetch_my_posts', List[Post], method='POST')
