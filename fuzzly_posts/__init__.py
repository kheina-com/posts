from kh_common.gateway import Gateway
from fuzzly_posts.models import Post
from kh_common.config.constants import posts_host


__version__: str = '0.0.2'


PostGateway: Gateway = Gateway(posts_host + '/v1/post/{post}', Post, 'GET')
