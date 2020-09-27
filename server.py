from models import FetchPostsRequest, GetPostRequest, VoteRequest
from kh_common.auth import authenticated, TokenData
from kh_common.exceptions import jsonErrorHandler
from kh_common.validation import validatedJson
from starlette.responses import UJSONResponse
from kh_common.logging import getLogger
from starlette.requests import Request
from traceback import format_tb
from posts import Posts
import time


logger = getLogger()
posts = Posts()


@jsonErrorHandler
@authenticated
@validatedJson
async def v1Vote(req: VoteRequest, token_data:TokenData=None) :
	"""
	{
		"post_id": str,
		"title": Optional[str],
		"description": Optional[str]
	}
	"""

	vote = True if req.vote > 0 else False if req.vote < 0 else None

	return UJSONResponse(
		posts.vote(token_data.data['user_id'], req.post_id, vote)
	)


@jsonErrorHandler
@authenticated
@validatedJson
async def v1FetchPosts(req: FetchPostsRequest, token_data:TokenData=None) :
	"""
	{
		"post_id": str,
		"privacy": str
	}
	"""

	return UJSONResponse(
		posts.fetchPosts(token_data.data['user_id'], req.sort, tuple(req.tags), req.count, req.page)
	)


@jsonErrorHandler
@authenticated
@validatedJson
async def v1GetPost(req: FetchPostsRequest, token_data:TokenData=None) :
	"""
	{
		"post_id": str,
	}
	"""

	return UJSONResponse(
		posts.getPost(token_data.data['user_id'], req.post_id)
	)


async def v1Help(req) :
	return UJSONResponse({
		'/v1/upload_image': {
			'auth': {
				'required': True,
				'user_id': 'int',
			},
			'file': 'image',
			'post_id': 'Optional[str]',
		},
	})


async def shutdown() :
	uploader.close()


from starlette.applications import Starlette
from starlette.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.routing import Route, Mount

middleware = [
	Middleware(TrustedHostMiddleware, allowed_hosts={ 'localhost', '127.0.0.1', 'upload.kheina.com', 'upload-dev.kheina.com' }),
]

routes = [
	Route('/v1/vote', endpoint=v1Vote, methods=('POST',)),
	Route('/v1/fetch_posts', endpoint=v1FetchPosts, methods=('POST',)),
	Route('/v1/get_post', endpoint=v1GetPost, methods=('POST',)),
	Route('/v1/help', endpoint=v1Help, methods=('GET',)),
]

app = Starlette(
	routes=routes,
	middleware=middleware,
	on_shutdown=[shutdown],
)

if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='127.0.0.1', port=5003)
