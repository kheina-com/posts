from models import BaseFetchRequest, FetchPostsRequest, GetPostRequest, VoteRequest
from starlette.middleware.trustedhost import TrustedHostMiddleware
from kh_common.exceptions import jsonErrorHandler
from starlette.responses import UJSONResponse
from kh_common.auth import KhAuthMiddleware
from fastapi import FastAPI, Request
from kh_common.scoring import _sign
from posts import Posts


app = FastAPI()
app.add_exception_handler(Exception, jsonErrorHandler)
app.add_middleware(TrustedHostMiddleware, allowed_hosts={ 'localhost', '127.0.0.1', 'posts.kheina.com', 'posts-dev.kheina.com' })
app.add_middleware(KhAuthMiddleware)

posts = Posts()


@app.on_event('shutdown')
async def shutdown() :
	posts.close()


@app.post('/v1/vote')
async def v1Vote(req: Request, body: VoteRequest) :
	vote = True if req.vote > 0 else False if req.vote < 0 else None

	return UJSONResponse(
		posts.vote(req.user.user_id, body.post_id, vote)
	)


@app.post('/v1/fetch_posts')
async def v1FetchPosts(req: Request, body: FetchPostsRequest) :
	return UJSONResponse(
		posts.fetchPosts(req.user.user_id, body.sort, body.tags, body.count, body.page)
	)


@app.post('/v1/fetch_my_posts')
async def v1GetPost(req: Request, body: GetPostRequest) :
	return UJSONResponse(
		posts.getPost(req.user.user_id, body.post_id)
	)


@app.post('/v1/get_post')
async def v1FetchMyPosts(req: Request, body: BaseFetchRequest) :
	return UJSONResponse(
		posts.fetchUserPosts(req.user.user_id, body.sort, body.count, body.page)
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='127.0.0.1', port=5003)
