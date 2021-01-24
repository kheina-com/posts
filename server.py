from models import BaseFetchRequest, FetchPostsRequest, GetPostRequest, VoteRequest
from kh_common.server import Request, ServerApp, UJSONResponse
from posts import Posts


app = ServerApp(auth_required=False)
posts = Posts()


@app.on_event('shutdown')
async def shutdown() :
	posts.close()


@app.post('/v1/vote')
async def v1Vote(req: Request, body: VoteRequest) :
	req.user.authenticated()
	vote = True if body.vote > 0 else False if body.vote < 0 else None

	return UJSONResponse(
		posts.vote(req.user.user_id, body.post_id, vote)
	)


@app.post('/v1/fetch_posts')
async def v1FetchPosts(req: Request, body: FetchPostsRequest) :
	return UJSONResponse(
		posts.fetchPosts(req.user.user_id, body.sort, body.tags, body.count, body.page)
	)


@app.post('/v1/get_post')
async def v1GetPost(req: Request, body: GetPostRequest) :
	return UJSONResponse(
		posts.getPost(req.user.user_id, body.post_id)
	)


@app.post('/v1/fetch_my_posts')
async def v1FetchMyPosts(req: Request, body: BaseFetchRequest) :
	req.user.authenticated()
	return UJSONResponse(
		posts.fetchUserPosts(req.user.user_id, body.sort, body.count, body.page)
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5003)
