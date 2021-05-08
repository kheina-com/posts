from models import BaseFetchRequest, FetchCommentsRequest, FetchPostsRequest, GetUserPostsRequest, VoteRequest
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
		posts.vote(req.user, body.post_id, vote)
	)


@app.post('/v1/fetch_posts')
async def v1FetchPosts(req: Request, body: FetchPostsRequest) :
	return UJSONResponse(
		await posts.fetchPosts(req.user, body.sort, body.tags, body.count, body.page)
	)


@app.post('/v1/fetch_comments')
async def v1FetchPosts(req: Request, body: FetchCommentsRequest) :
	return UJSONResponse(
		await posts.fetchComments(req.user, body.post_id, body.sort, body.count, body.page)
	)


@app.get('/v1/post/{post_id}')
async def v1GetPost(req: Request, post_id: str) :
	return UJSONResponse(
		posts.getPost(req.user, post_id)
	)


@app.post('/v1/fetch_user_posts')
async def v1FetchUserPosts(req: Request, body: GetUserPostsRequest) :
	return UJSONResponse(
		await posts.fetchUserPosts(req.user, body.handle, body.count, body.page)
	)


@app.post('/v1/fetch_my_posts')
async def v1FetchMyPosts(req: Request, body: BaseFetchRequest) :
	req.user.authenticated()
	return UJSONResponse(
		posts.fetchOwnPosts(req.user, body.sort, body.count, body.page)
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5003)
