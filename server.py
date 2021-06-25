from models import BaseFetchRequest, FetchCommentsRequest, FetchPostsRequest, GetUserPostsRequest, Post, Score, VoteRequest
from kh_common.server import JsonResponse, NoContentResponse, Request, ServerApp
from typing import List
from posts import Posts


app = ServerApp(auth_required=False)
posts = Posts()


@app.on_event('shutdown')
async def shutdown() :
	posts.close()


@app.post('/v1/vote', responses={ 200: { 'model': Score } })
async def v1Vote(req: Request, body: VoteRequest) -> Score :
	await req.user.authenticated()
	vote = True if body.vote > 0 else False if body.vote < 0 else None

	return posts.vote(req.user, body.post_id, vote)

@app.post('/v1/fetch_posts', responses={ 200: { 'model': List[Post] } })
async def v1FetchPosts(req: Request, body: FetchPostsRequest) -> List[Post] :
	return await posts.fetchPosts(req.user, body.sort, body.tags, body.count, body.page)


@app.post('/v1/fetch_comments', responses={ 200: { 'model': List[Post] } })
async def v1FetchComments(req: Request, body: FetchCommentsRequest) -> List[Post] :
	return await posts.fetchComments(req.user, body.post_id, body.sort, body.count, body.page)


@app.get('/v1/post/{post_id}', responses={ 200: { 'model': Post } })
async def v1GetPost(req: Request, post_id: str) -> Post :
	return await posts.getPost(req.user, post_id)


@app.post('/v1/fetch_user_posts', responses={ 200: { 'model': List[Post] } })
async def v1FetchUserPosts(req: Request, body: GetUserPostsRequest) -> List[Post] :
	return await posts.fetchUserPosts(req.user, body.handle, body.count, body.page)


@app.post('/v1/fetch_my_posts', responses={ 200: { 'model': List[Post] } })
async def v1FetchMyPosts(req: Request, body: BaseFetchRequest) -> List[Post] :
	await req.user.authenticated()
	return await posts.fetchOwnPosts(req.user, body.sort, body.count, body.page)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5003)
