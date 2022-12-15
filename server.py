from models import BaseFetchRequest, FetchCommentsRequest, FetchPostsRequest, GetUserPostsRequest, Post, RssDateFormat, RssFeed, RssItem, RssTitle, RssDescription, RssMedia, Score, TimelineRequest, VoteRequest
from kh_common.server import Request, Response, ServerApp
from kh_common.config.constants import users_host
from kh_common.backblaze import B2Interface
from kh_common.models.user import User
from kh_common.gateway import Gateway
from asyncio import ensure_future
from urllib.parse import quote
from typing import List
from posts import Posts
from html import escape


app = ServerApp(
	auth_required = False,
	allowed_hosts = [
		'localhost',
		'127.0.0.1',
		'*.kheina.com',
		'kheina.com',
		'*.fuzz.ly',
		'fuzz.ly',
	],
	allowed_origins = [
		'localhost',
		'127.0.0.1',
		'dev.kheina.com',
		'kheina.com',
		'dev.fuzz.ly',
		'fuzz.ly',
	],
)
b2 = B2Interface()
posts = Posts()
UsersService = Gateway(users_host + '/v1/fetch_self', User)


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


@app.post('/v1/timeline_posts', responses={ 200: { 'model': List[Post] } })
async def v1TimelinePosts(req: Request, body: TimelineRequest) -> List[Post] :
	await req.user.authenticated()
	return await posts.timelinePosts(req.user, body.count, body.page)


async def get_post_media(post: Post) :
	file_info = await b2.b2_get_file_info(post.post_id, post.filename)
	return RssMedia.format(
		url=f'https://cdn.kheina.com/file/kheina-content/{post.post_id}/{escape(quote(post.filename))}',
		mime_type=file_info['contentType'],
		length=file_info['contentLength'],
	)


@app.get('/v1/feed.rss')
async def v1Rss(req: Request) :
	await req.user.authenticated()

	timeline = ensure_future(posts.RssFeedPosts(req.user))
	user = ensure_future(UsersService(auth=req.user.token.token_string))

	retrieved, timeline = await timeline
	media = { }

	for post in timeline :
		if post.filename :
			media[post.post_id] = ensure_future(get_post_media(post))

	user = await user

	return Response(
		media_type='application/xml',
		content=RssFeed.format(
			description=f'RSS feed timeline for @{user.handle}',
			pub_date=(
				max(map(lambda post : post.updated, timeline))
				if timeline else retrieved
			).strftime(RssDateFormat),
			last_build_date=retrieved.strftime(RssDateFormat),
			items='\n'.join([
				RssItem.format(
					title=RssTitle.format(escape(post.title)) if post.title else '',
					link=f'https://dev.kheina.com/p/{post.post_id}',
					description=RssDescription.format(escape(post.description)) if post.description else '',
					user=f'https://dev.kheina.com/{post.user.handle}',
					created=post.created.strftime(RssDateFormat),
					media=await media[post.post_id] if post.filename else '',
					post_id=post.post_id,
				) for post in timeline
			]),
		),
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5003)
