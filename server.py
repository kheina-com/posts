from asyncio import ensure_future
from html import escape
from typing import List
from urllib.parse import quote

from kh_common.backblaze import B2Interface
from kh_common.config.constants import environment, users_host
from kh_common.gateway import Gateway
from kh_common.models.auth import Scope
from kh_common.models.user import User
from kh_common.server import Request, Response, ServerApp

from fuzzly_posts.internal import InternalPost, Post
from fuzzly_posts.models import BaseFetchRequest, FetchCommentsRequest, FetchPostsRequest, GetUserPostsRequest, PostId, RssDateFormat, RssDescription, RssFeed, RssItem, RssMedia, RssTitle, Score, TimelineRequest, VoteRequest
from fuzzly_posts.scoring import Scoring
from posts import Posts


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
Scores: Scoring = Scoring()


@app.on_event('shutdown')
async def shutdown() :
	posts.close()


################################################## INTERNAL ##################################################
@app.get('/i1/post/{post_id}', response_model=InternalPost)
async def i1Post(req: Request, post_id: PostId) -> InternalPost :
	await req.user.verify_scope(Scope.internal)
	return await posts._get_post(PostId(post_id))


##################################################  PUBLIC  ##################################################
@app.get('/v1/post/{post_id}', responses={ 200: { 'model': Post } })
async def v1Post(req: Request, post_id: PostId) -> Post :
	# fastapi doesn't parse to PostId automatically, only str
	return await posts.getPost(req.user, PostId(post_id))


@app.post('/v1/vote', responses={ 200: { 'model': Score } })
async def v1Vote(req: Request, body: VoteRequest) -> Score :
	await req.user.authenticated(Scope.user)
	vote = True if body.vote > 0 else False if body.vote < 0 else None
	return posts.vote(req.user, body.post_id, vote)


@app.post('/v1/fetch_posts', responses={ 200: { 'model': List[Post] } })
@app.post('/v1/posts', responses={ 200: { 'model': List[Post] } })
async def v1FetchPosts(req: Request, body: FetchPostsRequest) -> List[Post] :
	return await posts.fetchPosts(req.user, body.sort, body.tags, body.count, body.page)


@app.post('/v1/fetch_comments', responses={ 200: { 'model': List[Post] } })
@app.post('/v1/comments', responses={ 200: { 'model': List[Post] } })
async def v1FetchComments(req: Request, body: FetchCommentsRequest) -> List[Post] :
	return await posts.fetchComments(req.user, body.post_id, body.sort, body.count, body.page)


@app.post('/v1/fetch_user_posts', responses={ 200: { 'model': List[Post] } })
@app.post('/v1/user_posts', responses={ 200: { 'model': List[Post] } })
async def v1FetchUserPosts(req: Request, body: GetUserPostsRequest) -> List[Post] :
	return await posts.fetchUserPosts(req.user, body.handle, body.count, body.page)


@app.post('/v1/fetch_my_posts', responses={ 200: { 'model': List[Post] } })
@app.post('/v1/my_posts', responses={ 200: { 'model': List[Post] } })
async def v1FetchMyPosts(req: Request, body: BaseFetchRequest) -> List[Post] :
	await req.user.authenticated()
	return await posts.fetchOwnPosts(req.user, body.sort, body.count, body.page)


@app.get('/v1/fetch_drafts', responses={ 200: { 'model': List[Post] } })
@app.get('/v1/drafts', responses={ 200: { 'model': List[Post] } })
async def v1FetchDrafts(req: Request) -> List[Post] :
	await req.user.authenticated()
	return await posts.fetchDrafts(req.user)


@app.post('/v1/timeline_posts', responses={ 200: { 'model': List[Post] } })
@app.post('/v1/timeline', responses={ 200: { 'model': List[Post] } })
async def v1TimelinePosts(req: Request, body: TimelineRequest) -> List[Post] :
	await req.user.authenticated()
	return await posts.timelinePosts(req.user, body.count, body.page)


async def get_post_media(post: Post) -> str :
	filename: str = f'{post.post_id}/{escape(quote(post.filename))}'
	file_info = await b2.b2_get_file_info(filename)
	return RssMedia.format(
		url='https://cdn.fuzz.ly/' + filename,
		mime_type=file_info['contentType'],
		length=file_info['contentLength'],
	)


@app.get('/v1/feed.rss', response_model=str)
async def v1Rss(req: Request) -> Response :
	await req.user.authenticated(Scope.user)

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
					link=f'https://fuzz.ly/p/{post.post_id}' if environment.is_prod() else f'https://dev.fuzz.ly/p/{post.post_id}',
					description=RssDescription.format(escape(post.description)) if post.description else '',
					user=f'https://fuzz.ly/{post.user.handle}' if environment.is_prod() else f'https://dev.fuzz.ly/{post.user.handle}',
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
