from kh_common.exceptions import checkJsonKeys, JsonErrorHandler
from starlette.responses import UJSONResponse
from kh_common.auth import AuthenticatedAsync
from kh_common.logging import getLogger
from traceback import format_tb
from tagger import Tagger
import time


logger = getLogger()
tagger = Tagger()


@JsonErrorHandler()
@AuthenticatedAsync()
async def v1AddTags(req, token_data={ }) :
	"""
	{
		"post_id": str,
		"tags": [
			str
		]
	}
	"""
	requestJson = await req.json()
	checkJsonKeys(requestJson, ['post_id', 'tags'])

	return UJSONResponse(
		tagger.removeTags(
			token_data['data']['user_id'],
			requestJson['post_id'],
			tuple(requestJson['tags']),
		)
	)


@JsonErrorHandler()
@AuthenticatedAsync()
async def v1RemoveTags(req, token_data={ }) :
	"""
	{
		"post_id": str,
		"tags": [
			str
		]
	}
	"""
	requestJson = await req.json()
	checkJsonKeys(requestJson, ['post_id', 'tags'])

	return UJSONResponse(
		tagger.removeTags(
			token_data['data']['user_id'],
			requestJson['post_id'],
			tuple(requestJson['tags']),
		)
	)


@JsonErrorHandler()
@AuthenticatedAsync()
async def v1InheritTag(req, token_data={ }) :
	"""
	{
		"parent_tag": str,
		"child_tag": str,
		"deprecate": Optional[bool],
		"admin": Optional[bool]
	}
	"""
	requestJson = await req.json()
	checkJsonKeys(requestJson, ['parent_tag', 'child_tag'])

	return UJSONResponse(
		tagger.inheritTag(
			token_data['data']['user_id'],
			requestJson['parent_tag'],
			requestJson['child_tag'],
			requestJson.get('deprecate'),
			token_data['data'].get('admin'),
		)
	)


@JsonErrorHandler()
@AuthenticatedAsync()
async def v1UpdateTag(req, token_data={ }) :
	"""
	{
		"tag": str,
		"tag_class": Optional[str],
		"owner": Optional[str],
		"admin": Optional[bool]
	}
	"""
	requestJson = await req.json()
	checkJsonKeys(requestJson, ['tag', 'tag_class'])

	return UJSONResponse(
		tagger.updateTag(
			token_data['data']['user_id'],
			requestJson['tag'],
			requestJson['tag_class'],
			requestJson.get('owner'),
			token_data['data'].get('admin'),
		)
	)


@JsonErrorHandler()
@AuthenticatedAsync()
async def v1FetchTags(req, token_data={ }) :
	"""
	{
		"post_id": str
	}
	"""
	requestJson = await req.json()
	checkJsonKeys(requestJson, ['post_id'])

	return UJSONResponse(
		tagger.fetchTagsByPost(
			token_data['data']['user_id'],
			requestJson['post_id'],
		)
	)


async def v1Help(req) :
	return UJSONResponse({
		'/v1/create_post': {
			'auth': {
				'required': True,
				'user_id': 'int',
			},
		},
		'/v1/upload_image': {
			'auth': {
				'required': True,
				'user_id': 'int',
			},
			'file': 'image',
			'post_id': 'Optional[str]',
		},
		'/v1/update_post': {
			'auth': {
				'required': True,
				'user_id': 'int',
			},
			'privacy': 'Optional[str]',
			'title': 'Optional[str]',
			'description': 'Optional[str]',
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
	Middleware(TrustedHostMiddleware, allowed_hosts={ 'localhost', '127.0.0.1', 'tags.kheina.com', 'tags-dev.kheina.com' }),
]

routes = [
	Route('/v1/add_tags', endpoint=v1AddTags, methods=('POST',)),
	Route('/v1/remove_tags', endpoint=v1RemoveTags, methods=('POST',)),
	Route('/v1/inherit_tag', endpoint=v1InheritTag, methods=('POST',)),
	Route('/v1/update_tag', endpoint=v1UpdateTag, methods=('POST',)),
	Route('/v1/fetch_tags', endpoint=v1FetchTags, methods=('POST',)),
	Route('/v1/help', endpoint=v1Help, methods=('GET',)),
]

app = Starlette(
	routes=routes,
	middleware=middleware,
	on_shutdown=[shutdown],
)

if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='127.0.0.1', port=5002)
