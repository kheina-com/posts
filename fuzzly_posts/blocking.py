from typing import Dict, Iterable, Set

from fuzzly_configs.api import ConfigClient
from fuzzly_configs.models import UserConfig
from kh_common.auth import KhUser
from kh_common.caching import ArgsCache
from kh_common.config.credentials import fuzzly_client_token


config_client: ConfigClient = ConfigClient(fuzzly_client_token, internal=True)


class BlockTree :

	def dict(self: 'BlockTree') :
		result = { }

		if not self.match and not self.nomatch :
			result['end'] = True

		if self.match :
			result['match'] = { k: v.dict() for k, v in self.match.items() }

		if self.nomatch :
			result['nomatch'] = { k: v.dict() for k, v in self.nomatch.items() }

		return result


	def __init__(self: 'BlockTree') :
		self.tags: Set[str] = None
		self.match: Dict[str, BlockTree] = None
		self.nomatch: Dict[str, BlockTree] = None


	def populate(self: 'BlockTree', tags: Iterable[Iterable[str]]) :
		for tag_set in tags :
			tree: BlockTree = self

			for tag in tag_set :
				match = True

				if tag.startswith('-') :
					match = False
					tag = tag[1:]

				if match :
					if not tree.match :
						tree.match = { }

					tree = tree.match

				else :
					if not tree.nomatch :
						tree.nomatch = { }

					tree = tree.nomatch

				if tag not in tree :
					tree[tag] = BlockTree()

				tree = tree[tag]


	def blocked(self: 'BlockTree', tags: Iterable[str]) -> bool :
		if not self.match and not self.nomatch :
			return False

		self.tags = set(tags)
		return self._blocked(self)


	def _blocked(self: 'BlockTree', tree: 'BlockTree') -> bool :
		# TODO: it really feels like there's a better way to do this check
		if not tree.match and not tree.nomatch :
			return True

		# eliminate as many keys immediately as possible, then iterate over them
		if tree.match :
			for key in tree.match.keys() & self.tags :
				if self._blocked(tree.match[key]) :
					return True

		if tree.nomatch :
			for key in tree.nomatch.keys() - self.tags :
				if self._blocked(tree.nomatch[key]) :
					return True

		return False


DefaultBlockTree: BlockTree = BlockTree()


@ArgsCache(5)
async def fetch_block_tree(user: KhUser) -> BlockTree :
	if not user.token :
		return DefaultBlockTree

	# TODO: return underlying UserConfig here, once internal tokens are implemented
	user_config: UserConfig = await config_client._config(user_id=user.user_id)
	tree: BlockTree = BlockTree()
	tree.populate(user_config.blocked_tags or [])
	return tree


async def is_post_blocked(user: KhUser, uploader: str, uploader_id: int, tags: Iterable[str]) -> bool :
	block_tree: BlockTree = await fetch_block_tree(user)

	tags: Set[str] = set(tags)
	tags.add('@' + uploader)

	return block_tree.blocked(tags)
