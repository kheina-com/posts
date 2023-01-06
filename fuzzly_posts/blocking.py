from typing import Dict, Iterable, List, Set


class BlockTree :

	def dict(self) :
		result = { }

		if not self.match and not self.nomatch :
			result['end'] = True

		if self.match :
			result['match'] = { k: v.dict() for k, v in self.match.items() }

		if self.nomatch :
			result['nomatch'] = { k: v.dict() for k, v in self.nomatch.items() }

		return result


	def __init__(self) :
		self.tags: Set[str] = None
		self.match: Dict[str, BlockTree] = None
		self.nomatch: Dict[str, BlockTree] = None


	def populate(self, tags: Iterable[Iterable[str]]) :
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


	def blocked(self, tags: Iterable[str]) -> bool :
		self.tags = set(tags)
		return self._blocked(self)


	def _blocked(self, tree: 'BlockTree') -> bool :
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
