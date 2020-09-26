from enum import Enum, unique


@unique
class PostSort(Enum) :
	top: int = 1
	hot: int = 2
	best: int = 3
	controversial: int = 4
