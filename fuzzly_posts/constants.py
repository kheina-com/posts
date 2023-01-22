from kh_common.config.constants import Environment, environment


CONSTANTS = {
	Environment.test: {
		'Host': '',
	},
	Environment.local: {
		'Host': 'http://localhost:5003',
	},
	Environment.dev: {
		'Host': 'https://posts-dev.fuzz.ly',
	},
	Environment.prod: {
		'Host': 'https://posts.fuzz.ly',
	},
}


locals().update(CONSTANTS[environment])


del CONSTANTS
